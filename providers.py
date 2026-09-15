
import time
import json
import socket
import queue
import logging
import threading
import requests
from abc import ABC, abstractmethod
from collections import defaultdict

# Shure ULX-D/QLX-D/SLX-D "Command Strings" protocol (Ethernet TCP port
# 2202, ASCII). Confirmed live against a real ULXD4Q receiver, and matches
# Shure's published spec:
# https://content-files.shure.com/Pubs/ulx/ulx-d-network-string-commands.pdf
#   GET  < GET  x PARAM       >  ->  < REP x PARAM value >
#   SET  < SET  x PARAM value >  ->  < REP x PARAM value >
#   metering, once enabled: < SAMPLE x ALL nn aaa eee > pushed periodically
#     aaa = RF level 000-115 (subtract 128 for dBm); eee = audio level 000-050
# "x" is the channel (1-4 on multi-channel receivers; 0 means "all channels").

# Confirming a SET needs a longer read window than a routine poll: a device
# with metering already running (poll() enables it on first connect) is
# also pushing an unsolicited SAMPLE roughly once a second, so there can be
# real backlog to drain through before the actual confirmation shows up.
CONFIRM_READ_TIMEOUT = 1.2

# A dead network doesn't always fail loudly: an established TCP socket often
# accepts writes and simply times out on reads instead of raising -- so a
# provider that only flips to DISCONNECTED on a hard socket error can stay
# "CONNECTED" (with frozen, increasingly stale metrics) indefinitely once its
# network path is gone. Every polling provider below instead counts
# consecutive empty poll cycles and, past this limit, marks itself
# disconnected AND clears its metrics -- so a device the app can no longer
# actually hear from stops showing battery/RF/audio levels instead of
# silently repeating the last numbers it happened to have.
NETWORK_MISS_LIMIT = 5

class BaseProvider(ABC):
    def __init__(self, ip, device_type, photo=None):
        self.ip = ip
        self.type = device_type
        self.photo = photo # Path to the photo
        self.status = 'DISCONNECTED'
        self.metrics = {}
        self.spectrum_data = []
        self.alerts = []
        # Polling and a command can now run on different threads at once
        # (both go through omniwave.py's DEVICE_EXECUTOR); this serializes
        # any two operations against this *same* device's socket/buffer so
        # they can't interleave and corrupt each other, while leaving other
        # devices free to run fully in parallel.
        self._io_lock = threading.Lock()

    @abstractmethod
    def connect(self): pass
    @abstractmethod
    def disconnect(self): pass
    @abstractmethod
    def poll(self): pass
    @abstractmethod
    def scan_rf(self): pass
    @abstractmethod
    def send_command(self, cmd_type, value, channel=1): pass

    def get_json(self):
        return {
            'ip': self.ip,
            'type': self.type,
            'photo': self.photo,
            'status': self.status,
            'metrics': self.metrics,
            'spectrum': self.spectrum_data,
            'alerts': self.alerts,
            'model': getattr(self, 'model', None),
            'channel': getattr(self, 'channel', 1),
        }

class ShureProvider(BaseProvider):
    def __init__(self, ip, device_type, photo=None, channel=1):
        super().__init__(ip, device_type, photo)
        self.sock = None
        self.write_queue = queue.Queue()
        self.channel = channel
        self.model = None
        self._buffer = ''
        self._metering_started = False
        self._miss_count = 0

    def connect(self):
        with self._io_lock:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(0.5)
                self.sock.connect((self.ip, 2202))
                self.status = 'CONNECTED'
                self._metering_started = False
                self._buffer = ''
                self._miss_count = 0
            except Exception:
                self.status = 'DISCONNECTED'

    def _mark_unreachable(self):
        self.status = 'DISCONNECTED'
        self.metrics = {}
        self._metering_started = False
        self._miss_count = 0

    def disconnect(self):
        with self._io_lock:
            if self.sock:
                try: self.sock.close()
                except Exception: pass
            self.status = 'DISCONNECTED'

    def _send(self, message):
        self.sock.sendall(message.encode('ascii'))

    def _read_messages(self, timeout=0.4, stop_early=True):
        """Read whatever arrives within `timeout`, split into complete
        '< ... >' command strings. Drains in short sub-reads rather than a
        single recv() -- a response split across TCP segments, or one that
        lands a beat after a SET, would otherwise get left in the kernel
        buffer for whatever the *next* call happens to be (e.g. the next
        poll cycle), which is exactly wrong when this call is checking for
        a specific command's own confirmation.

        stop_early=True (poll()'s use) returns as soon as a quiet gap
        follows some data, since polling doesn't need the full window.
        stop_early=False (send_command()'s confirmation reads) keeps
        draining for the whole timeout regardless of gaps -- an
        unrelated SAMPLE can arrive first with a real pause before the
        actual confirmation shows up, and bailing on that gap was
        exactly the bug that made confirmation reads flaky."""
        deadline = time.time() + timeout
        self.sock.settimeout(0.1)
        got_any = False
        while time.time() < deadline:
            try:
                chunk = self.sock.recv(4096)
                if not chunk:
                    break
                self._buffer += chunk.decode('ascii', errors='ignore')
                got_any = True
            except socket.timeout:
                if stop_early and got_any:
                    break
        messages = []
        while True:
            start = self._buffer.find('<')
            end = self._buffer.find('>', start)
            if start == -1 or end == -1:
                break
            messages.append(self._buffer[start + 1:end].strip())
            self._buffer = self._buffer[end + 1:]
        return messages

    def _apply_report(self, param, value):
        value = value.strip()
        if param == 'BATT_CHARGE':
            # Per Shure's spec, 255/254/253/252 on a 3-digit field mean the
            # transmitter is off/unavailable/using non-rechargeable
            # batteries -- not a literal 255% charge.
            try:
                parsed = int(value)
                self.metrics['batt'] = None if parsed >= 252 else parsed
            except ValueError: pass
        elif param == 'FREQUENCY':
            try: self.metrics['frequency_mhz'] = int(value) / 1000
            except ValueError: pass
        elif param == 'MODEL':
            self.model = value.strip('{}').strip()
        elif param == 'AUDIO_MUTE':
            self.metrics['muted'] = (value == 'ON')
        elif param == 'RF_INT_DET' and value == 'CRITICAL':
            alert = 'RF Interference Detected'
            if not self.alerts or self.alerts[-1] != alert:
                self.alerts.append(alert)
                self.alerts = self.alerts[-20:]

    def _parse_messages(self, messages):
        for msg in messages:
            parts = msg.split()
            if not parts:
                continue
            if parts[0] == 'SAMPLE' and len(parts) >= 6:
                # SAMPLE x ALL nn aaa eee -- nn=antenna LEDs, aaa=RF (000-115,
                # subtract 128 for dBm), eee=audio level (000-050)
                chan = parts[1]
                if chan not in ('0', str(self.channel)):
                    continue
                try:
                    self.metrics['rf'] = int(parts[4]) - 128
                    self.metrics['audio'] = int(parts[5])
                except (ValueError, IndexError):
                    pass
                continue
            if parts[0] != 'REP' or len(parts) < 2:
                continue
            rest = parts[1:]
            if rest[0].isdigit():
                # channel-scoped: REP <chan> <PARAM> <value...>
                if len(rest) < 3 or rest[0] not in ('0', str(self.channel)):
                    continue
                self._apply_report(rest[1], ' '.join(rest[2:]))
            else:
                # device-scoped: REP <PARAM> <value...>
                self._apply_report(rest[0], ' '.join(rest[1:]))

    def poll(self):
        if self.status != 'CONNECTED': return
        with self._io_lock:
            try:
                if not self._metering_started:
                    self._send(f'< SET {self.channel} METER_RATE 01000 >')
                    self._metering_started = True
                self._send(f'< GET {self.channel} BATT_CHARGE >')
                self._send(f'< GET {self.channel} FREQUENCY >')
                messages = self._read_messages()
                self._parse_messages(messages)
                if not messages:
                    self._miss_count += 1
                    if self._miss_count >= NETWORK_MISS_LIMIT:
                        self._mark_unreachable()
                        return
                else:
                    self._miss_count = 0

                batt = self.metrics.get('batt')
                if batt is not None and batt < 20:
                    alert = f"Low Battery: {batt}%"
                    if not self.alerts or self.alerts[-1] != alert:
                        self.alerts.append(alert)
                        self.alerts = self.alerts[-20:]
            except (OSError, socket.error):
                self._mark_unreachable()

    def scan_rf(self):
        # Wideband spectrum scanning isn't part of this documented ASCII
        # command-string protocol -- WWB drives it through a separate,
        # undocumented mechanism. Left empty rather than fabricating
        # scan-shaped data for a capability we can't actually perform yet.
        self.spectrum_data = []

    def send_command(self, cmd_type, value, channel=1):
        """Sends the SET and reads back the receiver's own REP confirmation
        before reporting success -- a successful socket write only means the
        bytes went out, not that the hardware actually applied the change."""
        if self.status != 'CONNECTED': return False
        with self._io_lock:
            try:
                if cmd_type == 'MUTE':
                    self._send(f'< SET {channel} AUDIO_MUTE {"ON" if value else "OFF"} >')
                    self._parse_messages(self._read_messages(timeout=CONFIRM_READ_TIMEOUT, stop_early=False))
                    return self.metrics.get('muted') == bool(value)
                elif cmd_type == 'FREQUENCY':
                    khz = int(round(float(value) * 1000))
                    self._send(f'< SET {channel} FREQUENCY {khz:06d} >')
                    self._parse_messages(self._read_messages(timeout=CONFIRM_READ_TIMEOUT, stop_early=False))
                    return self.metrics.get('frequency_mhz') == round(khz / 1000, 4)
                else:
                    self._send(f'< SET {channel} {cmd_type} {value} >')
                    return True
            except (OSError, socket.error, ValueError):
                return False

# Shure UHF-R (UR4S/UR4D) "Command Strings" protocol -- an older, different
# protocol from ULX-D/QLX-D. Confirmed live against a real UR4D: transport is
# UDP (the source bulletin's "Connection: Ethernet (UPD/IP)" line is a typo --
# verified empirically, since TCP gets no response at all), and messages are
# '*'-delimited rather than '< >'.
# https://content-files.shure.com/KnowledgeBaseFiles/uhfr-network-string-commands.pdf
#   GET/SET  * GET/SET x PARAM [value] *  ->  * REPORT x PARAM value *
#   metering: * METER x ALL sss * (sss = speed in 30ms steps) enables periodic
#     * SAMPLE x ALL nn aaa bbb d eee *
#   nn=antenna LEDs; aaa/bbb=RF level per antenna (coarse category, NOT dBm:
#     020=overload, 070=strong ... 100=weak); d=battery 1-5/U; eee=audio 0-255
# "x" is the channel: "1" (single/UR4S or left) or "2" (right, UR4D only).
UHFR_METER_STEPS = 40  # 40 * 30ms ~= 1.2s update interval
UHFR_MISS_LIMIT = 5    # consecutive empty polls before considering it unreachable

class UHFRProvider(BaseProvider):
    def __init__(self, ip, device_type, photo=None, channel=1):
        super().__init__(ip, device_type, photo)
        self.channel = channel
        self.sock = None
        self._metering_started = False
        self._miss_count = 0

    def _mark_unreachable(self):
        self.status = 'DISCONNECTED'
        self.metrics = {}
        self._metering_started = False
        self._miss_count = 0

    def connect(self):
        with self._io_lock:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.sock.settimeout(0.5)
                # UDP has no handshake -- poll() confirms real reachability
                # by tracking consecutive misses, not by trusting socket().
                self.status = 'CONNECTED'
                self._metering_started = False
                self._miss_count = 0
            except Exception:
                self.status = 'DISCONNECTED'

    def disconnect(self):
        with self._io_lock:
            if self.sock:
                try: self.sock.close()
                except Exception: pass
            self.status = 'DISCONNECTED'

    def _send(self, message):
        self.sock.sendto(message.encode('ascii'), (self.ip, 2202))

    def _read_messages(self, timeout=0.4, max_reads=8, stop_early=True):
        """UHF-R sends one UDP datagram per '* ... *' message, bounded by
        the overall `timeout` budget either way (not `timeout` per read --
        that could add up to max_reads * timeout for a slow trickle).

        stop_early=True (poll()'s use) gives up as soon as one read times
        out. stop_early=False (send_command()'s confirmation reads) keeps
        retrying in short sub-reads for the whole budget -- an unrelated
        SAMPLE arriving first can leave a real gap before the actual
        confirmation shows up, and bailing on that gap was exactly the
        bug that made confirmation reads flaky."""
        deadline = time.time() + timeout
        messages = []
        reads = 0
        while reads < max_reads or not stop_early:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            self.sock.settimeout(remaining if stop_early else min(0.15, remaining))
            try:
                data, _ = self.sock.recvfrom(4096)
            except socket.timeout:
                if stop_early:
                    break
                continue
            reads += 1
            text = data.decode('ascii', errors='ignore')
            start = text.find('*')
            end = text.rfind('*')
            if start != -1 and end > start:
                messages.append(text[start + 1:end].strip())
        return messages

    def _apply_report(self, param, value):
        value = value.strip()
        if param == 'TX_BAT':
            # 1-5 bars -> rough 0-100; 'U' = undefined (transmitter off/absent)
            self.metrics['batt'] = None if value == 'U' else int(value) * 20
        elif param == 'FREQUENCY':
            try: self.metrics['frequency_mhz'] = int(value) / 1000
            except ValueError: pass
        elif param == 'MUTE':
            self.metrics['muted'] = (value == 'ON')

    def _parse_messages(self, messages):
        if messages:
            self._miss_count = 0
        for msg in messages:
            parts = msg.split()
            if not parts:
                continue
            if parts[0] == 'SAMPLE' and len(parts) >= 8:
                # SAMPLE x ALL nn aaa bbb d eee
                if parts[1] != str(self.channel):
                    continue
                try:
                    aaa, bbb = int(parts[4]), int(parts[5])
                    audio_raw = int(parts[7])
                except (ValueError, IndexError):
                    continue
                strongest = min(aaa, bbb)
                if strongest <= 20:
                    alert = 'RF Overload'
                    if not self.alerts or self.alerts[-1] != alert:
                        self.alerts.append(alert)
                        self.alerts = self.alerts[-20:]
                    quality = 20
                else:
                    quality = max(0, 100 - strongest)
                self.metrics['rf'] = quality
                self.metrics['audio'] = round(audio_raw / 255 * 100)
                continue
            if parts[0] != 'REPORT' or len(parts) < 2:
                continue
            rest = parts[1:]
            if rest[0].isdigit():
                if len(rest) < 3 or rest[0] != str(self.channel):
                    continue
                self._apply_report(rest[1], ' '.join(rest[2:]))
            else:
                self._apply_report(rest[0], ' '.join(rest[1:]))

    def poll(self):
        if self.status != 'CONNECTED': return
        with self._io_lock:
            try:
                if not self._metering_started:
                    self._send(f'* METER {self.channel} ALL {UHFR_METER_STEPS:03d} *')
                    self._metering_started = True
                self._send(f'* GET {self.channel} TX_BAT *')
                self._send(f'* GET {self.channel} FREQUENCY *')
                messages = self._read_messages()
                self._parse_messages(messages)
                if not messages:
                    self._miss_count += 1
                    if self._miss_count >= UHFR_MISS_LIMIT:
                        self._mark_unreachable()
                        return
                else:
                    self._miss_count = 0

                batt = self.metrics.get('batt')
                if batt is not None and batt < 20:
                    alert = f"Low Battery: {batt}%"
                    if not self.alerts or self.alerts[-1] != alert:
                        self.alerts.append(alert)
                        self.alerts = self.alerts[-20:]
            except (OSError, socket.error):
                self._mark_unreachable()

    def scan_rf(self):
        # No wideband scan in this documented protocol either.
        self.spectrum_data = []

    def send_command(self, cmd_type, value, channel=1):
        """Sends the SET and reads back the receiver's own REPORT
        confirmation before reporting success, same as ShureProvider."""
        if self.status != 'CONNECTED': return False
        with self._io_lock:
            try:
                if cmd_type == 'MUTE':
                    self._send(f'* SET {channel} MUTE {"ON" if value else "OFF"} *')
                    self._parse_messages(self._read_messages(timeout=CONFIRM_READ_TIMEOUT, stop_early=False))
                    return self.metrics.get('muted') == bool(value)
                elif cmd_type == 'FREQUENCY':
                    khz = int(round(float(value) * 1000))
                    self._send(f'* SET {channel} FREQUENCY {khz:06d} *')
                    self._parse_messages(self._read_messages(timeout=CONFIRM_READ_TIMEOUT, stop_early=False))
                    return self.metrics.get('frequency_mhz') == round(khz / 1000, 4)
                else:
                    self._send(f'* SET {channel} {cmd_type} {value} *')
                    return True
            except (OSError, socket.error, ValueError):
                return False

# Shure PSM1000 (P10T transmitter) network telemetry -- confirmed live
# against real hardware, but there is no public command-strings reference
# for this product the way there is for ULX-D/QLX-D/UHF-R. Unlike all of
# those, this is pure one-way push: the moment you connect on TCP port
# 2202 it continuously streams
#   < REPORT x AUDIO_IN_LVL_L yyy >
#   < REPORT x AUDIO_IN_LVL_R yyy >
# and does not respond to any GET/SET command tried, in either '< >' or
# '*' delimiter style, over TCP or UDP. So only what's actually observed
# on the wire is implemented -- no battery/frequency/RF, since the P10T
# exposes none of that here (it's a transmitter base station, not a
# receiver, so those wouldn't mean the same thing even if they existed).
PSM1000_ASSUMED_MAX = 1023  # no documented scale; treated as a rough 10-bit meter

class PSM1000Provider(BaseProvider):
    def __init__(self, ip, device_type, photo=None, channel=1):
        super().__init__(ip, device_type, photo)
        self.channel = channel
        self.sock = None
        self._buffer = ''
        self._miss_count = 0

    def connect(self):
        with self._io_lock:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(0.5)
                self.sock.connect((self.ip, 2202))
                self.status = 'CONNECTED'
                self._buffer = ''
                self._miss_count = 0
            except Exception:
                self.status = 'DISCONNECTED'

    def _mark_unreachable(self):
        self.status = 'DISCONNECTED'
        self.metrics = {}
        self._miss_count = 0

    def disconnect(self):
        with self._io_lock:
            if self.sock:
                try: self.sock.close()
                except Exception: pass
            self.status = 'DISCONNECTED'

    def _read_messages(self, timeout=0.4):
        self.sock.settimeout(timeout)
        try:
            chunk = self.sock.recv(8192)
            if chunk:
                self._buffer += chunk.decode('ascii', errors='ignore')
        except socket.timeout:
            pass
        messages = []
        while True:
            start = self._buffer.find('<')
            end = self._buffer.find('>', start)
            if start == -1 or end == -1:
                break
            messages.append(self._buffer[start + 1:end].strip())
            self._buffer = self._buffer[end + 1:]
        return messages

    def poll(self):
        if self.status != 'CONNECTED': return
        with self._io_lock:
            try:
                messages = self._read_messages()
                left = right = None
                for msg in messages:
                    parts = msg.split()
                    if len(parts) < 4 or parts[0] != 'REPORT' or parts[1] != str(self.channel):
                        continue
                    try:
                        value = int(parts[3])
                    except ValueError:
                        continue
                    if parts[2] == 'AUDIO_IN_LVL_L':
                        left = value
                    elif parts[2] == 'AUDIO_IN_LVL_R':
                        right = value
                readings = [v for v in (left, right) if v is not None]
                if readings:
                    peak = min(max(readings), PSM1000_ASSUMED_MAX)
                    self.metrics['audio'] = round(peak / PSM1000_ASSUMED_MAX * 100)

                if not messages:
                    self._miss_count += 1
                    if self._miss_count >= NETWORK_MISS_LIMIT:
                        self._mark_unreachable()
                        return
                else:
                    self._miss_count = 0
            except (OSError, socket.error):
                self._mark_unreachable()

    def scan_rf(self):
        self.spectrum_data = []

    def send_command(self, cmd_type, value, channel=1):
        return False  # no responsive control channel found for this device

# Shure SLX-D "Command Strings" protocol -- same TCP 2202 bracket syntax as
# ULX-D/QLX-D, but NOT hardware-verified (no SLX-D unit available to test
# against). Built from Shure's published spec (Version 2, 2020-G):
# https://www.shure.com/en-US/docs/commandstrings/SLXD
#   SAMPLE x ALL audPeak audRms rfRssi -- 3 fields, each 0-120 raw; actual
#     value = raw - 120 (dBFS for audio, dBm for RF). Shown here as raw/120
#     scaled to a 0-100 "signal strength" percent rather than converting to
#     the negative dB value, to match how the rest of this app displays rf/audio.
#   battery: TX_BATT_BARS (0-5 bars, 255=unknown) -- there is no percent-
#     based battery parameter in this protocol, unlike ULX-D's BATT_CHARGE.
#   mute: absent from the entire published command set -- SLX-D has no
#     remote mute over this protocol, confirmed by its absence from all 13
#     pages of the spec (contrast with ULX-D/Axient Digital's AUDIO_MUTE).
class SLXDProvider(BaseProvider):
    def __init__(self, ip, device_type, photo=None, channel=1):
        super().__init__(ip, device_type, photo)
        self.channel = channel
        self.sock = None
        self.model = None
        self._buffer = ''
        self._metering_started = False
        self._miss_count = 0

    def connect(self):
        with self._io_lock:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(0.5)
                self.sock.connect((self.ip, 2202))
                self.status = 'CONNECTED'
                self._metering_started = False
                self._buffer = ''
                self._miss_count = 0
            except Exception:
                self.status = 'DISCONNECTED'

    def _mark_unreachable(self):
        self.status = 'DISCONNECTED'
        self.metrics = {}
        self._metering_started = False
        self._miss_count = 0

    def disconnect(self):
        with self._io_lock:
            if self.sock:
                try: self.sock.close()
                except Exception: pass
            self.status = 'DISCONNECTED'

    def _send(self, message):
        self.sock.sendall(message.encode('ascii'))

    def _read_messages(self, timeout=0.4, stop_early=True):
        deadline = time.time() + timeout
        self.sock.settimeout(0.1)
        got_any = False
        while time.time() < deadline:
            try:
                chunk = self.sock.recv(4096)
                if not chunk:
                    break
                self._buffer += chunk.decode('ascii', errors='ignore')
                got_any = True
            except socket.timeout:
                if stop_early and got_any:
                    break
        messages = []
        while True:
            start = self._buffer.find('<')
            end = self._buffer.find('>', start)
            if start == -1 or end == -1:
                break
            messages.append(self._buffer[start + 1:end].strip())
            self._buffer = self._buffer[end + 1:]
        return messages

    def _apply_report(self, param, value):
        value = value.strip()
        if param == 'TX_BATT_BARS':
            try:
                bars = int(value)
                self.metrics['batt'] = None if bars >= 255 else min(100, bars * 20)
            except ValueError: pass
        elif param == 'FREQUENCY':
            try: self.metrics['frequency_mhz'] = int(value) / 1000
            except ValueError: pass
        elif param == 'MODEL':
            self.model = value.strip('{}').strip()

    def _parse_messages(self, messages):
        for msg in messages:
            parts = msg.split()
            if not parts:
                continue
            if parts[0] == 'SAMPLE' and len(parts) >= 6 and parts[2] == 'ALL':
                chan = parts[1]
                if chan not in ('0', str(self.channel)):
                    continue
                try:
                    self.metrics['audio'] = max(0, min(100, round(int(parts[3]) / 120 * 100)))
                    self.metrics['rf'] = max(0, min(100, round(int(parts[5]) / 120 * 100)))
                except (ValueError, IndexError):
                    pass
                continue
            if parts[0] != 'REP' or len(parts) < 2:
                continue
            rest = parts[1:]
            if rest[0].isdigit():
                if len(rest) < 3 or rest[0] not in ('0', str(self.channel)):
                    continue
                self._apply_report(rest[1], ' '.join(rest[2:]))
            else:
                self._apply_report(rest[0], ' '.join(rest[1:]))

    def poll(self):
        if self.status != 'CONNECTED': return
        with self._io_lock:
            try:
                if not self._metering_started:
                    self._send(f'< SET {self.channel} METER_RATE 01000 >')
                    self._metering_started = True
                self._send(f'< GET {self.channel} TX_BATT_BARS >')
                self._send(f'< GET {self.channel} FREQUENCY >')
                messages = self._read_messages()
                self._parse_messages(messages)
                if not messages:
                    self._miss_count += 1
                    if self._miss_count >= NETWORK_MISS_LIMIT:
                        self._mark_unreachable()
                        return
                else:
                    self._miss_count = 0

                batt = self.metrics.get('batt')
                if batt is not None and batt < 20:
                    alert = f"Low Battery: {batt}%"
                    if not self.alerts or self.alerts[-1] != alert:
                        self.alerts.append(alert)
                        self.alerts = self.alerts[-20:]
            except (OSError, socket.error):
                self._mark_unreachable()

    def scan_rf(self):
        self.spectrum_data = []

    def send_command(self, cmd_type, value, channel=1):
        if cmd_type == 'MUTE':
            return False  # not supported: no mute parameter in SLX-D's command strings
        if self.status != 'CONNECTED': return False
        with self._io_lock:
            try:
                if cmd_type == 'FREQUENCY':
                    khz = int(round(float(value) * 1000))
                    self._send(f'< SET {channel} FREQUENCY {khz:06d} >')
                    self._parse_messages(self._read_messages(timeout=CONFIRM_READ_TIMEOUT, stop_early=False))
                    return self.metrics.get('frequency_mhz') == round(khz / 1000, 4)
                else:
                    self._send(f'< SET {channel} {cmd_type} {value} >')
                    return True
            except (OSError, socket.error, ValueError):
                return False

# Shure Axient Digital "Command Strings" protocol -- same TCP 2202 bracket
# syntax and AUDIO_MUTE/FREQUENCY parameters as ULX-D, but a richer SAMPLE
# format and a direct-percent battery parameter. NOT hardware-verified (no
# Axient Digital unit available to test against). Built from Shure's
# published spec (Preliminary, May 2018):
# https://content-files.shure.com/Pubs/AD4D/Axient_Digital_network_string_commands.pdf
#   SAMPLE chNum ALL qual audBitmap audPeak audRms rfAntStats rfBitmapA
#     rfRssiA rfBitmapB rfRssiB -- this is the "standard channel" layout
#     (Quadversity=OFF, FD=OFF/FD-S); Quadversity and FD-C channels report
#     additional antenna fields this doesn't attempt to parse. audPeak/
#     rfRssiA are 0-120 raw, actual dBFS/dBm = raw-120 (same convention as
#     SLX-D above).
#   battery: TX_BATT_CHARGE_PERCENT (0-100 direct percent, 255=unknown).
class AxientDigitalProvider(BaseProvider):
    def __init__(self, ip, device_type, photo=None, channel=1):
        super().__init__(ip, device_type, photo)
        self.channel = channel
        self.sock = None
        self.model = None
        self._buffer = ''
        self._metering_started = False
        self._miss_count = 0

    def connect(self):
        with self._io_lock:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(0.5)
                self.sock.connect((self.ip, 2202))
                self.status = 'CONNECTED'
                self._metering_started = False
                self._buffer = ''
                self._miss_count = 0
            except Exception:
                self.status = 'DISCONNECTED'

    def _mark_unreachable(self):
        self.status = 'DISCONNECTED'
        self.metrics = {}
        self._metering_started = False
        self._miss_count = 0

    def disconnect(self):
        with self._io_lock:
            if self.sock:
                try: self.sock.close()
                except Exception: pass
            self.status = 'DISCONNECTED'

    def _send(self, message):
        self.sock.sendall(message.encode('ascii'))

    def _read_messages(self, timeout=0.4, stop_early=True):
        deadline = time.time() + timeout
        self.sock.settimeout(0.1)
        got_any = False
        while time.time() < deadline:
            try:
                chunk = self.sock.recv(4096)
                if not chunk:
                    break
                self._buffer += chunk.decode('ascii', errors='ignore')
                got_any = True
            except socket.timeout:
                if stop_early and got_any:
                    break
        messages = []
        while True:
            start = self._buffer.find('<')
            end = self._buffer.find('>', start)
            if start == -1 or end == -1:
                break
            messages.append(self._buffer[start + 1:end].strip())
            self._buffer = self._buffer[end + 1:]
        return messages

    def _apply_report(self, param, value):
        value = value.strip()
        if param == 'TX_BATT_CHARGE_PERCENT':
            try:
                parsed = int(value)
                self.metrics['batt'] = None if parsed >= 255 else parsed
            except ValueError: pass
        elif param == 'FREQUENCY':
            try: self.metrics['frequency_mhz'] = int(value) / 1000
            except ValueError: pass
        elif param == 'MODEL':
            self.model = value.strip('{}').strip()
        elif param == 'AUDIO_MUTE':
            self.metrics['muted'] = (value == 'ON')

    def _parse_messages(self, messages):
        for msg in messages:
            parts = msg.split()
            if not parts:
                continue
            if parts[0] == 'SAMPLE' and len(parts) >= 12 and parts[2] == 'ALL':
                chan = parts[1]
                if chan not in ('0', str(self.channel)):
                    continue
                try:
                    self.metrics['audio'] = max(0, min(100, round(int(parts[5]) / 120 * 100)))
                    self.metrics['rf'] = max(0, min(100, round(int(parts[9]) / 120 * 100)))
                except (ValueError, IndexError):
                    pass
                continue
            if parts[0] != 'REP' or len(parts) < 2:
                continue
            rest = parts[1:]
            if rest[0].isdigit():
                if len(rest) < 3 or rest[0] not in ('0', str(self.channel)):
                    continue
                self._apply_report(rest[1], ' '.join(rest[2:]))
            else:
                self._apply_report(rest[0], ' '.join(rest[1:]))

    def poll(self):
        if self.status != 'CONNECTED': return
        with self._io_lock:
            try:
                if not self._metering_started:
                    self._send(f'< SET {self.channel} METER_RATE 01000 >')
                    self._metering_started = True
                self._send(f'< GET {self.channel} TX_BATT_CHARGE_PERCENT >')
                self._send(f'< GET {self.channel} FREQUENCY >')
                messages = self._read_messages()
                self._parse_messages(messages)
                if not messages:
                    self._miss_count += 1
                    if self._miss_count >= NETWORK_MISS_LIMIT:
                        self._mark_unreachable()
                        return
                else:
                    self._miss_count = 0

                batt = self.metrics.get('batt')
                if batt is not None and batt < 20:
                    alert = f"Low Battery: {batt}%"
                    if not self.alerts or self.alerts[-1] != alert:
                        self.alerts.append(alert)
                        self.alerts = self.alerts[-20:]
            except (OSError, socket.error):
                self._mark_unreachable()

    def scan_rf(self):
        self.spectrum_data = []

    def send_command(self, cmd_type, value, channel=1):
        if self.status != 'CONNECTED': return False
        with self._io_lock:
            try:
                if cmd_type == 'MUTE':
                    self._send(f'< SET {channel} AUDIO_MUTE {"ON" if value else "OFF"} >')
                    self._parse_messages(self._read_messages(timeout=CONFIRM_READ_TIMEOUT, stop_early=False))
                    return self.metrics.get('muted') == bool(value)
                elif cmd_type == 'FREQUENCY':
                    khz = int(round(float(value) * 1000))
                    self._send(f'< SET {channel} FREQUENCY {khz:06d} >')
                    self._parse_messages(self._read_messages(timeout=CONFIRM_READ_TIMEOUT, stop_early=False))
                    return self.metrics.get('frequency_mhz') == round(khz / 1000, 4)
                else:
                    self._send(f'< SET {channel} {cmd_type} {value} >')
                    return True
            except (OSError, socket.error, ValueError):
                return False

# Shure Microflex Wireless (MXW) conferencing system -- same TCP 2202
# bracket syntax as ULX-D, but a materially different command set. NOT
# hardware-verified (no MXW unit available to test against). Built from
# Shure's published spec: https://www.shure.com/en-US/docs/commandstrings/MXW
#   The network device is the MXWAPT access point (2/4/8 channel); "channel"
#   here addresses one of its linked mic slots (1-8), matching this app's
#   existing one-IP-with-channel model used for multi-channel receivers.
#   SAMPLE x aaa eee -- no "ALL" token (unlike ULX-D/SLX-D/AD), and the spec
#     doesn't publish a dB conversion for aaa/eee the way the other Shure
#     lines do, so these are shown as raw 0-100-ish values rather than a
#     documented dB scale.
#   battery: BATT_CHARGE (0-100 percent direct, 255=device off).
#   mute: no AUDIO_MUTE -- mute is one state of the TX_STATUS enum
#     (ACTIVE/MUTE/STANDBY/ON_CHARGER/UNKNOWN); SET TX_STATUS MUTE/ACTIVE.
#   frequency: no FREQUENCY command exists anywhere in the spec -- MXW hops
#     automatically in the 2.4GHz band and isn't user-tunable to a carrier.
class MXWProvider(BaseProvider):
    def __init__(self, ip, device_type, photo=None, channel=1):
        super().__init__(ip, device_type, photo)
        self.channel = channel
        self.sock = None
        self.model = None
        self._buffer = ''
        self._metering_started = False
        self._miss_count = 0

    def connect(self):
        with self._io_lock:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(0.5)
                self.sock.connect((self.ip, 2202))
                self.status = 'CONNECTED'
                self._metering_started = False
                self._buffer = ''
                self._miss_count = 0
            except Exception:
                self.status = 'DISCONNECTED'

    def _mark_unreachable(self):
        self.status = 'DISCONNECTED'
        self.metrics = {}
        self._metering_started = False
        self._miss_count = 0

    def disconnect(self):
        with self._io_lock:
            if self.sock:
                try: self.sock.close()
                except Exception: pass
            self.status = 'DISCONNECTED'

    def _send(self, message):
        self.sock.sendall(message.encode('ascii'))

    def _read_messages(self, timeout=0.4, stop_early=True):
        deadline = time.time() + timeout
        self.sock.settimeout(0.1)
        got_any = False
        while time.time() < deadline:
            try:
                chunk = self.sock.recv(4096)
                if not chunk:
                    break
                self._buffer += chunk.decode('ascii', errors='ignore')
                got_any = True
            except socket.timeout:
                if stop_early and got_any:
                    break
        messages = []
        while True:
            start = self._buffer.find('<')
            end = self._buffer.find('>', start)
            if start == -1 or end == -1:
                break
            messages.append(self._buffer[start + 1:end].strip())
            self._buffer = self._buffer[end + 1:]
        return messages

    def _apply_report(self, param, value):
        value = value.strip()
        if param == 'BATT_CHARGE':
            try:
                parsed = int(value)
                self.metrics['batt'] = None if parsed >= 255 else parsed
            except ValueError: pass
        elif param == 'TX_STATUS':
            self.metrics['muted'] = (value == 'MUTE')
        elif param == 'TX_TYPE':
            self.model = value.strip()

    def _parse_messages(self, messages):
        for msg in messages:
            parts = msg.split()
            if not parts:
                continue
            if parts[0] == 'SAMPLE' and len(parts) >= 4 and parts[1] != 'SEC':
                chan = parts[1]
                if chan not in ('0', str(self.channel)):
                    continue
                try:
                    self.metrics['rf'] = max(0, min(100, int(parts[2])))
                    self.metrics['audio'] = max(0, min(100, int(parts[3])))
                except (ValueError, IndexError):
                    pass
                continue
            if parts[0] != 'REP' or len(parts) < 2:
                continue
            rest = parts[1:]
            if rest[0] == 'SEC':
                continue  # secondary-mic reports not modeled by this app's one-channel-per-IP data model
            if rest[0].isdigit():
                if len(rest) < 3 or rest[0] not in ('0', str(self.channel)):
                    continue
                self._apply_report(rest[1], ' '.join(rest[2:]))
            else:
                self._apply_report(rest[0], ' '.join(rest[1:]))

    def poll(self):
        if self.status != 'CONNECTED': return
        with self._io_lock:
            try:
                if not self._metering_started:
                    self._send(f'< SET {self.channel} METER_RATE 01000 >')
                    self._metering_started = True
                self._send(f'< GET {self.channel} BATT_CHARGE >')
                self._send(f'< GET {self.channel} TX_STATUS >')
                messages = self._read_messages()
                self._parse_messages(messages)
                if not messages:
                    self._miss_count += 1
                    if self._miss_count >= NETWORK_MISS_LIMIT:
                        self._mark_unreachable()
                        return
                else:
                    self._miss_count = 0

                batt = self.metrics.get('batt')
                if batt is not None and batt < 20:
                    alert = f"Low Battery: {batt}%"
                    if not self.alerts or self.alerts[-1] != alert:
                        self.alerts.append(alert)
                        self.alerts = self.alerts[-20:]
            except (OSError, socket.error):
                self._mark_unreachable()

    def scan_rf(self):
        self.spectrum_data = []

    def send_command(self, cmd_type, value, channel=1):
        if cmd_type == 'FREQUENCY':
            return False  # not supported: MXW hops automatically, no tunable carrier
        if self.status != 'CONNECTED': return False
        with self._io_lock:
            try:
                if cmd_type == 'MUTE':
                    self._send(f'< SET {channel} TX_STATUS {"MUTE" if value else "ACTIVE"} >')
                    self._parse_messages(self._read_messages(timeout=CONFIRM_READ_TIMEOUT, stop_early=False))
                    return self.metrics.get('muted') == bool(value)
                else:
                    self._send(f'< SET {channel} {cmd_type} {value} >')
                    return True
            except (OSError, socket.error, ValueError):
                return False

class NoNetworkProvider(BaseProvider):
    """Stub for models confirmed to have no Ethernet/IP control at all (e.g.
    Shure GLX-D/BLX/PSM300/PSM900; Sennheiser XSW-D/XSW IEM/AVX) -- rather
    than silently faking metrics for hardware that structurally can't be
    network-monitored, this stays DISCONNECTED and explains why via an
    alert instead of pretending to speak a protocol these devices don't have."""
    def connect(self):
        self.status = 'DISCONNECTED'
        self.alerts = ['No network control for this model -- monitor/control it directly on the hardware.']
    def disconnect(self):
        self.status = 'DISCONNECTED'
    def poll(self):
        pass
    def scan_rf(self):
        self.spectrum_data = []
    def send_command(self, cmd_type, value, channel=1):
        return False

class SennheiserProvider(BaseProvider):
    def __init__(self, ip, device_type, photo=None):
        super().__init__(ip, device_type, photo)

    def connect(self): self.status = 'CONNECTED'
    def disconnect(self): self.status = 'DISCONNECTED'
    def poll(self):
        self.metrics = {'audio': 15, 'rf': 75, 'batt': 90}
    def scan_rf(self):
        self.spectrum_data = [ (f, 40 + (i%15)) for i, f in enumerate(range(500, 600)) ]
    def send_command(self, cmd_type, value, channel=1):
        return True

# Sennheiser Sound Control Protocol (SSC / SSCv1) -- JSON-over-socket, used
# by Sennheiser's current networked digital wireless lines (EW-DX, EW-D,
# Digital 9000/6000, Spectera). NOT hardware-verified (no Sennheiser unit
# available to test against). Built from Sennheiser's own published specs:
#   Wire protocol (transport, framing, GET/SET conventions):
#     "SSC Developer's Guide for TeamConnect Ceiling 2" (TI_1245), and
#     https://docs.cloud.sennheiser.com/en-us/control-cockpit/control-cockpit/ssc-protocols.html
#     - TCP or UDP, default port 45.
#     - Each message is a single JSON object; over TCP, messages are
#       terminated by CRLF or LFLF (an unescaped newline can't otherwise
#       appear in valid JSON).
#     - A GET is a SET with a `null` value at that address, e.g.
#       {"rx1":{"mute":null}} -> device replies with the real value filled in.
#   Device-specific addresses (channel name/mute/frequency, RF/audio meters,
#   transmitter battery): "SSC Developer's Guide for EW-DX" (03/2023), the
#   one Shure-comparable family this was actually checked against in detail:
#     /rx{n}/mute (bool), /rx{n}/frequency (kHz, settable),
#     /m/rx{n}/rsqi (0-100%), /m/rx{n}/af (-138.5..0 dBFS),
#     /mates/tx{n}/battery/gauge (0-100%), /device/identity/product.
#   Digital 9000/6000 and Spectera are assumed (NOT individually verified)
#   to share this same general SSC addressing scheme, since Sennheiser's own
#   docs describe it as one common protocol across their SSC-capable line --
#   only EW-DX's address paths were confirmed against a published spec here.
SSC_PORT = 45

def _ssc_extract(msg, *path):
    node = msg
    for key in path:
        if not isinstance(node, dict) or key not in node:
            return None
        node = node[key]
    return node

class SennheiserSSCProvider(BaseProvider):
    def __init__(self, ip, device_type, photo=None, channel=1):
        super().__init__(ip, device_type, photo)
        self.channel = channel
        self.sock = None
        self.model = None
        self._buffer = ''
        self._miss_count = 0

    def _rx(self):
        return f'rx{self.channel}'

    def _tx(self):
        return f'tx{self.channel}'

    def _mark_unreachable(self):
        self.status = 'DISCONNECTED'
        self.metrics = {}
        self._miss_count = 0

    def connect(self):
        with self._io_lock:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(0.5)
                self.sock.connect((self.ip, SSC_PORT))
                self.status = 'CONNECTED'
                self._buffer = ''
                self._miss_count = 0
                # Pull the real model string once, for display (identify_device()'s
                # equivalent of ShureProvider's MODEL/DEVICE_ID parsing).
                self._send({'device': {'identity': {'product': None}}})
                for msg in self._read_messages(timeout=0.5, stop_early=False):
                    product = _ssc_extract(msg, 'device', 'identity', 'product')
                    if isinstance(product, str) and product.strip():
                        self.model = product.strip()
            except Exception:
                self.status = 'DISCONNECTED'

    def disconnect(self):
        with self._io_lock:
            if self.sock:
                try: self.sock.close()
                except Exception: pass
            self.status = 'DISCONNECTED'

    def _send(self, obj):
        self.sock.sendall((json.dumps(obj) + '\r\n').encode('utf-8'))

    def _read_messages(self, timeout=0.4, stop_early=True):
        """Reads whatever arrives within `timeout` and splits the buffer on
        SSC's line-based message separator (CRLF or LFLF -- splitting on any
        bare '\\n' covers both, since valid SSC JSON can't contain one)."""
        deadline = time.time() + timeout
        self.sock.settimeout(0.1)
        got_any = False
        while time.time() < deadline:
            try:
                chunk = self.sock.recv(4096)
                if not chunk:
                    break
                self._buffer += chunk.decode('utf-8', errors='ignore')
                got_any = True
            except socket.timeout:
                if stop_early and got_any:
                    break
        messages = []
        while True:
            idx = self._buffer.find('\n')
            if idx == -1:
                break
            line = self._buffer[:idx].strip('\r\n ')
            self._buffer = self._buffer[idx + 1:]
            if not line:
                continue
            try:
                messages.append(json.loads(line))
            except ValueError:
                continue
        return messages

    def _apply_message(self, msg):
        rx, tx = self._rx(), self._tx()
        mute = _ssc_extract(msg, rx, 'mute')
        if isinstance(mute, bool):
            self.metrics['muted'] = mute
        freq_khz = _ssc_extract(msg, rx, 'frequency')
        if isinstance(freq_khz, (int, float)):
            self.metrics['frequency_mhz'] = round(freq_khz / 1000, 4)
        rsqi = _ssc_extract(msg, 'm', rx, 'rsqi')
        if isinstance(rsqi, (int, float)):
            self.metrics['rf'] = max(0, min(100, round(rsqi)))
        af = _ssc_extract(msg, 'm', rx, 'af')
        if isinstance(af, (int, float)):
            # -138.5..0 dBFS -> 0-100, same distance-from-floor convention
            # used for the Shure lines above.
            self.metrics['audio'] = max(0, min(100, round((af + 138.5) / 138.5 * 100)))
        gauge = _ssc_extract(msg, 'mates', tx, 'battery', 'gauge')
        if isinstance(gauge, (int, float)):
            self.metrics['batt'] = round(gauge)

    def poll(self):
        if self.status != 'CONNECTED': return
        with self._io_lock:
            try:
                rx, tx = self._rx(), self._tx()
                query = {
                    rx: {'mute': None, 'frequency': None},
                    'm': {rx: {'rsqi': None, 'af': None}},
                    'mates': {tx: {'battery': {'gauge': None}}},
                }
                self._send(query)
                messages = self._read_messages()
                for msg in messages:
                    self._apply_message(msg)
                if not messages:
                    self._miss_count += 1
                    if self._miss_count >= NETWORK_MISS_LIMIT:
                        self._mark_unreachable()
                        return
                else:
                    self._miss_count = 0

                batt = self.metrics.get('batt')
                if batt is not None and batt < 20:
                    alert = f"Low Battery: {batt}%"
                    if not self.alerts or self.alerts[-1] != alert:
                        self.alerts.append(alert)
                        self.alerts = self.alerts[-20:]
            except (OSError, socket.error):
                self._mark_unreachable()

    def scan_rf(self):
        # SSC exposes carrier metering per-channel, not a wideband scan --
        # left empty rather than fabricating scan-shaped data, same as the
        # Shure providers above.
        self.spectrum_data = []

    def send_command(self, cmd_type, value, channel=1):
        if self.status != 'CONNECTED': return False
        with self._io_lock:
            try:
                rx = f'rx{channel}'
                if cmd_type == 'MUTE':
                    self._send({rx: {'mute': bool(value)}})
                    for msg in self._read_messages(timeout=CONFIRM_READ_TIMEOUT, stop_early=False):
                        self._apply_message(msg)
                    return self.metrics.get('muted') == bool(value)
                elif cmd_type == 'FREQUENCY':
                    khz = int(round(float(value) * 1000))
                    self._send({rx: {'frequency': khz}})
                    for msg in self._read_messages(timeout=CONFIRM_READ_TIMEOUT, stop_early=False):
                        self._apply_message(msg)
                    return self.metrics.get('frequency_mhz') == round(khz / 1000, 4)
                else:
                    return False
            except (OSError, socket.error, ValueError):
                return False
