
import time
import json
import math
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
# METER_RATE is set to 250ms (Shure's documented floor is 100ms) on first
# connect -- fast enough for the audio meter to read as live rather than
# stepping once a second, without flooding the network at every device's
# absolute minimum rate.

# Confirming a SET needs a longer read window than a routine poll: a device
# with metering already running (poll() enables it on first connect) is
# also pushing an unsolicited SAMPLE every 250ms, so there can be real
# backlog to drain through before the actual confirmation shows up.
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

# Published frequency-range-by-band charts. RF_BAND is a queryable hardware
# parameter on SLX-D and Axient Digital (confirmed against Shure's own
# command-string specs), so those providers query it live and look up the
# actual MHz range below -- no guessing involved for those two lines.
# ULX-D/QLX-D's spec was checked directly and does NOT include RF_BAND (it
# was added to the command-string protocol later, for SLX-D/AD only), and
# UHF-R's protocol wasn't confirmed to expose a band query either -- for
# those, and for Sennheiser's mocked (non-SSC) line, there is no live way to
# learn a specific unit's tunable range over the network, so this app can't
# claim to know it; range enforcement for them instead relies on the
# hardware's own explicit "REP ERR" rejection of an out-of-range SET (see
# each provider's _apply_report handling of the 'ERR' parameter).
# Sennheiser SSC devices (EW-DX/EW-D/etc.) are the most precise case: they
# report their exact tunable range(s) directly via /device/frequency_ranges,
# queried live in SennheiserSSCProvider -- no static table needed there.
# Sources: manufacturer product-page frequency-range listings for each band,
# cross-checked across multiple independent retailers (not the official PDF
# band chart, whose bar-graph layout doesn't survive text extraction
# reliably enough to trust for a safety check like this one).
SLXD_BAND_RANGES = {
    'G58': [(470.0, 514.0)],
    'H55': [(514.0, 558.0)],
    'J52': [(558.0, 602.0), (614.0, 616.0)],
}
AXIENT_DIGITAL_BAND_RANGES = {
    'G57': [(470.0, 616.0)],
}

def freq_in_ranges(freq_mhz, ranges):
    """True if freq_mhz falls in any (lo, hi) pair in ranges, or if ranges
    is falsy (nothing known to enforce -- callers should treat that as
    "can't verify" rather than "confirmed valid")."""
    if not ranges:
        return True
    return any(lo <= freq_mhz <= hi for lo, hi in ranges)

class BaseProvider(ABC):
    def __init__(self, ip, device_type, photo=None):
        self.ip = ip
        self.type = device_type
        self.photo = photo # Path to the photo
        self.status = 'DISCONNECTED'
        self.metrics = {}
        self.spectrum_data = []
        self.alerts = []
        # Populated (when the protocol supports it) by identifying the RF
        # band/range live from the device itself -- see the module comment
        # above. None means "unknown", not "unrestricted".
        self.rf_band = None
        self.rf_range_mhz = None
        # Human-readable reason the last send_command('FREQUENCY', ...) call
        # failed, e.g. because the hardware sent back a REP ERR. Read by
        # FrequencyHandler right after a failed call; not persisted.
        self.last_command_error = None
        # 'receiver' (picks up RF from a body-worn mic/pack) or 'transmitter'
        # (an IEM base station sending audio out to a body-worn receiver
        # pack) -- set by omniwave.py's make_provider from the model type.
        self.role = 'receiver'
        # Set by a provider whose protocol structurally cannot report a
        # frequency at all (e.g. PSM1000's one-way push protocol has no
        # query channel) -- distinct from simply not having one assigned
        # yet. None means a real frequency is knowable, just not present.
        self.freq_unavailable_reason = None
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
            'rf_band': self.rf_band,
            'rf_range_mhz': self.rf_range_mhz,
            'role': self.role,
            'freq_unavailable_reason': self.freq_unavailable_reason,
            'dante_status': getattr(self, 'dante_status', None),
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
        self._last_error = False
        # See BaseProvider.get_json() -- None means "not yet queried",
        # populated once at connect() below.
        self.dante_status = None

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
                self._query_dante_status()
            except Exception:
                self.status = 'DISCONNECTED'

    def _query_dante_status(self):
        """NA_DEVICE_NAME is Shure's own confirmed command string --
        documented as "Discovers the Dante device name on dual and quad
        devices" -- the same query used earlier this session to confirm
        live Dante hardware presence on a real ULXD4Q (it returned the
        unit's actual Dante name). Device-scoped, not per-channel, so this
        runs once at connect() rather than every poll cycle. A model
        without Dante hardware isn't documented to return anything
        specific here, so an empty/absent reply is treated as "no Dante"
        rather than guessed at."""
        try:
            self._send('< GET NA_DEVICE_NAME >')
            self._parse_messages(self._read_messages(timeout=0.5, stop_early=False))
        except (OSError, socket.error):
            pass

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
        elif param == 'BATT_RUN_TIME':
            # Minutes until the transmitter turns itself off -- confirmed in
            # ULX-D's own spec. 65535 = off or using AA (non-rechargeable)
            # batteries, where no runtime estimate is possible.
            try:
                parsed = int(value)
                self.metrics['batt_minutes'] = None if parsed >= 65535 else parsed
            except ValueError: pass
        elif param == 'FREQUENCY':
            try: self.metrics['frequency_mhz'] = int(value) / 1000
            except ValueError: pass
        elif param == 'MODEL':
            self.model = value.strip('{}').strip()
        elif param == 'NA_DEVICE_NAME':
            name = value.strip('{}').strip()
            self.dante_status = {
                'interface_present': bool(name), 'interfaces': None, 'auto': None,
                'ip': None, 'device_name': name or None,
            }
        elif param == 'AUDIO_MUTE':
            self.metrics['muted'] = (value == 'ON')
        elif param == 'RF_INT_DET' and value == 'CRITICAL':
            alert = 'RF Interference Detected'
            if not self.alerts or self.alerts[-1] != alert:
                self.alerts.append(alert)
                self.alerts = self.alerts[-20:]
        elif param == 'ERR':
            # The device's own explicit rejection of the last SET (e.g. a
            # frequency outside its tunable range) -- confirmed for ULX-D:
            # "REP ERR occurs when a command is improperly formatted or
            # when the values are out of range."
            self._last_error = True

    def _parse_messages(self, messages):
        for msg in messages:
            parts = msg.split()
            if not parts:
                continue
            if parts[0] == 'SAMPLE' and len(parts) >= 6:
                # SAMPLE x ALL nn aaa eee -- nn=antenna LEDs, aaa=RF (000-115,
                # subtract 128 for dBm), eee=audio level (000-050, per
                # Shure's own ULX-D command-strings spec). Rescaled to 0-100
                # here -- it used to be stored as the raw 0-50 value
                # directly, which the UI's 0-100% bar renders as never
                # filling past halfway even at genuinely loud input.
                chan = parts[1]
                if chan not in ('0', str(self.channel)):
                    continue
                try:
                    self.metrics['rf'] = int(parts[4]) - 128
                    self.metrics['audio'] = max(0, min(100, round(int(parts[5]) / 50 * 100)))
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
                    self._send(f'< SET {self.channel} METER_RATE 00250 >')
                    self._metering_started = True
                self._send(f'< GET {self.channel} BATT_CHARGE >')
                self._send(f'< GET {self.channel} BATT_RUN_TIME >')
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
        self.last_command_error = None
        with self._io_lock:
            try:
                if cmd_type == 'MUTE':
                    self._last_error = False
                    self._send(f'< SET {channel} AUDIO_MUTE {"ON" if value else "OFF"} >')
                    self._parse_messages(self._read_messages(timeout=CONFIRM_READ_TIMEOUT, stop_early=False))
                    return self.metrics.get('muted') == bool(value)
                elif cmd_type == 'FREQUENCY':
                    self._last_error = False
                    khz = int(round(float(value) * 1000))
                    self._send(f'< SET {channel} FREQUENCY {khz:06d} >')
                    self._parse_messages(self._read_messages(timeout=CONFIRM_READ_TIMEOUT, stop_early=False))
                    if self._last_error:
                        self.last_command_error = "Device rejected the frequency (REP ERR) -- likely outside this unit's tunable range."
                        return False
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
# 1.2s (the old value, 40) is too coarse for a live meter -- confirmed live:
# polling the running app every second for 15s straight while someone was
# actually talking into a UHF-R mic showed audio=0 the entire time, even
# though a direct 150ms-rate capture over the same kind of window caught
# real (if quiet/intermittent) activity. Between SAMPLE pushes the
# dashboard just holds the last value it got, so a 1.2s gap means it's
# easy to never land on a moment with real signal. 10 steps = 300ms keeps
# this well within Shure's own documented "metering and updating" mode
# (anything under 12s), just responsive enough to actually track speech.
UHFR_METER_STEPS = 10  # 10 * 30ms = 300ms update interval
UHFR_MISS_LIMIT = 5    # consecutive empty polls before considering it unreachable

# eee's 0-255 range is Shure's own documented full-scale (confirmed against
# the official PDF above, not guessed) -- but live-verified against a real
# UHF-R during actual singing: normal vocal level peaks around raw 20-30,
# only ~8-12% on a flat linear 0-255-to-100% mapping. That's accurate data
# (the raw capture clearly shows a real rising-and-falling envelope in sync
# with the singing), but a linear bar makes completely normal input look
# like it's barely registering, when a real meter -- including this
# receiver's own front-panel LEDs -- is logarithmic, like every audio
# meter, to give useful visual resolution to typical speech/singing levels
# rather than only the rarely-hit top few dB near clipping. Unlike
# PSM1000's AUDIO_IN_LVL (genuinely undocumented, open-ended), 255 here is
# a real documented full-scale reference, so this converts to dBFS against
# it properly rather than guessing a curve.
UHFR_AUDIO_DB_FLOOR = -40  # dBFS; at/below this reads 0%

def _uhfr_audio_pct(raw):
    if raw <= 0:
        return 0
    dbfs = 20 * math.log10(raw / 255)
    pct = (dbfs - UHFR_AUDIO_DB_FLOOR) / (0 - UHFR_AUDIO_DB_FLOOR) * 100
    return max(0, min(100, round(pct)))

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
                self.metrics['audio'] = _uhfr_audio_pct(audio_raw)
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
# Live measurement against real hardware (8 units, actual speech vs. quiet
# background): raw AUDIO_IN_LVL values ranged from ~17,000 up to
# ~3,548,184 -- a >150x spread within one ~20s capture. That rules out any
# fixed linear ceiling: the old PSM1000_ASSUMED_MAX=1023 guess this
# replaces was ~3,000x too low, which is why this metric always read a
# pinned 100% regardless of actual level (every real reading clamped
# straight to the assumed max). No single linear scale can represent that
# much dynamic range sensibly, so this uses logarithmic (dB-style)
# scaling instead -- the standard way real audio meters handle wide
# dynamic range. Floor/ceiling below are picked with headroom around the
# observed range, NOT calibrated against Shure's own internal formula:
# that's undocumented, and Wireless Workbench's own live display for this
# is an 8-segment LED ladder, not a number, so there's no exact reference
# to solve against. This is an honest *relative* meter -- quiet reads
# low, loud reads high, and it will never falsely peg at 100% for
# ordinary speech the way the fixed ceiling did -- not a precise match to
# WWB's internal calibration.
PSM1000_AUDIO_LOG_FLOOR = 4.0    # log10(10,000) -- at/below this reads 0%
PSM1000_AUDIO_LOG_CEILING = 6.7  # log10(~5,000,000) -- at/above this reads 100%

def _psm1000_audio_pct(raw):
    if raw <= 0:
        return 0
    pct = (math.log10(raw) - PSM1000_AUDIO_LOG_FLOOR) / (PSM1000_AUDIO_LOG_CEILING - PSM1000_AUDIO_LOG_FLOOR) * 100
    return max(0, min(100, round(pct)))

class PSM1000Provider(BaseProvider):
    def __init__(self, ip, device_type, photo=None, channel=1):
        super().__init__(ip, device_type, photo)
        self.channel = channel
        self.sock = None
        self._buffer = ''
        self._miss_count = 0
        # Genuinely unknowable over this protocol, not just "not assigned
        # yet": the P10T is a one-way push transmitter with no GET/SET
        # channel at all (see the module comment above), so there's no way
        # to ask it what frequency it's tuned to -- even though the real
        # hardware obviously is tuned to one, since RF is actually flowing.
        self.freq_unavailable_reason = "Not reported by this transmitter (one-way protocol, no query channel)"

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
                    self.metrics['audio'] = _psm1000_audio_pct(max(readings))

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
        self._last_error = False

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
                self._query_rf_band()
            except Exception:
                self.status = 'DISCONNECTED'

    def _query_rf_band(self):
        """Real hardware discovery of this specific unit's RF band, so
        FrequencyHandler can refuse a deploy outside its actual tunable
        range instead of finding out only after the hardware rejects it."""
        try:
            self._send('< GET RF_BAND >')
            for msg in self._read_messages(timeout=0.5, stop_early=False):
                parts = msg.split()
                if len(parts) >= 3 and parts[0] == 'REP' and parts[1] == 'RF_BAND':
                    band = ' '.join(parts[2:]).strip('{}').strip()
                    if band:
                        self.rf_band = band
                        self.rf_range_mhz = SLXD_BAND_RANGES.get(band)
        except (OSError, socket.error):
            pass

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
        elif param == 'TX_BATT_MINS':
            # Confirmed in SLX-D's own spec: 0-65532 = minutes of runtime;
            # 65533 = battery comm warning, 65534 = still calculating,
            # 65535 = unknown/not applicable. None covers all three.
            try:
                parsed = int(value)
                self.metrics['batt_minutes'] = parsed if parsed <= 65532 else None
            except ValueError: pass
        elif param == 'FREQUENCY':
            try: self.metrics['frequency_mhz'] = int(value) / 1000
            except ValueError: pass
        elif param == 'MODEL':
            self.model = value.strip('{}').strip()
        elif param == 'ERR':
            self._last_error = True

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
                    self._send(f'< SET {self.channel} METER_RATE 00250 >')
                    self._metering_started = True
                self._send(f'< GET {self.channel} TX_BATT_BARS >')
                self._send(f'< GET {self.channel} TX_BATT_MINS >')
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
        self.last_command_error = None
        with self._io_lock:
            try:
                if cmd_type == 'FREQUENCY':
                    self._last_error = False
                    khz = int(round(float(value) * 1000))
                    self._send(f'< SET {channel} FREQUENCY {khz:06d} >')
                    self._parse_messages(self._read_messages(timeout=CONFIRM_READ_TIMEOUT, stop_early=False))
                    if self._last_error:
                        self.last_command_error = "Device rejected the frequency (REP ERR) -- likely outside this unit's tunable range."
                        return False
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
        self._last_error = False
        self.dante_status = None

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
                self._query_rf_band()
                self._query_dante_status()
            except Exception:
                self.status = 'DISCONNECTED'

    def _query_rf_band(self):
        """Real hardware discovery of this specific unit's RF band, so
        FrequencyHandler can refuse a deploy outside its actual tunable
        range instead of finding out only after the hardware rejects it."""
        try:
            self._send('< GET RF_BAND >')
            for msg in self._read_messages(timeout=0.5, stop_early=False):
                parts = msg.split()
                if len(parts) >= 3 and parts[0] == 'REP' and parts[1] == 'RF_BAND':
                    band = ' '.join(parts[2:]).strip('{}').strip()
                    if band:
                        self.rf_band = band
                        self.rf_range_mhz = AXIENT_DIGITAL_BAND_RANGES.get(band)
        except (OSError, socket.error):
            pass

    def _query_dante_status(self):
        """Axient Digital is Dante-native -- same confirmed NA_DEVICE_NAME
        command string as ULX-D (see ShureProvider._query_dante_status)."""
        try:
            self._send('< GET NA_DEVICE_NAME >')
            self._parse_messages(self._read_messages(timeout=0.5, stop_early=False))
        except (OSError, socket.error):
            pass

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
        elif param == 'TX_BATT_MINS':
            # Same encoding as SLX-D (confirmed in Axient Digital's own
            # spec): 0-65532 = minutes of runtime; 65533-65535 = comm
            # warning / still calculating / unknown, all covered by None.
            try:
                parsed = int(value)
                self.metrics['batt_minutes'] = parsed if parsed <= 65532 else None
            except ValueError: pass
        elif param == 'FREQUENCY':
            try: self.metrics['frequency_mhz'] = int(value) / 1000
            except ValueError: pass
        elif param == 'MODEL':
            self.model = value.strip('{}').strip()
        elif param == 'NA_DEVICE_NAME':
            name = value.strip('{}').strip()
            self.dante_status = {
                'interface_present': bool(name), 'interfaces': None, 'auto': None,
                'ip': None, 'device_name': name or None,
            }
        elif param == 'AUDIO_MUTE':
            self.metrics['muted'] = (value == 'ON')
        elif param == 'ERR':
            self._last_error = True

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
                    self._send(f'< SET {self.channel} METER_RATE 00250 >')
                    self._metering_started = True
                self._send(f'< GET {self.channel} TX_BATT_CHARGE_PERCENT >')
                self._send(f'< GET {self.channel} TX_BATT_MINS >')
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
        self.last_command_error = None
        with self._io_lock:
            try:
                if cmd_type == 'MUTE':
                    self._last_error = False
                    self._send(f'< SET {channel} AUDIO_MUTE {"ON" if value else "OFF"} >')
                    self._parse_messages(self._read_messages(timeout=CONFIRM_READ_TIMEOUT, stop_early=False))
                    return self.metrics.get('muted') == bool(value)
                elif cmd_type == 'FREQUENCY':
                    self._last_error = False
                    khz = int(round(float(value) * 1000))
                    self._send(f'< SET {channel} FREQUENCY {khz:06d} >')
                    self._parse_messages(self._read_messages(timeout=CONFIRM_READ_TIMEOUT, stop_early=False))
                    if self._last_error:
                        self.last_command_error = "Device rejected the frequency (REP ERR) -- likely outside this unit's tunable range."
                        return False
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
        self._last_error = False
        # Not a missing value -- there's no fixed carrier to report at all,
        # since MXW hops automatically across the 2.4GHz band (no FREQUENCY
        # command exists in its spec; see send_command below).
        self.freq_unavailable_reason = "No fixed carrier -- this system hops automatically in the 2.4GHz band"

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
        elif param == 'BATT_RUN_TIME':
            # MXW's own sentinel scheme (different from SLX-D/AD4's):
            # 0-65531 = minutes; 65532 = wall-wart powered (not draining);
            # 65533 = on charger; 65534 = calculating; 65535 = off. Only
            # the first case is a real countdown.
            try:
                parsed = int(value)
                self.metrics['batt_minutes'] = parsed if parsed <= 65531 else None
            except ValueError: pass
        elif param == 'TX_STATUS':
            self.metrics['muted'] = (value == 'MUTE')
        elif param == 'TX_TYPE':
            self.model = value.strip()
        elif param == 'ERR':
            self._last_error = True

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
                    self._send(f'< SET {self.channel} METER_RATE 00250 >')
                    self._metering_started = True
                self._send(f'< GET {self.channel} BATT_CHARGE >')
                self._send(f'< GET {self.channel} BATT_RUN_TIME >')
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
        self.freq_unavailable_reason = 'No network control for this model'
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

# Sennheiser ew G4 "Media control protocol" -- a completely different wire
# protocol from SSC below: plain ASCII (not JSON), on a single UDP port used
# for both sending and receiving, one attribute per bare-<CR>-terminated
# line (no LF). This is what ew 300/500 G4 stationary receivers (EM) and IEM
# transmitters (SR) actually speak -- NOT SSC/port 45, which only current
# digital lines (EW-DX/EW-D/9000/6000/Spectera) use. Confirmed live against
# a real EM unit; built from Sennheiser's own published spec (TI 1254 v1.0,
# "Media control protocol description for ew G4"):
#   Port 53212, ASCII, "Command param1 ... paramN<CR>" (single \r, no \n).
#   Push <timeoutSec> <cyclicMs> <flags><CR> subscribes to periodic status:
#     the device stops all cyclic/on-change pushes once timeoutSec elapses
#     without a fresh Push, so this must be resent well before it expires.
#     flags=3 here (send config-on-change + cyclic-on-warning-change).
#     Single one-off commands (Name, Frequency, ...) work with no active
#     subscription at all.
#   Cyclic attributes pushed together each cycle -- for EM: RF1, RF2,
#     States, RF, AF, Bat, Msg, Config. For SR: AF, States, Msg, Config.
#     EM and SR share the exact same port/framing/Push mechanism but have
#     different command sets (e.g. Squelch/AfOut only exist on EM,
#     Sensitivity/Mode only on SR) -- used here to tell them apart for real
#     (see identify_device()) instead of guessing from a model string.
#   Bat (EM only) is a coarse 4-point scale -- 0/30/70/100%, or '?' meaning
#     "no battery telegram received from the paired transmitter" -- not a
#     continuous percentage like every Shure battery field in this app.
G4_PORT = 53212
G4_PUSH_TIMEOUT_SEC = 10
G4_PUSH_CYCLIC_MS = 250

class SennheiserG4Provider(BaseProvider):
    def __init__(self, ip, device_type, photo=None, channel=1):
        super().__init__(ip, device_type, photo)
        # G4 stationary units are one channel per physical unit/IP -- no
        # multi-channel model in this family (unlike ULXD4Q) -- channel is
        # kept only so device_key()/get_json() stay consistent with every
        # other provider.
        self.channel = channel
        self._is_em = (device_type == 'sennheiser-g4-em')
        self.sock = None
        self.model = None
        self._buffer = ''
        self._miss_count = 0
        self._last_push_sent = 0
        self._last_error = False

    def connect(self):
        with self._io_lock:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.sock.settimeout(0.5)
                self.sock.connect((self.ip, G4_PORT))
                self.status = 'CONNECTED'
                self._buffer = ''
                self._miss_count = 0
                self._send(f'Push {G4_PUSH_TIMEOUT_SEC} {G4_PUSH_CYCLIC_MS} 3\r')
                self._last_push_sent = time.time()
                self._send('Name\r')
                self._send('FirmwareRevision\r')
                self._send('Frequency\r')
                self._send('Mute\r')
                for line in self._read_messages(timeout=0.5):
                    self._apply_line(line)
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

    def _send(self, message):
        self.sock.send(message.encode('ascii'))

    def _read_messages(self, timeout=0.4):
        """UDP datagrams queue in the kernel receive buffer between polls --
        no persistent listener thread needed, same pattern PSM1000Provider
        uses over TCP. Drains whatever has arrived since the last call; a
        single datagram can carry several attributes (the device sends all
        cyclic attributes "in one go"), each its own bare-<CR>-terminated line."""
        deadline = time.time() + timeout
        self.sock.settimeout(0.1)
        got_any = False
        while time.time() < deadline:
            try:
                chunk = self.sock.recv(4096)
                if chunk:
                    self._buffer += chunk.decode('ascii', errors='ignore')
                    got_any = True
            except socket.timeout:
                if got_any:
                    break
            except OSError:
                break
        lines = [l.strip() for l in self._buffer.split('\r') if l.strip()]
        self._buffer = ''
        return lines

    def _apply_line(self, line):
        parts = line.split()
        if not parts:
            return
        tag, vals = parts[0], parts[1:]
        if tag.endswith(':') and tag[:-1].isdigit():
            # Negative response to some prior command, e.g.
            # "1020: Value out of range [ Frequency 56 2 9 ]" -- not a
            # cyclic/config attribute, just note a command was rejected.
            self._last_error = True
        elif tag == 'Name' and vals:
            self.model = ' '.join(vals)
        elif tag == 'Frequency' and vals:
            try: self.metrics['frequency_mhz'] = int(vals[0]) / 1000
            except ValueError: pass
        elif tag == 'Mute' and vals:
            self.metrics['muted'] = vals[0] == '1'
        elif tag == 'Bat' and vals:
            # EM only -- coarse 4-point scale; '?' means the paired
            # transmitter's battery telegram hasn't been received at all.
            if vals[0] == '?':
                self.metrics['batt'] = None
            else:
                try: self.metrics['batt'] = int(vals[0])
                except ValueError: pass
        elif tag == 'RF' and vals:
            # EM only: current RF level in %, 100% = 40 dBuV.
            try: self.metrics['rf'] = int(float(vals[0]))
            except ValueError: pass
        elif tag == 'AF' and vals:
            # First value is always a live audio-level percentage on both EM
            # (peak) and SR (RX-path-1 peak), which is all this app's single
            # "audio" meter needs.
            try: self.metrics['audio'] = int(float(vals[0]))
            except ValueError: pass
        elif tag == 'States' and vals:
            if self._is_em:
                # Mute flags bitfield since last cycle -- bit 0 means some
                # kind of mute (TX/RF/RX) was active for the whole cycle.
                try: self.metrics['muted'] = bool(int(vals[0]) & 0b1)
                except ValueError: pass
            else:
                # SR: first value is the RF-Mute state right now (0=on air).
                self.metrics['muted'] = vals[0] == '1'
        elif tag == 'Msg':
            warning = ' '.join(vals)
            if warning and warning != 'OK':
                for w in vals:
                    label = w.replace('_', ' ')
                    if not self.alerts or self.alerts[-1] != label:
                        self.alerts.append(label)
                self.alerts = self.alerts[-20:]

    def poll(self):
        if self.status != 'CONNECTED': return
        with self._io_lock:
            try:
                now = time.time()
                if now - self._last_push_sent > G4_PUSH_TIMEOUT_SEC / 2:
                    self._send(f'Push {G4_PUSH_TIMEOUT_SEC} {G4_PUSH_CYCLIC_MS} 3\r')
                    self._last_push_sent = now
                lines = self._read_messages()
                for line in lines:
                    self._apply_line(line)
                if not lines:
                    self._miss_count += 1
                    if self._miss_count >= NETWORK_MISS_LIMIT:
                        self._mark_unreachable()
                        return
                else:
                    self._miss_count = 0
            except (OSError, socket.error):
                self._mark_unreachable()

    def scan_rf(self):
        self.spectrum_data = []  # no spectrum sweep in this protocol

    def send_command(self, cmd_type, value, channel=1):
        if self.status != 'CONNECTED': return False
        self.last_command_error = None
        with self._io_lock:
            try:
                if cmd_type == 'MUTE':
                    self._last_error = False
                    self._send(f'Mute {1 if value else 0}\r')
                    for line in self._read_messages(timeout=CONFIRM_READ_TIMEOUT):
                        self._apply_line(line)
                    return self.metrics.get('muted') == bool(value)
                elif cmd_type == 'FREQUENCY':
                    self._last_error = False
                    khz = int(round(float(value) * 1000))
                    self._send(f'Frequency {khz}\r')
                    for line in self._read_messages(timeout=CONFIRM_READ_TIMEOUT):
                        self._apply_line(line)
                    if self._last_error:
                        self.last_command_error = 'Rejected by device (value out of range for this unit)'
                        return False
                    return self.metrics.get('frequency_mhz') == round(khz / 1000, 4)
            except (OSError, socket.error):
                self._mark_unreachable()
        return False

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
        self._miss_count = 0
        # Populated on connect() -- confirmed real SSC addresses (not
        # guessed): /device/network/ether/interfaces lists which physical
        # interfaces the unit actually has ("CONTROL", and "DANTE" only on
        # Dante-capable variants); ipv4_dante/{auto,ipaddr} is that
        # interface's own network config, separate from the main control
        # IP this provider is already connected to. None means "not yet
        # queried" (e.g. device unreachable), not "confirmed absent".
        self.dante_status = None

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
                # UDP/IP, not TCP: per Sennheiser's own SSC spec, EVERY
                # networked SSC device "MUST implement the UDP/IP transport"
                # -- TCP is an optional addition some product lines layer on
                # top, not a given. Digital 6000 (EM 6000/L 6000) is
                # documented as supporting ONLY UDP, confirmed against a
                # real EM 6000 that never answered on TCP port 45 at all
                # despite being alive on the network.
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.sock.settimeout(0.6)
                self.sock.connect((self.ip, SSC_PORT))
                self._miss_count = 0
                # Pull the real model string once, for display (identify_device()'s
                # equivalent of ShureProvider's MODEL/DEVICE_ID parsing), and this
                # specific unit's actual tunable range(s) -- unlike Shure's RF_BAND
                # (a code requiring a separate lookup table), SSC reports the exact
                # numeric range(s) directly: no guessing needed for Sennheiser.
                # This round-trip also doubles as the actual reachability check --
                # UDP's connect() just records a default peer and never fails on
                # its own, unlike TCP's, so "did we get any reply at all" is the
                # only real signal that something is actually listening.
                self._send({'device': {
                    'identity': {'product': None},
                    'frequency_ranges': None, 'frequency_code': None,
                    'network': {'ether': {'interfaces': None}, 'ipv4_dante': {'auto': None, 'ipaddr': None}},
                }})
                replies = self._read_messages(timeout=0.6, stop_early=False)
                if not replies:
                    self.status = 'DISCONNECTED'
                    return
                self.status = 'CONNECTED'
                interfaces = None
                dante_auto = None
                dante_ip = None
                for msg in replies:
                    product = _ssc_extract(msg, 'device', 'identity', 'product')
                    if isinstance(product, str) and product.strip():
                        self.model = product.strip()
                    code = _ssc_extract(msg, 'device', 'frequency_code')
                    if isinstance(code, str) and code.strip():
                        self.rf_band = code.strip()
                    ranges = _ssc_extract(msg, 'device', 'frequency_ranges')
                    if isinstance(ranges, list):
                        parsed = self._parse_frequency_ranges(ranges)
                        if parsed:
                            self.rf_range_mhz = parsed
                    ifaces = _ssc_extract(msg, 'device', 'network', 'ether', 'interfaces')
                    if isinstance(ifaces, list):
                        interfaces = ifaces
                    auto = _ssc_extract(msg, 'device', 'network', 'ipv4_dante', 'auto')
                    if isinstance(auto, bool):
                        dante_auto = auto
                    ip = _ssc_extract(msg, 'device', 'network', 'ipv4_dante', 'ipaddr')
                    if isinstance(ip, str) and ip.strip():
                        dante_ip = ip.strip()
                # A device only reports the ipv4_dante branch at all if it
                # actually has that interface -- "DANTE" in the interfaces
                # list is the clean confirmation when present, but some
                # firmware may answer the ipv4_dante query without echoing
                # the interfaces list back in the same reply, so either
                # signal on its own is enough to call the interface present.
                dante_present = bool(interfaces and 'DANTE' in interfaces) or dante_ip is not None or dante_auto is not None
                self.dante_status = {
                    'interface_present': dante_present,
                    'interfaces': interfaces,
                    'auto': dante_auto,
                    'ip': dante_ip,
                }
            except Exception:
                self.status = 'DISCONNECTED'

    @staticmethod
    def _parse_frequency_ranges(range_strings):
        """Parses SSC's /device/frequency_ranges reply, a list of
        "<start_hz>:<step_hz>:<end_hz>" strings, into (min_mhz, max_mhz)
        tuples. e.g. ["470000000:25000:514875000"] -> [(470.0, 514.875)]."""
        parsed = []
        for entry in range_strings:
            try:
                start_hz, _step_hz, end_hz = entry.split(':')
                parsed.append((round(int(start_hz) / 1e6, 4), round(int(end_hz) / 1e6, 4)))
            except (ValueError, AttributeError):
                continue
        return parsed or None

    def disconnect(self):
        with self._io_lock:
            if self.sock:
                try: self.sock.close()
                except Exception: pass
            self.status = 'DISCONNECTED'

    def _send(self, obj):
        # Per spec: "One UDP datagram is used to transport one SSC Message"
        # -- no CRLF/LFLF framing needed (that convention exists only to
        # find message boundaries in TCP's byte stream, which UDP doesn't have).
        self.sock.send(json.dumps(obj).encode('utf-8'))

    def _read_messages(self, timeout=0.4, stop_early=True):
        """Each UDP datagram IS exactly one complete SSC JSON message --
        no stream reassembly needed, unlike TCP. Datagrams queue in the
        kernel receive buffer between calls (same "no persistent listener
        thread needed" pattern the other UDP-based providers in this file
        use), so this just drains whatever's arrived since the last call."""
        deadline = time.time() + timeout
        self.sock.settimeout(0.1)
        got_any = False
        messages = []
        while time.time() < deadline:
            try:
                chunk = self.sock.recv(8192)
                if chunk:
                    got_any = True
                    try:
                        messages.append(json.loads(chunk.decode('utf-8', errors='ignore')))
                    except ValueError:
                        pass
            except socket.timeout:
                if stop_early and got_any:
                    break
            except OSError:
                break
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
