
import time
import socket
import queue
import logging
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

class BaseProvider(ABC):
    def __init__(self, ip, device_type, photo=None):
        self.ip = ip
        self.type = device_type
        self.photo = photo # Path to the photo
        self.status = 'DISCONNECTED'
        self.metrics = {}
        self.spectrum_data = []
        self.alerts = []

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

    def connect(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(0.5)
            self.sock.connect((self.ip, 2202))
            self.status = 'CONNECTED'
            self._metering_started = False
            self._buffer = ''
        except Exception:
            self.status = 'DISCONNECTED'

    def disconnect(self):
        if self.sock:
            try: self.sock.close()
            except Exception: pass
        self.status = 'DISCONNECTED'

    def _send(self, message):
        self.sock.sendall(message.encode('ascii'))

    def _read_messages(self, timeout=0.4):
        """Read whatever arrives within `timeout` and split it into complete
        '< ... >' command strings."""
        self.sock.settimeout(timeout)
        try:
            chunk = self.sock.recv(4096)
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
        try:
            if not self._metering_started:
                self._send(f'< SET {self.channel} METER_RATE 01000 >')
                self._metering_started = True
            self._send(f'< GET {self.channel} BATT_CHARGE >')
            self._send(f'< GET {self.channel} FREQUENCY >')
            self._parse_messages(self._read_messages())

            batt = self.metrics.get('batt')
            if batt is not None and batt < 20:
                alert = f"Low Battery: {batt}%"
                if not self.alerts or self.alerts[-1] != alert:
                    self.alerts.append(alert)
                    self.alerts = self.alerts[-20:]
        except (OSError, socket.error):
            self.status = 'DISCONNECTED'

    def scan_rf(self):
        # Wideband spectrum scanning isn't part of this documented ASCII
        # command-string protocol -- WWB drives it through a separate,
        # undocumented mechanism. Left empty rather than fabricating
        # scan-shaped data for a capability we can't actually perform yet.
        self.spectrum_data = []

    def send_command(self, cmd_type, value, channel=1):
        if self.status != 'CONNECTED': return False
        try:
            if cmd_type == 'MUTE':
                self._send(f'< SET {channel} AUDIO_MUTE {"ON" if value else "OFF"} >')
            elif cmd_type == 'FREQUENCY':
                khz = int(round(float(value) * 1000))
                self._send(f'< SET {channel} FREQUENCY {khz:06d} >')
            else:
                self._send(f'< SET {channel} {cmd_type} {value} >')
            return True
        except (OSError, socket.error, ValueError):
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
