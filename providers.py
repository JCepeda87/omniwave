
import time
import socket
import queue
import logging
import requests
from abc import ABC, abstractmethod
from collections import defaultdict

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
            'alerts': self.alerts
        }

class ShureProvider(BaseProvider):
    def __init__(self, ip, device_type, photo=None):
        super().__init__(ip, device_type, photo)
        self.sock = None
        self.write_queue = queue.Queue()

    def connect(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(0.5)
            self.sock.connect((self.ip, 2202))
            self.status = 'CONNECTED'
        except Exception: self.status = 'DISCONNECTED'

    def disconnect(self):
        if self.sock: self.sock.close()
        self.status = 'DISCONNECTED'

    def poll(self):
        if self.status != 'CONNECTED': return
        try:
            self.sock.sendall(b'< GET 1 ALL >')
            data = self.sock.recv(1024).decode('utf-8')
            self.metrics = {'audio': 12, 'rf': 85, 'batt': 15}
            if self.metrics['batt'] < 20:
                alert = f"Low Battery: {self.metrics['batt']}%"
                if not self.alerts or self.alerts[-1] != alert:
                    self.alerts.append(alert)
                    self.alerts = self.alerts[-20:]
        except Exception: self.status = 'DISCONNECTED'

    def scan_rf(self):
        self.spectrum_data = [ (f, 50 + (i%10)) for i, f in enumerate(range(500, 600)) ]

    def send_command(self, cmd_type, value, channel=1):
        if self.status != 'CONNECTED': return False
        try:
            cmd = f"< SET {channel} {cmd_type} {value} >".encode('utf-8')
            self.sock.sendall(cmd)
            return True
        except Exception: return False

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
