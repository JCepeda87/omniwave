
import time
import logging
import json
import os
import sys
import sqlite3
import hmac
import requests
import subprocess
from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler
from providers import ShureProvider, SennheiserProvider

# --- CONFIGURATION ---
CENTRAL_HUB_URL = "https://hub.omniwave.io/api/register" # Example Hub URL
VERSION = "2.1.0"
INSTANCE_ID = os.getenv("OMNIWAVE_ID", "unregistered-instance")
OTA_TOKEN = os.getenv("OMNIWAVE_OTA_TOKEN", "SUPER_SECRET_OTA_TOKEN")
CONFIG_PATH = 'config.json'
# ---------------------

Devices = {}
DB_PATH = 'omniwave_history.db'

# Columns the metrics table must have. Add a new provider metric here (and it
# will be picked up automatically on the next restart, including the restart
# triggered by an OTA update) rather than editing the CREATE TABLE by hand.
METRIC_SCHEMA = {
    'batt': 'INTEGER',
    'rf': 'INTEGER',
    'audio': 'INTEGER',
}

def init_db():
    """Creates the metrics table if missing, and migrates in any new
    METRIC_SCHEMA columns for existing databases (so an OTA update that
    changes providers.py doesn't require wiping history)."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS metrics
                    (timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, ip TEXT)''')
    existing_cols = {row[1] for row in conn.execute('PRAGMA table_info(metrics)').fetchall()}
    for col, col_type in METRIC_SCHEMA.items():
        if col not in existing_cols:
            conn.execute(f'ALTER TABLE metrics ADD COLUMN {col} {col_type}')
    conn.commit()
    conn.close()

def register_installation():
    """Telemetry: Phone home to the central hub on startup."""
    payload = {
        "instance_id": INSTANCE_ID,
        "version": VERSION,
        "ip": "LAN_DYNAMIC", 
        "os": os.name,
        "timestamp": time.time()
    }
    try:
        # In a real deployment, this would be a POST to a central database
        # requests.post(CENTRAL_HUB_URL, json=payload, timeout=2)
        print(f"Telemetry: Registered instance {INSTANCE_ID} (v{VERSION}) with Central Hub.")
    except Exception as e:
        print(f"Telemetry Error: {e}")

def log_metrics(ip, metrics):
    cols = ['ip'] + list(METRIC_SCHEMA.keys())
    values = [ip] + [metrics.get(c) for c in METRIC_SCHEMA.keys()]
    placeholders = ','.join(['?'] * len(cols))
    conn = sqlite3.connect(DB_PATH)
    conn.execute(f'INSERT INTO metrics ({",".join(cols)}) VALUES ({placeholders})', values)
    conn.commit()
    conn.close()

def send_webhook(message):
    webhook_url = "https://hooks.slack.com/services/T000/B000/XXXX" 
    try:
        requests.post(webhook_url, json={"text": f"🚨 [OmniWave Alert]: {message}"}, timeout=1)
    except: pass

class DataHandler(RequestHandler):
    def get(self):
        data = {ip: dev.get_json() for ip, dev in Devices.items()}
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(data))

class UpdateHandler(RequestHandler):
    """OTA Update Handler: pulls the latest code and restarts in place.

    Restarting via os.execv means any change to providers.py (new metric
    fields, new device types) takes effect immediately, and init_db()'s
    migration step runs again on the new code to pick up schema changes.
    """
    def post(self):
        auth_token = self.get_argument('token', None)
        if not auth_token or not hmac.compare_digest(auth_token, OTA_TOKEN):
            self.set_status(403)
            return

        try:
            print("OTA Update triggered. Pulling latest version...")
            # 1. Pull latest code from git
            subprocess.run(["git", "pull", "origin", "master"], check=True)
            # 2. Update dependencies
            subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], check=True)

            self.write(json.dumps({"status": "success", "message": "Update applied. Restarting server..."}))

            # 3. Re-exec this process so the freshly pulled code (and its
            # schema migrations) take over immediately.
            os.execv(sys.executable, [sys.executable, os.path.abspath(__file__)])

        except Exception as e:
            self.set_status(500)
            self.write(json.dumps({"status": "error", "message": str(e)}))

class AnalyticsHandler(RequestHandler):
    def get(self):
        ip = self.get_argument('ip', None)
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        if ip:
            cursor.execute('SELECT * FROM (SELECT * FROM metrics WHERE ip=? ORDER BY timestamp DESC LIMIT 50)', (ip,))
        else:
            cursor.execute('SELECT * FROM metrics ORDER BY timestamp DESC LIMIT 50')
        data = cursor.fetchall()
        conn.close()
        self.write(json.dumps(data))

class CommandHandler(RequestHandler):
    def post(self):
        params = json.loads(self.request.body)
        ip = params.get('ip')
        cmd = params.get('command')
        val = params.get('value')
        chan = params.get('channel', 1)
        if ip in Devices:
            success = Devices[ip].send_command(cmd, val, chan)
            self.write(json.dumps({'success': success}))
        else: self.set_status(404)

class ScanHandler(RequestHandler):
    def get(self):
        ip = self.get_argument('ip', None)
        if ip and ip in Devices:
            dev = Devices[ip]
            dev.scan_rf()
            self.write(json.dumps({'spectrum': dev.spectrum_data}))
        else: self.set_status(404)

class StreamHandler(RequestHandler):
    def get(self):
        ip = self.get_argument('ip', None)
        if not ip or ip not in Devices: self.set_status(404); return
        self.set_header('Content-Type', 'audio/mpeg')
        self.write(f"Audio stream proxy for {ip}")

class IndexHandler(RequestHandler):
    def get(self):
        with open(os.path.join(os.path.abspath('.'), 'static', 'index.html'), 'rb') as f:
            self.write(f.read())

class StaticHandler(RequestHandler):
    def get(self):
        path = self.get_argument('path')
        base_dir = os.path.abspath('.')
        full_path = os.path.abspath(os.path.join(base_dir, path))
        if not full_path.startswith(base_dir + os.sep):
            self.set_status(403); return
        if not os.path.isfile(full_path):
            self.set_status(404); return
        with open(full_path, 'rb') as f:
            self.write(f.read())

class DeviceHandler(RequestHandler):
    """Add or remove a mic/wireless-system IP at runtime, persisted to
    config.json so it's still there on the next restart."""
    def post(self):
        params = json.loads(self.request.body)
        ip = params.get('ip')
        brand = params.get('brand', 'shure')
        dtype = params.get('type', 'axtd')
        if not ip:
            self.set_status(400)
            self.write(json.dumps({'error': 'ip is required'}))
            return

        dev = ShureProvider(ip, dtype) if brand == 'shure' else SennheiserProvider(ip, dtype)
        dev.connect()
        Devices[ip] = dev
        save_device_to_config(ip, brand, dtype)
        self.write(json.dumps({'success': True, 'ip': ip, 'status': dev.status}))

    def delete(self):
        ip = self.get_argument('ip', None)
        if ip and ip in Devices:
            Devices[ip].disconnect()
            del Devices[ip]
            remove_device_from_config(ip)
            self.write(json.dumps({'success': True}))
        else:
            self.set_status(404)

def poll_devices():
    for ip, dev in Devices.items():
        dev.poll()
        log_metrics(ip, dev.metrics)
        if dev.metrics.get('batt', 100) < 15:
            send_webhook(f"CRITICAL LOW BATTERY: {ip} is at {dev.metrics['batt']}%")
    IOLoop.current().call_later(1, poll_devices)

def load_config():
    if not os.path.exists(CONFIG_PATH): return []
    with open(CONFIG_PATH, 'r') as f:
        return json.load(f).get('devices', [])

def save_config(device_list):
    with open(CONFIG_PATH, 'w') as f:
        json.dump({'devices': device_list}, f, indent=2)

def save_device_to_config(ip, brand, dtype):
    devices = [d for d in load_config() if d.get('ip') != ip]
    devices.append({'ip': ip, 'brand': brand, 'type': dtype})
    save_config(devices)

def remove_device_from_config(ip):
    devices = [d for d in load_config() if d.get('ip') != ip]
    save_config(devices)

def main():
    init_db()
    register_installation()
    
    app = Application([
        (r'/', IndexHandler),
        (r'/data', DataHandler),
        (r'/analytics', AnalyticsHandler),
        (r'/command', CommandHandler),
        (r'/scan', ScanHandler),
        (r'/stream', StreamHandler),
        (r'/system/update', UpdateHandler),
        (r'/devices', DeviceHandler),
        (r'/static/(.*)', StaticHandler),
    ])
    app.listen(9000, address='0.0.0.0')
    device_list = load_config()
    for dev_cfg in device_list:
        ip = dev_cfg['ip']
        brand = dev_cfg.get('brand', 'shure')
        dtype = dev_cfg.get('type', 'axtd')
        Devices[ip] = ShureProvider(ip, dtype) if brand == 'shure' else SennheiserProvider(ip, dtype)
        Devices[ip].connect()

    poll_devices()
    print(f"OmniWave OS v{VERSION} running on 0.0.0.0:9000...")
    IOLoop.current().start()

if __name__ == '__main__':
    main()
