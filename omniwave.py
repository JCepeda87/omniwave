
import time
import logging
import json
import os
import sys
import re
import socket
import ipaddress
import sqlite3
import hmac
import requests
import subprocess
from concurrent.futures import ThreadPoolExecutor
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
DeviceNames = {}  # ip -> user-assigned label, e.g. "Lead Vocal"
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
        data = {}
        for ip, dev in Devices.items():
            entry = dev.get_json()
            entry['name'] = DeviceNames.get(ip, '')
            entry['brand'] = 'shure' if isinstance(dev, ShureProvider) else 'sennheiser'
            data[ip] = entry
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
        name = (params.get('name') or '').strip()
        if not ip:
            self.set_status(400)
            self.write(json.dumps({'error': 'ip is required'}))
            return

        dev = ShureProvider(ip, dtype) if brand == 'shure' else SennheiserProvider(ip, dtype)
        dev.connect()
        Devices[ip] = dev
        DeviceNames[ip] = name
        save_device_to_config(ip, brand, dtype, name)
        self.write(json.dumps({'success': True, 'ip': ip, 'status': dev.status}))

    def delete(self):
        ip = self.get_argument('ip', None)
        if ip and ip in Devices:
            Devices[ip].disconnect()
            del Devices[ip]
            DeviceNames.pop(ip, None)
            remove_device_from_config(ip)
            self.write(json.dumps({'success': True}))
        else:
            self.set_status(404)

class RenameHandler(RequestHandler):
    """Relabel a device without touching its connection (e.g. "Lead Vocal"
    instead of a raw IP), unlike DeviceHandler.post which reconnects."""
    def post(self):
        params = json.loads(self.request.body)
        ip = params.get('ip')
        name = (params.get('name') or '').strip()
        if not ip or ip not in Devices:
            self.set_status(404)
            return
        DeviceNames[ip] = name
        update_device_name_in_config(ip, name)
        self.write(json.dumps({'success': True, 'ip': ip, 'name': name}))

# Heuristic auto-discovery: probe the local /24 subnet for hosts with a
# known brand control port open. This is NOT the brands' certified
# discovery protocols (Shure uses Multicast SLP on 239.255.254.253 and
# also supports mDNS; Sennheiser Control Cockpit uses mDNS/DNS-SD with
# service type "_ssc") -- it's a best-effort port probe so discovery
# works without an extra mDNS/SLP dependency. Ports match what the
# providers already use/assume: 2202 for Shure, 45 for Sennheiser SSC.
DISCOVERY_PORTS = {
    2202: 'shure',
    45: 'sennheiser',
}
DISCOVERY_TIMEOUT = 0.3
DISCOVERY_MAX_WORKERS = 128

def get_local_subnet():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        local_ip = s.getsockname()[0]
    finally:
        s.close()
    return ipaddress.ip_network(local_ip + '/24', strict=False)

def probe_host(ip, port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(DISCOVERY_TIMEOUT)
            return s.connect_ex((ip, port)) == 0
    except OSError:
        return False

def scan_subnet():
    """Blocking; call via an executor so it doesn't stall the IOLoop."""
    network = get_local_subnet()
    hosts = list(network.hosts())
    found = []
    with ThreadPoolExecutor(max_workers=DISCOVERY_MAX_WORKERS) as pool:
        futures = {}
        for host in hosts:
            for port, brand in DISCOVERY_PORTS.items():
                futures[pool.submit(probe_host, str(host), port)] = (str(host), port, brand)
        for future in futures:
            if future.result():
                ip, port, brand = futures[future]
                found.append({'ip': ip, 'brand': brand, 'port': port})
    return found

class DiscoverHandler(RequestHandler):
    async def get(self):
        candidates = await IOLoop.current().run_in_executor(None, scan_subnet)
        candidates = [c for c in candidates if c['ip'] not in Devices]
        self.write(json.dumps({'candidates': candidates}))

# --- Frequency plan / scan import -----------------------------------------
# Shure and Sennheiser's native show-file formats (.wwb, .show) are
# undocumented proprietary containers, so this parses the interchange
# formats both brands actually document/support instead:
#   - Sennheiser WSM frequency list CSV (semicolon-delimited):
#       name;type;frequency_kHz;tolerance;lower;upper;priority;noise_level
#   - Sennheiser WSM wideband scan CSV export: 7 header rows ending in the
#     column row ['Frequency','RF level (%)','RF level','Memory (%)',
#     'Memory','Squelch (%)','Squelch'], then one data row per scan point.
#   - Shure WWB frequency list: plain frequencies in MHz (<=3 decimals)
#     separated by comma, tab, or newline -- no name/header fields.
WSM_SCAN_HEADER = ['Frequency', 'RF level (%)', 'RF level', 'Memory (%)', 'Memory', 'Squelch (%)', 'Squelch']

def parse_wsm_scan(rows):
    header_idx = next((i for i, row in enumerate(rows) if row == WSM_SCAN_HEADER), None)
    if header_idx is None:
        return None
    spectrum = []
    for row in rows[header_idx + 1:]:
        if len(row) < 3:
            continue
        raw_freq, raw_level = row[0].strip(), row[2].strip()
        if not raw_freq.isdigit():
            continue
        try:
            freq_mhz = float(raw_freq[:3] + '.' + raw_freq[3:]) if len(raw_freq) > 3 else float(raw_freq)
            level = float(raw_level)
        except ValueError:
            continue
        spectrum.append([round(freq_mhz, 3), level])
    return spectrum or None

def parse_wsm_frequency_list(rows):
    entries = []
    for row in rows:
        if len(row) < 3:
            continue
        name, dtype, freq_raw = row[0].strip(), row[1].strip(), row[2].strip()
        try:
            freq_khz = float(freq_raw)
        except ValueError:
            continue
        entries.append({'name': name or None, 'type': dtype or None, 'frequency_mhz': round(freq_khz / 1000, 4)})
    return entries or None

def parse_wwb_frequency_list(text):
    entries = []
    for token in re.split(r'[,\t\r\n]+', text):
        token = token.strip()
        if not token:
            continue
        try:
            freq = float(token)
        except ValueError:
            continue
        if 20 <= freq <= 7000:  # sane RF range in MHz; filters out stray non-frequency numbers
            entries.append({'name': None, 'type': None, 'frequency_mhz': round(freq, 3)})
    return entries or None

class ImportHandler(RequestHandler):
    def post(self):
        upload = self.request.files.get('file')
        if not upload:
            self.set_status(400)
            self.write(json.dumps({'error': 'file is required'}))
            return
        raw = upload[0]['body'].decode('utf-8', errors='ignore')
        rows = [line.split(';') for line in raw.splitlines() if line.strip()]

        scan = parse_wsm_scan(rows)
        if scan is not None:
            self.write(json.dumps({'format': 'wsm_scan', 'spectrum': scan}))
            return
        freq_list = parse_wsm_frequency_list(rows)
        if freq_list is not None:
            self.write(json.dumps({'format': 'wsm_frequency_list', 'frequencies': freq_list}))
            return
        wwb_list = parse_wwb_frequency_list(raw)
        if wwb_list is not None:
            self.write(json.dumps({'format': 'wwb_frequency_list', 'frequencies': wwb_list}))
            return

        self.set_status(422)
        self.write(json.dumps({'error': "Couldn't recognize this as a WSM or WWB frequency file."}))

_last_webhook_sent = {}
WEBHOOK_COOLDOWN_SECONDS = 60

def poll_devices():
    for ip, dev in Devices.items():
        dev.poll()
        log_metrics(ip, dev.metrics)
        batt = dev.metrics.get('batt', 100)
        if batt < 15 and time.time() - _last_webhook_sent.get(ip, 0) > WEBHOOK_COOLDOWN_SECONDS:
            send_webhook(f"CRITICAL LOW BATTERY: {ip} is at {batt}%")
            _last_webhook_sent[ip] = time.time()
    IOLoop.current().call_later(1, poll_devices)

def load_config():
    if not os.path.exists(CONFIG_PATH): return []
    with open(CONFIG_PATH, 'r') as f:
        return json.load(f).get('devices', [])

def save_config(device_list):
    with open(CONFIG_PATH, 'w') as f:
        json.dump({'devices': device_list}, f, indent=2)

def save_device_to_config(ip, brand, dtype, name=''):
    devices = [d for d in load_config() if d.get('ip') != ip]
    devices.append({'ip': ip, 'brand': brand, 'type': dtype, 'name': name})
    save_config(devices)

def remove_device_from_config(ip):
    devices = [d for d in load_config() if d.get('ip') != ip]
    save_config(devices)

def update_device_name_in_config(ip, name):
    devices = load_config()
    for d in devices:
        if d.get('ip') == ip:
            d['name'] = name
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
        (r'/devices/rename', RenameHandler),
        (r'/discover', DiscoverHandler),
        (r'/import', ImportHandler),
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
        DeviceNames[ip] = dev_cfg.get('name', '')

    poll_devices()
    print(f"OmniWave OS v{VERSION} running on 0.0.0.0:9000...")
    IOLoop.current().start()

if __name__ == '__main__':
    main()
