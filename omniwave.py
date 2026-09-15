
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
import asyncio
import mimetypes
import requests
import subprocess
from concurrent.futures import ThreadPoolExecutor
from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler
from providers import (ShureProvider, SennheiserProvider, UHFRProvider, PSM1000Provider,
                       SLXDProvider, AxientDigitalProvider, MXWProvider, SennheiserSSCProvider,
                       NoNetworkProvider)
import spectrum_planner

# All blocking device I/O (poll, send_command, connect) runs here instead of
# on the main IOLoop thread, so one slow/unresponsive device -- or a command
# waiting on a hardware confirmation -- can't stall polling every other
# device or serving any other HTTP request. Sized well above the device
# count so polling everything in parallel plus the occasional command never
# has to queue for a worker.
DEVICE_EXECUTOR = ThreadPoolExecutor(max_workers=64)

# --- CONFIGURATION ---
CENTRAL_HUB_URL = "https://hub.omniwave.io/api/register" # Example Hub URL
VERSION = "2.1.0"
INSTANCE_ID = os.getenv("OMNIWAVE_ID", "unregistered-instance")
OTA_TOKEN = os.getenv("OMNIWAVE_OTA_TOKEN")  # no default: /system/update refuses to run without a real one
CONFIG_PATH = 'config.json'
PHOTO_DIR = os.path.join('static', 'photos')  # user-uploaded device photos, gitignored like config.json
# ---------------------

Devices = {}
DeviceNames = {}  # ip -> unit label (e.g. "VOX 1", "ULXD4Q-1") -- fixed at add time, not user-renamed
DeviceAssignedUsers = {}  # ip -> name of the person currently using the device (User Board editable)
DeviceFrequencies = {}  # ip -> assigned carrier frequency in MHz
DeviceLayout = {}  # ip -> {'size': 'sm'|'md'|'lg', 'order': int} -- User Board card layout, admin-only
CARD_SIZES = {'sm', 'md', 'lg'}
DB_PATH = 'omniwave_history.db'

# dtype tags matching the "type" picker in static/index.html's DEVICE_MODELS.
# Models not listed here (ulxd/qlxd/uhf-r/psm1000/custom, and every
# Sennheiser type not in the two sets below) keep routing to the original
# generic providers -- either because that's already correct (ULX-D/QLX-D/
# UHF-R/PSM1000 are hardware-verified), or because the model is ambiguous
# (ew-g4 spans both networked and non-networked variants) or wasn't
# researched deeply enough here to implement confidently (Digital 9000/6000,
# Spectera, SpeechLine DW, 2000 Series).
SHURE_NO_NETWORK_TYPES = {'glxd-plus', 'blx', 'psm900', 'psm300'}
SENNHEISER_SSC_TYPES = {'ewdx', 'ewd', 'digital9000', 'digital6000', 'spectera'}
SENNHEISER_NO_NETWORK_TYPES = {'xswd', 'xsw-iem', 'avx'}

def make_provider(ip, brand, dtype, channel=1):
    if brand == 'shure':
        if dtype == 'uhf-r':
            return UHFRProvider(ip, dtype, channel=channel)
        if dtype == 'psm1000':
            return PSM1000Provider(ip, dtype, channel=channel)
        if dtype == 'slxd-plus':
            return SLXDProvider(ip, dtype, channel=channel)
        if dtype == 'axient-digital':
            return AxientDigitalProvider(ip, dtype, channel=channel)
        if dtype == 'mxw':
            return MXWProvider(ip, dtype, channel=channel)
        if dtype in SHURE_NO_NETWORK_TYPES:
            return NoNetworkProvider(ip, dtype)
        return ShureProvider(ip, dtype, channel=channel)
    if dtype in SENNHEISER_SSC_TYPES:
        return SennheiserSSCProvider(ip, dtype, channel=channel)
    if dtype in SENNHEISER_NO_NETWORK_TYPES:
        return NoNetworkProvider(ip, dtype)
    return SennheiserProvider(ip, dtype)

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
            entry['assigned_user'] = DeviceAssignedUsers.get(ip, '')
            layout = DeviceLayout.get(ip, {})
            entry['card_size'] = layout.get('size', 'md')
            entry['card_order'] = layout.get('order', 0)
            entry['brand'] = 'shure' if isinstance(dev, (ShureProvider, UHFRProvider, PSM1000Provider)) else 'sennheiser'
            # Prefer the live, hardware-reported frequency (real Shure gear
            # reports this every poll) over the locally-assigned one, so the
            # UI reflects what's actually on air rather than stale bookkeeping.
            entry['frequency_mhz'] = dev.metrics.get('frequency_mhz', DeviceFrequencies.get(ip))
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
        if not OTA_TOKEN or not auth_token or not hmac.compare_digest(auth_token, OTA_TOKEN):
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
    async def post(self):
        params = json.loads(self.request.body)
        ip = params.get('ip')
        cmd = params.get('command')
        val = params.get('value')
        chan = params.get('channel', 1)
        if ip in Devices:
            success = await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, Devices[ip].send_command, cmd, val, chan)
            self.write(json.dumps({'success': success}))
        else: self.set_status(404)

class ScanHandler(RequestHandler):
    async def get(self):
        ip = self.get_argument('ip', None)
        if ip and ip in Devices:
            dev = Devices[ip]
            await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, dev.scan_rf)
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

class UserIndexHandler(RequestHandler):
    """The read-mostly, on-stage-facing view (battery/RF/name/photo per
    device) -- as opposed to IndexHandler's full admin dashboard."""
    def get(self):
        with open(os.path.join(os.path.abspath('.'), 'static', 'user.html'), 'rb') as f:
            self.write(f.read())

class StaticHandler(RequestHandler):
    def get(self, path):
        base_dir = os.path.abspath('static')
        full_path = os.path.abspath(os.path.join(base_dir, path))
        if not full_path.startswith(base_dir + os.sep):
            self.set_status(403); return
        if not os.path.isfile(full_path):
            self.set_status(404); return
        content_type, _ = mimetypes.guess_type(full_path)
        self.set_header('Content-Type', content_type or 'application/octet-stream')
        with open(full_path, 'rb') as f:
            self.write(f.read())

class DeviceHandler(RequestHandler):
    """Add or remove a mic/wireless-system IP at runtime, persisted to
    config.json so it's still there on the next restart."""
    async def post(self):
        params = json.loads(self.request.body)
        ip = params.get('ip')
        brand = params.get('brand', 'shure')
        dtype = params.get('type', 'axtd')
        name = (params.get('name') or '').strip()
        try:
            channel = int(params.get('channel', 1) or 1)
        except (TypeError, ValueError):
            channel = 1
        if not ip:
            self.set_status(400)
            self.write(json.dumps({'error': 'ip is required'}))
            return

        dev = make_provider(ip, brand, dtype, channel)
        await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, dev.connect)
        Devices[ip] = dev
        DeviceNames[ip] = name
        DeviceLayout.setdefault(ip, {'size': 'md', 'order': len(Devices)})
        save_device_to_config(ip, brand, dtype, name, channel)
        self.write(json.dumps({'success': True, 'ip': ip, 'status': dev.status}))

    async def delete(self):
        ip = self.get_argument('ip', None)
        if ip and ip in Devices:
            await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, Devices[ip].disconnect)
            del Devices[ip]
            DeviceNames.pop(ip, None)
            DeviceAssignedUsers.pop(ip, None)
            DeviceFrequencies.pop(ip, None)
            DeviceLayout.pop(ip, None)
            remove_device_from_config(ip)
            self.write(json.dumps({'success': True}))
        else:
            self.set_status(404)

class RenameHandler(RequestHandler):
    """Relabel a device's unit name without touching its connection, unlike
    DeviceHandler.post which reconnects. Admin-only -- the User Board treats
    the unit name as fixed and only lets people edit who's using it
    (see AssignUserHandler)."""
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

class AssignUserHandler(RequestHandler):
    """Sets who's currently using a device (shown over the photo on the User
    Board) -- separate from the device's own unit name/label, which stays
    fixed here."""
    def post(self):
        params = json.loads(self.request.body)
        ip = params.get('ip')
        assigned_user = (params.get('assigned_user') or '').strip()
        if not ip or ip not in Devices:
            self.set_status(404)
            return
        DeviceAssignedUsers[ip] = assigned_user
        update_device_assigned_user_in_config(ip, assigned_user)
        self.write(json.dumps({'success': True, 'ip': ip, 'assigned_user': assigned_user}))

class CardSizeHandler(RequestHandler):
    """Admin-only: sets a device's card size on the User Board (sm/md/lg)."""
    def post(self):
        params = json.loads(self.request.body)
        ip = params.get('ip')
        size = params.get('size')
        if not ip or ip not in Devices:
            self.set_status(404)
            return
        if size not in CARD_SIZES:
            self.set_status(400)
            self.write(json.dumps({'error': f'size must be one of {sorted(CARD_SIZES)}'}))
            return
        DeviceLayout.setdefault(ip, {})['size'] = size
        update_device_card_size_in_config(ip, size)
        self.write(json.dumps({'success': True, 'ip': ip, 'size': size}))

class CardOrderHandler(RequestHandler):
    """Admin-only: persists the full User Board card order after a
    drag-and-drop reorder -- the client sends the complete ordered IP list
    rather than a single move, which is simpler and more robust than
    reconciling pairwise position swaps."""
    def post(self):
        params = json.loads(self.request.body)
        order = params.get('order', [])
        for idx, ip in enumerate(order):
            if ip in Devices:
                DeviceLayout.setdefault(ip, {})['order'] = idx
                update_device_card_order_in_config(ip, idx)
        self.write(json.dumps({'success': True}))

PHOTO_EXTENSIONS = {'image/jpeg': '.jpg', 'image/png': '.png', 'image/webp': '.webp', 'image/gif': '.gif'}

def _photo_filename(ip, ext):
    return f"{ip.replace('.', '_')}{ext}"

class PhotoHandler(RequestHandler):
    """Attach/replace or remove the photo shown for a device on the user-facing
    board (who's wearing this mic) -- stored on disk under PHOTO_DIR and
    persisted to config.json like name/frequency so it survives a restart."""
    def post(self):
        ip = self.get_body_argument('ip', None)
        if not ip or ip not in Devices:
            self.set_status(404)
            return
        upload = self.request.files.get('photo')
        if not upload:
            self.set_status(400)
            self.write(json.dumps({'error': 'photo file is required'}))
            return
        file_info = upload[0]
        content_type = file_info.get('content_type', '')
        ext = PHOTO_EXTENSIONS.get(content_type)
        if not ext:
            self.set_status(415)
            self.write(json.dumps({'error': 'unsupported image type (use jpeg/png/webp/gif)'}))
            return

        os.makedirs(PHOTO_DIR, exist_ok=True)
        for existing_ext in PHOTO_EXTENSIONS.values():
            stale = os.path.join(PHOTO_DIR, _photo_filename(ip, existing_ext))
            if os.path.isfile(stale):
                os.remove(stale)
        filename = _photo_filename(ip, ext)
        with open(os.path.join(PHOTO_DIR, filename), 'wb') as f:
            f.write(file_info['body'])

        photo_url = f"/static/photos/{filename}"
        Devices[ip].photo = photo_url
        update_device_photo_in_config(ip, photo_url)
        self.write(json.dumps({'success': True, 'ip': ip, 'photo': photo_url}))

    def delete(self):
        ip = self.get_argument('ip', None)
        if not ip or ip not in Devices:
            self.set_status(404)
            return
        for ext in PHOTO_EXTENSIONS.values():
            stale = os.path.join(PHOTO_DIR, _photo_filename(ip, ext))
            if os.path.isfile(stale):
                os.remove(stale)
        Devices[ip].photo = None
        update_device_photo_in_config(ip, None)
        self.write(json.dumps({'success': True, 'ip': ip}))

class FrequencyHandler(RequestHandler):
    """Assign a carrier frequency (MHz) to a device and, for a connected
    device, actually push it to the hardware (SET FREQUENCY) -- this is the
    real "Assign & Deploy" step, not just local bookkeeping."""
    async def post(self):
        params = json.loads(self.request.body)
        ip = params.get('ip')
        freq = params.get('frequency_mhz')
        if not ip or ip not in Devices:
            self.set_status(404)
            return
        try:
            freq = round(float(freq), 4) if freq not in (None, '') else None
        except (TypeError, ValueError):
            self.set_status(400)
            self.write(json.dumps({'error': 'frequency_mhz must be a number'}))
            return
        if freq is None:
            DeviceFrequencies.pop(ip, None)
        else:
            DeviceFrequencies[ip] = freq
        update_device_frequency_in_config(ip, freq)

        deployed = False
        if freq is not None and Devices[ip].status == 'CONNECTED':
            deployed = await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, Devices[ip].send_command, 'FREQUENCY', freq)
        self.write(json.dumps({'success': True, 'ip': ip, 'frequency_mhz': freq, 'deployed': deployed}))

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

def probe_host_udp_uhfr(ip):
    """UHF-R only responds over UDP -- a plain TCP connect_ex() (scan_subnet's
    check) never sees it, since UDP has no equivalent "is it listening"
    probe short of actually speaking the protocol."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(DISCOVERY_TIMEOUT)
            s.sendto(b'* GET 1 CHAN_NAME *', (ip, 2202))
            s.recvfrom(4096)
        return True
    except OSError:
        return False

def scan_subnet_udp():
    """Blocking; call via an executor. Finds UDP-only hosts (UHF-R) that
    scan_subnet()'s TCP probe can't see."""
    network = get_local_subnet()
    hosts = list(network.hosts())
    found = []
    with ThreadPoolExecutor(max_workers=DISCOVERY_MAX_WORKERS) as pool:
        futures = {pool.submit(probe_host_udp_uhfr, str(host)): str(host) for host in hosts}
        for future in futures:
            if future.result():
                found.append({'ip': futures[future], 'brand': 'shure', 'port': 2202})
    return found

def scan_subnet_all():
    """Blocking; call via an executor. TCP + UDP candidates combined,
    deduplicated by IP (TCP result wins if a host somehow shows up in both)."""
    combined = {}
    for c in scan_subnet_udp() + scan_subnet():
        combined[c['ip']] = c
    return list(combined.values())

class DiscoverHandler(RequestHandler):
    async def get(self):
        candidates = await IOLoop.current().run_in_executor(None, scan_subnet_all)
        candidates = [c for c in candidates if c['ip'] not in Devices]
        self.write(json.dumps({'candidates': candidates}))

# Real identification, not just "is the port open": speaks each protocol
# we've actually implemented (ShureProvider's TCP command strings,
# UHFRProvider's UDP one, PSM1000Provider's one-way push) well enough to
# tell them apart, and pulls a real name off the wire when one's available.
IDENTIFY_TIMEOUT = 0.6
DEVICE_ID_RE = re.compile(r'DEVICE_ID \{([^}]*)\}')
MODEL_RE = re.compile(r'MODEL \{([^}]*)\}')

def _drain_tcp(sock, seconds):
    """A single recv() can catch a multi-line '< GET x ALL >' reply
    mid-burst; keep reading until the window closes instead."""
    sock.settimeout(0.15)
    deadline = time.time() + seconds
    chunks = []
    while time.time() < deadline:
        try:
            chunk = sock.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
        except socket.timeout:
            if chunks:
                break
    return b''.join(chunks).decode('ascii', errors='ignore')

def identify_device(ip):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(IDENTIFY_TIMEOUT)
            s.connect((ip, 2202))
            s.sendall(b'< GET 1 ALL >')
            text = _drain_tcp(s, IDENTIFY_TIMEOUT)
        if 'AUDIO_IN_LVL' in text:
            return {'brand': 'shure', 'type': 'psm1000', 'label': 'PSM1000 (P10T)', 'name': None}
        device_id = DEVICE_ID_RE.search(text)
        model = MODEL_RE.search(text)
        raw = (device_id.group(1) if device_id else (model.group(1) if model else '')).strip()
        if 'ULXD' in text or raw.startswith('ULXD'):
            return {'brand': 'shure', 'type': 'ulxd', 'label': 'ULX-D', 'name': raw or None}
        if 'QLXD' in text or raw.startswith('QLXD'):
            return {'brand': 'shure', 'type': 'qlxd', 'label': 'QLX-D', 'name': raw or None}
        if 'SLXD' in text or raw.startswith('SLXD'):
            return {'brand': 'shure', 'type': 'slxd-plus', 'label': 'SLX-D', 'name': raw or None}
        if 'AD4' in text or 'ADX' in text or raw.startswith(('AD4', 'ADX')):
            return {'brand': 'shure', 'type': 'axient-digital', 'label': 'Axient Digital', 'name': raw or None}
        if 'MXWAPT' in text or raw.startswith('MXWAPT'):
            return {'brand': 'shure', 'type': 'mxw', 'label': 'MXW (Microflex Wireless)', 'name': raw or None}
        if '<' in text and 'REP' in text:
            return {'brand': 'shure', 'type': 'custom', 'label': 'Shure (unidentified model)', 'name': raw or None}
    except OSError:
        pass

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(IDENTIFY_TIMEOUT)
            s.sendto(b'* GET 1 CHAN_NAME *', (ip, 2202))
            data, _ = s.recvfrom(4096)
        text = data.decode('ascii', errors='ignore')
        if 'REPORT' in text:
            parts = text.strip('* \r\n').split()
            name = parts[3] if len(parts) >= 4 else None
            return {'brand': 'shure', 'type': 'uhf-r', 'label': 'UHF-R (UR4D/UR4S)', 'name': name}
    except OSError:
        pass

    # Sennheiser SSC (Sound Control Protocol): JSON over TCP port 45. A real
    # GET for the device's model string, not just "is the port open" --
    # https://docs.cloud.sennheiser.com/en-us/control-cockpit/control-cockpit/ssc-protocols.html
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(IDENTIFY_TIMEOUT)
            s.connect((ip, 45))
            s.sendall(b'{"device":{"identity":{"product":null}}}\r\n')
            data = s.recv(4096)
        text = data.decode('utf-8', errors='ignore').strip()
        product = None
        if text:
            try:
                parsed = json.loads(text.splitlines()[0])
                device = parsed.get('device')
                if isinstance(device, dict):
                    product = device.get('identity', {}).get('product')
            except (ValueError, AttributeError, IndexError):
                product = None
        if isinstance(product, str) and product.strip():
            product = product.strip()
            product_upper = product.upper()
            if 'EW-DX' in product_upper:
                dtype = 'ewdx'
            elif 'EW-D' in product_upper:
                dtype = 'ewd'
            elif '9000' in product_upper:
                dtype = 'digital9000'
            elif '6000' in product_upper:
                dtype = 'digital6000'
            elif 'SPECTERA' in product_upper:
                dtype = 'spectera'
            else:
                dtype = 'ewdx'
            return {'brand': 'sennheiser', 'type': dtype, 'label': product, 'name': product}
    except OSError:
        pass

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(DISCOVERY_TIMEOUT)
            if s.connect_ex((ip, 45)) == 0:
                return {'brand': 'sennheiser', 'type': 'ew-g4', 'label': 'Sennheiser (SSC port open, model undetermined)', 'name': None}
    except OSError:
        pass
    return None

AUTO_DISCOVERY_ENABLED = False
AUTO_DISCOVERY_INTERVAL_SECONDS = 20

def run_auto_discovery_scan():
    """Blocking; call via an executor. Finds live hosts, identifies each one
    for real, and returns only ones not already in Devices."""
    found = []
    for candidate in scan_subnet_all():
        ip = candidate['ip']
        if ip in Devices:
            continue
        info = identify_device(ip)
        if info:
            found.append({'ip': ip, **info})
    return found

async def auto_discovery_tick():
    if AUTO_DISCOVERY_ENABLED:
        try:
            found = await IOLoop.current().run_in_executor(None, run_auto_discovery_scan)
            for f in found:
                ip = f['ip']
                if ip in Devices:  # could've been added manually mid-scan
                    continue
                name = f.get('name') or f"{f['label']} ({ip})"
                dev = make_provider(ip, f['brand'], f['type'], channel=1)
                await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, dev.connect)
                Devices[ip] = dev
                DeviceNames[ip] = name
                DeviceLayout.setdefault(ip, {'size': 'md', 'order': len(Devices)})
                save_device_to_config(ip, f['brand'], f['type'], name, 1)
                print(f"Auto-discovery: added {name} at {ip}")
        except Exception as e:
            print(f"Auto-discovery tick failed: {e}")
    IOLoop.current().call_later(AUTO_DISCOVERY_INTERVAL_SECONDS, lambda: IOLoop.current().spawn_callback(auto_discovery_tick))

class AutoDiscoveryToggleHandler(RequestHandler):
    def get(self):
        self.write(json.dumps({'enabled': AUTO_DISCOVERY_ENABLED}))

    def post(self):
        global AUTO_DISCOVERY_ENABLED
        params = json.loads(self.request.body)
        AUTO_DISCOVERY_ENABLED = bool(params.get('enabled'))
        self.write(json.dumps({'enabled': AUTO_DISCOVERY_ENABLED}))

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

# --- RF coordination (TV-channel + intermodulation planning) --------------

class RegionsHandler(RequestHandler):
    def get(self):
        out = {}
        for key, region in spectrum_planner.REGIONS.items():
            out[key] = {
                'name': region['name'],
                'tv_channel_start': region['tv_channel_start'],
                'tv_channel_end': region['tv_channel_end'],
                'tv_channel_width_mhz': region['tv_channel_width_mhz'],
                'tv_band_start_mhz': region['tv_band_start_mhz'],
                'wireless_mic_ranges_mhz': region['wireless_mic_ranges_mhz'],
                'notes': region['notes'],
            }
        self.write(json.dumps(out))

def _active_device_frequencies(exclude_ip=None):
    """Every device's best-known frequency: the live hardware reading when
    available (so coordination always accounts for what's actually on air),
    falling back to the locally-assigned one for devices that don't report it.
    `exclude_ip` leaves one device's own frequency out -- e.g. when finding a
    replacement for that exact device, its old value shouldn't count as a
    conflict against the new one."""
    freqs = []
    for ip, dev in Devices.items():
        if ip == exclude_ip:
            continue
        freq = dev.metrics.get('frequency_mhz', DeviceFrequencies.get(ip))
        if freq is not None:
            freqs.append(freq)
    return freqs

class CoordinationCheckHandler(RequestHandler):
    def post(self):
        params = json.loads(self.request.body)
        region = params.get('region')
        if region not in spectrum_planner.REGIONS:
            self.set_status(400)
            self.write(json.dumps({'error': 'unknown region'}))
            return
        occupied = [int(c) for c in params.get('occupied_channels', [])]
        extra = [float(f) for f in params.get('frequencies', [])]
        scan_exclusions = [tuple(r) for r in params.get('scan_exclusions', [])]
        im_margin_khz = float(params.get('im_margin_khz', spectrum_planner.IM_MARGIN_KHZ_DEFAULT))

        all_freqs = _active_device_frequencies() + extra
        conflicts = spectrum_planner.find_im_conflicts(all_freqs, im_margin_khz)
        ranges = spectrum_planner.usable_ranges(region, occupied, scan_exclusions)
        self.write(json.dumps({
            'usable_ranges_mhz': ranges,
            'conflicts': conflicts,
            'frequencies_checked': all_freqs,
        }))

class CoordinationSuggestHandler(RequestHandler):
    def post(self):
        params = json.loads(self.request.body)
        region = params.get('region')
        if region not in spectrum_planner.REGIONS:
            self.set_status(400)
            self.write(json.dumps({'error': 'unknown region'}))
            return
        occupied = [int(c) for c in params.get('occupied_channels', [])]
        extra = [float(f) for f in params.get('frequencies', [])]
        scan_exclusions = [tuple(r) for r in params.get('scan_exclusions', [])]
        count = max(1, min(int(params.get('count', 1)), 50))
        min_spacing_mhz = float(params.get('min_spacing_khz', spectrum_planner.MIN_SPACING_MHZ_DEFAULT * 1000)) / 1000
        im_margin_khz = float(params.get('im_margin_khz', spectrum_planner.IM_MARGIN_KHZ_DEFAULT))
        near_mhz = params.get('near_mhz')
        near_mhz = float(near_mhz) if near_mhz not in (None, '') else None
        exclude_ip = params.get('exclude_ip')

        existing = _active_device_frequencies(exclude_ip=exclude_ip) + extra
        suggested = spectrum_planner.suggest_frequencies(
            region, occupied, existing, count,
            min_spacing_mhz=min_spacing_mhz, im_margin_khz=im_margin_khz,
            extra_excluded_ranges=scan_exclusions, near_mhz=near_mhz)
        self.write(json.dumps({'suggested_mhz': suggested}))

class ScanExclusionsHandler(RequestHandler):
    """Turns raw scan data (from a device's own /scan or an imported WSM
    scan) into excluded frequency ranges, same "exclusion generation" +
    "threshold calculation" step WWB runs on its own scan data."""
    def post(self):
        params = json.loads(self.request.body)
        spectrum = params.get('spectrum', [])
        try:
            threshold = float(params.get('threshold'))
        except (TypeError, ValueError):
            self.set_status(400)
            self.write(json.dumps({'error': 'threshold must be a number'}))
            return
        ranges = spectrum_planner.exclusions_from_scan(spectrum, threshold)
        self.write(json.dumps({'excluded_ranges': ranges}))

_last_webhook_sent = {}
WEBHOOK_COOLDOWN_SECONDS = 60

async def _poll_one_device(ip, dev):
    try:
        await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, dev.poll)
        log_metrics(ip, dev.metrics)
        batt = dev.metrics.get('batt')
        if batt is not None and batt < 15 and time.time() - _last_webhook_sent.get(ip, 0) > WEBHOOK_COOLDOWN_SECONDS:
            send_webhook(f"CRITICAL LOW BATTERY: {ip} is at {batt}%")
            _last_webhook_sent[ip] = time.time()
    except Exception as e:
        print(f"poll_devices: {ip} failed: {e}")

async def poll_devices():
    """Polls every device in parallel on DEVICE_EXECUTOR instead of one at
    a time on the IOLoop thread -- with 16+ real devices each taking up to
    ~0.4s of blocking socket I/O, sequential polling meant a full cycle
    could take several seconds, during which the entire server (including
    any command waiting on a hardware confirmation) was stalled."""
    if Devices:
        await asyncio.gather(*(_poll_one_device(ip, dev) for ip, dev in list(Devices.items())))
    IOLoop.current().call_later(1, lambda: IOLoop.current().spawn_callback(poll_devices))

def load_config():
    if not os.path.exists(CONFIG_PATH): return []
    with open(CONFIG_PATH, 'r') as f:
        return json.load(f).get('devices', [])

def save_config(device_list):
    with open(CONFIG_PATH, 'w') as f:
        json.dump({'devices': device_list}, f, indent=2)

def save_device_to_config(ip, brand, dtype, name='', channel=1):
    devices = [d for d in load_config() if d.get('ip') != ip]
    devices.append({'ip': ip, 'brand': brand, 'type': dtype, 'name': name, 'channel': channel})
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

def update_device_frequency_in_config(ip, freq):
    devices = load_config()
    for d in devices:
        if d.get('ip') == ip:
            d['frequency_mhz'] = freq
    save_config(devices)

def update_device_assigned_user_in_config(ip, assigned_user):
    devices = load_config()
    for d in devices:
        if d.get('ip') == ip:
            d['assigned_user'] = assigned_user
    save_config(devices)

def update_device_card_size_in_config(ip, size):
    devices = load_config()
    for d in devices:
        if d.get('ip') == ip:
            d['card_size'] = size
    save_config(devices)

def update_device_card_order_in_config(ip, order):
    devices = load_config()
    for d in devices:
        if d.get('ip') == ip:
            d['card_order'] = order
    save_config(devices)

def update_device_photo_in_config(ip, photo_url):
    devices = load_config()
    for d in devices:
        if d.get('ip') == ip:
            d['photo'] = photo_url
    save_config(devices)

def main():
    init_db()
    register_installation()
    
    app = Application([
        (r'/', IndexHandler),
        (r'/user', UserIndexHandler),
        (r'/data', DataHandler),
        (r'/analytics', AnalyticsHandler),
        (r'/command', CommandHandler),
        (r'/scan', ScanHandler),
        (r'/stream', StreamHandler),
        (r'/system/update', UpdateHandler),
        (r'/devices', DeviceHandler),
        (r'/devices/rename', RenameHandler),
        (r'/devices/assign-user', AssignUserHandler),
        (r'/devices/card-size', CardSizeHandler),
        (r'/devices/card-order', CardOrderHandler),
        (r'/devices/photo', PhotoHandler),
        (r'/devices/frequency', FrequencyHandler),
        (r'/discover', DiscoverHandler),
        (r'/discover/auto', AutoDiscoveryToggleHandler),
        (r'/import', ImportHandler),
        (r'/regions', RegionsHandler),
        (r'/coordination/check', CoordinationCheckHandler),
        (r'/coordination/suggest', CoordinationSuggestHandler),
        (r'/coordination/scan-exclusions', ScanExclusionsHandler),
        (r'/static/(.*)', StaticHandler),
    ])
    app.listen(9000, address='0.0.0.0')
    device_list = load_config()
    for idx, dev_cfg in enumerate(device_list):
        ip = dev_cfg['ip']
        brand = dev_cfg.get('brand', 'shure')
        dtype = dev_cfg.get('type', 'axtd')
        channel = dev_cfg.get('channel', 1)
        Devices[ip] = make_provider(ip, brand, dtype, channel)
        Devices[ip].connect()
        Devices[ip].photo = dev_cfg.get('photo')
        DeviceNames[ip] = dev_cfg.get('name', '')
        DeviceAssignedUsers[ip] = dev_cfg.get('assigned_user', '')
        DeviceLayout[ip] = {
            'size': dev_cfg.get('card_size', 'md'),
            'order': dev_cfg.get('card_order', idx),
        }
        if dev_cfg.get('frequency_mhz') is not None:
            DeviceFrequencies[ip] = dev_cfg['frequency_mhz']

    IOLoop.current().spawn_callback(poll_devices)
    IOLoop.current().spawn_callback(auto_discovery_tick)
    print(f"OmniWave OS v{VERSION} running on 0.0.0.0:9000...")
    IOLoop.current().start()

if __name__ == '__main__':
    main()
