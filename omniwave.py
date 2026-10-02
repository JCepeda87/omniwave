
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
import threading
import tornado.websocket
from concurrent.futures import ThreadPoolExecutor
from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler
from providers import (ShureProvider, SennheiserProvider, UHFRProvider, PSM1000Provider,
                       SLXDProvider, AxientDigitalProvider, MXWProvider, SennheiserSSCProvider,
                       SennheiserG4Provider, NoNetworkProvider, freq_in_ranges)
import aes67
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

# A multi-channel receiver (ULXD4Q, UR4D, a multi-channel PSM1000 P10T, ...)
# is one IP with several independent channels, each carrying a different
# mic/pack. Every dict below is keyed not by bare IP but by device_key(ip,
# channel) -- "ip:channel" -- so each channel gets its own provider
# instance, socket connection, name, frequency, and User Board card,
# instead of the old one-channel-per-IP limitation.
def device_key(ip, channel):
    return f'{ip}:{channel}'

Devices = {}
DeviceNames = {}  # key -> unit label (e.g. "VOX 1", "ULXD4Q-1") -- fixed at add time, not user-renamed
DeviceAssignedUsers = {}  # key -> name of the person currently using the device (User Board editable)
DeviceFrequencies = {}  # key -> assigned carrier frequency in MHz
DeviceLayout = {}  # key -> {'size': 'sm'|'md'|'lg', 'order': int} -- User Board card layout, admin-only
DeviceListenStreams = {}  # key -> AES67 stream dict (multicast_addr/port/payload_type/encoding/sample_rate/channels)

# One RTPReceiver per device key, created lazily on first "Listen" click and
# torn down when the last listener leaves (see aes67.RTPReceiver) -- kept
# here rather than on the provider itself since it's a continuous media
# stream, not a control-plane concern like everything in providers.py.
_rtp_receivers = {}
_rtp_receivers_lock = threading.Lock()
_sap_listener = aes67.SAPListener()

def _get_or_create_receiver(key, stream):
    with _rtp_receivers_lock:
        receiver = _rtp_receivers.get(key)
        if receiver is None or receiver.stream != stream:
            receiver = aes67.RTPReceiver(stream)
            _rtp_receivers[key] = receiver
        return receiver
CARD_SIZES = {'sm', 'md', 'lg'}
DB_PATH = 'omniwave_history.db'

# dtype tags matching the "type" picker in static/index.html's DEVICE_MODELS.
# Models not listed here (ulxd/qlxd/uhf-r/psm1000/custom, and every
# Sennheiser type not in the sets below) keep routing to the original
# generic providers -- either because that's already correct (ULX-D/QLX-D/
# UHF-R/PSM1000 are hardware-verified), or because the model is ambiguous
# or wasn't researched deeply enough here to implement confidently
# (Digital 9000/6000, Spectera, SpeechLine DW, 2000 Series).
SHURE_NO_NETWORK_TYPES = {'glxd-plus', 'blx', 'psm900', 'psm300'}
SENNHEISER_SSC_TYPES = {'ewdx', 'ewd', 'digital9000', 'digital6000', 'spectera'}
SENNHEISER_NO_NETWORK_TYPES = {'xswd', 'xsw-iem', 'avx'}
# ew G4 stationary units (EM receiver / SR IEM transmitter) speak the
# ASCII "Media control protocol" on UDP port 53212 -- a completely
# different wire protocol from SSC above, confirmed via Sennheiser's own
# TI 1254 spec and identify_device()'s live probe below. EM vs SR is
# determined for real from which commands the unit actually accepts
# (see identify_device()), not guessed from a model string.
SENNHEISER_G4_TYPES = {'sennheiser-g4-em', 'sennheiser-g4-sr'}

# A receiver picks up RF from a body-worn mic/instrument transmitter and
# hands off clean audio (ULX-D, QLX-D, SLX-D, Axient Digital, UHF-R, MXW,
# every SSC type, and the G4 EM -- SennheiserSSCProvider/SennheiserG4Provider
# specifically model the receiver side of their protocols). A transmitter is
# the opposite: an IEM ("in-ear monitor") base station that sends an audio
# mix out to a body-worn receiver pack -- same rack-mounted shape, opposite
# signal direction, and for PSM1000 specifically, a genuinely different
# (one-way, unqueryable) protocol as a result. This distinction is also why
# some devices below can never report a frequency -- see
# providers.py's freq_unavailable_reason.
TRANSMITTER_TYPES = {'psm1000', 'psm900', 'psm300', 'xsw-iem', 'sennheiser-g4-sr'}

def device_role(dtype):
    return 'transmitter' if dtype in TRANSMITTER_TYPES else 'receiver'

def make_provider(ip, brand, dtype, channel=1):
    dev = _make_provider_instance(ip, brand, dtype, channel)
    # Stored directly rather than derived from isinstance() -- DataHandler
    # used to infer brand from a hardcoded isinstance() tuple that predated
    # SLXDProvider/AxientDigitalProvider/MXWProvider/NoNetworkProvider, so
    # those would misreport as Sennheiser. This is the actual source of
    # truth, set once at construction from what the caller asked for.
    dev.brand = brand
    dev.role = device_role(dtype)
    return dev

def _make_provider_instance(ip, brand, dtype, channel=1):
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
    if dtype in SENNHEISER_G4_TYPES:
        return SennheiserG4Provider(ip, dtype, channel=channel)
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
        for key, dev in Devices.items():
            entry = dev.get_json()
            entry['name'] = DeviceNames.get(key, '')
            entry['assigned_user'] = DeviceAssignedUsers.get(key, '')
            layout = DeviceLayout.get(key, {})
            entry['card_size'] = layout.get('size', 'md')
            entry['card_order'] = layout.get('order', 0)
            entry['visible_on_board'] = layout.get('visible', True)
            entry['brand'] = getattr(dev, 'brand', 'shure')
            # Prefer the live, hardware-reported frequency (real Shure gear
            # reports this every poll) over the locally-assigned one, so the
            # UI reflects what's actually on air rather than stale bookkeeping.
            entry['frequency_mhz'] = dev.metrics.get('frequency_mhz', DeviceFrequencies.get(key))
            entry['listen_stream'] = DeviceListenStreams.get(key)
            data[key] = entry
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
        key = self.get_argument('ip', None)  # device_key(ip, channel); "ip" kept as the wire/column name
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        if key:
            cursor.execute('SELECT * FROM (SELECT * FROM metrics WHERE ip=? ORDER BY timestamp DESC LIMIT 50)', (key,))
        else:
            cursor.execute('SELECT * FROM metrics ORDER BY timestamp DESC LIMIT 50')
        data = cursor.fetchall()
        conn.close()
        self.write(json.dumps(data))

class CommandHandler(RequestHandler):
    async def post(self):
        params = json.loads(self.request.body)
        key = params.get('ip')
        cmd = params.get('command')
        val = params.get('value')
        if key in Devices:
            dev = Devices[key]
            # The channel to target is whatever this specific provider
            # instance was created for -- not a value the frontend has to
            # track and resend, which could drift out of sync with which
            # channel this key actually addresses.
            success = await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, dev.send_command, cmd, val, dev.channel)
            self.write(json.dumps({'success': success}))
        else: self.set_status(404)

class ScanHandler(RequestHandler):
    async def get(self):
        key = self.get_argument('ip', None)
        if key and key in Devices:
            dev = Devices[key]
            await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, dev.scan_rf)
            self.write(json.dumps({'spectrum': dev.spectrum_data}))
        else: self.set_status(404)

class ListenHandler(tornado.websocket.WebSocketHandler):
    """Real-time "listen" over AES67: one WebSocket connection per browser
    tab listening to one device. Streams decoded 16-bit PCM as binary
    frames for the browser's WebAudio side to schedule and play in real
    time -- see aes67.py for the actual multicast RTP receive, which only
    runs while at least one listener (across all tabs) is attached."""
    def open(self, key):
        self.device_key = key
        self._receiver = None
        stream = DeviceListenStreams.get(key)
        if not stream:
            self.close(code=4404, reason='No listen stream configured for this device')
            return
        receiver = _get_or_create_receiver(key, stream)
        # Capture the IOLoop here, on the IOLoop's own thread (open() runs
        # on it) -- the callback below fires from RTPReceiver's background
        # receive thread, where IOLoop.current() would resolve against the
        # wrong thread (no event loop there at all) instead of this one.
        loop = IOLoop.current()
        self._callback = lambda pcm: loop.add_callback(self._send_pcm, pcm)
        if not receiver.add_listener(self._callback):
            self.close(code=4500, reason=receiver.last_error or 'Could not join audio stream')
            return
        self._receiver = receiver

    def _send_pcm(self, pcm):
        if self.ws_connection is None:
            return
        try:
            self.write_message(pcm, binary=True)
        except tornado.websocket.WebSocketClosedError:
            pass

    def on_close(self):
        if self._receiver:
            self._receiver.remove_listener(self._callback)

    def check_origin(self, origin):
        # Local venue tool with no auth on any other endpoint either --
        # same trust model as the rest of this app.
        return True

class ListenStreamHandler(RequestHandler):
    """Manual fallback for a device's AES67 listen-stream config (multicast
    address/port/payload format) -- for when SAP/SSC auto-discovery isn't
    available. Confirmed necessary this session: venue network policy can
    block multicast/unicast traffic to a device even when it's otherwise
    healthy and correctly configured (see the EM 6000 investigation)."""
    def post(self):
        params = json.loads(self.request.body)
        key = params.get('ip')  # device_key(ip, channel)
        if not key or key not in Devices:
            self.set_status(404)
            return
        multicast_addr = (params.get('multicast_addr') or '').strip()
        encoding = (params.get('encoding') or 'L16').strip().upper()
        try:
            port = int(params.get('port'))
            sample_rate = int(params.get('sample_rate') or 48000)
            channels = int(params.get('channels') or 1)
            payload_type = int(params.get('payload_type') or 97)
        except (TypeError, ValueError):
            self.set_status(400)
            self.write(json.dumps({'error': 'port/sample_rate/channels/payload_type must be numbers'}))
            return
        if not multicast_addr or encoding not in ('L16', 'L24'):
            self.set_status(400)
            self.write(json.dumps({'error': 'multicast_addr is required and encoding must be L16 or L24'}))
            return
        stream = {
            'multicast_addr': multicast_addr, 'port': port, 'payload_type': payload_type,
            'encoding': encoding, 'sample_rate': sample_rate, 'channels': channels,
        }
        DeviceListenStreams[key] = stream
        dev = Devices[key]
        update_device_listen_stream_in_config(dev.ip, dev.channel, stream)
        self.write(json.dumps({'success': True, 'ip': key, 'stream': stream}))

    def delete(self):
        key = self.get_argument('ip', None)
        if not key or key not in Devices:
            self.set_status(404)
            return
        DeviceListenStreams.pop(key, None)
        dev = Devices[key]
        update_device_listen_stream_in_config(dev.ip, dev.channel, None)
        self.write(json.dumps({'success': True}))

class SapStreamsHandler(RequestHandler):
    """Streams discovered passively via SAP (see aes67.SAPListener) -- lets
    the "Configure Listen Stream" UI offer real detected candidates instead
    of pure manual entry, when SAP multicast happens to be reachable."""
    def get(self):
        self.write(json.dumps({'streams': _sap_listener.streams}))

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

async def register_device_channels(ip, brand, dtype, name):
    """Discovers this unit's real channels (see discover_channels()) and
    creates one connected provider instance per channel, each under its own
    device_key and persisted to config.json as a separate entry -- shared by
    the manual Add Device flow and background auto-discovery so a ULXD4Q or
    multi-channel PSM1000 shows up as N independent devices, not one."""
    channels = await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, discover_channels, ip, brand, dtype)
    added = []
    for channel in channels:
        key = device_key(ip, channel)
        dev = make_provider(ip, brand, dtype, channel)
        await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, dev.connect)
        Devices[key] = dev
        DeviceNames[key] = name
        DeviceLayout.setdefault(key, {'size': 'md', 'order': len(Devices)})
        save_device_to_config(ip, brand, dtype, name, channel)
        added.append({'ip': ip, 'channel': channel, 'key': key, 'status': dev.status})
    return added

class DeviceHandler(RequestHandler):
    """Add or remove a mic/wireless-system IP at runtime, persisted to
    config.json so it's still there on the next restart."""
    async def post(self):
        params = json.loads(self.request.body)
        ip = params.get('ip')
        brand = params.get('brand', 'shure')
        dtype = params.get('type', 'axtd')
        name = (params.get('name') or '').strip()
        if not ip:
            self.set_status(400)
            self.write(json.dumps({'error': 'ip is required'}))
            return

        added = await register_device_channels(ip, brand, dtype, name)
        self.write(json.dumps({
            'success': True, 'ip': ip, 'channels': [a['channel'] for a in added], 'devices': added,
        }))

    async def delete(self):
        key = self.get_argument('ip', None)  # device_key(ip, channel)
        if key and key in Devices:
            dev = Devices[key]
            await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, dev.disconnect)
            del Devices[key]
            DeviceNames.pop(key, None)
            DeviceAssignedUsers.pop(key, None)
            DeviceFrequencies.pop(key, None)
            DeviceLayout.pop(key, None)
            remove_device_from_config(dev.ip, dev.channel)
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
        key = params.get('ip')  # device_key(ip, channel)
        name = (params.get('name') or '').strip()
        if not key or key not in Devices:
            self.set_status(404)
            return
        DeviceNames[key] = name
        dev = Devices[key]
        update_device_name_in_config(dev.ip, dev.channel, name)
        self.write(json.dumps({'success': True, 'ip': key, 'name': name}))

class AssignUserHandler(RequestHandler):
    """Sets who's currently using a device (shown over the photo on the User
    Board) -- separate from the device's own unit name/label, which stays
    fixed here."""
    def post(self):
        params = json.loads(self.request.body)
        key = params.get('ip')  # device_key(ip, channel)
        assigned_user = (params.get('assigned_user') or '').strip()
        if not key or key not in Devices:
            self.set_status(404)
            return
        DeviceAssignedUsers[key] = assigned_user
        dev = Devices[key]
        update_device_assigned_user_in_config(dev.ip, dev.channel, assigned_user)
        self.write(json.dumps({'success': True, 'ip': key, 'assigned_user': assigned_user}))

class CardSizeHandler(RequestHandler):
    """Admin-only: sets a device's card size on the User Board (sm/md/lg)."""
    def post(self):
        params = json.loads(self.request.body)
        key = params.get('ip')  # device_key(ip, channel)
        size = params.get('size')
        if not key or key not in Devices:
            self.set_status(404)
            return
        if size not in CARD_SIZES:
            self.set_status(400)
            self.write(json.dumps({'error': f'size must be one of {sorted(CARD_SIZES)}'}))
            return
        DeviceLayout.setdefault(key, {})['size'] = size
        dev = Devices[key]
        update_device_card_size_in_config(dev.ip, dev.channel, size)
        self.write(json.dumps({'success': True, 'ip': key, 'size': size}))

class CardVisibilityHandler(RequestHandler):
    """Admin-only: shows/hides a device's card on the User Board without
    removing it from the admin dashboard or disconnecting it -- e.g. a spare
    receiver kept online for RF planning but not worn tonight, or a channel
    the stage crew doesn't need to see."""
    def post(self):
        params = json.loads(self.request.body)
        key = params.get('ip')  # device_key(ip, channel)
        visible = params.get('visible')
        if not key or key not in Devices:
            self.set_status(404)
            return
        if not isinstance(visible, bool):
            self.set_status(400)
            self.write(json.dumps({'error': 'visible must be a boolean'}))
            return
        DeviceLayout.setdefault(key, {})['visible'] = visible
        dev = Devices[key]
        update_device_visibility_in_config(dev.ip, dev.channel, visible)
        self.write(json.dumps({'success': True, 'ip': key, 'visible': visible}))

class CardOrderHandler(RequestHandler):
    """Admin-only: persists the full User Board card order after a
    drag-and-drop reorder -- the client sends the complete ordered device-key
    list rather than a single move, which is simpler and more robust than
    reconciling pairwise position swaps."""
    def post(self):
        params = json.loads(self.request.body)
        order = params.get('order', [])
        for idx, key in enumerate(order):
            if key in Devices:
                DeviceLayout.setdefault(key, {})['order'] = idx
                dev = Devices[key]
                update_device_card_order_in_config(dev.ip, dev.channel, idx)
        self.write(json.dumps({'success': True}))

PHOTO_EXTENSIONS = {'image/jpeg': '.jpg', 'image/png': '.png', 'image/webp': '.webp', 'image/gif': '.gif'}

def _photo_filename(key, ext):
    return f"{key.replace('.', '_').replace(':', '-')}{ext}"

class PhotoHandler(RequestHandler):
    """Attach/replace or remove the photo shown for a device on the user-facing
    board (who's wearing this mic) -- stored on disk under PHOTO_DIR and
    persisted to config.json like name/frequency so it survives a restart."""
    def post(self):
        key = self.get_body_argument('ip', None)  # device_key(ip, channel)
        if not key or key not in Devices:
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
            stale = os.path.join(PHOTO_DIR, _photo_filename(key, existing_ext))
            if os.path.isfile(stale):
                os.remove(stale)
        filename = _photo_filename(key, ext)
        with open(os.path.join(PHOTO_DIR, filename), 'wb') as f:
            f.write(file_info['body'])

        photo_url = f"/static/photos/{filename}"
        dev = Devices[key]
        dev.photo = photo_url
        update_device_photo_in_config(dev.ip, dev.channel, photo_url)
        self.write(json.dumps({'success': True, 'ip': key, 'photo': photo_url}))

    def delete(self):
        key = self.get_argument('ip', None)  # device_key(ip, channel)
        if not key or key not in Devices:
            self.set_status(404)
            return
        for ext in PHOTO_EXTENSIONS.values():
            stale = os.path.join(PHOTO_DIR, _photo_filename(key, ext))
            if os.path.isfile(stale):
                os.remove(stale)
        dev = Devices[key]
        dev.photo = None
        update_device_photo_in_config(dev.ip, dev.channel, None)
        self.write(json.dumps({'success': True, 'ip': key}))

class FrequencyHandler(RequestHandler):
    """Assign a carrier frequency (MHz) to a device and, for a connected
    device, actually push it to the hardware (SET FREQUENCY) -- this is the
    real "Assign & Deploy" step, not just local bookkeeping."""
    async def post(self):
        params = json.loads(self.request.body)
        key = params.get('ip')  # device_key(ip, channel)
        freq = params.get('frequency_mhz')
        if not key or key not in Devices:
            self.set_status(404)
            return
        try:
            freq = round(float(freq), 4) if freq not in (None, '') else None
        except (TypeError, ValueError):
            self.set_status(400)
            self.write(json.dumps({'error': 'frequency_mhz must be a number'}))
            return

        dev = Devices[key]
        # Refuse outright -- don't even save it as a "planning" value --
        # when this specific unit's real tunable range is known (discovered
        # live from the hardware itself; see providers.py's RF_BAND/
        # frequency_ranges queries) and the target falls outside every
        # sub-range. This is a hardware-capability fact, not a connectivity
        # one, so it holds regardless of whether the device is online right
        # now -- unlike "not deployed", this can't be fixed by reconnecting.
        if freq is not None and dev.rf_range_mhz and not freq_in_ranges(freq, dev.rf_range_mhz):
            ranges_str = ', '.join(f'{lo:.3f}-{hi:.3f}' for lo, hi in dev.rf_range_mhz)
            band_str = f' (band {dev.rf_band})' if dev.rf_band else ''
            self.set_status(400)
            self.write(json.dumps({
                'error': f'{freq} MHz is outside this unit\'s tunable range{band_str}: {ranges_str} MHz',
            }))
            return

        if freq is None:
            DeviceFrequencies.pop(key, None)
        else:
            DeviceFrequencies[key] = freq
        update_device_frequency_in_config(dev.ip, dev.channel, freq)

        deployed = False
        deploy_error = None
        if freq is not None and dev.status == 'CONNECTED':
            deployed = await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, dev.send_command, 'FREQUENCY', freq, dev.channel)
            if not deployed:
                deploy_error = dev.last_command_error
        self.write(json.dumps({
            'success': True, 'ip': key, 'frequency_mhz': freq, 'deployed': deployed, 'deploy_error': deploy_error,
        }))

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

def _parse_ifconfig_subnets(output):
    """Pure parsing, no I/O -- split out so it's testable without a live
    `ifconfig` call. Walks macOS ifconfig's block-per-interface text
    format:
        en0: flags=8863<...> mtu 1500
            inet 10.60.2.63 netmask 0xffff0000 broadcast 10.60.255.255
    A new interface block starts at a non-indented line; everything below
    it until the next one is that interface's own lines (ether/inet6/inet/
    media/status/...). Only 'inet' (IPv4) lines matter here -- 'inet6' is
    skipped explicitly since both start with the same 4 characters."""
    subnets = {}
    in_block = False
    for line in output.splitlines():
        if line and not line[0].isspace():
            in_block = True
            continue
        if not in_block:
            continue
        stripped = line.strip()
        if not stripped.startswith('inet ') or stripped.startswith('inet6'):
            continue
        parts = stripped.split()
        # "inet <ip> netmask <hex> [broadcast <ip>]" -- position-based,
        # matching ifconfig's own fixed field order.
        if len(parts) < 4 or parts[2] != 'netmask':
            continue
        ip = parts[1]
        try:
            netmask = str(ipaddress.IPv4Address(int(parts[3], 16)))
            network = ipaddress.ip_network(f'{ip}/{netmask}', strict=False)
        except ValueError:
            continue
        if ip.startswith('127.'):
            continue
        # Skip point-to-point /32s (e.g. some VPN adapters) -- nothing to
        # scan on a network with exactly one address.
        if network.num_addresses <= 1:
            continue
        # Cap scan size: a large corporate/venue network can report a /16
        # or bigger, which would mean tens of thousands of probes -- narrow
        # it to the /24 containing this machine's own address, matching the
        # scope this app has always actually scanned per subnet. Anything
        # already /24 or smaller (like a link-local 169.254.x.x/24 segment)
        # is kept at its real, exact size.
        if network.prefixlen < 24:
            network = ipaddress.ip_network(f'{ip}/24', strict=False)
        subnets[str(network)] = network
    return list(subnets.values())

def get_local_subnets():
    """Every active, non-loopback IPv4 /24-or-larger subnet this machine
    currently has an address on -- not just whichever interface happens to
    win the route to the public internet.

    The original version picked one subnet via a "connect a UDP socket to
    8.8.8.8 and see which local address gets used" trick, which reflects
    only the default-route interface. A laptop can easily be on several
    real network segments at once -- a venue control network, a VPN, and a
    device plugged in directly with no DHCP server (which self-assigns a
    169.254.0.0/16 link-local address, RFC 3927) -- and only scanning the
    default-route one silently misses devices on the others.

    Parses `ifconfig` directly instead of using the `netifaces` package
    that used to live here: netifaces is a compiled C extension pinned to
    one specific Python build's ABI, which this session hit real, repeated
    friction from -- it needs network access and a working compiler to
    install, and a copy already installed for one interpreter silently
    can't be imported by a different one (exactly what happened switching
    this app's dev-server interpreter earlier this session). Shelling out
    to `ifconfig` -- already macOS-only tooling, like this app's `dns-sd`
    usage elsewhere -- has no such dependency at all."""
    try:
        output = subprocess.run(['ifconfig'], capture_output=True, text=True, timeout=3).stdout
    except (OSError, subprocess.TimeoutExpired):
        return []
    return _parse_ifconfig_subnets(output)

def probe_host(ip, port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(DISCOVERY_TIMEOUT)
            return s.connect_ex((ip, port)) == 0
    except OSError:
        return False

def scan_subnet():
    """Blocking; call via an executor so it doesn't stall the IOLoop."""
    hosts = [h for network in get_local_subnets() for h in network.hosts()]
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

def probe_host_udp_g4(ip):
    """ew G4 stationary units also only respond over UDP (port 53212, ASCII
    Media control protocol -- see identify_device()) -- same reasoning as
    the UHF-R probe above, just a different port/protocol."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(DISCOVERY_TIMEOUT)
            s.sendto(b'FirmwareRevision\r', (ip, 53212))
            s.recvfrom(4096)
        return True
    except OSError:
        return False

def probe_host_udp_ssc(ip):
    """Sennheiser SSC's mandatory transport is UDP (port 45) -- TCP is an
    optional extra some product lines layer on top, and Digital 6000
    (EM 6000/L 6000) implements ONLY UDP (confirmed live: a real EM 6000
    never answered scan_subnet()'s TCP-45 probe at all). Without this,
    scan_subnet()'s TCP-only check silently drops every UDP-only SSC unit."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(DISCOVERY_TIMEOUT)
            s.sendto(b'{"device":{"identity":{"product":null}}}', (ip, 45))
            s.recvfrom(4096)
        return True
    except OSError:
        return False

def scan_subnet_udp():
    """Blocking; call via an executor. Finds UDP-only hosts (UHF-R, ew G4,
    Sennheiser SSC/Digital 6000) that scan_subnet()'s TCP probe can't see."""
    hosts = [h for network in get_local_subnets() for h in network.hosts()]
    found = []
    with ThreadPoolExecutor(max_workers=DISCOVERY_MAX_WORKERS) as pool:
        futures = {}
        for host in hosts:
            futures[pool.submit(probe_host_udp_uhfr, str(host))] = (str(host), 'shure', 2202)
            futures[pool.submit(probe_host_udp_g4, str(host))] = (str(host), 'sennheiser', 53212)
            futures[pool.submit(probe_host_udp_ssc, str(host))] = (str(host), 'sennheiser', 45)
        for future in futures:
            if future.result():
                ip, brand, port = futures[future]
                found.append({'ip': ip, 'brand': brand, 'port': port})
    return found


# mDNS/Bonjour discovery -- a second, fundamentally different discovery path
# from every probe_host_* function above. Those all actively send a unicast
# packet straight at one specific IP and wait for a reply, which is exactly
# the traffic a locked-down venue network can (and, we confirmed live this
# session against a real EM 6000, sometimes does) silently drop for a client
# it doesn't recognize -- while the device itself is completely healthy and
# a different, already-trusted control PC on the same network reaches it
# fine. Sennheiser's own SSC spec makes this exact scenario recoverable:
# "Networked devices implement DNS-SD (Apple Bonjour) as discovery
# protocol... MUST all publish a DNS-SD service under '_ssc._udp'". DNS-SD
# runs over IP multicast, which many networks treat as "ambient discovery
# noise" and allow through even where they lock down unicast between hosts
# -- so a unit invisible to every unicast probe above can still announce
# itself this way. Uses macOS's built-in `dns-sd` CLI (this app only runs
# on macOS per its existing tooling); on any other OS these simply find
# nothing and every probe_host_* path above still applies unaffected.
MDNS_BROWSE_TIMEOUT = 2.0
MDNS_RESOLVE_TIMEOUT = 1.5
MDNS_RESOLVE_RE = re.compile(r'can be reached at\s+(\S+?)\.?:(\d+)')

def _dns_sd_run(args, timeout):
    """`dns-sd -B`/`-L` browse/resolve forever until killed -- there's no
    "done" signal, so this always runs for the full timeout and reads
    whatever was captured up to that point (subprocess still hands back
    stdout captured so far on a TimeoutExpired). Every real call takes this
    TimeoutExpired path, and on this Python version `e.stdout` comes back
    as bytes there even with text=True passed to run() -- confirmed live
    (a real bug in this interpreter, not a hypothetical) -- so this decodes
    explicitly rather than trusting text=True to have applied."""
    try:
        proc = subprocess.run(['dns-sd'] + args, timeout=timeout, capture_output=True, text=True)
        out = proc.stdout
    except subprocess.TimeoutExpired as e:
        out = e.stdout or ''
    except FileNotFoundError:
        return ''
    return out.decode('utf-8', errors='ignore') if isinstance(out, bytes) else out

def _dns_sd_browse(service_type):
    output = _dns_sd_run(['-B', service_type, 'local.'], MDNS_BROWSE_TIMEOUT)
    instances = []
    for line in output.splitlines():
        parts = line.split(None, 6)
        if len(parts) == 7 and parts[1] == 'Add':
            instances.append(parts[6])
    return instances

def _dns_sd_resolve_ip(instance_name, service_type):
    output = _dns_sd_run(['-L', instance_name, service_type, 'local.'], MDNS_RESOLVE_TIMEOUT)
    m = MDNS_RESOLVE_RE.search(output)
    if not m:
        return None
    host = m.group(1)
    try:
        return socket.gethostbyname(host if host.endswith('.local') else host + '.local')
    except OSError:
        return None

def mdns_discover_sennheiser_ips():
    """Blocking; call via an executor. Real DNS-SD service discovery (not a
    guess) for the two service types Sennheiser's spec mandates SSC devices
    publish -- see module comment above."""
    service_types = ('_ssc._udp', '_ssc._tcp')
    ips = set()
    # Browse each service type in parallel, then resolve every found
    # instance in parallel too -- a resolve can take up to
    # MDNS_RESOLVE_TIMEOUT each, and there can be several instances.
    with ThreadPoolExecutor(max_workers=len(service_types)) as pool:
        browse_results = dict(zip(service_types, pool.map(_dns_sd_browse, service_types)))
    resolve_tasks = [(instance, st) for st, instances in browse_results.items() for instance in instances]
    if resolve_tasks:
        with ThreadPoolExecutor(max_workers=max(4, len(resolve_tasks))) as pool:
            for ip in pool.map(lambda t: _dns_sd_resolve_ip(*t), resolve_tasks):
                if ip:
                    ips.add(ip)
    return ips

def scan_subnet_all():
    """Blocking; call via an executor. TCP + UDP port-probe candidates plus
    mDNS/Bonjour-discovered ones (see mdns_discover_sennheiser_ips() above),
    deduplicated by IP -- an explicit port-probe result wins if a host
    somehow shows up in both."""
    combined = {}
    for ip in mdns_discover_sennheiser_ips():
        combined[ip] = {'ip': ip, 'brand': 'sennheiser', 'port': None}
    for c in scan_subnet_udp() + scan_subnet():
        combined[c['ip']] = c
    return list(combined.values())

class DiscoverHandler(RequestHandler):
    async def get(self):
        candidates = await IOLoop.current().run_in_executor(None, run_discovery_scan)
        self.write(json.dumps({'candidates': candidates}))

class ProbeHandler(RequestHandler):
    """Identify exactly one IP directly -- bypasses get_local_subnets()
    entirely, so it isn't limited to interface-attached subnets, the /24
    scan-size cap, or whatever this machine's own address happens to be on
    right now. The venues this app runs at don't stay on one network: a
    laptop hops between a venue's main LAN, an isolated link-local segment
    for a single unit plugged in directly, and VPN tunnels, sometimes
    mid-session (an adapter can pick up a totally different subnet between
    one scan and the next -- see get_local_subnets()'s own docstring). A
    device the user can read an IP off of (its own front-panel display, a
    network menu, a label) should always be reachable this way as long as
    the OS can route to it at all, whether or not that subnet was ever
    auto-detected."""
    async def post(self):
        params = json.loads(self.request.body)
        ip = (params.get('ip') or '').strip()
        if not ip:
            self.set_status(400)
            self.write(json.dumps({'error': 'ip is required'}))
            return
        try:
            ipaddress.ip_address(ip)
        except ValueError:
            self.set_status(400)
            self.write(json.dumps({'error': f'"{ip}" is not a valid IP address'}))
            return
        if _ip_already_registered(ip):
            self.write(json.dumps({'ip': ip, 'identified': False, 'already_added': True}))
            return
        info = await IOLoop.current().run_in_executor(None, identify_device, ip)
        if info:
            self.write(json.dumps({'ip': ip, 'identified': True, **info}))
        else:
            self.write(json.dumps({
                'ip': ip, 'identified': False,
                'error': "No response on any known port/protocol -- if the device is alive (try pinging it), it may not have network control at all, or it's speaking a protocol this app doesn't know yet.",
            }))

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

    # Sennheiser SSC (Sound Control Protocol): JSON, port 45. A real GET for
    # the device's model string, not just "is the port open" --
    # https://docs.cloud.sennheiser.com/en-us/control-cockpit/control-cockpit/ssc-protocols.html
    # UDP is tried first and is the one that actually matters: per
    # Sennheiser's own SSC spec, every networked SSC device "MUST implement
    # the UDP/IP transport", while TCP is an optional addition some product
    # lines layer on top -- and Digital 6000 (EM 6000/L 6000) is documented
    # as supporting ONLY UDP, confirmed live against a real EM 6000 that
    # never answered on TCP 45 at all despite being alive on the network.
    # TCP is kept as a second attempt in case some line answers only there.
    ssc_query = b'{"device":{"identity":{"product":null}}}'

    def _ssc_product_to_type(product):
        product_upper = product.upper()
        if 'EW-DX' in product_upper:
            return 'ewdx'
        if 'EW-D' in product_upper:
            return 'ewd'
        if '9000' in product_upper:
            return 'digital9000'
        if '6000' in product_upper:
            return 'digital6000'
        if 'SPECTERA' in product_upper:
            return 'spectera'
        return 'ewdx'

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(IDENTIFY_TIMEOUT)
            s.connect((ip, 45))
            s.send(ssc_query)
            data, _ = s.recvfrom(4096)
        parsed = json.loads(data.decode('utf-8', errors='ignore'))
        product = parsed.get('device', {}).get('identity', {}).get('product')
        if isinstance(product, str) and product.strip():
            product = product.strip()
            return {'brand': 'sennheiser', 'type': _ssc_product_to_type(product), 'label': product, 'name': product}
    except (OSError, ValueError, AttributeError):
        pass

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(IDENTIFY_TIMEOUT)
            s.connect((ip, 45))
            s.sendall(ssc_query + b'\r\n')
            data = s.recv(4096)
        parsed = json.loads(data.decode('utf-8', errors='ignore').strip().splitlines()[0])
        product = parsed.get('device', {}).get('identity', {}).get('product')
        if isinstance(product, str) and product.strip():
            product = product.strip()
            return {'brand': 'sennheiser', 'type': _ssc_product_to_type(product), 'label': product, 'name': product}
    except (OSError, ValueError, AttributeError, IndexError):
        pass

    # Sennheiser ew G4 "Media control protocol": plain ASCII over UDP port
    # 53212 (same port for send+receive), NOT the SSC/port-45 protocol
    # probed above -- G4 stationary EM (receiver) / SR (IEM transmitter)
    # units never spoke SSC at all. A one-off get command (no Push
    # subscription needed, per the spec) confirms the device is really
    # there; a second real command (Squelch, EM-only) tells EM and SR
    # apart for real instead of guessing -- EM echoes back its squelch
    # value, SR rejects it with error 1000 "Invalid command" since Squelch
    # doesn't exist in its command set.
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(IDENTIFY_TIMEOUT)
            s.connect((ip, 53212))
            s.send(b'FirmwareRevision\r')
            data = s.recv(4096)
        text = data.decode('ascii', errors='ignore')
        if text.startswith('FirmwareRevision'):
            name = None
            is_em = False
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                    s.settimeout(IDENTIFY_TIMEOUT)
                    s.connect((ip, 53212))
                    s.send(b'Name\r')
                    name_data = s.recv(4096)
                name_text = name_data.decode('ascii', errors='ignore').strip('\r\n ')
                if name_text.startswith('Name '):
                    name = name_text[len('Name '):].strip()
            except OSError:
                pass
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                    s.settimeout(IDENTIFY_TIMEOUT)
                    s.connect((ip, 53212))
                    s.send(b'Squelch\r')
                    squelch_data = s.recv(4096)
                is_em = squelch_data.decode('ascii', errors='ignore').lstrip().startswith('Squelch')
            except OSError:
                pass
            if is_em:
                return {'brand': 'sennheiser', 'type': 'sennheiser-g4-em', 'label': 'ew G4 (EM receiver)', 'name': name}
            return {'brand': 'sennheiser', 'type': 'sennheiser-g4-sr', 'label': 'ew G4 (SR IEM transmitter)', 'name': name}
    except OSError:
        pass
    return None

# Defaults to on: this app moves between venues with this laptop, and each
# one is a different network/subnet (confirmed repeatedly this session --
# 169.254.x.x link-local segments, /16 corporate LANs, isolated production
# VLANs, and everything in between, sometimes several within one visit).
# get_local_subnets() already re-detects whatever's active on every scan
# tick, so the only thing standing between "just works on the next network"
# and "silently inert until someone remembers to flip Auto-Add back on" was
# this defaulting to off after every restart -- and restarts have been
# frequent (code changes, interpreter switches). Still toggleable off via
# the admin UI for anyone who wants manual-only scanning.
AUTO_DISCOVERY_ENABLED = True
AUTO_DISCOVERY_INTERVAL_SECONDS = 20

def _ip_already_registered(ip):
    """Devices is keyed by device_key(ip, channel), not bare ip -- so
    "already added" now means "any channel of this ip is already added",
    checked against the real ip each provider stores on itself."""
    return any(dev.ip == ip for dev in Devices.values())

# Real per-unit channel discovery for multi-channel receivers (ULXD4Q, UR4D,
# a dual PSM1000 P10T, ...): queries the device directly for which channels
# it actually has, rather than guessing a count from a model-name suffix
# (e.g. the "Q" in ULXD4Q). Falls back to a single channel [1] wherever the
# protocol doesn't support this or the device doesn't respond -- honest
# about what's genuinely knowable, same principle as identify_device().
CHANNEL_REP_RE = re.compile(r'REP\s+(\d+)\s+\S')
PSM1000_CHANNEL_RE = re.compile(r'REPORT\s+(\d+)\s+AUDIO_IN_LVL')

def discover_channels(ip, brand, dtype):
    if brand == 'shure' and dtype in ('ulxd', 'qlxd', 'slxd-plus', 'axient-digital'):
        # GET 0 ALL asks for every channel's data in one shot (0 = "all
        # channels" in this protocol family); each channel reports its own
        # REP <n> ... lines, so the distinct channel numbers seen are the
        # unit's real channel count.
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(IDENTIFY_TIMEOUT)
                s.connect((ip, 2202))
                s.sendall(b'< GET 0 ALL >')
                text = _drain_tcp(s, IDENTIFY_TIMEOUT)
            channels = {int(m.group(1)) for m in CHANNEL_REP_RE.finditer(text)} - {0}
            if channels:
                return sorted(channels)
        except OSError:
            pass
        return [1]

    if brand == 'shure' and dtype == 'uhf-r':
        # No "all channels" query confirmed for this protocol -- probe the
        # two channels UHF-R actually supports (1=single/left, 2=right on a
        # UR4D) directly instead.
        found = []
        for chan in (1, 2):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                    s.settimeout(IDENTIFY_TIMEOUT)
                    s.sendto(f'* GET {chan} CHAN_NAME *'.encode('ascii'), (ip, 2202))
                    data, _ = s.recvfrom(4096)
                if b'REPORT' in data:
                    found.append(chan)
            except OSError:
                pass
        return found or [1]

    if brand == 'shure' and dtype == 'psm1000':
        # One-way push-only protocol -- no GET/SET works at all (see
        # PSM1000Provider), so there's nothing to query. Instead, listen
        # passively for a short window and record whichever channel numbers
        # actually show up in the unit's own unsolicited REPORT messages.
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1.5)
                s.connect((ip, 2202))
                text = _drain_tcp(s, 1.5)
            channels = {int(m.group(1)) for m in PSM1000_CHANNEL_RE.finditer(text)}
            if channels:
                return sorted(channels)
        except OSError:
            pass
        return [1]

    # MXW (channel count depends on which mic slots are actually linked,
    # not just the AP's port capacity) and the Sennheiser SSC line (channel
    # discovery not yet built) aren't covered here -- rather than guess,
    # they get treated as single-channel until this is extended for real.
    return [1]

def run_discovery_scan():
    """Blocking; call via an executor. Used by the manual "Scan Network"
    button: finds live hosts and identifies each one for real via
    identify_device() (speaks the actual protocol) rather than guessing a
    model from which port merely happened to respond. A host that answers
    on a known port but can't be positively identified is still surfaced --
    just honestly labeled as unidentified -- instead of being silently
    dropped or mislabeled by port alone."""
    found = []
    for candidate in scan_subnet_all():
        ip = candidate['ip']
        if _ip_already_registered(ip):
            continue
        info = identify_device(ip)
        if info:
            found.append({'ip': ip, 'identified': True, **info})
        elif candidate['port'] is None:
            # Found only via mDNS/Bonjour self-announcement (see
            # mdns_discover_sennheiser_ips()), not by a port responding to a
            # direct query -- a real, specific pattern confirmed live this
            # session: the device genuinely publishes itself over multicast
            # but silently ignores unicast queries from this client, most
            # likely a switch/VLAN policy restricting which hosts it'll
            # actually talk to (not a device problem -- a different,
            # already-trusted PC on the same network can usually still
            # reach it fine).
            found.append({
                'ip': ip, 'identified': False, 'type': None,
                'brand': candidate['brand'],
                'label': "Announces itself via Bonjour/mDNS but isn't answering direct queries -- likely network policy blocking this computer specifically, not a device problem",
                'name': None,
            })
        else:
            found.append({
                'ip': ip, 'identified': False, 'type': None,
                'brand': candidate['brand'],
                'label': f"Unidentified device (responded on {candidate['brand']} port {candidate['port']}, couldn't confirm model)",
                'name': None,
            })
    return found

def run_auto_discovery_scan():
    """Blocking; call via an executor. Finds live hosts, identifies each one
    for real, and returns only ones not already in Devices. Unlike
    run_discovery_scan() above, unidentified hosts are dropped here rather
    than surfaced -- auto-add should never register a device under a
    guessed type with no human reviewing it first."""
    found = []
    for candidate in scan_subnet_all():
        ip = candidate['ip']
        if _ip_already_registered(ip):
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
                if _ip_already_registered(ip):  # could've been added manually mid-scan
                    continue
                name = f.get('name') or f"{f['label']} ({ip})"
                added = await register_device_channels(ip, f['brand'], f['type'], name)
                print(f"Auto-discovery: added {name} at {ip} ({len(added)} channel(s))")
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

def _active_device_frequencies(exclude_key=None):
    """Every device's best-known frequency: the live hardware reading when
    available (so coordination always accounts for what's actually on air),
    falling back to the locally-assigned one for devices that don't report it.
    `exclude_key` leaves one device's own frequency out -- e.g. when finding a
    replacement for that exact device, its old value shouldn't count as a
    conflict against the new one."""
    freqs = []
    for key, dev in Devices.items():
        if key == exclude_key:
            continue
        freq = dev.metrics.get('frequency_mhz', DeviceFrequencies.get(key))
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
        exclude_key = params.get('exclude_key')

        existing = _active_device_frequencies(exclude_key=exclude_key) + extra
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
    any command waiting on a hardware confirmation) was stalled. Cycling
    every 0.5s (DEVICE_EXECUTOR has 64 worker threads, so this stays truly
    parallel well past today's device counts) keeps the audio meter reading
    as live instead of visibly stepping -- it's the tightest link in the
    chain up to each provider's own METER_RATE/cyclic-push rate."""
    if Devices:
        await asyncio.gather(*(_poll_one_device(ip, dev) for ip, dev in list(Devices.items())))
    IOLoop.current().call_later(0.5, lambda: IOLoop.current().spawn_callback(poll_devices))

def load_config():
    if not os.path.exists(CONFIG_PATH): return []
    with open(CONFIG_PATH, 'r') as f:
        return json.load(f).get('devices', [])

def save_config(device_list):
    with open(CONFIG_PATH, 'w') as f:
        json.dump({'devices': device_list}, f, indent=2)

# Every config.json entry is identified by (ip, channel), not ip alone --
# a multi-channel receiver has several entries sharing the same ip. Matching
# on ip alone here used to mean adding/updating one channel of a device
# would silently clobber every other channel's entry for that same ip.
def _same_device(entry, ip, channel):
    return entry.get('ip') == ip and int(entry.get('channel', 1)) == channel

def save_device_to_config(ip, brand, dtype, name='', channel=1):
    devices = [d for d in load_config() if not _same_device(d, ip, channel)]
    devices.append({'ip': ip, 'brand': brand, 'type': dtype, 'name': name, 'channel': channel})
    save_config(devices)

def remove_device_from_config(ip, channel=1):
    devices = [d for d in load_config() if not _same_device(d, ip, channel)]
    save_config(devices)

def update_device_name_in_config(ip, channel, name):
    devices = load_config()
    for d in devices:
        if _same_device(d, ip, channel):
            d['name'] = name
    save_config(devices)

def update_device_frequency_in_config(ip, channel, freq):
    devices = load_config()
    for d in devices:
        if _same_device(d, ip, channel):
            d['frequency_mhz'] = freq
    save_config(devices)

def update_device_listen_stream_in_config(ip, channel, stream):
    devices = load_config()
    for d in devices:
        if _same_device(d, ip, channel):
            if stream is None:
                d.pop('listen_stream', None)
            else:
                d['listen_stream'] = stream
    save_config(devices)

def update_device_assigned_user_in_config(ip, channel, assigned_user):
    devices = load_config()
    for d in devices:
        if _same_device(d, ip, channel):
            d['assigned_user'] = assigned_user
    save_config(devices)

def update_device_card_size_in_config(ip, channel, size):
    devices = load_config()
    for d in devices:
        if _same_device(d, ip, channel):
            d['card_size'] = size
    save_config(devices)

def update_device_visibility_in_config(ip, channel, visible):
    devices = load_config()
    for d in devices:
        if _same_device(d, ip, channel):
            d['visible'] = visible
    save_config(devices)

def update_device_card_order_in_config(ip, channel, order):
    devices = load_config()
    for d in devices:
        if _same_device(d, ip, channel):
            d['card_order'] = order
    save_config(devices)

def update_device_photo_in_config(ip, channel, photo_url):
    devices = load_config()
    for d in devices:
        if _same_device(d, ip, channel):
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
        (r'/listen/(.+)', ListenHandler),
        (r'/devices/listen-stream', ListenStreamHandler),
        (r'/discover/sap-streams', SapStreamsHandler),
        (r'/system/update', UpdateHandler),
        (r'/devices', DeviceHandler),
        (r'/devices/rename', RenameHandler),
        (r'/devices/assign-user', AssignUserHandler),
        (r'/devices/card-size', CardSizeHandler),
        (r'/devices/visibility', CardVisibilityHandler),
        (r'/devices/card-order', CardOrderHandler),
        (r'/devices/photo', PhotoHandler),
        (r'/devices/frequency', FrequencyHandler),
        (r'/discover', DiscoverHandler),
        (r'/discover/probe', ProbeHandler),
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
        key = device_key(ip, channel)
        Devices[key] = make_provider(ip, brand, dtype, channel)
        Devices[key].connect()
        Devices[key].photo = dev_cfg.get('photo')
        DeviceNames[key] = dev_cfg.get('name', '')
        DeviceAssignedUsers[key] = dev_cfg.get('assigned_user', '')
        DeviceLayout[key] = {
            'size': dev_cfg.get('card_size', 'md'),
            'order': dev_cfg.get('card_order', idx),
            'visible': dev_cfg.get('visible', True),
        }
        if dev_cfg.get('frequency_mhz') is not None:
            DeviceFrequencies[key] = dev_cfg['frequency_mhz']
        if dev_cfg.get('listen_stream'):
            DeviceListenStreams[key] = dev_cfg['listen_stream']

    _sap_listener.start()
    IOLoop.current().spawn_callback(poll_devices)
    IOLoop.current().spawn_callback(auto_discovery_tick)
    print(f"OmniWave OS v{VERSION} running on 0.0.0.0:9000...")
    IOLoop.current().start()

if __name__ == '__main__':
    main()
