
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
import struct
import secrets
import threading
import tornado.websocket
from concurrent.futures import ThreadPoolExecutor
from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler
from providers import (ShureProvider, SennheiserProvider, UHFRProvider, PSM1000Provider,
                       SLXDProvider, AxientDigitalProvider, MXWProvider, SennheiserSSCProvider,
                       SennheiserG4Provider, SennheiserEWDXProvider, NoNetworkProvider, freq_in_ranges,
                       EWDX_PRODUCT_CHANNELS)
import aes67
import spectrum_planner
import rfvenue

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
# key -> zone name (e.g. "Stage", "Lobby") -- a lighter-weight sub-grouping
# WITHIN one location, for a rig too large to eyeball as one flat list but
# not warranting a whole separate location (which fully disconnects and
# reconnects a different device set -- see switch_active_location). Purely
# organizational/filtering, never touches hardware, so unlike DeviceNames
# there's no separate "known zones" list to manage: a zone exists exactly
# when at least one device in the active location is assigned to it.
DeviceZones = {}

# One optional RF Venue Spectrum Recorder, board-wide rather than
# per-location (like ACTIVE_LOCATION, unlike Devices) -- a venue typically
# has at most one of these physical units regardless of how many logical
# location boards it's organized into. None until configured via
# SpectrumRecorderHandler; see rfvenue.py for what it actually does.
SPECTRUM_RECORDER = None

# This app moves between venues constantly (see HANDOVER.md), and a flat
# device list accumulates every unit ever seen at every venue -- useless for
# "just show me tonight's rig." Devices/DeviceNames/etc. above are therefore
# scoped: they only ever hold the CURRENTLY ACTIVE location's units in
# memory (and connected). config.json still holds every device from every
# location, each tagged with 'location'; switching the active location
# disconnects everything currently loaded and reconnects just that
# location's saved entries (see switch_active_location()). DataHandler and
# every discovery/auto-add path need no location-awareness of their own --
# they already only ever see what's in Devices, which is exactly "this
# location" by construction.
DEFAULT_LOCATION = 'Default'
ACTIVE_LOCATION = DEFAULT_LOCATION  # overwritten from config.json at startup, see main()

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
# EW-DX over SSCv2 (HTTPS REST, port 443, password-authenticated) -- a wholly
# different transport from SENNHEISER_SSC_TYPES' SSCv1 (plain JSON, port 45,
# unauthenticated), not just a newer version of the same connection, so it's
# its own provider/type rather than folded into SENNHEISER_SSC_TYPES. Only
# reachable with a user-supplied 3rd-party password (see
# SennheiserEWDXProvider's module comment in providers.py), so -- unlike
# every other type here -- this one is never auto-discovered/auto-added:
# identify_device() can recognize it (its /device/identity needs no auth),
# but nothing here can guess a password, so it's manual-add-only.
SENNHEISER_SSCV2_TYPES = {'ewdx-sscv2'}

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

def make_provider(ip, brand, dtype, channel=1, password=None):
    dev = _make_provider_instance(ip, brand, dtype, channel, password)
    # Stored directly rather than derived from isinstance() -- DataHandler
    # used to infer brand from a hardcoded isinstance() tuple that predated
    # SLXDProvider/AxientDigitalProvider/MXWProvider/NoNetworkProvider, so
    # those would misreport as Sennheiser. This is the actual source of
    # truth, set once at construction from what the caller asked for.
    dev.brand = brand
    dev.role = device_role(dtype)
    return dev

def _make_provider_instance(ip, brand, dtype, channel=1, password=None):
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
    if dtype in SENNHEISER_SSCV2_TYPES:
        return SennheiserEWDXProvider(ip, dtype, channel=channel, password=password)
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
    # Speeds both prune_old_metrics()'s DELETE and SnapshotHandler's SELECT,
    # both filtering on timestamp alone.
    conn.execute('CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp)')
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

# No pruning ever existed before this -- confirmed live this session:
# 122MB, 2.6 million rows, dating back to the first time this app was ever
# run (Sept 11), growing unbounded at roughly 2 rows/sec/connected device
# (one poll_devices() cycle every 0.5s). 7 days keeps a real, useful window
# for troubleshooting while bounding growth -- the Instant Replay feature
# itself only ever looks back 30 minutes, so this is deliberately far more
# generous than the replay UI needs.
METRICS_RETENTION_DAYS = 7
METRICS_PRUNE_INTERVAL_SECONDS = 3600  # hourly

def prune_old_metrics():
    """Blocking; call via run_in_executor -- the first run after this
    shipped deletes millions of rows, which can take real wall-clock time
    and must never block the IOLoop. SQLite doesn't shrink the file on
    DELETE alone (confirmed: deleting 1.5M of 2.6M rows left the file
    *larger*, from WAL/journal overhead, until VACUUMed down from 195MB to
    80MB) -- VACUUM only when something was actually deleted, since it
    rewrites the whole file and there's no point paying that cost on an
    empty prune (most of the hourly runs, once the initial backlog is
    cleared)."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute(
        "DELETE FROM metrics WHERE timestamp < datetime('now', ?)",
        (f'-{METRICS_RETENTION_DAYS} days',),
    )
    deleted = cursor.rowcount
    conn.commit()
    if deleted:
        conn.execute('VACUUM')
    conn.close()
    return deleted

async def prune_metrics_tick():
    try:
        deleted = await IOLoop.current().run_in_executor(None, prune_old_metrics)
        if deleted:
            print(f"Pruned {deleted} metrics row(s) older than {METRICS_RETENTION_DAYS} days")
    except Exception as e:
        print(f"Metrics pruning failed: {e}")
    IOLoop.current().call_later(METRICS_PRUNE_INTERVAL_SECONDS, lambda: IOLoop.current().spawn_callback(prune_metrics_tick))

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
            entry['zone'] = DeviceZones.get(key, '')
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

class SnapshotHandler(RequestHandler):
    """Instant Replay: every device's most-recent metrics row at or before
    a given moment, in one query -- not per-device like AnalyticsHandler,
    since the whole board needs to time-travel together. Relies on a real,
    documented SQLite behavior (not a trick): when MAX() is the only
    aggregate in the result list, SQLite guarantees the non-aggregated
    columns come from the same row that produced the max, so one GROUP BY
    query is enough instead of N "latest row per device" lookups."""
    def get(self):
        t = self.get_argument('t', None)
        try:
            t = float(t)
        except (TypeError, ValueError):
            self.set_status(400)
            self.write(json.dumps({'error': 't (unix seconds) is required'}))
            return
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT ip, batt, rf, audio, MAX(timestamp) FROM metrics "
            "WHERE timestamp <= datetime(?, 'unixepoch') GROUP BY ip",
            (t,),
        ).fetchall()
        conn.close()
        snapshot = {
            key: {'batt': batt, 'rf': rf, 'audio': audio, 'timestamp': ts}
            for key, batt, rf, audio, ts in rows
        }
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(snapshot))

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

class ReportHandler(RequestHandler):
    """A genuinely read-only view -- unlike UserIndexHandler (which still
    lets whoever's looking at it claim a device and upload a photo),
    report.html has no write endpoints wired into it at all, and unlike
    every other page in this app, getting to it requires a real token (see
    create_report_token()) rather than just knowing/guessing a path. Meant
    for handing a link to someone who should see live status -- FOH, a
    producer, a client -- without handing them the admin board or even the
    User Board's limited self-service controls."""
    def get(self, token):
        if not is_valid_report_token(token):
            self.set_status(404)
            return
        with open(os.path.join(os.path.abspath('.'), 'static', 'report.html'), 'rb') as f:
            self.write(f.read())

class ReportTokensHandler(RequestHandler):
    """Create/list/revoke the tokens ReportHandler checks -- admin-only in
    intent (same as every other management endpoint here; see
    HANDOVER.md's security-posture note -- nothing in this app actually
    enforces that distinction today)."""
    def get(self):
        self.write(json.dumps({'tokens': load_full_config()['report_tokens']}))

    def post(self):
        params = json.loads(self.request.body)
        label = (params.get('label') or '').strip()
        entry = create_report_token(label)
        self.write(json.dumps(entry))

    def delete(self):
        token = self.get_argument('token', None)
        if not token:
            self.set_status(400)
            return
        revoke_report_token(token)
        self.write(json.dumps({'success': True}))

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

async def register_device_channels(ip, brand, dtype, name, password=None):
    """Discovers this unit's real channels (see discover_channels()) and
    creates one connected provider instance per channel, each under its own
    device_key and persisted to config.json as a separate entry -- shared by
    the manual Add Device flow and background auto-discovery so a ULXD4Q or
    multi-channel PSM1000 shows up as N independent devices, not one.
    `password` only applies to SENNHEISER_SSCV2_TYPES (see that set's
    comment) -- ignored otherwise, same as every provider's constructor."""
    channels = await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, discover_channels, ip, brand, dtype, password)
    # Explicit (re-)registration always wins over a past removal -- clears
    # any stale ignored_ips entry so this ip isn't silently blocked from
    # auto-discovery forever after being deliberately brought back.
    remove_ignored_ip(ip)
    added = []
    for channel in channels:
        key = device_key(ip, channel)
        dev = make_provider(ip, brand, dtype, channel, password)
        await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, dev.connect)
        Devices[key] = dev
        DeviceNames[key] = name
        DeviceLayout.setdefault(key, {'size': 'md', 'order': len(Devices)})
        save_device_to_config(ip, brand, dtype, name, channel, password=password)
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
        password = params.get('password') or None
        if not ip:
            self.set_status(400)
            self.write(json.dumps({'error': 'ip is required'}))
            return

        added = await register_device_channels(ip, brand, dtype, name, password=password)
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
            DeviceZones.pop(key, None)
            DeviceAssignedUsers.pop(key, None)
            DeviceFrequencies.pop(key, None)
            DeviceLayout.pop(key, None)
            DeviceListenStreams.pop(key, None)
            remove_device_from_config(dev.ip, dev.channel)
            # Deliberately removed -- auto-discovery shouldn't bring it back
            # on its own within the next scan (see run_auto_discovery_scan).
            add_ignored_ip(dev.ip)
            self.write(json.dumps({'success': True}))
        else:
            self.set_status(404)

class RemoveAllDevicesHandler(RequestHandler):
    """Clears every device in the ACTIVE location at once -- for getting
    back to a clean board between events/venues without clicking Remove on
    each one. Scoped to the current location, not every device ever saved:
    Devices only ever holds the active location's units (see
    switch_active_location), so `keys` here already is exactly "this
    location's devices" -- other locations' saved devices are untouched in
    config.json."""
    async def post(self):
        keys = list(Devices.keys())
        removed_ips = set()
        for key in keys:
            dev = Devices[key]
            removed_ips.add(dev.ip)
            await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, dev.disconnect)
            del Devices[key]
            DeviceNames.pop(key, None)
            DeviceZones.pop(key, None)
            DeviceAssignedUsers.pop(key, None)
            DeviceFrequencies.pop(key, None)
            DeviceLayout.pop(key, None)
            DeviceListenStreams.pop(key, None)
        remaining = [d for d in load_config() if d.get('location', DEFAULT_LOCATION) != ACTIVE_LOCATION]
        save_config(remaining)
        # Deliberately removed -- see DeviceHandler.delete's same note.
        for ip in removed_ips:
            add_ignored_ip(ip)
        self.write(json.dumps({'success': True, 'removed': len(keys), 'location': ACTIVE_LOCATION}))

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

class AssignZoneHandler(RequestHandler):
    """Sub-groups a device within the active location (e.g. "Stage",
    "Lobby") -- purely organizational, never touches hardware, so (unlike
    RenameHandler) this is safe to leave reachable from the User Board too
    if that's ever wanted. Blank clears it back to unassigned."""
    def post(self):
        params = json.loads(self.request.body)
        key = params.get('ip')  # device_key(ip, channel)
        zone = (params.get('zone') or '').strip()
        if not key or key not in Devices:
            self.set_status(404)
            return
        DeviceZones[key] = zone
        dev = Devices[key]
        update_device_zone_in_config(dev.ip, dev.channel, zone)
        self.write(json.dumps({'success': True, 'ip': key, 'zone': zone}))

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

IFCONFIG_INET_LINE_RE = re.compile(r'^\s*inet\s+(\d+\.\d+\.\d+\.\d+)\s+netmask', re.MULTILINE)

def get_local_ips():
    """This machine's own IPv4 addresses (every interface, not just the
    default route) -- used to filter passive-discovery results (see
    ShureDiscoveryListener): multicast delivers to every socket that joined
    the group on the LAN segment, including ones on this same host, so
    something else on this Mac sending real SSDP/SLP traffic (confirmed
    live: happens on its own, nothing to do with this app) would otherwise
    show up as a 'discovered device' that's actually just this computer."""
    try:
        output = subprocess.run(['ifconfig'], capture_output=True, text=True, timeout=3).stdout
    except (OSError, subprocess.TimeoutExpired):
        return set()
    return {m.group(1) for m in IFCONFIG_INET_LINE_RE.finditer(output)}

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

UDP_PROBE_ATTEMPTS = 2  # see _udp_probe()'s docstring

def _udp_probe(ip, port, payload):
    """Sends `payload` and waits up to DISCOVERY_TIMEOUT for any reply,
    retrying up to UDP_PROBE_ATTEMPTS times on a fresh send before giving
    up. UDP has no retransmission of its own (unlike probe_host()'s TCP
    connect_ex, which gets the kernel's own SYN retries for free) -- a
    single dropped or delayed packet under DISCOVERY_MAX_WORKERS-way
    concurrent load reads as "device not found" with only one attempt,
    confirmed live: a real UHF-R unit that answered an isolated probe
    instantly was missing from a full scan's results. Re-sending on the
    same socket rather than opening a new one each attempt."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(DISCOVERY_TIMEOUT)
            for attempt in range(UDP_PROBE_ATTEMPTS):
                try:
                    s.sendto(payload, (ip, port))
                    s.recvfrom(4096)
                    return True
                except OSError:
                    if attempt == UDP_PROBE_ATTEMPTS - 1:
                        raise
    except OSError:
        return False
    return False

def probe_host_udp_uhfr(ip):
    """UHF-R only responds over UDP -- a plain TCP connect_ex() (scan_subnet's
    check) never sees it, since UDP has no equivalent "is it listening"
    probe short of actually speaking the protocol."""
    return _udp_probe(ip, 2202, b'* GET 1 CHAN_NAME *')

def probe_host_udp_g4(ip):
    """ew G4 stationary units also only respond over UDP (port 53212, ASCII
    Media control protocol -- see identify_device()) -- same reasoning as
    the UHF-R probe above, just a different port/protocol."""
    return _udp_probe(ip, 53212, b'FirmwareRevision\r')

def probe_host_udp_ssc(ip):
    """Sennheiser SSC's mandatory transport is UDP (port 45) -- TCP is an
    optional extra some product lines layer on top, and Digital 6000
    (EM 6000/L 6000) implements ONLY UDP (confirmed live: a real EM 6000
    never answered scan_subnet()'s TCP-45 probe at all). Without this,
    scan_subnet()'s TCP-only check silently drops every UDP-only SSC unit."""
    return _udp_probe(ip, 45, b'{"device":{"identity":{"product":null}}}')

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
    mDNS/Bonjour-discovered ones (see mdns_discover_sennheiser_ips()) plus
    Shure-multicast passively-discovered ones (see ShureDiscoveryListener,
    below), deduplicated by IP -- an explicit port-probe result wins if a
    host somehow shows up in more than one of these (each source sets
    'source' so callers can still tell a passive-only/mDNS-only hit apart
    from a directly-probed one)."""
    combined = {}
    for ip in ShureDiscoveryListener.drain():
        combined[ip] = {'ip': ip, 'brand': 'shure', 'port': None, 'source': 'passive'}
    for ip in mdns_discover_sennheiser_ips():
        combined[ip] = {'ip': ip, 'brand': 'sennheiser', 'port': None, 'source': 'mdns'}
    for c in scan_subnet_udp() + scan_subnet():
        combined[c['ip']] = {**c, 'source': 'probe'}
    return list(combined.values())

# Passive discovery -- a third, fundamentally different discovery path from
# every probe_host_* function and mdns_discover_sennheiser_ips() above.
# Those are all *active*: something here sends a packet straight at a
# specific IP (or a targeted mDNS query) and waits for a reply. This instead
# just joins the well-known multicast groups Shure's "Shure Control" device
# family (ULX-D/QLX-D-class receivers, Axient Digital, PSM1000, SBC
# chargers) use to announce themselves on their own, unprompted, and
# records whoever's been talking there -- confirmed real, not guessed, from
# Shure's own published WWB6 Ports and Protocol Information:
# https://service.shure.com/articles/en_US/Knowledge/wwb6-ports-and-protocol-information
#   239.255.255.250:1900 -- SSDP ("Service Discovery")
#   239.255.254.253:8427 -- Shure's own "Multicast SLP" ("Required for
#     service discovery")
# Same pattern aes67.SAPListener already uses for AES67/Dante stream
# discovery (join a well-known multicast group, let devices announce
# themselves instead of being asked) -- this is that same idea applied to
# Shure's own discovery channels rather than Dante's. It catches two real
# cases the active probes above can miss entirely: a device that only just
# powered on or reconnected (no need to wait for the next scan cycle to
# happen to probe it), and a device on a network whose policy silently
# drops unicast traffic from a client it doesn't recognize while still
# letting ambient multicast through -- the exact pattern already confirmed
# live this session against a real Sennheiser EM6000 over mDNS, now covered
# for Shure's own discovery channels too. The payload is never parsed --
# the mere fact a host sent *anything* to one of these addresses is itself
# the signal; identify_device() (run on every candidate from every
# discovery path, including this one) is what actually confirms what the
# device really is, so a false positive here (some unrelated SSDP-chatty
# device, e.g. a smart TV) costs one extra, cheap identify_device() call
# and nothing more.
SHURE_DISCOVERY_GROUPS = [
    ('239.255.255.250', 1900),  # SSDP
    ('239.255.254.253', 8427),  # Shure Multicast SLP
]

OWN_IPS_REFRESH_SECONDS = 30

class ShureDiscoveryListener:
    """One instance per multicast group (see SHURE_DISCOVERY_GROUPS) --
    joins it and records the source IP of every packet received, until the
    process exits. discovered_ips is shared (class-level) across every
    instance since callers just want "everything heard on any group"."""
    discovered_ips = set()
    _lock = threading.Lock()
    _own_ips = frozenset()
    _own_ips_checked_at = 0.0

    @classmethod
    def _is_own_ip(cls, ip):
        # Cached rather than shelled out to ifconfig per packet -- a busy
        # multicast group (other local software chattering on it, see the
        # module comment above) could mean many packets a second.
        now = time.time()
        if now - cls._own_ips_checked_at > OWN_IPS_REFRESH_SECONDS:
            cls._own_ips = frozenset(get_local_ips())
            cls._own_ips_checked_at = now
        return ip in cls._own_ips

    def __init__(self, group, port):
        self.group = group
        self.port = port
        self._sock = None

    def start(self):
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Lets this coexist with other software on the same machine
            # also listening on this same well-known port (e.g. Wireless
            # Workbench running alongside this app) -- best-effort, not
            # available on every platform.
            if hasattr(socket, 'SO_REUSEPORT'):
                try: self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                except OSError: pass
            self._sock.bind(('', self.port))
            mreq = struct.pack('4sL', socket.inet_aton(self.group), socket.INADDR_ANY)
            self._sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            self._sock.settimeout(1.0)
        except OSError as e:
            print(f"Shure discovery: couldn't join {self.group}:{self.port}: {e}")
            self._sock = None
            return False
        threading.Thread(target=self._run, daemon=True).start()
        return True

    def _run(self):
        while self._sock:
            try:
                _data, (ip, _src_port) = self._sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            if self._is_own_ip(ip):
                continue
            with self._lock:
                ShureDiscoveryListener.discovered_ips.add(ip)

    @classmethod
    def drain(cls):
        with cls._lock:
            ips = list(cls.discovered_ips)
            cls.discovered_ips.clear()
        return ips

def start_passive_discovery():
    for group, port in SHURE_DISCOVERY_GROUPS:
        ShureDiscoveryListener(group, port).start()

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

    # EW-DX SSCv2 (HTTPS REST, port 443) -- a completely different protocol
    # from SSCv1 above (port 45), confirmed live: a real EW-DX with
    # 3rd-party access not yet enabled refuses every connection on port 45
    # and 2202, but /api/device/identity needs no auth at all (per the
    # published OpenAPI spec), so this alone can positively identify the
    # device even before anyone has set a 3rd-party password on it -- the
    # password (needed for everything else) still has to be entered
    # manually; see SENNHEISER_SSCV2_TYPES.
    try:
        r = requests.get(f'https://{ip}:443/api/device/identity', timeout=IDENTIFY_TIMEOUT, verify=False)
        if r.ok:
            product = r.json().get('product')
            if isinstance(product, str) and product.strip():
                product = product.strip()
                return {'brand': 'sennheiser', 'type': 'ewdx-sscv2', 'label': f'EW-DX ({product}, SSCv2)', 'name': product}
    except (requests.RequestException, ValueError):
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
    """Checked against the FULL saved config (every location), not just the
    active location's in-memory Devices -- confirmed with the user that
    locations here are logical boards on one shared network (e.g. WTL vs
    HILLS), not separate venues with colliding private IP ranges, so the
    same ip is always the same real device no matter which location it's
    filed under. Checking only the active location let auto-discovery
    re-add a device within ~20s of it being deleted or assigned to a
    different location -- it looked "new" the moment it wasn't in the
    currently-active board, even though it was still registered elsewhere."""
    return any(d.get('ip') == ip for d in load_config())

# Real per-unit channel discovery for multi-channel receivers (ULXD4Q, UR4D,
# a dual PSM1000 P10T, ...): queries the device directly for which channels
# it actually has, rather than guessing a count from a model-name suffix
# (e.g. the "Q" in ULXD4Q). Falls back to a single channel [1] wherever the
# protocol doesn't support this or the device doesn't respond -- honest
# about what's genuinely knowable, same principle as identify_device().
CHANNEL_REP_RE = re.compile(r'REP\s+(\d+)\s+\S')
PSM1000_CHANNEL_RE = re.compile(r'REPORT\s+(\d+)\s+AUDIO_IN_LVL')

def discover_channels(ip, brand, dtype, password=None):
    if dtype in SENNHEISER_SSCV2_TYPES:
        # /device/identity needs no auth (per the OpenAPI spec) and its
        # `product` enum (EWDX2CHS/EWDX2CHDS/EWDX4CHDS) directly names the
        # real channel count -- no need to even have the password yet just
        # to know how many channels to register.
        try:
            r = requests.get(f'https://{ip}:443/api/device/identity', timeout=IDENTIFY_TIMEOUT, verify=False)
            if r.ok:
                product = r.json().get('product', '')
                for key, count in EWDX_PRODUCT_CHANNELS.items():
                    if key == product:
                        return list(range(1, count + 1))
        except requests.RequestException:
            pass
        return [1]

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
        elif candidate.get('source') in ('mdns', 'passive'):
            # Found only via self-announcement -- mDNS/Bonjour
            # (mdns_discover_sennheiser_ips()) or Shure's own discovery
            # multicast groups (drain_passive_discovered_ips()) -- not by a
            # port responding to a direct query. A real, specific pattern
            # confirmed live this session: a device can genuinely announce
            # itself over multicast while silently ignoring unicast queries
            # from this client, most likely a switch/VLAN policy
            # restricting which hosts it'll actually talk to directly (not
            # a device problem -- a different, already-trusted PC on the
            # same network can usually still reach it fine).
            via = 'Bonjour/mDNS' if candidate['source'] == 'mdns' else "Shure's discovery multicast (SSDP/SLP)"
            found.append({
                'ip': ip, 'identified': False, 'type': None,
                'brand': candidate['brand'],
                'label': f"Announces itself via {via} but isn't answering direct queries -- likely network policy blocking this computer specifically, not a device problem",
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
    for real, and returns only ones not already registered (any location)
    and not explicitly removed by the user. Unlike run_discovery_scan()
    above, unidentified hosts are dropped here rather than surfaced --
    auto-add should never register a device under a guessed type with no
    human reviewing it first."""
    found = []
    for candidate in scan_subnet_all():
        ip = candidate['ip']
        if _ip_already_registered(ip) or is_ignored_ip(ip):
            continue
        info = identify_device(ip)
        if info and info['type'] in SENNHEISER_SSCV2_TYPES:
            # Needs a password nothing here can guess -- auto-adding it
            # would just register a permanently-disconnected device with no
            # way to fix it short of deleting and re-adding manually. Still
            # surfaced fine by the manual Scan/Probe flows (see
            # run_discovery_scan()), which show it to a human to enter one.
            continue
        if info:
            found.append({'ip': ip, **info})
    return found

async def auto_discovery_tick():
    if AUTO_DISCOVERY_ENABLED:
        try:
            found = await IOLoop.current().run_in_executor(None, run_auto_discovery_scan)
            for f in found:
                ip = f['ip']
                if _ip_already_registered(ip) or is_ignored_ip(ip):  # could've changed mid-scan
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

class SpectrumRecorderHandler(RequestHandler):
    """Configure and read an RF Venue Spectrum Recorder -- a standalone
    scanner, board-wide like ACTIVE_LOCATION rather than one of
    Devices/DeviceNames/etc. See rfvenue.py for what actually happens on
    connect/poll and SPECTRUM_RECORDER's comment for why this is global."""
    def get(self):
        if SPECTRUM_RECORDER is None:
            self.write(json.dumps({'configured': False}))
            return
        self.write(json.dumps({'configured': True, **SPECTRUM_RECORDER.get_json()}))

    async def post(self):
        global SPECTRUM_RECORDER
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
        if SPECTRUM_RECORDER is not None:
            await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, SPECTRUM_RECORDER.disconnect)
        SPECTRUM_RECORDER = rfvenue.RFVenueSpectrumRecorder(ip)
        await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, SPECTRUM_RECORDER.connect)
        save_spectrum_recorder_ip(ip)
        self.write(json.dumps({'configured': True, **SPECTRUM_RECORDER.get_json()}))

    async def delete(self):
        global SPECTRUM_RECORDER
        if SPECTRUM_RECORDER is not None:
            await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, SPECTRUM_RECORDER.disconnect)
            SPECTRUM_RECORDER = None
        save_spectrum_recorder_ip(None)
        self.write(json.dumps({'configured': False}))

async def spectrum_recorder_tick():
    """Self-rescheduling, same call_later pattern as poll_devices()/
    auto_discovery_tick()/prune_metrics_tick(). Only ever touches
    SPECTRUM_RECORDER's own re-read (poll()) -- if it's gone DISCONNECTED
    (device off/unreachable), retries connect() at the same cadence rather
    than needing a human to re-enter the IP, matching _poll_one_device's
    auto-reconnect behavior for mic/IEM devices."""
    if SPECTRUM_RECORDER is not None:
        try:
            if SPECTRUM_RECORDER.status != 'CONNECTED':
                await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, SPECTRUM_RECORDER.connect)
            else:
                await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, SPECTRUM_RECORDER.poll)
        except Exception as e:
            print(f"spectrum_recorder_tick failed: {e}")
    IOLoop.current().call_later(rfvenue.POLL_INTERVAL_SECONDS, lambda: IOLoop.current().spawn_callback(spectrum_recorder_tick))

_last_webhook_sent = {}
WEBHOOK_COOLDOWN_SECONDS = 60

# Every provider's poll() flips itself to DISCONNECTED after a run of missed
# polls (see providers.py's NETWORK_MISS_LIMIT/UHFR_MISS_LIMIT), but nothing
# ever called connect() again afterward -- a device that drops out from a
# transient network blip (a laptop's Wi-Fi hiccupping mid-service, say) and
# then recovers on its own stayed stuck showing offline until someone
# switched locations or restarted the server, even though the hardware was
# reachable again. Retrying connect() here, throttled so a genuinely
# powered-off device isn't hammered, lets it come back automatically.
RECONNECT_RETRY_SECONDS = 5
_last_reconnect_attempt = {}

async def _poll_one_device(ip, dev):
    try:
        if dev.status != 'CONNECTED':
            now = time.time()
            if now - _last_reconnect_attempt.get(ip, 0) >= RECONNECT_RETRY_SECONDS:
                _last_reconnect_attempt[ip] = now
                await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, dev.connect)
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

def load_full_config():
    """The whole config.json shape: devices (each tagged with a location),
    which location is active, and the full list of known location names
    (kept even for locations with zero devices right now, so a freshly
    created empty location survives a restart). Devices saved before the
    location feature existed have no 'location' field -- defaulted to
    DEFAULT_LOCATION here so nothing already on a user's board silently
    disappears when this ships."""
    if not os.path.exists(CONFIG_PATH):
        return {'devices': [], 'active_location': DEFAULT_LOCATION, 'locations': [DEFAULT_LOCATION],
                'ignored_ips': [], 'spectrum_recorder_ip': None, 'report_tokens': []}
    with open(CONFIG_PATH, 'r') as f:
        cfg = json.load(f)
    devices = cfg.get('devices', [])
    for d in devices:
        d.setdefault('location', DEFAULT_LOCATION)
    locations = cfg.get('locations') or []
    for d in devices:
        if d['location'] not in locations:
            locations.append(d['location'])
    if DEFAULT_LOCATION not in locations:
        locations.append(DEFAULT_LOCATION)
    active = cfg.get('active_location', DEFAULT_LOCATION)
    if active not in locations:
        locations.append(active)
    return {
        'devices': devices, 'active_location': active, 'locations': locations,
        # IPs explicitly removed by the user (Remove / Remove All) -- auto-
        # discovery skips these so deleting a device actually sticks instead
        # of it reappearing within 20s. Manual Add/Probe always bypasses
        # this (explicit intent wins), and registering a device for real
        # clears it from here -- see register_device_channels().
        'ignored_ips': cfg.get('ignored_ips', []),
        # Board-wide, not per-location -- see SPECTRUM_RECORDER's comment.
        'spectrum_recorder_ip': cfg.get('spectrum_recorder_ip'),
        # Revocable links for the read-only shareable report -- see
        # ReportHandler/ReportTokensHandler. Each entry:
        # {token, label, created_at}.
        'report_tokens': cfg.get('report_tokens', []),
    }

def load_config():
    """Device list only (all locations), for callers that just need the
    saved devices and don't touch location metadata."""
    return load_full_config()['devices']

def save_config(device_list):
    """Writes the device list while preserving whatever location metadata
    is already on disk -- every call site here only ever changes devices,
    never locations/active_location directly (see save_locations_meta)."""
    full = load_full_config()
    full['devices'] = device_list
    with open(CONFIG_PATH, 'w') as f:
        json.dump(full, f, indent=2)

def save_locations_meta(active_location, locations):
    full = load_full_config()
    full['active_location'] = active_location
    full['locations'] = locations
    with open(CONFIG_PATH, 'w') as f:
        json.dump(full, f, indent=2)

def add_ignored_ip(ip):
    full = load_full_config()
    ignored = set(full['ignored_ips'])
    ignored.add(ip)
    full['ignored_ips'] = sorted(ignored)
    with open(CONFIG_PATH, 'w') as f:
        json.dump(full, f, indent=2)

def remove_ignored_ip(ip):
    full = load_full_config()
    if ip in full['ignored_ips']:
        full['ignored_ips'] = [i for i in full['ignored_ips'] if i != ip]
        with open(CONFIG_PATH, 'w') as f:
            json.dump(full, f, indent=2)

def is_ignored_ip(ip):
    return ip in load_full_config()['ignored_ips']

def save_spectrum_recorder_ip(ip):
    full = load_full_config()
    full['spectrum_recorder_ip'] = ip
    with open(CONFIG_PATH, 'w') as f:
        json.dump(full, f, indent=2)

def create_report_token(label):
    full = load_full_config()
    # 32 url-safe chars from a CSPRNG (secrets, not random) -- this is the
    # only thing standing between "link I handed someone" and "anyone who
    # guesses a path", since nothing else in this app is authenticated
    # (see HANDOVER.md's security-posture note). Not a claim that /report
    # is cryptographically hardened overall: /data itself stays exactly as
    # open as it already is for the admin/User Board, this only gates the
    # read-only report *page* behind a real token instead of a guessable path.
    token = secrets.token_urlsafe(24)
    entry = {'token': token, 'label': label or '', 'created_at': time.time()}
    full['report_tokens'].append(entry)
    with open(CONFIG_PATH, 'w') as f:
        json.dump(full, f, indent=2)
    return entry

def revoke_report_token(token):
    full = load_full_config()
    full['report_tokens'] = [t for t in full['report_tokens'] if t['token'] != token]
    with open(CONFIG_PATH, 'w') as f:
        json.dump(full, f, indent=2)

def is_valid_report_token(token):
    return any(t['token'] == token for t in load_full_config()['report_tokens'])

# Every config.json entry is identified by (ip, channel, location), not
# just (ip, channel) -- a multi-channel receiver has several entries
# sharing the same ip, AND different venues can easily reuse the same
# private IP range, so the same ip:channel can legitimately exist under two
# different locations' history at once. Every update_device_*_in_config
# function below is only ever called for a device the caller already has
# loaded (i.e. one in the ACTIVE location), so matching against
# ACTIVE_LOCATION here -- rather than threading a location param through
# every one of those call sites -- is both correct and far less invasive.
def _same_device(entry, ip, channel):
    return (entry.get('ip') == ip and int(entry.get('channel', 1)) == channel
            and entry.get('location', DEFAULT_LOCATION) == ACTIVE_LOCATION)

def save_device_to_config(ip, brand, dtype, name='', channel=1, location=None, password=None):
    devices = [d for d in load_config() if not _same_device(d, ip, channel)]
    entry = {
        'ip': ip, 'brand': brand, 'type': dtype, 'name': name, 'channel': channel,
        'location': location or ACTIVE_LOCATION,
    }
    # Only SENNHEISER_SSCV2_TYPES uses this (see its comment) -- omitted
    # entirely for every other device rather than writing a pointless null.
    if password:
        entry['password'] = password
    devices.append(entry)
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

def update_device_zone_in_config(ip, channel, zone):
    devices = load_config()
    for d in devices:
        if _same_device(d, ip, channel):
            d['zone'] = zone
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

async def _connect_loaded_device(key, dev, dev_cfg, idx):
    await IOLoop.current().run_in_executor(DEVICE_EXECUTOR, dev.connect)
    dev.photo = dev_cfg.get('photo')
    DeviceNames[key] = dev_cfg.get('name', '')
    DeviceZones[key] = dev_cfg.get('zone', '')
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

async def _load_location(location, device_list=None):
    """Connects every saved device tagged with `location` (in parallel,
    same asyncio.gather pattern poll_devices() uses for N-devices-at-once
    work), populating Devices and the per-device dicts. Shared by startup
    (main(), via run_sync) and runtime location switching (below) so
    there's exactly one place that knows how a config.json entry becomes a
    live, connected device."""
    if device_list is None:
        device_list = load_config()
    matching = [d for d in device_list if d.get('location', DEFAULT_LOCATION) == location]
    tasks = []
    for idx, dev_cfg in enumerate(matching):
        ip = dev_cfg['ip']
        brand = dev_cfg.get('brand', 'shure')
        dtype = dev_cfg.get('type', 'axtd')
        channel = dev_cfg.get('channel', 1)
        key = device_key(ip, channel)
        dev = make_provider(ip, brand, dtype, channel, dev_cfg.get('password'))
        Devices[key] = dev
        tasks.append(_connect_loaded_device(key, dev, dev_cfg, idx))
    if tasks:
        await asyncio.gather(*tasks)

async def _unload_all_devices():
    """Disconnects and drops everything currently loaded -- used before
    loading a different location in, so Devices never holds two locations'
    worth of units (and their composite ip:channel keys, which can
    legitimately collide across locations -- see _same_device) at once."""
    if Devices:
        await asyncio.gather(*(
            IOLoop.current().run_in_executor(DEVICE_EXECUTOR, dev.disconnect) for dev in Devices.values()
        ))
    Devices.clear()
    DeviceNames.clear()
    DeviceZones.clear()
    DeviceAssignedUsers.clear()
    DeviceFrequencies.clear()
    DeviceLayout.clear()
    DeviceListenStreams.clear()

async def switch_active_location(name):
    """Tears down the currently-loaded location's devices and loads the
    named one instead, then persists the switch so it's still active after
    a restart. `name` need not already exist in config.json's locations
    list -- switching to a brand-new name both creates and activates it
    (there's deliberately no separate "create location" step beyond that)."""
    global ACTIVE_LOCATION
    await _unload_all_devices()
    ACTIVE_LOCATION = name
    await _load_location(name)
    full = load_full_config()
    locations = full['locations']
    if name not in locations:
        locations.append(name)
    save_locations_meta(name, locations)

class LocationsHandler(RequestHandler):
    """GET: every known location name (including empty ones) plus which is
    active. POST {name}: switch to it, creating it first if it's new."""
    def get(self):
        full = load_full_config()
        self.write(json.dumps({'locations': full['locations'], 'active': ACTIVE_LOCATION}))

    async def post(self):
        params = json.loads(self.request.body)
        name = (params.get('name') or '').strip()
        if not name:
            self.set_status(400)
            self.write(json.dumps({'error': 'name is required'}))
            return
        if name == ACTIVE_LOCATION:
            self.write(json.dumps({'success': True, 'active': ACTIVE_LOCATION, 'changed': False}))
            return
        await switch_active_location(name)
        self.write(json.dumps({'success': True, 'active': ACTIVE_LOCATION, 'changed': True}))

def main():
    init_db()
    register_installation()

    app = Application([
        (r'/', IndexHandler),
        (r'/user', UserIndexHandler),
        (r'/report/([^/]+)', ReportHandler),
        (r'/report-tokens', ReportTokensHandler),
        (r'/data', DataHandler),
        (r'/analytics', AnalyticsHandler),
        (r'/analytics/snapshot', SnapshotHandler),
        (r'/command', CommandHandler),
        (r'/scan', ScanHandler),
        (r'/listen/(.+)', ListenHandler),
        (r'/devices/listen-stream', ListenStreamHandler),
        (r'/discover/sap-streams', SapStreamsHandler),
        (r'/system/update', UpdateHandler),
        (r'/devices', DeviceHandler),
        (r'/devices/remove-all', RemoveAllDevicesHandler),
        (r'/locations', LocationsHandler),
        (r'/devices/rename', RenameHandler),
        (r'/devices/zone', AssignZoneHandler),
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
        (r'/spectrum-recorder', SpectrumRecorderHandler),
        (r'/static/(.*)', StaticHandler),
    ])
    app.listen(9000, address='0.0.0.0')
    global ACTIVE_LOCATION, SPECTRUM_RECORDER
    full_cfg = load_full_config()
    ACTIVE_LOCATION = full_cfg['active_location']
    IOLoop.current().run_sync(lambda: _load_location(ACTIVE_LOCATION, full_cfg['devices']))

    if full_cfg['spectrum_recorder_ip']:
        SPECTRUM_RECORDER = rfvenue.RFVenueSpectrumRecorder(full_cfg['spectrum_recorder_ip'])
        IOLoop.current().run_sync(lambda: IOLoop.current().run_in_executor(DEVICE_EXECUTOR, SPECTRUM_RECORDER.connect))

    _sap_listener.start()
    start_passive_discovery()
    IOLoop.current().spawn_callback(poll_devices)
    IOLoop.current().spawn_callback(auto_discovery_tick)
    IOLoop.current().spawn_callback(prune_metrics_tick)
    IOLoop.current().spawn_callback(spectrum_recorder_tick)
    print(f"OmniWave OS v{VERSION} running on 0.0.0.0:9000...")
    IOLoop.current().start()

if __name__ == '__main__':
    main()
