"""AES67 audio receive: SDP parsing, SAP discovery, and live RTP receive.

Real-time "listen" support (see the approved plan at
~/.claude/plans/moonlit-wibbling-donut.md). This is deliberately a separate
module from providers.py: the rest of this app is built around
BaseProvider's request/response control-plane polling model (send a GET,
read a REP), which doesn't fit a continuous inbound media stream. Everything
here is standards-based and license-free:
  - SDP (RFC 4566): the session description format that names a stream's
    multicast address, port, and payload format.
  - SAP (RFC 2974): how AES67/Dante-in-AES67-mode senders advertise those
    SDP descriptions on the network without prior configuration.
  - RTP (RFC 3550) carrying linear PCM (RFC 3190, L16/L24): AES67's actual
    audio transport -- uncompressed, so there's no codec to implement.
Dante's own proprietary control/transport protocol is NOT implemented here
-- that requires an Audinate OEM SDK license. Dante hardware reaches this
code the same way any AES67 device does, once "AES67 mode" is enabled for
it in Dante Controller (a normal supported Dante feature).
"""

import array
import socket
import struct
import sys
import threading
import time

SAP_MULTICAST_ADDR = '224.2.127.254'
SAP_PORT = 9875

RTP_HEADER_STRUCT = struct.Struct('!BBHII')


def parse_sdp(text):
    """Extracts the fields AES67 playback needs from an SDP description:
    multicast address, port, payload type, and the encoding/rate/channels
    an a=rtpmap line ties to that payload type. Returns None if the SDP
    doesn't describe a usable linear-PCM audio stream -- AES67 payload
    types are dynamic (RFC 3551 range 96-127) and MUST be defined by an
    rtpmap line, so this never guesses what an unlabeled payload type means."""
    session_addr = None
    media_port = None
    payload_type = None
    rtpmap = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or '=' not in line:
            continue
        key, _, value = line.partition('=')
        if key == 'c' and value.startswith('IN IP4 '):
            session_addr = value[len('IN IP4 '):].split('/')[0].strip()
        elif key == 'm' and value.startswith('audio '):
            parts = value.split()
            if len(parts) >= 4:
                try:
                    media_port = int(parts[1])
                    payload_type = int(parts[3])
                except ValueError:
                    pass
        elif key == 'a' and value.startswith('rtpmap:'):
            pt_str, _, desc = value[len('rtpmap:'):].partition(' ')
            try:
                pt = int(pt_str)
            except ValueError:
                continue
            desc_parts = desc.strip().split('/')
            if len(desc_parts) >= 2:
                encoding = desc_parts[0].upper()
                try:
                    rate = int(desc_parts[1])
                except ValueError:
                    continue
                channels = int(desc_parts[2]) if len(desc_parts) >= 3 and desc_parts[2].isdigit() else 1
                rtpmap[pt] = (encoding, rate, channels)
    if session_addr is None or media_port is None or payload_type is None:
        return None
    encoding, rate, channels = rtpmap.get(payload_type, (None, None, None))
    if encoding not in ('L16', 'L24') or not rate:
        return None
    return {
        'multicast_addr': session_addr,
        'port': media_port,
        'payload_type': payload_type,
        'encoding': encoding,
        'sample_rate': rate,
        'channels': channels,
    }


def parse_rtp_packet(data):
    """Parses an RTP packet's fixed 12-byte header (RFC 3550) plus any
    CSRC list / extension header, returning the payload bytes and the
    fields needed to sequence and validate it. Returns None for anything
    too short or not RTP version 2."""
    if len(data) < 12:
        return None
    b0, b1, seq, timestamp, ssrc = RTP_HEADER_STRUCT.unpack_from(data, 0)
    if (b0 >> 6) != 2:
        return None
    padding = bool(b0 & 0x20)
    extension = bool(b0 & 0x10)
    cc = b0 & 0x0F
    offset = 12 + cc * 4
    if extension:
        if len(data) < offset + 4:
            return None
        ext_len_words = struct.unpack_from('!H', data, offset + 2)[0]
        offset += 4 + ext_len_words * 4
    payload = data[offset:]
    if padding and payload:
        pad_len = payload[-1]
        if 0 < pad_len <= len(payload):
            payload = payload[:-pad_len]
    return {
        'seq': seq,
        'timestamp': timestamp,
        'ssrc': ssrc,
        'payload_type': b1 & 0x7F,
        'marker': bool(b1 & 0x80),
        'payload': payload,
    }


def decode_pcm(payload, encoding, channels):
    """Decodes AES67 linear PCM (network/big-endian byte order, RFC 3190)
    into interleaved little-endian 16-bit PCM for the browser's WebAudio
    side (an Int16 typed array). L24 is downsampled to 16-bit by dropping
    the low byte -- a deliberate simplification for a live listen feature,
    not a recording/mixing use case where the extra bit depth matters."""
    if encoding == 'L16':
        n = (len(payload) // 2) * 2
        samples = array.array('h')
        samples.frombytes(payload[:n])
        if sys.byteorder == 'little':
            samples.byteswap()
        return samples.tobytes()
    if encoding == 'L24':
        n_samples = len(payload) // 3
        out = array.array('h', bytes(n_samples * 2))
        for i in range(n_samples):
            off = i * 3
            value = (payload[off] << 16) | (payload[off + 1] << 8) | payload[off + 2]
            if value & 0x800000:
                value -= 0x1000000
            out[i] = max(-32768, min(32767, value >> 8))
        return out.tobytes()
    return b''


class JitterBuffer:
    """Reorders/degaps one live RTP stream for playback -- not a perfect
    reconstruction (that's what a recording/replay feature would need),
    just enough smoothing that minor reordering or a dropped packet
    doesn't stutter a live listen session. Tracks the next sequence number
    it's waiting on; once too many packets have arrived out of order past
    it, gives up on the gap and substitutes silence rather than stalling
    playback indefinitely."""

    def __init__(self, max_wait_packets=3):
        self._next_seq = None
        self._pending = {}
        self._max_wait = max_wait_packets

    def push(self, seq, pcm_bytes, silence_frame):
        if self._next_seq is None:
            self._next_seq = seq
        self._pending[seq] = pcm_bytes
        ready = []
        while self._next_seq in self._pending:
            ready.append(self._pending.pop(self._next_seq))
            self._next_seq = (self._next_seq + 1) & 0xFFFF
        while len(self._pending) >= self._max_wait:
            ready.append(silence_frame)
            self._next_seq = (self._next_seq + 1) & 0xFFFF
            while self._next_seq in self._pending:
                ready.append(self._pending.pop(self._next_seq))
                self._next_seq = (self._next_seq + 1) & 0xFFFF
        return ready


def _join_multicast(sock, addr):
    mreq = struct.pack('4sl', socket.inet_aton(addr), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)


class RTPReceiver:
    """Joins one AES67 multicast RTP stream and pushes decoded,
    jitter-buffered 16-bit PCM to any number of registered listener
    callbacks. Starts on the first listener, stops when the last one goes
    away -- a live continuous stream is only worth the multicast join and
    a background thread while someone is actually listening."""

    def __init__(self, stream):
        self.stream = stream
        self._sock = None
        self._thread = None
        self._running = False
        self._listeners = []
        self._lock = threading.Lock()
        self.last_error = None

    def add_listener(self, callback):
        with self._lock:
            self._listeners.append(callback)
            if not self._running:
                self._start()
        return self.last_error is None

    def remove_listener(self, callback):
        with self._lock:
            if callback in self._listeners:
                self._listeners.remove(callback)
            if not self._listeners and self._running:
                self._stop()

    def _start(self):
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._sock.bind(('', self.stream['port']))
            _join_multicast(self._sock, self.stream['multicast_addr'])
            self._sock.settimeout(0.5)
            self.last_error = None
        except OSError as e:
            self.last_error = str(e)
            self._sock = None
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _stop(self):
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
        self._sock = None

    def _run(self):
        jitter = JitterBuffer()
        channels = self.stream['channels']
        # ~1 packet's worth of silence at a typical AES67 packet time (1ms
        # @ 48kHz = 48 frames/channel) -- used only as a stand-in for a
        # packet the jitter buffer gave up waiting for.
        silence = bytes(2 * channels * 48)
        while self._running and self._sock:
            try:
                data, _addr = self._sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            pkt = parse_rtp_packet(data)
            if not pkt or pkt['payload_type'] != self.stream['payload_type']:
                continue
            pcm = decode_pcm(pkt['payload'], self.stream['encoding'], channels)
            if not pcm:
                continue
            for chunk in jitter.push(pkt['seq'], pcm, silence):
                with self._lock:
                    listeners = list(self._listeners)
                for cb in listeners:
                    try:
                        cb(chunk)
                    except Exception as e:
                        # One listener's callback failing shouldn't kill the
                        # stream for every other listener -- but silently
                        # swallowing it entirely once hid a real bug (a
                        # wrong-thread IOLoop.current() call) for a while,
                        # so at least surface what broke.
                        print(f"aes67 RTPReceiver: listener callback failed: {e}")


class SAPListener:
    """Passive discovery of AES67/Dante-AES67-mode streams via SAP
    (RFC 2974): senders periodically multicast their SDP description here
    unprompted, so this just joins that group and records whatever shows
    up -- no per-device query needed, and a genuinely different failure
    mode from unicast control-plane probing (this session found real
    Sennheiser units via multicast mDNS discovery after direct unicast
    queries to them were silently blocked by venue network policy, and
    SAP is the same kind of multicast advertisement)."""

    def __init__(self):
        self.streams = {}
        self._sock = None
        self._thread = None

    def start(self):
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._sock.bind(('', SAP_PORT))
            _join_multicast(self._sock, SAP_MULTICAST_ADDR)
            self._sock.settimeout(1.0)
        except OSError:
            self._sock = None
            return False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return True

    def _run(self):
        while self._sock:
            try:
                data, _addr = self._sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                break
            sdp_text = self._extract_sdp(data)
            if not sdp_text:
                continue
            stream = parse_sdp(sdp_text)
            if stream:
                name = self._session_name(sdp_text) or stream['multicast_addr']
                stream['discovered_at'] = time.time()
                stream['session_name'] = name
                self.streams[name] = stream

    @staticmethod
    def _extract_sdp(data):
        # SAP's own header (flags/auth-length/hash/originating-address,
        # RFC 2974 section 4) precedes the payload; rather than fully
        # parse it, just find where the SDP text itself starts.
        idx = data.find(b'v=0')
        if idx == -1:
            return None
        return data[idx:].decode('utf-8', errors='ignore')

    @staticmethod
    def _session_name(sdp_text):
        for line in sdp_text.splitlines():
            if line.startswith('s='):
                return line[2:].strip()
        return None
