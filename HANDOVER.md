# OmniWave — Handover

Last updated: 2026-10-02. Written for whoever picks this project up next.

## Status: everything below is committed and pushed

Commits `d2d51a8`..`23f8580` on `main`, already on `origin` — a normal `git clone` or
`git pull` gets you all of it. (For a while during this work, `git` itself was broken on
the original dev machine — an unaccepted Xcode license was blocking `git`, `python3`, and
`brew` all at once. That's resolved and was specific to that one Mac; see "Environment"
below before assuming any of that applies to your own setup.)

## What this is

A self-hosted dashboard for monitoring Shure/Sennheiser wireless mic and IEM systems on a
local network — real protocol integration against each device's own control protocol, not
mocked data. Two views: `/` (admin — add/configure/discover devices) and `/user` (read-mostly
stage-facing board). See [README.md](README.md) for the user-facing pitch; this doc is about
what's actually running under the hood and what's half-finished.

## Environment: how to actually run this

**Normal setup, on a normal machine:**

```bash
git clone https://github.com/JCepeda87/omniwave.git
cd omniwave
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt
./venv/bin/python3 omniwave.py
```

Three small, pure-Python dependencies (`tornado`, `requests`, `netifaces`) — this should
just work on any normal Python 3.8+ install with pip/network access. The rest of this
section only applies if you hit the *exact* symptom below; otherwise skip it.

---

**What follows is specific to the original dev machine**, which had a broken Xcode
Command Line Tools license that silently blocked `git`, `python3`, *and* `brew` all at
once (`"You have not agreed to the Xcode license agreements..."`). If you ever see that
exact error on a Mac, `sudo xcodebuild -license accept` (interactive terminal + password)
fixes it at the root — cheaper than any of the workarounds below. Only if that's not an
option did this project end up with:

- `/usr/bin/python3` and the project's own `venv/bin/python3` (a symlink into
  `/Applications/Xcode.app/.../Python3.framework/...`) both route through the same
  Xcode-license gate as git above. They work fine from an interactive shell once the
  license is accepted, but failed outright before that — and even after accepting the
  license, `venv/bin/python3` specifically fails under the **preview-server launcher's
  sandbox** (`.claude` tooling), with a permission error reading `venv/pyvenv.cfg` — a
  sandbox restriction unrelated to the license, confirmed by testing the exact same
  interpreter successfully from an interactive Bash shell.
- The working fix, already wired into **`.claude/launch.json`**: it points at
  `/opt/homebrew/bin/python3` (an independent Homebrew interpreter, not gated by any of
  the above). That interpreter's site-packages has `tornado`, `requests`, `certifi`,
  `charset_normalizer`, `idna`, and `urllib3` copied in directly (network was down at the
  time, so `pip install` wasn't an option — they were copied from the working `venv`'s
  site-packages instead, since they're pure Python / stable-ABI and load fine under a
  different CPython version).
- **`netifaces` is the one exception** — it's a compiled C extension pinned to the old
  venv's Python 3.9 ABI, and does not load under the Homebrew interpreter's Python 3.14.
  `omniwave.py`'s `import netifaces` is now wrapped in a `try/except ImportError`
  (`netifaces = None` on failure), and `get_local_subnets()` has a real fallback: when
  `netifaces` is unavailable, it falls back to the old single-subnet heuristic (reflects
  only the default-route interface) instead of hard-crashing. **Multi-subnet discovery is
  degraded under the Homebrew interpreter specifically** — if you want full netifaces
  support back, either `./venv/bin/python3 -m pip install -r requirements.txt` and run
  with that interpreter directly (works fine interactively, just not through the preview
  sandbox), or `/opt/homebrew/bin/python3 -m pip install netifaces` once network access
  and the Xcode license are sorted (untested whether it'll build cleanly there).
- Practical recommendation once git/Xcode is sorted: rebuild a clean venv from
  `/opt/homebrew/bin/python3 -m venv venv && ./venv/bin/pip install -r requirements.txt`,
  point `launch.json` back at `venv/bin/python3`, and retest whether the sandbox issue
  above still reproduces — it may have been specific to the venv being built from the
  Xcode interpreter rather than venvs in general. Nobody's verified this yet.
- To run manually without the preview tooling: `./venv/bin/python3 omniwave.py` (full
  netifaces support, Xcode-license-gated) or `/opt/homebrew/bin/python3 omniwave.py`
  (works everywhere, degraded subnet detection). Server listens on `0.0.0.0:9000`.

## Architecture

- **`omniwave.py`** — Tornado server: all HTTP/WebSocket handlers, device registry
  (`Devices`, `DeviceNames`, `DeviceFrequencies`, `DeviceLayout`, etc., all keyed by
  `device_key(ip, channel)` — composite key, *not* bare IP, because multi-channel units
  like a ULXD4Q or dual PSM1000 need one entry per channel), the polling loop
  (`poll_devices()`), and all network discovery (subnet detection, port scanning, mDNS,
  SAP). `main()` loads `config.json` on startup and reconnects every saved device.
- **`providers.py`** — one class per device family, all subclassing `BaseProvider`
  (`connect`/`disconnect`/`poll`/`scan_rf`/`send_command`/`get_json`). Real protocol
  implementations only — `identify_device()` in omniwave.py never guesses a model from
  which port happened to answer; it speaks each protocol enough to get a real answer.
  See each provider's module-level comment for exactly what's hardware-verified vs.
  "implemented from the published spec, never tested against real hardware."
- **`aes67.py`** — the real-time "Listen" feature (phase 1 only — see below).
- **`static/index.html`** — admin dashboard (vanilla JS, no build step, no framework).
- **`static/user.html`** — read-mostly stage board.
- **`config.json`** — runtime device state, gitignored, not source. Deleting it is safe;
  Auto-Discovery (on by default — see below) will repopulate anything it can identify.
- **`omniwave_history.db`** — SQLite metrics history (batt/rf/audio over time), **74MB and
  growing**. Already gitignored — just worth knowing it's there and will keep growing
  unbounded; nothing currently prunes old rows.
- **`micboard_modern.py`**, **`spectrum_planner.py`** — supporting modules (frequency
  coordination / RF scan planning). Not touched this round; not covered in depth here.

## Device/provider model — key concepts

- **Composite keying**: `device_key(ip, channel)` everywhere. A quad ULXD4Q at one IP
  becomes 4 separate `Devices` entries. `discover_channels()` queries each protocol family
  for its *real* channel count rather than guessing from a model-name suffix.
- **Role** (`transmitter` vs `receiver`): `device_role(dtype)` in omniwave.py, driven by
  `TRANSMITTER_TYPES`. IEM transmitters (PSM1000/900/300, XSW-IEM, the new
  `sennheiser-g4-sr`) send audio *out*; everything else is a receiver.
- **`freq_unavailable_reason`**: distinguishes "this protocol structurally can't report a
  frequency" (e.g. PSM1000's one-way push protocol) from "nobody's assigned one yet" — the
  UI shows the real reason instead of a misleading blank.
- **`dante_status`**: confirmed-live (not guessed) Dante-interface detection, per brand:
  - Sennheiser: `SennheiserSSCProvider` queries `/device/network/ether/interfaces` +
    `/device/network/ipv4_dante/{auto,ipaddr}` once at connect.
  - Shure: `ShureProvider`/`AxientDigitalProvider` query the real `NA_DEVICE_NAME` command
    string (confirmed against Shure's own ULX-D command-strings spec) once at connect.
    Deliberately **not** added to SLX-D (no Dante in its published command set), UHF-R
    (predates Dante), PSM1000 (one-way protocol, no query channel at all), or MXW (no
    Dante hardware).
  - `None` means "not yet queried" (e.g. currently unreachable) — never conflated with
    "confirmed absent." The admin UI's "Show Dante-confirmed devices only" checkbox
    filters on this.

## Discovery — why there are four different mechanisms

This machine moves between venues and networks constantly, and this was the dominant
theme of the most recent work. Four complementary discovery paths, each covering a gap the
others can't:

1. **`get_local_subnets()`** — dynamic, re-detects every currently-active interface
   subnet on every call (no hardcoded assumptions). Caps anything bigger than `/24` down
   to the `/24` containing this machine's own address (a `/16` corporate network would
   otherwise mean tens of thousands of probes). **Known gap**: if the real devices live in
   a *different* `/24` within a larger detected network, this won't see them — no fix
   shipped for that yet, just documented in the admin UI copy.
2. **Active port scanning** (`scan_subnet`/`scan_subnet_udp`) — TCP 2202 (Shure), TCP+UDP
   45 (Sennheiser SSC — UDP is the mandatory transport per Sennheiser's own spec, TCP is
   optional and some product lines, Digital 6000 included, don't implement it at all; this
   was a real bug fixed this round), UDP 53212 (Sennheiser G3/G4 "Media control protocol"),
   UDP 2202 (UHF-R).
3. **mDNS/Bonjour** (`mdns_discover_sennheiser_ips()`) — browses `_ssc._udp`/`_ssc._tcp`
   via the macOS `dns-sd` CLI. **This is the important one**: confirmed live this session
   that several real Sennheiser EM 6000 units announce themselves via Bonjour but never
   answer a single direct unicast query on any port (full port scan came back empty) — a
   venue network/switch policy blocking unicast to an unrecognized client, not a device
   problem. mDNS still finds them, so they at least surface in Scan Network results with
   an honest "can't reach it directly" label instead of vanishing entirely. Caught a real
   Python 3.9 bug along the way: `subprocess.TimeoutExpired.stdout` comes back as **bytes**
   even with `text=True` passed to `subprocess.run()` — every real `dns-sd` call hits the
   timeout path (it runs forever), so this would have silently broken all parsing if not
   caught and decoded explicitly.
4. **`Probe IP`** (admin UI) / `POST /discover/probe` — type in a specific address and it's
   checked directly, completely bypassing subnet detection. For when a device is on a
   segment this machine hasn't auto-detected, or the full-subnet scan's `/24` cap is
   hiding it.

**Auto-Discovery now defaults to ON** (`AUTO_DISCOVERY_ENABLED = True`, 20s interval) —
previously defaulted off and reset every restart, which fought directly against "just work
on whatever network I'm on," especially given how often this app has needed restarting
during active development. Still toggleable off in the admin UI.

## AES67 "Listen" feature — phase 1 only

Real plan on file at `~/.claude/plans/moonlit-wibbling-donut.md` (or ask the repo owner —
it may not survive a machine change). Summary of what's actually built vs. deliberately
deferred:

**Built** (`aes67.py` + `ListenHandler`/listen-stream config in `omniwave.py` + a
"🎧 Listen" button and "Configure Listen Stream" UI in `index.html`):
- SDP parsing, RTP packet parsing (12-byte header, L16/L24 big-endian PCM — no codec
  needed, AES67 is uncompressed), a jitter buffer, SAP listener (multicast
  `224.2.127.254:9875`) for discovering available AES67 streams.
- A WebSocket bridge (`tornado.websocket.WebSocketHandler`) streaming decoded PCM to the
  browser, played back via the native WebAudio API — no new JS dependency.
- Manual multicast-address/SDP entry as a fallback when SAP/discovery doesn't surface a
  stream (same "don't guess, offer an honest manual path" pattern as the rest of the app).
- **Deliberately scoped to AES67 only, not native Dante** — Dante's own wire protocol
  needs an Audinate OEM SDK license to implement; AES67 is the open, license-free standard
  Dante hardware can also speak once "AES67 mode" is turned on per-device in Dante
  Controller (a normal Audinate-supported feature, not a hack). That toggle is venue-side,
  in Dante Controller — this app has no way to flip it remotely.
- No PTP client — deliberate simplification. Fine for "listen to one stream as it
  arrives"; would matter for sample-accurate multi-channel sync (Instant Replay, below).

**Explicitly NOT built** (all discussed with the project owner, deferred on purpose, not
forgotten):
- **Instant Replay** (30-min rolling multi-channel buffer) — real storage design question
  (~170MB/channel/30min uncompressed PCM) that needs its own plan.
- **"Intelligent" mic issue detection** beyond the threshold-based red-alert highlighting
  already in the UI — genuine DSP/ML work, not a small addition.
- **Multi-user chat/collaboration** (images, reactions, voice notes) — independent of the
  audio work entirely, can be scoped separately any time.

**Unverified against real audio**: the venue network access needed to actually receive
live AES67 multicast audio was never confirmed working — same class of restriction as the
EM 6000 control-plane issue (see below). The code's correctness was verified with
synthetic/loopback tests, not live venue audio.

## Real-world network findings (tribal knowledge, not in any code comment)

- This is a working venue network (confirmed to be a Hillsong campus earlier in the
  project, via a Dante Domain Manager hostname found during mDNS reconnaissance) — **not**
  a lab. Expect switch/VLAN policy, not just flaky Wi-Fi.
- Several real Sennheiser EM 6000 units are on the network, alive and correctly
  configured (Remote Control + Online Mode both confirmed on, by the venue's own staff),
  confirmed reachable via a working Dante Controller session from a *different* PC on the
  same network — but this laptop specifically cannot reach them on any port. That's a
  switch/VLAN access-control decision on the venue's side. **Nothing in this codebase can
  fix that** — it needs this laptop's MAC/IP allow-listed by whoever manages the venue
  network. Don't spend more engineering time trying to route around it; it's been tried
  thoroughly (full port scans, every known protocol, local firewall ruled out).
- The laptop's own network state changes **constantly** and unpredictably during a single
  working session — different adapters (`en0` Wi-Fi vs `en10` a USB-Ethernet dongle) pick
  up completely different subnets, sometimes link-local (`169.254.x.x`, no DHCP server
  present), sometimes routed, sometimes direct, and it can flip between these within
  minutes. This is *why* discovery had to become this layered — don't assume the network
  picture from an hour ago is still true.

## What changed this round (for splitting into commits)

Roughly in dependency order:
1. Multi-channel device support (composite `device_key`), transmitter/receiver roles,
   `freq_unavailable_reason` honesty pattern.
2. User Board per-device visibility toggle.
3. Battery-as-runtime + alert-banner UI (WaveTool-inspired, protocol-verified).
4. Sennheiser G3/G4 "Media control protocol" provider (`SennheiserG4Provider`, UDP 53212) —
   net-new protocol support, including the EM vs SR disambiguation via a live `Squelch`
   probe.
5. `Probe IP` manual single-address discovery endpoint + UI.
6. mDNS/Bonjour discovery layer + the `TimeoutExpired.stdout`-is-bytes fix.
7. SSC transport fix: UDP instead of TCP-only (fixes Digital 6000/EM 6000 specifically).
8. `dante_status` detection for both brands + the "Dante-confirmed only" filter.
9. AES67 Listen feature (`aes67.py` + handlers + UI) — see plan file for full detail.
10. Auto-Discovery defaults to on.
11. `netifaces` graceful-degradation fallback + the Homebrew-interpreter environment fix
    (`.claude/launch.json`).

## Suggested next steps

1. **Accept the Xcode license and get this committed.** Everything above is uncommitted
   working-tree state on one machine. That's the actual emergency, not any code issue.
2. Decide whether to chase the venv-vs-sandbox mystery (does a venv built fresh from
   `/opt/homebrew/bin/python3` also fail under the preview sandbox, or was that specific
   to the Xcode-based venv?) — would let `netifaces` work again under the preview tooling.
3. Hardware-verify SLX-D, Axient Digital, and MXW providers against real units if any
   become available (currently spec-only per the README).
4. Whenever venue network access is sorted for the EM 6000s, re-verify both Dante
   detection and AES67 Listen against real hardware/audio — neither has been confirmed
   against a fully-reachable Sennheiser unit yet.
5. `omniwave_history.db` has no pruning/rotation — worth adding before it grows much
   further.
