# OmniWave — Handover

Last updated: 2026-10-02 (second revision, by Zac Beckenham, on taking the project over from
Julian Cepeda). This revision corrects places where the first version had fallen behind the
code, and adds setup steps for a fresh Mac. The original text is still in `omniwave-main.zip`
if you need it.

## Status

- Source of truth: `https://github.com/JCepeda87/omniwave`, branch `main`. Verified
  2026-10-02: the latest commit is `4dce76b` ("Clarify HANDOVER.md..."), and every source file
  in the `omniwave-main` zip matches that commit exactly. The only local differences are this
  revised `HANDOVER.md` and the updated `.claude/launch.json`. Zac now has collaborator
  access to the repo.
- Smoke-tested 2026-10-02 against this copy, in a Linux environment rather than macOS: all
  modules import, the server starts on `0.0.0.0:9000`, and `/`, `/user`, `/data` and
  `/regions` all respond. The `ifconfig` subnet parser was checked against sample macOS
  output and is correct. Nothing was tested against real hardware in this round.

## What this is

A self-hosted dashboard for monitoring Shure/Sennheiser wireless mic and IEM systems on a
local network. It speaks each device's own control protocol for real; nothing is mocked. It
has two views: `/` (admin: add, configure and discover devices) and `/user` (a read-mostly
board for the stage). See [README.md](README.md) for the user-facing summary. This doc covers
how it works under the hood and what's half-finished.

## Environment: running it on a Mac

**macOS only, in practice.** Discovery shells out to `ifconfig` and `dns-sd`, both
macOS-specific. On other platforms the server starts and the UI loads, but subnet detection
returns nothing and mDNS discovery doesn't work.

Dependencies: just `tornado` and `requests` (see `requirements.txt`). **`netifaces` is gone.**
The first handover described it at length, but `get_local_subnets()` now parses `ifconfig`
output directly, so none of the old netifaces fallback or "degraded multi-subnet" caveats
apply any more.

### First-time setup (fresh Mac)

```bash
cd ~/Documents/omniwave-main
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
./venv/bin/python3 omniwave.py
```

Then open `http://localhost:9000` (admin) or `http://localhost:9000/user`.

- If `python3` asks you to install the Command Line Developer Tools, accept and re-run.
- On a Hillsong-managed Mac that install can fail with "not currently available from the
  Software Update server". If it does, install Python from python.org instead and use
  `/usr/local/bin/python3 -m venv venv`. This is how Zac's machine was set up.
- If you see `"You have not agreed to the Xcode license agreements..."`, run
  `sudo xcodebuild -license accept` once. That error blocked `git`, `python3` and `brew` all
  at once on the original dev machine.
- **Run it from the project folder.** `config.json` and `omniwave_history.db` are opened by
  relative path, so they're created in whatever directory you launch from.
- The first launch may trigger a macOS "allow incoming connections" firewall prompt for
  Python. Allow it if other devices need to reach the board.

### `.claude/launch.json` (preview tooling)

This file was hard-coded to the original dev machine: `/Users/julian.cepeda/...` and a
Homebrew interpreter with hand-copied site-packages. On 2026-10-02 it was repointed to
`/Users/zachary.beckenham/Documents/omniwave-main/venv/bin/python3`, with `cwd` set to the
project folder. Anyone else taking it over will need to change these paths to match their
own machine.

The original machine had an unexplained problem: the preview sandbox couldn't read
`venv/pyvenv.cfg`. That venv was built from the Xcode-bundled Python. Nobody knows yet whether
a venv built from a normal Python hits the same thing. If it does, run the server from
Terminal instead (commands above).

## Architecture

- **`omniwave.py`**: the Tornado server. It holds all HTTP/WebSocket handlers, the device
  registry (`Devices`, `DeviceNames`, `DeviceFrequencies`, `DeviceLayout`, etc.), the polling
  loop (`poll_devices()`), and all network discovery (subnet detection, port scanning, mDNS,
  SAP). The registry is keyed by `device_key(ip, channel)`, a composite key rather than the
  bare IP, because multi-channel units like a ULXD4Q or a dual PSM1000 need one entry per
  channel. On startup, `main()` loads `config.json` and reconnects every saved device.
- **`providers.py`**: one class per device family, each subclassing `BaseProvider`
  (`connect`/`disconnect`/`poll`/`scan_rf`/`send_command`/`get_json`). These are real protocol
  implementations only: `identify_device()` in omniwave.py speaks each protocol enough to get
  a real answer, and never guesses the model from which port happened to answer. Each
  provider's module-level comment says which parts are hardware-verified and which were built
  from the spec but never tested on real hardware.
- **`aes67.py`**: the real-time "Listen" feature (phase 1 only, see below).
- **`static/index.html`**: the admin dashboard (vanilla JS, no build step, no framework).
- **`static/user.html`**: the read-mostly stage board.
- **`spectrum_planner.py`**: RF regions (`us_fcc`, `eu_etsi`, `au_acma`) and frequency
  coordination. The ACMA entry covers 520–694 MHz and tells users to check ACMA's Channel
  Finder before deploying.
- **`micboard_modern.py`**: a supporting module, not reviewed in depth.
- **`config.json`**: runtime device state, gitignored and not source. It's safe to delete;
  Auto-Discovery will repopulate anything it can identify.
- **`omniwave_history.db`**: SQLite metrics history (battery, RF and audio over time). It was
  **74MB and growing** on the original machine. Nothing prunes old rows.

### Security posture (read before using on a shared network)

- The server binds to `0.0.0.0:9000` and has **no authentication on any endpoint** except
  `/system/update`. That means anyone on the same network can open `/` and mute or unmute
  devices, change frequencies, rename devices and trigger scans.
- `/system/update` (OTA `git pull`) is disabled unless `OMNIWAVE_OTA_TOKEN` is set, and it
  compares tokens with `hmac.compare_digest`.
- Discovery actively port-scans and runs mDNS browses across every subnet it detects.
- **Policy:** this tool talks to real devices on Hillsong campus networks. Under Hillsong's
  IT/AI policies it must be reviewed by Hillsong IT before it's used against production
  systems or handed to anyone else. Raise this with the venue network team at the same time
  as the EM 6000 allow-listing (see below).

## Device/provider model: key concepts

- **Composite keying**: `device_key(ip, channel)` is used everywhere. A quad ULXD4Q at one IP
  becomes 4 separate `Devices` entries. `discover_channels()` asks each protocol family for
  its real channel count rather than guessing from a model-name suffix.
- **Role** (`transmitter` vs `receiver`): set by `device_role(dtype)`, driven by
  `TRANSMITTER_TYPES`. IEM transmitters (PSM1000/900/300, XSW-IEM, `sennheiser-g4-sr`) send
  audio out; everything else is a receiver.
- **`freq_unavailable_reason`**: separates "this protocol can't report a frequency at all"
  (e.g. PSM1000's one-way push protocol) from "nobody has assigned one yet", so the UI shows
  the real reason instead of a blank.
- **`dante_status`**: Dante interfaces are detected by querying the device, never guessed:
  - Sennheiser: `SennheiserSSCProvider` queries `/device/network/ether/interfaces` and
    `/device/network/ipv4_dante/{auto,ipaddr}` once at connect.
  - Shure: `ShureProvider`/`AxientDigitalProvider` query `NA_DEVICE_NAME` (taken from Shure's
    ULX-D command-strings spec) once at connect. Deliberately not added to SLX-D, UHF-R,
    PSM1000 or MXW, because none of them expose Dante over their control protocol.
  - `None` means "not yet queried" (for example, the device is unreachable right now), which
    is different from "confirmed absent". The admin UI's "Show Dante-confirmed devices only"
    filter uses this field.

## Discovery: why there are four mechanisms

The original laptop moved between venues and networks constantly, so discovery is layered:

1. **`get_local_subnets()`**: re-detects every active interface subnet by parsing
   `ifconfig` on every call. It caps anything bigger than `/24` down to the `/24` that
   contains this machine's own address. **Known gap**: devices in a different `/24` inside a
   larger network won't be found. Use Probe IP for those.
2. **Active port scanning** (`scan_subnet`/`scan_subnet_udp`):
   - TCP 2202 (Shure)
   - TCP and UDP 45 (Sennheiser SSC). UDP is the mandatory transport; Digital 6000 has no
     TCP at all.
   - UDP 53212 (Sennheiser G3/G4 Media control protocol)
   - UDP 2202 (UHF-R)
3. **mDNS/Bonjour** (`mdns_discover_sennheiser_ips()`): browses `_ssc._udp`/`_ssc._tcp`
   using the macOS `dns-sd` CLI. This finds Sennheiser units that a venue's switch policy
   blocks from answering unicast. Note: `subprocess.TimeoutExpired.stdout` comes back as
   bytes even with `text=True`, and every `dns-sd` call ends on the timeout path, so the code
   decodes it explicitly.
4. **Probe IP** (admin UI) / `POST /discover/probe`: checks one specific address directly,
   bypassing subnet detection entirely.

**Auto-Discovery is on by default** (`AUTO_DISCOVERY_ENABLED = True`, every 20s), and can be
switched off in the admin UI.

## AES67 "Listen" feature: phase 1 only

The detailed plan lived at `~/.claude/plans/moonlit-wibbling-donut.md` on the original
machine and **did not come across with the zip**. Ask Julian for it if you need it.

**Built** (`aes67.py`, `ListenHandler`/listen-stream config in `omniwave.py`, and the
"🎧 Listen" button plus "Configure Listen Stream" UI in `index.html`):

- SDP parsing, RTP parsing (L16/L24 big-endian PCM), a jitter buffer, and a SAP listener
  (`224.2.127.254:9875`).
- A WebSocket bridge that streams PCM to the browser, played back with WebAudio.
- Manual multicast/SDP entry as a fallback.
- AES67 only, not native Dante. Dante's wire protocol needs an Audinate OEM SDK licence.
  Dante devices must have AES67 mode turned on in Dante Controller, which happens on the
  venue side.
- No PTP client. That's fine for listening to one stream; it would matter for sample-accurate
  multi-channel sync.

**Deliberately not built:** Instant Replay (needs its own storage design, roughly
170MB/channel/30min), "intelligent" mic issue detection beyond threshold alerts, and
multi-user chat/collaboration.

**Unverified against real audio.** It has only been tested with synthetic and loopback data.

## Real-world network findings

- The network is a working Hillsong campus network, not a lab, so expect switch and VLAN
  policy.
- Several Sennheiser EM 6000 units are alive and correctly configured, and they're reachable
  from another PC running Dante Controller. The original laptop couldn't reach them on any
  port. That's a venue access-control decision: **it needs the laptop's MAC/IP allow-listed
  by whoever manages the venue network**, and no code change will fix it. Under the new
  owner this will be a different laptop, so the allow-listing request needs this machine's
  details.
- A laptop's network state can change within minutes: different adapters, link-local
  `169.254.x.x` addresses, routed vs. direct connections. Don't trust the network picture
  from an hour ago.

## History of changes (Julian's last round)

1. Multi-channel device support (composite `device_key`), transmitter/receiver roles, and
   `freq_unavailable_reason`.
2. User Board per-device visibility toggle.
3. Battery-as-runtime and alert-banner UI.
4. Sennheiser G3/G4 Media control protocol provider (`SennheiserG4Provider`, UDP 53212),
   including the EM vs SR disambiguation.
5. Probe IP endpoint and UI.
6. mDNS/Bonjour discovery and the `TimeoutExpired.stdout` fix.
7. SSC transport fix: UDP instead of TCP only.
8. `dante_status` detection and the "Dante-confirmed only" filter.
9. AES67 Listen feature.
10. Auto-Discovery on by default.
11. `netifaces` removed; `get_local_subnets()` now parses `ifconfig`.

## Suggested next steps

1. **Work from git.** Commit this revised `HANDOVER.md` and the `launch.json` change to the
   repo, so the next person isn't working from a zip. (The macOS `git` needs the Command
   Line Tools; GitHub Desktop ships its own git if those can't be installed.)
2. **Get Hillsong IT review** before using it on campus production networks. Combine this
   with the EM 6000 allow-listing request for this laptop.
3. **Add authentication** to the admin dashboard and command endpoints, or bind to
   `127.0.0.1` by default with an opt-in for LAN access.
4. **Add pruning/rotation** to `omniwave_history.db`.
5. Hardware-verify the SLX-D, Axient Digital, MXW and Sennheiser SSC providers against real
   units.
6. Once venue access is sorted, re-verify Dante detection and AES67 Listen against real
   EM 6000 hardware and live audio.
7. Optional: make the data file paths absolute (relative to the script) so launching from
   another directory doesn't create a second `config.json`/DB.
