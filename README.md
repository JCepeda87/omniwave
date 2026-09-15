# OmniWave

A self-hosted dashboard for monitoring and coordinating wireless mic/IEM systems on your local network — Shure and Sennheiser digital wireless, real protocol integration (not a mock), plus an RF frequency-coordination tool modeled on Shure Wireless Workbench / Sennheiser WSM.

- **Admin dashboard** (`/`) — add/remove devices, rename, assign frequencies, mute/unmute, auto-discovery, RF Scanner with intermodulation-aware frequency suggestions and a visual spectrum view, board layout editor.
- **User Board** (`/user`) — a simplified, read-mostly view for on-stage/backstage use: battery, RF signal, mic name and photo per device. Card size and order are set from the admin dashboard only.

## Requirements

- Python 3.8+
- Network access to the wireless receivers you want to monitor (this connects directly to each device's IP — it's not a cloud service)

## Install

```bash
git clone https://github.com/JCepeda87/omniwave.git
cd omniwave
pip3 install -r requirements.txt
```

## Run

```bash
python3 omniwave.py
```

The server listens on `0.0.0.0:9000`. Open:

- `http://localhost:9000` — admin dashboard
- `http://localhost:9000/user` — user board

From another device on the same network, use that machine's LAN IP instead of `localhost`.

**This app must run on the same network as your wireless receivers** — it opens direct sockets to each device's IP (TCP port 2202 for Shure, TCP/UDP port 45 for Sennheiser SSC). It won't discover or control anything if it's not on that LAN.

Devices, names, photos, frequencies, and board layout are stored in `config.json` (created on first use, not tracked in git — it's local runtime state, not source). Nothing carries over between installs automatically; use **Scan Network** / **Enable Auto-Add** in the admin UI to (re)discover devices on a fresh install.

## Supported hardware

**Shure** — ULX-D, QLX-D, UHF-R, PSM1000 are verified against real hardware. SLX-D, Axient Digital, and Microflex Wireless (MXW) are implemented from Shure's published command-string specs but not yet hardware-tested. GLX-D+, BLX, PSM900, and PSM300 have no Ethernet control on the hardware itself, so they're reported as such rather than faked.

**Sennheiser** — EW-DX, EW-D, Digital 9000/6000, and Spectera talk the real Sennheiser Sound Control Protocol (SSC), built from Sennheiser's published spec but not yet hardware-tested. XSW-D, XSW IEM, and AVX have no network control on the hardware itself. Other listed models fall back to a placeholder until they're implemented for real.

## Optional: OTA self-update

The app can update itself in place via `git pull` (triggered by a POST to `/system/update`). This is **disabled unless you set a real token**:

```bash
export OMNIWAVE_OTA_TOKEN="something-long-and-random"
python3 omniwave.py
```

Without this env var set, the update endpoint refuses every request.

## Keeping it running

For a persistent install (survives terminal close / reboot), run it under `systemd` (Linux), `launchd` (macOS), or a process manager like `pm2`/`tmux`, rather than a bare foreground terminal.
