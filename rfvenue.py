"""RF Venue Spectrum Recorder integration.

This is a standalone UHF spectrum scanner, not a wireless mic/IEM
receiver -- it doesn't fit the Devices/provider model the rest of this app
uses for mic gear (providers.py), so it's its own small module instead.

RF Venue does publish a live push API for this device ("Open API
integration... supports third-party tools such as SoundBase"), but access
to it is gated behind a sales inquiry form on their site, not public
documentation -- there's nothing to build against honestly without
guessing at undocumented endpoints. What IS fully public, in RF Venue's
own "Spectrum Recorder User Guide" (ver. C, 2024-07-19):
https://info.rfvenue.com/hubfs/Sales%20Content/Spectrum_Recorder_UserGuide.pdf

  - The device exposes a built-in guest SMB file share (\\\\<ip>\\share on
    Windows, smb://<ip> on Mac -- "If asked connect as Guest").
  - It continuously scans 400-700 MHz in 25 kHz steps, every 20 seconds.
  - Every 10 minutes it (re)writes three aggregate CSVs into the share
    root, each covering the last 24h (or since power-on):
      Avg.csv      -- average level per frequency step
      MaxHold.csv  -- peak level per frequency step ever seen
      Active.csv   -- MaxHold minus Avg (how "dynamically active" a step
                      is -- a loud, often-silent IEM pack stands out here
                      even if its average looks unremarkable)
    Each CSV is two columns, no header: frequency in MHz, amplitude in
    dBm. It also writes timestamped 20-second raw scans, but those aren't
    read here -- MaxHold/Avg/Active's own 10-minute aggregation already
    smooths exactly what this app uses spectrum data for (frequency
    coordination exclusions via spectrum_planner.exclusions_from_scan),
    and polling the raw files would mean re-reading a new, larger file
    every 20s for no benefit to that use case.

Not yet verified against a real Spectrum Recorder -- built from the
official user guide, matching the honesty standard the rest of this app
holds every protocol integration to (see e.g. providers.py's PSM1000/
Axient Digital module comments). Flag this clearly until confirmed live.
"""

import os
import csv
import time
import subprocess
import threading

MOUNT_TIMEOUT_SECONDS = 10
SMB_SHARE_NAME = 'share'
POLL_INTERVAL_SECONDS = 60  # Avg/MaxHold/Active only change server-side
                             # every 10 min -- this just re-reads on a
                             # shorter cycle so "can't reach it anymore"
                             # (device rebooted, network dropped) surfaces
                             # promptly rather than silently going stale.


class RFVenueSpectrumRecorder:
    """One instance per physical Spectrum Recorder. mount() connects once
    (mounts its SMB share read-only via macOS's mount_smbfs, guest
    credentials -- matches every providers.py class's "connect once, then
    just re-read/re-poll" shape, even though this isn't a providers.py
    class); poll() re-reads the already-mounted share's CSVs."""

    def __init__(self, ip):
        self.ip = ip
        self.status = 'DISCONNECTED'
        self.last_error = None
        self.last_updated = None
        # [[freq_mhz, level_dbm], ...] each -- see module comment for what
        # each file represents. spectrum == spectrum_maxhold, kept as the
        # default/primary field since MaxHold is the safer one for
        # frequency-coordination exclusions (an IEM pack only active part
        # of the time still shows up, not just whatever's loud right now).
        self.spectrum = []
        self.spectrum_avg = []
        self.spectrum_active = []
        self._mountpoint = None
        self._lock = threading.Lock()

    def connect(self):
        with self._lock:
            self._unmount_locked()
            mountpoint = f'/tmp/omniwave_rfvenue_{self.ip.replace(".", "_")}'
            try:
                os.makedirs(mountpoint, exist_ok=True)
            except OSError as e:
                self.status = 'DISCONNECTED'
                self.last_error = f"Couldn't create mountpoint: {e}"
                return
            try:
                result = subprocess.run(
                    ['mount_smbfs', '-N', f'//guest:@{self.ip}/{SMB_SHARE_NAME}', mountpoint],
                    capture_output=True, text=True, timeout=MOUNT_TIMEOUT_SECONDS,
                )
            except subprocess.TimeoutExpired:
                self.status = 'DISCONNECTED'
                self.last_error = f"Couldn't reach {self.ip} (timed out after {MOUNT_TIMEOUT_SECONDS}s)"
                return
            except OSError as e:
                self.status = 'DISCONNECTED'
                self.last_error = f"Couldn't reach {self.ip}: {e.strerror or e}"
                return
            if result.returncode != 0:
                self.status = 'DISCONNECTED'
                self.last_error = (result.stderr or result.stdout or 'mount_smbfs failed').strip()
                return
            self._mountpoint = mountpoint
            self.status = 'CONNECTED'
            self.last_error = None
        self.poll()

    def _unmount_locked(self):
        if self._mountpoint:
            try:
                subprocess.run(['umount', self._mountpoint], capture_output=True, timeout=MOUNT_TIMEOUT_SECONDS)
            except (OSError, subprocess.TimeoutExpired):
                pass
            self._mountpoint = None

    def disconnect(self):
        with self._lock:
            self._unmount_locked()
            self.status = 'DISCONNECTED'

    @staticmethod
    def _read_csv(path):
        points = []
        try:
            with open(path, newline='') as f:
                for row in csv.reader(f):
                    if len(row) < 2:
                        continue
                    try:
                        points.append([round(float(row[0]), 3), float(row[1])])
                    except ValueError:
                        continue
        except OSError:
            return None
        return points or None

    def poll(self):
        with self._lock:
            if self.status != 'CONNECTED' or not self._mountpoint:
                return
            mountpoint = self._mountpoint
            maxhold = self._read_csv(os.path.join(mountpoint, 'MaxHold.csv'))
            avg = self._read_csv(os.path.join(mountpoint, 'Avg.csv'))
            active = self._read_csv(os.path.join(mountpoint, 'Active.csv'))
            if maxhold is None and avg is None and active is None:
                # Mount itself is still up, but the share's gone missing
                # mid-session (device rebooted, network dropped) -- same
                # "technically still connected but can't actually read
                # it" case every providers.py class treats as a real
                # disconnect rather than silently repeating stale data.
                self.status = 'DISCONNECTED'
                self.last_error = "Lost contact with the Spectrum Recorder's share"
                return
            if maxhold is not None:
                self.spectrum = maxhold
            if avg is not None:
                self.spectrum_avg = avg
            if active is not None:
                self.spectrum_active = active
            self.last_updated = time.time()

    def get_json(self):
        return {
            'ip': self.ip,
            'status': self.status,
            'last_error': self.last_error,
            'last_updated': self.last_updated,
            'spectrum': self.spectrum,
            'spectrum_avg': self.spectrum_avg,
            'spectrum_active': self.spectrum_active,
        }
