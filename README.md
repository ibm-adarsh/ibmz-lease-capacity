# IBM Z lease schedule capacity

Interactive dashboard for **IBM Z / s390x** CI lease usage across
`libvirt-s390x-vpn*` cluster profiles in [`openshift/release`](https://github.com/openshift/release).

**Live dashboard:** https://ibm-adarsh.github.io/ibmz-lease-capacity/

## What it shows

- All periodic jobs using:
  - `libvirt-s390x-vpn`
  - `libvirt-s390x-vpn-amd64`
  - `libvirt-s390x-vpn-arm64`
  - `libvirt-s390x-vpn-hcp`
  - `libvirt-s390x-vpn-oz`
- OCP version, cron, workflow, and source file (linked into `openshift/release`)
- Week × hour **concurrency heatmap** (UTC) vs Boskos lease capacity
- Busiest hours and best free slots for scheduling more jobs

## Automatic refresh

GitHub Actions runs **every day at 08:00 IST** (`30 2 * * *` UTC):

1. Sparse-clones `openshift/release@master`
2. Regenerates `docs/` (HTML + CSV + JSON)
3. Commits **only when** the schedule fingerprint changes:
   - new / removed jobs on these profiles
   - cron / interval changes
   - Boskos lease capacity / name changes
   - duration model input changes

Manual run: **Actions → Update IBM Z lease dashboard → Run workflow**

## Local generation

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Point at a local openshift/release checkout
python3 scripts/generate_report.py \
  --release-repo /path/to/openshift/release \
  --out-dir docs \
  --duration-hours 5
```

Then open `docs/index.html`.

## Model notes

This is a **configured schedule planning model**, not live Boskos occupancy.

- Each periodic holds **1 lease** for `--duration-hours` after each cron start (default 5h)
- Prow cron times are **UTC**
- `low-frequency` jobs (`@yearly` / day-of-month) are listed but excluded from the weekly heatmap
- Presubmits are excluded

## Repo layout

```
scripts/generate_report.py          # generator
docs/index.html                     # GitHub Pages dashboard
docs/jobs.csv                       # Excel-friendly job inventory
docs/summary.json                   # machine-readable summary
docs/schedule.fingerprint           # change-detection hash
.github/workflows/update-dashboard.yml
```

## License

MIT
