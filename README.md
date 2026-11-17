# IBM Z lease schedule capacity

Interactive dashboard for **IBM Z / s390x** CI lease usage across
`libvirt-s390x-vpn*` cluster profiles in [`openshift/release`](https://github.com/openshift/release).

**Live dashboard:** https://ibm-adarsh.github.io/ibmz-lease-capacity/

## What it shows

- Full cluster profile names:
  - `libvirt-s390x-vpn`
  - `libvirt-s390x-vpn-amd64`
  - `libvirt-s390x-vpn-arm64`
  - `libvirt-s390x-vpn-hcp`
  - `libvirt-s390x-vpn-oz`
- All periodic jobs using those profiles across OCP versions
- Cron timing, workflow, config link, and Prow job-history link
- Observed hold durations from [Sippy](https://sippy.dptools.openshift.org) (with workflow / default fallbacks)
- Week × hour **concurrency heatmap** vs Boskos lease capacity
- Busiest hours, free hours, and multi-hour free windows for scheduling
- UTC / IST display toggle

## Automatic refresh

GitHub Actions runs **every day at 08:00 IST** (`30 2 * * *` UTC):

1. Sparse-clones `openshift/release@master`
2. Fetches Sippy average durations for relevant OCP releases
3. Regenerates `docs/` (HTML + CSV + JSON)
4. Commits **only when** the schedule fingerprint changes:
   - new / removed jobs on these profiles
   - cron / interval changes
   - Boskos lease capacity / name changes
   - modeled duration inputs change (Sippy / workflow / default)

Manual run: **Actions → Update IBM Z lease dashboard → Run workflow**

## Local generation

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Point at a local openshift/release checkout
python3 scripts/generate_report.py \
  --release-repo /path/to/openshift/release \
  --out-dir docs \
  --fetch-sippy-durations \
  --duration-hours 5 \
  --lease-buffer-minutes 15
```

Then open `docs/index.html`.

## Model notes

This is a **configured schedule planning model**, not live Boskos occupancy.

- Each periodic acquires **1 lease at job start** and keeps it for the modeled
  **full job wall-clock** (pre → test → post), not the test phase alone
- Example: profile capacity `10`, one job with a `3h` lease hold ⇒ for those 3 hours
  only `9` leases remain available
- Duration preference order:
  1. Sippy full ProwJob `current` / `previous` average minutes (biased toward the
     longer window when failures shorten the average)
  2. Median of Sippy-matched jobs that share the same workflow
  3. `--duration-hours` fallback (default 5h)
- A small `--lease-buffer-minutes` (default 15) is added on top of observed runtime
- Prow cron times are **UTC** (IST toggle only changes labels)
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
