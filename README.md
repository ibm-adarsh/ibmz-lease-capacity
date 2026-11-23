# IBM Z lease schedule capacity

Interactive dashboard for **IBM Z / s390x** CI lease usage across
`libvirt-s390x-vpn*` cluster profiles in [`openshift/release`](https://github.com/openshift/release).

**Live dashboard:** https://ibm-adarsh.github.io/ibmz-lease-capacity/

## What it shows

- Auto-discovered full `libvirt-s390x-vpn*` cluster profile names (including new profiles as they appear in openshift/release)
- All periodic jobs using those profiles across OCP versions
- Cron timing, workflow, config / Sippy / Prow history links
- Observed hold durations from [Sippy](https://sippy.dptools.openshift.org) **successful runs** (fail-only averages ignored; cross-version same-job borrow; 4h default until a pass exists)
- Correct lease counts for workflows that hold more than one lease of the same type
- Week × hour **concurrency heatmap** vs Boskos lease capacity
- Busiest hours, free hours, and multi-hour free windows for scheduling
- UTC / IST display toggle

## Automatic refresh (no manual steps)

GitHub Actions runs **every morning at 08:00 IST** (`30 2 * * *` UTC) on `main`:

1. Sparse-clones latest `openshift/release@master`
2. Auto-discovers `libvirt-s390x-vpn*` profiles and re-scans all periodics (no hardcoded job list)
3. Fetches Sippy full-job durations for those OCP releases (borrows nearest same job when a version has no history)
4. Counts workflow-declared extra leases of the same Boskos type
5. Regenerates `docs/` (HTML + CSV + JSON + fingerprint)
6. Commits and pushes whenever the generated dashboard content changed
7. GitHub Pages redeploys from `/docs` automatically

This covers, without human intervention:

- **New jobs** added under these cluster profiles
- **Removed jobs**
- **Cron / interval timing changes**
- **Boskos lease capacity / name changes**
- **Sippy duration updates** (lease-hold model)

Optional manual run: **Actions → Update IBM Z lease dashboard → Run workflow**

> Note: GitHub may pause scheduled workflows on repos with no activity for ~60 days.
> The daily publish commits keep this repo active; if a schedule ever pauses, re-enable
> it under **Actions** or click **Run workflow** once.

## Local generation

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Point at a local openshift/release checkout
python3 scripts/generate_report.py \
  --release-repo /path/to/openshift/release \
  --out-dir docs \
  --fetch-sippy-durations \
  --duration-hours 4 \
  --lease-buffer-minutes 15
```

Then open `docs/index.html`.

Regression checks (cron math, OCP labeling, lease counts, docs consistency):

```bash
python3 scripts/regression_check.py --release-repo /path/to/openshift/release --docs-dir docs
```

## Model notes

This is a **configured schedule planning model**, not live Boskos occupancy.

- Each periodic acquires **1 lease at job start** and keeps it for the modeled
  **full job wall-clock** (pre → test → post), not the test phase alone
- Example: profile capacity `10`, one job with a `3h` lease hold ⇒ for those 3 hours
  only `9` leases remain available
- Duration preference order:
  1. Sippy **successful** ProwJob timing (success-run median when pass rate is mixed; same OCP version, else nearest version with passes; never install↔upgrade mix). Fail-only averages are ignored.
  2. Median of the same job name / job family / workflow (from success-backed samples)
  3. `--duration-hours` fallback (default **4h**) when there is no successful history yet
- IST toggle **remaps** the heatmap, free/busy lists, peak metrics, cell details, and inventory schedules to IST hour bins (`H:00–H+1:00`): a job starting at `H:05–H:59` falls in column `H`, and a hold ending at `Y:10–Y:59` still covers column `Y`. Prow cron source values stay UTC.
- A small `--lease-buffer-minutes` (default 15) is added on top of observed runtime
- Prow cron expressions remain UTC; only display labels convert
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
