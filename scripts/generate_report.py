#!/usr/bin/env python3
"""Generate an IBM Z (s390x) lease schedule capacity report.

Reads an openshift/release checkout, models weekly concurrent lease demand for
libvirt-s390x-vpn* cluster profiles, and emits a self-contained HTML dashboard
plus CSV/JSON. Optionally enriches per-job lease-hold durations from Sippy
(current/previous average runtime) with workflow-level and default fallbacks.

Example:
  python3 scripts/generate_report.py --release-repo /path/to/openshift/release
  python3 scripts/generate_report.py --release-repo ./openshift-release --out-dir docs \\
      --fetch-sippy-durations --duration-hours 5
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import statistics
import subprocess
import sys
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover
    print("ERROR: PyYAML is required (pip install pyyaml)", file=sys.stderr)
    sys.exit(1)

DEFAULT_PROFILES = (
    "libvirt-s390x-vpn",
    "libvirt-s390x-vpn-amd64",
    "libvirt-s390x-vpn-arm64",
    "libvirt-s390x-vpn-hcp",
    "libvirt-s390x-vpn-oz",
)

DAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
RELEASE_BLOB_BASE = "https://github.com/openshift/release/blob/master"
SIPPY_API = "https://sippy.dptools.openshift.org"
PROW_HISTORY_BASE = "https://prow.ci.openshift.org/job-history/gs/test-platform-results/logs"
SIPPY_JOB_URL = "https://sippy.dptools.openshift.org/sippy-ng/jobs"

CRON_ALIASES = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}


def tool_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    root = tool_repo_root()
    parser = argparse.ArgumentParser(
        description="Generate IBM Z libvirt-s390x-vpn* lease schedule capacity report"
    )
    parser.add_argument(
        "--release-repo",
        "--repo-root",
        dest="release_repo",
        type=Path,
        required=True,
        help="Path to an openshift/release git checkout",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=root / "docs",
        help="Output directory for dashboard artifacts (default: <this-repo>/docs)",
    )
    parser.add_argument(
        "--duration-hours",
        type=float,
        default=5.0,
        help="Fallback lease-hold duration in hours when Sippy/workflow data is missing (default: 5)",
    )
    parser.add_argument(
        "--fetch-sippy-durations",
        action="store_true",
        help="Fetch observed job durations from Sippy and use them for concurrency modeling",
    )
    parser.add_argument(
        "--sippy-min-hours",
        type=float,
        default=0.5,
        help="Minimum modeled duration (hours) even if Sippy reports shorter runs (default: 0.5)",
    )
    parser.add_argument(
        "--sippy-max-hours",
        type=float,
        default=12.0,
        help="Maximum modeled duration (hours) clamp (default: 12)",
    )
    parser.add_argument(
        "--profiles",
        nargs="+",
        default=list(DEFAULT_PROFILES),
        help="Cluster profiles to include",
    )
    parser.add_argument(
        "--write-fingerprint",
        action="store_true",
        default=True,
        help="Write schedule.fingerprint for change detection (default: on)",
    )
    return parser.parse_args()


def git_metadata(release_repo: Path) -> dict[str, str]:
    """Return sha / short / subject / date for the openshift/release checkout."""

    def run(args: list[str]) -> str:
        try:
            out = subprocess.check_output(
                ["git", "-C", str(release_repo), *args],
                stderr=subprocess.DEVNULL,
                text=True,
            )
            return out.strip()
        except (subprocess.CalledProcessError, FileNotFoundError):
            return ""

    sha = run(["rev-parse", "HEAD"])
    return {
        "sha": sha,
        "short_sha": sha[:12] if sha else "unknown",
        "subject": run(["log", "-1", "--pretty=%s"]),
        "committed_at": run(["log", "-1", "--pretty=%cI"]),
        "source_url": f"https://github.com/openshift/release/commit/{sha}" if sha else "",
    }


def load_yaml(path: Path) -> Any:
    with path.open(encoding="utf-8") as fh:
        loader = getattr(yaml, "CSafeLoader", None) or yaml.SafeLoader
        return yaml.load(fh, Loader=loader)


def parse_profile_lease_map(config_path: Path, profiles: set[str]) -> dict[str, str]:
    """Map cluster profile name -> boskos lease_type via a lightweight scan."""
    mapping: dict[str, str] = {}
    current_lease: str | None = None
    current_name: str | None = None
    in_profiles = False
    for line in config_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("cluster_profiles:"):
            in_profiles = True
            continue
        if in_profiles and line and not line.startswith(" ") and not line.startswith("-"):
            break
        if not in_profiles:
            continue
        if line.startswith("- "):
            if current_name in profiles and current_lease:
                mapping[current_name] = current_lease
            current_lease = None
            current_name = None
            rest = line[2:].strip()
            if rest.startswith("lease_type:"):
                current_lease = rest.split(":", 1)[1].strip()
            elif rest.startswith("name:"):
                current_name = rest.split(":", 1)[1].strip()
            continue
        stripped = line.strip()
        if stripped.startswith("lease_type:"):
            current_lease = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("name:"):
            current_name = stripped.split(":", 1)[1].strip()
    if current_name in profiles and current_lease:
        mapping[current_name] = current_lease
    return mapping


def parse_boskos_leases(boskos_path: Path, lease_types: set[str]) -> dict[str, list[str]]:
    """Map lease_type -> resource names without loading the full Boskos YAML."""
    out: dict[str, list[str]] = {lt: [] for lt in lease_types}
    current_names: list[str] = []
    in_names = False
    for line in boskos_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("- names:"):
            current_names = []
            in_names = True
            continue
        if in_names and line.startswith("  - "):
            current_names.append(line.strip()[2:].strip())
            continue
        if line.startswith("  type:"):
            rtype = line.split(":", 1)[1].strip()
            if rtype in lease_types:
                out[rtype] = list(current_names)
            current_names = []
            in_names = False
            continue
        if line.startswith("  state:"):
            continue
        if line and not line.startswith(" "):
            in_names = False
    return out


def extract_ocp_version(job: dict[str, Any], source_file: str) -> str:
    env = job.get("steps", {}).get("env") or {}
    branch = env.get("BRANCH") or env.get("OCP_VERSION")
    if isinstance(branch, (int, float)):
        branch = str(branch)
    if isinstance(branch, str) and re.fullmatch(r"\d+\.\d+", branch.strip('"')):
        return branch.strip('"')

    for pattern in (
        r"__nightly-(\d+\.\d+)",
        r"release-(\d+\.\d+)",
        r"ocp(\d+\.\d+)",
        r"-(\d+\.\d+)__",
        r"-(\d+\.\d+)\.yaml$",
    ):
        m = re.search(pattern, source_file)
        if m:
            return m.group(1)
    return "unknown"


def construct_prow_job_name(source_file: str, as_name: str) -> str | None:
    """Build periodic Prow job name from ci-operator config path + test as."""
    parts = source_file.replace("\\", "/").split("/")
    if len(parts) < 5 or parts[0] != "ci-operator" or parts[1] != "config":
        return None
    org, repo = parts[2], parts[3]
    stem = Path(parts[-1]).stem
    prefix = f"{org}-{repo}-"
    rest = stem[len(prefix) :] if stem.startswith(prefix) else stem
    rest = rest.replace("__", "-")
    return f"periodic-ci-{org}-{repo}-{rest}-{as_name}"


def candidate_config_files(config_dir: Path, profiles: set[str]) -> list[Path]:
    """Return config files that mention any target profile (text prefilter)."""
    needles = tuple(sorted(profiles, key=len, reverse=True))
    matches: list[Path] = []
    for path in config_dir.rglob("*.yaml"):
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        if any(n in text for n in needles):
            matches.append(path)
    return sorted(matches)


def parse_jobs(config_dir: Path, profiles: set[str], repo: Path) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    candidates = candidate_config_files(config_dir, profiles)
    print(f"  candidate config files: {len(candidates)}", flush=True)
    for path in candidates:
        try:
            data = load_yaml(path)
        except Exception as exc:  # noqa: BLE001
            print(f"WARN: skip unreadable {path}: {exc}", file=sys.stderr)
            continue
        if not isinstance(data, dict):
            continue
        tests = data.get("tests") or []
        if not isinstance(tests, list):
            continue
        try:
            rel = str(path.relative_to(repo))
        except ValueError:
            rel = f"ci-operator/config/{path.relative_to(config_dir)}"

        for test in tests:
            if not isinstance(test, dict):
                continue
            steps = test.get("steps") or {}
            profile = steps.get("cluster_profile")
            if profile not in profiles:
                continue
            cron = test.get("cron")
            interval = test.get("interval")
            if not cron and not interval:
                continue
            as_name = test.get("as", "")
            workflow = steps.get("workflow")
            timeout = test.get("timeout")
            prow_name = construct_prow_job_name(rel, str(as_name))
            jobs.append(
                {
                    "as": as_name,
                    "profile": profile,
                    "cron": str(cron).strip("'\"") if cron is not None else None,
                    "interval": str(interval) if interval is not None else None,
                    "timeout": str(timeout) if timeout is not None else None,
                    "workflow": workflow,
                    "source_file": rel,
                    "ocp_version": extract_ocp_version(test, str(path)),
                    "prow_job_name": prow_name,
                }
            )
    return jobs


def parse_field(field: str, lo: int, hi: int) -> list[int]:
    """Parse a single cron field into sorted unique values."""
    field = field.strip()
    values: set[int] = set()
    for piece in field.split(","):
        piece = piece.strip()
        if not piece:
            continue
        step = 1
        if "/" in piece:
            base, step_s = piece.split("/", 1)
            step = int(step_s)
        else:
            base = piece
        if base in ("*", "?"):
            start, end = lo, hi
        elif "-" in base:
            a, b = base.split("-", 1)
            start, end = int(a), int(b)
        else:
            values.add(int(base))
            continue
        for n in range(start, end + 1, step):
            if lo <= n <= hi:
                values.add(n)
    return sorted(values)


def classify_and_expand_cron(expr: str) -> tuple[str, list[tuple[int, int, int]]]:
    """Return (schedule_class, weekly slots as (dow, hour, minute))."""
    raw = expr.strip().strip("'\"")
    if raw in CRON_ALIASES:
        raw = CRON_ALIASES[raw]

    parts = raw.split()
    if len(parts) != 5:
        return "invalid", []

    minute_s, hour_s, dom_s, mon_s, dow_s = parts

    if dom_s not in ("*", "?") or mon_s not in ("*", "?"):
        return "low-frequency", []

    try:
        minutes = parse_field(minute_s, 0, 59)
        hours = parse_field(hour_s, 0, 23)
        dow_raw = (
            parse_field(dow_s.replace("7", "0"), 0, 6)
            if dow_s not in ("*", "?")
            else list(range(7))
        )
        dows = sorted(set(0 if d == 7 else d for d in dow_raw))
    except ValueError:
        return "invalid", []

    slots = [(d, h, m) for d in dows for h in hours for m in minutes]
    schedule_class = "daily" if set(dows) == set(range(7)) else "weekly"
    return schedule_class, slots


def minutes_in_week(dow: int, hour: int, minute: int) -> int:
    return dow * 24 * 60 + hour * 60 + minute


def parse_timeout_hours(timeout: str | None) -> float | None:
    if not timeout:
        return None
    total = 0.0
    for amount, unit in re.findall(r"(\d+)(h|m|s)", timeout.lower()):
        n = float(amount)
        if unit == "h":
            total += n
        elif unit == "m":
            total += n / 60.0
        elif unit == "s":
            total += n / 3600.0
    return total or None


def round_duration_hours(hours: float) -> float:
    """Round to 15-minute planning buckets."""
    return max(0.25, round(hours * 4) / 4.0)


def fetch_sippy_jobs_for_release(release: str, timeout: float = 60.0) -> list[dict[str, Any]]:
    url = f"{SIPPY_API}/api/jobs?release={urllib.request.quote(release)}"
    req = urllib.request.Request(url, headers={"User-Agent": "ibmz-lease-capacity/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.load(resp)
    return data if isinstance(data, list) else []


def build_sippy_index(releases: list[str]) -> dict[str, Any]:
    """Fetch Sippy job stats and index by prow name / brief name / release."""
    by_name: dict[str, dict[str, Any]] = {}
    by_brief: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_release_brief: dict[tuple[str, str], dict[str, Any]] = {}
    fetched: list[str] = []
    errors: list[str] = []

    for release in releases:
        try:
            jobs = fetch_sippy_jobs_for_release(release)
            fetched.append(release)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as exc:
            errors.append(f"{release}: {exc}")
            print(f"WARN: Sippy fetch failed for {release}: {exc}", file=sys.stderr)
            continue
        for job in jobs:
            name = job.get("name") or ""
            brief = job.get("brief_name") or ""
            if not name:
                continue
            entry = {
                "name": name,
                "brief_name": brief,
                "release": release,
                "current_average_duration_minutes": job.get("current_average_duration_minutes") or 0,
                "previous_average_duration_minutes": job.get("previous_average_duration_minutes") or 0,
                "current_runs": job.get("current_runs") or 0,
                "previous_runs": job.get("previous_runs") or 0,
                "current_pass_percentage": job.get("current_pass_percentage"),
                "test_grid_url": job.get("test_grid_url") or "",
            }
            # Prefer same-release exact name; keep first seen otherwise.
            prev = by_name.get(name)
            if prev is None or prev.get("release") == release or (
                entry["current_runs"] > (prev.get("current_runs") or 0)
            ):
                by_name[name] = entry
            if brief:
                by_brief[brief].append(entry)
                by_release_brief[(release, brief)] = entry
        print(f"  Sippy {release}: {len(jobs)} jobs", flush=True)

    return {
        "by_name": by_name,
        "by_brief": dict(by_brief),
        "by_release_brief": by_release_brief,
        "fetched_releases": fetched,
        "errors": errors,
    }


def planning_minutes_from_sippy(entry: dict[str, Any]) -> float | None:
    """Choose a conservative observed duration in minutes from Sippy averages."""
    cur = float(entry.get("current_average_duration_minutes") or 0)
    prev = float(entry.get("previous_average_duration_minutes") or 0)
    cur_runs = int(entry.get("current_runs") or 0)
    prev_runs = int(entry.get("previous_runs") or 0)
    samples = []
    if cur > 0 and cur_runs > 0:
        samples.append(cur)
    if prev > 0 and prev_runs > 0:
        samples.append(prev)
    if not samples:
        return None
    # Prefer the higher of recent windows so short infra-fail averages don't
    # under-count lease hold time for capacity planning.
    return max(samples)


def match_sippy_entry(job: dict[str, Any], index: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    by_name: dict[str, dict[str, Any]] = index.get("by_name") or {}
    by_brief: dict[str, list[dict[str, Any]]] = index.get("by_brief") or {}
    by_release_brief: dict[tuple[str, str], dict[str, Any]] = index.get("by_release_brief") or {}

    prow = job.get("prow_job_name")
    if prow and prow in by_name:
        return by_name[prow], "sippy-name"

    release = job.get("ocp_version") or ""
    brief = job.get("as") or ""
    if release and brief and (release, brief) in by_release_brief:
        return by_release_brief[(release, brief)], "sippy-brief-release"

    if brief and brief in by_brief:
        candidates = by_brief[brief]
        # Prefer same OCP version, else newest-looking release string, else most runs.
        same = [c for c in candidates if c.get("release") == release]
        pool = same or candidates

        def score(c: dict[str, Any]) -> tuple:
            return (int(c.get("current_runs") or 0), float(c.get("current_average_duration_minutes") or 0))

        return max(pool, key=score), "sippy-brief"

    if prow:
        # Suffix fallback across indexed names (upgrade variants, etc.).
        suffix = f"-{brief}" if brief else None
        best = None
        best_score = (-1, -1.0)
        for name, entry in by_name.items():
            if name == prow or (suffix and name.endswith(suffix)):
                sc = (
                    int(entry.get("current_runs") or 0),
                    float(entry.get("current_average_duration_minutes") or 0),
                )
                if sc > best_score:
                    best = entry
                    best_score = sc
        if best is not None:
            return best, "sippy-suffix"

    return None, ""


def workflow_median_hours(jobs: list[dict[str, Any]]) -> dict[str, float]:
    buckets: dict[str, list[float]] = defaultdict(list)
    for job in jobs:
        source = job.get("duration_source") or ""
        if source.startswith("sippy") and job.get("duration_hours"):
            wf = job.get("workflow") or ""
            if wf:
                buckets[wf].append(float(job["duration_hours"]))
    return {wf: statistics.median(vals) for wf, vals in buckets.items() if vals}


def enrich_job_durations(
    jobs: list[dict[str, Any]],
    *,
    fetch_sippy: bool,
    default_hours: float,
    min_hours: float,
    max_hours: float,
) -> dict[str, Any]:
    """Attach duration_hours / duration_source / sippy fields to each job."""
    meta: dict[str, Any] = {
        "enabled": fetch_sippy,
        "default_hours": default_hours,
        "fetched_releases": [],
        "errors": [],
        "matched": 0,
        "workflow_fallback": 0,
        "default_fallback": 0,
    }

    if not fetch_sippy:
        for job in jobs:
            job["duration_hours"] = round_duration_hours(default_hours)
            job["duration_source"] = "default"
            job["duration_minutes"] = int(round(job["duration_hours"] * 60))
            job["sippy"] = None
        meta["default_fallback"] = len(jobs)
        return meta

    releases = sorted(
        {j["ocp_version"] for j in jobs if re.fullmatch(r"\d+\.\d+", j.get("ocp_version") or "")},
        key=lambda v: [int(x) for x in v.split(".")],
    )
    print(f"Fetching Sippy durations for releases: {', '.join(releases) or '(none)'}", flush=True)
    index = build_sippy_index(releases)
    meta["fetched_releases"] = index.get("fetched_releases") or []
    meta["errors"] = index.get("errors") or []

    # First pass: direct Sippy matches
    for job in jobs:
        entry, how = match_sippy_entry(job, index)
        minutes = planning_minutes_from_sippy(entry) if entry else None
        if entry and minutes:
            hours = minutes / 60.0
            hours = min(max(hours, min_hours), max_hours)
            timeout_h = parse_timeout_hours(job.get("timeout"))
            if timeout_h:
                hours = min(hours, timeout_h)
            job["duration_hours"] = round_duration_hours(hours)
            job["duration_minutes"] = int(round(job["duration_hours"] * 60))
            job["duration_source"] = how
            job["sippy"] = {
                "name": entry.get("name"),
                "release": entry.get("release"),
                "current_avg_min": entry.get("current_average_duration_minutes"),
                "previous_avg_min": entry.get("previous_average_duration_minutes"),
                "current_runs": entry.get("current_runs"),
                "previous_runs": entry.get("previous_runs"),
                "pass_pct": entry.get("current_pass_percentage"),
                "planning_min": int(round(minutes)),
            }
            meta["matched"] += 1
        else:
            job["duration_hours"] = None
            job["duration_minutes"] = None
            job["duration_source"] = None
            job["sippy"] = None

    wf_medians = workflow_median_hours(jobs)
    meta["workflow_medians_hours"] = {k: round(v, 2) for k, v in sorted(wf_medians.items())}

    # Raise suspiciously short Sippy averages (often early infra fails) toward
    # the workflow median so capacity planning does not under-count holds.
    for job in jobs:
        source = job.get("duration_source") or ""
        if not source.startswith("sippy"):
            continue
        wf = job.get("workflow") or ""
        if wf not in wf_medians:
            continue
        current = float(job.get("duration_hours") or 0)
        floor = wf_medians[wf]
        if current > 0 and current < max(1.0, floor * 0.5):
            raised = min(max(floor, min_hours), max_hours)
            job["duration_hours"] = round_duration_hours(raised)
            job["duration_minutes"] = int(round(job["duration_hours"] * 60))
            job["duration_source"] = "sippy+workflow-floor"

    # Second pass: workflow / default fallbacks
    for job in jobs:
        if job.get("duration_hours") is not None:
            continue
        wf = job.get("workflow") or ""
        if wf in wf_medians:
            hours = min(max(wf_medians[wf], min_hours), max_hours)
            job["duration_hours"] = round_duration_hours(hours)
            job["duration_minutes"] = int(round(job["duration_hours"] * 60))
            job["duration_source"] = "workflow-median"
            meta["workflow_fallback"] += 1
        else:
            job["duration_hours"] = round_duration_hours(default_hours)
            job["duration_minutes"] = int(round(job["duration_hours"] * 60))
            job["duration_source"] = "default"
            meta["default_fallback"] += 1

    return meta


def build_concurrency(
    jobs: list[dict[str, Any]],
    default_duration_hours: float,
) -> tuple[list[list[int]], list[list[list[dict[str, Any]]]]]:
    """Build 7x24 demand grid and per-cell overlapping job details."""
    week_min = 7 * 24 * 60
    demand = [[0 for _ in range(24)] for _ in range(7)]
    details: list[list[list[dict[str, Any]]]] = [[[] for _ in range(24)] for _ in range(7)]

    for job in jobs:
        if job.get("schedule_class") not in ("weekly", "daily"):
            continue
        duration_hours = float(job.get("duration_hours") or default_duration_hours)
        duration_min = max(1, int(round(duration_hours * 60)))
        detail = {
            "as": job["as"],
            "ocp_version": job["ocp_version"],
            "cron": job["cron"],
            "profile": job["profile"],
            "source_file": job["source_file"],
            "workflow": job.get("workflow"),
            "duration_hours": duration_hours,
            "duration_source": job.get("duration_source"),
        }
        for dow, hour, minute in job.get("slots") or []:
            start = minutes_in_week(dow, hour, minute)
            entry = {**detail, "start": f"{DAY_NAMES[dow]} {hour:02d}:{minute:02d}"}
            covered_hours: set[tuple[int, int]] = set()
            t = start
            end = start + duration_min
            while t < end:
                abs_t = t % week_min
                d = abs_t // (24 * 60)
                h = (abs_t % (24 * 60)) // 60
                covered_hours.add((d, h))
                next_hour = ((abs_t // 60) + 1) * 60
                t += next_hour - abs_t
            for d, h in covered_hours:
                demand[d][h] += 1
                details[d][h].append(entry)
    return demand, details


def rank_slots(
    demand: list[list[int]], capacity: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cells = []
    for d in range(7):
        for h in range(24):
            cells.append(
                {
                    "day": DAY_NAMES[d],
                    "dow": d,
                    "hour": h,
                    "demand": demand[d][h],
                    "capacity": capacity,
                    "spare": capacity - demand[d][h],
                }
            )
    busiest = sorted(cells, key=lambda c: (-c["demand"], c["dow"], c["hour"]))
    free = sorted(
        [c for c in cells if c["spare"] >= 1],
        key=lambda c: (-c["spare"], c["dow"], c["hour"]),
    )
    return busiest, free


def contiguous_free_blocks(
    demand: list[list[int]], capacity: int, min_spare: int = 1, min_hours: int = 2
) -> list[dict[str, Any]]:
    """Find weekday-friendly contiguous free windows for scheduling advice."""
    blocks: list[dict[str, Any]] = []
    for d in range(7):
        h = 0
        while h < 24:
            spare = capacity - demand[d][h]
            if spare < min_spare:
                h += 1
                continue
            start = h
            min_sp = spare
            while h < 24 and (capacity - demand[d][h]) >= min_spare:
                min_sp = min(min_sp, capacity - demand[d][h])
                h += 1
            length = h - start
            if length >= min_hours:
                blocks.append(
                    {
                        "day": DAY_NAMES[d],
                        "dow": d,
                        "start_hour": start,
                        "end_hour": h,
                        "hours": length,
                        "min_spare": min_sp,
                    }
                )
    blocks.sort(key=lambda b: (-b["min_spare"], -b["hours"], b["dow"], b["start_hour"]))
    return blocks


def workflow_summary(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for job in jobs:
        groups[job.get("workflow") or "(none)"].append(job)
    rows = []
    for workflow, items in sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        durs = [float(j["duration_hours"]) for j in items if j.get("duration_hours") is not None]
        sippy_n = sum(1 for j in items if (j.get("duration_source") or "").startswith("sippy"))
        rows.append(
            {
                "workflow": workflow,
                "jobs": len(items),
                "profiles": sorted({j["profile"] for j in items}),
                "median_duration_hours": round(statistics.median(durs), 2) if durs else None,
                "sippy_matched": sippy_n,
            }
        )
    return rows


def write_csv(
    path: Path,
    jobs: list[dict[str, Any]],
    lease_map: dict[str, str],
    leases: dict[str, list[str]],
) -> None:
    fields = [
        "profile",
        "lease_type",
        "lease_count",
        "job",
        "ocp_version",
        "cron",
        "interval",
        "schedule_class",
        "timeout",
        "workflow",
        "duration_hours",
        "duration_source",
        "sippy_planning_min",
        "sippy_current_avg_min",
        "sippy_runs",
        "prow_job_name",
        "source_file",
        "weekly_starts",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for job in sorted(jobs, key=lambda j: (j["profile"], j["ocp_version"], j["as"])):
            lt = lease_map.get(job["profile"], "")
            sippy = job.get("sippy") or {}
            writer.writerow(
                {
                    "profile": job["profile"],
                    "lease_type": lt,
                    "lease_count": len(leases.get(lt, [])),
                    "job": job["as"],
                    "ocp_version": job["ocp_version"],
                    "cron": job["cron"] or "",
                    "interval": job["interval"] or "",
                    "schedule_class": job.get("schedule_class", ""),
                    "timeout": job["timeout"] or "",
                    "workflow": job["workflow"] or "",
                    "duration_hours": job.get("duration_hours") or "",
                    "duration_source": job.get("duration_source") or "",
                    "sippy_planning_min": sippy.get("planning_min", ""),
                    "sippy_current_avg_min": sippy.get("current_avg_min", ""),
                    "sippy_runs": sippy.get("current_runs", ""),
                    "prow_job_name": job.get("prow_job_name") or "",
                    "source_file": job["source_file"],
                    "weekly_starts": len(job.get("slots") or []),
                }
            )


def schedule_fingerprint(
    jobs: list[dict[str, Any]],
    lease_map: dict[str, str],
    leases: dict[str, list[str]],
    duration_meta: dict[str, Any],
) -> str:
    """Stable hash of schedule + capacity + duration-model inputs."""
    rows: list[str] = [
        f"duration_default={duration_meta.get('default_hours')}",
        f"sippy_enabled={bool(duration_meta.get('enabled'))}",
    ]
    for profile in sorted(lease_map):
        lt = lease_map[profile]
        names = ",".join(leases.get(lt, []))
        rows.append(f"lease|{profile}|{lt}|{len(leases.get(lt, []))}|{names}")
    for job in sorted(jobs, key=lambda j: (j["profile"], j["as"], j["source_file"])):
        rows.append(
            "|".join(
                [
                    "job",
                    job["profile"],
                    job["as"],
                    job.get("cron") or "",
                    job.get("interval") or "",
                    job.get("ocp_version") or "",
                    job.get("workflow") or "",
                    job.get("source_file") or "",
                    job.get("timeout") or "",
                    str(job.get("duration_hours") or ""),
                    job.get("duration_source") or "",
                ]
            )
        )
    return hashlib.sha256("\n".join(rows).encode("utf-8")).hexdigest()


def render_html(
    out_path: Path,
    profiles: list[str],
    lease_map: dict[str, str],
    leases: dict[str, list[str]],
    jobs: list[dict[str, Any]],
    per_profile: dict[str, dict[str, Any]],
    duration_meta: dict[str, Any],
    generated_at: str,
    source_meta: dict[str, str],
    fingerprint: str,
) -> None:
    payload = {
        "generated_at": generated_at,
        "duration_meta": duration_meta,
        "day_names": DAY_NAMES,
        "source": source_meta,
        "fingerprint": fingerprint,
        "workflows": workflow_summary(jobs),
        "profiles": [],
    }
    for profile in profiles:
        lt = lease_map.get(profile, "")
        names = leases.get(lt, [])
        info = per_profile[profile]
        payload["profiles"].append(
            {
                "name": profile,
                "lease_type": lt,
                "lease_names": names,
                "capacity": len(names),
                "job_count": info["job_count"],
                "weekly_job_count": info["weekly_job_count"],
                "low_frequency_job_count": info["low_frequency_job_count"],
                "demand": info["demand"],
                "details": info["details"],
                "busiest": info["busiest"][:20],
                "free": info["free"][:40],
                "free_blocks": info["free_blocks"][:12],
                "peak_demand": info["peak_demand"],
                "overcommit_hours": info["overcommit_hours"],
                "median_duration_hours": info["median_duration_hours"],
                "sippy_coverage": info["sippy_coverage"],
                "jobs": info["jobs"],
                "workflows": info["workflows"],
            }
        )

    data_json = json.dumps(payload, separators=(",", ":")).replace("<", "\\u003c")
    sippy_note = (
        "Sippy-observed runtimes (per job, with workflow/default fallback)"
        if duration_meta.get("enabled")
        else f"fixed {duration_meta.get('default_hours')}h assumption"
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>IBM Z lease schedule capacity</title>
<style>
:root {{
  --bg: #0f1419;
  --panel: #1a2332;
  --border: #2d3a4d;
  --text: #e7ecf3;
  --muted: #9aa8bc;
  --accent: #3d8bfd;
  --ok: #1f6f4a;
  --warn: #8a6d1d;
  --bad: #8b2e2e;
  --ok-bg: #143528;
  --warn-bg: #3a3014;
  --bad-bg: #3a1818;
  --idle-bg: #151c27;
}}
* {{ box-sizing: border-box; }}
body {{
  margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
  background: var(--bg); color: var(--text); line-height: 1.45;
}}
header {{
  padding: 1.25rem 1.5rem; border-bottom: 1px solid var(--border);
  background: linear-gradient(180deg, #152033, var(--bg));
}}
header h1 {{ margin: 0 0 .35rem; font-size: 1.35rem; font-weight: 650; }}
header p {{ margin: .2rem 0; color: var(--muted); font-size: .92rem; }}
.toolbar {{
  display: flex; flex-wrap: wrap; gap: .5rem; align-items: center; margin-top: .75rem;
}}
.toggle {{
  display: inline-flex; border: 1px solid var(--border); border-radius: 8px; overflow: hidden;
}}
.toggle button {{
  background: var(--panel); color: var(--muted); border: 0; padding: .35rem .7rem;
  cursor: pointer; font-size: .8rem;
}}
.toggle button.active {{ background: #1e2f4a; color: var(--text); }}
main {{ padding: 1rem 1.5rem 3rem; max-width: 1480px; margin: 0 auto; }}
.tabs {{ display: flex; flex-wrap: wrap; gap: .45rem; margin: 1rem 0; }}
.tab {{
  background: var(--panel); border: 1px solid var(--border); color: var(--text);
  padding: .55rem .8rem; border-radius: 8px; cursor: pointer; font-size: .78rem;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  max-width: 100%;
}}
.tab.active {{ border-color: var(--accent); background: #1e2f4a; }}
.tab .count {{ color: var(--muted); font-family: ui-sans-serif, system-ui, sans-serif; }}
.panel {{
  background: var(--panel); border: 1px solid var(--border); border-radius: 10px;
  padding: 1rem; margin-bottom: 1rem;
}}
.panel h2 {{ margin: 0 0 .75rem; font-size: 1.05rem; }}
.meta {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: .75rem; }}
.metric {{
  background: var(--bg); border: 1px solid var(--border); border-radius: 8px; padding: .75rem;
}}
.metric .label {{ color: var(--muted); font-size: .72rem; text-transform: uppercase; letter-spacing: .04em; }}
.metric .value {{ font-size: 1.15rem; font-weight: 650; margin-top: .2rem; word-break: break-word; }}
.heatmap-wrap {{ overflow-x: auto; }}
table.heatmap {{ border-collapse: collapse; font-size: .72rem; min-width: 980px; }}
table.heatmap th, table.heatmap td {{
  border: 1px solid var(--border); width: 2.35rem; height: 2.1rem; text-align: center;
}}
table.heatmap th {{ color: var(--muted); font-weight: 500; background: var(--bg); line-height: 1.15; }}
table.heatmap th .sub {{ display:block; font-size: .62rem; opacity: .85; }}
table.heatmap td {{ cursor: pointer; }}
table.heatmap td.idle {{ background: var(--idle-bg); color: var(--muted); }}
table.heatmap td.ok {{ background: var(--ok-bg); color: #8fd6b0; }}
table.heatmap td.warn {{ background: var(--warn-bg); color: #e6c86a; }}
table.heatmap td.bad {{ background: var(--bad-bg); color: #f0a0a0; }}
.legend {{ display: flex; gap: 1rem; flex-wrap: wrap; color: var(--muted); font-size: .8rem; margin: .5rem 0 0; }}
.legend span {{ display: inline-flex; align-items: center; gap: .35rem; }}
.swatch {{ width: .85rem; height: .85rem; border-radius: 3px; border: 1px solid var(--border); display: inline-block; }}
.cols {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }}
@media (max-width: 980px) {{ .cols {{ grid-template-columns: 1fr; }} }}
ul.slots {{ list-style: none; padding: 0; margin: 0; max-height: 280px; overflow: auto; }}
ul.slots li {{
  padding: .45rem .55rem; border-bottom: 1px solid var(--border); font-size: .85rem;
  display: flex; justify-content: space-between; gap: .5rem;
}}
.badge {{
  font-size: .72rem; padding: .1rem .4rem; border-radius: 4px; background: var(--bg);
  border: 1px solid var(--border); color: var(--muted); white-space: nowrap;
}}
.filters {{ display: flex; flex-wrap: wrap; gap: .5rem; margin-bottom: .75rem; }}
.filters input, .filters select {{
  background: var(--bg); color: var(--text); border: 1px solid var(--border);
  border-radius: 6px; padding: .4rem .55rem; font-size: .85rem;
}}
table.jobs, table.wf {{ width: 100%; border-collapse: collapse; font-size: .8rem; }}
table.jobs th, table.jobs td, table.wf th, table.wf td {{
  border-bottom: 1px solid var(--border); padding: .4rem .45rem; text-align: left; vertical-align: top;
}}
table.jobs th, table.wf th {{ color: var(--muted); font-weight: 560; position: sticky; top: 0; background: var(--panel); }}
.jobs-wrap {{ max-height: 480px; overflow: auto; }}
#cell-detail {{
  margin-top: .75rem; padding: .75rem; background: var(--bg); border: 1px solid var(--border);
  border-radius: 8px; font-size: .85rem; display: none;
}}
#cell-detail h3 {{ margin: 0 0 .5rem; font-size: .95rem; }}
code, .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }}
.lease-names {{ color: var(--muted); font-size: .8rem; word-break: break-all; }}
a {{ color: var(--accent); }}
.pill {{
  display: inline-block; font-size: .68rem; padding: .05rem .35rem; border-radius: 999px;
  border: 1px solid var(--border); color: var(--muted); margin-left: .25rem;
}}
.pill.sippy {{ color: #8fd6b0; border-color: #2a5a40; }}
.pill.workflow {{ color: #e6c86a; border-color: #5a4a20; }}
.pill.default {{ color: var(--muted); }}
</style>
</head>
<body>
<header>
  <h1>IBM Z lease schedule capacity</h1>
  <p>Configured cron demand vs Boskos lease capacity for full <code>libvirt-s390x-vpn*</code> cluster profiles.</p>
  <p>Generated <span id="generated"></span> · duration model: <strong id="duration-model">{html_escape(sippy_note)}</strong></p>
  <p>Source <a id="source-link" href="https://github.com/openshift/release" target="_blank" rel="noopener">openshift/release</a>
     @ <code id="source-sha"></code>
     <span id="source-subject" style="color:var(--muted)"></span></p>
  <div class="toolbar">
    <div class="toggle" id="tz-toggle" title="Display timezone">
      <button type="button" data-tz="UTC" class="active">UTC</button>
      <button type="button" data-tz="IST">IST (UTC+5:30)</button>
    </div>
    <span style="color:var(--muted);font-size:.8rem">Heatmap planning remains cron-UTC; labels convert for reading.</span>
  </div>
</header>
<main>
  <div class="tabs" id="tabs"></div>
  <section class="panel">
    <h2>Profile summary</h2>
    <p id="profile-title" class="mono" style="margin:.2rem 0 .8rem;font-size:.95rem"></p>
    <div class="meta" id="metrics"></div>
    <p class="lease-names" id="lease-names" style="margin-top:.75rem"></p>
  </section>
  <section class="panel">
    <h2>Weekly concurrency heatmap</h2>
    <p style="color:var(--muted);font-size:.85rem;margin-top:0">
      Each cell is concurrent jobs assumed to hold a lease during that hour (cron schedule in UTC).
      Hold duration uses Sippy averages when available. Color vs capacity: idle / OK / near (≥80%) / overcommit.
      Click a cell for overlapping jobs.
    </p>
    <div class="heatmap-wrap">
      <table class="heatmap" id="heatmap"></table>
    </div>
    <div class="legend">
      <span><i class="swatch" style="background:var(--idle-bg)"></i> idle (0)</span>
      <span><i class="swatch" style="background:var(--ok-bg)"></i> under capacity</span>
      <span><i class="swatch" style="background:var(--warn-bg)"></i> ≥80% capacity</span>
      <span><i class="swatch" style="background:var(--bad-bg)"></i> overcommit</span>
    </div>
    <div id="cell-detail"></div>
  </section>
  <div class="cols">
    <section class="panel">
      <h2>Best free windows</h2>
      <p style="color:var(--muted);font-size:.8rem;margin-top:0">Longest contiguous blocks with spare ≥ 1 (good places to add jobs).</p>
      <ul class="slots" id="free-blocks"></ul>
      <h2 style="margin-top:1rem">Top free hours</h2>
      <ul class="slots" id="free-slots"></ul>
    </section>
    <section class="panel">
      <h2>Busiest hours</h2>
      <ul class="slots" id="busy-slots"></ul>
    </section>
  </div>
  <section class="panel">
    <h2>Workflows on this profile</h2>
    <div class="jobs-wrap" style="max-height:260px">
      <table class="wf">
        <thead>
          <tr><th>Workflow</th><th>Jobs</th><th>Median duration</th><th>Sippy matched</th></tr>
        </thead>
        <tbody id="wf-body"></tbody>
      </table>
    </div>
  </section>
  <section class="panel">
    <h2>Jobs</h2>
    <div class="filters">
      <input id="filter-text" placeholder="Filter job / cron / workflow / file…" style="min-width:240px"/>
      <select id="filter-version"><option value="">All versions</option></select>
      <select id="filter-class">
        <option value="">All schedule classes</option>
        <option value="daily">daily</option>
        <option value="weekly">weekly</option>
        <option value="low-frequency">low-frequency</option>
        <option value="invalid">invalid</option>
      </select>
      <select id="filter-duration">
        <option value="">All duration sources</option>
        <option value="sippy">Sippy observed</option>
        <option value="workflow">Workflow median</option>
        <option value="default">Default fallback</option>
      </select>
    </div>
    <div class="jobs-wrap">
      <table class="jobs">
        <thead>
          <tr>
            <th>Job</th><th>OCP</th><th>Cron</th><th>Class</th>
            <th>Duration</th><th>Workflow</th><th>Links</th>
          </tr>
        </thead>
        <tbody id="jobs-body"></tbody>
      </table>
    </div>
  </section>
  <section class="panel">
    <h2>How to read this</h2>
    <ul style="color:var(--muted);font-size:.9rem;margin:0;padding-left:1.1rem">
      <li>Profile tabs show the <strong>full</strong> cluster profile name (e.g. <code>libvirt-s390x-vpn-amd64</code>).</li>
      <li>Prow cron times are UTC. Capacity comes from Boskos <code>*-quota-slice</code> name lists.</li>
      <li>Each periodic holds <strong>1 lease</strong> for its modeled duration after each cron start.</li>
      <li>Durations prefer Sippy <code>current/previous_average_duration_minutes</code> (conservative max), then workflow median, then the configured default.</li>
      <li><code>low-frequency</code> jobs (day-of-month / @yearly) are listed but excluded from the weekly heatmap.</li>
      <li>This is a configured-schedule planning model, not live Boskos occupancy.</li>
      <li>Dashboard refreshes daily at 08:00 IST from <code>openshift/release</code> (+ Sippy durations).</li>
      <li>Raw data: <a href="jobs.csv">jobs.csv</a> · <a href="summary.json">summary.json</a></li>
    </ul>
  </section>
</main>
<script id="report-data" type="application/json">{data_json}</script>
<script>
const DATA = JSON.parse(document.getElementById('report-data').textContent);
const RELEASE_BLOB_BASE = '{RELEASE_BLOB_BASE}';
const PROW_HISTORY_BASE = '{PROW_HISTORY_BASE}';
let active = 0;
let tz = 'UTC';

document.getElementById('generated').textContent = DATA.generated_at;
if (DATA.source) {{
  const link = document.getElementById('source-link');
  if (DATA.source.source_url) link.href = DATA.source.source_url;
  document.getElementById('source-sha').textContent = DATA.source.short_sha || 'unknown';
  document.getElementById('source-subject').textContent = DATA.source.subject ? ' — ' + DATA.source.subject : '';
}}

document.querySelectorAll('#tz-toggle button').forEach(btn => {{
  btn.onclick = () => {{
    tz = btn.dataset.tz;
    document.querySelectorAll('#tz-toggle button').forEach(b => b.classList.toggle('active', b === btn));
    renderAll();
  }};
}});

function cellClass(demand, capacity) {{
  if (demand <= 0) return 'idle';
  if (demand > capacity) return 'bad';
  if (capacity > 0 && demand / capacity >= 0.8) return 'warn';
  return 'ok';
}}

function formatHourLabel(hour) {{
  if (tz === 'UTC') return String(hour).padStart(2, '0');
  // UTC hour start → IST clock (UTC+5:30)
  const total = hour * 60 + 5 * 60 + 30;
  const hh = Math.floor((total % (24 * 60)) / 60);
  const mm = total % 60;
  return String(hh).padStart(2,'0') + ':' + String(mm).padStart(2,'0');
}}

function formatSlot(day, hour) {{
  if (tz === 'UTC') return `${{day}} ${{String(hour).padStart(2,'0')}}:00 UTC`;
  const total = hour * 60 + 5 * 60 + 30;
  let dow = DATA.day_names.indexOf(day);
  let dayCarry = Math.floor(total / (24 * 60));
  const mins = ((total % (24 * 60)) + 24 * 60) % (24 * 60);
  const hh = Math.floor(mins / 60);
  const mm = mins % 60;
  dow = (dow + dayCarry) % 7;
  return `${{DATA.day_names[dow]}} ${{String(hh).padStart(2,'0')}}:${{String(mm).padStart(2,'0')}} IST`;
}}

function durationPill(source) {{
  if (!source) return '';
  if (source.startsWith('sippy')) return '<span class="pill sippy">sippy</span>';
  if (source === 'workflow-median') return '<span class="pill workflow">workflow</span>';
  return '<span class="pill default">default</span>';
}}

function renderTabs() {{
  const tabs = document.getElementById('tabs');
  tabs.innerHTML = '';
  DATA.profiles.forEach((p, i) => {{
    const b = document.createElement('button');
    b.className = 'tab' + (i === active ? ' active' : '');
    b.innerHTML = `${{p.name}} <span class="count">· ${{p.job_count}} jobs · cap ${{p.capacity}}</span>`;
    b.title = p.name;
    b.onclick = () => {{ active = i; renderAll(); }};
    tabs.appendChild(b);
  }});
}}

function renderMetrics(p) {{
  document.getElementById('profile-title').textContent = p.name;
  const items = [
    ['Capacity', p.capacity],
    ['Jobs', p.job_count],
    ['Weekly/daily in heatmap', p.weekly_job_count],
    ['Low-frequency', p.low_frequency_job_count],
    ['Peak demand', p.peak_demand],
    ['Overcommit hours / week', p.overcommit_hours],
    ['Median hold duration', (p.median_duration_hours != null ? p.median_duration_hours + 'h' : '—')],
    ['Sippy coverage', p.sippy_coverage != null ? Math.round(p.sippy_coverage * 100) + '%' : '—'],
  ];
  document.getElementById('metrics').innerHTML = items.map(([label, value]) =>
    `<div class="metric"><div class="label">${{label}}</div><div class="value">${{value}}</div></div>`
  ).join('');
  document.getElementById('lease-names').innerHTML =
    `<strong>lease_type</strong> <code>${{p.lease_type}}</code><br/>` +
    `<strong>leases</strong> ${{p.lease_names.map(n => `<code>${{n}}</code>`).join(', ') || '(none found)'}}`;
}}

function renderHeatmap(p) {{
  const days = DATA.day_names;
  let html = '<tr><th></th>' + [...Array(24).keys()].map(h =>
    `<th>${{formatHourLabel(h)}}${{tz==='IST' ? '<span class="sub">IST</span>' : '<span class="sub">UTC</span>'}}</th>`
  ).join('') + '</tr>';
  for (let d = 0; d < 7; d++) {{
    html += `<tr><th>${{days[d]}}</th>`;
    for (let h = 0; h < 24; h++) {{
      const demand = p.demand[d][h];
      const cls = cellClass(demand, p.capacity);
      const tip = `${{formatSlot(days[d], h)}} — demand ${{demand}} / ${{p.capacity}}`;
      html += `<td class="${{cls}}" data-d="${{d}}" data-h="${{h}}" title="${{tip}}">${{demand}}</td>`;
    }}
    html += '</tr>';
  }}
  const table = document.getElementById('heatmap');
  table.innerHTML = html;
  table.querySelectorAll('td[data-d]').forEach(td => {{
    td.onclick = () => showCell(p, +td.dataset.d, +td.dataset.h);
  }});
}}

function showCell(p, d, h) {{
  const el = document.getElementById('cell-detail');
  const jobs = p.details[d][h] || [];
  el.style.display = 'block';
  el.innerHTML = `<h3>${{formatSlot(DATA.day_names[d], h)}} — ${{jobs.length}} overlapping job(s) (capacity ${{p.capacity}})</h3>` +
    (jobs.length ? `<ul>${{jobs.map(j =>
      `<li><code>${{j.as}}</code> · OCP ${{j.ocp_version}} · starts ${{j.start}} UTC · hold ${{j.duration_hours}}h ${{durationPill(j.duration_source)}} · <span class="mono">${{j.workflow || ''}}</span></li>`
    ).join('')}}</ul>` : '<p style="color:var(--muted)">No overlapping weekly/daily jobs in this hour.</p>');
}}

function renderSlots(p) {{
  const blocks = document.getElementById('free-blocks');
  const free = document.getElementById('free-slots');
  const busy = document.getElementById('busy-slots');
  blocks.innerHTML = (p.free_blocks || []).map(b =>
    `<li><span>${{formatSlot(b.day, b.start_hour)}} → ${{String(b.end_hour).padStart(2,'0')}}:00 (${{b.hours}}h)</span><span class="badge">min spare ${{b.min_spare}}</span></li>`
  ).join('') || '<li style="color:var(--muted)">No multi-hour free windows under current model.</li>';
  free.innerHTML = p.free.slice(0, 18).map(c =>
    `<li><span>${{formatSlot(c.day, c.hour)}}</span><span class="badge">demand ${{c.demand}} · spare ${{c.spare}}</span></li>`
  ).join('') || '<li style="color:var(--muted)">No free capacity under current model.</li>';
  busy.innerHTML = p.busiest.slice(0, 18).map(c =>
    `<li><span>${{formatSlot(c.day, c.hour)}}</span><span class="badge">demand ${{c.demand}} / ${{c.capacity}}</span></li>`
  ).join('');
}}

function renderWorkflows(p) {{
  const rows = p.workflows || [];
  document.getElementById('wf-body').innerHTML = rows.map(w => `
    <tr>
      <td class="mono">${{w.workflow}}</td>
      <td>${{w.jobs}}</td>
      <td>${{w.median_duration_hours != null ? w.median_duration_hours + 'h' : '—'}}</td>
      <td>${{w.sippy_matched}} / ${{w.jobs}}</td>
    </tr>`).join('') || '<tr><td colspan="4" style="color:var(--muted)">No workflows.</td></tr>';
}}

function populateVersionFilter(p) {{
  const sel = document.getElementById('filter-version');
  const current = sel.value;
  const versions = [...new Set(p.jobs.map(j => j.ocp_version))].sort((a,b) => a.localeCompare(b, undefined, {{numeric:true}}));
  sel.innerHTML = '<option value="">All versions</option>' + versions.map(v => `<option value="${{v}}">${{v}}</option>`).join('');
  if (versions.includes(current)) sel.value = current;
}}

function renderJobs(p) {{
  populateVersionFilter(p);
  const q = document.getElementById('filter-text').value.toLowerCase();
  const ver = document.getElementById('filter-version').value;
  const cls = document.getElementById('filter-class').value;
  const dsrc = document.getElementById('filter-duration').value;
  const rows = p.jobs.filter(j => {{
    if (ver && j.ocp_version !== ver) return false;
    if (cls && j.schedule_class !== cls) return false;
    if (dsrc === 'sippy' && !(j.duration_source || '').startsWith('sippy')) return false;
    if (dsrc === 'workflow' && j.duration_source !== 'workflow-median') return false;
    if (dsrc === 'default' && j.duration_source !== 'default') return false;
    if (!q) return true;
    const hay = [j.as, j.cron || '', j.workflow || '', j.source_file, j.ocp_version, j.prow_job_name || ''].join(' ').toLowerCase();
    return hay.includes(q);
  }});
  document.getElementById('jobs-body').innerHTML = rows.map(j => {{
    const href = j.source_file ? (RELEASE_BLOB_BASE + '/' + j.source_file) : '#';
    const prow = j.prow_job_name ? `<a href="${{PROW_HISTORY_BASE}}/${{j.prow_job_name}}" target="_blank" rel="noopener">prow</a>` : '';
    const src = j.source_file ? `<a href="${{href}}" target="_blank" rel="noopener">config</a>` : '';
    const links = [prow, src].filter(Boolean).join(' · ');
    const dur = j.duration_hours != null ? `${{j.duration_hours}}h` : '—';
    return `
    <tr>
      <td><code>${{j.as}}</code></td>
      <td>${{j.ocp_version}}</td>
      <td class="mono">${{j.cron || j.interval || ''}}</td>
      <td>${{j.schedule_class}}</td>
      <td>${{dur}} ${{durationPill(j.duration_source)}}</td>
      <td class="mono">${{j.workflow || ''}}</td>
      <td style="font-size:.75rem;white-space:nowrap">${{links}}</td>
    </tr>`;
  }}).join('') || '<tr><td colspan="7" style="color:var(--muted)">No jobs match filters.</td></tr>';
}}

function renderAll() {{
  const p = DATA.profiles[active];
  renderTabs();
  renderMetrics(p);
  renderHeatmap(p);
  renderSlots(p);
  renderWorkflows(p);
  renderJobs(p);
  document.getElementById('cell-detail').style.display = 'none';
}}

['filter-text','filter-version','filter-class','filter-duration'].forEach(id => {{
  document.getElementById(id).addEventListener('input', () => renderJobs(DATA.profiles[active]));
  document.getElementById(id).addEventListener('change', () => renderJobs(DATA.profiles[active]));
}});

renderAll();
</script>
</body>
</html>
"""
    out_path.write_text(html, encoding="utf-8")


def html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def main() -> int:
    args = parse_args()
    release_repo: Path = args.release_repo.resolve()
    out_dir: Path = args.out_dir.resolve()
    profiles = list(dict.fromkeys(args.profiles))
    profile_set = set(profiles)

    config_dir = release_repo / "ci-operator" / "config"
    boskos_path = release_repo / "core-services" / "prow" / "02_config" / "_boskos.yaml"
    profiles_path = (
        release_repo
        / "ci-operator"
        / "step-registry"
        / "cluster-profiles"
        / "cluster-profiles-config.yaml"
    )

    for required in (config_dir, boskos_path, profiles_path):
        if not required.exists():
            print(f"ERROR: missing {required}", file=sys.stderr)
            return 1

    source_meta = git_metadata(release_repo)
    print(
        f"openshift/release @ {source_meta['short_sha']} {source_meta.get('subject', '')}",
        flush=True,
    )

    print("Parsing cluster profile lease map …", flush=True)
    lease_map = parse_profile_lease_map(profiles_path, profile_set)
    missing_map = [p for p in profiles if p not in lease_map]
    if missing_map:
        print(f"WARN: no lease_type mapping for: {', '.join(missing_map)}", file=sys.stderr)

    lease_types = set(lease_map.values())
    print("Parsing Boskos lease inventory …", flush=True)
    leases = parse_boskos_leases(boskos_path, lease_types)

    print(f"Parsing jobs under {config_dir} …", flush=True)
    jobs = parse_jobs(config_dir, profile_set, release_repo)

    for job in jobs:
        if job["cron"]:
            schedule_class, slots = classify_and_expand_cron(job["cron"])
        elif job["interval"]:
            schedule_class, slots = "interval-like", []
        else:
            schedule_class, slots = "invalid", []
        job["schedule_class"] = schedule_class
        job["slots"] = slots

    duration_meta = enrich_job_durations(
        jobs,
        fetch_sippy=args.fetch_sippy_durations,
        default_hours=args.duration_hours,
        min_hours=args.sippy_min_hours,
        max_hours=args.sippy_max_hours,
    )
    print(
        f"Durations: sippy={duration_meta['matched']} "
        f"workflow={duration_meta['workflow_fallback']} "
        f"default={duration_meta['default_fallback']}",
        flush=True,
    )

    per_profile: dict[str, dict[str, Any]] = {}
    for profile in profiles:
        pjobs = [j for j in jobs if j["profile"] == profile]
        weekly_jobs = [j for j in pjobs if j["schedule_class"] in ("weekly", "daily")]
        low_freq = [j for j in pjobs if j["schedule_class"] == "low-frequency"]
        demand, details = build_concurrency(weekly_jobs, args.duration_hours)
        lt = lease_map.get(profile, "")
        capacity = len(leases.get(lt, []))
        busiest, free = rank_slots(demand, capacity)
        free_blocks = contiguous_free_blocks(demand, capacity)
        peak = max((demand[d][h] for d in range(7) for h in range(24)), default=0)
        overcommit = sum(1 for d in range(7) for h in range(24) if demand[d][h] > capacity)
        durs = [float(j["duration_hours"]) for j in pjobs if j.get("duration_hours") is not None]
        sippy_n = sum(1 for j in pjobs if (j.get("duration_source") or "").startswith("sippy"))
        per_profile[profile] = {
            "job_count": len(pjobs),
            "weekly_job_count": len(weekly_jobs),
            "low_frequency_job_count": len(low_freq),
            "demand": demand,
            "details": details,
            "busiest": busiest,
            "free": free,
            "free_blocks": free_blocks,
            "peak_demand": peak,
            "overcommit_hours": overcommit,
            "median_duration_hours": round(statistics.median(durs), 2) if durs else None,
            "sippy_coverage": (sippy_n / len(pjobs)) if pjobs else 0.0,
            "workflows": workflow_summary(pjobs),
            "jobs": [
                {
                    "as": j["as"],
                    "ocp_version": j["ocp_version"],
                    "cron": j["cron"],
                    "interval": j["interval"],
                    "schedule_class": j["schedule_class"],
                    "workflow": j["workflow"],
                    "source_file": j["source_file"],
                    "timeout": j["timeout"],
                    "weekly_starts": len(j.get("slots") or []),
                    "duration_hours": j.get("duration_hours"),
                    "duration_source": j.get("duration_source"),
                    "prow_job_name": j.get("prow_job_name"),
                    "sippy": j.get("sippy"),
                }
                for j in sorted(pjobs, key=lambda x: (x["ocp_version"], x["as"]))
            ],
        }

    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    fingerprint = schedule_fingerprint(jobs, lease_map, leases, duration_meta)

    write_csv(out_dir / "jobs.csv", jobs, lease_map, leases)

    summary = {
        "generated_at": generated_at,
        "duration_meta": duration_meta,
        "fingerprint": fingerprint,
        "source": source_meta,
        "workflows": workflow_summary(jobs),
        "profiles": {
            profile: {
                "lease_type": lease_map.get(profile),
                "lease_names": leases.get(lease_map.get(profile, ""), []),
                "capacity": len(leases.get(lease_map.get(profile, ""), [])),
                "job_count": per_profile[profile]["job_count"],
                "weekly_job_count": per_profile[profile]["weekly_job_count"],
                "low_frequency_job_count": per_profile[profile]["low_frequency_job_count"],
                "peak_demand": per_profile[profile]["peak_demand"],
                "overcommit_hours": per_profile[profile]["overcommit_hours"],
                "median_duration_hours": per_profile[profile]["median_duration_hours"],
                "sippy_coverage": per_profile[profile]["sippy_coverage"],
                "top_free_blocks": per_profile[profile]["free_blocks"][:8],
                "top_free_slots": per_profile[profile]["free"][:15],
                "top_busy_slots": per_profile[profile]["busiest"][:15],
            }
            for profile in profiles
        },
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (out_dir / "schedule.fingerprint").write_text(fingerprint + "\n", encoding="utf-8")

    render_html(
        out_dir / "index.html",
        profiles,
        lease_map,
        leases,
        jobs,
        per_profile,
        duration_meta,
        generated_at,
        source_meta,
        fingerprint,
    )

    print(f"\nWrote report to {out_dir}")
    print(f"Fingerprint: {fingerprint}")
    print(
        f"Duration model: sippy={'on' if args.fetch_sippy_durations else 'off'} "
        f"| fallback={args.duration_hours}h | jobs={len(jobs)}"
    )
    for profile in profiles:
        info = per_profile[profile]
        lt = lease_map.get(profile, "?")
        cap = len(leases.get(lt, []))
        print(
            f"  {profile}: capacity={cap} jobs={info['job_count']} "
            f"weekly={info['weekly_job_count']} low-freq={info['low_frequency_job_count']} "
            f"peak={info['peak_demand']} overcommit_hours={info['overcommit_hours']} "
            f"median_hold={info['median_duration_hours']}h "
            f"sippy={int((info['sippy_coverage'] or 0)*100)}%"
        )
        if info["free_blocks"]:
            best = info["free_blocks"][0]
            print(
                f"    best window: {best['day']} {best['start_hour']:02d}:00-"
                f"{best['end_hour']:02d}:00 ({best['hours']}h, min spare {best['min_spare']})"
            )
        if info["busiest"]:
            worst = info["busiest"][0]
            print(
                f"    busiest:   {worst['day']} {worst['hour']:02d}:00 "
                f"(demand {worst['demand']} / {worst['capacity']})"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
