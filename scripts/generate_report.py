#!/usr/bin/env python3
"""Generate an IBM Z (s390x) lease schedule capacity report.

Reads an openshift/release checkout, models weekly concurrent lease demand for
libvirt-s390x-vpn* cluster profiles, and emits a self-contained HTML dashboard
plus CSV/JSON. Optionally enriches per-job lease-hold durations from Sippy
using successful (passed) run timings, with workflow-level and default fallbacks.

Example:
  python3 scripts/generate_report.py --release-repo /path/to/openshift/release
  python3 scripts/generate_report.py --release-repo ./openshift-release --out-dir docs \\
      --fetch-sippy-durations --duration-hours 4
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
import urllib.parse
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover
    print("ERROR: PyYAML is required (pip install pyyaml)", file=sys.stderr)
    sys.exit(1)

# Fallback only if cluster-profiles-config.yaml cannot be read. Prefer
# discover_profiles() so new libvirt-s390x-vpn* profiles appear automatically.
DEFAULT_PROFILES = (
    "libvirt-s390x-vpn",
    "libvirt-s390x-vpn-amd64",
    "libvirt-s390x-vpn-arm64",
    "libvirt-s390x-vpn-hcp",
    "libvirt-s390x-vpn-oz",
)
PROFILE_PREFIX = "libvirt-s390x-vpn"

DAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
DAY_FULL = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
]
MONTH_NAMES = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]
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
        default=4.0,
        help=(
            "Fallback lease-hold duration in hours when no successful Sippy "
            "history (or workflow/timeout sample) exists (default: 4)"
        ),
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
        "--lease-buffer-minutes",
        type=float,
        default=15.0,
        help=(
            "Extra minutes added on top of observed full-job wall-clock for lease-hold "
            "modeling (covers acquire→release lag around pre/test/post; default: 15)"
        ),
    )
    parser.add_argument(
        "--profiles",
        nargs="+",
        default=None,
        help=(
            "Cluster profiles to include (default: auto-discover every "
            f"{PROFILE_PREFIX}* name from cluster-profiles-config.yaml)"
        ),
    )
    parser.add_argument(
        "--write-fingerprint",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write schedule.fingerprint for change detection (default: on)",
    )
    return parser.parse_args()


def discover_profiles(profiles_path: Path) -> list[str]:
    """Discover all libvirt-s390x-vpn* cluster profile names from release config."""
    if not profiles_path.exists():
        return list(DEFAULT_PROFILES)
    names: set[str] = set()
    for match in re.finditer(
        rf"(?m)^\s*-?\s*name:\s*({re.escape(PROFILE_PREFIX)}\S*)\s*$",
        profiles_path.read_text(encoding="utf-8"),
    ):
        names.add(match.group(1).strip().strip("'\""))
    return sorted(names) if names else list(DEFAULT_PROFILES)


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
    """Resolve the OCP version this periodic targets.

    Prefer the config filename variant (``__nightly-X.Y`` / ``__release-X.Y``).
    For upgrade jobs, ``steps.env.BRANCH`` is the *source* release, so filename
    must win — otherwise upgrades are labeled one release too low.
    """
    base = Path(str(source_file).replace("\\", "/")).name
    for pattern in (
        r"__nightly-(\d+\.\d+)",
        r"__release-(\d+\.\d+)",
        r"__ocp(\d+\.\d+)",
        r"-ocp(\d+\.\d+)",
    ):
        m = re.search(pattern, base)
        if m:
            return m.group(1)

    env = job.get("steps", {}).get("env") or {}
    branch = env.get("BRANCH") or env.get("OCP_VERSION")
    if isinstance(branch, (int, float)):
        branch = str(branch)
    if isinstance(branch, str):
        branch = branch.strip().strip("\"'")
        if re.fullmatch(r"\d+\.\d+", branch):
            return branch

    for pattern in (r"release-(\d+\.\d+)", r"-(\d+\.\d+)\.yaml$"):
        m = re.search(pattern, base)
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


def build_workflow_lease_index(step_registry: Path) -> dict[str, list[dict[str, Any]]]:
    """Map workflow `as` name -> explicit leases[] from *-workflow.yaml files."""
    index: dict[str, list[dict[str, Any]]] = {}
    if not step_registry.exists():
        return index
    for path in step_registry.rglob("*-workflow.yaml"):
        try:
            data = load_yaml(path)
        except Exception:  # noqa: BLE001
            continue
        if not isinstance(data, dict):
            continue
        root = data.get("workflow") if isinstance(data.get("workflow"), dict) else data
        if not isinstance(root, dict):
            continue
        name = root.get("as") or path.name[: -len("-workflow.yaml")]
        steps = root.get("steps") or {}
        leases = steps.get("leases") if isinstance(steps, dict) else None
        if name and isinstance(leases, list) and leases:
            index[str(name)] = [l for l in leases if isinstance(l, dict)]
    return index


def leases_held_for_job(
    job: dict[str, Any],
    lease_map: dict[str, str],
    workflow_leases: dict[str, list[dict[str, Any]]],
) -> int:
    """How many Boskos leases of this profile's type the job holds at once.

    Cluster profile always takes 1. Workflows may declare extra leases of the
    same resource_type (e.g. HyperShift extinfra holds a second HCP lease).
    """
    profile = job.get("profile") or ""
    lease_type = lease_map.get(profile, "")
    held = 1
    wf = job.get("workflow") or ""
    for entry in workflow_leases.get(wf) or []:
        if entry.get("resource_type") == lease_type:
            held += 1
    return held


def parse_jobs(
    config_dir: Path,
    profiles: set[str],
    repo: Path,
    lease_map: dict[str, str] | None = None,
    workflow_leases: dict[str, list[dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    lease_map = lease_map or {}
    workflow_leases = workflow_leases or {}
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
            interval = test.get("interval") or test.get("minimum_interval")
            if not cron and not interval:
                continue
            as_name = test.get("as", "")
            workflow = steps.get("workflow")
            timeout = test.get("timeout")
            prow_name = construct_prow_job_name(rel, str(as_name))
            job = {
                "as": as_name,
                "profile": profile,
                "cron": str(cron).strip("'\"") if cron is not None else None,
                "interval": str(interval) if interval is not None else None,
                "timeout": str(timeout) if timeout is not None else None,
                "workflow": workflow,
                "source_file": rel,
                "ocp_version": extract_ocp_version(test, rel),
                "prow_job_name": prow_name,
            }
            job["leases_held"] = leases_held_for_job(job, lease_map, workflow_leases)
            jobs.append(job)
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
            step = max(1, int(step_s))
        else:
            base = piece
        if base in ("*", "?"):
            start, end = lo, hi
        elif "-" in base:
            a, b = base.split("-", 1)
            start, end = int(a), int(b)
        else:
            # N or N/step → N, N+step, … hi
            start = int(base)
            end = hi if "/" in piece else start
        if start > end:
            start, end = end, start
        for n in range(start, end + 1, step):
            if lo <= n <= hi:
                values.add(n)
    return sorted(values)


def parse_dow_field(field: str) -> list[int]:
    """Parse cron day-of-week; accept 0–6 and Sunday as 7."""
    if field in ("*", "?"):
        return list(range(7))
    raw = parse_field(field, 0, 7)
    return sorted({0 if d == 7 else d for d in raw})


def _format_english_list(items: list[str]) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return ", ".join(items[:-1]) + f", and {items[-1]}"


def _ordinal(n: int) -> str:
    if 10 <= n % 100 <= 20:
        suf = "th"
    else:
        suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suf}"


def _describe_days(dows: list[int]) -> str:
    if set(dows) == set(range(7)):
        return "every day"
    ranges: list[tuple[int, int]] = []
    start = prev = dows[0]
    for d in dows[1:]:
        if d == prev + 1:
            prev = d
            continue
        ranges.append((start, prev))
        start = prev = d
    ranges.append((start, prev))
    parts: list[str] = []
    for a, b in ranges:
        if a == b:
            parts.append(DAY_FULL[a])
        elif b == a + 1:
            parts.extend([DAY_FULL[a], DAY_FULL[b]])
        else:
            parts.append(f"{DAY_FULL[a]}–{DAY_FULL[b]}")
    return _format_english_list(parts)


def _describe_times(minutes: list[int], hours: list[int]) -> str:
    times = [f"{h:02d}:{m:02d}" for h in hours for m in minutes]
    if len(times) == 1:
        return f"at {times[0]} UTC"
    if len(hours) == 24 and minutes == [0]:
        return "every hour at :00 UTC"
    if len(times) <= 4:
        return "at " + _format_english_list(times) + " UTC"
    if len(minutes) == 1 and hours == list(range(hours[0], hours[-1] + 1)):
        return (
            f"hourly from {hours[0]:02d}:{minutes[0]:02d} "
            f"to {hours[-1]:02d}:{minutes[0]:02d} UTC"
        )
    return f"at {len(times)} times UTC ({times[0]} … {times[-1]})"


def cron_to_english(expr: str | None) -> str:
    """Convert a 5-field Prow cron (UTC) into a plain-English schedule."""
    if not expr:
        return ""
    raw = expr.strip().strip("'\"")
    alias_note = ""
    if raw in CRON_ALIASES:
        alias_note = raw
        raw = CRON_ALIASES[raw]
    parts = raw.split()
    if len(parts) != 5:
        return f"Custom schedule ({expr})"
    minute_s, hour_s, dom_s, mon_s, dow_s = parts
    try:
        minutes = parse_field(minute_s, 0, 59)
        hours = parse_field(hour_s, 0, 23)
        doms = [] if dom_s in ("*", "?") else parse_field(dom_s, 1, 31)
        mons = [] if mon_s in ("*", "?") else parse_field(mon_s, 1, 12)
        dows = parse_dow_field(dow_s)
    except ValueError:
        return f"Custom schedule ({expr})"

    time_bit = _describe_times(minutes, hours)
    dow_restricted = dow_s not in ("*", "?")
    if doms or mons:
        month_bit = (
            _format_english_list([MONTH_NAMES[m - 1] for m in mons]) if mons else "every month"
        )
        day_bit = _format_english_list([_ordinal(d) for d in doms]) if doms else "day 1"
        if mons and len(mons) == 1 and doms and len(doms) == 1:
            text = f"Every year on {MONTH_NAMES[mons[0] - 1]} {day_bit} {time_bit}"
        elif mons and not doms:
            text = f"On every day in {month_bit} {time_bit}"
        elif doms and not mons:
            text = f"On the {day_bit} of every month {time_bit}"
        else:
            text = f"On the {day_bit} of {month_bit} {time_bit}"
        if dow_restricted and set(dows) != set(range(7)):
            text += f" or every {_describe_days(dows)}"
    elif set(dows) == set(range(7)):
        text = f"Every day {time_bit}"
    else:
        text = f"Every {_describe_days(dows)} {time_bit}"

    if alias_note:
        text += f" ({alias_note})"
    return text


def classify_and_expand_cron(expr: str) -> tuple[str, list[tuple[int, int, int]]]:
    """Return (schedule_class, weekly slots as (dow, hour, minute))."""
    raw = expr.strip().strip("'\"")
    if raw in CRON_ALIASES:
        raw = CRON_ALIASES[raw]

    parts = raw.split()
    if len(parts) != 5:
        return "invalid", []

    minute_s, hour_s, dom_s, mon_s, dow_s = parts

    try:
        minutes = parse_field(minute_s, 0, 59)
        hours = parse_field(hour_s, 0, 23)
        dows = parse_dow_field(dow_s)
    except ValueError:
        return "invalid", []

    has_dom = dom_s not in ("*", "?")
    has_mon = mon_s not in ("*", "?")
    has_dow = dow_s not in ("*", "?")

    # Month-restricted or DOM-only → not modeled on the weekly grid.
    if has_mon or (has_dom and not has_dow):
        return "low-frequency", []

    # Pure DOW, or DOM+DOW (cron OR): include weekday slots (safe over-count).
    slots = [(d, h, m) for d in dows for h in hours for m in minutes]
    schedule_class = "daily" if set(dows) == set(range(7)) else "weekly"
    return schedule_class, slots


def expand_interval(interval: str) -> tuple[str, list[tuple[int, int, int]]]:
    """Spread an interval/minimum_interval across one week of planning slots."""
    hours = parse_timeout_hours(interval)
    if not hours or hours <= 0:
        return "invalid", []
    week_min = 7 * 24 * 60
    step = max(1, int(round(hours * 60)))
    if step >= week_min:
        return "low-frequency", []
    slots: list[tuple[int, int, int]] = []
    t = 0
    while t < week_min:
        d, rem = divmod(t, 24 * 60)
        h, m = divmod(rem, 60)
        slots.append((d, h, m))
        t += step
    schedule_class = "daily" if hours <= 24 else "weekly"
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
    """Round to 15-minute planning buckets (half-up, not banker's rounding)."""
    return max(0.25, int(hours * 4 + 0.5) / 4.0)


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
                "current_passes": job.get("current_passes") or 0,
                "previous_passes": job.get("previous_passes") or 0,
                "current_pass_percentage": job.get("current_pass_percentage"),
                "previous_pass_percentage": job.get("previous_pass_percentage"),
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


def sippy_pass_counts(entry: dict[str, Any]) -> tuple[int, int]:
    """Return (current_passes, previous_passes) for a Sippy job entry."""
    return (
        int(entry.get("current_passes") or 0),
        int(entry.get("previous_passes") or 0),
    )


def sippy_has_successful_runs(entry: dict[str, Any] | None) -> bool:
    """True when Sippy has at least one successful (passed) run in recent windows."""
    if not entry:
        return False
    cur_p, prev_p = sippy_pass_counts(entry)
    return (cur_p + prev_p) > 0


def planning_minutes_from_sippy(entry: dict[str, Any]) -> float | None:
    """Choose full-job wall-clock minutes from Sippy *successful* history only.

    Sippy's job-level average mixes failed runs (often much shorter). We only
    trust a window when it has at least one pass. Prefer an explicit
    success-run median on the entry when present; otherwise use pass-bearing
    window averages (near-100% pass jobs ≈ success timing).
    """
    success = entry.get("success_median_duration_minutes")
    if success is not None:
        try:
            minutes = float(success)
        except (TypeError, ValueError):
            minutes = 0.0
        if minutes > 0:
            return minutes

    if not sippy_has_successful_runs(entry):
        return None

    cur_p, prev_p = sippy_pass_counts(entry)
    cur = float(entry.get("current_average_duration_minutes") or 0) if cur_p > 0 else 0.0
    prev = float(entry.get("previous_average_duration_minutes") or 0) if prev_p > 0 else 0.0
    if cur <= 0:
        cur = 0.0
    if prev <= 0:
        prev = 0.0
    if cur <= 0 and prev <= 0:
        return None

    cur_pct = entry.get("current_pass_percentage")
    prev_pct = entry.get("previous_pass_percentage")
    # Prefer the healthier pass window; if both look solid, take the longer
    # hold so lease occupancy is not under-counted.
    if cur > 0 and prev > 0:
        cur_healthy = cur_pct is not None and float(cur_pct) >= 90
        prev_healthy = prev_pct is not None and float(prev_pct) >= 90
        if cur_healthy and not prev_healthy:
            return cur
        if prev_healthy and not cur_healthy:
            return prev
        return max(cur, prev)
    return cur or prev


def _sippy_json(url: str, timeout: float = 45.0) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "ibmz-lease-capacity/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def fetch_sippy_success_median_minutes(
    job_name: str,
    release: str,
    *,
    limit: int = 5,
    timeout: float = 45.0,
) -> float | None:
    """Median wall-clock minutes across recent *successful* Sippy job runs."""
    if not job_name or not release:
        return None
    filt = json.dumps(
        {
            "items": [
                {"columnField": "name", "operatorValue": "equals", "value": job_name},
                {"columnField": "overall_result", "operatorValue": "equals", "value": "S"},
            ],
            "linkOperator": "and",
        }
    )
    runs_url = (
        f"{SIPPY_API}/api/jobs/runs?release={urllib.parse.quote(release)}"
        f"&filter={urllib.parse.quote(filt)}&limit={limit}"
        f"&sortField=timestamp&sort=desc"
    )
    try:
        payload = _sippy_json(runs_url, timeout=timeout)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError):
        return None
    rows = payload.get("rows") if isinstance(payload, dict) else payload
    if not isinstance(rows, list) or not rows:
        return None

    minutes: list[float] = []
    for row in rows[:limit]:
        prow_id = row.get("prow_id") or row.get("id")
        if not prow_id:
            continue
        summary_url = (
            f"{SIPPY_API}/api/job/run/summary?"
            f"prow_job_run_id={urllib.parse.quote(str(prow_id))}"
        )
        try:
            summary = _sippy_json(summary_url, timeout=timeout)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError):
            continue
        if not isinstance(summary, dict):
            continue
        if str(summary.get("overallResult") or "").upper() not in ("S", "SUCCESS", ""):
            # Empty overallResult still ok if duration present from success list.
            if summary.get("overallResult") not in (None, ""):
                continue
        secs = summary.get("durationSeconds")
        try:
            secs_f = float(secs)
        except (TypeError, ValueError):
            continue
        if secs_f > 0:
            minutes.append(secs_f / 60.0)
    if not minutes:
        return None
    return float(statistics.median(minutes))


def _needs_success_run_sample(entry: dict[str, Any]) -> bool:
    """Fetch per-success durations when job averages may be polluted by fails."""
    if not sippy_has_successful_runs(entry):
        return False
    cur_p, _prev_p = sippy_pass_counts(entry)
    cur_pct = entry.get("current_pass_percentage")
    # Near-perfect current window ≈ success timing; skip extra API calls.
    if cur_p > 0 and cur_pct is not None and float(cur_pct) >= 95:
        return False
    return True


def enrich_sippy_entries_with_success_medians(
    entries: list[dict[str, Any]],
    *,
    workers: int = 6,
    limit: int = 5,
) -> int:
    """Attach success_median_duration_minutes on entries that need a clean sample.

    Returns how many entries received a success median.
    """
    # Dedupe by (release, name)
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for entry in entries:
        if not _needs_success_run_sample(entry):
            continue
        key = (str(entry.get("release") or ""), str(entry.get("name") or ""))
        if key[0] and key[1] and key not in unique:
            unique[key] = entry
    if not unique:
        return 0

    filled = 0

    def _one(item: tuple[tuple[str, str], dict[str, Any]]) -> tuple[dict[str, Any], float | None]:
        (_key, entry) = item
        minutes = fetch_sippy_success_median_minutes(
            str(entry.get("name") or ""),
            str(entry.get("release") or ""),
            limit=limit,
        )
        return entry, minutes

    print(
        f"  Sampling successful Sippy runs for {len(unique)} jobs "
        f"(pass-mixed averages)…",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [pool.submit(_one, item) for item in unique.items()]
        for fut in as_completed(futures):
            try:
                entry, minutes = fut.result()
            except Exception as exc:  # noqa: BLE001 — keep generator resilient
                print(f"WARN: success-duration sample failed: {exc}", file=sys.stderr)
                continue
            if minutes is None or minutes <= 0:
                continue
            entry["success_median_duration_minutes"] = minutes
            filled += 1
    # Propagate onto any duplicate references of the same entry object is N/A;
    # callers pass the matched entry objects themselves.
    return filled


def _release_tuple(release: str) -> tuple[int, ...]:
    try:
        return tuple(int(x) for x in (release or "0").split("."))
    except ValueError:
        return (0,)


def _release_distance(a: str, b: str) -> int:
    ta, tb = _release_tuple(a), _release_tuple(b)
    n = max(len(ta), len(tb))
    ta = ta + (0,) * (n - len(ta))
    tb = tb + (0,) * (n - len(tb))
    # Weight major heavily so 4.23 vs 5.0 is far, 4.12 vs 4.14 is near.
    dist = 0
    for i, (x, y) in enumerate(zip(ta, tb)):
        dist += abs(x - y) * (100 ** (n - 1 - i))
    return dist


def _sippy_entry_usable(entry: dict[str, Any] | None) -> bool:
    """Usable only when Sippy has successful-run history (not fail-only averages)."""
    if not entry or not sippy_has_successful_runs(entry):
        return False
    # Either a success median or a pass-bearing window average must resolve.
    return planning_minutes_from_sippy(entry) is not None


def _is_upgrade_token(text: str | None) -> bool:
    return "upgrade" in (text or "").lower()


def _sippy_compatible(job: dict[str, Any], entry: dict[str, Any]) -> bool:
    """Reject install↔upgrade mismatches when borrowing durations."""
    job_name = job.get("prow_job_name") or ""
    entry_name = entry.get("name") or ""
    job_up = _is_upgrade_token(job_name) or _is_upgrade_token(job.get("as"))
    entry_up = _is_upgrade_token(entry_name) or _is_upgrade_token(entry.get("brief_name"))
    return job_up == entry_up


def _best_sippy_entry(
    candidates: list[dict[str, Any]],
    *,
    prefer_release: str | None = None,
    job: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    usable = [
        c
        for c in candidates
        if _sippy_entry_usable(c) and (job is None or _sippy_compatible(job, c))
    ]
    if not usable:
        return None

    def score(c: dict[str, Any]) -> tuple:
        dist = _release_distance(prefer_release or "", str(c.get("release") or "")) if prefer_release else 0
        runs = int(c.get("current_runs") or 0) + int(c.get("previous_runs") or 0)
        return (
            -dist,
            runs,
            _release_tuple(str(c.get("release") or "")),
            float(c.get("current_average_duration_minutes") or 0),
        )

    return max(usable, key=score)


def _swap_prow_version(prow: str, target_release: str) -> str | None:
    """Replace the first nightly-X.Y / release-X.Y token in a Prow job name."""
    if not prow or not target_release:
        return None
    for pat in (r"(nightly-)\d+\.\d+", r"(release-)\d+\.\d+"):
        rewritten, n = re.subn(pat, rf"\g<1>{target_release}", prow, count=1)
        if n:
            return rewritten
    return None


def match_sippy_entry(job: dict[str, Any], index: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    """Match a configured job to Sippy runtime stats.

    Prefer the same OCP version when it has usable history. If that version has
    no Sippy runs, reuse the same job identity from the nearest OCP version that
    does — never cross install↔upgrade boundaries.
    """
    by_name: dict[str, dict[str, Any]] = index.get("by_name") or {}
    by_brief: dict[str, list[dict[str, Any]]] = index.get("by_brief") or {}
    by_release_brief: dict[tuple[str, str], dict[str, Any]] = index.get("by_release_brief") or {}

    prow = job.get("prow_job_name") or ""
    release = job.get("ocp_version") or ""
    brief = job.get("as") or ""

    if prow and _sippy_entry_usable(by_name.get(prow)):
        return by_name[prow], "sippy-name"

    if release and brief:
        same_release = by_release_brief.get((release, brief))
        if _sippy_entry_usable(same_release) and _sippy_compatible(job, same_release):
            return same_release, "sippy-brief-release"

    # Same Prow identity with only the nightly/release version token changed.
    if prow:
        rewrite_hits: list[dict[str, Any]] = []
        seen: set[str] = set()
        releases = {
            str(e.get("release") or "")
            for e in by_name.values()
            if e.get("release")
        }
        for rel in releases:
            if rel == release:
                continue
            cand = _swap_prow_version(prow, rel)
            if not cand or cand in seen:
                continue
            seen.add(cand)
            entry = by_name.get(cand)
            if entry is not None:
                rewrite_hits.append(entry)
        best_rewrite = _best_sippy_entry(rewrite_hits, prefer_release=release, job=job)
        if best_rewrite is not None:
            return best_rewrite, "sippy-brief"

    if brief and brief in by_brief:
        candidates = by_brief[brief]
        best = _best_sippy_entry(candidates, prefer_release=release, job=job)
        if best is not None:
            how = "sippy-brief-release" if best.get("release") == release else "sippy-brief"
            return best, how

    return None, ""


def sippy_jobs_ui_url(release: str, job_name: str) -> str:
    """Sippy UI filtered to a single job (the one that has history)."""
    filt = json.dumps(
        {
            "items": [
                {
                    "columnField": "name",
                    "operatorValue": "equals",
                    "value": job_name,
                }
            ]
        },
        separators=(",", ":"),
    )
    return (
        f"{SIPPY_JOB_URL}?"
        + urllib.parse.urlencode({"release": release, "filter": filt})
    )


def workflow_median_hours(jobs: list[dict[str, Any]]) -> dict[str, float]:
    buckets: dict[str, list[float]] = defaultdict(list)
    for job in jobs:
        source = job.get("duration_source") or ""
        if source.startswith("sippy") and job.get("duration_hours"):
            wf = job.get("workflow") or ""
            if wf:
                buckets[wf].append(float(job["duration_hours"]))
    return {wf: statistics.median(vals) for wf, vals in buckets.items() if vals}


def apply_lease_hold_hours(
    job: dict[str, Any],
    hours: float,
    buffer_minutes: float,
    *,
    max_hours: float | None = None,
) -> None:
    """Store observed runtime and lease-hold hours (observed + release buffer)."""
    observed = round_duration_hours(hours)
    hold = round_duration_hours(observed + (buffer_minutes / 60.0))
    timeout_h = parse_timeout_hours(job.get("timeout"))
    if timeout_h:
        hold = min(hold, round_duration_hours(timeout_h))
    if max_hours is not None:
        hold = min(hold, max_hours)
    job["observed_duration_hours"] = observed
    job["duration_hours"] = hold  # used by concurrency model = leases unavailable
    job["duration_minutes"] = int(round(hold * 60))
    job["lease_buffer_minutes"] = buffer_minutes


def enrich_job_durations(
    jobs: list[dict[str, Any]],
    *,
    fetch_sippy: bool,
    default_hours: float,
    min_hours: float,
    max_hours: float,
    lease_buffer_minutes: float,
) -> dict[str, Any]:
    """Attach full-job lease-hold durations (pre→test→post wall-clock) to each job."""
    meta: dict[str, Any] = {
        "enabled": fetch_sippy,
        "default_hours": default_hours,
        "lease_buffer_minutes": lease_buffer_minutes,
        "duration_basis": (
            "Successful ProwJob wall-clock only (lease acquire through pre/test/post); "
            "fail-only Sippy averages are ignored"
        ),
        "fetched_releases": [],
        "errors": [],
        "matched": 0,
        "success_samples": 0,
        "workflow_fallback": 0,
        "default_fallback": 0,
    }

    if not fetch_sippy:
        for job in jobs:
            apply_lease_hold_hours(job, default_hours, lease_buffer_minutes)
            job["duration_source"] = "default"
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

    # Resolve matches first, then sample successful runs where averages may be
    # polluted by failures (0% pass jobs stay unmatched → default hours).
    matched_pairs: list[tuple[dict[str, Any], dict[str, Any], str]] = []
    sample_entries: list[dict[str, Any]] = []
    for job in jobs:
        entry, how = match_sippy_entry(job, index)
        if entry and sippy_has_successful_runs(entry):
            matched_pairs.append((job, entry, how))
            sample_entries.append(entry)
        else:
            job["duration_hours"] = None
            job["observed_duration_hours"] = None
            job["duration_minutes"] = None
            job["duration_source"] = None
            job["sippy"] = None

    meta["success_samples"] = enrich_sippy_entries_with_success_medians(sample_entries)

    # First pass: Sippy matches backed by successful-run timing.
    for job, entry, how in matched_pairs:
        minutes = planning_minutes_from_sippy(entry)
        if not minutes:
            job["duration_hours"] = None
            job["observed_duration_hours"] = None
            job["duration_minutes"] = None
            job["duration_source"] = None
            job["sippy"] = None
            continue
        hours = minutes / 60.0
        hours = min(max(hours, min_hours), max_hours)
        apply_lease_hold_hours(
            job, hours, lease_buffer_minutes, max_hours=max_hours
        )
        job["duration_source"] = how
        matched_name = entry.get("name") or ""
        matched_release = entry.get("release") or ""
        borrowed = bool(
            matched_release
            and job.get("ocp_version")
            and matched_release != job.get("ocp_version")
        )
        success_median = entry.get("success_median_duration_minutes")
        job["sippy"] = {
            "name": matched_name,
            "release": matched_release,
            "borrowed": borrowed,
            "job_ocp_version": job.get("ocp_version"),
            "current_avg_min": entry.get("current_average_duration_minutes"),
            "previous_avg_min": entry.get("previous_average_duration_minutes"),
            "success_median_min": (
                int(round(float(success_median))) if success_median else None
            ),
            "current_runs": entry.get("current_runs"),
            "previous_runs": entry.get("previous_runs"),
            "current_passes": entry.get("current_passes"),
            "previous_passes": entry.get("previous_passes"),
            "pass_pct": entry.get("current_pass_percentage"),
            "planning_min": int(round(minutes)),
            "basis": (
                "successful-prowjob-wall-clock"
                if success_median
                else "pass-window-prowjob-wall-clock"
            ),
            "url": sippy_jobs_ui_url(matched_release, matched_name)
            if matched_release and matched_name
            else "",
            "prow_history_name": matched_name or job.get("prow_job_name"),
        }
        meta["matched"] += 1

    def job_kind(job: dict[str, Any]) -> str:
        prow = job.get("prow_job_name") or ""
        as_name = job.get("as") or ""
        return "upgrade" if _is_upgrade_token(prow) or _is_upgrade_token(as_name) else "install"

    def duration_family(job: dict[str, Any]) -> str:
        as_name = job.get("as") or ""
        wf = job.get("workflow") or ""
        return f"{as_name}|{job_kind(job)}|{wf}"

    # Medians from observed (pre-buffer) hours so the buffer is not double-applied.
    # Key same-job medians by (as, kind) so install and upgrade never mix.
    by_as_kind: dict[tuple[str, str], list[float]] = defaultdict(list)
    by_family: dict[str, list[float]] = defaultdict(list)
    by_workflow_kind: dict[tuple[str, str], list[float]] = defaultdict(list)
    for job in jobs:
        source = job.get("duration_source") or ""
        if not (source.startswith("sippy") and job.get("observed_duration_hours")):
            continue
        obs = float(job["observed_duration_hours"])
        kind = job_kind(job)
        if job.get("as"):
            by_as_kind[(str(job["as"]), kind)].append(obs)
        by_family[duration_family(job)].append(obs)
        if job.get("workflow"):
            by_workflow_kind[(str(job["workflow"]), kind)].append(obs)
    as_medians = {k: statistics.median(v) for k, v in by_as_kind.items() if v}
    family_medians = {k: statistics.median(v) for k, v in by_family.items() if v}
    wf_medians = {k: statistics.median(v) for k, v in by_workflow_kind.items() if v}
    meta["workflow_medians_hours"] = {
        f"{wf}|{kind}": round(v, 2) for (wf, kind), v in sorted(wf_medians.items())
    }
    meta["as_medians_hours"] = {
        f"{as_name}|{kind}": round(v, 2) for (as_name, kind), v in sorted(as_medians.items())
    }

    def fallback_hours_for(job: dict[str, Any]) -> tuple[float | None, str]:
        as_name = job.get("as") or ""
        kind = job_kind(job)
        if (as_name, kind) in as_medians:
            return as_medians[(as_name, kind)], "as-median"
        fam = duration_family(job)
        if fam in family_medians:
            return family_medians[fam], "family-median"
        wf = job.get("workflow") or ""
        if (wf, kind) in wf_medians:
            return wf_medians[(wf, kind)], "workflow-median"
        # No successful Sippy / median sample: use configured default (4h).
        # Job timeout remains a clamp in apply_lease_hold_hours, not a duration guess.
        return None, ""

    # Raise suspiciously short Sippy averages toward the same-job / family floor.
    for job in jobs:
        source = job.get("duration_source") or ""
        if not source.startswith("sippy"):
            continue
        floor, floor_src = fallback_hours_for(job)
        if floor is None or not floor_src.endswith("median"):
            continue
        current = float(job.get("observed_duration_hours") or 0)
        if current > 0 and current < max(1.0, floor * 0.5):
            raised = min(max(floor, min_hours), max_hours)
            apply_lease_hold_hours(
                job, raised, lease_buffer_minutes, max_hours=max_hours
            )
            job["duration_source"] = "sippy+median-floor"

    # Second pass: same-job / family / workflow / default fallbacks
    for job in jobs:
        if job.get("duration_hours") is not None:
            continue
        hours, how = fallback_hours_for(job)
        if hours is not None:
            hours = min(max(hours, min_hours), max_hours)
            apply_lease_hold_hours(
                job, hours, lease_buffer_minutes, max_hours=max_hours
            )
            job["duration_source"] = how
            meta["workflow_fallback"] += 1
        else:
            apply_lease_hold_hours(
                job, default_hours, lease_buffer_minutes, max_hours=max_hours
            )
            job["duration_source"] = "default"
            meta["default_fallback"] += 1

    return meta


def build_concurrency(
    jobs: list[dict[str, Any]],
    default_duration_hours: float,
) -> tuple[list[list[int]], list[list[list[dict[str, Any]]]]]:
    """Build 7x24 lease-occupancy grid.

    Each scheduled job occupies ``leases_held`` leases (usually 1; some
    workflows hold extras of the same Boskos type) for its modeled lease-hold
    duration (full job wall-clock: pre → test → post + buffer).
    """
    week_min = 7 * 24 * 60
    demand = [[0 for _ in range(24)] for _ in range(7)]
    details: list[list[list[dict[str, Any]]]] = [[[] for _ in range(24)] for _ in range(7)]

    for job in jobs:
        if job.get("schedule_class") not in ("weekly", "daily"):
            continue
        duration_hours = float(job.get("duration_hours") or default_duration_hours)
        duration_min = max(1, int(round(duration_hours * 60)))
        leases_held = max(1, int(job.get("leases_held") or 1))
        detail = {
            "as": job["as"],
            "ocp_version": job["ocp_version"],
            "cron": job["cron"],
            "profile": job["profile"],
            "source_file": job["source_file"],
            "workflow": job.get("workflow"),
            "duration_hours": duration_hours,
            "observed_duration_hours": job.get("observed_duration_hours"),
            "duration_source": job.get("duration_source"),
            "leases_held": leases_held,
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
                demand[d][h] += leases_held
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
    """Find contiguous free windows for scheduling advice (may wrap midnight/week)."""
    n = 7 * 24
    spare = [capacity - demand[d][h] for d in range(7) for h in range(24)]
    blocks: list[dict[str, Any]] = []

    if all(s >= min_spare for s in spare):
        blocks.append(
            {
                "day": DAY_NAMES[0],
                "dow": 0,
                "start_hour": 0,
                "end_dow": 0,
                "end_day": DAY_NAMES[0],
                "end_hour": 0,
                "hours": n,
                "min_spare": min(spare) if spare else 0,
            }
        )
        return blocks

    starts = [
        i
        for i in range(n)
        if spare[i] >= min_spare and spare[(i - 1) % n] < min_spare
    ]
    for i in starts:
        j = i
        min_sp = spare[i]
        while spare[j % n] >= min_spare:
            min_sp = min(min_sp, spare[j % n])
            j += 1
            if j - i >= n:
                break
        length = min(j - i, n)
        if length < min_hours:
            continue
        start_d, start_h = divmod(i, 24)
        end_pos = (i + length) % n
        end_d, end_h = divmod(end_pos, 24)
        blocks.append(
            {
                "day": DAY_NAMES[start_d],
                "dow": start_d,
                "start_hour": start_h,
                "end_dow": end_d,
                "end_day": DAY_NAMES[end_d],
                "end_hour": end_h,  # exclusive
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
        "cron_english",
        "interval",
        "schedule_class",
        "timeout",
        "workflow",
        "leases_held",
        "observed_duration_hours",
        "duration_hours",
        "lease_buffer_minutes",
        "duration_source",
        "sippy_planning_min",
        "sippy_current_avg_min",
        "sippy_runs",
        "sippy_donor_release",
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
                    "cron_english": job.get("cron_english") or "",
                    "interval": job["interval"] or "",
                    "schedule_class": job.get("schedule_class", ""),
                    "timeout": job["timeout"] or "",
                    "workflow": job["workflow"] or "",
                    "leases_held": job.get("leases_held") or 1,
                    "observed_duration_hours": job.get("observed_duration_hours") or "",
                    "duration_hours": job.get("duration_hours") or "",
                    "lease_buffer_minutes": job.get("lease_buffer_minutes")
                    if job.get("lease_buffer_minutes") is not None
                    else "",
                    "duration_source": job.get("duration_source") or "",
                    "sippy_planning_min": sippy.get("planning_min", ""),
                    "sippy_current_avg_min": sippy.get("current_avg_min", ""),
                    "sippy_runs": sippy.get("current_runs", ""),
                    "sippy_donor_release": sippy.get("release", ""),
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
        f"lease_buffer_minutes={duration_meta.get('lease_buffer_minutes')}",
        "duration_basis=successful-prowjob-wall-clock",
    ]
    for profile in sorted(lease_map):
        lt = lease_map[profile]
        names = ",".join(leases.get(lt, []))
        rows.append(f"lease|{profile}|{lt}|{len(leases.get(lt, []))}|{names}")
    for job in sorted(jobs, key=lambda j: (j["profile"], j["as"], j["source_file"])):
        sippy = job.get("sippy") or {}
        slots = ";".join(
            f"{d}:{h:02d}:{m:02d}" for d, h, m in (job.get("slots") or [])
        )
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
                    str(job.get("leases_held") or 1),
                    slots,
                    str(job.get("duration_hours") or ""),
                    job.get("duration_source") or "",
                    str(sippy.get("name") or ""),
                    str(sippy.get("release") or ""),
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
                "busiest": info["busiest"][:48],
                "free": info["free"][:48],
                "free_blocks": info["free_blocks"][:16],
                "peak_demand": info["peak_demand"],
                "overcommit_hours": info["overcommit_hours"],
                "median_duration_hours": info["median_duration_hours"],
                "sippy_coverage": info["sippy_coverage"],
                "jobs": info["jobs"],
                "workflows": info["workflows"],
            }
        )

    data_json = json.dumps(payload, separators=(",", ":")).replace("<", "\\u003c")
    buf = duration_meta.get("lease_buffer_minutes")
    buf_note = f" + {buf:g}m lease buffer" if buf else ""
    sippy_note = (
        f"Sippy successful runs{buf_note}"
        if duration_meta.get("enabled")
        else f"fixed {duration_meta.get('default_hours')}h{buf_note}"
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>IBM Z lease schedule capacity</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Crect width='64' height='64' rx='14' fill='%230f62fe'/%3E%3Crect x='12' y='38' width='8' height='14' rx='2' fill='%23ffffff' opacity='0.55'/%3E%3Crect x='24' y='28' width='8' height='24' rx='2' fill='%23ffffff' opacity='0.75'/%3E%3Crect x='36' y='18' width='8' height='34' rx='2' fill='%23ffffff'/%3E%3Cpath d='M46 14h6l-8 12h7l-12 18h-6l8-12h-7z' fill='%239ef0f0'/%3E%3C/svg%3E"/>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap" rel="stylesheet"/>
<style>
:root, [data-theme="light"] {{
  --bg: #eef3fb;
  --bg-elevated: #ffffff;
  --panel: #ffffff;
  --panel-soft: #f4f8ff;
  --border: #c9d7ef;
  --border-strong: #9eb6e0;
  --text: #121619;
  --text-secondary: #343a3f;
  --muted: #5a6876;
  --accent: #0f62fe;
  --accent-soft: rgba(15, 98, 254, 0.10);
  --accent-text: #0043ce;
  --code-bg: #e8efff;
  --code-fg: #002d9c;
  --ok: #198038;
  --ok-bg: #defbe6;
  --ok-text: #0e6027;
  --warn: #b28600;
  --warn-bg: #fcf4d6;
  --warn-text: #684e00;
  --bad: #da1e28;
  --bad-bg: #fff1f1;
  --bad-text: #a2191f;
  --idle-bg: #e4ebf7;
  --idle-text: #5a6876;
  --teal: #007d79;
  --glow-blue: rgba(15, 98, 254, 0.28);
  --glow-teal: rgba(0, 125, 121, 0.22);
  --glow-amber: rgba(241, 194, 27, 0.28);
  --hero-bg: linear-gradient(135deg, #ffffff 0%, #eef4ff 48%, #e8f8f6 100%);
  --chip-bg: linear-gradient(180deg, #f4f8ff, #eaf1ff);
  --tab-active-bg: linear-gradient(180deg, #e8f0ff, #ffffff);
  --table-head: #e8f0ff;
  --radius: 14px;
  --radius-sm: 10px;
  --shadow: 0 10px 28px rgba(15, 98, 254, 0.08);
  --font: "IBM Plex Sans", "Segoe UI", sans-serif;
  --mono: "IBM Plex Mono", ui-monospace, Menlo, Consolas, monospace;
}}
[data-theme="dark"] {{
  --bg: #0b1220;
  --bg-elevated: #121c2e;
  --panel: #152238;
  --panel-soft: #1a2a44;
  --border: #2d4263;
  --border-strong: #45618a;
  --text: #eef3fa;
  --text-secondary: #c5d0e0;
  --muted: #8fa0b8;
  --accent: #4589ff;
  --accent-soft: rgba(69, 137, 255, 0.16);
  --accent-text: #a6c8ff;
  --code-bg: rgba(69, 137, 255, 0.14);
  --code-fg: #a6c8ff;
  --ok: #42be65;
  --ok-bg: rgba(36, 161, 72, 0.18);
  --ok-text: #6fcf97;
  --warn: #f1c21b;
  --warn-bg: rgba(241, 194, 27, 0.16);
  --warn-text: #f5d76e;
  --bad: #fa4d56;
  --bad-bg: rgba(250, 77, 86, 0.16);
  --bad-text: #ffb3b8;
  --idle-bg: #172338;
  --idle-text: #7b8ba3;
  --teal: #3ddbd9;
  --glow-blue: rgba(69, 137, 255, 0.35);
  --glow-teal: rgba(61, 219, 217, 0.22);
  --glow-amber: rgba(241, 194, 27, 0.22);
  --hero-bg: linear-gradient(135deg, #101a2d 0%, #142447 55%, #0f2a32 100%);
  --chip-bg: linear-gradient(180deg, rgba(69, 137, 255, 0.14), rgba(21, 34, 56, 0.9));
  --tab-active-bg: linear-gradient(180deg, rgba(69, 137, 255, 0.22), #152238);
  --table-head: #1a2f4d;
  --shadow: 0 12px 32px rgba(0, 0, 0, 0.35);
}}
* {{ box-sizing: border-box; }}
html {{ scroll-behavior: smooth; }}
body {{
  margin: 0;
  font-family: var(--font);
  background:
    radial-gradient(900px 340px at 8% -8%, var(--glow-blue), transparent 58%),
    radial-gradient(780px 300px at 94% 0%, var(--glow-teal), transparent 52%),
    radial-gradient(640px 260px at 50% 120%, var(--glow-amber), transparent 55%),
    var(--bg);
  color: var(--text);
  line-height: 1.5;
  font-size: 15px;
  min-height: 100vh;
  transition: background .25s ease, color .2s ease;
}}
a {{ color: var(--accent-text); text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
code, .mono {{
  font-family: var(--mono); font-size: 0.92em;
  background: var(--code-bg); color: var(--code-fg);
  padding: 0.08rem 0.28rem; border-radius: 4px;
}}
header.hero {{
  padding: 1.85rem clamp(1.1rem, 2.5vw, 2.25rem) 1.45rem;
  border-bottom: 1px solid var(--border);
  background:
    radial-gradient(680px 240px at 8% 0%, var(--glow-blue), transparent 62%),
    radial-gradient(520px 200px at 92% 10%, var(--glow-teal), transparent 55%),
    var(--hero-bg);
  box-shadow: 0 8px 30px var(--glow-blue);
}}
.hero-inner {{ max-width: min(100%, 1760px); margin: 0 auto; }}
.hero-top {{
  display: flex; flex-wrap: wrap; align-items: flex-start; justify-content: space-between;
  gap: 1rem 1.5rem;
}}
.brand-row {{
  display: flex; align-items: center; gap: 1rem; min-width: 0;
}}
.brand-mark {{
  flex: 0 0 auto;
  width: 3.15rem; height: 3.15rem;
  border-radius: 0.9rem;
  background: linear-gradient(145deg, #0f62fe 0%, #0043ce 55%, #007d79 120%);
  display: grid; place-items: center;
  box-shadow: 0 0 0 3px rgba(15, 98, 254, 0.18), 0 12px 28px var(--glow-blue);
}}
.brand-mark svg {{ width: 1.85rem; height: 1.85rem; display: block; }}
.brand-text {{ min-width: 0; }}
.eyebrow {{
  display: inline-block;
  margin: 0 0 0.3rem;
  color: var(--accent-text);
  font-size: 0.7rem;
  font-weight: 650;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}}
header.hero h1 {{
  margin: 0;
  font-size: clamp(1.75rem, 2.8vw, 2.4rem);
  font-weight: 700;
  letter-spacing: -0.03em;
  line-height: 1.1;
  color: var(--text);
}}
.lede {{
  margin: 0.85rem 0 0;
  max-width: 42rem;
  color: var(--text-secondary);
  font-size: 1.02rem;
  line-height: 1.5;
}}
.hero-actions {{
  display: flex; flex-direction: column; align-items: flex-end; gap: 0.65rem;
}}
.control-row {{
  display: flex; flex-wrap: wrap; gap: 0.55rem; justify-content: flex-end;
}}
.meta-chips {{
  display: flex; flex-wrap: wrap; gap: 0.45rem;
  margin-top: 1.05rem;
}}
.meta-chip {{
  display: inline-flex; align-items: center; gap: 0.35rem;
  padding: 0.38rem 0.7rem;
  border-radius: 999px;
  border: 1px solid var(--border);
  background: var(--chip-bg);
  color: var(--muted);
  font-size: 0.78rem;
  line-height: 1.2;
  box-shadow: 0 0 16px rgba(15, 98, 254, 0.08);
}}
.meta-chip strong {{ color: var(--text-secondary); font-weight: 600; }}
.meta-chip a {{ color: var(--accent-text); }}
.meta-chip code {{ background: transparent; padding: 0; color: var(--accent-text); }}
.toggle {{
  display: inline-flex; border: 1px solid var(--border-strong); border-radius: var(--radius-sm);
  overflow: hidden; background: var(--bg-elevated);
  box-shadow: 0 0 0 1px var(--accent-soft);
}}
.toggle button {{
  background: transparent; color: var(--muted); border: 0; padding: 0.48rem 0.9rem;
  cursor: pointer; font: 500 0.82rem/1 var(--font);
}}
.toggle button.active {{
  background: linear-gradient(180deg, var(--accent), #0043ce);
  color: #fff;
  box-shadow: inset 0 0 12px rgba(255,255,255,0.18);
}}
.toolbar-note {{ color: var(--muted); font-size: 0.76rem; text-align: right; max-width: 16rem; }}
.hero-nav-link {{
  display: inline-flex; align-items: center; gap: 0.35rem;
  padding: 0.45rem 0.85rem;
  border-radius: var(--radius-sm);
  border: 1px solid var(--border-strong);
  background: var(--bg-elevated);
  color: var(--accent-text);
  font: 600 0.82rem/1.2 var(--font);
  text-decoration: none;
  box-shadow: 0 0 16px var(--accent-soft);
  white-space: nowrap;
}}
.hero-nav-link:hover {{
  text-decoration: none;
  border-color: var(--accent);
  box-shadow: 0 0 20px var(--glow-blue);
}}
.hero-nav-link svg {{ width: 0.95rem; height: 0.95rem; flex: 0 0 auto; }}
main {{ padding: 1.25rem clamp(1rem, 2vw, 1.75rem) 3.5rem; max-width: min(100%, 1760px); margin: 0 auto; }}
.tabs {{
  display: flex; flex-wrap: wrap; gap: 0.55rem;
  margin: 0 0 1.15rem; padding: 0.15rem 0 0.25rem;
}}
.tab {{
  appearance: none;
  background: var(--panel);
  border: 1px solid var(--border);
  color: var(--text);
  padding: 0.75rem 1rem;
  border-radius: var(--radius-sm);
  cursor: pointer;
  text-align: left;
  flex: 1 1 200px;
  min-width: 0;
  box-shadow: var(--shadow);
  transition: border-color .15s ease, box-shadow .15s ease, transform .15s ease;
}}
.tab:hover {{ border-color: var(--border-strong); transform: translateY(-1px); box-shadow: 0 0 20px var(--glow-blue); }}
.tab.active {{
  border-color: var(--accent);
  background: var(--tab-active-bg);
  box-shadow: 0 0 0 1px var(--accent), 0 0 22px var(--glow-blue);
}}
.tab .tab-name {{
  display: block;
  font-family: var(--mono);
  font-size: 0.78rem;
  font-weight: 500;
  letter-spacing: -0.01em;
  color: var(--accent-text);
  background: transparent;
  padding: 0;
}}
.tab .count {{
  display: block;
  margin-top: 0.28rem;
  color: var(--muted);
  font-family: var(--font);
  font-size: 0.74rem;
  font-weight: 500;
}}
.panel {{
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 1.15rem 1.2rem 1.25rem;
  margin-bottom: 1.1rem;
  box-shadow: var(--shadow);
}}
.panel-head {{ margin-bottom: 0.95rem; }}
.panel-head .eyebrow {{ margin-bottom: 0.35rem; }}
.panel h2 {{
  margin: 0;
  font-size: 1.2rem;
  font-weight: 650;
  letter-spacing: -0.015em;
  line-height: 1.25;
  color: var(--text);
}}
.panel-lead {{
  margin: 0.45rem 0 0;
  color: var(--muted);
  font-size: 0.95rem;
  width: 100%;
  max-width: none;
}}
.profile-title {{
  margin: 0 0 0.95rem;
  font-family: var(--mono);
  font-size: 1rem;
  font-weight: 500;
  color: var(--accent-text);
  background: transparent;
  padding: 0;
}}
.meta {{
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 0.85rem;
}}
@media (max-width: 1100px) {{
  .meta {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
}}
@media (max-width: 640px) {{
  .meta {{ grid-template-columns: 1fr; }}
}}
.metric {{
  background: linear-gradient(180deg, var(--panel-soft), var(--bg-elevated));
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 1rem 1.05rem;
  border-left: 3px solid var(--accent);
  min-width: 0;
  box-shadow: 0 0 18px rgba(15, 98, 254, 0.06);
}}
.metric:nth-child(3n+2) {{ border-left-color: var(--teal); box-shadow: 0 0 18px var(--glow-teal); }}
.metric:nth-child(3n) {{ border-left-color: #f1c21b; box-shadow: 0 0 18px var(--glow-amber); }}
.metric .label {{
  color: var(--text-secondary);
  font-size: 0.82rem;
  font-weight: 650;
  letter-spacing: 0.01em;
  line-height: 1.3;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}}
.metric .value {{
  margin-top: 0.4rem;
  font-size: clamp(1.45rem, 2vw, 1.75rem);
  font-weight: 700;
  letter-spacing: -0.02em;
  line-height: 1.1;
  color: var(--text);
}}
.metric .hint {{
  margin-top: 0.4rem;
  color: var(--muted);
  font-size: 0.78rem;
  line-height: 1.4;
}}
.lease-names {{
  margin: 1rem 0 0;
  padding: 0.9rem 1rem;
  background: var(--panel-soft);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  color: var(--muted);
  font-size: 0.82rem;
  line-height: 1.55;
  word-break: break-word;
}}
.lease-names .lease-label {{
  display: inline-block;
  min-width: 5.75rem;
  color: var(--text-secondary);
  font-weight: 600;
}}
.class-label {{
  display: inline-block;
  font-size: 0.8rem;
  font-weight: 560;
  color: var(--text-secondary);
  letter-spacing: 0.01em;
}}
.callout {{
  border-left: 3px solid var(--teal);
  background: linear-gradient(90deg, color-mix(in srgb, var(--teal) 12%, transparent), var(--panel) 40%);
  box-shadow: 0 0 24px var(--glow-teal);
}}
.callout .panel-lead {{ color: var(--text-secondary); }}
.heatmap-wrap {{
  overflow-x: auto;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  background: var(--panel-soft);
  box-shadow: inset 0 0 30px var(--accent-soft);
  width: 100%;
}}
table.heatmap {{
  border-collapse: separate;
  border-spacing: 3px;
  font-size: 0.85rem;
  width: 100%;
  table-layout: fixed;
  padding: 0.45rem;
}}
table.heatmap th, table.heatmap td {{
  text-align: center;
  border-radius: 5px;
  min-width: 0;
}}
table.heatmap th:first-child,
table.heatmap td:first-child {{
  width: 3.4rem;
  min-width: 3.4rem;
}}
table.heatmap th {{
  color: var(--muted); font-weight: 600; background: transparent; line-height: 1.15;
  font-family: var(--mono); font-size: 0.72rem;
  height: 2.4rem;
}}
table.heatmap th .sub {{ display:block; font-size: 0.62rem; opacity: 0.85; font-weight: 500; }}
table.heatmap td {{
  cursor: pointer; font-family: var(--mono); font-weight: 600;
  height: clamp(2.6rem, 4.2vw, 3.35rem);
  transition: transform .12s ease, outline-color .12s ease;
  border: 1px solid transparent;
}}
table.heatmap td:hover {{ transform: scale(1.04); outline: 1px solid var(--border-strong); z-index: 1; }}
table.heatmap td.idle {{ background: var(--idle-bg); color: var(--idle-text); border-color: var(--border); }}
table.heatmap td.ok {{ background: var(--ok-bg); color: var(--ok-text); border-color: color-mix(in srgb, var(--ok) 35%, transparent); }}
table.heatmap td.warn {{ background: var(--warn-bg); color: var(--warn-text); border-color: color-mix(in srgb, var(--warn) 35%, transparent); }}
table.heatmap td.bad {{ background: var(--bad-bg); color: var(--bad-text); border-color: color-mix(in srgb, var(--bad) 35%, transparent); }}
.legend {{
  display: flex; gap: 1rem; flex-wrap: wrap;
  color: var(--muted); font-size: 0.8rem; margin: 0.85rem 0 0;
}}
.legend span {{ display: inline-flex; align-items: center; gap: 0.4rem; }}
.swatch {{
  width: 0.9rem; height: 0.9rem; border-radius: 3px;
  border: 1px solid var(--border); display: inline-block;
}}
.cols {{
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 1rem;
  margin-bottom: 1.1rem;
}}
@media (max-width: 1100px) {{
  .cols {{ grid-template-columns: 1fr; }}
}}
.slot-block {{
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  border-left: 4px solid var(--accent);
  box-shadow: var(--shadow);
  display: flex;
  flex-direction: column;
  min-height: 0;
  overflow: hidden;
}}
.slot-block.free {{ border-left-color: var(--ok); box-shadow: 0 0 24px rgba(25, 128, 56, 0.12), var(--shadow); }}
.slot-block.hours {{ border-left-color: var(--teal); box-shadow: 0 0 24px var(--glow-teal), var(--shadow); }}
.slot-block.busy {{ border-left-color: #f1c21b; box-shadow: 0 0 24px var(--glow-amber), var(--shadow); }}
.slot-block-head {{
  padding: 0.95rem 1rem 0.75rem;
  border-bottom: 1px solid var(--border);
  background: var(--panel-soft);
}}
.slot-block-head .eyebrow {{ margin-bottom: 0.3rem; }}
.slot-block-head h2 {{
  margin: 0;
  font-size: 1.05rem;
  font-weight: 650;
  letter-spacing: -0.01em;
}}
.slot-block-head .panel-lead {{
  margin: 0.35rem 0 0;
  font-size: 0.82rem;
  color: var(--muted);
}}
.slot-scroll {{
  position: relative;
  flex: 1;
  min-height: 240px;
}}
.slot-scroll::after {{
  content: "";
  pointer-events: none;
  position: absolute;
  left: 0; right: 0; bottom: 0;
  height: 2.4rem;
  background: linear-gradient(180deg, transparent, var(--panel));
  opacity: 0.95;
}}
.slot-scroll.is-bottom::after {{ opacity: 0; }}
ul.slots {{
  list-style: none;
  padding: 0.35rem 0.85rem 1.6rem;
  margin: 0;
  max-height: 320px;
  overflow-y: auto;
  scrollbar-gutter: stable;
  scrollbar-width: thin;
  scrollbar-color: var(--border-strong) transparent;
}}
ul.slots::-webkit-scrollbar {{ width: 8px; }}
ul.slots::-webkit-scrollbar-thumb {{
  background: var(--border-strong);
  border-radius: 8px;
}}
ul.slots li {{
  padding: 0.65rem 0.35rem;
  border-bottom: 1px solid var(--border);
  font-size: 0.88rem;
  display: flex; justify-content: space-between; gap: 0.65rem; align-items: baseline;
}}
ul.slots li:last-child {{ border-bottom: 0; }}
.slot-foot {{
  padding: 0.45rem 1rem 0.7rem;
  border-top: 1px solid var(--border);
  color: var(--muted);
  font-size: 0.74rem;
  font-weight: 560;
  letter-spacing: 0.02em;
  display: flex;
  align-items: center;
  gap: 0.4rem;
  background: var(--panel-soft);
}}
.slot-foot .chev {{
  display: inline-block;
  animation: slot-bounce 1.4s ease-in-out infinite;
}}
@keyframes slot-bounce {{
  0%, 100% {{ transform: translateY(0); opacity: 0.7; }}
  50% {{ transform: translateY(3px); opacity: 1; }}
}}
.badge {{
  font-size: 0.75rem; font-weight: 650; padding: 0.22rem 0.55rem;
  border-radius: 6px;
  background: color-mix(in srgb, var(--ok-bg) 85%, var(--panel));
  border: 1px solid color-mix(in srgb, var(--ok) 45%, var(--border));
  color: var(--ok-text);
  white-space: nowrap;
  letter-spacing: 0.01em;
}}
.badge.busy {{
  background: color-mix(in srgb, var(--warn-bg) 85%, var(--panel));
  border-color: color-mix(in srgb, var(--warn) 45%, var(--border));
  color: var(--warn-text);
}}
[data-theme="dark"] .badge {{
  background: rgba(66, 190, 101, 0.22);
  border-color: rgba(66, 190, 101, 0.65);
  color: #b6f5c8;
}}
[data-theme="dark"] .badge.busy {{
  background: rgba(241, 194, 27, 0.22);
  border-color: rgba(241, 194, 27, 0.65);
  color: #f5d76e;
}}
.filters {{ display: flex; flex-wrap: wrap; gap: 0.55rem; margin-bottom: 0.85rem; }}
.filters input, .filters select {{
  background: var(--bg-elevated); color: var(--text); border: 1px solid var(--border);
  border-radius: var(--radius-sm); padding: 0.5rem 0.65rem; font: 500 0.86rem/1.2 var(--font);
}}
.filters input:focus, .filters select:focus {{
  outline: 2px solid color-mix(in srgb, var(--accent) 45%, transparent); outline-offset: 1px;
}}
table.jobs, table.wf {{ width: 100%; border-collapse: collapse; font-size: 0.84rem; }}
table.jobs th, table.jobs td, table.wf th, table.wf td {{
  border-bottom: 1px solid var(--border);
  padding: 0.55rem 0.5rem; text-align: left; vertical-align: top;
}}
table.jobs th, table.wf th {{
  color: var(--muted); font-weight: 650; font-size: 0.72rem;
  text-transform: uppercase; letter-spacing: 0.05em;
  position: sticky; top: 0; background: var(--table-head); z-index: 1;
}}
table.jobs tbody tr:hover, table.wf tbody tr:hover {{ background: var(--accent-soft); }}
.jobs-wrap {{
  max-height: 480px; overflow: auto;
  border: 1px solid var(--border); border-radius: var(--radius-sm);
  background: var(--bg-elevated);
  box-shadow: inset 0 0 24px var(--accent-soft);
}}
#cell-detail {{
  margin-top: 0.9rem; padding: 0.95rem 1rem; background: var(--panel-soft);
  border: 1px solid var(--border); border-radius: var(--radius-sm);
  font-size: 0.88rem; display: none;
  box-shadow: 0 0 20px var(--glow-blue);
}}
#cell-detail h3 {{ margin: 0 0 0.55rem; font-size: 1rem; color: var(--text); }}
#cell-detail ul {{ margin: 0; padding-left: 1.1rem; color: var(--text-secondary); }}
.pill {{
  display: inline-block; font-size: 0.68rem; font-weight: 600;
  padding: 0.12rem 0.4rem; border-radius: 6px; margin-left: 0.25rem;
  border: 1px solid var(--border); color: var(--muted); vertical-align: middle;
}}
.pill.sippy {{ color: var(--ok-text); border-color: color-mix(in srgb, var(--ok) 40%, transparent); background: var(--ok-bg); cursor: help; }}
.pill.workflow {{ color: var(--warn-text); border-color: color-mix(in srgb, var(--warn) 40%, transparent); background: var(--warn-bg); cursor: help; }}
.pill.default {{ color: var(--muted); background: var(--panel-soft); cursor: help; }}
.links a {{ color: var(--accent-text); text-decoration: none; }}
.links a:hover {{ text-decoration: underline; }}
.guide-grid {{
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 0.75rem;
}}
@media (max-width: 860px) {{
  .guide-grid {{ grid-template-columns: 1fr; }}
}}
.guide-card {{
  margin: 0;
  padding: 0.9rem 1rem;
  background: var(--panel-soft);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  border-top: 3px solid var(--accent);
  box-shadow: 0 0 18px var(--accent-soft);
}}
.guide-card:nth-child(2n) {{ border-top-color: var(--teal); box-shadow: 0 0 18px var(--glow-teal); }}
.guide-card:nth-child(3n) {{ border-top-color: #f1c21b; box-shadow: 0 0 18px var(--glow-amber); }}
.guide-card h3 {{
  margin: 0 0 0.35rem;
  font-size: 0.92rem;
  font-weight: 650;
  color: var(--text);
}}
.guide-card p {{
  margin: 0;
  color: var(--text-secondary);
  font-size: 0.88rem;
  line-height: 1.45;
}}
.guide-foot {{
  margin: 0.9rem 0 0;
  color: var(--muted);
  font-size: 0.84rem;
}}
.cron-english {{
  color: var(--text-secondary);
  font-size: 0.82rem;
  line-height: 1.35;
  min-width: 12rem;
  max-width: 22rem;
}}
</style>
<script>
(() => {{
  const saved = localStorage.getItem('ibmz-lease-theme');
  document.documentElement.setAttribute('data-theme', saved || 'dark');
}})();
</script>
</head>
<body>
<header class="hero">
  <div class="hero-inner">
    <div class="hero-top">
      <div class="brand-row">
        <div class="brand-mark" aria-hidden="true">
          <svg viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
            <rect x="5" y="20" width="5" height="8" rx="1.2" fill="#fff" opacity="0.55"/>
            <rect x="12" y="14" width="5" height="14" rx="1.2" fill="#fff" opacity="0.78"/>
            <rect x="19" y="8" width="5" height="20" rx="1.2" fill="#fff"/>
            <path d="M24.2 6.2h3.2L23.2 12h3.4L20.4 22h-3.1l4.1-6.2h-3.3z" fill="#9ef0f0"/>
          </svg>
        </div>
        <div class="brand-text">
          <p class="eyebrow">IBM Z · s390x CI</p>
          <h1>Lease capacity</h1>
        </div>
      </div>
      <div class="hero-actions">
        <a class="hero-nav-link" href="https://ibm-adarsh.github.io/ibmz-openshift-job-monitor/" target="_blank" rel="noopener" title="Open IBM Z CI Job Monitor">
          <svg viewBox="0 0 16 16" fill="none" aria-hidden="true"><path d="M6 3h7v7M13 3 7 9M3 6v7h7" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>
          Job Monitor
        </a>
        <div class="control-row">
          <div class="toggle" id="theme-toggle" title="Color theme">
            <button type="button" data-theme-choice="light">Light</button>
            <button type="button" data-theme-choice="dark">Dark</button>
          </div>
          <div class="toggle" id="tz-toggle" title="Display timezone">
            <button type="button" data-tz="UTC" class="active">UTC</button>
            <button type="button" data-tz="IST">IST</button>
          </div>
        </div>
        <span class="toolbar-note">IST remaps the heatmap, free windows, and job start times (cron source stays UTC).</span>
      </div>
    </div>
    <p class="lede">See when <code>libvirt-s390x-vpn*</code> jobs need leases — and how much capacity is still free.</p>
    <div class="meta-chips">
      <span class="meta-chip">Updated <strong id="generated"></strong></span>
      <span class="meta-chip">Hold model <strong id="duration-model">{html_escape(sippy_note)}</strong></span>
      <span class="meta-chip">Source <a id="source-link" href="https://github.com/openshift/release" target="_blank" rel="noopener">openshift/release</a> <code id="source-sha"></code></span>
    </div>
  </div>
</header>
<main>
  <div class="tabs" id="tabs" role="tablist" aria-label="Cluster profiles"></div>
  <section class="panel">
    <div class="panel-head">
      <p class="eyebrow">Overview</p>
      <h2>Profile summary</h2>
      <p class="panel-lead">Simple snapshot of capacity, how busy this profile gets, and how long jobs typically hold a lease.</p>
    </div>
    <p id="profile-title" class="profile-title"></p>
    <div class="meta" id="metrics"></div>
    <p class="lease-names" id="lease-names"></p>
  </section>
  <section class="panel callout">
    <div class="panel-head">
      <p class="eyebrow">Semantics</p>
      <h2>Lease occupancy model</h2>
    </div>
    <p class="panel-lead">
      A lease is taken when the job <strong>starts</strong> and stays busy through
      <strong>pre → test → post</strong>. Example: capacity <code>10</code> and one <code>3h</code> hold
      leaves <code>9</code> leases free for those 3 hours. Heatmap cells show <strong>leases in use</strong>.
    </p>
  </section>
  <section class="panel">
    <div class="panel-head">
      <p class="eyebrow">Weekly view</p>
      <h2>Concurrency heatmap</h2>
      <p class="panel-lead">
        Each cell is concurrent leases in use during that hour
        (most jobs hold 1 lease; some workflows hold more of the same type).
        Hold time = full job wall-clock (pre/test/post) + lease buffer.
        Toggle <strong>IST</strong> to remap columns, cell details, and job start times from cron UTC (+05:30).
        Click a cell for details.
      </p>
    </div>
    <div class="heatmap-wrap">
      <table class="heatmap" id="heatmap"></table>
    </div>
    <div class="legend">
      <span><i class="swatch" style="background:var(--idle-bg)"></i> Idle (0)</span>
      <span><i class="swatch" style="background:var(--ok-bg)"></i> Under capacity</span>
      <span><i class="swatch" style="background:var(--warn-bg)"></i> ≥80% capacity</span>
      <span><i class="swatch" style="background:var(--bad-bg)"></i> Overcommit</span>
    </div>
    <div id="cell-detail"></div>
  </section>
  <div class="cols">
    <section class="slot-block free" aria-label="Best free windows">
      <div class="slot-block-head">
        <p class="eyebrow">Scheduling</p>
        <h2>Best free windows</h2>
        <p class="panel-lead">Longest open stretches — safest places to add jobs.</p>
      </div>
      <div class="slot-scroll">
        <ul class="slots" id="free-blocks"></ul>
      </div>
      <div class="slot-foot"><span class="chev">↓</span> Scroll for more free windows</div>
    </section>
    <section class="slot-block hours" aria-label="Top free hours">
      <div class="slot-block-head">
        <p class="eyebrow">Scheduling</p>
        <h2>Top free hours</h2>
        <p class="panel-lead">Hours with the most leases still available.</p>
      </div>
      <div class="slot-scroll">
        <ul class="slots" id="free-slots"></ul>
      </div>
      <div class="slot-foot"><span class="chev">↓</span> Scroll for more free hours</div>
    </section>
    <section class="slot-block busy" aria-label="Busiest hours">
      <div class="slot-block-head">
        <p class="eyebrow">Pressure</p>
        <h2>Busiest hours</h2>
        <p class="panel-lead">Hours with the highest lease occupancy.</p>
      </div>
      <div class="slot-scroll">
        <ul class="slots" id="busy-slots"></ul>
      </div>
      <div class="slot-foot"><span class="chev">↓</span> Scroll for more busy hours</div>
    </section>
  </div>
  <section class="panel">
    <div class="panel-head">
      <p class="eyebrow">Composition</p>
      <h2>Workflows on this profile</h2>
      <p class="panel-lead">Job counts and median lease-hold duration by workflow.</p>
    </div>
    <div class="jobs-wrap" style="max-height:280px">
      <table class="wf">
        <thead>
          <tr><th>Workflow</th><th>Jobs</th><th>Median duration</th><th>Sippy matched</th></tr>
        </thead>
        <tbody id="wf-body"></tbody>
      </table>
    </div>
  </section>
  <section class="panel">
    <div class="panel-head">
      <p class="eyebrow">Inventory</p>
      <h2>Jobs</h2>
      <p class="panel-lead">All periodics on this profile across OCP versions, with lease-hold duration and links.</p>
    </div>
    <div class="filters">
      <input id="filter-text" placeholder="Filter job / cron / workflow / file…" style="min-width:240px"/>
      <select id="filter-version"><option value="">All versions</option></select>
      <select id="filter-class">
        <option value="">All schedule classes</option>
        <option value="daily">Daily</option>
        <option value="weekly">Weekly</option>
        <option value="low-frequency">Low-frequency</option>
      </select>
      <select id="filter-duration">
        <option value="">All duration sources</option>
        <option value="sippy">Sippy observed</option>
        <option value="workflow">Median fallback</option>
        <option value="default">Default</option>
      </select>
    </div>
    <div class="jobs-wrap">
      <table class="jobs">
        <thead>
          <tr>
            <th>Job</th><th>OCP</th><th>Cron</th><th>Schedule (plain English)</th><th>Class</th>
            <th>Lease hold</th><th>Workflow</th><th>Links</th>
          </tr>
        </thead>
        <tbody id="jobs-body"></tbody>
      </table>
    </div>
  </section>
  <section class="panel">
    <div class="panel-head">
      <p class="eyebrow">Guide</p>
      <h2>How to use this</h2>
      <p class="panel-lead">A short path through the page — what to look at first, and what the numbers mean.</p>
    </div>
    <div class="guide-grid">
      <article class="guide-card">
        <h3>1. Pick a profile</h3>
        <p>Use the tabs for the full cluster profile name (for example <code>libvirt-s390x-vpn-amd64</code>). Capacity comes from Boskos lease lists.</p>
      </article>
      <article class="guide-card">
        <h3>2. Read the heatmap</h3>
        <p>Each cell is leases in use that hour. Toggle IST to remap the grid from cron UTC. Available = total − in use. Click a cell to see which jobs overlap.</p>
      </article>
      <article class="guide-card">
        <h3>3. Find free windows</h3>
        <p>Best free windows and top free hours are the safest places to add or move jobs. Busiest hours show pressure.</p>
      </article>
      <article class="guide-card">
        <h3>4. Check job hold times</h3>
        <p>Lease hold is full job time (pre → test → post), usually from Sippy. Green/amber badges are source labels, not links — use Links for history.</p>
      </article>
      <article class="guide-card">
        <h3>5. Older OCP versions</h3>
        <p>If a version has no Sippy history, hold time is taken from the same job on another version. Sippy/Prow links open that version’s history.</p>
      </article>
      <article class="guide-card">
        <h3>6. Low-frequency jobs</h3>
        <p>Monthly or yearly jobs are listed in Inventory but stay out of the weekly heatmap so the grid stays readable.</p>
      </article>
    </div>
    <p class="guide-foot">
      This is a <strong>planned schedule</strong> model (not live Boskos occupancy). Refreshes daily at 08:00 IST from
      <code>openshift/release</code>. Raw data: <a href="jobs.csv">jobs.csv</a> · <a href="summary.json">summary.json</a>
    </p>
  </section>
</main>
<script id="report-data" type="application/json">{data_json}</script>
<script>
const DATA = JSON.parse(document.getElementById('report-data').textContent);
const RELEASE_BLOB_BASE = '{RELEASE_BLOB_BASE}';
const PROW_HISTORY_BASE = '{PROW_HISTORY_BASE}';
let active = 0;
let tz = (localStorage.getItem('ibmz-lease-tz') === 'IST') ? 'IST' : 'UTC';

document.getElementById('generated').textContent = DATA.generated_at;
if (DATA.source) {{
  const link = document.getElementById('source-link');
  if (DATA.source.source_url) link.href = DATA.source.source_url;
  document.getElementById('source-sha').textContent = DATA.source.short_sha || 'unknown';
}}

function prettyLabel(value) {{
  if (!value) return '';
  return String(value).split('-').map(part => part ? part.charAt(0).toUpperCase() + part.slice(1) : part).join('-');
}}

function syncThemeToggle() {{
  const theme = document.documentElement.getAttribute('data-theme') || 'light';
  document.querySelectorAll('#theme-toggle button').forEach(b => {{
    b.classList.toggle('active', b.dataset.themeChoice === theme);
  }});
}}
syncThemeToggle();
document.querySelectorAll('#theme-toggle button').forEach(btn => {{
  btn.onclick = () => {{
    const next = btn.dataset.themeChoice || 'light';
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem('ibmz-lease-theme', next);
    syncThemeToggle();
  }};
}});

function syncTzToggle() {{
  document.querySelectorAll('#tz-toggle button').forEach(b => {{
    b.classList.toggle('active', b.dataset.tz === tz);
  }});
}}
syncTzToggle();
document.querySelectorAll('#tz-toggle button').forEach(btn => {{
  btn.onclick = () => {{
    tz = btn.dataset.tz || 'UTC';
    localStorage.setItem('ibmz-lease-tz', tz);
    syncTzToggle();
    renderAll();
  }};
}});

function cellClass(demand, capacity) {{
  if (demand <= 0) return 'idle';
  if (demand > capacity) return 'bad';
  if (capacity > 0 && demand / capacity >= 0.8) return 'warn';
  return 'ok';
}}

const IST_OFFSET_MIN = 5 * 60 + 30;
const WEEK_MIN = 7 * 24 * 60;

function pad2(n) {{
  return String(n).padStart(2, '0');
}}

function formatHourLabel(hour) {{
  return pad2(hour);
}}

/** Shift a week-minute timestamp by delta minutes, wrapping the 7-day week. */
function wrapWeekMinutes(total) {{
  return ((total % WEEK_MIN) + WEEK_MIN) % WEEK_MIN;
}}

/** Convert UTC day-index + hour(+optional minute) → IST day-index + clock. */
function utcPartsToIst(dow, hour, minute = 0) {{
  const total = wrapWeekMinutes(dow * 24 * 60 + hour * 60 + minute + IST_OFFSET_MIN);
  return {{
    dow: Math.floor(total / (24 * 60)),
    hour: Math.floor((total % (24 * 60)) / 60),
    minute: total % 60,
  }};
}}

/** Convert IST day-index + hour(+minute) → UTC day-index + clock. */
function istPartsToUtc(dow, hour, minute = 0) {{
  const total = wrapWeekMinutes(dow * 24 * 60 + hour * 60 + minute - IST_OFFSET_MIN);
  return {{
    dow: Math.floor(total / (24 * 60)),
    hour: Math.floor((total % (24 * 60)) / 60),
    minute: total % 60,
  }};
}}

/**
 * IST heatmap column H means the wall-clock hour [H:00, H+1:00) IST.
 * Because IST is UTC+05:30, that window overlaps two UTC hour buckets.
 * Example: IST Tue 10:00–11:00 = UTC 04:30–05:30 → UTC hours 4 and 5.
 * A job starting 10:30 IST therefore appears under column 10 (not 11).
 */
function utcHoursOverlappingIstHour(d, h) {{
  const samples = [istPartsToUtc(d, h, 0), istPartsToUtc(d, h, 30)];
  const out = [];
  const seen = new Set();
  for (const u of samples) {{
    const key = u.dow + ',' + u.hour;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({{ dow: u.dow, hour: u.hour }});
  }}
  return out;
}}

function displayDemand(p, d, h) {{
  if (tz === 'UTC') return p.demand[d][h] || 0;
  // Must match jobsForDisplayHour: never paint green from a UTC hour bucket
  // that only partially overlaps this IST hour when the job itself does not.
  // Example bug: job 00:40 UTC (=06:10 IST) paints UTC hour 0; IST 05:00–06:00
  // overlaps UTC 23:30–00:30 so max(UTC0) was 1 while no job overlaps 05:xx IST.
  const jobs = jobsForDisplayHour(p, d, h);
  return jobs.reduce((sum, j) => sum + (j.leases_held != null ? j.leases_held : 1), 0);
}}

function parseJobStartUtc(startStr) {{
  const m = String(startStr || '').match(/^([A-Za-z]+)\s+(\d{{1,2}}):(\d{{2}})/);
  if (!m) return null;
  const dow = DATA.day_names.indexOf(m[1]);
  if (dow < 0) return null;
  return {{
    dow,
    hour: Number(m[2]),
    minute: Number(m[3]),
    weekMin: dow * 24 * 60 + Number(m[2]) * 60 + Number(m[3]),
  }};
}}

function jobOverlapsDisplayHour(j, d, h) {{
  if (tz === 'UTC') {{
    // Details are already bucketed by UTC hour in the generator.
    return true;
  }}
  const start = parseJobStartUtc(j.start);
  if (!start) return true;
  const durMin = Math.max(1, Math.round(Number(j.duration_hours || 0) * 60));
  const jobStart = start.weekMin;
  const jobEnd = jobStart + durMin;
  const winUtc = istPartsToUtc(d, h, 0);
  const winStart = winUtc.dow * 24 * 60 + winUtc.hour * 60 + winUtc.minute;
  const winEnd = winStart + 60;
  // Half-open [start, end) overlap with IST hour window in UTC minutes.
  const a0 = jobStart;
  const a1 = jobEnd;
  const b0 = winStart;
  const b1 = winEnd;
  // Normalize across week wrap for safety.
  const overlaps = (x0, x1, y0, y1) => x0 < y1 && x1 > y0;
  if (overlaps(a0, a1, b0, b1)) return true;
  if (overlaps(a0 + WEEK_MIN, a1 + WEEK_MIN, b0, b1)) return true;
  if (overlaps(a0, a1, b0 + WEEK_MIN, b1 + WEEK_MIN)) return true;
  return false;
}}

function jobsForDisplayHour(p, d, h) {{
  if (tz === 'UTC') return p.details[d][h] || [];
  const jobs = [];
  const seen = new Set();
  for (const u of utcHoursOverlappingIstHour(d, h)) {{
    for (const j of (p.details[u.dow][u.hour] || [])) {{
      const key = (j.as || '') + '|' + (j.start || '') + '|' + (j.ocp_version || '') + '|' + (j.workflow || '');
      if (seen.has(key)) continue;
      if (!jobOverlapsDisplayHour(j, d, h)) continue;
      seen.add(key);
      jobs.push(j);
    }}
  }}
  return jobs;
}}

function formatDisplaySlot(d, h) {{
  // Heatmap / slot display coordinate in the active timezone (hour bin H:00–H+1:00).
  const day = DATA.day_names[d];
  const suffix = tz === 'IST' ? 'IST' : 'UTC';
  return `${{day}} ${{pad2(h)}}:00 ${{suffix}}`;
}}

function formatJobStart(startStr) {{
  // Job detail starts are stored as "Day HH:MM" in UTC.
  const m = String(startStr || '').match(/^([A-Za-z]+)\s+(\d{{1,2}}):(\d{{2}})/);
  if (!m) return startStr || '—';
  const day = m[1];
  const hour = Number(m[2]);
  const minute = Number(m[3]);
  if (tz === 'UTC') return `${{day}} ${{pad2(hour)}}:${{pad2(minute)}} UTC`;
  let dow = DATA.day_names.indexOf(day);
  if (dow < 0) return `${{startStr}} IST`;
  const ist = utcPartsToIst(dow, hour, minute);
  return `${{DATA.day_names[ist.dow]}} ${{pad2(ist.hour)}}:${{pad2(ist.minute)}} IST`;
}}

function formatEnglishDayList(dows) {{
  const longDays = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
  const uniq = [...new Set(dows)].sort((a, b) => a - b);
  if (uniq.length === 7) return 'every day';
  if (!uniq.length) return '';
  const ranges = [];
  let start = uniq[0], prev = uniq[0];
  for (let i = 1; i < uniq.length; i++) {{
    const d = uniq[i];
    if (d === prev + 1) {{ prev = d; continue; }}
    ranges.push([start, prev]);
    start = prev = d;
  }}
  ranges.push([start, prev]);
  const parts = [];
  for (const [a, b] of ranges) {{
    if (a === b) parts.push(longDays[a]);
    else if (b === a + 1) {{ parts.push(longDays[a]); parts.push(longDays[b]); }}
    else parts.push(`${{longDays[a]}}–${{longDays[b]}}`);
  }}
  if (parts.length === 1) return parts[0];
  if (parts.length === 2) return `${{parts[0]}} and ${{parts[1]}}`;
  return parts.slice(0, -1).join(', ') + ', and ' + parts[parts.length - 1];
}}

function parseEnglishDayPhrase(phrase) {{
  const longDays = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
  const raw = String(phrase || '').trim();
  // "Every day at …" captures dayPart as "day".
  if (!raw || /^day$/i.test(raw) || /^every day$/i.test(raw)) {{
    return {{ every: true, days: [0,1,2,3,4,5,6] }};
  }}
  const days = [];
  const tokens = raw.split(/,|\\band\\b/i).map(s => s.trim()).filter(Boolean);
  for (const tok of tokens) {{
    const m = tok.match(/^(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday)\\s*[–-]\\s*(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday)$/i);
    if (m) {{
      const ai = longDays.findIndex(d => d.toLowerCase() === m[1].toLowerCase());
      const bi = longDays.findIndex(d => d.toLowerCase() === m[2].toLowerCase());
      if (ai >= 0 && bi >= 0 && ai <= bi) {{
        for (let d = ai; d <= bi; d++) days.push(d);
        continue;
      }}
    }}
    const di = longDays.findIndex(d => d.toLowerCase() === tok.toLowerCase());
    if (di >= 0) days.push(di);
  }}
  return {{ every: false, days: [...new Set(days)].sort((a, b) => a - b) }};
}}

function convertUtcClockList(text) {{
  return String(text || '').replace(/\\b(\\d{{2}}):(\\d{{2}})\\b/g, (full, hh, mm) => {{
    const ist = utcPartsToIst(0, Number(hh), Number(mm));
    return `${{pad2(ist.hour)}}:${{pad2(ist.minute)}}`;
  }});
}}

function formatCronEnglish(text) {{
  // cron_english is authored in UTC; rewrite day spans + clock times for IST.
  // Note: use \\b in this Python f-string so the emitted JS keeps word boundaries.
  if (tz === 'UTC' || !text) return text || '';
  let out = String(text);

  // "every hour at :00 UTC" → ":30 IST" (before the generic Every … at … rewriter).
  out = out.replace(/\\bevery hour at :(\\d{{2}}) UTC\\b/gi, (full, mm) => {{
    const ist = utcPartsToIst(0, 0, Number(mm));
    return `every hour at :${{pad2(ist.minute)}} IST`;
  }});

  // "hourly from 08:00 to 17:00 UTC"
  out = out.replace(
    /hourly from\\s+(\\d{{2}}):(\\d{{2}})\\s+to\\s+(\\d{{2}}):(\\d{{2}})\\s+UTC\\b/gi,
    (full, h1, m1, h2, m2) => {{
      const a = utcPartsToIst(0, Number(h1), Number(m1));
      const b = utcPartsToIst(0, Number(h2), Number(m2));
      return `hourly from ${{pad2(a.hour)}}:${{pad2(a.minute)}} to ${{pad2(b.hour)}}:${{pad2(b.minute)}} IST`;
    }}
  );

  // "Every Monday–Thursday at 22:00 UTC" / "Every day at 00:40 UTC"
  // Remap the full weekday set with the first clock time (midnight-crossing safe).
  out = out.replace(
    /\\bEvery\\s+(?!hour\\b)(.+?)\\s+at\\s+(.+?)\\s+UTC\\b/gi,
    (full, dayPart, timePart) => {{
      const parsed = parseEnglishDayPhrase(dayPart);
      const first = String(timePart).match(/(\\d{{2}}):(\\d{{2}})/);
      const hh = first ? Number(first[1]) : 0;
      const mm = first ? Number(first[2]) : 0;
      if (!parsed.days.length && !parsed.every) return full;
      const istDays = parsed.days.map(d => utcPartsToIst(d, hh, mm).dow);
      const dayBit = (parsed.every || new Set(istDays).size === 7)
        ? 'day'
        : formatEnglishDayList(istDays);
      const timeBit = convertUtcClockList(timePart);
      return `Every ${{dayBit}} at ${{timeBit}} IST`;
    }}
  );

  // Remaining phrases that still end in UTC: convert every HH:MM, then the label.
  // Bare ":MM UTC" (not part of HH:MM) is converted separately so "08:00 UTC"
  // is not mistaken for a :00 minute marker.
  if (/\\bUTC\\b/.test(out)) {{
    out = out.replace(/\\b(\\d{{2}}):(\\d{{2}})\\b/g, (full, hh, mm) => {{
      const ist = utcPartsToIst(0, Number(hh), Number(mm));
      return `${{pad2(ist.hour)}}:${{pad2(ist.minute)}}`;
    }});
    out = out.replace(/(?<!\\d):(\\d{{2}}) UTC\\b/g, (full, mm) => {{
      const ist = utcPartsToIst(0, 0, Number(mm));
      return `:${{pad2(ist.minute)}} IST`;
    }});
    out = out.replace(/\\bUTC\\b/g, 'IST');
  }}
  return out;
}}

function durationPill(j) {{
  const source = j.duration_source || '';
  if (!source) return '';
  const sippy = j.sippy || {{}};
  if (source.startsWith('sippy')) {{
    const from = sippy.borrowed && sippy.release
      ? `Duration from successful runs of the same job on OCP ${{sippy.release}} (this version has no passes yet)`
      : 'Duration from successful Sippy ProwJob runs (fail-only averages ignored; label only — use Links → sippy/prow for history)';
    const label = sippy.borrowed && sippy.release ? `sippy·${{sippy.release}}` : 'sippy';
    return `<span class="pill sippy" title="${{from}}">${{label}}</span>`;
  }}
  if (source === 'as-median' || source === 'family-median' || source === 'workflow-median') {{
    const tip = source === 'as-median'
      ? 'No Sippy history for this OCP version; using median of the same job name'
      : source === 'family-median'
        ? 'No Sippy history; using median of the same job family'
        : 'No Sippy history for this job on any indexed OCP version; using workflow median';
    return `<span class="pill workflow" title="${{tip}}">workflow</span>`;
  }}
  if (source === 'timeout') {{
    return '<span class="pill default" title="No Sippy sample; using job timeout">timeout</span>';
  }}
  return '<span class="pill default" title="No Sippy/workflow sample; using configured default">default</span>';
}}

function jobHistoryLinks(j) {{
  const sippy = j.sippy || {{}};
  const historyName = sippy.prow_history_name || sippy.name || j.prow_job_name || '';
  const sippyUrl = sippy.url || '';
  const parts = [];
  if (sippyUrl) {{
    const tip = sippy.borrowed
      ? `Sippy history for same job on OCP ${{sippy.release}} (this OCP version has no runs)`
      : 'Sippy job history';
    parts.push(`<a href="${{sippyUrl}}" target="_blank" rel="noopener" title="${{tip}}">sippy</a>`);
  }}
  if (historyName) {{
    const tip = sippy.borrowed
      ? `Prow job history for same job on OCP ${{sippy.release}}`
      : 'Prow job history';
    parts.push(`<a href="${{PROW_HISTORY_BASE}}/${{historyName}}" target="_blank" rel="noopener" title="${{tip}}">prow</a>`);
  }}
  if (j.source_file) {{
    parts.push(`<a href="${{RELEASE_BLOB_BASE}}/${{j.source_file}}" target="_blank" rel="noopener" title="openshift/release config">config</a>`);
  }}
  return parts.join(' · ');
}}

function renderTabs() {{
  const tabs = document.getElementById('tabs');
  tabs.innerHTML = '';
  DATA.profiles.forEach((p, i) => {{
    const b = document.createElement('button');
    b.type = 'button';
    b.className = 'tab' + (i === active ? ' active' : '');
    b.setAttribute('role', 'tab');
    b.setAttribute('aria-selected', i === active ? 'true' : 'false');
    b.innerHTML = `<span class="tab-name">${{p.name}}</span><span class="count">${{p.job_count}} jobs · capacity ${{p.capacity}}</span>`;
    b.title = p.name;
    b.onclick = () => {{ active = i; renderAll(); }};
    tabs.appendChild(b);
  }});
}}

function displayPeakStats(p) {{
  // In IST mode recompute from the same overlap rules as the heatmap.
  if (tz === 'UTC') {{
    return {{
      peak: p.peak_demand || 0,
      overcommit: p.overcommit_hours || 0,
    }};
  }}
  let peak = 0;
  let overcommit = 0;
  for (let d = 0; d < 7; d++) {{
    for (let h = 0; h < 24; h++) {{
      const dem = displayDemand(p, d, h);
      peak = Math.max(peak, dem);
      if (dem > p.capacity) overcommit += 1;
    }}
  }}
  return {{ peak, overcommit }};
}}

function renderMetrics(p) {{
  document.getElementById('profile-title').textContent = p.name;
  const stats = displayPeakStats(p);
  const minAvail = Math.max(0, p.capacity - stats.peak);
  const items = [
    ['Total leases', p.capacity, 'How many machines/leases this profile can run at once.'],
    ['Busiest moment', stats.peak, 'Most leases in use at the same time in a normal week' + (tz === 'IST' ? ' (IST hour bins).' : '.')],
    ['Still free at peak', minAvail, 'Leases left even during the busiest hour (capacity − peak).'],
    ['All jobs', p.job_count, 'Total periodic jobs configured on this profile.'],
    ['Weekly jobs', p.weekly_job_count, 'Jobs that run every week/day and appear in the heatmap.'],
    ['Rare jobs', p.low_frequency_job_count, 'Monthly/yearly jobs listed below but not in the heatmap.'],
    ['Overbooked hours', stats.overcommit, 'Hours where planned jobs need more leases than capacity' + (tz === 'IST' ? ' (IST hour bins).' : '.')],
    ['Typical job length', (p.median_duration_hours != null ? p.median_duration_hours + 'h' : '—'), 'Median time a job holds a lease (full pre → test → post).'],
    ['Sippy data coverage', p.sippy_coverage != null ? Math.round(p.sippy_coverage * 100) + '%' : '—', 'Share of jobs with real runtime data from Sippy CI.'],
  ];
  document.getElementById('metrics').innerHTML = items.map(([label, value, hint]) =>
    `<div class="metric"><div class="label">${{label}}</div><div class="value">${{value}}</div><div class="hint">${{hint}}</div></div>`
  ).join('');
  document.getElementById('lease-names').innerHTML =
    `<span class="lease-label">Lease type</span> <code>${{p.lease_type || '—'}}</code><br/>` +
    `<span class="lease-label">Leases</span> ${{p.lease_names.map(n => `<code>${{n}}</code>`).join(', ') || '(none found)'}}`;
}}

function renderHeatmap(p) {{
  const days = DATA.day_names;
  const tzLabel = tz === 'IST' ? 'IST' : 'UTC';
  let html = '<tr><th></th>' + [...Array(24).keys()].map(h =>
    `<th>${{formatHourLabel(h)}}<span class="sub">${{tzLabel}}</span></th>`
  ).join('') + '</tr>';
  for (let d = 0; d < 7; d++) {{
    html += `<tr><th>${{days[d]}}</th>`;
    for (let h = 0; h < 24; h++) {{
      const inUse = displayDemand(p, d, h);
      const available = p.capacity - inUse;
      const cls = cellClass(inUse, p.capacity);
      const tip = `${{formatDisplaySlot(d, h)}} — ${{inUse}} leases in use, ${{available}} available (of ${{p.capacity}})`;
      html += `<td class="${{cls}}" data-d="${{d}}" data-h="${{h}}" title="${{tip}}">${{inUse}}</td>`;
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
  const jobs = jobsForDisplayHour(p, d, h);
  const inUse = displayDemand(p, d, h);
  const available = p.capacity - inUse;
  el.style.display = 'block';
  el.innerHTML = `<h3>${{formatDisplaySlot(d, h)}} — ${{inUse}} lease(s) in use · ${{available}} available (capacity ${{p.capacity}})</h3>` +
    (jobs.length ? `<ul>${{jobs.map(j =>
      `<li><code>${{j.as}}</code> · OCP ${{j.ocp_version}} · starts ${{formatJobStart(j.start)}} · holds ${{j.leases_held != null ? j.leases_held : 1}} lease(s) · lease hold ${{j.duration_hours}}h${{j.observed_duration_hours != null && j.observed_duration_hours !== j.duration_hours ? ` (observed ${{j.observed_duration_hours}}h)` : ''}} ${{durationPill(j)}} · <span class="mono">${{j.workflow || ''}}</span></li>`
    ).join('')}}</ul>` : '<p style="color:var(--muted)">No overlapping weekly/daily jobs — all leases available this hour.</p>');
}}

function wireSlotScroll(listEl) {{
  if (!listEl) return;
  const wrap = listEl.closest('.slot-scroll');
  const block = listEl.closest('.slot-block');
  const foot = block ? block.querySelector('.slot-foot') : null;
  const update = () => {{
    const canScroll = listEl.scrollHeight > listEl.clientHeight + 4;
    const atBottom = listEl.scrollTop + listEl.clientHeight >= listEl.scrollHeight - 6;
    if (wrap) wrap.classList.toggle('is-bottom', !canScroll || atBottom);
    if (foot) foot.style.display = canScroll && !atBottom ? 'flex' : 'none';
  }};
  listEl.onscroll = update;
  requestAnimationFrame(update);
}}

function buildDisplayDemandGrid(p) {{
  const grid = Array.from({{ length: 7 }}, () => Array(24).fill(0));
  for (let d = 0; d < 7; d++) {{
    for (let h = 0; h < 24; h++) grid[d][h] = displayDemand(p, d, h);
  }}
  return grid;
}}

function rankDisplaySlots(grid, capacity) {{
  const cells = [];
  for (let d = 0; d < 7; d++) {{
    for (let h = 0; h < 24; h++) {{
      const demand = grid[d][h];
      cells.push({{ dow: d, hour: h, demand, spare: capacity - demand, capacity }});
    }}
  }}
  const busiest = [...cells].sort((a, b) => b.demand - a.demand || a.dow - b.dow || a.hour - b.hour);
  const free = cells
    .filter(c => c.spare >= 1)
    .sort((a, b) => b.spare - a.spare || a.dow - b.dow || a.hour - b.hour);
  return {{ busiest, free }};
}}

function contiguousDisplayBlocks(grid, capacity, minSpare = 1, minHours = 2) {{
  const n = 7 * 24;
  const spare = [];
  for (let d = 0; d < 7; d++) for (let h = 0; h < 24; h++) spare.push(capacity - grid[d][h]);
  const blocks = [];
  if (spare.every(s => s >= minSpare)) {{
    blocks.push({{ dow: 0, start_hour: 0, end_dow: 0, end_hour: 0, hours: n, min_spare: Math.min(...spare) }});
    return blocks;
  }}
  const starts = [];
  for (let i = 0; i < n; i++) {{
    if (spare[i] >= minSpare && spare[(i - 1 + n) % n] < minSpare) starts.push(i);
  }}
  for (const i of starts) {{
    let j = i;
    let minSp = spare[i];
    while (spare[j % n] >= minSpare) {{
      minSp = Math.min(minSp, spare[j % n]);
      j += 1;
      if (j - i >= n) break;
    }}
    const length = Math.min(j - i, n);
    if (length < minHours) continue;
    const start_d = Math.floor(i / 24);
    const start_h = i % 24;
    const endPos = (i + length) % n;
    const end_d = Math.floor(endPos / 24);
    const end_h = endPos % 24;
    blocks.push({{
      dow: start_d,
      start_hour: start_h,
      end_dow: end_d,
      end_hour: end_h,
      hours: length,
      min_spare: minSp,
    }});
  }}
  blocks.sort((a, b) => b.min_spare - a.min_spare || b.hours - a.hours || a.dow - b.dow || a.start_hour - b.start_hour);
  return blocks;
}}

function renderSlots(p) {{
  const blocksEl = document.getElementById('free-blocks');
  const freeEl = document.getElementById('free-slots');
  const busyEl = document.getElementById('busy-slots');
  // Always derive lists from the active timezone grid so IST matches the heatmap.
  const grid = buildDisplayDemandGrid(p);
  const ranked = rankDisplaySlots(grid, p.capacity);
  const blocks = contiguousDisplayBlocks(grid, p.capacity);
  blocksEl.innerHTML = blocks.map(b =>
    `<li><span>${{formatDisplaySlot(b.dow, b.start_hour)}} → ${{formatDisplaySlot(b.end_dow, b.end_hour)}} (${{b.hours}}h)</span><span class="badge">≥${{b.min_spare}} leases free</span></li>`
  ).join('') || '<li style="color:var(--muted)">No multi-hour free windows under current model.</li>';
  freeEl.innerHTML = ranked.free.slice(0, 48).map(c =>
    `<li><span>${{formatDisplaySlot(c.dow, c.hour)}}</span><span class="badge">${{c.demand}} in use · ${{c.spare}} available</span></li>`
  ).join('') || '<li style="color:var(--muted)">No free capacity under current model.</li>';
  busyEl.innerHTML = ranked.busiest.slice(0, 48).map(c =>
    `<li><span>${{formatDisplaySlot(c.dow, c.hour)}}</span><span class="badge busy">${{c.demand}} in use / ${{c.capacity}} · ${{c.spare}} free</span></li>`
  ).join('');
  [blocksEl, freeEl, busyEl].forEach(wireSlotScroll);
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
    if (dsrc === 'workflow' && !['workflow-median','as-median','family-median'].includes(j.duration_source)) return false;
    if (dsrc === 'default' && !['default','timeout'].includes(j.duration_source)) return false;
    if (!q) return true;
    const english = formatCronEnglish(j.cron_english || (j.interval ? ('Every ' + j.interval) : ''));
    const hay = [j.as, j.cron || '', j.cron_english || '', english, j.workflow || '', j.source_file, j.ocp_version, j.prow_job_name || ''].join(' ').toLowerCase();
    return hay.includes(q);
  }});
  document.getElementById('jobs-body').innerHTML = rows.map(j => {{
    const hold = j.duration_hours != null ? `${{j.duration_hours}}h` : '—';
    const observed = (j.observed_duration_hours != null && j.observed_duration_hours !== j.duration_hours)
      ? ` <span style="color:var(--muted)">(obs ${{j.observed_duration_hours}}h)</span>` : '';
    const english = formatCronEnglish(j.cron_english || (j.interval ? ('Every ' + j.interval) : ''));
    const holdTitle = (j.sippy && j.sippy.borrowed)
      ? `Full job wall-clock from same job on OCP ${{j.sippy.release}} (this version has no Sippy history), plus lease buffer`
      : 'Full job wall-clock (pre→test→post) plus lease buffer';
    return `
    <tr>
      <td><code>${{j.as}}</code></td>
      <td>${{j.ocp_version}}</td>
      <td class="mono">${{j.cron || j.interval || ''}}</td>
      <td class="cron-english">${{english}}</td>
      <td><span class="class-label">${{prettyLabel(j.schedule_class)}}</span></td>
      <td title="${{holdTitle}}">${{hold}}${{observed}} ${{durationPill(j)}}</td>
      <td class="mono">${{j.workflow || ''}}</td>
      <td class="links" style="font-size:.75rem;white-space:nowrap">${{jobHistoryLinks(j)}}</td>
    </tr>`;
  }}).join('') || '<tr><td colspan="8" style="color:var(--muted)">No jobs match filters.</td></tr>';
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

    config_dir = release_repo / "ci-operator" / "config"
    boskos_path = release_repo / "core-services" / "prow" / "02_config" / "_boskos.yaml"
    profiles_path = (
        release_repo
        / "ci-operator"
        / "step-registry"
        / "cluster-profiles"
        / "cluster-profiles-config.yaml"
    )
    step_registry = release_repo / "ci-operator" / "step-registry"

    for required in (config_dir, boskos_path, profiles_path):
        if not required.exists():
            print(f"ERROR: missing {required}", file=sys.stderr)
            return 1

    if args.profiles:
        profiles = list(dict.fromkeys(args.profiles))
    else:
        profiles = discover_profiles(profiles_path)
    profile_set = set(profiles)

    source_meta = git_metadata(release_repo)
    print(
        f"openshift/release @ {source_meta['short_sha']} {source_meta.get('subject', '')}",
        flush=True,
    )
    print(f"Profiles ({len(profiles)}): {', '.join(profiles)}", flush=True)

    print("Parsing cluster profile lease map …", flush=True)
    lease_map = parse_profile_lease_map(profiles_path, profile_set)
    missing_map = [p for p in profiles if p not in lease_map]
    if missing_map:
        print(f"WARN: no lease_type mapping for: {', '.join(missing_map)}", file=sys.stderr)

    lease_types = set(lease_map.values())
    print("Parsing Boskos lease inventory …", flush=True)
    leases = parse_boskos_leases(boskos_path, lease_types)

    print("Indexing workflow lease declarations …", flush=True)
    workflow_leases = build_workflow_lease_index(step_registry)
    if workflow_leases:
        print(f"  workflows with explicit leases: {len(workflow_leases)}", flush=True)

    print(f"Parsing jobs under {config_dir} …", flush=True)
    jobs = parse_jobs(
        config_dir,
        profile_set,
        release_repo,
        lease_map=lease_map,
        workflow_leases=workflow_leases,
    )

    for job in jobs:
        if job["cron"]:
            schedule_class, slots = classify_and_expand_cron(job["cron"])
            job["cron_english"] = cron_to_english(job["cron"])
        elif job["interval"]:
            schedule_class, slots = expand_interval(job["interval"])
            job["cron_english"] = f"Every {job['interval']}"
        else:
            schedule_class, slots = "invalid", []
            job["cron_english"] = ""
        job["schedule_class"] = schedule_class
        job["slots"] = slots

    duration_meta = enrich_job_durations(
        jobs,
        fetch_sippy=args.fetch_sippy_durations,
        default_hours=args.duration_hours,
        min_hours=args.sippy_min_hours,
        max_hours=args.sippy_max_hours,
        lease_buffer_minutes=args.lease_buffer_minutes,
    )
    print(
        f"Lease holds (full job wall-clock pre→test→post): "
        f"sippy={duration_meta['matched']} "
        f"workflow={duration_meta['workflow_fallback']} "
        f"default={duration_meta['default_fallback']} "
        f"buffer={args.lease_buffer_minutes:g}m",
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
                    "cron_english": j.get("cron_english"),
                    "interval": j["interval"],
                    "schedule_class": j["schedule_class"],
                    "workflow": j["workflow"],
                    "source_file": j["source_file"],
                    "timeout": j["timeout"],
                    "weekly_starts": len(j.get("slots") or []),
                    "observed_duration_hours": j.get("observed_duration_hours"),
                    "duration_hours": j.get("duration_hours"),
                    "lease_buffer_minutes": j.get("lease_buffer_minutes"),
                    "duration_source": j.get("duration_source"),
                    "leases_held": j.get("leases_held") or 1,
                    "prow_job_name": j.get("prow_job_name"),
                    "sippy": j.get("sippy"),
                }
                for j in sorted(pjobs, key=lambda x: (x["ocp_version"], x["as"]))
            ],
        }

    out_dir.mkdir(parents=True, exist_ok=True)
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(IST)
    generated_at = (
        f"{now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC · "
        f"{now_ist.strftime('%Y-%m-%d %H:%M:%S')} IST"
    )
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
    if args.write_fingerprint:
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
        f"Duration model: successful ProwJob wall-clock | "
        f"sippy={'on' if args.fetch_sippy_durations else 'off'} "
        f"| fallback={args.duration_hours}h | buffer={args.lease_buffer_minutes:g}m | jobs={len(jobs)}"
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
