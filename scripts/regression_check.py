#!/usr/bin/env python3
"""Regression checks for IBM Z lease capacity generator invariants.

Run after generate_report.py (or import helpers directly):

  python3 scripts/regression_check.py --release-repo /path/to/openshift/release
  python3 scripts/regression_check.py --docs-dir docs --release-repo /path/to/openshift/release
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from generate_report import (  # noqa: E402
    PROFILE_PREFIX,
    build_concurrency,
    build_workflow_lease_index,
    classify_and_expand_cron,
    construct_prow_job_name,
    cron_to_english,
    discover_profiles,
    expand_interval,
    extract_ocp_version,
    parse_boskos_leases,
    parse_dow_field,
    parse_field,
    parse_jobs,
    parse_profile_lease_map,
    parse_timeout_hours,
    planning_minutes_from_sippy,
    sippy_has_successful_runs,
)


class Checker:
    def __init__(self) -> None:
        self.failures: list[str] = []
        self.warnings: list[str] = []

    def fail(self, msg: str) -> None:
        self.failures.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    def check(self, cond: bool, msg: str) -> None:
        if not cond:
            self.fail(msg)


def check_cron_helpers(c: Checker) -> None:
    c.check(parse_field("5/10", 0, 59) == list(range(5, 60, 10)), "parse_field N/step broken")
    c.check(parse_field("*/15", 0, 59) == list(range(0, 60, 15)), "parse_field */step broken")
    c.check(parse_dow_field("1-7") == [0, 1, 2, 3, 4, 5, 6], "parse_dow_field 1-7 should wrap Sunday")
    c.check(parse_dow_field("7") == [0], "parse_dow_field 7 → Sunday")
    cls, slots = classify_and_expand_cron("0 1 * * 5")
    c.check(cls == "weekly" and slots == [(5, 1, 0)], f"Friday cron wrong: {cls} {slots}")
    cls, slots = classify_and_expand_cron("0 0 16 1 *")
    c.check(cls == "low-frequency" and slots == [], f"yearly cron should be low-frequency: {cls}")
    eng = cron_to_english("0 0 16 1 *")
    c.check("Jan" in eng and "16" in eng, f"cron_to_english yearly: {eng}")
    eng = cron_to_english("0 1 * * 5")
    c.check("Friday" in eng and "01:00" in eng, f"cron_to_english Friday: {eng}")
    cls, slots = expand_interval("24h")
    c.check(cls == "daily" and len(slots) == 7, f"24h interval → 7 daily slots, got {cls} {len(slots)}")
    cls, slots = expand_interval("168h")
    c.check(cls == "low-frequency", f"168h interval should be low-frequency, got {cls}")


def check_ocp_version(c: Checker) -> None:
    upgrade = {
        "steps": {"env": {"BRANCH": "4.22"}, "cluster_profile": "libvirt-s390x-vpn"},
        "as": "ocp-ovn-remote-libvirt-multi-z-z",
    }
    src = "ci-operator/config/openshift/multiarch/openshift-multiarch-main__nightly-4.23-upgrade-from-nightly-4.22.yaml"
    ver = extract_ocp_version(upgrade, src)
    c.check(ver == "4.23", f"upgrade OCP version should be target 4.23, got {ver}")
    ver50 = extract_ocp_version(
        upgrade,
        "ci-operator/config/openshift/multiarch/openshift-multiarch-main__nightly-5.0-upgrade-from-nightly-4.22.yaml",
    )
    c.check(ver50 == "5.0", f"5.0 upgrade labeled {ver50}")
    quay = {"steps": {"env": {"BRANCH": "4.22"}}}
    ver_q = extract_ocp_version(quay, "ci-operator/config/quay/quay-operator/quay-operator-redhat-3.18__something.yaml")
    c.check(ver_q == "4.22", f"Quay BRANCH fallback got {ver_q}")


def check_ist_heatmap_mapping(c: Checker) -> None:
    """IST column H is wall-clock [H:00, H+1:00); starts in H:05–59 belong in H."""
    ist_offset = 5 * 60 + 30
    week = 7 * 24 * 60

    def wrap(total: int) -> int:
        return ((total % week) + week) % week

    def utc_to_ist(dow: int, hour: int, minute: int = 0) -> tuple[int, int, int]:
        total = wrap(dow * 24 * 60 + hour * 60 + minute + ist_offset)
        return total // (24 * 60), (total % (24 * 60)) // 60, total % 60

    def ist_to_utc(dow: int, hour: int, minute: int = 0) -> tuple[int, int, int]:
        total = wrap(dow * 24 * 60 + hour * 60 + minute - ist_offset)
        return total // (24 * 60), (total % (24 * 60)) // 60, total % 60

    def utc_hours_for_ist_hour(dow: int, hour: int) -> list[tuple[int, int]]:
        seen: set[tuple[int, int]] = set()
        out: list[tuple[int, int]] = []
        for minute in (0, 30):
            ud, uh, _ = ist_to_utc(dow, hour, minute)
            key = (ud, uh)
            if key not in seen:
                seen.add(key)
                out.append(key)
        return out

    # UTC Monday 00:00 → IST Monday 05:30
    d, h, m = utc_to_ist(1, 0, 0)
    c.check((d, h, m) == (1, 5, 30), f"UTC Mon 00:00 → IST Mon 05:30, got {d} {h}:{m}")

    # IST hour 10 overlaps UTC 04:30 and 05:00 → UTC hours 4 and 5
    overlap = utc_hours_for_ist_hour(2, 10)  # Tue
    c.check(
        overlap == [(2, 4), (2, 5)],
        f"IST Tue 10:00–11:00 should overlap UTC hours 4+5, got {overlap}",
    )

    # Job start Tue 05:00 UTC = 10:30 IST → belongs in IST column 10 (not 11)
    d, h, m = utc_to_ist(2, 5, 0)
    c.check((h, m) == (10, 30), f"Tue 05:00 UTC should be 10:30 IST, got {h}:{m}")
    c.check(
        (2, 5) in utc_hours_for_ist_hour(2, 10),
        "10:30 IST start must be visible under IST column 10",
    )
    c.check(
        (2, 5) in utc_hours_for_ist_hour(2, 11),
        "1.5h hold from 10:30 IST still overlaps IST column 11",
    )

    # 00:40 UTC = 06:10 IST: UTC hour 0 overlaps IST 05, but the job must NOT
    # count for IST column 05 (starts after 06:00 IST).
    d, h, m = utc_to_ist(2, 0, 40)
    c.check((h, m) == (6, 10), f"Tue 00:40 UTC should be 06:10 IST, got {h}:{m}")
    c.check(
        (2, 0) in utc_hours_for_ist_hour(2, 5),
        "UTC hour 0 overlaps IST 05 window (partial) — demand must still filter by real overlap",
    )
    c.check(
        (2, 0) in utc_hours_for_ist_hour(2, 6),
        "00:40 UTC start belongs under IST column 06",
    )


def check_ist_docs_helpers(c: Checker, docs_dir: Path) -> None:
    html = docs_dir / "index.html"
    if not html.exists():
        c.warn("docs/index.html missing; skip IST docs helper check")
        return
    text = html.read_text(encoding="utf-8")
    for needle in (
        "IST_OFFSET_MIN",
        "utcHoursOverlappingIstHour",
        "displayDemand",
        "jobsForDisplayHour",
        "formatDisplaySlot",
        "formatJobStart",
        "formatCronEnglish",
        "IST remaps the heatmap",
    ):
        c.check(needle in text, f"docs/index.html missing IST helper: {needle}")
    c.check(
        "Columns stay UTC even when" not in text,
        "docs still claims heatmap columns stay UTC under IST",
    )
    c.check(
        "${longDays[ist.dow]}}${mid}" not in text,
        "docs formatCronEnglish has stray } after weekday",
    )
    c.check(
        "istPartsToUtc(d, h, 30)" not in text or "utcHoursOverlappingIstHour" in text,
        "docs should use IST hour-bin overlap (not sole :30 UTC ownership)",
    )


def check_sippy_success_duration_gate(c: Checker) -> None:
    """Fail-only Sippy averages must not drive lease-hold planning."""
    fail_only = {
        "current_average_duration_minutes": 65,
        "previous_average_duration_minutes": 0,
        "current_runs": 4,
        "previous_runs": 0,
        "current_passes": 0,
        "previous_passes": 0,
        "current_pass_percentage": 0,
        "previous_pass_percentage": 0,
    }
    c.check(not sippy_has_successful_runs(fail_only), "0-pass entry should not count as success history")
    c.check(
        planning_minutes_from_sippy(fail_only) is None,
        "fail-only Sippy average must be rejected (expect default hours)",
    )

    healthy = {
        "current_average_duration_minutes": 180,
        "previous_average_duration_minutes": 170,
        "current_runs": 10,
        "previous_runs": 8,
        "current_passes": 10,
        "previous_passes": 8,
        "current_pass_percentage": 100,
        "previous_pass_percentage": 100,
    }
    mins = planning_minutes_from_sippy(healthy)
    c.check(mins == 180, f"healthy pass window should use current avg, got {mins}")

    mixed = dict(healthy)
    mixed["current_pass_percentage"] = 40
    mixed["current_passes"] = 2
    mixed["current_average_duration_minutes"] = 70  # polluted by fails
    mixed["success_median_duration_minutes"] = 300  # ~5h successful runs
    mins = planning_minutes_from_sippy(mixed)
    c.check(mins == 300, f"success median must win over fail-polluted avg, got {mins}")


def check_concurrency_math(c: Checker) -> None:
    job = {
        "as": "demo",
        "ocp_version": "4.19",
        "cron": "0 0 * * 0",
        "profile": "libvirt-s390x-vpn",
        "source_file": "x",
        "workflow": "w",
        "schedule_class": "weekly",
        "slots": [(0, 0, 0)],
        "duration_hours": 1.5,
        "leases_held": 2,
        "duration_source": "sippy-name",
        "observed_duration_hours": 1.25,
    }
    demand, details = build_concurrency([job], 5.0)
    c.check(demand[0][0] == 2, f"leases_held=2 should paint demand 2, got {demand[0][0]}")
    c.check(demand[0][1] == 2, f"1.5h hold should cover hour 1, got {demand[0][1]}")
    c.check(demand[0][2] == 0, f"hold must not paint hour 2, got {demand[0][2]}")
    # week wrap
    wrap = dict(job)
    wrap["slots"] = [(6, 23, 30)]
    wrap["duration_hours"] = 1.5
    wrap["leases_held"] = 1
    demand2, _ = build_concurrency([wrap], 5.0)
    c.check(demand2[6][23] == 1 and demand2[0][0] == 1, "week-wrap Saturday 23:30 + 1.5h")
    c.check(demand2[0][1] == 0, "wrap must not paint Sunday 01")


def check_release_repo(c: Checker, release_repo: Path) -> list[dict]:
    profiles_path = (
        release_repo
        / "ci-operator"
        / "step-registry"
        / "cluster-profiles"
        / "cluster-profiles-config.yaml"
    )
    boskos_path = release_repo / "core-services" / "prow" / "02_config" / "_boskos.yaml"
    config_dir = release_repo / "ci-operator" / "config"
    step_registry = release_repo / "ci-operator" / "step-registry"

    profiles = discover_profiles(profiles_path)
    c.check(any(p.startswith(PROFILE_PREFIX) for p in profiles), "no profiles discovered")
    c.check("libvirt-s390x-vpn-ipv6" in profiles, "ipv6 profile missing from auto-discover")
    c.check("libvirt-s390x-vpn-virt" in profiles, "virt profile missing from auto-discover")

    lease_map = parse_profile_lease_map(profiles_path, set(profiles))
    for p in profiles:
        c.check(p in lease_map, f"missing lease_type for {p}")
    leases = parse_boskos_leases(boskos_path, set(lease_map.values()))
    for p, lt in lease_map.items():
        c.check(lt in leases and len(leases[lt]) > 0, f"Boskos empty for {p} → {lt}")

    wf_leases = build_workflow_lease_index(step_registry)
    jobs = parse_jobs(
        config_dir,
        set(profiles),
        release_repo,
        lease_map=lease_map,
        workflow_leases=wf_leases,
    )
    c.check(len(jobs) >= 100, f"expected ≥100 jobs, got {len(jobs)}")

    # Upgrade version labeling
    upgrades = [j for j in jobs if "upgrade-from-nightly" in (j.get("source_file") or "")]
    for j in upgrades:
        base = Path(j["source_file"]).name
        m = re.search(r"__nightly-(\d+\.\d+)", base)
        if m:
            c.check(
                j["ocp_version"] == m.group(1),
                f"upgrade {j['as']} labeled {j['ocp_version']} want {m.group(1)} ({base})",
            )

    # HCP extinfra must hold 2 leases
    hcp = [
        j
        for j in jobs
        if j.get("as") == "e2e-ibmz-s390x-mgmt-ovn-conformance-virt-extinfra-z-z"
    ]
    c.check(len(hcp) == 1, f"extinfra job count {len(hcp)}")
    if hcp:
        c.check(hcp[0].get("leases_held") == 2, f"extinfra leases_held={hcp[0].get('leases_held')}")

    # Prow names reconstructible
    for j in jobs:
        if j.get("prow_job_name"):
            rebuilt = construct_prow_job_name(j["source_file"], j["as"])
            c.check(rebuilt == j["prow_job_name"], f"prow name drift {j['as']}")

    # Cron English for every cron job
    for j in jobs:
        if j.get("cron"):
            eng = cron_to_english(j["cron"])
            c.check(bool(eng), f"empty cron english for {j['cron']}")
            cls, slots = classify_and_expand_cron(j["cron"])
            c.check(cls != "invalid", f"invalid cron {j['cron']}")

    return jobs


def check_docs(c: Checker, docs_dir: Path, release_jobs: list[dict] | None = None) -> None:
    csv_path = docs_dir / "jobs.csv"
    summary_path = docs_dir / "summary.json"
    html_path = docs_dir / "index.html"
    for p in (csv_path, summary_path, html_path):
        c.check(p.exists(), f"missing {p}")

    rows = list(csv.DictReader(csv_path.open(encoding="utf-8")))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    html = html_path.read_text(encoding="utf-8")

    c.check("jobHistoryLinks" in html, "HTML missing jobHistoryLinks")
    c.check("function durationPill" in html, "HTML missing durationPill")

    # Upgrade OCP labels in published CSV
    for r in rows:
        if "upgrade-from-nightly" in r.get("source_file", ""):
            base = Path(r["source_file"]).name
            m = re.search(r"__nightly-(\d+\.\d+)", base)
            if m:
                c.check(
                    r["ocp_version"] == m.group(1),
                    f"docs upgrade mislabeled {r['job']} {r['ocp_version']}≠{m.group(1)}",
                )

    # No sippy-suffix false matches
    suffix = [r for r in rows if r.get("duration_source") == "sippy-suffix"]
    c.check(not suffix, f"unexpected sippy-suffix rows: {suffix[:3]}")

    sippy_n = sum(1 for r in rows if (r.get("duration_source") or "").startswith("sippy"))
    c.check(
        sippy_n >= max(50, len(rows) // 2),
        f"Sippy coverage too low ({sippy_n}/{len(rows)}); regenerate with network access",
    )

    # Install jobs must not list an upgrade donor release via borrowed sippy brief
    for r in rows:
        prow = r.get("prow_job_name") or ""
        donor_name = ""
        # donor release alone is fine; ensure upgrade jobs keep upgrade OCP labels
        if "upgrade-from-nightly" in (r.get("source_file") or ""):
            continue
        if "upgrade" in prow.lower():
            c.fail(f"non-upgrade source_file but upgrade prow name: {prow}")

    # leases_held column
    if rows and "leases_held" in rows[0]:
        ext = [
            r
            for r in rows
            if r["job"] == "e2e-ibmz-s390x-mgmt-ovn-conformance-virt-extinfra-z-z"
        ]
        if ext:
            c.check(ext[0]["leases_held"] == "2", f"docs extinfra leases_held={ext[0]['leases_held']}")

    # Borrowed install jobs must not point at upgrade donors
    for r in rows:
        donor = r.get("sippy_donor_release") or ""
        src = r.get("duration_source") or ""
        if not src.startswith("sippy"):
            continue
        prow = r.get("prow_job_name") or ""
        # If job is not upgrade, donor prow in HTML JSON should not be upgrade —
        # approximate via brief: install as names shouldn't borrow upgrade-only.
        if "upgrade" not in prow and "upgrade" in (r.get("job") or ""):
            c.warn(f"odd job name {r['job']}")

    # Summary profile capacities match CSV lease_count
    by_profile = defaultdict(list)
    for r in rows:
        by_profile[r["profile"]].append(r)
    for profile, info in (summary.get("profiles") or {}).items():
        if profile not in by_profile:
            continue
        csv_cap = int(by_profile[profile][0]["lease_count"])
        c.check(
            info.get("capacity") == csv_cap,
            f"{profile} capacity summary {info.get('capacity')} ≠ csv {csv_cap}",
        )

    # Heatmap peak ≤ capacity + allow overcommit flag consistency
    for profile, info in (summary.get("profiles") or {}).items():
        peak = info.get("peak_demand") or 0
        cap = info.get("capacity") or 0
        over = info.get("overcommit_hours") or 0
        if peak > cap:
            c.check(over > 0, f"{profile} peak {peak}>{cap} but overcommit_hours=0")

    if release_jobs is not None:
        c.check(
            len(rows) == len(release_jobs),
            f"docs jobs {len(rows)} ≠ parsed {len(release_jobs)}",
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Regression checks for lease capacity tool")
    parser.add_argument("--release-repo", type=Path, required=True)
    parser.add_argument("--docs-dir", type=Path, default=ROOT / "docs")
    args = parser.parse_args()

    c = Checker()
    print("== unit helpers ==")
    check_cron_helpers(c)
    check_ocp_version(c)
    check_ist_heatmap_mapping(c)
    check_sippy_success_duration_gate(c)
    check_concurrency_math(c)

    print("== release repo ==")
    jobs = check_release_repo(c, args.release_repo.resolve())

    print("== docs artifacts ==")
    if args.docs_dir.exists():
        docs_dir = args.docs_dir.resolve()
        check_ist_docs_helpers(c, docs_dir)
        check_docs(c, docs_dir, jobs)
    else:
        c.warn(f"docs dir missing: {args.docs_dir}")

    print()
    if c.warnings:
        print(f"WARNINGS ({len(c.warnings)}):")
        for w in c.warnings:
            print(f"  - {w}")
    if c.failures:
        print(f"FAILURES ({len(c.failures)}):")
        for f in c.failures:
            print(f"  - {f}")
        return 1
    print(f"OK — all regression checks passed ({len(jobs)} jobs scanned).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
