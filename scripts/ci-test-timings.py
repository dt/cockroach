#!/usr/bin/env python3
"""
Collect per-test and per-package timing data from CI unit_test runs on master.

Produces two CSVs:
  packages.csv: run_id, run_date, package, status, duration_s
  tests.csv:    run_id, run_date, package, test_name, status, duration_s

Prerequisites:
  - gh CLI authenticated
  - engflow_auth authenticated: engflow_auth login mesolite.cluster.engflow.com

Usage:
  # Collect from the last 7 days (packages only -- fast)
  python3 scripts/ci-test-timings.py --from 2026-02-20 --to 2026-02-26

  # Collect packages + per-test detail for top 10 slowest packages
  python3 scripts/ci-test-timings.py --from 2026-02-26 --tests --top-packages 10

  # Collect packages + ALL per-test detail (slow -- thousands of downloads)
  python3 scripts/ci-test-timings.py --from 2026-02-26 --tests

  # Append to existing CSVs (skips already-collected run_ids)
  python3 scripts/ci-test-timings.py --from 2026-02-20 --outdir /tmp/timings
"""

import argparse
import concurrent.futures
import csv
import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta


REPO = "cockroachdb/cockroach"
ENGFLOW_SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".claude", "skills", "engflow-artifacts", "engflow_artifacts.py",
)


def run_cmd(cmd, timeout=60):
    """Run a command and return stdout."""
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        print(f"  cmd failed: {' '.join(cmd[:5])}...", file=sys.stderr)
        if result.stderr:
            print(f"  {result.stderr[:200]}", file=sys.stderr)
    return result.stdout


def find_ci_runs(from_date, to_date):
    """Find completed Essential CI unit_test runs on master in a date range."""
    # gh run list doesn't support date filtering directly, so fetch a generous
    # number and filter client-side.
    cmd = [
        "gh", "run", "list",
        "--repo", REPO,
        "--branch", "master",
        "--workflow", "GitHub Actions Essential CI",
        "--status", "completed",
        "--limit", "200",
        "--json", "databaseId,conclusion,createdAt",
    ]
    out = run_cmd(cmd, timeout=30)
    if not out:
        return []
    runs = json.loads(out)
    filtered = []
    for r in runs:
        if r["conclusion"] != "success":
            continue
        date = r["createdAt"][:10]
        if from_date <= date <= to_date:
            filtered.append({"run_id": r["databaseId"], "run_date": date})
    return filtered


def parse_ci_logs(run_id):
    """Parse CI logs for a run. Returns (invocation_id, package_rows).

    package_rows: list of (package, status, duration_s)
    """
    out = run_cmd(
        ["gh", "run", "view", str(run_id), "--repo", REPO, "--log"],
        timeout=120,
    )
    if not out:
        return None, []

    invocation_id = None
    packages = []
    for line in out.splitlines():
        if "unit_tests" not in line:
            continue
        if "run tests" in line:
            m = re.search(r"Invocation ID: ([0-9a-f-]+)", line)
            if m:
                invocation_id = m.group(1)
            m = re.search(r"(//\S+:\S+)\s+(PASSED|FAILED) in (\d+\.?\d*)s", line)
            if m:
                label = m.group(1)
                # //pkg/foo/bar:bar_test -> foo/bar
                pkg = label.removeprefix("//pkg/").rsplit(":", 1)[0]
                packages.append((pkg, m.group(2), float(m.group(3))))

    return invocation_id, packages


def get_shard_count(invocation_id, target_label):
    """Get the number of shards for a target via EngFlow API."""
    out = run_cmd(
        ["python3", ENGFLOW_SCRIPT, "list", invocation_id,
         "--target", target_label],
        timeout=30,
    )
    if not out:
        return 0
    m = re.search(r"Total shards with artifacts: (\d+)", out)
    return int(m.group(1)) if m else 0


def download_test_xml(invocation_id, target_label, shard, outdir):
    """Download test.xml for a specific shard. Returns path or None."""
    shard_dir = os.path.join(outdir, f"shard-{shard}")
    os.makedirs(shard_dir, exist_ok=True)
    xml_path = os.path.join(shard_dir, "test.xml")
    if os.path.exists(xml_path):
        return xml_path

    run_cmd(
        ["python3", ENGFLOW_SCRIPT, "download", invocation_id,
         "--target", target_label, "--shard", str(shard),
         "--artifacts", "test.xml", "--outdir", shard_dir],
        timeout=30,
    )
    return xml_path if os.path.exists(xml_path) else None


def parse_test_xml(xml_path):
    """Parse JUnit test.xml. Returns list of (test_name, status, duration_s)."""
    results = []
    try:
        tree = ET.parse(xml_path)
    except ET.ParseError:
        return results
    for tc in tree.findall(".//testcase"):
        name = tc.get("name", "")
        dur = float(tc.get("time", "0"))
        if tc.findall("failure") or tc.findall("error"):
            status = "FAIL"
        elif tc.findall("skipped"):
            status = "SKIP"
        else:
            status = "PASS"
        results.append((name, status, dur))
    return results


def collect_tests_for_target(invocation_id, target_label, cache_dir,
                             max_workers=8):
    """Download all shards and return per-test rows for a target."""
    safe = target_label.replace("//", "").replace("/", "_").replace(":", "_")
    outdir = os.path.join(cache_dir, safe)
    os.makedirs(outdir, exist_ok=True)

    shard_count = get_shard_count(invocation_id, target_label)
    if shard_count == 0:
        return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(download_test_xml, invocation_id, target_label, s, outdir): s
            for s in range(shard_count)
        }
        xml_paths = []
        for f in concurrent.futures.as_completed(futures):
            path = f.result()
            if path:
                xml_paths.append(path)

    all_tests = []
    for xml_path in xml_paths:
        all_tests.extend(parse_test_xml(xml_path))
    return all_tests


def load_existing_run_ids(csv_path):
    """Load run_ids already present in a CSV to avoid reimporting."""
    ids = set()
    if not os.path.exists(csv_path):
        return ids
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ids.add(int(row["run_id"]))
    return ids


def append_csv(path, fieldnames, rows):
    """Append rows to a CSV, creating it with headers if needed."""
    write_header = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Collect CI test timing data into CSVs"
    )
    parser.add_argument(
        "--from", dest="from_date", required=True,
        help="Start date inclusive (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--to", dest="to_date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date inclusive (YYYY-MM-DD). Default: today."
    )
    parser.add_argument(
        "--tests", action="store_true",
        help="Also collect per-test timings (requires engflow_auth). "
             "Without this flag, only package-level timings are collected."
    )
    parser.add_argument(
        "--top-packages", type=int, default=0,
        help="When collecting tests, only drill into the N slowest packages "
             "per run (0 = all). Ignored without --tests."
    )
    parser.add_argument(
        "--outdir", default="/tmp/ci-test-timings",
        help="Output directory for CSVs and cache (default: /tmp/ci-test-timings)"
    )
    parser.add_argument(
        "--workers", type=int, default=8,
        help="Parallel download workers (default: 8)"
    )
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    pkg_csv = os.path.join(args.outdir, "packages.csv")
    test_csv = os.path.join(args.outdir, "tests.csv")
    cache_dir = os.path.join(args.outdir, "cache")

    pkg_fields = ["run_id", "run_date", "package", "status", "duration_s"]
    test_fields = ["run_id", "run_date", "package", "test_name", "status",
                   "duration_s"]

    existing_pkg_runs = load_existing_run_ids(pkg_csv)
    existing_test_runs = load_existing_run_ids(test_csv) if args.tests else set()

    print(f"Finding CI runs on master from {args.from_date} to {args.to_date}...")
    runs = find_ci_runs(args.from_date, args.to_date)
    print(f"Found {len(runs)} successful run(s).")

    for ci_run in runs:
        run_id = ci_run["run_id"]
        run_date = ci_run["run_date"]

        # Skip if already fully collected.
        pkg_done = run_id in existing_pkg_runs
        test_done = not args.tests or run_id in existing_test_runs
        if pkg_done and test_done:
            print(f"  Run {run_id} ({run_date}): already collected, skipping.")
            continue

        print(f"  Run {run_id} ({run_date}): ", end="", flush=True)
        invocation_id, packages = parse_ci_logs(run_id)
        print(f"{len(packages)} packages", end="", flush=True)

        # Write package rows if not already done.
        if not pkg_done:
            pkg_rows = [
                {"run_id": run_id, "run_date": run_date, "package": pkg,
                 "status": status, "duration_s": dur}
                for pkg, status, dur in packages
            ]
            append_csv(pkg_csv, pkg_fields, pkg_rows)

        # Optionally collect per-test detail.
        if args.tests and invocation_id and not test_done:
            # Sort by duration desc, optionally limit.
            targets = sorted(packages, key=lambda x: -x[2])
            if args.top_packages > 0:
                targets = targets[:args.top_packages]

            print(f", drilling into {len(targets)} packages for per-test data...")
            test_rows = []
            for pkg, status, pkg_dur in targets:
                # Reconstruct target label from package name.
                # pkg like "backup" -> //pkg/backup:backup_test
                # pkg like "ccl/changefeedccl" -> //pkg/ccl/changefeedccl:changefeedccl_test
                base = pkg.rsplit("/", 1)[-1]
                target_label = f"//pkg/{pkg}:{base}_test"

                print(f"    {pkg:<55} {pkg_dur:>7.1f}s  ", end="", flush=True)
                tests = collect_tests_for_target(
                    invocation_id, target_label,
                    os.path.join(cache_dir, str(run_id)),
                    max_workers=args.workers,
                )
                print(f"({len(tests)} test cases)")

                for test_name, test_status, dur in tests:
                    test_rows.append({
                        "run_id": run_id, "run_date": run_date,
                        "package": pkg, "test_name": test_name,
                        "status": test_status, "duration_s": dur,
                    })

            append_csv(test_csv, test_fields, test_rows)
        else:
            print()

    # Summary.
    pkg_count = 0
    if os.path.exists(pkg_csv):
        with open(pkg_csv) as f:
            pkg_count = sum(1 for _ in f) - 1
    test_count = 0
    if os.path.exists(test_csv):
        with open(test_csv) as f:
            test_count = sum(1 for _ in f) - 1

    print(f"\nOutput:")
    print(f"  {pkg_csv} ({pkg_count} rows)")
    if args.tests:
        print(f"  {test_csv} ({test_count} rows)")
    print(f"\nQuery with DuckDB:")
    print(f"  duckdb -c \"SELECT package, avg(duration_s)::int avg_s FROM '{pkg_csv}' GROUP BY package ORDER BY avg_s DESC LIMIT 20\"")


if __name__ == "__main__":
    main()
