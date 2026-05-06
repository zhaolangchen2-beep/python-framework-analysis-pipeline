#!/usr/bin/env python3
"""
Collect PySpark TPC-H benchmark results and parse timing data.

This wrapper:
1. Executes spark-submit run.py with specified query and row count
2. Captures stdout for PostUDF/JobTimeListener output
3. Parses [BENCHMARK_SUMMARY] JSON lines
4. Writes timing-normalized.json in the expected acquisition format

Usage:
    python collect_results.py --query q06 --rows 10000000 [--spark-home /opt/spark]
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


BENCHMARK_SUMMARY_RE = re.compile(r"\[BENCHMARK_SUMMARY\]\s*(\{.*\})")


def main():
    parser = argparse.ArgumentParser(
        description="PySpark TPC-H benchmark result collector"
    )
    parser.add_argument(
        "--query",
        required=True,
        help="Query ID (e.g., q06, q12)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        required=True,
        help="Number of rows for TPC-H data",
    )
    parser.add_argument(
        "--spark-home",
        default="/opt/spark",
        help="Path to Spark home directory (default: /opt/spark)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.cwd(),
        help="Output directory for timing-normalized.json (default: current directory)",
    )

    args = parser.parse_args()
    args.output_dir = Path(args.output_dir)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Execute the benchmark
    summaries = run_benchmark(
        query=args.query,
        rows=args.rows,
        spark_home=args.spark_home,
    )

    # Write normalized timing data
    write_timing_data(
        summaries=summaries,
        query=args.query,
        output_dir=args.output_dir,
    )


def run_benchmark(query: str, rows: int, spark_home: str) -> list[dict[str, Any]]:
    """Execute PySpark benchmark and collect [BENCHMARK_SUMMARY] lines.

    Parameters
    ----------
    query : str
        Query ID (e.g., q06)
    rows : int
        Number of rows for TPC-H data
    spark_home : str
        Path to Spark home directory

    Returns
    -------
    list of parsed BENCHMARK_SUMMARY JSON objects
    """
    workload_dir = Path(__file__).parent.absolute()
    spark_submit = Path(spark_home) / "bin" / "spark-submit"

    if not spark_submit.exists():
        raise FileNotFoundError(f"spark-submit not found at {spark_submit}")

    # Construct spark-submit command
    cmd = [
        str(spark_submit),
        "--master",
        "spark://spark-master:7077",
        "--deploy-mode",
        "client",
        str(workload_dir / "run.py"),
        "--query",
        query,
        "--rows",
        str(rows),
    ]

    print(f"Running: {' '.join(cmd)}", file=sys.stderr)

    try:
        result = subprocess.run(
            cmd,
            cwd=str(workload_dir),
            capture_output=True,
            text=True,
            timeout=600,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Benchmark timed out after 600s for query {query}")

    if result.returncode != 0:
        print(f"STDERR: {result.stderr}", file=sys.stderr)
        raise RuntimeError(
            f"spark-submit failed with return code {result.returncode}"
        )

    # Parse [BENCHMARK_SUMMARY] lines from stdout
    summaries = []
    for line in result.stdout.splitlines():
        m = BENCHMARK_SUMMARY_RE.search(line)
        if m:
            try:
                summary = json.loads(m.group(1))
                summaries.append(summary)
                print(f"Parsed summary: {summary}", file=sys.stderr)
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON: {e}", file=sys.stderr)
                continue

    if not summaries:
        print(
            "Warning: No [BENCHMARK_SUMMARY] found in stdout. "
            "This may indicate PostUDF instrumentation is missing.",
            file=sys.stderr,
        )

    return summaries


def write_timing_data(
    summaries: list[dict[str, Any]],
    query: str,
    output_dir: Path,
) -> None:
    """Write timing data in the normalized format expected by acquisition layer.

    Parameters
    ----------
    summaries : list of dict
        Parsed [BENCHMARK_SUMMARY] JSON objects
    query : str
        Query ID
    output_dir : Path
        Output directory for timing-normalized.json
    """
    # Group summaries by caseId (default to query ID)
    grouped: dict[str, dict[str, Any]] = {}

    for summary in summaries:
        case_id = summary.get("caseId", query)
        if case_id not in grouped:
            grouped[case_id] = {
                "raw": [],
                "total_overhead": 0,
                "total_py": 0,
                "total_records": 0,
            }
        grouped[case_id]["raw"].append(summary)
        grouped[case_id]["total_records"] += summary.get("recordCount", 0)
        grouped[case_id]["total_overhead"] += summary.get("totalFrameworkOverheadNs", 0)
        grouped[case_id]["total_py"] += summary.get("totalPyDurationNs", 0)

    # Compute normalized metrics
    cases = []
    for case_id, group in grouped.items():
        case_metrics = compute_metrics(case_id, group)
        cases.append(case_metrics)

    # Write output
    output_file = output_dir / "timing-normalized.json"
    output_data = {
        "schemaVersion": 1,
        "platform": "",
        "cases": cases,
    }

    output_file.write_text(
        json.dumps(output_data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print(f"Wrote timing data to {output_file}", file=sys.stderr)


def compute_metrics(case_id: str, group: dict[str, Any]) -> dict[str, Any]:
    """Compute normalized timing metrics for one case.

    Parameters
    ----------
    case_id : str
        Case identifier
    group : dict
        Aggregated timing data with keys: raw, total_overhead, total_py, total_records

    Returns
    -------
    dict with normalized metrics matching acquisition layer schema
    """
    n = group["total_records"]
    total_overhead = group["total_overhead"]
    total_py = group["total_py"]
    subtask_count = len(group["raw"])

    # Weighted averages (nanoseconds)
    avg_overhead = total_overhead / n if n > 0 else 0
    avg_py = total_py / n if n > 0 else 0

    return {
        "caseId": case_id,
        "platform": "",
        "recordCount": n,
        "subtaskCount": subtask_count,
        "timingSource": "benchmark_runner_post_udf",
        "metrics": {
            "frameworkCallTime": {
                "batch_total_ns": total_overhead,
                "per_invocation_ns": round(avg_overhead, 1),
                "per_record_ns": round(avg_overhead, 1),
            },
            "businessOperatorTime": {
                "batch_total_ns": total_py,
                "per_invocation_ns": round(avg_py, 1),
                "per_record_ns": round(avg_py, 1),
            },
        },
        "normalization": {
            "default": "per_invocation_ns",
            "available": ["batch_total_ns", "per_invocation_ns", "per_record_ns"],
        },
    }


if __name__ == "__main__":
    main()
