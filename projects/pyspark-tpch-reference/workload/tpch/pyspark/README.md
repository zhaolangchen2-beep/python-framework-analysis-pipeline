# PySpark TPC-H Workload

This directory contains the PySpark TPC-H benchmark workload integration.

## Files

- **`run.py`** — Entry point for benchmark execution (stub; to be replaced with upstream implementation)
- **`collect_results.py`** — Result collection wrapper that executes run.py and parses timing data

## Setup

The actual TPC-H query implementations should be obtained from:
https://github.com/zhaolangchen2-beep/lang_env

Expected structure:
```
pyspark/
├── run.py (main benchmark runner with PostUDF instrumentation)
├── apps/ (query implementations)
└── ...
```

## Timing Collection

The `collect_results.py` wrapper:
1. Calls `spark-submit run.py --query <Q> --rows <N>`
2. Captures stdout for `[BENCHMARK_SUMMARY]` JSON lines
3. Writes `timing-normalized.json` matching acquisition schema

### [BENCHMARK_SUMMARY] Format

The run.py must output lines like:
```
[BENCHMARK_SUMMARY] {"caseId":"q06","recordCount":1000000,"totalFrameworkOverheadNs":..,"totalPyDurationNs":..}
```

Required fields:
- `recordCount`: Number of records processed
- `totalFrameworkOverheadNs`: Framework overhead in nanoseconds
- `totalPyDurationNs`: Python execution time in nanoseconds
- `caseId` (optional): Query ID, defaults to --query parameter
