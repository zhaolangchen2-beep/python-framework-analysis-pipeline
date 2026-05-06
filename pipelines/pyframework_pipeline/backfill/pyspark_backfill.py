"""PySpark-specific backfill: source anchors and UDF tracking.

Creates source anchors mapping Python UDF functions to their source code locations.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


def backfill_pyspark_source(
    project_path: Path,
    source_file: Path,
) -> dict[str, Any]:
    """Backfill PySpark source anchors from UDF modules.

    Parameters
    ----------
    project_path : Path
        Project directory containing workload code
    source_file : Path
        Path to source.json to update

    Returns
    -------
    dict with updated source content
    """
    source = json.loads(source_file.read_text(encoding="utf-8"))

    # Find PySpark UDF modules
    workload_dir = project_path / "workload" / "tpch" / "pyspark"
    udf_dir = workload_dir / "udfs"

    source_anchors = {}

    # Scan each UDF module for function definitions
    if udf_dir.exists():
        for udf_file in sorted(udf_dir.glob("tpch_*.py")):
            if udf_file.name == "tpch_constants.py":
                continue

            content = udf_file.read_text(encoding="utf-8")
            lines = content.splitlines()

            module_name = udf_file.stem

            # Extract function definitions and their line numbers
            for idx, line in enumerate(lines, 1):
                if line.strip().startswith("def "):
                    func_name = line.split("(")[0].replace("def ", "").strip()
                    func_id = f"{module_name}.{func_name}"

                    func_type = "setup" if func_name == "setup" else "utility"

                    source_anchors[func_id] = {
                        "file": str(udf_file.relative_to(project_path)),
                        "module": module_name,
                        "function": func_name,
                        "line": idx,
                        "type": func_type,
                    }

    # Add framework runner functions
    runner_file = workload_dir / "framework" / "runner.py"
    if runner_file.exists():
        content = runner_file.read_text(encoding="utf-8")
        lines = content.splitlines()

        for idx, line in enumerate(lines, 1):
            if line.strip().startswith("def "):
                func_name = line.split("(")[0].replace("def ", "").strip()
                func_id = f"runner.{func_name}"

                source_anchors[func_id] = {
                    "file": str(runner_file.relative_to(project_path)),
                    "module": "runner",
                    "function": func_name,
                    "line": idx,
                    "type": "framework",
                }

    source["sourceAnchors"] = source_anchors
    return source


def update_source_file(
    source: dict[str, Any],
    source_file: Path,
) -> None:
    """Write updated source back to file.

    Parameters
    ----------
    source : dict
        Updated source content
    source_file : Path
        Path to source.json file
    """
    source_file.write_text(
        json.dumps(source, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
