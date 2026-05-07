"""PySpark-specific backfill: source anchors and UDF tracking.

Creates schema-compliant sourceFiles and sourceAnchors for Python UDF code.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _file_id(relative_path: str) -> str:
    return relative_path.replace("/", "_").replace(".", "_")


def _anchor_id(module_name: str, func_name: str) -> str:
    return f"src_{module_name}_{func_name}"


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

    workload_dir = project_path / "workload" / "tpch" / "pyspark"
    udf_dir = workload_dir / "udfs"

    source_files: list[dict[str, Any]] = []
    source_anchors: list[dict[str, Any]] = []
    seen_files: set[str] = set()

    def add_file(relative_path: str) -> str:
        file_id = _file_id(relative_path)
        if file_id not in seen_files:
            source_files.append({
                "id": file_id,
                "path": relative_path,
            })
            seen_files.add(file_id)
        return file_id

    def add_anchors(file_path: Path, module_name: str, anchor_type: str) -> None:
        relative_path = str(file_path.relative_to(project_path))
        file_id = add_file(relative_path)
        content = file_path.read_text(encoding="utf-8")
        lines = content.splitlines()

        for idx, line in enumerate(lines, 1):
            if not line.strip().startswith("def "):
                continue
            func_name = line.split("(")[0].replace("def ", "").strip()
            source_anchors.append({
                "id": _anchor_id(module_name, func_name),
                "fileId": file_id,
                "functionId": f"{module_name}.{func_name}",
                "symbol": func_name,
                "sourceFile": relative_path,
                "line": idx,
                "type": "setup" if func_name == "setup" else anchor_type,
            })

    if udf_dir.exists():
        for udf_file in sorted(udf_dir.glob("tpch_*.py")):
            if udf_file.name == "tpch_constants.py":
                continue
            add_anchors(udf_file, udf_file.stem, "utility")

    runner_file = workload_dir / "framework" / "runner.py"
    if runner_file.exists():
        add_anchors(runner_file, "runner", "framework")

    source["sourceFiles"] = source_files
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
