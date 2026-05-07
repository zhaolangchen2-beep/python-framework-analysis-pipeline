"""Step 6 orchestrator: load four-layer JSON, run backfill sub-modules, write back.

Coordinates timing_backfill, perf_backfill, asm_backfill, and binding_generator
to transform acquisition outputs into the four-layer data model.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def run_backfill(
    project_yaml: Path,
    arm_run_dir: Path,
    x86_run_dir: Path,
    output_dir: Path | None = None,
) -> int:
    """Run Step 6: data backfill.

    Parameters
    ----------
    project_yaml : Path
        Path to project.yaml.
    arm_run_dir : Path
        ARM platform run directory with acquisition outputs.
    x86_run_dir : Path
        x86 platform run directory with acquisition outputs.
    output_dir : Path or None
        Output directory for updated four-layer JSON. If None, updates in place.

    Returns
    -------
    int
        0 on success, 1 on failure.
    """
    from ..config import resolve_four_layer_root

    root = resolve_four_layer_root(project_yaml)
    target = output_dir or root

    if target != root:
        target.mkdir(parents=True, exist_ok=True)

    # Load existing four-layer JSON.
    layers = _load_layers(root)
    if layers is None:
        logger.error("Failed to load four-layer data from %s", root)
        return 1

    dataset, source, project_data = layers

    # Run backfill sub-modules.
    from .timing_backfill import backfill_timing
    from .perf_backfill import backfill_perf
    from .asm_backfill import backfill_asm
    from .binding_generator import generate_bindings

    timing_result = backfill_timing(arm_run_dir, x86_run_dir, dataset)
    logger.info("Timing backfill: %d cases updated", timing_result["cases_updated"])

    perf_result = backfill_perf(arm_run_dir, x86_run_dir, dataset)
    logger.info(
        "Perf backfill: %d functions, %d components, %d categories",
        perf_result.get("functions", 0),
        perf_result.get("components", 0),
        perf_result.get("categories", 0),
    )

    # Ensure required top-level arrays exist (schema requires patterns and rootCauses).
    dataset.setdefault("patterns", [])
    dataset.setdefault("rootCauses", [])

    asm_result = backfill_asm(arm_run_dir, x86_run_dir, source, dataset)
    logger.info(
        "ASM backfill: %d artifacts, %d functions (%d both, %d arm-only, %d x86-only)",
        asm_result.get("newArtifacts", 0),
        asm_result.get("uniqueSymbols", 0),
        asm_result.get("bothPlatforms", 0),
        asm_result.get("armOnly", 0),
        asm_result.get("x86Only", 0),
    )

    bindings = generate_bindings(dataset, source)
    project_data.setdefault("caseBindings", [])
    project_data.setdefault("functionBindings", [])

    # Merge bindings: update existing by ID, add new, prune stale.
    dataset_func_ids = {f["id"] for f in dataset.get("functions", []) if "id" in f}
    _merge_bindings(project_data, bindings, dataset_func_ids)

    # Write updated layers.
    _write_layers(target, dataset, source, project_data, layers)
    logger.info("Backfill complete. Output written to %s", target)

    return 0


def _load_layers(root: Path) -> tuple:
    """Load Dataset, Source, and Project JSON from the four-layer root.

    Returns empty templates for any layer that has no existing file.
    """
    dataset = _load_single_json(root / "datasets", ".dataset.json")
    source = _load_single_json(root / "sources", ".source.json")
    project_data = _load_single_json(root / "projects", ".project.json")

    return dataset, source, project_data


def _load_single_json(directory: Path, suffix: str) -> dict:
    """Load the single JSON file matching *suffix* in *directory*.

    Returns empty dict if directory or file doesn't exist.
    """
    if not directory.is_dir():
        return {}
    matches = list(directory.glob(f"*{suffix}"))
    if not matches:
        return {}
    path = matches[0]
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Failed to load %s: %s", path, e)
        return {}


def _merge_bindings(project_data: dict, new_bindings: dict, valid_func_ids: set[str]) -> None:
    """Merge generated bindings into project_data, updating by ID and pruning stale."""
    # Merge caseBindings
    existing_cases = {b["caseId"]: b for b in project_data.get("caseBindings", [])}
    for binding in new_bindings.get("caseBindings", []):
        existing_cases[binding["caseId"]] = binding
    project_data["caseBindings"] = list(existing_cases.values())

    # Merge functionBindings: add/update new, keep existing that are still valid
    existing_funcs = {b["functionId"]: b for b in project_data.get("functionBindings", [])}
    for binding in new_bindings.get("functionBindings", []):
        fid = binding["functionId"]
        if fid in existing_funcs:
            existing = existing_funcs[fid]
            if "armArtifactIds" in binding:
                existing["armArtifactIds"] = binding["armArtifactIds"]
            if "x86ArtifactIds" in binding:
                existing["x86ArtifactIds"] = binding["x86ArtifactIds"]
        else:
            existing_funcs[fid] = binding

    # Prune bindings that reference functions no longer in the dataset
    stale = [fid for fid in existing_funcs if fid not in valid_func_ids]
    for fid in stale:
        existing_funcs.pop(fid, None)
    if stale:
        logger.info("Pruned %d stale functionBindings (functions cut by top_n)", len(stale))

    project_data["functionBindings"] = list(existing_funcs.values())


def _write_layers(
    target: Path,
    dataset: dict,
    source: dict,
    project_data: dict,
    original_layers: tuple,
) -> None:
    """Write updated Dataset, Source, and Project JSON to target."""
    _original_dataset, _original_source, _original_project = original_layers

    # Determine original filenames to preserve naming.
    ds_files = list((target / "datasets").glob("*.dataset.json")) if (target / "datasets").is_dir() else []
    src_files = list((target / "sources").glob("*.source.json")) if (target / "sources").is_dir() else []
    proj_files = list((target / "projects").glob("*.project.json")) if (target / "projects").is_dir() else []

    # Write dataset.
    ds_dir = target / "datasets"
    ds_dir.mkdir(parents=True, exist_ok=True)
    ds_name = ds_files[0].name if ds_files else "dataset.dataset.json"
    (ds_dir / ds_name).write_text(
        json.dumps(dataset, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Write source.
    src_dir = target / "sources"
    src_dir.mkdir(parents=True, exist_ok=True)
    src_name = src_files[0].name if src_files else "source.source.json"
    (src_dir / src_name).write_text(
        json.dumps(source, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Write project.
    proj_dir = target / "projects"
    proj_dir.mkdir(parents=True, exist_ok=True)
    proj_name = proj_files[0].name if proj_files else "project.project.json"
    (proj_dir / proj_name).write_text(
        json.dumps(project_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
