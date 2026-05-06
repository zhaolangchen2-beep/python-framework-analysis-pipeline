"""
UDF auto-discovery.

Each udfs/*.py must export:

    UDF_SPEC = {
        "name":        str,        # Unique identifier for -q parameter
        "description": str,        # Human-readable description
        "setup":       Callable,   # (spark, args) -> (sql, tag, n_rows, expected_out)
    }

setup() responsibilities:
  1. Generate required DataFrame (data generator self-contained)
  2. Create and register UDF via spark.udf.register("process", ...)
  3. Register temp views
  4. Return (sql_string, display_tag, input_row_count, expected_output_count)
     expected_output_count = -1 means unknown
"""

import importlib
import pkgutil
from typing import Dict

_REGISTRY: Dict[str, dict] = {}


def discover(package_name: str = "udfs"):
    pkg = importlib.import_module(package_name)
    for _, mod_name, _ in pkgutil.iter_modules(pkg.__path__):
        module = importlib.import_module(f"{package_name}.{mod_name}")
        spec = getattr(module, "UDF_SPEC", None)
        if spec is not None:
            _REGISTRY[spec["name"]] = spec


def get(name: str) -> dict:
    if not _REGISTRY:
        discover()
    if name not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise KeyError(f"Unknown UDF '{name}'. Available: {available}")
    return _REGISTRY[name]


def list_all() -> list:
    if not _REGISTRY:
        discover()
    return sorted(_REGISTRY.keys())
