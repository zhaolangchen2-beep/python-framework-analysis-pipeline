"""PySpark ASM (assembly) collection and parsing.

Collects Java/JVM bytecode from Spark executor processes and maps to
framework taxonomy categories.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Any


def collect_pyspark_asm(
    run_dir: Path,
    platform: str,
    executor: Any | None = None,
) -> dict[str, Any]:
    """Collect assembly from PySpark worker containers.

    Parameters
    ----------
    run_dir : Path
        The run output directory (e.g., runs/2026-04-16-arm/)
    platform : str
        Platform identifier
    executor : SshExecutor or None
        SSH executor for remote container access.
        If None, assumes local execution.

    Returns
    -------
    dict with keys: files, raw_file, summary
    """
    asm_dir = run_dir / "asm"
    asm_dir.mkdir(parents=True, exist_ok=True)

    # Collect assembly from each Spark worker container
    container_asms = {}
    workers = ["spark-master", "spark-worker-1", "spark-worker-2"]

    for container in workers:
        asm_file = asm_dir / f"{container}-dump.s"

        if executor:
            # Remote execution: extract assembly from container
            asm_content = _extract_asm_from_container(executor, container)
        else:
            # Local execution: read from local process
            asm_content = ""

        if asm_content:
            asm_file.write_text(asm_content, encoding="utf-8")
            container_asms[container] = str(asm_file.relative_to(run_dir))

    # Parse and categorize assembly
    categories = _parse_asm_to_categories(container_asms, asm_dir)

    return {
        "files": list(container_asms.values()),
        "categories": categories,
        "containerCount": len(container_asms),
    }


def _extract_asm_from_container(executor: Any, container: str) -> str:
    """Extract assembly from a running Spark container.

    Uses 'perf report' output or objdump on JAR files.
    """
    try:
        # Try to extract from perf data
        result = executor.run(
            f"docker exec {container} bash -c "
            f"'perf report -i /tmp/perf-worker.data --stdio 2>&1 | head -100'",
            timeout=30,
        )
        if result.returncode == 0:
            return result.stdout or ""
    except Exception:
        pass

    # Fallback: get list of loaded classes/methods
    try:
        result = executor.run(
            f"docker exec {container} bash -c "
            f"'ps aux | grep java | grep -v grep'",
            timeout=10,
        )
        if result.returncode == 0 and result.stdout:
            return f"[Java process info]\n{result.stdout}"
    except Exception:
        pass

    return ""


def _parse_asm_to_categories(
    container_asms: dict[str, str],
    asm_dir: Path,
) -> dict[str, dict[str, Any]]:
    """Parse assembly content and map to framework categories.

    Maps symbols and instructions to PySpark framework taxonomy:
    - Interpreter: JVM bytecode interpretation
    - Memory: Allocation, GC markers
    - GC: Garbage collection operations
    - Object Model: Object creation, field access
    - Type Operations: Type checking, conversion
    - Calls/Dispatch: Method invocation, dispatch tables
    - Native Boundary: JNI calls, native methods
    - Kernel: System calls
    """
    categories = {
        "Interpreter": {"symbols": [], "count": 0},
        "Memory": {"symbols": [], "count": 0},
        "GC": {"symbols": [], "count": 0},
        "Object Model": {"symbols": [], "count": 0},
        "Type Operations": {"symbols": [], "count": 0},
        "Calls / Dispatch": {"symbols": [], "count": 0},
        "Native Boundary": {"symbols": [], "count": 0},
        "Kernel": {"symbols": [], "count": 0},
    }

    # Patterns to detect category-specific code
    patterns = {
        "Interpreter": [
            r"invoke", r"bytecode", r"execution_counter", r"dispatch",
            r"InterpreterRuntime", r"invocation_counter"
        ],
        "Memory": [
            r"malloc", r"alloc", r"realloc", r"free", r"memory",
            r"heap", r"arena", r"MemAllocationContext"
        ],
        "GC": [
            r"scavenge", r"mark_sweep", r"mark_compact", r"concurrent_mark",
            r"GC", r"CollectedHeap", r"SharedHeap", r"evacuation"
        ],
        "Object Model": [
            r"Klass", r"oopDesc", r"markOop", r"instanceKlass",
            r"java_lang_Object", r"field_offset", r"field"
        ],
        "Type Operations": [
            r"cast", r"type_check", r"class_check", r"instanceof",
            r"checkcast", r"Type::", r"is_klass_type"
        ],
        "Calls / Dispatch": [
            r"vtable", r"itable", r"call_site", r"method_entry",
            r"LinkResolver", r"VtableStubs", r"SharedRuntime::resolve"
        ],
        "Native Boundary": [
            r"jni", r"JNI", r"native", r"extern", r"call_native",
            r"entry_point", r"wrapper"
        ],
        "Kernel": [
            r"syscall", r"sys_", r"mmap", r"brk", r"madvise",
            r"futex", r"clock_gettime", r"epoll"
        ],
    }

    # Parse each assembly file
    for container, asm_file_path in container_asms.items():
        asm_file = asm_dir / (container + "-dump.s")
        if asm_file.exists():
            content = asm_file.read_text(encoding="utf-8", errors="ignore")

            # Extract symbols from perf/objdump output
            symbols = _extract_symbols(content)

            # Categorize each symbol
            for symbol in symbols:
                for category, pattern_list in patterns.items():
                    if any(re.search(p, symbol, re.IGNORECASE) for p in pattern_list):
                        categories[category]["symbols"].append(symbol)
                        categories[category]["count"] += 1
                        break

    return categories


def _extract_symbols(asm_content: str) -> list[str]:
    """Extract function symbols from assembly/perf output."""
    symbols = []

    # Match common symbol patterns
    patterns = [
        r"^[\da-f]+\s+<(.+?)>",  # objdump format: <symbol>
        r"(\w+::\w+)",  # C++ method names
        r"java\.\w+\.\w+",  # Java class methods
        r"[A-Za-z_]\w+\(.*\)",  # Function calls
    ]

    for line in asm_content.splitlines():
        for pattern in patterns:
            matches = re.findall(pattern, line)
            symbols.extend(matches)

    return list(set(symbols))  # Deduplicate


def categorize_asm_data(
    asm_categories: dict[str, dict[str, Any]],
    framework_taxonomy: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    """Map ASM categories to framework taxonomy.

    Parameters
    ----------
    asm_categories : dict
        Categories extracted from assembly
    framework_taxonomy : dict
        Framework taxonomy from pyspark.framework.json

    Returns
    -------
    dict mapping framework categories to ASM findings
    """
    result = {}

    # Map ASM categories to framework L1 categories
    category_mapping = {
        "Interpreter": "Interpreter",
        "Memory": "Memory",
        "GC": "GC",
        "Object Model": "Object Model",
        "Type Operations": "Type Operations",
        "Calls / Dispatch": "Calls / Dispatch",
        "Native Boundary": "Native Boundary",
        "Kernel": "Kernel",
    }

    for asm_cat, framework_cat in category_mapping.items():
        if asm_cat in asm_categories:
            result[framework_cat] = {
                "asm_count": asm_categories[asm_cat]["count"],
                "symbols": asm_categories[asm_cat]["symbols"][:10],  # Top 10
                "source": "pyspark_asm_collection",
            }

    return result
