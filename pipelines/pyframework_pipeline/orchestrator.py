"""Step-by-step pipeline orchestrator with resumability.

Chains steps 3 through 7 for all configured platforms, tracking state
in pipeline-run.json for resume-from-failure.
"""

from __future__ import annotations

import json
import logging
import shlex
import textwrap
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Step registry
# ---------------------------------------------------------------------------

STEP_DEFS: list[dict[str, str]] = [
    {"step": "3",      "name": "environment deploy"},
    {"step": "4",      "name": "workload deploy"},
    {"step": "5a",     "name": "benchmark run"},
    {"step": "5b.1",   "name": "collect perf.data"},
    {"step": "5b.2",   "name": "run perf-kits"},
    {"step": "5b.2b",  "name": "extract CPython source"},
    {"step": "5b.3",   "name": "collect objdump ASM"},
    {"step": "5c",     "name": "acquire all"},
    {"step": "6",      "name": "backfill run"},
    {"step": "6b",     "name": "platform compare"},
    {"step": "7",      "name": "bridge publish"},
]

# Steps that run per-platform (need --platform).
PER_PLATFORM_STEPS = {"3", "4", "5a", "5b.1", "5b.2", "5b.2b", "5b.3"}

# Steps that run once after all platforms.
GLOBAL_STEPS = {"5c", "6", "6b", "7"}

# Mapping from old "5b" to its sub-steps (for resume-from backward compat).
_STEP_ALIASES: dict[str, list[str]] = {
    "5b": ["5b.1", "5b.2", "5b.2b", "5b.3"],
}


def _resolve_step_alias(step: str) -> str:
    """Resolve step aliases to their first sub-step (e.g. '5b' -> '5b.1')."""
    if step in _STEP_ALIASES:
        return _STEP_ALIASES[step][0]
    return step


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")


class PipelineRunState:
    """Tracks pipeline run state in pipeline-run.json."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.data: dict[str, Any] = {}
        if path.exists():
            try:
                self.data = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                self.data = {}

    def init(self, project_id: str, platforms: list[str]) -> None:
        if not self.data or not self.data.get("steps"):
            self.data = {
                "runId": _run_id(),
                "projectId": project_id,
                "platforms": platforms,
                "steps": [],
            }

    def is_completed(self, step: str, platform: str | None = None) -> bool:
        for s in self.data.get("steps", []):
            if s["step"] == step and s.get("platform") == platform:
                return s["status"] == "completed"
        return False

    def mark_running(self, step: str, platform: str | None = None) -> None:
        self.data.setdefault("steps", []).append({
            "step": step,
            "name": next(
                (d["name"] for d in STEP_DEFS if d["step"] == step), step
            ),
            "platform": platform,
            "status": "running",
            "startedAt": _now_iso(),
        })
        self._save()

    def mark_completed(self, step: str, platform: str | None = None) -> None:
        for s in reversed(self.data.get("steps", [])):
            if s["step"] == step and s.get("platform") == platform and s["status"] == "running":
                s["status"] = "completed"
                s["completedAt"] = _now_iso()
                break
        self._save()

    def mark_failed(
        self, step: str, platform: str | None = None, error: str = "",
    ) -> None:
        for s in reversed(self.data.get("steps", [])):
            if s["step"] == step and s.get("platform") == platform and s["status"] == "running":
                s["status"] = "failed"
                s["error"] = error
                s["completedAt"] = _now_iso()
                break
        self._save()

    def clear_from(self, step: str) -> None:
        """Remove state entries for *step* and all subsequent steps."""
        step_ids = [d["step"] for d in STEP_DEFS]
        # Resolve aliases (e.g. "5b" -> "5b.1").
        resolved = _resolve_step_alias(step)
        if resolved not in step_ids:
            return
        cutoff = step_ids.index(resolved)
        clear_ids = set(step_ids[cutoff:])
        self.data["steps"] = [
            s for s in self.data.get("steps", [])
            if s["step"] not in clear_ids
        ]
        self._save()

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )


# ---------------------------------------------------------------------------
# Orchestrator helpers
# ---------------------------------------------------------------------------

def _init_submodules(repo_root: Path) -> None:
    """Initialise git submodules if vendor directories are missing."""
    gitmodules = repo_root / ".gitmodules"
    if not gitmodules.exists():
        return
    import subprocess
    try:
        subprocess.run(
            ["git", "submodule", "update", "--init"],
            cwd=str(repo_root),
            check=True,
            capture_output=True,
            timeout=120,
        )
        logger.info("Initialised git submodules in %s", repo_root)
    except (subprocess.CalledProcessError, FileNotFoundError, OSError) as exc:
        logger.warning("Failed to init git submodules: %s", exc)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(
    project_path: Path,
    run_dir: Path,
    *,
    resume_from: str | None = None,
    stop_before: str | None = None,
    force: bool = False,
    yes: bool = False,
) -> int:
    """Execute the full pipeline from step 3 to 7.

    Returns 0 on success, 1 on failure.
    """
    from .config import (
        get_run_config,
        get_workload_config,
        get_bridge_config,
        load_project_config,
    )

    config = load_project_config(project_path)
    project_id = config.get("id", "unknown")
    run_config = get_run_config(project_path)
    platforms = run_config.get("platforms", [])

    state_path = run_dir / "pipeline-run.json"
    state = PipelineRunState(state_path)

    if force:
        state.data = {}

    state.init(project_id, platforms)

    # Determine start point.
    step_ids = [d["step"] for d in STEP_DEFS]
    start_idx = 0
    is_resume = bool(resume_from)
    if resume_from:
        resolved = _resolve_step_alias(resume_from)
        if resolved in step_ids:
            start_idx = step_ids.index(resolved)
            state.clear_from(resume_from)
            logger.info("Resuming from step %s — cleared downstream state", resume_from)
        else:
            logger.error("Unknown step: %s. Valid: %s", resume_from, step_ids)
            return 1

    stop_idx = len(STEP_DEFS)
    if stop_before:
        resolved_stop = _resolve_step_alias(stop_before)
        if resolved_stop in step_ids:
            stop_idx = step_ids.index(resolved_stop)
        else:
            logger.error("Unknown step: %s. Valid: %s", stop_before, step_ids)
            return 1

    for idx in range(start_idx, stop_idx):
        step_def = STEP_DEFS[idx]
        step_id = step_def["step"]
        step_name = step_def["name"]

        if step_id in PER_PLATFORM_STEPS:
            # Run for each platform.
            for plat in platforms:
                if state.is_completed(step_id, plat):
                    logger.info("[S%s/%s] Already completed, skipping", step_id, plat)
                    continue

                state.mark_running(step_id, plat)
                logger.info("[S%s/%s] >> ENTER %s", step_id, plat, step_name)

                try:
                    _execute_step(
                        step_id, project_path, run_dir, plat,
                        yes=yes, force=is_resume,
                    )
                    state.mark_completed(step_id, plat)
                    logger.info("[S%s/%s] << EXIT %s (ok)", step_id, plat, step_name)
                except StepError as exc:
                    state.mark_failed(step_id, plat, str(exc))
                    _print_resume_hint(step_id, plat, project_path, error=str(exc))
                    return 1

        elif step_id in GLOBAL_STEPS:
            if state.is_completed(step_id):
                logger.info("[S%s] Already completed, skipping", step_id)
                continue

            state.mark_running(step_id)
            logger.info("[S%s] >> ENTER %s", step_id, step_name)

            try:
                _execute_step(
                    step_id, project_path, run_dir, None,
                    yes=yes, force=is_resume,
                )
                state.mark_completed(step_id)
                logger.info("[S%s] << EXIT %s (ok)", step_id, step_name)
            except StepError as exc:
                state.mark_failed(step_id, error=str(exc))
                _print_resume_hint(step_id, None, project_path, error=str(exc))
                return 1

    logger.info("Pipeline completed successfully")
    return 0


class StepError(Exception):
    """Raised when a pipeline step fails."""


def _execute_step(
    step_id: str,
    project_path: Path,
    run_dir: Path,
    platform: str | None,
    *,
    yes: bool = False,
    force: bool = False,
) -> None:
    """Execute a single pipeline step."""
    if step_id == "3":
        from .environment.deploy import deploy_plan
        plan_path = run_dir / platform / "environment-plan.json"
        # On resume/force, clear old plan + records so deploy re-generates
        # and re-executes everything (including the --privileged check).
        if force:
            for old_file in [
                plan_path,
                run_dir / platform / "deploy-record.json",
                run_dir / platform / "environment-record.json",
                project_path.parent / "output" / platform / "deploy-record.json",
            ]:
                if old_file.exists():
                    old_file.unlink()
                    logger.info("Cleared old file: %s", old_file)
        result = deploy_plan(project_path, platform, plan_path, yes=yes)
        # Save record.
        record = result.get("record", {})
        if record:
            record_dir = run_dir / platform
            record_dir.mkdir(parents=True, exist_ok=True)
            (record_dir / "environment-record.json").write_text(
                json.dumps(record, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        if result.get("status") == "failed":
            fs = result.get("failedStep", {})
            cmd = fs.get("command", "")
            stderr = fs.get("stderr", "")
            desc = fs.get("description", "")
            exit_code = fs.get("exitCode", "")
            parts = [f"environment deploy failed: {result.get('failed', 0)} step(s) failed"]
            if desc:
                parts.append(f"  Step: {fs.get('id', '')} — {desc}")
            if cmd:
                parts.append(f"  Command: {cmd}")
            if exit_code:
                parts.append(f"  Exit code: {exit_code}")
            if stderr:
                parts.append(f"  stderr: {stderr[:500]}")
            raise StepError("\n".join(parts))

    elif step_id == "4":
        _run_workload_deploy(project_path, run_dir, platform, yes=yes)

    elif step_id == "5a":
        _run_benchmark(project_path, run_dir, platform, force=force)

    elif step_id == "5b.1":
        _run_collect_substep(project_path, run_dir, platform, "5b.1")
    elif step_id == "5b.2":
        _run_collect_substep(project_path, run_dir, platform, "5b.2")
    elif step_id == "5b.2b":
        _run_collect_substep(project_path, run_dir, platform, "5b.2b")
    elif step_id == "5b.3":
        _run_collect_substep(project_path, run_dir, platform, "5b.3")

    elif step_id == "5c":
        _run_acquire_all(project_path, run_dir, force=force)

    elif step_id == "6":
        _run_backfill(project_path, run_dir, force=force)

    elif step_id == "6b":
        _run_compare(project_path, run_dir)

    elif step_id == "7":
        _run_bridge_publish(project_path)

    else:
        raise StepError(f"Unknown step: {step_id}")


# ---------------------------------------------------------------------------
# Step implementations
# ---------------------------------------------------------------------------

def _run_workload_deploy(
    project_path: Path, run_dir: Path, platform: str, *, yes: bool = False,
) -> None:
    from .config import get_workload_config, load_environment_config
    from .framework_config import get_framework_config
    from .remote import build_executor, get_platform_host_ref

    workload = get_workload_config(project_path)
    local_dir = project_path.parent / workload["localDir"]

    if not local_dir.exists():
        raise StepError(f"Workload directory not found: {local_dir}")

    env_config = load_environment_config(project_path)
    host_ref = get_platform_host_ref(env_config, platform)
    executor = build_executor(host_ref, env_config)
    fw_config = get_framework_config(project_path)

    if fw_config.framework_id == "pyspark" and fw_config.lang_env_root:
        test_sh = f"{fw_config.lang_env_root}/pyspark/test.sh"
        lang_env_dir = f"{fw_config.lang_env_root}/pyspark"

        check_root = executor.run(f"test -d {shlex.quote(fw_config.lang_env_root)}", timeout=15)
        if check_root.returncode != 0:
            raise StepError(f"langEnvRoot not found on remote host: {fw_config.lang_env_root}")

        check_dir = executor.run(f"test -d {shlex.quote(lang_env_dir)}", timeout=15)
        if check_dir.returncode != 0:
            raise StepError(f"PySpark lang_env directory not found on remote host: {lang_env_dir}")

        check_script = executor.run(f"test -f {shlex.quote(test_sh)}", timeout=15)
        if check_script.returncode != 0:
            raise StepError(f"PySpark test.sh not found on remote host: {test_sh}")

        logger.info("Verified remote lang_env PySpark benchmark at %s", lang_env_dir)
        return

    # Upload workload to remote host staging.
    remote_dir = "/tmp/pyframework-workload"
    # Remove stale remote directory so scp -r doesn't create a nested
    # sub-directory when the target already exists.
    executor.run(f"rm -rf {remote_dir}", timeout=15)
    logger.info("Uploading %s to %s:%s", local_dir, host_ref, remote_dir)
    ok = executor.push_dir(local_dir, remote_dir)
    if not ok:
        raise StepError(f"Failed to upload workload to {host_ref}:\n  Local: {local_dir}\n  Remote: {remote_dir}")

    # Distribute to containers via docker cp.
    # docker cp writes as root; chown to the container user so subsequent
    # operations (build.sh, perf-kits, etc.) can access the files.
    master_container = fw_config.master_container
    master_workdir = fw_config.master_workdir

    master_result = executor.run(f"docker cp {remote_dir}/. {master_container}:{master_workdir}")
    if master_result.returncode != 0:
        raise StepError(
            f"Failed to copy workload to {master_container} (exit {master_result.returncode}):\n"
            f"  Command: docker cp {remote_dir}/. {master_container}:{master_workdir}\n"
            f"  stdout: {master_result.stdout[:500]}\n"
            f"  stderr: {master_result.stderr[:500]}"
        )

    # Determine chown user based on framework
    chown_user = "root:root" if fw_config.framework_id == "pyspark" else "flink:flink"
    executor.run(
        f"docker exec -u root {master_container} chown -R {chown_user} {master_workdir}",
        timeout=15,
    )

    # Distribute to worker containers
    worker_containers = fw_config.get_worker_containers(count=2)
    for worker_container in worker_containers:
        worker_workdir = fw_config.worker_workdir
        worker_result = executor.run(
            f"docker cp {remote_dir}/. {worker_container}:{worker_workdir}"
        )
        if worker_result.returncode != 0:
            raise StepError(
                f"Failed to copy workload to {worker_container} (exit {worker_result.returncode}):\n"
                f"  Command: docker cp {remote_dir}/. {worker_container}:{worker_workdir}\n"
                f"  stdout: {worker_result.stdout[:500]}\n"
                f"  stderr: {worker_result.stderr[:500]}"
            )
        executor.run(
            f"docker exec -u root {worker_container} chown -R {chown_user} {worker_workdir}",
            timeout=15,
        )

    # Build JAR inside container if missing (only for PyFlink).
    if fw_config.framework_id == "pyflink":
        build_sh = local_dir / "java-udf" / "build.sh"
        jar_name = "FlinkDemo-1.0-SNAPSHOT.jar"
        jar_local = local_dir / "java-udf" / jar_name
        if build_sh.exists() and not jar_local.exists():
            logger.info("JAR not found locally, building inside container...")
            result = executor.run(
                f"docker exec {master_container} bash -c "
                f"'cd {master_workdir}/java-udf && bash build.sh'",
                timeout=120,
                stream=True,
            )
            if result.returncode != 0:
                raise StepError(
                    f"Container build failed (exit {result.returncode}):\n"
                    f"  Command: docker exec {master_container} bash -c 'cd {master_workdir}/java-udf && bash build.sh'\n"
                    f"  output: {result.stdout[-2000:]}"
                )


def _run_benchmark_pyspark(
    project_path: Path, run_dir: Path, platform: str,
    *, force: bool = False,
) -> None:
    """Run PySpark benchmark using the remote lang_env test.sh baseline."""
    from .config import get_workload_config, load_environment_config
    from .framework_config import get_framework_config
    from .remote import build_executor, get_platform_host_ref

    workload = get_workload_config(project_path)
    queries = workload.get("queries", [])
    rows = workload.get("rows", 10_000_000)
    env_config = load_environment_config(project_path)
    host_ref = get_platform_host_ref(env_config, platform)
    executor = build_executor(host_ref, env_config)
    fw_config = get_framework_config(project_path)

    platform_run_dir = run_dir / platform
    platform_run_dir.mkdir(parents=True, exist_ok=True)
    timing_path = platform_run_dir / "timing" / "timing-normalized.json"

    if force:
        removed = []
        for old in [
            platform_run_dir / "timing" / "timing-normalized.json",
            platform_run_dir / "perf" / "data" / f"perf-{platform}.data",
        ]:
            if old.exists():
                old.unlink()
                removed.append(old.relative_to(run_dir))
        if removed:
            logger.info("[5a] Force: removed old artifacts: %s", removed)

    if timing_path.exists() and timing_path.stat().st_size > 0:
        logger.info("[5a] timing-normalized.json exists, skipping benchmark on %s", platform)
        return

    if not queries:
        raise StepError("No queries configured")
    if not fw_config.lang_env_root:
        raise StepError("PySpark langEnvRoot is not configured in environment.yaml software.langEnvRoot")

    lang_env_pyspark_dir = f"{fw_config.lang_env_root}/pyspark"
    test_sh = f"{lang_env_pyspark_dir}/test.sh"
    test_sh_non_tty = "/tmp/pyframework-pyspark-test-non-tty.sh"
    master_container = fw_config.master_container
    worker_containers = fw_config.get_worker_containers(count=_parse_tm_count(env_config))
    perf_data_path = fw_config.get_perf_data_path()

    logger.info("[5a] Preparing non-TTY test.sh copy and perf wrapper on %s...", platform)
    executor.run(
        f"cp {shlex.quote(test_sh)} {shlex.quote(test_sh_non_tty)} && "
        f"sed -i 's/docker exec -it /docker exec -i /g' {shlex.quote(test_sh_non_tty)} && "
        f"chmod +x {shlex.quote(test_sh_non_tty)}",
        timeout=30,
    )
    perf_binary = _find_container_perf(executor, container=master_container)
    wrapper_path = _deploy_lang_env_perf_wrapper(
        executor,
        master_container,
        worker_containers,
        perf_binary,
        perf_data_path,
    )

    wall_clock_times: dict[str, dict] = {}

    for query in queries:
        logger.info("[5a] Running query %s on %s via lang_env test.sh...", query, platform)
        cmd = (
            f"cd {shlex.quote(lang_env_pyspark_dir)} && "
            f"bash {shlex.quote(test_sh_non_tty)} -q {shlex.quote(query)} -c {rows} -v "
            f"--spark-args {shlex.quote(f'--conf spark.executorEnv.PYSPARK_PYTHON={wrapper_path}') }"
        )
        result = executor.run(cmd, timeout=600, stream=True)
        if result.returncode != 0:
            raise StepError(
                f"Benchmark {query} failed (exit {result.returncode}):\n"
                f"  Command: {cmd}\n"
                f"  output: {result.stdout[-2000:]}"
            )

        parsed = _parse_lang_env_benchmark_output(result.stdout, query, rows)
        wall_clock_times[query] = parsed
        logger.info(
            "  %s: wall-clock %.3fs, throughput %s rows/s, py=%d ns, fw=%d ns",
            query,
            parsed["wallClockSeconds"],
            parsed.get("throughputRowsPerSec", "-"),
            parsed.get("totalPyDurationNs", 0),
            parsed.get("totalFrameworkOverheadNs", 0),
        )

        master_logs = executor.docker_logs(master_container, tail=200)
        (platform_run_dir / "tm-stdout-master.log").write_text(master_logs, encoding="utf-8")
        for i, worker_container in enumerate(worker_containers, 1):
            logs = executor.docker_logs(worker_container, tail=50)
            (platform_run_dir / f"tm-stdout-worker-{i}.log").write_text(logs, encoding="utf-8")

    _merge_wall_clock_times(platform_run_dir, platform, wall_clock_times)

    perf_check = None
    perf_container = None
    for c in [master_container] + worker_containers:
        check = executor.run(
            f"docker exec {c} bash -c 'ls -lh {perf_data_path} 2>&1'",
            timeout=15,
        )
        if check.returncode == 0 and "No such file" not in check.stdout:
            perf_check = check
            perf_container = c
            break
    if perf_check is None:
        logger.warning(
            "[5a] perf.data was not found in any container after running %d queries. "
            "This may indicate that the test.sh spark-args wrapper was not applied.",
            len(queries)
        )
    else:
        logger.info("[5a] perf.data verified in %s: %s", perf_container, perf_check.stdout.strip())

    logger.info("[5b] Collecting ASM from PySpark workers...")
    _collect_pyspark_asm(executor, platform_run_dir, fw_config, _parse_tm_count(env_config))


def _run_benchmark(
    project_path: Path, run_dir: Path, platform: str,
    *, force: bool = False,
) -> None:
    from .framework_config import get_framework_config
    from .config import get_workload_config, load_environment_config
    from .remote import build_executor, get_platform_host_ref

    fw_config = get_framework_config(project_path)

    # Dispatch to framework-specific implementation
    if fw_config.framework_id == "pyspark":
        _run_benchmark_pyspark(project_path, run_dir, platform, force=force)
        return

    # Original PyFlink implementation
    import time

    from .config import get_workload_config, load_environment_config
    from .remote import build_executor, get_platform_host_ref

    workload = get_workload_config(project_path)
    queries = workload.get("queries", [])
    rows = workload.get("rows", 10_000_000)
    env_config = load_environment_config(project_path)
    host_ref = get_platform_host_ref(env_config, platform)
    executor = build_executor(host_ref, env_config)
    python_bin = _find_container_python(executor, env_config)

    # Ensure Java UDF JAR exists inside JM container.
    _ensure_jar(executor)

    platform_run_dir = run_dir / platform
    platform_run_dir.mkdir(parents=True, exist_ok=True)
    timing_path = platform_run_dir / "timing" / "timing-normalized.json"

    tm_count = _parse_tm_count(env_config)

    # When resuming, re-deploy workload so the latest benchmark_runner.py
    # is in the container.
    if force:
        _run_workload_deploy(project_path, run_dir, platform)
        removed = []
        for old in [
            platform_run_dir / "timing" / "timing-normalized.json",
            platform_run_dir / "timing" / "timing-raw.json",
            platform_run_dir / "perf" / "data" / f"perf-{platform}.data",
        ]:
            if old.exists():
                old.unlink()
                removed.append(old.relative_to(run_dir))
        if removed:
            logger.info("[5a] Force: removed old artifacts: %s", removed)

    # --- Sub-step: run benchmark with perf (artifact: timing-normalized.json) ---
    if timing_path.exists() and timing_path.stat().st_size > 0:
        logger.info("[5a] timing-normalized.json exists, skipping benchmark on %s", platform)
    else:
        if not queries:
            raise StepError("No queries configured")

        # Set up perf recording and run benchmarks.
        # python.executable is set to a perf wrapper script that wraps
        # every Python worker invocation with perf record.  Java-side probes
        # (e.g. getPythonUdfRunnerScript) also go through the wrapper, but
        # perf works correctly for those short-lived commands too since the
        # containers have --privileged and perf_event_paranoid=0.
        logger.info("[5a] Deploying perf wrapper on %s...", platform)
        _ensure_container_perf(executor, tm_count, include_jm=True)
        _ensure_pyflink_runner(executor, python_bin, tm_count)
        perf_binary = _find_container_perf(executor)
        _deploy_perf_wrapper(executor, tm_count, python_bin, perf_binary, include_jm=True)

        # Run queries with --python-executable pointing to the perf wrapper.
        import json as _json

        wall_clock_times: dict[str, dict] = {}

        for query in queries:
            logger.info("[5a] Running query %s on %s...", query, platform)
            result = executor.run(
                f"docker exec flink-jm {python_bin} "
                f"/opt/flink/usrlib/benchmark_runner.py "
                f"--query {query} --rows {rows} "
                f"--python-executable /tmp/_perf_python_wrapper.sh",
                timeout=300,
                stream=True,
            )
            if result.returncode != 0:
                raise StepError(
                    f"Benchmark {query} failed (exit {result.returncode}):\n"
                    f"  Command: docker exec flink-jm {python_bin} benchmark_runner.py --query {query} --rows {rows}\n"
                    f"  output: {result.stdout[-2000:]}"
                )

            wc = _parse_benchmark_result(result.stdout, query)
            if wc:
                wall_clock_times[query] = wc
                logger.info("  %s: wall-clock %.3fs, throughput %s rows/s",
                            query, wc["wallClockSeconds"], wc.get("throughputRowsPerSec", "-"))

            # Collect JM logs (Python workers run in JM for local mini-cluster mode).
            jm_logs = executor.docker_logs("flink-jm", tail=200)
            (platform_run_dir / "tm-stdout-jm.log").write_text(jm_logs, encoding="utf-8")
            for i in range(1, tm_count + 1):
                logs = executor.docker_logs(f"flink-tm{i}", tail=50)
                (platform_run_dir / f"tm-stdout-tm{i}.log").write_text(logs, encoding="utf-8")

            _collect_operator_timing(executor, tm_count, query, wall_clock_times)

        _merge_wall_clock_times(platform_run_dir, platform, wall_clock_times)

        # Verify perf.data was created (check JM first for local mode, then TMs).
        perf_check = None
        perf_container = None
        for c in ["flink-jm"] + [f"flink-tm{i}" for i in range(1, tm_count + 1)]:
            check = executor.run(
                f"docker exec {c} bash -c "
                "'ls -lh /tmp/perf-udf.data 2>&1'",
                timeout=15,
            )
            if check.returncode == 0 and "No such file" not in check.stdout:
                perf_check = check
                perf_container = c
                break
        if perf_check is None:
            raise StepError(
                f"[5a] perf.data was not found in any container (JM + TMs) after "
                f"running {len(queries)} queries.  Last check: {check.stdout.strip()}"
            )
        logger.info("[5a] perf.data verified in %s: %s", perf_container, perf_check.stdout.strip())


def _collect_operator_timing(
    executor: "SshExecutor",
    tm_count: int,
    query_id: str,
    wall_clock_times: dict[str, dict],
) -> None:
    """Collect operator/framework timing from PostUDF's [BENCHMARK_SUMMARY].

    PostUDF (CalcOverhead) accumulates per-record py_duration and framework
    overhead in AtomicLongs, then prints a JSON summary to stdout on close().

    System.out goes to the JVM process stdout, which Docker captures as
    container logs (not Flink's log4j files). We grep the actual TM log
    files (flink--taskexecutor-*.log) as a first attempt, then fall back
    to docker logs --tail if needed.
    """
    import json as _json

    # Check JM first (local mini-cluster: Python workers in JM),
    # then TMs (cluster mode: Python workers in TMs).
    containers = ["flink-jm"] + [f"flink-tm{i}" for i in range(1, tm_count + 1)]
    for c in containers:
        label = "JM" if c == "flink-jm" else c.replace("flink-", "").upper()
        # No --tail limit: BENCHMARK_SUMMARY may be thousands of lines back
        # after multiple queries with verbose Flink output.
        result = executor.run(
            f"docker logs {c} 2>&1 | grep BENCHMARK_SUMMARY | tail -1",
            timeout=120,
        )
        if result.returncode == 0 and "BENCHMARK_SUMMARY" in (result.stdout or ""):
            try:
                line = result.stdout.strip()
                json_str = line.split("BENCHMARK_SUMMARY] ", 1)[1].strip()
                stats = _json.loads(json_str)
                wc = wall_clock_times.get(query_id, {})
                wc["recordCount"] = wc.get("recordCount", 0) + stats.get("recordCount", 0)
                wc["totalPyDurationNs"] = wc.get("totalPyDurationNs", 0) + stats.get("totalPyDurationNs", 0)
                wc["totalFrameworkOverheadNs"] = (
                    wc.get("totalFrameworkOverheadNs", 0)
                    + stats.get("totalFrameworkOverheadNs", 0)
                )
                wall_clock_times[query_id] = wc
                logger.info("  %s %s: %d records, py=%d ns, fw=%d ns",
                            query_id, label, stats.get("recordCount", 0),
                            stats.get("totalPyDurationNs", 0),
                            stats.get("totalFrameworkOverheadNs", 0))
                break
            except (_json.JSONDecodeError, IndexError):
                pass


def _ensure_container_perf(
    executor: "SshExecutor",
    tm_count: int,
    include_jm: bool = False,
    *,
    master_container: str = "flink-jm",
    worker_containers: list[str] | None = None,
    perf_data_path: str = "/tmp/perf-udf.data",
) -> str:
    """Verify perf is available in containers (installed during image build)."""
    containers = []
    if include_jm:
        containers.append(master_container)
    containers.extend(worker_containers or [f"flink-tm{i}" for i in range(1, tm_count + 1)])

    for c in containers:
        check = executor.run(
            f"docker exec {c} bash -lc "
            "'command -v perf || ls /usr/lib/linux-tools-*/perf 2>/dev/null | sort -V | tail -1'",
            timeout=30,
        )
        if check.returncode != 0 or not check.stdout.strip():
            raise StepError(
                f"perf not found in {c}. linux-tools/perf must be installed in the target container image.\n"
                f"  stdout: {check.stdout[:500]}\n"
                f"  stderr: {check.stderr[:500]}"
            )
    probe_container = (worker_containers or [f"flink-tm{i}" for i in range(1, tm_count + 1)])[0]
    return executor.run(
        f"docker exec {probe_container} bash -lc "
        "'command -v perf || ls /usr/lib/linux-tools-*/perf 2>/dev/null | sort -V | tail -1'",
        timeout=30,
    ).stdout.strip()


def _ensure_pyflink_runner(
    executor: "SshExecutor",
    python_bin: str,
    tm_count: int,
) -> None:
    """Ensure pyflink-udf-runner.sh exists in all containers.

    May be missing when apache-flink is installed with --no-build-isolation.
    Checks JM and all TMs — the worker may run on any of them.
    """
    # Resolve runner path once (same on all containers from same image).
    check = executor.run(
        f"docker exec flink-jm {python_bin} -c "
        "'import pyflink, os; print(os.path.join(os.path.dirname(pyflink.__file__), \"bin\", \"pyflink-udf-runner.sh\"))'",
        timeout=15,
    )
    if check.returncode != 0:
        logger.warning("[5a] Could not locate pyflink package dir: %s", check.stderr.strip())
        return

    runner_path = check.stdout.strip()
    containers = ["flink-jm"] + [f"flink-tm{i}" for i in range(1, tm_count + 1)]

    for c in containers:
        exists = executor.run(f"docker exec {c} test -f {runner_path}", timeout=10)
        if exists.returncode == 0:
            logger.info("[5a] pyflink-udf-runner.sh exists on %s", c)
            continue

        logger.info("[5a] pyflink-udf-runner.sh missing on %s, creating", c)
        import base64

        runner_script = (
            '#!/usr/bin/env bash\n'
            'python=${python:-python}\n'
            'if [ -n "$_PYTHON_WORKING_DIR" ]; then\n'
            '    cd "$_PYTHON_WORKING_DIR"\n'
            '    if [[ "$python" == ${_PYTHON_WORKING_DIR}* ]]; then\n'
            '        chmod +x "$python"\n'
            '    fi\n'
            'fi\n'
            'log="${BOOT_LOG_DIR}/flink-python-udf-boot.log"\n'
            '${python} -m pyflink.fn_execution.beam.beam_boot "$@" 2>&1 | tee ${log}\n'
        )
        encoded = base64.b64encode(runner_script.encode()).decode()
        bin_dir = runner_path.rsplit("/", 1)[0]
        executor.run(
            f"docker exec {c} bash -c "
            f"'mkdir -p {bin_dir} && "
            f"echo {encoded} | base64 -d > {runner_path} && "
            f"chmod +x {runner_path}'",
            timeout=15,
        )
        logger.info("[5a] Created pyflink-udf-runner.sh on %s", c)


def _deploy_lang_env_perf_wrapper(
    executor: "SshExecutor",
    master_container: str,
    worker_containers: list[str],
    perf_binary: str,
    perf_data_path: str,
) -> str:
    """Deploy a python wrapper into PySpark containers for test.sh --spark-args injection."""
    import base64

    wrapper_path = "/tmp/_perf_python_wrapper.sh"
    script = (
        "#!/bin/bash\n"
        "export OPENBLAS_NUM_THREADS=1\n"
        "export OMP_NUM_THREADS=1\n"
        "export MKL_NUM_THREADS=1\n"
        "export NUMEXPR_NUM_THREADS=1\n"
        f"exec {perf_binary} record -F 999 -g -e task-clock -o {perf_data_path} -- python3 \"$@\" 2>/dev/null\n"
    )
    encoded = base64.b64encode(script.encode()).decode()

    for container in [master_container] + worker_containers:
        executor.run(f"docker exec {container} rm -f {perf_data_path}", timeout=30)
        executor.run(
            f"docker exec {container} bash -c "
            f"'echo {encoded} | base64 -d > {wrapper_path} && chmod +x {wrapper_path}'",
            timeout=30,
        )

    return wrapper_path


def _parse_lang_env_benchmark_output(output: str, query: str, rows: int) -> dict:
    import re as _re

    line_pattern = _re.compile(
        r"(?P<query>\S+)\s*\|\s*UDF=\s*(?P<udf>\d+)\s*ns\s*\|\s*"
        r"Overhead=\s*(?P<overhead>\d+)\s*ns\s*\|\s*"
        r"Throughput=\s*(?P<throughput>\d+)\s*rows/s\s*\|\s*"
        r"Duration=\s*(?P<duration>[0-9.]+)\s*s"
    )

    for raw_line in output.splitlines():
        line = raw_line.strip()
        match = line_pattern.search(line)
        if match and match.group("query") == query:
            per_udf = int(match.group("udf"))
            per_overhead = int(match.group("overhead"))
            duration = float(match.group("duration"))
            throughput = int(match.group("throughput"))
            return {
                "wallClockSeconds": duration,
                "throughputRowsPerSec": throughput,
                "totalPyDurationNs": per_udf * rows,
                "totalFrameworkOverheadNs": per_overhead * rows,
            }

    raise StepError(f"Could not parse lang_env benchmark output for query {query}")


def _parse_benchmark_result(stdout: str, query_id: str) -> dict | None:
    """Parse BENCHMARK_RESULT JSON from benchmark_runner.py stdout."""
    import json as _json
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if '"BENCHMARK_RESULT"' in line:
            try:
                data = _json.loads(line)
                if data.get("type") == "BENCHMARK_RESULT":
                    return data
            except _json.JSONDecodeError:
                continue
    return None


def _merge_wall_clock_times(
    platform_run_dir: Path,
    platform: str,
    wall_clock_times: dict[str, dict],
) -> None:
    """Merge wall-clock timing into timing/timing-normalized.json."""
    import json as _json

    timing_path = platform_run_dir / "timing" / "timing-normalized.json"
    timing_path.parent.mkdir(parents=True, exist_ok=True)

    if timing_path.exists():
        data = _json.loads(timing_path.read_text(encoding="utf-8"))
    else:
        data = {"schemaVersion": 1, "platform": platform, "cases": []}

    cases_by_id = {c["caseId"]: c for c in data.get("cases", [])}

    for query_id, wc in wall_clock_times.items():
        case = cases_by_id.get(query_id)
        if case is None:
            case = {"caseId": query_id, "metrics": {}}
            data.setdefault("cases", []).append(case)
            cases_by_id[query_id] = case

        wall_clock_ns = int(wc["wallClockSeconds"] * 1e9)
        case["metrics"]["wallClockTime"] = {"wall_clock_ns": wall_clock_ns}
        case["metrics"]["tmE2eTime"] = {"wall_clock_ns": wall_clock_ns}

        py_ns = wc.get("totalPyDurationNs", 0)
        fw_ns = wc.get("totalFrameworkOverheadNs", 0)
        if py_ns > 0:
            case["metrics"]["businessOperatorTime"] = {
                "total_ns": py_ns,
            }
        if fw_ns > 0:
            case["metrics"]["frameworkCallTime"] = {
                "total_ns": fw_ns,
            }

    timing_path.write_text(
        _json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info("Wrote wall-clock timing for %d queries to %s",
                len(wall_clock_times), timing_path.relative_to(platform_run_dir))


def _find_container_python(
    executor: "SshExecutor",
    env_config: dict | None = None,
    *,
    container: str = "flink-jm",
) -> str:
    """Find the Python binary path inside the target master container."""
    py_version = "3.14.3"
    if env_config:
        py_version = env_config.get("software", {}).get("pythonVersion", py_version)
    expected = f"/root/.pyenv/versions/{py_version}/bin/python3"
    result = executor.run(
        f"docker exec {container} ls {expected}",
        timeout=15,
    )
    if result.returncode == 0 and result.stdout.strip():
        return expected
    result = executor.run(
        f"docker exec {container} bash -c "
        "'ls /root/.pyenv/versions/*/bin/python3 2>/dev/null | sort -V | tail -1'",
        timeout=15,
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    return "python3"


def _ensure_jar(executor: "SshExecutor") -> None:
    """Ensure Java UDF JAR exists inside JM container, build if missing."""
    jar_path = "/opt/flink/usrlib/java-udf/FlinkDemo-1.0-SNAPSHOT.jar"
    check = executor.run(
        f"docker exec flink-jm ls {jar_path}",
        timeout=15,
    )
    if check.returncode == 0 and check.stdout.strip():
        return
    logger.info("[5a] JAR not found, building inside JM container...")
    result = executor.run(
        "docker exec flink-jm bash -c "
        "'cd /opt/flink/usrlib/java-udf && bash build.sh'",
        timeout=120,
        stream=True,
    )
    if result.returncode != 0:
        raise StepError(
            f"JAR build failed (exit {result.returncode}):\n"
            f"  output: {result.stdout[-2000:]}"
        )


def _find_container_perf(
    executor: "SshExecutor",
    *,
    container: str = "flink-tm1",
) -> str:
    """Find the perf binary path inside the target container."""
    result = executor.run(
        f"docker exec {container} bash -lc "
        "'command -v perf || ls /usr/lib/linux-tools-*/perf 2>/dev/null | sort -V | tail -1'",
        timeout=30,
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    return "/usr/bin/perf"


def _deploy_perf_wrapper(
    executor: "SshExecutor",
    tm_count: int,
    python_bin: str,
    perf_binary: str,
    include_jm: bool = False,
    *,
    master_container: str = "flink-jm",
    worker_containers: list[str] | None = None,
    perf_data_path: str = "/tmp/perf-udf.data",
) -> None:
    """Deploy perf wrapper script to containers.

    The wrapper uses ``exec perf record ... -- python "$@"`` so that perf
    wraps the entire Python UDF worker lifecycle.  Flink's
    ``python.executable`` config is set to this wrapper; when a job starts
    a Python worker, perf records it from first instruction to exit.

    Uses base64 encoding to avoid shell quoting issues when the script
    content (which contains ``$@``) passes through SSH and docker exec.
    """
    import base64

    script = (
        "#!/bin/bash\n"
        "export OPENBLAS_NUM_THREADS=1\n"
        "export OMP_NUM_THREADS=1\n"
        "export MKL_NUM_THREADS=1\n"
        "export NUMEXPR_NUM_THREADS=1\n"
        f"exec {perf_binary} record -F 999 -g -e task-clock "
        f"-o {perf_data_path} -- {python_bin} \"$@\" 2>/dev/null\n"
    )
    encoded = base64.b64encode(script.encode()).decode()

    wrapper_path = "/tmp/_perf_python_wrapper.sh"
    containers = []
    if include_jm:
        containers.append(master_container)
    containers.extend(worker_containers or [f"flink-tm{i}" for i in range(1, tm_count + 1)])
    for c in containers:
        executor.run(
            f"docker exec {c} rm -f {perf_data_path}",
            timeout=30,
        )
        executor.run(
            f"docker exec {c} bash -c "
            f"'echo {encoded} | base64 -d > {wrapper_path} && "
            f"chmod +x {wrapper_path}'",
            timeout=30,
        )


def _parse_tm_count(env_config: dict) -> int:
    """Parse TM count from environment.yaml software.clusterTopology (e.g. '1jm-2tm')."""
    software = env_config.get("software", {})
    topology = software.get("clusterTopology", "")
    if "-" in topology:
        parts = topology.split("-")
        if len(parts) >= 2 and parts[-1].endswith("tm"):
            try:
                return int(parts[-1].rstrip("tm"))
            except ValueError:
                pass
    return 2  # fallback


def _run_collect_substep(
    project_path: Path, run_dir: Path, platform: str,
    substep: str,
) -> None:
    """Execute a single 5b sub-step.

    Sub-steps: 5b.1 (perf.data), 5b.2 (perf-kits), 5b.2b (source extraction),
    5b.3 (objdump ASM).  Each checks its own output artifact and skips if present.
    """
    from .config import load_environment_config
    from .remote import build_executor, get_platform_host_ref

    env_config = load_environment_config(project_path)
    host_ref = get_platform_host_ref(env_config, platform)
    executor = build_executor(host_ref, env_config)

    platform_run_dir = run_dir / platform
    perf_dir = platform_run_dir / "perf" / "data"
    perf_dir.mkdir(parents=True, exist_ok=True)
    perf_data_local = perf_dir / f"perf-{platform}.data"
    perf_csv = perf_dir / "perf_records.csv"

    if substep == "5b.1":
        if perf_data_local.exists() and perf_data_local.stat().st_size > 0:
            logger.info("[5b.1] perf.data already exists (%d bytes), skipping",
                         perf_data_local.stat().st_size)
            return
        perf_container = _find_perf_container(executor, env_config, project_path)
        logger.info("[5b.1] Collecting perf.data from %s on %s...", perf_container, platform)
        _collect_binary_from_container(
            executor, perf_container, get_framework_config(project_path).get_perf_data_path(), perf_data_local,
        )
        return

    # Sub-steps 5b.2+ need perf.data to exist.
    if not perf_data_local.exists() or perf_data_local.stat().st_size == 0:
        raise StepError(f"[{substep}] perf.data not found — run 5b.1 first")

    if substep == "5b.2":
        if perf_csv.exists() and perf_csv.stat().st_size > 0:
            logger.info("[5b.2] perf_records.csv exists, skipping perf-kits on %s", platform)
            return
        perf_container = _find_perf_container(executor, env_config)
        logger.info("[5b.2] Running perf-kits analysis pipeline on %s (timeout=600s)...", platform)
        _run_perf_kits_on_remote(executor, perf_data_local, perf_dir, platform, project_path, perf_container)
        return

    # Sub-steps 5b.2b+ need perf_records.csv.
    if not perf_csv.exists() or perf_csv.stat().st_size == 0:
        raise StepError(f"[{substep}] perf_records.csv not found — run 5b.2 first")

    if substep == "5b.2b":
        source_map_path = perf_dir / "symbol_source_map.json"
        if source_map_path.exists() and source_map_path.stat().st_size > 10:
            logger.info("[5b.2b] CPython source map exists, skipping extraction on %s", platform)
            return
        perf_container = _find_perf_container(executor, env_config)
        logger.info("[5b.2b] Extracting CPython source for hotspot symbols on %s...", platform)
        _extract_cpython_sources(executor, perf_csv, source_map_path, perf_container)
        return

    if substep == "5b.3":
        asm_dir = platform_run_dir / "asm" / ("arm64" if platform == "arm" else "x86_64")
        asm_dir.mkdir(parents=True, exist_ok=True)
        if list(asm_dir.glob("*.s")):
            logger.info("[5b.3] ASM files exist (%d), skipping objdump on %s",
                         len(list(asm_dir.glob("*.s"))), platform)
            return
        perf_container = _find_perf_container(executor, env_config)
        logger.info("[5b.3] Collecting objdump for hotspot symbols on %s...", platform)
        _collect_asm_from_all_libs(executor, perf_dir, asm_dir, platform, perf_container)
        return

    raise StepError(f"Unknown 5b sub-step: {substep}")


def _find_perf_container(executor: "SshExecutor", env_config: dict, project_path: Path | None = None) -> str:
    """Find which container has perf.data."""
    _tm_count = _parse_tm_count(env_config)
    if project_path is not None:
        fw_config = get_framework_config(project_path)
        perf_data_path = fw_config.get_perf_data_path()
        containers = [fw_config.master_container] + fw_config.get_worker_containers(count=_tm_count)
        fallback = fw_config.master_container
    else:
        perf_data_path = "/tmp/perf-udf.data"
        containers = ["flink-jm"] + [f"flink-tm{i}" for i in range(1, _tm_count + 1)]
        fallback = "flink-jm"

    for c in containers:
        check = executor.run(
            f"docker exec {c} bash -c "
            f"'test -s {perf_data_path} && echo found'",
            timeout=15,
        )
        if "found" in check.stdout:
            logger.info("perf.data found in %s", c)
            return c
    logger.warning("Could not locate perf.data in any container, using %s as fallback", fallback)
    return fallback



def _load_symbol_map(asm_dir: Path) -> dict[str, str]:
    """Load hash→symbol mapping from symbol_map.json."""
    map_path = asm_dir / "symbol_map.json"
    if map_path.exists():
        try:
            return json.loads(map_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _extract_cpython_sources(
    executor: "SshExecutor",
    perf_csv: Path,
    output_path: Path,
    container: str = "flink-jm",
) -> None:
    """Extract C source code for hotspot symbols from CPython source in container.

    Writes a Python script + symbol list to local temp dir, transfers into the
    container via docker cp, runs the script, and collects the result back via
    docker cp.  Avoids base64 encoding and stdout-through-SSH issues.
    """
    import csv as _csv
    import tempfile

    # Read symbols from perf CSV.
    symbols: set[str] = set()
    try:
        with open(perf_csv, newline="", encoding="utf-8") as f:
            for row in _csv.DictReader(f):
                sym = (row.get("symbol") or "").strip()
                if sym and not sym.startswith("0x") and sym != "[unknown]":
                    if len(sym) >= 8 and all(c in "0123456789abcdef" for c in sym.lower()):
                        continue
                    symbols.add(sym)
    except Exception:
        return

    if not symbols:
        return

    # Write extraction script and symbol list to a local temp directory,
    # then docker cp into the container.
    extract_script = textwrap.dedent("""\
        import sys, os, re, json
        src = sys.argv[1]
        sym_file = sys.argv[2]
        output = sys.argv[3]
        with open(sym_file) as f:
            symbols = [l.strip() for l in f if l.strip()]
        result = {}
        c_dirs = [os.path.join(src, d) for d in ['Objects', 'Python', 'Modules', 'Parser']]
        c_files = []
        for d in c_dirs:
            if not os.path.isdir(d):
                continue
            for f in os.listdir(d):
                if f.endswith('.c'):
                    c_files.append(os.path.join(d, f))
        sym_patterns = {s: re.compile(r'\\b' + re.escape(s) + r'\\s*\\(') for s in symbols}
        for cf in c_files:
            try:
                with open(cf) as fh:
                    lines = fh.readlines()
            except Exception:
                continue
            rel = os.path.relpath(cf, src)
            for i, line in enumerate(lines):
                if '{' in line or line.strip().startswith('//') or line.strip().startswith('#'):
                    continue
                for sym, pat in list(sym_patterns.items()):
                    if sym in result:
                        continue
                    if not pat.search(line):
                        continue
                    depth = 0
                    body_lines = []
                    started = False
                    for j in range(max(0, i-2), min(len(lines), i+200)):
                        body_lines.append(lines[j].rstrip())
                        depth += lines[j].count('{') - lines[j].count('}')
                        if '{' in lines[j]:
                            started = True
                        if started and depth <= 0:
                            break
                    if not started:
                        continue
                    result[sym] = {'sourceFile': rel, 'snippet': '\\n'.join(body_lines)}
                    break
                if len(result) == len(symbols):
                    break
            if len(result) == len(symbols):
                break
        with open(output, 'w') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"extracted:{len(result)}/{len(symbols)}")
    """)

    with tempfile.TemporaryDirectory(prefix="pyframework_src_") as tmp_dir:
        tmp = Path(tmp_dir)
        (tmp / "_extract_src.py").write_text(extract_script, encoding="utf-8")
        (tmp / "_symbols.txt").write_text("\n".join(sorted(symbols)) + "\n", encoding="utf-8")

        # Push to remote host, then docker cp into container.
        host_staging = "/tmp/pyframework-src-extract"
        executor.run(f"rm -rf {host_staging} && mkdir -p {host_staging}", timeout=15)
        executor.push_file(tmp / "_extract_src.py", f"{host_staging}/_extract_src.py")
        executor.push_file(tmp / "_symbols.txt", f"{host_staging}/_symbols.txt")
        executor.run(f"docker cp {host_staging}/. {container}:/tmp/_src_extract/", timeout=30)
        executor.run(f"docker exec -u root {container} chown -R flink:flink /tmp/_src_extract", timeout=15)

    # Ensure CPython source is available (extract from pyenv cache if needed).
    src_prep = executor.run(
        f"docker exec {container} bash -c '"
        "test -d /tmp/cpython-src/Objects && echo ok || "
        "(TB=$(ls /root/.pyenv/cache/Python-*.tar.xz 2>/dev/null | head -1) && "
        "mkdir -p /tmp/cpython-src && tar xf $TB -C /tmp/cpython-src --strip-components=1 && "
        "echo extracted)'",
        timeout=60,
        stream=True,
    )
    if "ok" not in src_prep.stdout and "extracted" not in src_prep.stdout:
        logger.warning("CPython source not available in container, skipping source extraction")
        executor.run(f"docker exec {container} rm -rf /tmp/_src_extract", timeout=15)
        executor.run(f"rm -rf {host_staging}", timeout=15)
        return

    # Run the extraction script inside the container.
    result = executor.run(
        f"docker exec {container} python3 /tmp/_src_extract/_extract_src.py "
        f"/tmp/cpython-src /tmp/_src_extract/_symbols.txt /tmp/_src_extract/_result.json",
        timeout=180,
        stream=True,
    )
    logger.info("Source extraction: %s", result.stdout.strip() if result.stdout else result.stderr[:200])

    # Collect result via docker cp.
    host_result = "/tmp/pyframework-src-result.json"
    executor.run(f"docker cp {container}:/tmp/_src_extract/_result.json {host_result}", timeout=30)
    local_result = output_path.parent / "_result_tmp.json"
    local_result.parent.mkdir(parents=True, exist_ok=True)
    executor.fetch_file(host_result, local_result)

    try:
        source_map = json.loads(local_result.read_text(encoding="utf-8"))
        if source_map:
            output_path.write_text(
                json.dumps(source_map, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            logger.info("Extracted CPython source for %d/%d symbols", len(source_map), len(symbols))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to read source extraction result: %s", e)
    finally:
        local_result.unlink(missing_ok=True)

    # Cleanup.
    executor.run(f"docker exec {container} rm -rf /tmp/_src_extract", timeout=15)
    executor.run(f"rm -rf {host_staging} {host_result}", timeout=15)


def _collect_asm_from_all_libs(
    executor: "SshExecutor",
    perf_dir: Path,
    asm_dir: Path,
    platform: str,
    container: str = "flink-jm",
) -> None:
    """Collect objdump for top hotspot symbols from ALL shared libraries.

    Writes a Python script that runs entirely inside the container — finds
    libraries, runs objdump, extracts symbols, writes .s files and
    symbol_map.json into an output directory.  Then docker cp the results
    back.  Avoids per-library round trips through SSH.
    """
    import csv
    import tempfile
    from collections import Counter

    perf_csv = perf_dir / "perf_records.csv"
    if not perf_csv.exists():
        logger.warning("perf_records.csv not found, skipping multi-lib ASM collection")
        return

    # Group symbols by shared_object, filtering to self >= 0.5%.
    so_to_syms: dict[str, list[str]] = {}
    try:
        with open(perf_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                sym = (row.get("symbol") or "").strip()
                so = (row.get("shared_object") or "").strip()
                if not sym or sym.startswith("0x") or so in ("", "[unknown]"):
                    continue
                if len(sym) >= 8 and all(c in "0123456789abcdef" for c in sym.lower()):
                    continue
                if so == "[kernel.kallsyms]":
                    continue
                try:
                    self_pct = float(row.get("self", 0))
                except (ValueError, TypeError):
                    continue
                if self_pct < 0.5:
                    continue
                so_to_syms.setdefault(so, []).append(sym)
    except Exception as e:
        logger.warning("Failed to read perf_records.csv: %s", e)
        return

    for so, syms in so_to_syms.items():
        counts = Counter(syms)
        so_to_syms[so] = [s for s, _ in counts.most_common()]

    # Load existing symbol_map so the in-container script can skip collected symbols.
    existing_map = _load_symbol_map(asm_dir)

    # Write a single Python script + JSON manifest to run inside the container.
    asm_script = textwrap.dedent("""\
        import sys, os, re, json, subprocess, hashlib

        manifest = sys.argv[1]
        output_dir = sys.argv[2]

        with open(manifest) as f:
            data = json.load(f)
        so_to_syms = data['so_to_syms']
        existing_map = data.get('existing_map', {})

        collected_hashes = set(existing_map.keys()) if existing_map else set()

        symbol_map = dict(existing_map)
        search_dirs = '/usr/lib /usr/local/lib /opt /lib /root/.pyenv /root'.split()
        total_collected = 0

        def find_so(so_name):
            base = os.path.basename(so_name)
            stem = base.split('.so')[0]
            for d in search_dirs:
                for root, dirs, files in os.walk(d):
                    for fn in files:
                        if fn == base:
                            return os.path.join(root, fn)
                        if '.so' in fn and stem in fn:
                            return os.path.join(root, fn)
            return None

        for so_name, syms in sorted(so_to_syms.items()):
            so_path = find_so(so_name)
            if not so_path:
                print(f"skip:{so_name}:not_found")
                continue

            remaining = {}
            for sym in syms:
                h = hashlib.md5(sym.encode()).hexdigest()[:8]
                if h not in collected_hashes:
                    remaining[sym] = h

            if not remaining:
                print(f"{so_name}: already_collected/{len(syms)}")
                continue

            # Generate awk script: for each symbol, match /<symbol.*>:/ to /^$/
            awk_file = os.path.join(output_dir, '_extract.awk')
            with open(awk_file, 'w') as f:
                for sym, h in remaining.items():
                    f.write('/<' + sym + '.*>:/ { file="' + output_dir + '/' + h + '.s"; printing=1 }\\n')
                f.write('/^$/ { if (printing) { close(file); printing=0 }; next }\\n')
                f.write('printing { print > file }\\n')
                f.write('END { if (printing) close(file) }\\n')

            # objdump -d (no -S) + awk extracts each function's disassembly
            cmd = 'objdump -d ' + so_path + ' | awk -f ' + awk_file
            print(f"CMD:{so_name}: {cmd}")
            subprocess.run(cmd, shell=True, timeout=300)

            # Check results
            for sym, h in remaining.items():
                out_file = os.path.join(output_dir, h + '.s')
                if os.path.exists(out_file) and os.path.getsize(out_file) > 0:
                    symbol_map[h] = sym
                    collected_hashes.add(h)
                elif os.path.exists(out_file):
                    os.unlink(out_file)

            if os.path.exists(awk_file):
                os.unlink(awk_file)

            collected = sum(1 for s in syms
                           if hashlib.md5(s.encode()).hexdigest()[:8] in collected_hashes)
            no_addr = [s for s in remaining
                       if hashlib.md5(s.encode()).hexdigest()[:8] not in collected_hashes]
            if no_addr:
                print(f"{so_name}: no_addr symbols: {no_addr}")
            print(f"{so_name}: collected={collected},no_addr={len(no_addr)}/{len(syms)}")
            total_collected += collected

        map_path = os.path.join(output_dir, 'symbol_map.json')
        with open(map_path, 'w') as f:
            json.dump(symbol_map, f, ensure_ascii=False, indent=2)
        print(f"done:total={total_collected},map_entries={len(symbol_map)}")
    """)

    manifest = {
        "so_to_syms": so_to_syms,
        "existing_map": existing_map,
    }

    with tempfile.TemporaryDirectory(prefix="pyframework_asm_") as tmp_dir:
        tmp = Path(tmp_dir)
        (tmp / "_asm_collect.py").write_text(asm_script, encoding="utf-8")
        (tmp / "_manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False) + "\n", encoding="utf-8",
        )

        # Push to remote host, then docker cp into container.
        host_staging = "/tmp/pyframework-asm-collect"
        executor.run(f"rm -rf {host_staging} && mkdir -p {host_staging}", timeout=15)
        executor.push_file(tmp / "_asm_collect.py", f"{host_staging}/_asm_collect.py")
        executor.push_file(tmp / "_manifest.json", f"{host_staging}/_manifest.json")
        executor.run(f"docker cp {host_staging}/. {container}:/tmp/_asm_collect/", timeout=30)
        executor.run(f"docker exec -u root {container} chown -R flink:flink /tmp/_asm_collect", timeout=15)

    # Create output directory inside container and run the script.
    executor.run(
        f"docker exec {container} mkdir -p /tmp/_asm_output", timeout=15,
    )
    executor.run(
        f"docker exec -u root {container} chown flink:flink /tmp/_asm_output", timeout=15,
    )
    result = executor.run(
        f"docker exec {container} python3 /tmp/_asm_collect/_asm_collect.py "
        f"/tmp/_asm_collect/_manifest.json /tmp/_asm_output",
        timeout=600,
        stream=True,
    )
    if result.stdout:
        for line in result.stdout.strip().splitlines():
            logger.info("  ASM: %s", line)

    # Collect results: docker cp output dir → remote host → scp → local.
    host_output = "/tmp/pyframework-asm-output"
    executor.run(f"rm -rf {host_output}", timeout=15)
    executor.run(f"docker cp {container}:/tmp/_asm_output/. {host_output}", timeout=120)
    executor.fetch_dir(host_output, asm_dir)

    # Cleanup container + remote host.
    executor.run(f"docker exec {container} rm -rf /tmp/_asm_collect /tmp/_asm_output", timeout=15)
    executor.run(f"rm -rf {host_staging} {host_output}", timeout=15)

    logger.info("ASM collection: %d files in %s", len(list(asm_dir.glob("*.s"))), asm_dir)


def _run_perf_kits_on_remote(
    executor: "SshExecutor",
    perf_data_local: Path,
    perf_dir: Path,
    platform: str,
    project_path: Path | None = None,
    container: str = "flink-jm",
) -> None:
    """Run python-performance-kits pipeline inside the container.

    Running inside the container gives perf report access to the exact
    binaries (libpython3.14.so, etc.) so symbols resolve correctly.
    """
    # Resolve vendor dir: project_path is projects/<id>/project.yaml,
    # repo root is project_path.parent.parent.parent.
    if project_path:
        repo_root = project_path.parent.parent.parent
    else:
        repo_root = Path(__file__).resolve().parents[2]
    kits_local = repo_root / "vendor" / "python-performance-kits"
    scripts_dir = kits_local / "scripts" / "perf_insights"
    if not scripts_dir.exists():
        _init_submodules(repo_root)
    if not scripts_dir.exists():
        logger.warning("python-performance-kits not found at %s, skipping remote pipeline", kits_local)
        return

    container_kits = "/opt/flink/perf-kits-scripts"
    container_output = "/opt/flink/perf-kits-output"
    perf_data_container = "/tmp/perf-udf.data"
    python_bin = _find_container_python(executor)
    perf_bin = _find_container_perf(executor)

    # Deploy scripts into container via host staging.
    host_staging = "/tmp/pyframework-perf-kits-scripts"
    executor.run(f"rm -rf {host_staging} && mkdir -p {host_staging}", timeout=30)
    for script_name in [
        "run_single_platform_pipeline.py",
        "perf_data_to_csv.py",
        "perf_script_to_csv.py",
        "normalize_perf_records.py",
        "summarize_platform_perf.py",
        "annotate_perf_hotspots.py",
        "perf_analysis_common.py",
        "render_platform_report.py",
        "render_platform_visuals.py",
        "render_platform_machine_code_report.py",
        "show_symbol_machine_code.py",
        "cpython_category_rules.json",
    ]:
        src = scripts_dir / script_name
        if src.exists():
            executor.push_file(src, f"{host_staging}/{script_name}")

    # Copy scripts into container.
    executor.run(
        f"docker exec {container} rm -rf {container_kits}",
        timeout=30,
    )
    executor.run(
        f"docker cp {host_staging}/. {container}:{container_kits}",
        timeout=30,
    )
    executor.run(
        f"docker exec -u root {container} chown -R flink:flink {container_kits}",
        timeout=15,
    )
    executor.run(f"rm -rf {host_staging}", timeout=30)

    # Run the pipeline inside the container.
    logger.info("Running python-performance-kits pipeline inside %s (%s)...", container, platform)
    result = executor.run(
        f"docker exec {container} {python_bin} "
        f"{container_kits}/run_single_platform_pipeline.py "
        f"{perf_data_container} -o {container_output} "
        f"--benchmark tpch --platform {platform} "
        f"--perf-bin {perf_bin} "
        f"--skip-annotate --no-print-report",
        timeout=600,
        stream=True,
    )
    if result.returncode != 0:
        raise StepError(
            f"perf-kits pipeline failed inside {container} (exit {result.returncode}):\n"
            f"  Command: {python_bin} {container_kits}/run_single_platform_pipeline.py ...\n"
            f"  stderr: {result.stderr[:500]}\n"
            f"  stdout: {result.stdout[:500]}"
        )

    # Collect outputs from container via host staging.
    host_output = "/tmp/pyframework-perf-kits-output"
    executor.run(f"rm -rf {host_output}", timeout=30)
    cp_result = executor.run(
        f"docker cp {container}:{container_output}/ {host_output}",
        timeout=120,
        stream=True,
    )
    if cp_result.returncode != 0:
        raise StepError(
            f"docker cp perf-kits output failed: {cp_result.stdout}"
        )

    for remote_rel in [
        "data/perf_records.csv",
        "tables/category_summary.csv",
        "tables/shared_object_summary.csv",
        "tables/symbol_hotspots.csv",
    ]:
        remote_path = f"{host_output}/{remote_rel}"
        local_path = perf_dir.parent / remote_rel  # perf_dir is perf/data/
        local_path.parent.mkdir(parents=True, exist_ok=True)
        executor.fetch_file(remote_path, local_path)
        logger.info("Collected %s", remote_rel)

    # Cleanup host staging only (keep container output for inspection).
    executor.run(f"docker exec {container} rm -rf {container_kits}", timeout=30)
    executor.run(f"rm -rf {host_output}", timeout=30)


def _find_task_tm(executor: "SshExecutor") -> str | None:
    """Query JM REST API to find which TM container is running the task."""
    import json as _json

    # Get running jobs.
    result = executor.run(
        "docker exec flink-jm curl -sf http://localhost:8081/jobs",
        timeout=30,
    )
    if result.returncode != 0 or not result.stdout:
        return None

    try:
        jobs = _json.loads(result.stdout)
    except _json.JSONDecodeError:
        return None

    jobs_data = jobs.get("jobs", [])
    # Flink REST API returns {"jobs": []} when no jobs exist, or
    # {"jobs": {"running": [...], "finished": [...]}} with running jobs.
    if isinstance(jobs_data, list):
        # All jobs listed directly (v2 API) — find recent finished job
        running = []
        finished = jobs_data
    else:
        running = jobs_data.get("running", [])
        finished = jobs_data.get("finished", [])

    job_id = None
    if running:
        job_id = running[0]
    elif finished:
        job_id = finished[-1]  # most recent finished job

    # Get job vertices to find task location.
    result = executor.run(
        f"docker exec flink-jm curl -sf http://localhost:8081/jobs/{job_id}",
        timeout=30,
    )
    if result.returncode != 0 or not result.stdout:
        return None

    try:
        job_detail = _json.loads(result.stdout)
    except _json.JSONDecodeError:
        return None

    vertices = job_detail.get("vertices", [])
    if not vertices:
        return None

    # Look for the vertex containing Python UDF (usually has "Python" or "CHAIN" in name).
    for vertex in vertices:
        subtasks = vertex.get("subtasks", [])
        if subtasks:
            host = subtasks[0].get("host", "")
            # The host is the container ID or hostname; map back to container name.
            # In Docker network mode, host is the container hostname (e.g., container ID).
            # Fallback: try to match by checking which TM has this host.
            for i in range(1, 10):
                r = executor.run(
                    f"docker exec flink-tm{i} hostname",
                    timeout=30,
                )
                if r.returncode == 0 and r.stdout.strip() == host:
                    return f"flink-tm{i}"

    return None


def _collect_binary_from_container(
    executor: "SshExecutor",
    container: str,
    remote_path: str,
    local_path: Path,
) -> bool:
    """Collect a binary file from a container via docker cp + scp."""
    staging = f"/opt/flink/_collect_{local_path.name}"
    host_tmp = f"/tmp/pyframework-collect-{container}-{local_path.name}"

    # Copy inside container to a path accessible by docker cp.
    executor.run(
        f"docker exec -u root {container} cp {remote_path} {staging} 2>/dev/null",
        timeout=30,
    )
    executor.run(
        f"docker exec -u root {container} chmod 644 {staging} 2>/dev/null",
        timeout=30,
    )

    # docker cp from container to host filesystem.
    cp_result = executor.run(
        f"docker cp {container}:{staging} {host_tmp}",
        timeout=120,
        stream=True,
    )
    if cp_result.returncode != 0:
        logger.warning("docker cp failed for %s:%s: %s", container, staging, cp_result.stderr)
        return False

    # scp from host to local (binary-safe).
    ok = executor.fetch_file(host_tmp, local_path)

    # Cleanup.
    executor.run(f"docker exec {container} rm -f {staging}", timeout=30)
    executor.run(f"rm -f {host_tmp}", timeout=30)

    return ok


def _run_acquire_all(project_path: Path, run_dir: Path, *, force: bool = False) -> None:
    from .acquisition.timing import collect_timing

    config = load_project_config(project_path)
    run_config = get_run_config(project_path)
    platforms = run_config.get("platforms", [])

    for plat in platforms:
        plat_dir = run_dir / plat
        # Step 5a's _merge_wall_clock_times writes timing-normalized.json
        # directly from container logs.  Only fall back to log-file parsing
        # if step 5a didn't produce the file.
        timing_file = plat_dir / "timing" / "timing-normalized.json"
        if timing_file.exists() and timing_file.stat().st_size > 10:
            import json as _json
            existing = _json.loads(timing_file.read_text(encoding="utf-8"))
            cases = existing.get("cases", [])
            logger.info("Timing %s: %d cases (from step 5a)", plat, len(cases))
        else:
            stdout_files = list(plat_dir.glob("tm-stdout-*.log"))
            timing_result = collect_timing(plat_dir, plat, stdout_files or None)
            logger.info("Timing %s: %d cases", plat, len(timing_result.get("cases", [])))

        # Perf-kits and objdump already ran on remote in step 5b.
        # The local collect_perf/collect_asm would fail because the binaries
        # (libpython3.14.so, etc.) live inside the Docker container, not locally.
        perf_data_dir = plat_dir / "perf" / "data"
        asm_dir = plat_dir / "asm" / ("arm64" if plat == "arm" else "x86_64")
        perf_csv = perf_data_dir / "perf_records.csv"
        asm_files = list(asm_dir.glob("*.s")) if asm_dir.exists() else []
        logger.info("Perf %s: %s", plat,
                     "collected" if perf_csv.exists() and perf_csv.stat().st_size > 0 else "missing")
        logger.info("ASM %s: %s", plat,
                     f"{len(asm_files)} files" if asm_files else "missing")


def _run_backfill(project_path: Path, run_dir: Path, *, force: bool = False) -> None:
    from .backfill.pipeline import run_backfill
    from .config import get_run_config

    run_config = get_run_config(project_path)
    platforms = run_config.get("platforms", [])

    if len(platforms) < 2:
        raise StepError("Need at least 2 platforms for backfill")

    arm_dir = run_dir / platforms[0]
    x86_dir = run_dir / platforms[1]

    rc = run_backfill(project_path, arm_dir, x86_dir)
    if rc != 0:
        raise StepError(
            f"Backfill failed (rc={rc}). Check logs above for which sub-module failed.\n"
            f"  ARM dir: {arm_dir}\n"
            f"  x86 dir: {x86_dir}"
        )


def _run_compare(project_path: Path, run_dir: Path) -> None:
    """Step 6b: run cross-platform comparison using python-performance-kits."""
    from .compare.pipeline import run_compare

    arm_dir = run_dir / "arm"
    x86_dir = run_dir / "x86"

    if not arm_dir.is_dir():
        raise StepError(f"ARM run directory not found: {arm_dir}")
    if not x86_dir.is_dir():
        raise StepError(f"x86 run directory not found: {x86_dir}")

    result = run_compare(project_path, arm_dir, x86_dir)
    if result.get("status") != "completed":
        raise StepError(f"Compare failed: {result}")
    logger.info("[S6b] Compare complete: %s", result.get("output_dir", ""))


def _run_bridge_publish(project_path: Path) -> None:
    from .bridge.analysis import publish
    from .config import get_bridge_config

    bridge_config = get_bridge_config(project_path)
    result = publish(
        project_path,
        repo=bridge_config["repo"],
        platform=bridge_config["platform"],
        token=bridge_config["token"],
        bridge_type=bridge_config.get("type", "discussion"),
        discussion_category=bridge_config.get("category", "General"),
    )
    if result.get("errors", 0) > 0:
        raise StepError(
            f"Bridge publish had {result['errors']} error(s):\n"
            f"  repo: {bridge_config['repo']}\n"
            f"  platform: {bridge_config['platform']}\n"
            f"  type: {bridge_config.get('type', 'discussion')}\n"
            f"  details: {result.get('error_details', 'see logs above')}"
        )


import sys

from .config import load_project_config, get_run_config


def _print_resume_hint(
    step_id: str, platform: str | None, project_path: Path,
    error: str = "",
) -> None:
    plat_str = f' on platform "{platform}"' if platform else ""
    step_name = next((d["name"] for d in STEP_DEFS if d["step"] == step_id), step_id)
    print(
        f"\nERROR: Step {step_id} \"{step_name}\" failed{plat_str}",
        file=sys.stderr,
    )
    if error:
        print(error, file=sys.stderr)
    print(
        f"\nResume from this step:\n"
        f"  pyframework-pipeline run {project_path} --resume-from {step_id}",
        file=sys.stderr,
    )


def _collect_pyspark_asm(
    executor: "SshExecutor",
    platform_run_dir: Path,
    fw_config,
    worker_count: int,
) -> None:
    """Collect and parse assembly from PySpark worker containers.

    Extracts JVM bytecode and maps to framework taxonomy categories.
    """
    import json as _json

    asm_dir = platform_run_dir / "asm"
    asm_dir.mkdir(parents=True, exist_ok=True)

    master_container = fw_config.master_container
    worker_containers = fw_config.get_worker_containers(count=worker_count)

    # Collect perf report or objdump output from each container
    collected_containers = []
    for container in [master_container] + worker_containers:
        try:
            # Try to extract perf report output
            result = executor.run(
                f"docker exec {container} bash -c "
                f"'perf report -i /tmp/perf-worker.data --stdio 2>&1 | head -200'",
                timeout=30,
            )
            if result.returncode == 0 and result.stdout:
                asm_file = asm_dir / f"{container}-asm.txt"
                asm_file.write_text(result.stdout, encoding="utf-8")
                collected_containers.append(container)
                logger.info("[5b] Collected ASM from %s (%d lines)", container, len(result.stdout.splitlines()))
        except Exception as e:
            logger.warning("[5b] Failed to collect ASM from %s: %s", container, e)

    if not collected_containers:
        logger.warning("[5b] No ASM data collected from any container")
        return

    # Parse and categorize the assembly data
    asm_summary = {
        "schemaVersion": 1,
        "platform": "",
        "containers": collected_containers,
        "categories": {
            "Interpreter": {"count": 0, "samples": []},
            "Memory": {"count": 0, "samples": []},
            "GC": {"count": 0, "samples": []},
            "Object Model": {"count": 0, "samples": []},
            "Type Operations": {"count": 0, "samples": []},
            "Calls / Dispatch": {"count": 0, "samples": []},
            "Native Boundary": {"count": 0, "samples": []},
            "Kernel": {"count": 0, "samples": []},
        },
    }

    # Pattern matching for each category
    patterns = {
        "Interpreter": [r"InterpreterRuntime", r"bytecode", r"invoke", r"_dispatch"],
        "Memory": [r"malloc", r"alloc", r"free", r"heap", r"memory"],
        "GC": [r"scavenge", r"mark_sweep", r"GC", r"CollectedHeap"],
        "Object Model": [r"Klass", r"oopDesc", r"instanceKlass", r"field"],
        "Type Operations": [r"cast", r"isinstance", r"checkcast", r"Type::"],
        "Calls / Dispatch": [r"vtable", r"itable", r"method_entry", r"LinkResolver"],
        "Native Boundary": [r"jni", r"native", r"JNI_", r"entry_point"],
        "Kernel": [r"syscall", r"sys_", r"mmap", r"futex", r"epoll"],
    }

    import re as _re

    # Process collected ASM files
    for asm_file in asm_dir.glob("*-asm.txt"):
        content = asm_file.read_text(encoding="utf-8", errors="ignore")
        for line in content.splitlines():
            # Try to categorize the line
            for category, pattern_list in patterns.items():
                if any(_re.search(p, line, _re.IGNORECASE) for p in pattern_list):
                    asm_summary["categories"][category]["count"] += 1
                    if len(asm_summary["categories"][category]["samples"]) < 5:
                        # Keep up to 5 sample lines per category
                        asm_summary["categories"][category]["samples"].append(line.strip()[:100])
                    break

    # Write summary
    asm_summary_file = asm_dir / "asm-summary.json"
    asm_summary_file.write_text(
        _json.dumps(asm_summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info("[5b] ASM summary written to %s", asm_summary_file.relative_to(platform_run_dir.parent))
