import argparse
import json
import sys
from pathlib import Path

from .validators.four_layer import validate_four_layer_project


def _schemas_dir() -> Path:
    return Path(__file__).resolve().parent.parent.parent / "schemas"


def _resolve_project_yaml(path: Path) -> Path:
    if path.is_dir():
        return path / "project.yaml"
    return path


def _load_adapter(framework: str):
    if framework == "pyflink":
        from .adapters.pyflink.environment import PyFlinkEnvironmentAdapter
        return PyFlinkEnvironmentAdapter()
    elif framework == "pyspark":
        from .adapters.pyspark.environment import PySparkEnvironmentAdapter
        return PySparkEnvironmentAdapter()
    raise ValueError(f"No environment adapter for framework: {framework}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pyframework-pipeline",
        description="Python 框架自动化分析流程工具。",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # config
    config_parser = subparsers.add_parser(
        "config",
        help="[Step 1] 配置获取和完整性校验。",
    )
    config_sub = config_parser.add_subparsers(dest="config_command", required=True)
    config_validate = config_sub.add_parser(
        "validate",
        help="[S1] 校验 project.yaml / environment.yaml / 运行配置。",
    )
    config_validate.add_argument("project", help="project.yaml 路径")
    config_validate.add_argument(
        "--skip-bridge-token",
        action="store_true",
        help="只做桥接前流程时跳过 token 校验。",
    )

    # validate
    subparsers.add_parser(
        "validate",
        help="校验四层输入目录或 project.yaml。",
    ).add_argument("path", help="四层输入目录或 project.yaml 路径")

    # run (one-click)
    run_p = subparsers.add_parser(
        "run",
        help="[S3→S7] 一键全流程（可断点续跑）。",
    )
    run_p.add_argument("project", help="project.yaml 路径")
    run_p.add_argument("--run-dir", help="运行输出目录")
    run_p.add_argument("--resume-from", help="从指定步骤恢复（如 5b.2b）")
    run_p.add_argument("--stop-before", help="在指定步骤前停止（如 5b.3）")
    run_p.add_argument("--force", action="store_true", help="清空状态重新跑")
    run_p.add_argument("--yes", action="store_true", help="跳过确认提示")

    # environment [Step 3]
    env_parser = subparsers.add_parser(
        "environment",
        help="[Step 3] 环境搭建。",
    )
    env_sub = env_parser.add_subparsers(dest="env_command", required=True)

    # environment plan
    plan_parser = env_sub.add_parser(
        "plan",
        help="[S3] 生成环境搭建计划（plan-only，不执行）。",
    )
    plan_parser.add_argument("project", help="project.yaml 路径")
    plan_parser.add_argument("--platform", required=True, help="目标平台 ID")
    plan_parser.add_argument("--output", help="输出目录")

    # environment deploy
    deploy_parser = env_sub.add_parser(
        "deploy",
        help="[S3] SSH 执行环境部署计划。",
    )
    deploy_parser.add_argument("project", help="project.yaml 路径")
    deploy_parser.add_argument("--platform", required=True, help="目标平台 ID")
    deploy_parser.add_argument("--plan", help="已有的 environment-plan.json 路径")
    deploy_parser.add_argument("--yes", action="store_true", help="跳过确认提示")
    deploy_parser.add_argument("--output", help="输出目录（与 plan 共用）")

    # environment teardown
    teardown_parser = env_sub.add_parser(
        "teardown",
        help="[S3] 销毁远程集群。",
    )
    teardown_parser.add_argument("project", help="project.yaml 路径")
    teardown_parser.add_argument("--platform", required=True, help="目标平台 ID")
    teardown_parser.add_argument("--yes", action="store_true", help="跳过确认提示")

    # environment validate
    env_validate_parser = env_sub.add_parser(
        "validate",
        help="[S3] 校验环境记录和 readiness 报告。",
    )
    env_validate_parser.add_argument("run_dir", help="运行目录")

    # workload [Step 4]
    workload_parser = subparsers.add_parser(
        "workload",
        help="[Step 4] Workload 部署。",
    )
    workload_sub = workload_parser.add_subparsers(dest="workload_command", required=True)

    workload_deploy = workload_sub.add_parser(
        "deploy",
        help="[S4] 上传 workload 并分发到容器。",
    )
    workload_deploy.add_argument("project", help="project.yaml 路径")
    workload_deploy.add_argument("--platform", required=True, help="目标平台 ID")

    # benchmark [Step 5a]
    bench_parser = subparsers.add_parser(
        "benchmark",
        help="[Step 5a] 基准测试执行。",
    )
    bench_sub = bench_parser.add_subparsers(dest="bench_command", required=True)

    bench_run = bench_sub.add_parser(
        "run",
        help="[S5a] SSH 执行 benchmark + 容器内 perf 采集。",
    )
    bench_run.add_argument("project", help="project.yaml 路径")
    bench_run.add_argument("--platform", required=True, help="目标平台 ID")
    bench_run.add_argument("--run-dir", help="运行输出目录")

    # collect [Step 5b]
    collect_parser = subparsers.add_parser(
        "collect",
        help="[Step 5b] 远程数据采集。",
    )
    collect_sub = collect_parser.add_subparsers(dest="collect_command", required=True)

    collect_run = collect_sub.add_parser(
        "run",
        help="[S5b] 从远程容器拉取 perf/asm/logs。",
    )
    collect_run.add_argument("project", help="project.yaml 路径")
    collect_run.add_argument("--platform", required=True, help="目标平台 ID")
    collect_run.add_argument("--run-dir", required=True, help="本地运行输出目录")

    # acquire [Step 5c]
    acquire_parser = subparsers.add_parser(
        "acquire",
        help="[Step 5c] 数据解析（本地）。",
    )
    acquire_sub = acquire_parser.add_subparsers(dest="acquire_command", required=True)

    acq_timing = acquire_sub.add_parser(
        "timing",
        help="[S5c] 采集用例数据（框架开销计时）。",
    )
    acq_timing.add_argument("project", help="project.yaml 路径")
    acq_timing.add_argument("--platform", required=True)
    acq_timing.add_argument("--run-dir", required=True)
    acq_timing.add_argument("--stdout-file", action="append", dest="stdout_files")

    acq_perf = acquire_sub.add_parser(
        "perf",
        help="[S5c] 采集性能分析数据（perf profile）。",
    )
    acq_perf.add_argument("project", help="project.yaml 路径")
    acq_perf.add_argument("--platform", required=True)
    acq_perf.add_argument("--run-dir", required=True)
    acq_perf.add_argument("--perf-data")
    acq_perf.add_argument("--kits-dir")
    acq_perf.add_argument("--top-n", type=int, default=50)

    acq_asm = acquire_sub.add_parser(
        "asm",
        help="[S5c] 采集机器码（反汇编）。",
    )
    acq_asm.add_argument("project", help="project.yaml 路径")
    acq_asm.add_argument("--platform", required=True)
    acq_asm.add_argument("--run-dir", required=True)
    acq_asm.add_argument("--perf-data")
    acq_asm.add_argument("--kits-dir")
    acq_asm.add_argument("--binary", action="append", dest="binaries")
    acq_asm.add_argument("--top-n", type=int, default=20)

    acquire_sub.add_parser(
        "validate",
        help="[S5c] 校验采集清单完整性。",
    ).add_argument("run_dir")

    acq_all = acquire_sub.add_parser(
        "all",
        help="[S5c] 执行全部采集子步骤。",
    )
    acq_all.add_argument("project", help="project.yaml 路径")
    acq_all.add_argument("--platform", required=True)
    acq_all.add_argument("--run-dir", required=True)
    acq_all.add_argument("--perf-data")
    acq_all.add_argument("--kits-dir")
    acq_all.add_argument("--binary", action="append", dest="binaries")
    acq_all.add_argument("--stdout-file", action="append", dest="stdout_files")
    acq_all.add_argument("--top-n", type=int, default=50)

    # backfill [Step 6]
    backfill_parser = subparsers.add_parser(
        "backfill",
        help="[Step 6] 数据回填。",
    )
    backfill_sub = backfill_parser.add_subparsers(dest="backfill_command", required=True)

    bf_run = backfill_sub.add_parser(
        "run",
        help="[S6] 执行数据回填。",
    )
    bf_run.add_argument("project", help="project.yaml 路径")
    bf_run.add_argument("--arm-run-dir", required=True)
    bf_run.add_argument("--x86-run-dir", required=True)
    bf_run.add_argument("--output")

    backfill_sub.add_parser(
        "status",
        help="[S6] 查看回填状态。",
    ).add_argument("project")

    # bridge [Step 7]
    bridge_parser = subparsers.add_parser(
        "bridge",
        help="[Step 7] 差异分析 Issue 桥接。",
    )
    bridge_sub = bridge_parser.add_subparsers(dest="bridge_command", required=True)

    br_pub = bridge_sub.add_parser(
        "publish",
        help="[S7] 发布分析 Issue。",
    )
    br_pub.add_argument("project", help="project.yaml 路径")
    br_pub.add_argument("--repo", help="覆盖 project.yaml 中的 bridge.repo")
    br_pub.add_argument("--platform", choices=["github", "gitcode"], help="覆盖 bridge.platform")
    br_pub.add_argument("--token", help="覆盖环境变量中的 token")
    br_pub.add_argument("--dry-run", action="store_true")
    br_pub.add_argument("--max-lines", type=int, default=2000)
    br_pub.add_argument("--base-url")

    br_fetch = bridge_sub.add_parser(
        "fetch",
        help="[S7] 拉取分析评论并回填 Dataset。",
    )
    br_fetch.add_argument("project", help="project.yaml 路径")
    br_fetch.add_argument("--repo", help="覆盖 project.yaml 中的 bridge.repo")
    br_fetch.add_argument("--platform", choices=["github", "gitcode"], help="覆盖 bridge.platform")
    br_fetch.add_argument("--token", help="覆盖环境变量中的 token")
    br_fetch.add_argument("--base-url")

    bridge_sub.add_parser(
        "status",
        help="[S7] 查看桥接状态。",
    ).add_argument("project")

    # --- compare (Step 6b) ---
    compare_parser = subparsers.add_parser(
        "compare",
        help="[Step 6b] 双平台性能对比。",
    )
    compare_parser.add_argument("project", help="project.yaml 路径")
    compare_parser.add_argument("--arm-run-dir", type=Path, help="ARM run 目录")
    compare_parser.add_argument("--x86-run-dir", type=Path, help="x86 run 目录")
    compare_parser.add_argument("--output-dir", type=Path, help="输出目录")
    compare_parser.add_argument("--top-n", type=int, default=20, help="报告展示前 N 条")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "validate":
        report = validate_four_layer_project(Path(args.path))
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
        return 0 if report.status == "ok" else 1

    if args.command == "config":
        return _handle_config(args)

    if args.command == "run":
        return _cmd_run(args)

    if args.command == "environment":
        return _handle_environment(args)

    if args.command == "workload":
        return _handle_workload(args)

    if args.command == "benchmark":
        return _handle_benchmark(args)

    if args.command == "collect":
        return _handle_collect(args)

    if args.command == "acquire":
        return _handle_acquire(args)

    if args.command == "backfill":
        return _handle_backfill(args)

    if args.command == "bridge":
        return _handle_bridge(args)

    if args.command == "compare":
        return _cmd_compare(args)

    parser.print_help(sys.stderr)
    return 2


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------

def _handle_config(args) -> int:
    if args.config_command == "validate":
        return _cmd_config_validate(args)
    return 2


def _cmd_config_validate(args) -> int:
    from .config import validate_pipeline_config

    result = validate_pipeline_config(
        _resolve_project_yaml(Path(args.project)),
        require_bridge_token=not args.skip_bridge_token,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "ok" else 1


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

def _cmd_run(args) -> int:
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    from .config import get_run_config, load_project_config, validate_pipeline_config

    project_path = _resolve_project_yaml(Path(args.project))
    config_report = validate_pipeline_config(
        project_path,
        require_bridge_token=_run_requires_bridge_token(args.stop_before),
    )
    if config_report["status"] != "ok":
        print("Error: config validation failed", file=sys.stderr)
        for issue in config_report["issues"]:
            print(
                f"  - {issue['path']}: {issue['message']}",
                file=sys.stderr,
            )
        return 1

    from .orchestrator import run_pipeline

    config = load_project_config(project_path)
    run_config = get_run_config(project_path)

    # Resolve run directory.
    if args.run_dir:
        run_dir = Path(args.run_dir)
    else:
        runs_base = project_path.parent / "runs"
        run_dir = runs_base / _now_date_str()
    run_dir.mkdir(parents=True, exist_ok=True)

    return run_pipeline(
        project_path,
        run_dir,
        resume_from=args.resume_from,
        stop_before=args.stop_before,
        force=args.force,
        yes=args.yes,
    )


def _run_requires_bridge_token(stop_before: str | None) -> bool:
    if stop_before is None:
        return True
    step_ids = ["3", "4", "5a", "5b", "5c", "6", "7"]
    if stop_before not in step_ids:
        return True
    return step_ids.index(stop_before) > step_ids.index("7")


def _now_date_str() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# environment
# ---------------------------------------------------------------------------

def _handle_environment(args) -> int:
    if args.env_command == "plan":
        return _cmd_env_plan(args)
    if args.env_command == "deploy":
        return _cmd_env_deploy(args)
    if args.env_command == "teardown":
        return _cmd_env_teardown(args)
    if args.env_command == "validate":
        return _cmd_env_validate(args)
    return 2


def _cmd_env_plan(args) -> int:
    from .environment.parser import load_environment_yaml
    from .environment.planning import generate_plan

    project_path = _resolve_project_yaml(Path(args.project))
    env_yaml_path = project_path.parent / "environment.yaml"
    if not env_yaml_path.exists():
        print(f"Error: environment.yaml not found at {env_yaml_path}", file=sys.stderr)
        return 1

    env_config = load_environment_yaml(env_yaml_path)
    framework = env_config.get("framework", "")

    try:
        adapter = _load_adapter(framework)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    try:
        plan = generate_plan(project_path, args.platform, adapter)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    plan_json = json.dumps(plan, ensure_ascii=False, indent=2)
    if args.output:
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "environment-plan.json").write_text(plan_json, encoding="utf-8")
        print(f"Plan written to {output_dir / 'environment-plan.json'}")
    else:
        print(plan_json)
    return 0


def _cmd_env_deploy(args) -> int:
    from .environment.deploy import deploy_plan

    project_path = _resolve_project_yaml(Path(args.project))
    plan_path = Path(args.plan) if args.plan else None
    output_dir = Path(args.output) if args.output else None

    result = deploy_plan(project_path, args.platform, plan_path,
                         output_dir=output_dir, yes=args.yes)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") != "failed" else 1


def _cmd_env_teardown(args) -> int:
    from .environment.deploy import teardown

    project_path = _resolve_project_yaml(Path(args.project))
    result = teardown(project_path, args.platform, yes=args.yes)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") != "failed" else 1


def _cmd_env_validate(args) -> int:
    from .environment.records import validate_run

    run_dir = Path(args.run_dir)
    if not run_dir.is_dir():
        print(f"Error: {run_dir} is not a directory", file=sys.stderr)
        return 1

    schemas = _schemas_dir()
    report = validate_run(run_dir, schemas)
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    return 0 if report.status == "ok" else 1


# ---------------------------------------------------------------------------
# workload
# ---------------------------------------------------------------------------

def _handle_workload(args) -> int:
    if args.workload_command == "deploy":
        return _cmd_workload_deploy(args)
    return 2


def _cmd_workload_deploy(args) -> int:
    from .orchestrator import _run_workload_deploy

    project_path = _resolve_project_yaml(Path(args.project))
    try:
        _run_workload_deploy(project_path, Path("/tmp/run"), args.platform, yes=False)
        print(json.dumps({"status": "deployed", "platform": args.platform}))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}), file=sys.stderr)
        return 1


# ---------------------------------------------------------------------------
# benchmark
# ---------------------------------------------------------------------------

def _handle_benchmark(args) -> int:
    if args.bench_command == "run":
        return _cmd_benchmark_run(args)
    return 2


def _cmd_benchmark_run(args) -> int:
    from .orchestrator import _run_benchmark
    from .config import load_project_config

    project_path = _resolve_project_yaml(Path(args.project))
    run_dir = Path(args.run_dir) if args.run_dir else project_path.parent / "runs" / _now_date_str()
    run_dir.mkdir(parents=True, exist_ok=True)

    try:
        _run_benchmark(project_path, run_dir, args.platform)
        print(json.dumps({"status": "completed", "platform": args.platform}))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}), file=sys.stderr)
        return 1


# ---------------------------------------------------------------------------
# collect
# ---------------------------------------------------------------------------

def _handle_collect(args) -> int:
    if args.collect_command == "run":
        return _cmd_collect_run(args)
    return 2


def _cmd_collect_run(args) -> int:
    from .orchestrator import _run_collect

    project_path = _resolve_project_yaml(Path(args.project))
    run_dir = Path(args.run_dir)

    try:
        _run_collect(project_path, run_dir, args.platform)
        print(json.dumps({"status": "collected", "platform": args.platform}))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}), file=sys.stderr)
        return 1


# ---------------------------------------------------------------------------
# acquire
# ---------------------------------------------------------------------------

def _handle_acquire(args) -> int:
    if args.acquire_command == "timing":
        return _cmd_acquire_timing(args)
    if args.acquire_command == "perf":
        return _cmd_acquire_perf(args)
    if args.acquire_command == "asm":
        return _cmd_acquire_asm(args)
    if args.acquire_command == "validate":
        return _cmd_acquire_validate(args)
    if args.acquire_command == "all":
        return _cmd_acquire_all(args)
    return 2


def _resolve_run_dir(args) -> Path:
    project_dir = _resolve_project_yaml(Path(args.project)).resolve().parent
    run_dir = Path(args.run_dir)
    if not run_dir.is_absolute():
        run_dir = project_dir / run_dir
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _write_manifest(run_dir: Path, manifest: "AcquisitionManifest") -> None:
    from .acquisition.manifest import AcquisitionManifest
    manifest.write(run_dir / "acquisition-manifest.json")


def _cmd_acquire_timing(args) -> int:
    from .acquisition.timing import collect_timing
    from .acquisition.manifest import AcquisitionManifest, AcquisitionSection

    run_dir = _resolve_run_dir(args)
    stdout_files = [Path(f) for f in (args.stdout_files or [])]

    result = collect_timing(run_dir, args.platform, stdout_files or None)
    print(json.dumps(result, ensure_ascii=False, indent=2))

    manifest_path = run_dir / "acquisition-manifest.json"
    if manifest_path.exists():
        from .acquisition.manifest import load_manifest
        manifest = load_manifest(manifest_path)
    else:
        manifest = AcquisitionManifest(platform=args.platform, runDir=str(run_dir))
    manifest.timing = AcquisitionSection(
        status="collected" if result["cases"] else "skipped",
        files={"raw": result.get("raw_file", ""), "normalized": result.get("normalized_file", "")},
        extra={"cases": result.get("cases", [])},
    )
    _write_manifest(run_dir, manifest)
    return 0


def _cmd_acquire_perf(args) -> int:
    from .acquisition.perf_profile import collect_perf
    from .acquisition.manifest import AcquisitionManifest, AcquisitionSection

    run_dir = _resolve_run_dir(args)
    perf_data = Path(args.perf_data) if args.perf_data else None
    kits_dir = Path(args.kits_dir) if args.kits_dir else None

    result = collect_perf(run_dir, args.platform, perf_data, kits_dir, top_n=args.top_n)
    print(json.dumps(result, ensure_ascii=False, indent=2))

    manifest_path = run_dir / "acquisition-manifest.json"
    if manifest_path.exists():
        from .acquisition.manifest import load_manifest
        manifest = load_manifest(manifest_path)
    else:
        manifest = AcquisitionManifest(platform=args.platform, runDir=str(run_dir))
    manifest.perf = AcquisitionSection(
        status=result.get("status", "pending"),
        files=result.get("files", {}),
    )
    _write_manifest(run_dir, manifest)
    return 0 if result.get("status") != "failed" else 1


def _cmd_acquire_asm(args) -> int:
    from .acquisition.machine_code import collect_asm
    from .acquisition.manifest import AcquisitionManifest, AcquisitionSection

    run_dir = _resolve_run_dir(args)
    perf_data = Path(args.perf_data) if args.perf_data else None
    kits_dir = Path(args.kits_dir) if args.kits_dir else None
    binaries = [Path(b) for b in (args.binaries or [])]

    result = collect_asm(run_dir, args.platform, perf_data, kits_dir, binaries, args.top_n)
    print(json.dumps(result, ensure_ascii=False, indent=2))

    manifest_path = run_dir / "acquisition-manifest.json"
    if manifest_path.exists():
        from .acquisition.manifest import load_manifest
        manifest = load_manifest(manifest_path)
    else:
        manifest = AcquisitionManifest(platform=args.platform, runDir=str(run_dir))
    manifest.asm = AcquisitionSection(
        status=result.get("status", "pending"),
        extra={
            "hotspotCount": result.get("hotspotCount", 0),
            "objdumpFiles": result.get("objdumpFiles", []),
        },
    )
    _write_manifest(run_dir, manifest)
    return 0 if result.get("status") != "failed" else 1


def _cmd_acquire_validate(args) -> int:
    import jsonschema as _js

    run_dir = Path(args.run_dir)
    manifest_path = run_dir / "acquisition-manifest.json"

    if not manifest_path.exists():
        print(json.dumps({
            "status": "error",
            "errors": [f"acquisition-manifest.json not found in {run_dir}"],
        }, ensure_ascii=False, indent=2))
        return 1

    manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
    schema_path = _schemas_dir() / "acquisition-manifest.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    try:
        _js.validate(instance=manifest_data, schema=schema)
    except _js.ValidationError as e:
        print(json.dumps({"status": "error", "errors": [str(e.message)]}, ensure_ascii=False, indent=2))
        return 1

    errors = []
    for section in ("timing", "perf", "asm"):
        sec = manifest_data.get(section, {})
        if sec.get("status") == "collected":
            for fname in sec.get("files", {}).values():
                if not (run_dir / fname).exists():
                    errors.append(f"{section}: missing file {fname}")

    if errors:
        print(json.dumps({"status": "error", "errors": errors}, ensure_ascii=False, indent=2))
        return 1

    print(json.dumps({
        "status": "ok",
        "sections": {
            s: manifest_data.get(s, {}).get("status", "pending")
            for s in ("timing", "perf", "asm")
        },
    }, ensure_ascii=False, indent=2))
    return 0


def _cmd_acquire_all(args) -> int:
    rc = 0
    rc |= _cmd_acquire_timing(args)
    rc |= _cmd_acquire_perf(args)
    rc |= _cmd_acquire_asm(args)
    return rc


# ---------------------------------------------------------------------------
# backfill
# ---------------------------------------------------------------------------

def _handle_backfill(args) -> int:
    if args.backfill_command == "run":
        return _cmd_backfill_run(args)
    if args.backfill_command == "status":
        return _cmd_backfill_status(args)
    return 2


def _cmd_backfill_run(args) -> int:
    from .backfill.pipeline import run_backfill

    project_path = _resolve_project_yaml(Path(args.project))
    if not project_path.exists():
        print(f"Error: {project_path} not found", file=sys.stderr)
        return 1

    arm_dir = Path(args.arm_run_dir)
    x86_dir = Path(args.x86_run_dir)
    output = Path(args.output) if args.output else None

    return run_backfill(project_path, arm_dir, x86_dir, output)


def _cmd_backfill_status(args) -> int:
    from .config import resolve_four_layer_root

    project_path = _resolve_project_yaml(Path(args.project))
    root = resolve_four_layer_root(project_path)

    info: dict = {"project": str(project_path), "fourLayerRoot": str(root)}

    ds_dir = root / "datasets"
    ds_files = list(ds_dir.glob("*.dataset.json")) if ds_dir.is_dir() else []
    info["dataset"] = {"exists": bool(ds_files), "file": ds_files[0].name if ds_files else None}

    src_dir = root / "sources"
    src_files = list(src_dir.glob("*.source.json")) if src_dir.is_dir() else []
    info["source"] = {"exists": bool(src_files), "file": src_files[0].name if src_files else None}

    proj_dir = root / "projects"
    proj_files = list(proj_dir.glob("*.project.json")) if proj_dir.is_dir() else []
    info["project"] = {"exists": bool(proj_files), "file": proj_files[0].name if proj_files else None}

    if ds_files:
        ds = json.loads(ds_files[0].read_text(encoding="utf-8"))
        info["dataset"]["cases"] = len(ds.get("cases", []))
        info["dataset"]["functions"] = len(ds.get("functions", []))
        info["dataset"]["hasStackOverview"] = bool(ds.get("stackOverview"))

    print(json.dumps(info, ensure_ascii=False, indent=2))
    return 0


# ---------------------------------------------------------------------------
# bridge
# ---------------------------------------------------------------------------

def _handle_bridge(args) -> int:
    if args.bridge_command == "publish":
        return _cmd_bridge_publish(args)
    if args.bridge_command == "fetch":
        return _cmd_bridge_fetch(args)
    if args.bridge_command == "status":
        return _cmd_bridge_status(args)
    return 2


def _resolve_bridge_config(args):
    """Get bridge config from project.yaml, with CLI overrides."""
    from .config import load_project_config

    project_path = _resolve_project_yaml(Path(args.project))
    try:
        full_config = load_project_config(project_path)
    except (FileNotFoundError, ValueError):
        full_config = {}

    config: dict[str, Any] = full_config.get("bridge", {})

    # Resolve token from env var (don't fail — caller decides if required).
    env_var = config.get("tokenEnvVar", "PYFRAMEWORK_BRIDGE_TOKEN")
    import os
    token = os.environ.get(env_var, "")
    if token:
        config["token"] = token

    # CLI overrides.
    if hasattr(args, "repo") and args.repo:
        config["repo"] = args.repo
    if hasattr(args, "platform") and args.platform:
        config["platform"] = args.platform
    if hasattr(args, "token") and args.token:
        config["token"] = args.token

    is_dry_run = getattr(args, "dry_run", False)

    missing = []
    if not config.get("repo"):
        missing.append("bridge.repo (project.yaml or --repo)")
    if not config.get("platform"):
        missing.append("bridge.platform (project.yaml or --platform)")
    if not config.get("token") and not is_dry_run:
        missing.append("token (env var PYFRAMEWORK_BRIDGE_TOKEN or --token)")

    if missing:
        print(f"Error: missing {', '.join(missing)}", file=sys.stderr)
        return None

    return config


def _cmd_bridge_publish(args) -> int:
    from .bridge.analysis import publish

    project_path = _resolve_project_yaml(Path(args.project))
    if not project_path.exists():
        print(f"Error: {project_path} not found", file=sys.stderr)
        return 1

    config = _resolve_bridge_config(args)
    if config is None:
        return 1

    try:
        result = publish(
            project_path,
            repo=config["repo"],
            platform=config["platform"],
            token=config.get("token", ""),
            bridge_type=config.get("type", "discussion"),
            discussion_category=config.get("category", "General"),
            dry_run=args.dry_run,
            max_lines=args.max_lines,
            base_url=args.base_url,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _cmd_bridge_fetch(args) -> int:
    from .bridge.analysis import fetch

    project_path = _resolve_project_yaml(Path(args.project))
    if not project_path.exists():
        print(f"Error: {project_path} not found", file=sys.stderr)
        return 1

    config = _resolve_bridge_config(args)
    if config is None:
        return 1

    try:
        result = fetch(
            project_path,
            repo=config["repo"],
            platform=config["platform"],
            token=config["token"],
            bridge_type=config.get("type", "discussion"),
            base_url=args.base_url,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("failed", 0) == 0 else 1


def _cmd_bridge_status(args) -> int:
    from .bridge.analysis import status

    project_path = _resolve_project_yaml(Path(args.project))
    if not project_path.exists():
        print(f"Error: {project_path} not found", file=sys.stderr)
        return 1

    result = status(project_path)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


# ---------------------------------------------------------------------------
# compare (Step 6b)
# ---------------------------------------------------------------------------

def _cmd_compare(args) -> int:
    from .compare.pipeline import run_compare

    project_path = _resolve_project_yaml(Path(args.project))
    if not project_path.exists():
        print(f"Error: {project_path} not found", file=sys.stderr)
        return 1

    arm_dir = args.arm_run_dir
    x86_dir = args.x86_run_dir

    # Auto-detect run dirs from latest run.
    if not arm_dir or not x86_dir:
        runs_dir = project_path.parent / "runs"
        if not runs_dir.is_dir():
            print("Error: no runs/ directory found, specify --arm-run-dir and --x86-run-dir",
                  file=sys.stderr)
            return 1
        latest = max(runs_dir.iterdir(), key=lambda p: p.name)
        if not arm_dir:
            arm_dir = latest / "arm"
        if not x86_dir:
            x86_dir = latest / "x86"

    if not arm_dir.is_dir():
        print(f"Error: ARM run directory not found: {arm_dir}", file=sys.stderr)
        return 1
    if not x86_dir.is_dir():
        print(f"Error: x86 run directory not found: {x86_dir}", file=sys.stderr)
        return 1

    result = run_compare(
        project_path, arm_dir, x86_dir,
        output_dir=args.output_dir,
        top_n=args.top_n,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") == "completed" else 1
