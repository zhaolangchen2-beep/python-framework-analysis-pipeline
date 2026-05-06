from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def resolve_four_layer_root(path: Path) -> Path:
    if path.is_dir():
        return path

    if path.name != "project.yaml":
        raise ValueError(f"Unsupported project config: {path}")

    config = parse_simple_yaml(path)
    root_value = config.get("fourLayerRoot")
    if not root_value:
        raise ValueError(f"{path} is missing fourLayerRoot")

    return (path.parent / root_value).resolve()


def parse_simple_yaml(path: Path) -> dict[str, str]:
    """Extract top-level flat key:value pairs from a YAML file."""
    result: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        if value.strip():
            result[key.strip()] = value.strip().strip('"').strip("'")
    return result


def load_project_config(path: Path) -> dict[str, Any]:
    """Load project.yaml with full nested YAML support."""
    if not path.exists():
        raise FileNotFoundError(f"Project config not found: {path}")
    text = path.read_text(encoding="utf-8")
    from .environment.parser import parse_yaml
    return parse_yaml(text)


def get_bridge_config(project_path: Path) -> dict[str, Any]:
    """Read bridge configuration from project.yaml + resolve token from env."""
    config = load_project_config(project_path)
    bridge = config.get("bridge", {})
    if not bridge.get("repo"):
        raise ValueError(
            "project.yaml missing bridge.repo. "
            "Add: bridge:\n  repo: owner/repo\n  platform: github|gitcode"
        )
    if not bridge.get("platform"):
        raise ValueError("project.yaml missing bridge.platform (github or gitcode)")
    env_var = bridge.get("tokenEnvVar", "PYFRAMEWORK_BRIDGE_TOKEN")
    token = os.environ.get(env_var, "")
    bridge["token"] = token
    if not token:
        raise ValueError(
            f"Bridge token not set. Run: export {env_var}=<your-api-token>"
        )
    return bridge


def get_workload_config(project_path: Path) -> dict[str, Any]:
    """Read workload configuration from project.yaml."""
    config = load_project_config(project_path)
    workload = config.get("workload", {})
    if not workload.get("localDir"):
        raise ValueError(
            "project.yaml missing workload.localDir. "
            "Add: workload:\n  localDir: workload/tpch/pyflink"
        )
    return workload


def get_run_config(project_path: Path) -> dict[str, Any]:
    """Read run configuration from project.yaml."""
    config = load_project_config(project_path)
    run = config.get("run", {})
    platforms = run.get("platforms", [])
    if not platforms:
        raise ValueError(
            "project.yaml missing run.platforms. "
            "Add: run:\n  platforms: [arm, x86]"
        )
    return run


def load_environment_config(project_path: Path) -> dict[str, Any]:
    """Load environment.yaml sibling to project.yaml."""
    env_path = project_path.parent / "environment.yaml"
    if not env_path.exists():
        raise FileNotFoundError(f"environment.yaml not found at {env_path}")
    from .environment.parser import load_environment_yaml
    return load_environment_yaml(env_path)


def validate_pipeline_config(
    project_path: Path,
    *,
    require_bridge_token: bool = True,
) -> dict[str, Any]:
    """Validate project configuration before any remote pipeline work starts."""

    issues: list[dict[str, str]] = []

    def add(path: str, message: str) -> None:
        issues.append({"path": path, "message": message})

    project_id = ""
    project_config: dict[str, Any] = {}
    if not project_path.exists():
        add("project.yaml", f"project.yaml not found: {project_path}")
    elif project_path.name != "project.yaml":
        add("project.yaml", f"expected project.yaml, got {project_path.name}")
    else:
        try:
            project_config = load_project_config(project_path)
            project_id = str(project_config.get("id", ""))
            if not project_id:
                add("project.id", "project.yaml missing id")
        except Exception as exc:
            add("project.yaml", f"failed to parse project.yaml: {exc}")

    root: Path | None = None
    if project_config:
        root_value = project_config.get("fourLayerRoot")
        if not root_value:
            add("fourLayerRoot", "project.yaml missing fourLayerRoot")
        else:
            root = (project_path.parent / str(root_value)).resolve()
            if not root.exists():
                add("fourLayerRoot", f"fourLayerRoot does not exist: {root}")
            else:
                # Skip four-layer validation if data hasn't been generated yet.
                has_data = (
                    any((root / "datasets").glob("*.dataset.json"))
                    and any((root / "sources").glob("*.source.json"))
                )
                if has_data:
                    from .validators.four_layer import validate_four_layer_project
                    report = validate_four_layer_project(project_path)
                    if report.status != "ok":
                        for err in report.errors:
                            add("fourLayerRoot", f"[{err.code}] {err.message}")

    env_path = project_path.parent / "environment.yaml"
    env_config: dict[str, Any] = {}
    if not env_path.exists():
        add("environment.yaml", f"environment.yaml not found at {env_path}")
    else:
        try:
            from .environment.parser import load_environment_yaml
            env_config = load_environment_yaml(env_path)
        except Exception as exc:
            add("environment.yaml", f"failed to parse environment.yaml: {exc}")

    workload = project_config.get("workload", {}) if project_config else {}
    local_dir = workload.get("localDir") if isinstance(workload, dict) else None
    if not local_dir:
        add("workload.localDir", "project.yaml missing workload.localDir")
    else:
        workload_path = (project_path.parent / str(local_dir)).resolve()
        if not workload_path.exists():
            add("workload.localDir", f"workload.localDir does not exist: {workload_path}")

    run = project_config.get("run", {}) if project_config else {}
    platforms = run.get("platforms", []) if isinstance(run, dict) else []
    if not platforms:
        add("run.platforms", "project.yaml missing run.platforms")

    if env_config:
        env_platforms = {
            str(p.get("id")): p for p in env_config.get("platforms", [])
        }
        host_refs = env_config.get("hostRefs", {})
        for platform in platforms:
            platform_id = str(platform)
            platform_config = env_platforms.get(platform_id)
            if platform_config is None:
                add(
                    f"run.platforms.{platform_id}",
                    f"platform {platform_id} not found in environment.yaml",
                )
                continue
            for host_entry in platform_config.get("hosts", []):
                host_ref = host_entry.get("hostRef")
                if host_ref not in host_refs:
                    add(
                        f"environment.platforms.{platform_id}.hosts",
                        f"hostRef {host_ref} not found in environment.yaml hostRefs",
                    )

        software = env_config.get("software", {})

        # Determine framework and select appropriate image field name
        framework_id = str(project_config.get("frameworkId", "pyflink")) if project_config else "pyflink"
        if framework_id == "pyspark":
            framework_images = software.get("sparkImages", {})
            images_field = "sparkImages"
        else:
            framework_images = software.get("flinkPyflinkImages", {})
            images_field = "flinkPyflinkImages"

        if not framework_images:
            add(
                f"software.{images_field}",
                f"environment.yaml missing software.{images_field}",
            )
        else:
            for platform in platforms:
                if str(platform) not in framework_images:
                    add(
                        f"software.{images_field}.{platform}",
                        f"missing {framework_id} image for platform {platform}",
                    )

    bridge = project_config.get("bridge", {}) if project_config else {}
    if not isinstance(bridge, dict) or not bridge.get("repo"):
        add("bridge.repo", "project.yaml missing bridge.repo")
    if not isinstance(bridge, dict) or not bridge.get("platform"):
        add("bridge.platform", "project.yaml missing bridge.platform")
    if require_bridge_token:
        token_env = bridge.get("tokenEnvVar", "PYFRAMEWORK_BRIDGE_TOKEN") if isinstance(bridge, dict) else "PYFRAMEWORK_BRIDGE_TOKEN"
        token = os.environ.get(str(token_env), "")
        if not token:
            add("bridge.token", f"Bridge token not set: export {token_env}=<your-api-token>")
        elif _is_placeholder_token(token):
            add("bridge.token", f"Bridge token in {token_env} looks like a placeholder")

    return {
        "status": "ok" if not issues else "error",
        "projectId": project_id,
        "project": str(project_path),
        "fourLayerRoot": str(root) if root else "",
        "issueCount": len(issues),
        "issues": issues,
    }


def _is_placeholder_token(token: str) -> bool:
    normalized = token.strip().lower()
    if not normalized:
        return True
    placeholders = {
        "fake",
        "fake-token",
        "dummy",
        "dummy-token",
        "test",
        "test-token",
        "token",
        "your-api-token",
        "<your-api-token>",
        "changeme",
        "change-me",
        "placeholder",
    }
    return normalized in placeholders or normalized.startswith("fake-")
