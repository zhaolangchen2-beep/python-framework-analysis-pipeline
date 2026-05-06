"""SSH executor factory: build SshExecutor instances from environment.yaml hostRefs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .acquisition.ssh_executor import SshExecutor


def build_executor(host_ref_id: str, env_config: dict[str, Any]) -> SshExecutor:
    """Build an SshExecutor for a specific hostRef from environment.yaml config.

    Parameters
    ----------
    host_ref_id : str
        The hostRef ID (e.g. "kunpeng", "zen5").
    env_config : dict
        Parsed environment.yaml config.

    Returns
    -------
    SshExecutor
        Configured executor for the given host.
    """
    host_refs = env_config.get("hostRefs", {})
    ref = host_refs.get(host_ref_id, {})
    if not ref:
        raise ValueError(
            f"hostRef '{host_ref_id}' not found in environment.yaml. "
            f"Available: {list(host_refs.keys())}"
        )

    alias = ref.get("alias", host_ref_id)
    user = ref.get("user", "")
    key = ref.get("key")
    port = ref.get("port", 22)
    env = ref.get("env", {})
    if not isinstance(env, dict):
        env = {}

    return SshExecutor(
        host=alias,
        user=user,
        key=Path(key) if key else None,
        port=int(port),
        env=env,
    )


def build_executors_per_host(
    env_config: dict[str, Any],
) -> dict[str, SshExecutor]:
    """Build SshExecutor instances for all hostRefs.

    Returns
    -------
    dict[str, SshExecutor]
        Mapping of hostRef ID to SshExecutor.
    """
    host_refs = env_config.get("hostRefs", {})
    return {
        ref_id: build_executor(ref_id, env_config)
        for ref_id in host_refs
    }


def get_platform_host_ref(
    env_config: dict[str, Any],
    platform_id: str,
    role: str = "jobmanager",
) -> str:
    """Resolve which hostRef a platform uses for a given role.

    Parameters
    ----------
    env_config : dict
        Parsed environment.yaml.
    platform_id : str
        Platform ID (e.g. "arm", "x86").
    role : str
        Role to look up (e.g. "client", "jobmanager", "taskmanager").

    Returns
    -------
    str
        The hostRef ID.
    """
    for plat in env_config.get("platforms", []):
        if plat.get("id") == platform_id:
            for host_entry in plat.get("hosts", []):
                if host_entry.get("role") == role:
                    return host_entry.get("hostRef", "")
            # Fallback to first host
            hosts = plat.get("hosts", [])
            if hosts:
                return hosts[0].get("hostRef", "")
    raise ValueError(f"Platform '{platform_id}' not found in environment.yaml")
