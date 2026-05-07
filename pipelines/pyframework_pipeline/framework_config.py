"""Framework-specific configuration for orchestrator operations.

Abstracts differences between PyFlink and PySpark deployments.
"""

from pathlib import Path
from typing import Any

from .config import load_environment_config, load_project_config


def get_framework_id(project_path: Path) -> str:
    """Get the framework ID from project.yaml, defaulting to 'pyflink'.

    Parameters
    ----------
    project_path : Path
        Path to the resolved project.yaml file.
    """
    config = load_project_config(project_path)
    return config.get("frameworkId", "pyflink")


class FrameworkConfig:
    """Framework-specific configuration."""

    def __init__(self, framework_id: str, *, env_config: dict[str, Any] | None = None):
        self.framework_id = framework_id
        self.env_config = env_config or {}

    @property
    def master_container(self) -> str:
        """Name of the master/coordinator container."""
        if self.framework_id == "pyspark":
            software = self.env_config.get("software", {}) if isinstance(self.env_config, dict) else {}
            names = software.get("pysparkContainerNames", {}) if isinstance(software, dict) else {}
            return names.get("master", "pyspark-spark-master")
        return "flink-jm"

    def get_worker_containers(self, count: int = 2) -> list[str]:
        """Names of worker containers."""
        if self.framework_id == "pyspark":
            software = self.env_config.get("software", {}) if isinstance(self.env_config, dict) else {}
            names = software.get("pysparkContainerNames", {}) if isinstance(software, dict) else {}
            configured = names.get("workers", []) if isinstance(names, dict) else []
            if isinstance(configured, list) and configured:
                return configured[:count]
            return [f"pyspark-spark-worker-{i}" for i in range(1, count + 1)]
        return [f"flink-tm{i}" for i in range(1, count + 1)]

    @property
    def master_workdir(self) -> str:
        """Working directory path in the master container."""
        if self.framework_id == "pyspark":
            return "/opt/spark/apps"
        return "/opt/flink/usrlib"

    @property
    def worker_workdir(self) -> str:
        """Working directory path in worker containers."""
        if self.framework_id == "pyspark":
            return "/opt/spark/apps"
        return "/opt/flink/usrlib"

    @property
    def lang_env_root(self) -> str:
        """Remote lang_env project root for PySpark execution."""
        software = self.env_config.get("software", {}) if isinstance(self.env_config, dict) else {}
        if self.framework_id == "pyspark":
            return software.get("langEnvRoot", "")
        return ""

    def benchmark_command(
        self,
        python_bin: str,
        workdir: str,
        query: str,
        rows: int,
        perf_wrapper: str | None = None,
    ) -> str:
        """Generate the benchmark execution command for this framework."""
        if self.framework_id == "pyspark":
            cmd = f"{python_bin} {workdir}/collect_results.py --query {query} --rows {rows}"
            return cmd
        else:
            cmd = f"{python_bin} {workdir}/benchmark_runner.py --query {query} --rows {rows}"
            if perf_wrapper:
                cmd += f" --python-executable {perf_wrapper}"
            return cmd

    def get_perf_data_path(self) -> str:
        """Path to the perf.data file in containers."""
        if self.framework_id == "pyspark":
            return "/tmp/perf-worker.data"
        return "/tmp/perf-udf.data"


def get_framework_config(project_path: Path) -> FrameworkConfig:
    """Load framework-specific configuration from project."""
    framework_id = get_framework_id(project_path)
    env_config = load_environment_config(project_path)
    return FrameworkConfig(framework_id, env_config=env_config)
