"""Framework-specific configuration for orchestrator operations.

Abstracts differences between PyFlink and PySpark deployments.
"""

from pathlib import Path
from typing import Any

from .config import load_project_config


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

    def __init__(self, framework_id: str):
        self.framework_id = framework_id

    @property
    def master_container(self) -> str:
        """Name of the master/coordinator container."""
        if self.framework_id == "pyspark":
            return "pyspark-spark-master"
        return "flink-jm"

    def get_worker_containers(self, count: int = 2) -> list[str]:
        """Names of worker containers."""
        if self.framework_id == "pyspark":
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
            # For PySpark, use collect_results.py wrapper
            cmd = f"{python_bin} {workdir}/collect_results.py --query {query} --rows {rows}"
            # Note: perf is not directly wrapped here; it's collected via spark logs
            return cmd
        else:
            # PyFlink: use benchmark_runner.py with optional perf wrapper
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
    return FrameworkConfig(framework_id)
