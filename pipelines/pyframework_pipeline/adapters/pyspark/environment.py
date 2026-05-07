"""PySpark environment adapter.

Declares the framework-specific steps needed to set up a PySpark analysis
environment in Docker containers (1 master + N workers), with readiness
verification via the Spark Web UI.
"""

from __future__ import annotations

from typing import Any

from pyframework_pipeline.environment.planning import PlanStep

DEFAULT_IMAGE = "spark:3.5.0-py311"
DEFAULT_NETWORK = "spark-network"
DEFAULT_WORKER_COUNT = 2


class PySparkEnvironmentAdapter:
    """Generates PySpark-specific environment plan steps.

    Assumes a containerised deployment: Spark runs in Docker containers,
    the host only needs Docker. No Java/Python/pip on the host.
    """

    framework_id = "pyspark"

    def get_plan_steps(
        self,
        platform: str,
        platform_config: dict[str, Any],
        software: dict[str, Any],
        host_refs: dict[str, Any],
    ) -> list[PlanStep]:
        """Return framework-specific plan steps for PySpark."""
        steps: list[PlanStep] = []

        image = software.get("sparkImages", {}).get(
            platform,
            software.get("sparkImage", DEFAULT_IMAGE),
        )
        worker_count = DEFAULT_WORKER_COUNT

        # Determine the host
        hosts_by_role = {}
        for host_entry in platform_config.get("hosts", []):
            hosts_by_role[host_entry["role"]] = host_entry["hostRef"]

        master_host = hosts_by_role.get("master", hosts_by_role.get("client", ""))
        host_alias = host_refs.get(master_host, {}).get("alias", master_host)

        container_names = software.get("pysparkContainerNames", {}) if isinstance(software, dict) else {}
        master_container = container_names.get("master", "pyspark-spark-master")
        worker_containers = container_names.get("workers", []) if isinstance(container_names, dict) else []
        if not isinstance(worker_containers, list) or not worker_containers:
            worker_containers = [f"pyspark-spark-worker-{i}" for i in range(1, worker_count + 1)]

        # Step 0: Verify expected containers already exist
        expected_containers = [master_container] + worker_containers[:worker_count]
        for name in expected_containers:
            steps.append(PlanStep(
                id=f"check-container-{name}",
                kind="framework-readiness",
                hostRef=master_host,
                command=f"docker inspect {name} >/dev/null 2>&1",
                description=f"Verify existing container {name} on {host_alias}",
                timeout=30,
            ))

        # Step 1: Readiness — verify Spark master is responding
        steps.append(PlanStep(
            id="readiness-cluster-health",
            kind="framework-readiness",
            hostRef=master_host,
            command=f"docker exec {master_container} curl -sf http://localhost:8080/json/ >/dev/null",
            description=f"Verify Spark master health on {host_alias}",
            timeout=120,
        ))

        # Step 2: Readiness — verify worker count
        steps.append(PlanStep(
            id="readiness-worker-count",
            kind="framework-smoke-test",
            hostRef=master_host,
            command=(
                f"for i in \\$(seq 1 10); do "
                f"count=\\$(docker exec {master_container} curl -sf "
                f"http://localhost:8080/json/ | "
                f"python3 -c 'import sys,json; "
                f"d=json.load(sys.stdin); print(len(d.get(\"workers\",[])))'); "
                f"if [ \"\\$count\" -ge {worker_count} ] 2>/dev/null; then "
                f"echo \"Workers registered: \\$count\"; exit 0; fi; "
                f"sleep 3; done; "
                f"echo \"Only \\$count/{worker_count} workers registered\"; exit 1"
            ),
            description=f"Verify {worker_count} workers registered on {host_alias}",
            timeout=60,
        ))

        # Step 3: Verify profiling tools in existing containers
        profiling_tools = software.get("profilingTools", [])
        if profiling_tools:
            tool_packages = {
                "perf": "linux-tools-generic",
                "strace": "strace",
                "objdump": "binutils",
                "gdb": "gdb",
                "readelf": "binutils",
            }
            packages = sorted({tool_packages.get(t, t) for t in profiling_tools})
            pkg_str = " ".join(packages)

            for name in expected_containers:
                steps.append(PlanStep(
                    id=f"verify-profiling-tools-{name}",
                    kind="framework-readiness",
                    hostRef=master_host,
                    command=(
                        f"docker exec {name} bash -c "
                        f"'dpkg -s {pkg_str} >/dev/null 2>&1 "
                        f"|| echo WARNING: profiling tools not in image'"
                    ),
                    description=f"Verify profiling tools in {name} on {host_alias}",
                    required=False,
                ))

            verify_cmds = {
                "perf": "perf --version",
                "strace": "strace --version",
                "objdump": "objdump --version",
                "gdb": "gdb --version",
                "readelf": "readelf --version",
            }
            verifications = " && ".join(
                verify_cmds[t] for t in profiling_tools if t in verify_cmds
            )
            steps.append(PlanStep(
                id="verify-profiling-tools",
                kind="framework-readiness",
                hostRef=master_host,
                command=f"docker exec {master_container} bash -c '{verifications}'",
                description=f"Verify profiling tools available on {host_alias}",
            ))

        return steps


def _docker_reconcile_container(name: str, image: str, run_args: str) -> str:
    return (
        f"if docker inspect {name} >/dev/null 2>&1; then "
        f"current=$(docker inspect -f '{{{{.Config.Image}}}}' {name}); "
        f"if [ \"$current\" = \"{image}\" ]; then "
        f"state=$(docker inspect -f '{{{{.State.Running}}}}' {name}); "
        f"if [ \"$state\" = \"true\" ]; then "
        f"echo {name} already running with {image}; "
        f"else docker start {name}; fi; "
        f"else docker rm -f {name} && {run_args}; fi; "
        f"else {run_args}; fi"
    )
