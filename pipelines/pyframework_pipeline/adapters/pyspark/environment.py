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
        registry = software.get("dockerRegistry", "")
        if registry:
            image = f"{registry}/{image}"
        network = software.get("containerNetwork", DEFAULT_NETWORK)
        worker_count = DEFAULT_WORKER_COUNT
        arch = platform_config.get("arch", "x86_64")

        # Determine the host
        hosts_by_role = {}
        for host_entry in platform_config.get("hosts", []):
            hosts_by_role[host_entry["role"]] = host_entry["hostRef"]

        master_host = hosts_by_role.get("master", hosts_by_role.get("client", ""))
        host_alias = host_refs.get(master_host, {}).get("alias", master_host)
        raw_host_env = host_refs.get(master_host, {}).get("env", {})
        host_env = raw_host_env if isinstance(raw_host_env, dict) else {}

        # Build docker exec proxy flags
        _proxy_vars = ["http_proxy", "https_proxy", "no_proxy", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY"]
        docker_proxy_flags = " ".join(
            f"-e {k}={v}" for k, v in host_env.items() if k in _proxy_vars and v
        )
        python_version = software.get("pythonVersion", "3.11")

        # Step 0: Build Spark+PySpark image (skip if already exists)
        build_script = "adapters/pyspark/scripts/build-spark-image.sh"
        base_image = software.get("sparkImage", DEFAULT_IMAGE)
        if registry:
            base_image = f"{registry}/{base_image}"
        build_env = (
            f"IMAGE_NAME={image} BASE_IMAGE={base_image} "
            f"NETWORK={network} PYTHON_VERSION={python_version} "
            f"WORKER_COUNT={worker_count}"
        )
        steps.append(PlanStep(
            id="build-spark-image",
            kind="build",
            hostRef=master_host,
            command=(
                f"docker image inspect {image} >/dev/null 2>&1 "
                f"|| {build_env} bash /tmp/build-spark-image.sh {arch}"
            ),
            description=f"Build Spark+PySpark image on {host_alias} (~30 min first run)",
            mutatesHost=True,
            requiresApproval=True,
            rollbackHint=f"docker rmi {image}",
            scriptPath=build_script,
            timeout=2400,  # ~40 min
        ))

        # Step 1: Create Docker network
        steps.append(PlanStep(
            id="create-network",
            kind="prepare",
            hostRef=master_host,
            command=f"docker network create {network} 2>/dev/null || true",
            description=f"Create Docker network '{network}' on {host_alias}",
        ))

        # Step 2: Start Master
        master_run_args = (
            f"docker run -d --name pyspark-spark-master --network {network} "
            f"-p 8080:8080 -p 7077:7077 --privileged {image} "
            f"/opt/spark/bin/spark-class org.apache.spark.deploy.master.Master"
        )
        steps.append(PlanStep(
            id="start-master",
            kind="framework-start",
            hostRef=master_host,
            command=_docker_reconcile_container(
                name="pyspark-spark-master",
                image=image,
                run_args=master_run_args,
            ),
            description=f"Start Spark Master container on {host_alias}",
            mutatesHost=True,
            requiresApproval=True,
            rollbackHint="docker rm -f pyspark-spark-master",
        ))

        # Step 3: Start Workers
        for i in range(1, worker_count + 1):
            steps.append(PlanStep(
                id=f"start-worker-{i}",
                kind="framework-start",
                hostRef=master_host,
                command=_docker_reconcile_container(
                    name=f"pyspark-spark-worker-{i}",
                    image=image,
                    run_args=(
                        f"docker run -d --name pyspark-spark-worker-{i} --network {network} "
                        f"--privileged {image} "
                        f"/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker "
                        f"spark://pyspark-spark-master:7077"
                    ),
                ),
                description=f"Start Worker {i} container on {host_alias}",
                mutatesHost=True,
                requiresApproval=True,
                rollbackHint=f"docker rm -f pyspark-spark-worker-{i}",
            ))

        # Step 4: Readiness — ensure cluster running + check Web UI
        worker_names = " ".join(f"pyspark-spark-worker-{i}" for i in range(1, worker_count + 1))
        steps.append(PlanStep(
            id="readiness-cluster-health",
            kind="framework-readiness",
            hostRef=master_host,
            command=(
                f"docker exec pyspark-spark-master curl -sf http://localhost:8080/json/ >/dev/null || "
                f"{{ echo 'Spark not responding, restarting cluster...'; "
                f"docker restart pyspark-spark-master {worker_names}; sleep 10; "
                f"docker exec pyspark-spark-master curl -sf http://localhost:8080/json/; }}"
            ),
            description=f"Start cluster if stopped, then check health on {host_alias}",
            timeout=120,
        ))

        # Step 5: Readiness — verify worker count
        steps.append(PlanStep(
            id="readiness-worker-count",
            kind="framework-smoke-test",
            hostRef=master_host,
            command=(
                f"for i in \\$(seq 1 10); do "
                f"count=\\$(docker exec pyspark-spark-master curl -sf "
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

        # Step 6: Verify profiling tools (installed during image build)
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

            for name in ["pyspark-spark-master"] + [f"pyspark-spark-worker-{i}" for i in range(1, worker_count + 1)]:
                steps.append(PlanStep(
                    id=f"verify-profiling-tools-{name}",
                    kind="framework-readiness",
                    hostRef=master_host,
                    command=(
                        f"docker exec {name} bash -c "
                        f"'dpkg -s {pkg_str} >/dev/null 2>&1 "
                        f"|| echo WARNING: profiling tools not in image, rebuild needed'"
                    ),
                    description=f"Verify profiling tools in {name} on {host_alias}",
                    required=False,
                ))

            # Step 7: Verify profiling tools availability
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
                command=f"docker exec pyspark-spark-master bash -c '{verifications}'",
                description=f"Verify profiling tools available on {host_alias}",
            ))

            # Step 8: Enable perf_event_paranoid on host
            if "perf" in profiling_tools:
                steps.append(PlanStep(
                    id="enable-perf-paranoid",
                    kind="prepare",
                    hostRef=master_host,
                    command="sudo sysctl -w kernel.perf_event_paranoid=0",
                    description=f"Set kernel.perf_event_paranoid=0 on {host_alias}",
                    requiresPrivilege=True,
                    requiresApproval=True,
                    rollbackHint="sudo sysctl -w kernel.perf_event_paranoid=2",
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
