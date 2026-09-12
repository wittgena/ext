# xphi.watcher.plane.phase.topos
import os
import yaml
import asyncio
import shutil
import httpx
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Any, Optional, Type, Tuple

from xphi.watcher.tracer.infra.topos import ContainerStateAuditor, EntropyAuditor, UniversalLogAuditor
from xphi.watcher.tracer.infra.router import InfraRouter
from xphi.watcher.plane.emitter import get_emitter, flow_scope
from xphi.watcher.tracer.bound import SystemBound

log = get_emitter("plane.topos")

class ToposBlueprint:
    @staticmethod
    def get_cluster_spec(workspace_dir: str) -> Dict[str, Any]:
        return {
            "services": {
                # 1. 분산망 상태 동기화 및 메시지 큐 (Redis)
                "fiber-tunnel": {
                    "image": "redis:7-alpine",
                    "container_name": "fiber-tunnel",
                    "ports": ["6379:6379"],
                    "healthcheck": {"test": ["CMD", "redis-cli", "ping"], "interval": "3s", "retries": 5}
                },
                
                # 2. 엣지 진입점 (Gateway)
                "fiber-gateway": {
                    "build": {
                        "context": workspace_dir,
                        "dockerfile": "Dockerfile"
                    },
                    "container_name": "fiber-gateway",
                    "tmpfs": [
                        "/root/.anchor"  # USER 모드 샌드박스
                    ],
                    "environment": [
                        "REDIS_URL=redis://fiber-tunnel:6379/0", 
                        "NODE_PROFILE=EDGE"
                    ],
                    "command": ["fiber", "daemon", "--start", "rest_edge,gateway_edge"],
                    "ports": ["8000:8000"],
                    "depends_on": {"fiber-tunnel": {"condition": "service_healthy"}}
                },
                
                # 3. 분산 워커 노드 A (Multiplex Oracle)
                "fiber-worker-oracle": {
                    "build": {
                        "context": workspace_dir,
                        "dockerfile": "Dockerfile"
                    },
                    "container_name": "fiber-worker-oracle",
                    "tmpfs": ["/root/.anchor"],
                    "environment": [
                        "REDIS_URL=redis://fiber-tunnel:6379/0", 
                        "NODE_PROFILE=COMPUTE"
                    ],
                    "command": ["fiber", "connect", "--target", "oracle-01", "--exec", "agent.oracle"],
                    "depends_on": {"fiber-tunnel": {"condition": "service_healthy"}}
                },
                
                # 4. 분산 워커 노드 B (Linear Finlib)
                "fiber-worker-finlib": {
                    "build": {
                        "context": workspace_dir,
                        "dockerfile": "Dockerfile"
                    },
                    "container_name": "fiber-worker-finlib",
                    "tmpfs": ["/root/.anchor"],
                    "environment": [
                        "REDIS_URL=redis://fiber-tunnel:6379/0", 
                        "NODE_PROFILE=COMPUTE"
                    ],
                    "command": ["fiber", "connect", "--target", "finlib-01", "--exec", "python -m fiber.dphi.worker.mcp.finlib"],
                    "depends_on": {"fiber-tunnel": {"condition": "service_healthy"}}
                }
            }
        }

class BaseToposAdapter:
    async def apply(self, spec: Dict[str, Any]) -> bool:
        raise NotImplementedError
    async def teardown(self) -> None:
        raise NotImplementedError

class DockerComposeAdapter(BaseToposAdapter):
    def __init__(self, workspace: Path, boundary: SystemBound):
        self.workspace = workspace
        self.compose_file = self.workspace / "docker-compose.yml"
        self.boundary = boundary

    async def apply(self, spec: Dict[str, Any]) -> bool:
        self.workspace.mkdir(parents=True, exist_ok=True)
        with open(self.compose_file, "w") as f:
            yaml.dump(spec, f, sort_keys=False)
            
        log.info(f"[Adapter:Compose] Manifest materialized. Igniting Remote-Dist Hologram cluster...")
        cmd = ["docker-compose", "-f", str(self.compose_file), "up", "--build", "-d"]
        code, out, err = await self.boundary.run_command(cmd, cwd=str(self.workspace), capture=True)
        if code != 0:
            log.error(f"[Adapter:Compose] Apply failed: {err}")
            return False
        return True

    async def teardown(self) -> None:
        if self.compose_file.exists():
            log.info("[Adapter:Compose] Gracefully collapsing physical manifolds...")
            cmd = ["docker-compose", "-f", str(self.compose_file), "down", "-v", "--remove-orphans"]
            await self.boundary.run_command(cmd, cwd=str(self.workspace), capture=False)

class KomposeK3sAdapter(BaseToposAdapter):
    def __init__(self, workspace: Path, boundary: SystemBound, namespace: str):
        self.workspace = workspace
        self.namespace = namespace
        self.compose_file = self.workspace / "docker-compose.yml"
        self.k8s_manifest = self.workspace / "k8s-manifests.yaml"
        self.boundary = boundary

    async def apply(self, spec: Dict[str, Any]) -> bool:
        self.workspace.mkdir(parents=True, exist_ok=True)
        
        with open(self.compose_file, "w") as f:
            yaml.dump(spec, f, sort_keys=False)
            
        log.info("[Adapter:K3s] Translating Blueprint via Kompose...")
        convert_cmd = ["kompose", "convert", "-f", str(self.compose_file), "-o", str(self.k8s_manifest)]
        code, _, err = await self.boundary.run_command(convert_cmd, cwd=str(self.workspace), capture=True)
        if code != 0:
            log.error(f"[Adapter:K3s] Kompose translation failed: {err}")
            return False

        await self.boundary.run_command(["kubectl", "create", "namespace", self.namespace], capture=True) 
        
        log.info(f"[Adapter:K3s] Injecting Topology into Kube Namespace '{self.namespace}'...")
        apply_cmd = ["kubectl", "apply", "-f", str(self.k8s_manifest), "-n", self.namespace]
        code, _, err = await self.boundary.run_command(apply_cmd, cwd=str(self.workspace), capture=True)
        
        if code != 0:
            log.error(f"[Adapter:K3s] Kubectl apply failed: {err}")
            return False
        return True

    async def teardown(self) -> None:
        if self.k8s_manifest.exists():
            log.info(f"[Adapter:K3s] Eradicating topology from namespace '{self.namespace}'...")
            cmd = ["kubectl", "delete", "-f", str(self.k8s_manifest), "-n", self.namespace, "--ignore-not-found=true"]
            await self.boundary.run_command(cmd, cwd=str(self.workspace), capture=False)

@dataclass
class ToposContext:
    router: InfraRouter
    boundary: SystemBound
    auditors: Dict[str, Any]

class ToposOrchestrator:
    def __init__(self, target_name: str = "dphi-topos-sandbox", mode: str = "dev", 
                 infra_type: str = "compose", namespace: str = "fiber-topos",
                 timeout: int = 120, suites: Dict[str, Type] = None):
        self.worker_name = target_name
        self.mode = mode
        self.infra_type = infra_type
        self.namespace = namespace
        self.timeout = timeout
        self.suites = suites or {}
        self.keep_workspace = False  
        
        self.workspace = Path(f"/tmp/fiber_topos_{self.mode}_{self.infra_type}")
        self.boundary = SystemBound()
        
        if self.infra_type == "kube":
            self.adapter = KomposeK3sAdapter(self.workspace, self.boundary, self.namespace)
        else:
            self.adapter = DockerComposeAdapter(self.workspace, self.boundary)
        
        self.router = InfraRouter(host_url="http://localhost:8000")
        
        self.gateway_state = ContainerStateAuditor("fiber-gateway", self.boundary, infra_type=self.infra_type, namespace=self.namespace)
        self.compute_entropy = EntropyAuditor("fiber-worker-oracle", self.boundary, infra_type=self.infra_type, namespace=self.namespace)
        self.gateway_logs = UniversalLogAuditor("fiber-gateway", "boot_check", self.boundary, infra_type=self.infra_type, namespace=self.namespace)

    def _generate_dynamic_dockerfile(self, git_ref="main"):
        """
        @desc: 로컬 디렉토리 참조를 버리고, Github에서 직접 fiber를 받아와 빌드하는 
        순수 원격(Direct Remote) 배포 환경을 동적으로 굽습니다.
        """
        dockerfile_path = self.workspace / "Dockerfile"
        dockerfile_content = f"""\
FROM python:3.11-slim
RUN apt-get update && apt-get install -y git gcc g++ make && rm -rf /var/lib/apt/lists/*
WORKDIR /home/fiber
RUN pip install --no-cache-dir git+https://github.com/wittgena/fiber.git@{git_ref}
"""
        dockerfile_path.write_text(dockerfile_content)
        log.info(f"  [BUILDER] Dynamic Dockerfile generated targeting Github (ref: {git_ref}) at {dockerfile_path}")

    async def _verify_resonance(self) -> bool:
        log.info(f"\n>>> [PHASE] Stabilizing Topology & Verifying Resonance ({self.infra_type.upper()}) <<<")
        
        wait_count = 0
        is_network_ready = False
        
        while wait_count < self.timeout:
            if not self.gateway_state.is_running:
                log.error(f"  [CRASH] Gateway container collapsed unexpectedly. (Exit: {self.gateway_state.exit_code})")
                return False

            if not is_network_ready:
                try:
                    health_url = f"{self.router.host_url}/keys"
                    headers = self.router.build_headers()
                    async with httpx.AsyncClient() as client:
                        res = await client.get(health_url, headers=headers, timeout=2.0)
                        if res.status_code == 200:
                            log.info("  [NETWORK] Gateway Ingress Accessibility: PASSED ✅")
                            is_network_ready = True
                except Exception:
                    pass 

            if is_network_ready and self.compute_entropy.last_cpu_usage < 80.0:
                log.info(f"  [ENTROPY] Compute Node CPU stabilized at {self.compute_entropy.last_cpu_usage}% ✅")
                log.crit(f"[{self.worker_name}] ✅ Topology Resonance Confirmed! Cluster is fully operational.")
                return True
            
            await asyncio.sleep(2)
            wait_count += 2
            
        log.error("  [TIMEOUT] Cluster failed to reach stable resonance in time.")
        return False

    async def _run_all_suites(self, broker: Any, context: ToposContext) -> int:
        total_fails = 0
        for suite_name, suite_cls in self.suites.items():
            log.info(f"\n>>> [PHASE] Starting Integration Suite: {suite_name.upper()} <<<")
            try:
                suite_instance = suite_cls(broker=broker, context=context)
                await suite_instance.run_all()
                total_fails += getattr(suite_instance, 'fail_count', 0)
            except Exception as e:
                log.error(f"[ERROR] Suite '{suite_name}' crashed: {e}", exc_info=True)
                total_fails += 1
        return total_fails

    async def execute(self, broker: Any = None) -> Tuple[bool, str]:
        log.info(f"\n--- [START] Orchestrating Topology ({self.mode.upper()} | {self.infra_type.upper()}) ---")
        
        try:
            # 1. 인프라 프로비저닝 준비
            self.workspace.mkdir(parents=True, exist_ok=True)
            self._generate_dynamic_dockerfile(git_ref="main") # Github Target
            
            # Blueprint 병합 (로컬 참조를 완전히 배제하고 workspace 경로만 전달)
            spec = ToposBlueprint.get_cluster_spec(str(self.workspace))
            success = await self.adapter.apply(spec)
            if not success:
                return False, f"Failed to apply physical topology via {self.infra_type} adapter."
            
            # 2. 다차원 센서 부착
            self.gateway_state.attach()
            self.compute_entropy.attach()
            if self.gateway_logs: 
                self.gateway_logs.attach()
            
            # 3. 안정화 및 네트워크 검증
            with flow_scope(phase="RESONANCE_CHECK"):
                is_stable = await self._verify_resonance()
                if not is_stable:
                    return False, "Topology failed multidimensional verification."
            
            # 4. 비즈니스 로직(E2E Test) 실행
            if self.suites:
                with flow_scope(phase="TEST_EXECUTION"):
                    test_context = ToposContext(
                        router=self.router,
                        boundary=self.boundary,
                        auditors={
                            "state": self.gateway_state,
                            "entropy": self.compute_entropy,
                            "log": self.gateway_logs
                        }
                    )
                    total_fails = await self._run_all_suites(broker, test_context)
                    if total_fails > 0:
                        return False, f"Topology verified, but {total_fails} logical E2E tests failed."
            
            return True, ""
            
        except Exception as e:
            log.error(f"[FATAL] Orchestration crashed: {e}")
            return False, str(e)
            
        finally:
            log.info("\n[SYSTEM] Triggering Teardown Sequence...")
            self.gateway_state.detach()
            self.compute_entropy.detach()
            if self.gateway_logs: 
                self.gateway_logs.detach()
                
            # 💡 Keep-Workspace 옵션 보존
            if not self.keep_workspace:
                await self.adapter.teardown()
                if self.workspace.exists():
                    shutil.rmtree(self.workspace, ignore_errors=True)
                log.info("[SYSTEM] All manifolds collapsed and cleaned up.")
            else:
                log.warning(f"[SYSTEM] ⚠️ Keep-Workspace is ON. Inspect logs via 'docker logs fiber-gateway'.")