# xphi.watcher.tracer.infra.topos
import sys
import json
import asyncio
from functools import wraps
from typing import List, Tuple, Dict, Any, Optional, Generic, TypeVar, Union, Callable

from xphi.arch.bound.xor.parser.ruleset.stream import ElasticDSLRulesetParser, LocalStreamRulesetParser
from xphi.arch.bound.xor.parser.ruleset.engine import CompiledEngine
from xphi.arch.contract.registry.tracer import TracerRegistry
from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.tracer.bound import (
    BaseAuditor, 
    BaseStreamAuditor, 
    BaseBoundary,
    SystemBound,
    log_streamer,
    ReproBaseTracer, 
    LifecycleOp, 
    PhaseOp
)

log = get_emitter("tracer.infra.topos")

T = TypeVar('T')

# =====================================================================
# 1. RESOLVER & MULTI-DIMENSIONAL AUDITORS (Agnostic)
# =====================================================================

class LogResolver(Generic[T]):
    """@desc: Translates high-level domain rulesets into deeply nested queries or compiled engines."""
    
    DEFAULT_RULESET = {
        "global_config": {
            "base_query": {"environment": "production"},
            "noise_exclusions": {"tags": ["debug", "load-test"], "service": "test-runner"}
        },
        "targets": [
            {"tag": "ingestor-memory-critical", "condition": {"service": "ingest.issue", "level": "ERROR"}, "keywords": [{"OR": ["OOM", "memory leak", "heap dump"]}], "apply_exclusions": True},
            {"tag": "gateway-auth-anomaly", "condition": {"service": "api_gateway"}, "keywords": [{"AND": ["unauthorized", "token"]}, {"OR": ["expired", "invalid signature"]}], "apply_exclusions": True}
        ]
    }

    def __init__(self, ruleset: Optional[Dict[str, Any]] = None, parser: Optional[Any] = None):
        self.ruleset = ruleset if ruleset is not None else self.DEFAULT_RULESET
        self.parser = parser if parser is not None else ElasticDSLRulesetParser()

    def resolve(self, target_tags: Optional[List[str]] = None) -> T:
        try:
            resolved_result = self.parser.parse_ruleset(self.ruleset, target_tags)
            log.info(f"✅ Successfully resolved ruleset topology via {self.parser.__class__.__name__}.")
            return resolved_result
        except Exception as e:
            log.error(f"🚨 Failed to resolve ruleset: {str(e)}")
            return None


class ToposAuditor(BaseAuditor):
    """@desc: Kubernetes/K3s 전용의 Topology(형상) 상태 수집 센서."""
    def __init__(self, target: str, namespace: str, boundary: SystemBound):
        super().__init__(target, namespace, boundary)
        self.current_replicas = 0
        self.peak_replicas = 0
        self.restart_count = 0
        self.log = get_emitter(f"auditor.topos.{target}")

    async def _observe(self) -> None:
        try:
            while True:
                # 1. Replicas 관측
                rep_cmd = ["kubectl", "get", "deployment", self.target, "-n", self.namespace, "-o", "jsonpath={.spec.replicas}:{.status.readyReplicas}"]
                code, out, _ = await self.boundary.run_command(rep_cmd, capture=True)
                if code == 0 and out:
                    parts = out.split(":")
                    spec_replicas = int(parts[0]) if parts[0] else 0
                    self.current_replicas = spec_replicas
                    if spec_replicas > self.peak_replicas:
                        self.peak_replicas = spec_replicas

                # 2. Restarts 관측
                rest_cmd = ["kubectl", "get", "pods", "-l", f"app={self.target}", "-n", self.namespace, "-o", "jsonpath={.items[*].status.containerStatuses[*].restartCount}"]
                code, out, _ = await self.boundary.run_command(rest_cmd, capture=True)
                if code == 0 and out:
                    self.restart_count = sum(int(r) for r in out.split() if r.isdigit())

                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass


class ContainerStateAuditor(BaseAuditor):
    """@desc: [Boundary Axis] Docker Compose와 K8s/K3s를 모두 지원하는 다형성 상태 검증 센서"""
    def __init__(self, target: str, boundary: SystemBound, infra_type: str = "compose", namespace: str = "default"):
        super().__init__(target=target, namespace=namespace, boundary=boundary)
        self.infra_type = infra_type
        self.is_running = True
        self.exit_code = "0"
        self.log = get_emitter(f"auditor.state.{target}")

    async def _observe(self) -> None:
        try:
            while True:
                if self.infra_type == "compose":
                    cmd = ["docker", "inspect", self.target, "--format", "{{.State.Running}}:{{.State.ExitCode}}"]
                else:
                    cmd = ["kubectl", "get", "pod", "-l", f"app={self.target}", "-n", self.namespace, "-o", "jsonpath={.items[0].status.phase}:{.items[0].status.containerStatuses[0].state.terminated.exitCode}"]

                code, out, _ = await self.boundary.run_command(cmd, capture=True)
                
                if code == 0 and out:
                    if self.infra_type == "compose":
                        if out.startswith("false"):
                            self.is_running = False
                            self.exit_code = out.split(":")[1].strip() if ":" in out else "Unknown"
                    else:
                        # Kube 판독기 (Phase가 Failed이거나 Terminated 정보가 있을 때)
                        if "Failed" in out or "Succeeded" in out or "Error" in out:
                            self.is_running = False
                            parts = out.split(":")
                            self.exit_code = parts[1].strip() if len(parts) > 1 and parts[1] else "Unknown"
                
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass


class EntropyAuditor(BaseAuditor):
    """@desc: [Energy Axis] 인프라 규격에 맞추어 물리적 에너지(CPU/Mem) 변화를 관측합니다."""
    def __init__(self, target: str, boundary: SystemBound, infra_type: str = "compose", namespace: str = "default"):
        super().__init__(target=target, namespace=namespace, boundary=boundary)
        self.infra_type = infra_type
        self.log = get_emitter(f"auditor.entropy.{target}", phase="agent")
        self.last_cpu_usage = 0.0

    async def _observe(self) -> None:
        try:
            while True:
                if self.infra_type == "compose":
                    cmd = ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}} | {{.MemUsage}}", self.target]
                else:
                    cmd = ["kubectl", "top", "pod", "-l", f"app={self.target}", "-n", self.namespace, "--no-headers"]

                code, out, _ = await self.boundary.run_command(cmd, capture=True)
                if code == 0 and out:
                    out = out.strip()
                    self.log.info(f"  [METRICS] {out}")
                    try:
                        if self.infra_type == "compose":
                            self.last_cpu_usage = float(out.split("%")[0].strip())
                        else:
                            # Kube: "pod-name   5m   12Mi" -> Millicores 파싱
                            parts = out.split()
                            if len(parts) >= 2:
                                cpu_m = parts[1].replace("m", "")
                                if cpu_m.isdigit():
                                    self.last_cpu_usage = float(cpu_m) / 10.0  # 10m = 1.0% (실용적 환산)
                    except ValueError:
                        pass
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass


class UniversalLogAuditor(BaseStreamAuditor):
    """@desc: [Semantics Axis] Docker/Kube 로그 스트림을 동적으로 물어서 DSL 엔진으로 파싱합니다."""
    def __init__(self, target: str, verify_type: str, boundary: SystemBound, infra_type: str = "compose", namespace: str = "default", ruleset: Optional[Dict] = None):
        super().__init__(target=target, boundary=boundary, delay=1)
        self.infra_type = infra_type
        self.namespace = namespace
        self.log = get_emitter(f"auditor.universal_log.{target}", phase="agent")
        
        _ruleset = ruleset or {"targets": []}
        self.resolver = LogResolver[CompiledEngine](ruleset=_ruleset, parser=LocalStreamRulesetParser())
        self.rule_engine: CompiledEngine = self.resolver.resolve()
        
        self.max_type_depth = 0
        self.hit_fatal_limit = False

    async def run_stream(self) -> None:
        """@desc: 정적 데코레이터를 벗어나 서브프로세스 파이프라인을 직접 통제합니다."""
        if self.infra_type == "compose":
            cmd = ["docker", "logs", "-f", self.target]
        else:
            cmd = ["kubectl", "logs", "-l", f"app={self.target}", "-n", self.namespace, "-f"]

        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        
        try:
            while True:
                line = await process.stdout.readline()
                if not line: break
                
                decoded = line.decode('utf-8', errors='replace').strip()
                if not decoded or not self.rule_engine: continue
                
                matched_tags = self.rule_engine.execute(decoded)
                for tag in matched_tags:
                    self._handle_matched_tag(tag, decoded)
                    
        except asyncio.CancelledError:
            process.terminate()

    def _handle_matched_tag(self, tag: str, line: str):
        if tag == "rustc-recursion-depth":
            depth = line.count("SkipWhile")
            if depth > self.max_type_depth:
                self.max_type_depth = depth
                if depth % 5 == 0:
                    self.log.info(f"  [DIVERGENCE] Type Depth reached: {depth}")
        elif tag in ["rustc-fatal-limit", "wasm-fatal-panic"]:
            self.hit_fatal_limit = True
            self.log.warning(f"  [FATAL] Boundary rupture detected: {tag}")


class LeakObserverAuditor(BaseStreamAuditor):
    def __init__(self, boundary: SystemBound):
        super().__init__(target="leak_observer", boundary=boundary, delay=0)

    @log_streamer([sys.executable, "-m", "OBSERVER_MODULE"])
    async def run_stream(self, line: str) -> None:
        if "🚨" in line or "└─" in line or "online" in line:
            print(f"  [OBSERVER] {line}")


# =====================================================================
# 2. VERDICT TABLES
# =====================================================================

HANG_VERDICT_TABLE: Dict[str, Callable[['UniversalLogAuditor'], bool]] = {
    "rustc_recursion": lambda semantic: getattr(semantic, 'max_type_depth', 0) > 20,
    "cranelift_loop": lambda semantic: getattr(semantic, 'optimization_loop_count', 0) > 500,
}


# =====================================================================
# 3. TRACERS (Pure Observation Layer)
# =====================================================================

class ReproTracer(ReproBaseTracer):
    """@desc: 인프라 제어권을 포기하고, 오직 관측과 자극 주입(Stimulus)에만 집중합니다."""
    def __init__(self, target_name: str = "repro_worker", timeout: int = 35):
        super().__init__(target_name=target_name, timeout=timeout)
        self.config = TracerRegistry.get(target_name)
        self.workspace = self.config["workspace_path"]
        self.container_name = self.config.get("container_name", "worker")
        self.observer = LeakObserverAuditor(self.boundary)

    @PhaseOp.stimulus(
        ["docker-compose", "-f", "{compose_file}", "exec", "-T", "{container_name}", "python", "app.py"], 
        cwd="{workspace}", capture=True, strict=True
    )
    async def inject_stimulus(self, exit_code: int = 0, stdout: str = "") -> None:
        self.log.info("## @phase.3: Injecting Stimulus (Delayed Messages)...")

    async def execute(self) -> None:
        try:
            self.log.info("## @phase.1: ToposOrchestrator manages Manifolds. Tuning Entropy Observatory...")
            self.register_auditors(self.observer)

            await self.inject_stimulus()
            
            self.log.info(f"## @phase.2: Waiting for Signal Transition (ETA: {self.timeout}s)...")
            await self.await_rupture()

            self.log.info("## @phase.3: Finalizing Observation...")
            await asyncio.sleep(2)
        finally:
            self.log.info("## @phase.4: Releasing Observers. (Teardown handled by Orchestrator)")


class OOMTracer(ReproBaseTracer):
    """@desc: OOM 붕괴(Collapse) 검증에 특화된 순수 관측 레이어"""
    def __init__(self, target_name: str, timeout: int = 60, infra_type: str = "compose", namespace: str = "default"):
        super().__init__(target_name=target_name, timeout=timeout)
        self.config = TracerRegistry.get(target_name)
        self.workspace = self.config["workspace_path"]
        
        c_name = self.config["container_name"]
        v_type = self.config["verify_type"]
        
        # 주입받은 infra_type에 따라 다형성 센서 부착
        self.state_auditor = ContainerStateAuditor(c_name, self.boundary, infra_type, namespace)
        self.entropy_auditor = EntropyAuditor(c_name, self.boundary, infra_type, namespace)
        self.semantic_auditor = UniversalLogAuditor(c_name, v_type, self.boundary, infra_type, namespace)

    async def _check_boundary_hook(self, remaining: int) -> None:
        if not getattr(self.state_auditor, 'is_running', True):
            exit_code = getattr(self.state_auditor, 'exit_code', 'Unknown')
            self.log.warning(f"  [BOUNDARY RUPTURE] Target Container Collapsed! (ExitCode: {exit_code})")
            
            # K8s(137, Error, OOMKilled)와 Docker(137) 모두 호환되는 논리 처리
            if "137" in exit_code or "OOM" in exit_code.upper():
                self.log.crit("[SUCCESS] Absolute OOM confirmed. Sandbox boundary crushed.")
            elif exit_code not in ["0", "Unknown"]:
                if getattr(self.semantic_auditor, 'hit_fatal_limit', False):
                    self.log.crit(f"[SUCCESS] Semantic fatal divergence confirmed for {self.config['verify_type']}.")
                else:
                    self.log.error(f"[FAIL] Container died with code {exit_code}, lacking structural proof.")
            
            self.rupture_confirmed = True 

    async def execute(self) -> None:
        self.log.crit(f"## @trace.init Injecting Target Topology Sensor: {self.config.get('desc', self.config['verify_type'])}")
        
        try:
            self.log.info("## @phase.1: Attaching Multidimensional Auditors (State, Entropy, Semantics)...")
            self.register_auditors(self.state_auditor, self.entropy_auditor, self.semantic_auditor)

            self.log.info("## @phase.2: Waiting for Boundary Collapse...")
            await self.await_rupture(hook_fn=self._check_boundary_hook)

            if not self.rupture_confirmed and getattr(self.state_auditor, 'is_running', True):
                self.log.info("## @phase.3: Evaluating Hang/Livelock Judgment...")
                
                # CPU 95% 이상 교착 상태 판독
                if getattr(self.entropy_auditor, 'last_cpu_usage', 0.0) > 95.0:
                    v_type = self.config["verify_type"]
                    verdict_fn = HANG_VERDICT_TABLE.get(v_type)
                    
                    if verdict_fn and verdict_fn(self.semantic_auditor):
                        self.log.crit(f"[SUCCESS] Semantic Hang confirmed in {v_type}.")
                    else:
                        self.log.error("[FAIL] High CPU detected, but logical divergence depth is insufficient.")
                else:
                    self.log.error(f"[FAIL] Target survived without collapsing. CPU: {getattr(self.entropy_auditor, 'last_cpu_usage', 0.0)}%")
                    
        finally:
            self.log.info("## @phase.4: Releasing Observers. (Teardown handled by Orchestrator)")