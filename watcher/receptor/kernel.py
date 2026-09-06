# xphi.watcher.receptor.kernel
import asyncio
import json
import time
from contextlib import suppress
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.plane.metric.trajectory import (
    CoDiffBoundLensStrategy,
    DefaultBoundLensStrategy,
    Point,
    TopologicalStructure,
    WindowedTrajectory,
)
from xphi.watcher.plane.sink import EmitterSink

# =============================================================================
# Loggers & Constants
# =============================================================================
kernel_log = get_emitter("receptor.kernel")
filter_log = get_emitter("mem.filter")

STATE_KEY_PHASE = "meta.self:state:current_phase"
CHANNEL_SIGNAL_MUTATION = "meta.self:signals:phase_mutation"
CHANNEL_PSI_FEEDBACK = "meta.self:signals:psi"
CHANNEL_AUTOSCALER = "system:autoscaler:events"


# =============================================================================
# Layer 1: Firewall (기존 Anchor + Membrane + L0 Interceptor 통합)
# =============================================================================
class TunnelInterceptor:
    """터널(Redis/EventBus) 페이로드가 커널 위상과 일치하는지 검증하는 L0 방화벽"""
    def __init__(self):
        self._valid_signatures: Set[str] = {"core_genesis"}
        self._processed_count: int = 0

    def update_signatures(self, signatures: Set[str]):
        """커널로부터 허용된 위상(Topology) 서명 목록을 동기화"""
        self._valid_signatures = signatures
        filter_log.debug(f"[Firewall] Security signatures updated: {len(self._valid_signatures)} items.")

    def intercept(self, channel: str, raw_payload: bytes) -> bool:
        """True 반환 시 패킷 Drop, False 반환 시 Pass."""
        try:
            # 안전한 디코딩 및 파싱 (기존의 불안정한 텍스트 슬라이싱 제거)
            payload_str = raw_payload.decode('utf-8', errors='ignore')
            data = json.loads(payload_str)
            structural_hash = data.get("hash", "")
            intent_tag = data.get("intent", "")
        except Exception as e:
            filter_log.debug(f"[Firewall] ❌ Malformed payload on {channel} (Blocked). Error: {e}")
            return True 

        # 서명 일치 여부 확인
        if intent_tag not in self._valid_signatures and structural_hash not in self._valid_signatures:
            filter_log.debug(f"[Firewall] ❌ Unaligned intent '{intent_tag}' on {channel} (Blocked).")
            return True 

        # 통과 성공
        self._processed_count += 1
        return False


# =============================================================================
# Layer 2: Receptor Kernel
# =============================================================================
def build_system_topos() -> List[TopologicalStructure]:
    """초기 시스템 위상 구조(Topological Structures) 구성"""
    structures = []
    
    arch = []
    with suppress(ImportError): import xphi.arch.model.sensor as m; arch.append(m.__name__)
    with suppress(ImportError): import xphi.kernel.space.topos.tunnel.surface as m; arch.append(m.__name__)
    with suppress(ImportError): import xphi.kernel.space.topos.tunnel.factory as m; arch.append(m.__name__)
    if arch: structures.append(TopologicalStructure(name="arch.topos", members=arch))

    phase = []
    with suppress(ImportError): import xphi.kernel.space.bind.resolver as m; phase.append(m.__name__)
    with suppress(ImportError): import xphi.watcher.receptor.bootstrap as m; phase.append(m.__name__)
    with suppress(ImportError): import xphi.watcher.receptor.kernel as m; phase.append(m.__name__)
    if phase: structures.append(TopologicalStructure(name="phase.runtime", members=phase))

    watcher = []
    with suppress(ImportError): import xphi.kernel.resonance as m; watcher.append(m.__name__)
    with suppress(ImportError): import xphi.state.ledger.consensus as m; watcher.append(m.__name__)
    with suppress(ImportError): import xphi.kernel.singularity as m; watcher.append(m.__name__)
    if watcher: structures.append(TopologicalStructure(name="watcher.kernel", members=watcher))

    return structures


class ReceptorKernel:
    def __init__(
        self, 
        sink: EmitterSink, 
        window_steps: int = 14, 
        structures: Optional[List[TopologicalStructure]] = None
    ):
        self.sink = sink
        self.window_steps = window_steps
        self.structures = structures or []
        
        # 1. 내부적으로 방화벽(Interceptor)을 캡슐화하여 소유
        self.firewall = TunnelInterceptor()
        
        self.kinematic_lens = DefaultBoundLensStrategy(preset_name="tail_risk")
        self.codiff_lens = CoDiffBoundLensStrategy(diff_threshold=0.1)
        self.trajectory_buffer: Dict[str, List[Point]] = {}
        self.last_known_values: Dict[str, float] = {}

        # 2. 커널 초기화 시 방화벽에 현재 위상 서명 동기화
        self._sync_immune_memory()

    def attach_firewall(self, tunnel: Any):
        """커널이 소유한 방화벽을 외부 터널(UniversalFacade)에 부착"""
        if hasattr(tunnel, "register_ingress_filter"):
            tunnel.register_ingress_filter(self.firewall.intercept)
            kernel_log.info("[Boot] L0 Tunnel Firewall physically attached to UniversalFacade.")

    def _sync_immune_memory(self):
        """현재 마운트된 위상 정보를 기반으로 방화벽 서명(Signature) 동기화"""
        valid_intents: Set[str] = {"system_health_probe", "reload", "core_genesis"}
        for struct in self.structures:
            valid_intents.add(struct.name)
        
        self.firewall.update_signatures(valid_intents)
        kernel_log.debug(f"[ImmuneSync] Firewall synchronized with {len(valid_intents)} topological identities.")

    async def reload_system_structures(self, new_structures: List[TopologicalStructure]):
        """동적으로 시스템 위상을 재로드하고 방화벽 규칙을 재정렬"""
        self.structures = new_structures
        self._sync_immune_memory()
        kernel_log.info("[Topology] Firewall signatures synchronized with physical structure mutation.")
    
    def _get_structure_for(self, signal_id: str) -> Optional[TopologicalStructure]:
        for struct in self.structures:
            if signal_id in struct.members:
                return struct
        return None

    def _calculate_structure_center(self, structure: TopologicalStructure) -> Optional[float]:
        active_vals = [
            self.last_known_values[m] for m in structure.members 
            if m in self.last_known_values
        ]
        if not active_vals:
            return None
        return sum(active_vals) / len(active_vals)

    async def get_current_phase(self) -> str:
        val = await self.sink.get_control_flag(STATE_KEY_PHASE)
        return val or "Φ0"

    async def watch_mutations(self):
        """환경적 변이(파일 변경, 시그널 등) 모니터링"""
        async for msg in self.sink.subscribe(CHANNEL_SIGNAL_MUTATION):
            if isinstance(msg, bytes):
                msg = msg.decode('utf-8')
            if isinstance(msg, str):
                with suppress(json.JSONDecodeError):
                    msg = json.loads(msg)
            
            if isinstance(msg, dict):
                signal_id = msg.get("signal_id")
                value = msg.get("value")
                if signal_id and value is not None:
                    await self._ingest_and_evaluate(signal_id, float(value))

    async def _ingest_and_evaluate(self, signal_id: str, current_value: float):
        """Kinematic 및 CoDiff 렌즈를 통해 원시 신호(Raw Signals) 분석"""
        now = datetime.now()
        
        self.last_known_values[signal_id] = current_value
        if signal_id not in self.trajectory_buffer:
            self.trajectory_buffer[signal_id] = []
            
        buffer = self.trajectory_buffer[signal_id]
        buffer.append(Point(timestamp=now, value=current_value))
        
        if len(buffer) > self.window_steps:
            buffer.pop(0)
            
        if len(buffer) < self.window_steps:
            return

        window = WindowedTrajectory(
            identity=signal_id,
            start_time=buffer[0].timestamp,
            end_time=buffer[-1].timestamp,
            points=buffer
        )

        k_scan = self.kinematic_lens.scan(window)
        if k_scan["status"] == "valid":
            metrics = k_scan["metrics"]
            is_high_tension = metrics.get("trend", 0) > 0.8
            is_flatlined = (metrics.get("mean", 1.0) == 0.0) and (metrics.get("volatility", 1.0) == 0.0)
            is_volatile = metrics.get("volatility", 0) >= 0.05
            
            if is_high_tension:
                await self._emit_rupture("KINEMATIC_TENSION_HIGH", signal_id, metrics)
            elif is_flatlined:
                await self._emit_rupture("KINEMATIC_FLATLINE", signal_id, metrics)
            elif is_volatile:
                await self._emit_rupture("KINEMATIC_VOLATILITY", signal_id, metrics)

        structure = self._get_structure_for(signal_id)
        if structure:
            struct_val = self._calculate_structure_center(structure)
            if struct_val is not None:
                struct_window = WindowedTrajectory(
                    identity=structure.name,
                    start_time=buffer[0].timestamp,
                    end_time=now,
                    points=[Point(timestamp=now, value=struct_val)]
                )
                
                c_scan = self.codiff_lens.scan(window, struct_window)
                if c_scan["status"] == "valid" and c_scan.get("is_ruptured"):
                    await self._emit_rupture("CO_DIFF", signal_id, c_scan["metrics"], structure.name)

    async def _emit_rupture(self, rupture_type: str, signal_id: str, metrics: dict, structure_name: str = None):
        """파열/이상 징후(Rupture Events)를 표준화하여 오토스케일러 등으로 발행"""
        trace_record = {
            "event": "xphi_structure_event",
            "rupture_type": rupture_type,
            "signal": signal_id,
            "structure": structure_name,
            "metrics": metrics,
            "timestamp": datetime.now().isoformat()
        }
        await self.sink.tunnel.publish(CHANNEL_AUTOSCALER, json.dumps(trace_record))

    async def emit_analysis_event(self, payload: dict):
        """분석 결과(메타 이벤트, 소스코드 변이 등)를 피드백 루프에 발행"""
        merged_payload = payload.copy() if payload else {}
        merged_payload.update({
            "event": "xphi_analysis_event",
            "weight": 1,
            "ts": time.time()
        })
        kernel_log.info(f"Ψ emit → {merged_payload}")
        await self.sink.publish(CHANNEL_PSI_FEEDBACK, json.dumps(merged_payload))

    async def watch_psi_feedback(self):
        """시스템 재진입 궤적(Ψ 피드백) 모니터링"""
        async for msg in self.sink.subscribe(CHANNEL_PSI_FEEDBACK):
            kernel_log.debug(f"🌀 [ReceptorKernel] Re-entry Ψ′ feedback → {msg}")

    async def start_daemons(self):
        """내부 옵저버 데몬(Daemons) 구동"""
        asyncio.create_task(self.watch_mutations())
        asyncio.create_task(self.watch_psi_feedback())