# xphi.state.phase.inversion
## @lineage: xphi.kernel.phase.state.inversion
## @lineage: kernel.phase.state.inversion
## @lineage: kernel.bind.state.inversion
"""@desc: Critical Detectors (Singularity, Inversion) and Superposition Kernel (Resonance)"""
from __future__ import annotations
from typing import List, Dict, Optional, Any

from xphi.arch.event.psi import PsiCarrier, PsiEvent
from xphi.arch.contract.interface import ICriticalDetector, IPhaseField, IDynamicsKernel
from xphi.arch.contract.registry.unified import registry, contract

@contract.ator("kernel.inversion", role="watcher")
class KernelInversion(ICriticalDetector):
    def __init__(self, **kwargs):
        self.peak_threshold = kwargs.get("peak_threshold", 353.0)
        self.anchor_target = kwargs.get("anchor_target", 5.0)
        self.last_pressure = 0.0
        self.last_dp = 0.0

    def extract(self, field: IPhaseField) -> Dict[str, float]:
        return {"pressure": getattr(field, "pressure", 0.0)}

    def evaluate(self, field: IPhaseField, history: List[Any], current_tick: int) -> Optional[PsiEvent]:
        metrics = self.extract(field)
        current_pressure = metrics.get("pressure", 0.0)

        dp = current_pressure - self.last_pressure
        ddp = dp - self.last_dp

        self.last_pressure = current_pressure
        self.last_dp = dp

        is_peak_reached = (current_pressure >= self.peak_threshold) and (dp <= 0.01) and (ddp < 0)

        if is_peak_reached:
            expected = current_pressure - self.anchor_target
            carrier = PsiCarrier(
                kind="INVERSION", 
                tag="ARBITRAGE_SETTLEMENT", 
                payload={
                    "peak_tension": current_pressure,
                    "target": self.anchor_target,
                    "expected_yield": expected
                }
            )
            
            return PsiEvent(
                event_id=f"isorhesis-inversion-{current_tick}", parent_id=None,
                source_id="kernel.inversion", scope="GLOBAL", tick=current_tick,
                carrier=carrier, context={"state": "damping_injected", "action": "execute_bridge_tx"}
            )
        return None

@contract.ator("kernel.singularity", role="watcher")
class KernelSingularity(ICriticalDetector):
    def __init__(self, **kwargs):
        self.candidate_limit = kwargs.get("candidate_limit", 10.0)
        self.rupture_limit = kwargs.get("rupture_limit", 25.0)

    def extract(self, field: IPhaseField) -> Dict[str, float]:
        return {"pressure": getattr(field, "pressure", 0.0)}

    def evaluate(self, field: IPhaseField, history: List[Any], current_tick: int) -> Optional[PsiEvent]:
        metrics = self.extract(field)
        pressure = metrics.get("pressure", 0.0)
        
        if pressure >= self.rupture_limit:
            carrier = PsiCarrier(kind="RUPTURE", tag="CRITICAL", payload={"pressure": pressure})
            return PsiEvent(
                event_id="system-rupture", parent_id=None, source_id="kernel.singularity",
                scope="GLOBAL", tick=current_tick, carrier=carrier, context={"state": "collapse"}
            )
        return None

@contract.ator("kernel.resonance", role="kernel")
class KernelResonance(IDynamicsKernel):
    """@role: Superposition of Physical Coupling (Kuramoto) and Cognitive Clustering (SensorAtor)"""
    def __init__(self, **kwargs):
        # [개선됨] 카테고리("kernel") 인자 삭제 및 정확한 식별자 매핑
        self.kuramoto = registry.create_component({"type": "sensor.kuramoto", "params": kwargs.get("kuramoto_params", {})})
        self.ator = registry.create_component({"type": "sensor.ator", "params": kwargs.get("ator_params", {})})
        self.alpha = kwargs.get("alpha", 0.5) 

    def compute_step(self, states, dt):
        k_deltas = self.kuramoto.compute_step(states, dt)
        a_deltas = self.ator.compute_step(states, dt)

        deltas = {}
        for node_id in states:
            d_phase = (k_deltas[node_id]["d_phase"] * self.alpha) + (a_deltas[node_id]["d_phase"] * (1.0 - self.alpha))
            tension = k_deltas[node_id]["target_tension"] + a_deltas[node_id]["target_tension"]
            deltas[node_id] = {"d_phase": d_phase, "target_tension": tension}
        return deltas

    def render_state(self, states):
        return self.ator.render_state(states)