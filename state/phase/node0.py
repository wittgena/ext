# xphi.state.phase.node0
## @lineage: xphi.kernel.phase.state.node0
## @lineage: kernel.phase.state.node0
## @lineage: kernel.bind.state.node0
## @lineage: kernel.topos.state.node0
import uuid
import json
from typing import List, AsyncIterator, Dict, Any, Tuple
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from xphi.arch.event.psi import PsiCarrier
from xphi.kernel.space.topos.tunnel.factory import UniversalFacade

from arch.contract.interpreter import NodeInterpreter, AnchoredIR, AnchorFlow
from xphi.kernel.wasm.broker import DphiMethod
from xphi.kernel.adapter.state import StateAdapter

from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("state.node0")

@dataclass
class CoreState:
    phase: str
    version: int
    meta: Dict[str, Any] = field(default_factory=dict)

async def capture_snapshot_async(
    interpreter: NodeInterpreter, 
    tunnel: UniversalFacade, 
    collapse_tag: str = "system:panic"
) -> Tuple[CoreState, Dict[str, Any]]:
    state = CoreState(phase=interpreter.phase, version=interpreter.anchor.version)
    signals: Dict[str, Any] = {}
    pattern = f"*{collapse_tag.split(':')[0]}*"
    try:
        cursor = 0
        collected_keys = []
        while True:
            cursor, keys = await tunnel.scan(cursor=cursor, match=pattern, count=100)
            collected_keys.extend(keys)
            if len(collected_keys) >= 5 or int(cursor) == 0:
                break
        
        for k in collected_keys[:5]:
            val = await tunnel.get(k)
            if val is not None:
                signals[k] = val
    except Exception as e:
        log.error(f"[node0] Snapshot retrieval failed: {e}")

    return state, signals

class Node0State:
    def __init__(self, node_id: str):
        self.context_id = f"ctx-0-{uuid.uuid4().hex[:8]}"
        self.node_id = node_id
        self.origin_anchor: AnchoredIR = AnchorFlow.bootstrap()

    async def sync_to_origin(self, interpreter: NodeInterpreter, tunnel: UniversalFacade) -> None:
        log.trace(f"[node0:{self.context_id}] Initiating state reconstruction for {self.node_id}")
        
        snapshot_state, surface_signals = await capture_snapshot_async(interpreter, tunnel)
        boundaries_json = json.dumps(list(self.origin_anchor.recept_boundaries)) # 내용물 자체의 문자열화는 허용
        phase_root = StateAdapter.build_core_node(
            name="origin_root", 
            content=boundaries_json
        )
        phase_root["kind"] = "ANCHOR"
        evolution_ctx = StateAdapter.build_evolution_context(phase_root=phase_root)
        intent_payload = {
            "collapsed_state": {
                "phase": snapshot_state.phase,
                "version": snapshot_state.version
            },
            "cached_states": surface_signals
        }

        transition_payload_dict = StateAdapter.build_transition_payload(
            intent_action="ORIGIN_RECONSTRUCT",
            intent_payload=intent_payload,
            evolution_ctx=evolution_ctx
        )
        canonical_payload_str = StateAdapter.to_canonical_bytes(transition_payload_dict).decode('utf-8')
        try:
            await interpreter.broker.invoke(
                target_func=DphiMethod.EXECUTE_TRANSITION,
                payload=canonical_payload_str
            )
        except Exception as e:
            log.warn(f"[node0] WASM broker sync failed during reconstruction: {e}")

        interpreter.anchor = self.origin_anchor
        interpreter._current_phase = "PHASE_IDLE"
        log.signal(f"[node0] Reconstruction complete. Node is back to Origin (PHASE_IDLE).")

    def quarantine_collapse_log(self, collapse_log: List[PsiCarrier]) -> List[PsiCarrier]:
        recovered_signals = []
        for psi in collapse_log:
            if "error" in psi.kind or "panic" in psi.kind:
                log.trace(f"[node0] Quarantining toxic signal: {psi.symbol}")
                recovered_signals.append(PsiCarrier(source=psi.source, kind=f"{psi.kind}:quarantined", tag=psi.tag))
            else:
                log.trace(f"[node0] Prepping unhandled signal for retry: {psi.symbol}")
                recovered_signals.append(PsiCarrier(source=psi.source, kind=f"{psi.kind}:retry", tag=psi.tag))
        return recovered_signals

@asynccontextmanager
async def enter_node0(interpreter: NodeInterpreter, tunnel: UniversalFacade, node_id: str) -> AsyncIterator[Node0State]:
    log.trace(f"[node0] {node_id} ENTERS origin context")
    n0_state = Node0State(node_id)
    try:
        await n0_state.sync_to_origin(interpreter, tunnel)
        yield n0_state
    except Exception as e:
        log.trace(f"[node0] Exception caught inside origin context: {e}. Forcing hard reconstruction.")
        await n0_state.sync_to_origin(interpreter, tunnel)
    finally:
        log.trace(f"## [node0] {node_id} EXITS origin context. Ready at {interpreter.phase}")