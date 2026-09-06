# xphi.state.runtime.flow.cont
## @lineage: xphi.kernel.phase.runtime.flow.cont
from __future__ import annotations
import asyncio
import json
import logging
from typing import List, Dict, Optional, Any, Callable
import redis.asyncio as redis_async

from xphi.arch.event.next import next_id, next_phase_id, parse_id, parse_phase_id 
from xphi.state.runtime.executor.base import BaseExecutor
from xphi.arch.contract.registry.unified import registry
from xphi.arch.event.bus import AsyncEventBus
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.arch.event.psi import PsiEvent, PsiCarrier

log = logging.getLogger("flow.cont")

def _get_bound_metric(bound: Any, attr_name: str, default: int = 0) -> int:
    """@desc: bound 객체에서 메트릭(Topology, Pressure 등)을 안전하게 추출하는 유틸리티"""
    if not bound:
        return default
    val = getattr(bound, attr_name, default)
    try:
        return int(val() if callable(val) else val)
    except (TypeError, ValueError):
        return default


class PhaseField(type(BaseExecutor)):
    """@phase.bound: Assign unique Snowflake ID on class creation"""
    def __new__(mcs, name, bases, namespace):
        ## Ensure global uniqueness using Snowflake ID
        namespace['bound_id'] = f"bound.{next_id()}"
        return super().__new__(mcs, name, bases, namespace)

class XeCont(BaseExecutor, metaclass=PhaseField):
    def __init__(self, bound: Any, ex: str = "void", origin: str = "void"):
        super().__init__()
        self.trace_id = next_id() 
        self.phase_id = 0
        self.ex = ex
        self.origin = origin
        self.bound = bound

    async def execute(self, psi: Any) -> List[Any]:
        """Base execution flow - Override in subclasses"""
        raise NotImplementedError("Subclasses must implement execute()")

    def _ext__(self) -> 'XeCont':
        """@epoch.flip: Isolate lineage and respawn on rupture"""
        return self.__class__(
            bound=self.bound,
            ex=f"Base.bind(inversion.overflow.{self.ex})",
            origin=self.trace_id
        )

class DynamicsXe(XeCont):
    async def execute(self, psi: Any) -> List[Any]:
        payload = psi.context.get("payload", []) if hasattr(psi, 'context') else [{"payload": getattr(psi, "symbol", "")}]
        if hasattr(self.bound, 'absorb'):
            self.bound.absorb(payload)

        # 공통 유틸리티를 사용하여 메트릭 추출
        topo_val = _get_bound_metric(self.bound, 'topology', 0)
        press_val = _get_bound_metric(self.bound, 'pressure', 0)

        self.phase_id = next_phase_id(topo=topo_val, press=press_val)
        decision = "CONTINUE"
        
        if hasattr(self.bound, 'evaluate'):
            decision = self.bound.evaluate()
        
        if decision == "DEPOSIT":
            self.phase_id = next_phase_id(
                topo=topo_val,
                press=press_val,
                rupture=True
            )
            if hasattr(self.bound, 'commit'):
                self.bound.commit()
            
            ext_base = self._ext__()
            log.info(f"!!! [RUPTURE] Epoch.flip: {self.trace_id} (Phase:{hex(self.phase_id)}) -> {ext_base.trace_id}")
            self.ex = ext_base.ex
            self.origin = ext_base.trace_id

        psi.event_id = next_id()
        psi.phase_id = self.phase_id
        return [psi]

class LoopCarrier(BaseExecutor):
    def __init__(self, xe: XeCont, max_ticks: int = 100, interval: float = 0.1):
        super().__init__()
        self.xe = xe
        self.tick = 0
        self.max_ticks = max_ticks
        self.interval = interval

    async def execute(self, psi: Any) -> List[Any]:
        out = []
        
        xe_out = await self.xe.execute(psi)
        out.extend(xe_out) 

        if self.tick < self.max_ticks:
            incoming_psi = None
            
            if hasattr(self, "node") and self.node and hasattr(self.node, "bus"):
                try:
                    incoming_psi = await asyncio.wait_for(
                        self.node.bus.wait_for_event(predicate=lambda e: getattr(e.carrier, 'kind', '') == "SIGNAL"),
                        timeout=self.interval
                    )
                except (asyncio.TimeoutError, AttributeError):
                    ## Proceed normally on timeout
                    pass
            else:
                await asyncio.sleep(self.interval)

            current_tick = self.tick + 1
            
            # [수정됨] 매 틱마다 현재 bound 상태를 기반으로 동기화된 Phase ID 재발급
            bound_obj = getattr(self.xe, 'bound', None)
            topo_val = _get_bound_metric(bound_obj, 'topology', 0)
            press_val = _get_bound_metric(bound_obj, 'pressure', 0)
            sync_phase_id = next_phase_id(topo=topo_val, press=press_val, tick=current_tick)

            if incoming_psi:
                next_psi = incoming_psi
                next_psi.tick = current_tick
                next_psi.phase_id = sync_phase_id
            else:
                next_psi = psi.__class__(
                    event_id=next_id(), 
                    parent_id=getattr(psi, "event_id", None),
                    source_id="loop.carrier",
                    scope=getattr(psi, "scope", "GLOBAL"),
                    carrier=getattr(psi, "carrier", None),
                    phase_id=sync_phase_id,
                    tick=current_tick,
                    context=getattr(psi, "context", {}).copy()
                )

            if hasattr(self, "node") and self.node and hasattr(self.node, "bus"):
                await self.node.bus.publish(next_psi)
            else:
                out.append(next_psi) 
                
            self.tick += 1
            
        return out

class SeekerLogic(BaseExecutor):
    """@role: Attractor-Seeker / @flow: Psi → Intent.vector"""
    async def execute(self, psi: Any) -> List[Any]:
        psi.kind = "attempt:vector"
        psi.context["vector_field"] = "directional_flow"
        return [psi]