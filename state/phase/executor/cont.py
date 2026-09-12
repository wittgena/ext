# xphi.state.phase.executor.cont
## @lineage: xphi.state.phase.flow.cont
from __future__ import annotations

import os
import sys
import uuid
import json
import asyncio
import logging
import subprocess
import importlib
from typing import List, Dict, Optional, Any, Callable
from dataclasses import asdict
import redis.asyncio as redis_async

from xphi.arch.event.next import next_id, next_phase_id, parse_id, parse_phase_id, LogEvent
from xphi.arch.event.psi import PsiEvent, PsiCarrier
from xphi.arch.event.bus import AsyncEventBus
from xphi.arch.contract.registry.unified import registry

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.state.phase.executor.base import BaseExecutor
from xphi.watcher.plane.emitter import get_logger, flow_scope

cont_log = logging.getLogger("flow.cont")
swarm_log = get_logger("swarm.executor")


# =====================================================================
# 1. Utilities & Metaclasses
# =====================================================================

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


# =====================================================================
# 2. Core Continuation Flow (XeCont)
# =====================================================================

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
            cont_log.info(f"!!! [RUPTURE] Epoch.flip: {self.trace_id} (Phase:{hex(self.phase_id)}) -> {ext_base.trace_id}")
            self.ex = ext_base.ex
            self.origin = ext_base.trace_id

        psi.event_id = next_id()
        psi.phase_id = self.phase_id
        return [psi]


# =====================================================================
# 3. Specialized Executors & Carriers
# =====================================================================

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


class SwarmExecutor(BaseExecutor):
    """특정 인스턴스가 아닌, 레지스트리에서 태스크를 동적으로 찾아 실행하는 스웜용 실행기"""
    
    def __init__(self, completion_signal: asyncio.Event):
        super().__init__()
        self.completion_signal = completion_signal
        self.node = None
        self.log = swarm_log

    async def execute(self, psi: PsiEvent) -> list:
        if not hasattr(psi, 'carrier') or psi.carrier.kind != "COMMAND":
            return []

        context = psi.carrier.payload.get("_context", {})
        command = context.get("command") or psi.carrier.tag
        cli_args = context.get("cli_args", [])
        task_id = getattr(psi, 'event_id', None) or f"task-{next_id()}"

        if not command: 
            self.log.error(f"[Swarm] Cannot resolve command from payload or tag. (Event ID: {task_id})")
            return []

        ## 레지스트리에서 해당 커맨드에 매핑된 모듈/함수 정보 획득
        task_info_list = registry.registered_cli_tasks.get(command)
        if not task_info_list: 
            self.log.error(f"[Swarm] No registered task found for: {command}")
            return []

        with flow_scope(flow_id=task_id, phase="EXECUTION"):
            self.log.info(f"[SwarmCliExecutor] flow_id: {task_id}")
            
            try:
                task_info = task_info_list[0]
                module_fqn = task_info.get("module_fqn")
                entry_func_name = task_info.get("entry", "entry_task")
                
                ## 모듈 동적 임포트
                module = importlib.import_module(module_fqn)
                if not hasattr(module, entry_func_name):
                    self.log.error(f"[Swarm] '{entry_func_name}' not found in {module.__name__}")
                    return []

                ## 진입점 함수 추출 및 태스크 인스턴스 생성
                entry_func = getattr(module, entry_func_name)
                task_instance = entry_func(cli_args)
                
                ## 내부 실행기를 위한 독립적인 완료 시그널 생성
                sub_completion_signal = asyncio.Event()
                if hasattr(task_instance, "execute_flow") or hasattr(task_instance, "execute"):
                    from xphi.state.phase.executor.flow import FlowExecutor
                    internal_executor = FlowExecutor(sub_completion_signal)
                    self.log.info(f"[Swarm] Allocated FlowExecutor for {command}")
                else:
                    from xphi.state.phase.executor.cli import _GenericCliExecutor
                    internal_executor = _GenericCliExecutor(task_instance, sub_completion_signal)
                    self.log.info(f"[Swarm] Allocated _GenericCliExecutor for {command}")

                if self.node:
                    internal_executor.node = self.node
                else:
                    self.log.warn("[Swarm] Executor has no node reference! Reflection might fail.")

                await internal_executor.execute(psi)
            except Exception as e:
                self.log.error(f"[Swarm] Execution Failed: {e}")
                import traceback; traceback.print_exc()
            finally:
                self.completion_signal.set()
                
        return []