# xphi.kernel.ops.boot
import os
import json
import asyncio
from typing import Optional

from xphi.arch.event.next import LogEvent
from xphi.arch.event.psi import PsiEvent, PsiCarrier, CarrierType
from xphi.arch.event.bus import AsyncEventBus
from xphi.arch.contract.registry.unified import registry

from xphi.state.phase.executor.base import BaseExecutor
from xphi.state.phase.executor.cont import SwarmExecutor
from xphi.state.phase.executor.flow import FlowExecutor
from xphi.state.phase.reactor import PhaseReactor
from xphi.state.phase.runtime.node import NodeRuntime
from xphi.state.ledger.consensus import KernelLedger
from xphi.state.ledger.gateway import GatewayPolicy 

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.kernel.wasm.broker import DphiBroker

from xphi.arch.event.next import EventObserver
from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.plane.regulator import default_plane
from xphi.watcher.receptor.bootstrap import receptor_bootstrap

log = get_emitter("kernel.boot")

_node_instance: Optional[NodeRuntime] = None
_background_tasks = set()

class PhaseSignal(EventObserver):
    def __init__(self, event_bus: AsyncEventBus):
        self.bus = event_bus
        self.log = get_emitter("anchor.bridge", phase="BRANE")

    def update(self, event: LogEvent) -> None:
        if event.level != "SIGNAL":
            return
            
        event_message = str(event.message).strip()
        payload = event.context.copy() if event.context else {}
        payload.update({
            "bridged_from": "Surface.LogEvent",
            "original_source": getattr(event, 'source_id', 'unknown'),
            "boundary": "surface_to_kernel"
        })

        carrier = PsiCarrier(
            kind="SIGNAL",          
            tag=event_message,      
            payload=payload,
            carrier_type=CarrierType.FIXED
        )

        psi_event = PsiEvent(
            event_id=event.event_id,
            parent_id=getattr(event, 'parent_id', None),
            source_id="phase.bridge", 
            scope=getattr(event, 'scope', 'GLOBAL'),
            tick=getattr(event, 'tick', 0) or 0,
            phase_id=getattr(event, 'phase_id', 0),
            carrier=carrier,
            context={"domain": "boundary.crossing"}
        )

        try:
            loop = asyncio.get_running_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop is closed")
            loop.create_task(self._async_dispatch(psi_event, event_message))
        except RuntimeError as e:
            self.log.error(f"[PhaseBridge] Cannot dispatch SIGNAL. Event loop unavailable: {e}")

    async def _async_dispatch(self, psi_event: PsiEvent, original_msg: str):
        try:
            symbol = getattr(psi_event, 'symbol', original_msg)
            self.log.trace(f"[PhaseBridge] Elevating SIGNAL -> Psi: {symbol}")
            await self.bus.publish(psi_event)
        except Exception as e:
            self.log.error(f"[PhaseBridge] Dispatch failed for {original_msg}: {e}", exc_info=True)

class RoutingExecutor(BaseExecutor):
    def __init__(self, completion_signal: asyncio.Event):
        super().__init__()
        self.completion_signal = completion_signal
        self.swarm_executor = SwarmExecutor(completion_signal)
        self.flow_executor = FlowExecutor(completion_signal)
        self._node = None

    @property
    def node(self):
        return self._node

    @node.setter
    def node(self, value):
        self._node = value
        self.swarm_executor.node = value
        self.flow_executor.node = value

    async def execute(self, psi) -> list:
        if not hasattr(psi, 'carrier') or psi.carrier.kind != "COMMAND":
            return []

        context = psi.carrier.payload.get("_context", {})
        command = context.get("command") or psi.carrier.tag
        task_info_list = registry.registered_cli_tasks.get(command)
        if not task_info_list:
            return await self.swarm_executor.execute(psi)

        task_type = task_info_list[0].get("type", "cli")
        if task_type == "flow":
            try:
                return await self.flow_executor.execute(psi)
            except Exception as e:
                log.error(f"[Router] FlowExecutor failed with {e}. Falling back to SwarmExecutor.")
                return await self.swarm_executor.execute(psi)
        else:
            return await self.swarm_executor.execute(psi)


async def main_async():
    global _node_instance
    
    log.info("[Boot] Initiating Control Plane (Master Watcher)...")
    system_bus = AsyncEventBus()
    
    bridge_watcher = PhaseSignal(event_bus=system_bus)
    default_plane.attach(bridge_watcher)

    ## Gateway 보안 정책(Action Cost, Rate Limit) 동적 주입
    log.info("[Boot] Injecting Gateway Security Policies...")
    try:
        env_costs_str = os.getenv("GATEWAY_ACTION_COSTS", "{}")
        env_costs = json.loads(env_costs_str)
        
        env_trusted_str = os.getenv("GATEWAY_TRUSTED_ACTIONS", "")
        if env_trusted_str:
            env_trusted = {a.strip() for a in env_trusted_str.split(",") if a.strip()}
        else:
            # 설정값이 없으면 시스템의 필수 안전 동작들을 기본 신뢰 액션으로 지정
            env_trusted = {"LOGSTREAM_BULK_INSERT", "SECURITY_TENSION_ALERT", "AUDIT_LOG_APPEND"}
            
        capacity = int(os.getenv("GATEWAY_RATE_CAPACITY", "100"))
        refill = float(os.getenv("GATEWAY_RATE_REFILL", "5.0"))
        
        GatewayPolicy.configure(
            action_costs=env_costs,
            trusted_actions=env_trusted,
            capacity=capacity,
            refill=refill
        )
        log.info(f"[Boot] Gateway Policies applied: capacity={capacity}, refill={refill}, trusted_actions={len(env_trusted)}")
    except Exception as e:
        log.error(f"[Boot] Failed to inject Gateway policies. Using safe defaults. Error: {e}")
    
    log.info("[Boot] Igniting Physical Membrane Receptor...")
    tunnel = await TunnelFactory.get_default()
    receptor_task = asyncio.create_task(receptor_bootstrap(tunnel))
    _background_tasks.add(receptor_task)
    receptor_task.add_done_callback(_background_tasks.discard)

    log.info("[Boot] Waiting for module discovery and registry sync...")
    active_daemons_str = os.getenv("KERNEL_DAEMONS", "")
    active_daemons = [d.strip() for d in active_daemons_str.split(",") if d.strip()]
    
    wait_timeout = 30.0
    elapsed = 0.0
    poll_interval = 0.2
    
    if active_daemons:
        while elapsed < wait_timeout:
            discovered = getattr(registry, "_daemons", {})
            if all(d in discovered for d in active_daemons):
                log.info(f"[Boot] Registry sync verified in {elapsed:.1f}s. Proceeding to ignite node...")
                break
                
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
        else:
            missing_daemons = [d for d in active_daemons if d not in getattr(registry, "_daemons", {})]
            log.warning(f"[Boot] Registry sync timed out after {wait_timeout}s! Missing daemons: {missing_daemons}")
    else:
        log.info("[Boot] No specific KERNEL_DAEMONS requested. Proceeding directly to ignite node...")

    log.info("[Boot] Igniting Embedded Phase Runtime Node...")
    completion_signal = asyncio.Event()
    executor = RoutingExecutor(completion_signal)
    
    _node_instance = NodeRuntime(executor=executor)
    _node_instance.tunnel = tunnel
    
    node_profile = os.getenv("NODE_PROFILE", "ALL").upper()
    if node_profile != "EDGE":
        _node_instance.broker = DphiBroker(tunnel_factory=TunnelFactory)
    else:
        log.info(f"[Boot] Bypassing DphiBroker master initialization for {node_profile} profile.")

    await _node_instance.start()

    log.info("[Boot] Control Plane and Embedded Node fully operational.")
    log.info("[Boot] Entering observation mode (External layers delegated to Daemons).")
    await _node_instance.wait_until_stopped()

async def teardown():
    global _node_instance
    log.info("[Boot] Releasing system resources...")
    
    # 1. NodeRuntime 및 하위 컴포넌트(Broker, Daemons, Workers) 종료
    if _node_instance and getattr(_node_instance, 'running', False):
        await _node_instance.shutdown()

    # 2. 잔여 백그라운드 태스크 취소
    for task in list(_background_tasks):
        if not task.done():
            task.cancel()

    # 3. Ledger DB 정리
    try:
        KernelLedger().close()
        log.info("[Boot] KernelLedger lock safely released.")
    except Exception as e:
        log.warning(f"[Boot] Error while releasing KernelStore lock: {e}")
        
    # 4. Redis 터널 정리
    await TunnelFactory.close_all()
    log.info("[Boot] Resource cleanup complete.")

if __name__ == "__main__":
    PhaseReactor.ignite(main_coro_func=main_async, teardown_hook=teardown)