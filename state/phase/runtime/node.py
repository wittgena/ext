# xphi.state.phase.runtime.node
import asyncio
import time
import json
import multiprocessing
import os
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

from xphi.arch.contract.interpreter import NodeInterpreter, AnchorFlow
from xphi.kernel.space.topos.tunnel.factory import UniversalFacade, TunnelFactory
from xphi.arch.event.psi import PsiEvent, PsiCarrier
from xphi.arch.event.bus import TunnelEventBus
from xphi.arch.event.next import next_id
from xphi.arch.contract.interface import IPhaseAtor, IPhaseField, IEventBus
from xphi.arch.contract.registry.unified import registry

from xphi.kernel.space.bind.resolver import find_current_self
from xphi.state.phase.executor.swarm import SwarmExecutor
from xphi.state.phase.runtime.sensor import SurfaceSensor, SurfaceActuator
from xphi.state.phase.runtime.context import RuntimeContext
from xphi.kernel.ops.task.supervisor import TaskSupervisor, Dispatcher
from xphi.kernel.ops.daemon.bootstrap import mount_master_layer, EventBusDaemon
from xphi.kernel.wasm.broker import DphiBroker

from xphi.watcher.plane.sink import TunnelSink
from xphi.watcher.plane.emitter import get_emitter

from xphi.state.phase.runtime.worker import worker_process_entry

RUNTIME_KEY = {
    "node": "runtime:node:{node_id}",
    "index_requires": "runtime:index:requires:{req_key}",
    "index_emits": "runtime:index:emits:{emit_key}"
}

class RuntimeKeyResolver:
    def __init__(self, templates: Optional[Dict[str, str]] = None):
        self.templates = templates or RUNTIME_KEY.copy()

    def get(self, template_key: str, **kwargs) -> str:
        template = self.templates.get(template_key)
        if not template:
            raise KeyError(f"Unrecognized runtime key template: '{template_key}'")
        
        return template.format(**kwargs)

runtime_keys = RuntimeKeyResolver()

class NodeRuntime(IPhaseAtor):
    """
    @runtime.node: Master-Worker control manifold
    @flow: Master가 상태/센서 관리 및 CLI 제어 -> Redis Stream -> 각 Worker의 Dispatcher -> WASM 처리
    """
    def __init__(self, executor=None):
        self._id = f"node-{next_id()}" 
        self.node_id = self._id
        self.tunnel: Optional[UniversalFacade] = None
        self.executor = executor
        self.running = True
        self.supervisor = TaskSupervisor(source=f"Master-{self._id}")
        
        # 데몬 등 하위 태스크의 에러를 잡는 글로벌 핸들러 등록
        self.supervisor.add_error_handler(self._global_task_error)

        self.bus: Optional[TunnelEventBus] = None
        self.log = get_emitter("node.master", phase="SYSTEM")
        
        self.broker: Optional[DphiBroker] = None
        self.interpreter = None
        
        self.dispatcher = None
        self.sensor = None
        self.actuator = None
        self.ctx: Optional[RuntimeContext] = None
        
        # 생성된 워커 프로세스를 관리하기 위한 배열
        self.worker_processes: List[multiprocessing.Process] = []
        
        self._stop_event = asyncio.Event()

    def _global_task_error(self, task: asyncio.Task[Any], exc: BaseException) -> None:
        """
        [개선] 과잉 방어(Panic) 정책 제거
        하위 데몬/태스크에서 발생한 에러가 커널 전체 셧다운으로 이어지지 않도록 격리(Isolation) 처리합니다.
        """
        self.log.crit(f"⚠️ Critical fault in Master Task [{task.get_name()}]: {exc}")
        self.log.warn(f"[{task.get_name()}] has been isolated. NodeRuntime continues operating.")

    @property
    def local_manifold(self):
        """
        [개선] registry.registered_nodes 제거에 대응하는 안전한 우회 로직.
        해당 속성이 없을 경우 시스템 셧다운을 방지하고 빈 딕셔너리로 폴백합니다.
        """
        if hasattr(registry, 'registered_nodes'):
            return registry.registered_nodes
            
        # 에러 스택에서 제안된 'registered_daemons'가 호환되는지 확인 (contract 유무)
        if hasattr(registry, 'registered_daemons'):
            daemons = registry.registered_daemons
            if daemons and any(hasattr(meta, 'contract') for meta in daemons.values()):
                return daemons
                
        # 매칭되는 레지스트리 풀이 없다면 빈 딕셔너리를 반환하여 부팅 루틴(fallback to system:ping) 유지
        return {}

    @property
    def ator_id(self) -> str:
        return self._id

    @property
    def state(self) -> str:
        return "RUNNING" if self.running else "STOPPED"

    def set_state(self, new_state: str) -> None:
        if new_state == "STOPPED" and self.running:
            self.running = False
        elif new_state == "RUNNING" and not self.running:
            self.running = True

    async def react(self, event: PsiEvent, field: IPhaseField, bus: IEventBus):
        if self.running and self.dispatcher:
            try:
                self.dispatcher.dispatch(event)
            except Exception as e:
                self.log.error(f"Failed to directly dispatch event {event.symbol}: {e}")

    def _create_phase_handler(self):
        async def handler(psi: PsiEvent):
            judgment = await self.interpreter.process(psi.carrier)
            return {
                "psi": judgment.psi_symbol,
                "action": judgment.action.value,
                "phase": judgment.phase,
                "version": judgment.version,
                "resonance": judgment.is_resonance, 
                "node_id": self.node_id
            }
        return handler

    async def start(self):
        self.log.info(f"Starting Master NodeRuntime [{self.node_id}]")
        
        # 외부(boot.py 등)에서 주입된 터널이 없다면 스스로 생성 (방어적 초기화)
        if not self.tunnel:
            self.tunnel = await TunnelFactory.get_default()
            
        self.bus = TunnelEventBus(self.tunnel)
        self.bus.subscribe(self, predicate=lambda e: True)
        if self.executor:
            self.executor.node = self

        watch_dir = find_current_self()
        self.log.info(f"Loaded {len(self.local_manifold)} local phasenodes from registry.")
        
        await self.register_node()

        all_recepts = set()
        for meta in self.local_manifold.values():
            all_recepts.update(getattr(meta.contract, "recept", []))

        # local_manifold가 비어있어도 여기서 기본 통신망을 확보하므로 안전함
        if not all_recepts:
            all_recepts = {"system:signal", "system:ping"}

        anchor = AnchorFlow.bootstrap(frozenset(all_recepts))
        
        # 주입된 Broker가 없다면 생성하여 덮어쓰기(Overwrite) 방지
        if getattr(self, 'broker', None) is None:
            self.broker = DphiBroker(tunnel_factory=TunnelFactory)
        
        self.interpreter = NodeInterpreter(broker=self.broker, anchor=anchor)
        
        self.log.info(f"Boot phase: {self.interpreter.phase}, boundaries: {len(anchor.recept_boundaries)}")

        self.sensor = SurfaceSensor(tunnel=self.tunnel)
        self.actuator = SurfaceActuator(TunnelSink(tunnel=self.tunnel))
        self.dispatcher = Dispatcher(
            supervisor=self.supervisor,
            default_handler=self._create_phase_handler(),
            executor=self.executor,
            actuator=self.actuator,
        )
        
        # RuntimeContext 재활용 및 Broker 안전 바인딩
        if getattr(self, 'ctx', None) is None:
            self.ctx = RuntimeContext(
                node_id=self.node_id,
                tunnel=self.tunnel,
                bus=self.bus,
                dispatcher=self.dispatcher,
                sensor=self.sensor,
                actuator=self.actuator,
                idle_timeout=0.0,
                watch_dir=watch_dir,
                shutdown_hook=self.shutdown
            )
        else:
            # 부모 계층에서 생성만 해두고 비어있을 수 있으므로 핵심 인프라 속성 갱신
            self.ctx.tunnel = self.tunnel
            self.ctx.bus = self.bus
            self.ctx.dispatcher = self.dispatcher
            self.ctx.sensor = self.sensor
            self.ctx.actuator = self.actuator

        # App Layer 데몬(risk_vault 등)들이 사용할 수 있도록 ctx에 Broker를 반드시 삽입!
        self.ctx.broker = self.broker

        # -----------------------------------------------------------------
        # [개선] 노드 프로파일 판별 (기본값 ALL)
        # -----------------------------------------------------------------
        node_profile = os.getenv("NODE_PROFILE", "ALL").upper()
        self.log.info(f"Node Profile detected as: {node_profile}")

        # 3. Master 마운트 (App 데몬 포함)
        mount_master_layer(self.supervisor, self.ctx, profile=node_profile)
        
        master_control_bus = EventBusDaemon(
            tunnel=self.tunnel,
            dispatcher=self.dispatcher, 
            node_id=self.node_id,
            idle_timeout=0.0,
            shutdown_hook=self.shutdown,
            group_name="master_control_group" 
        )
        self.supervisor.mount_daemon(master_control_bus)
        
        self.log.info(f"Master Infra Layer (ControlBus) mounted successfully. (Profile: {node_profile})")
        if node_profile in ["ALL", "COMPUTE"]:
            worker_count = int(os.environ.get("DPHI_FIXED_WORKERS", multiprocessing.cpu_count()))
            self.log.info(f"Spawning {worker_count} isolated Worker Processes for pure computation...")
            
            for i in range(worker_count):
                p = multiprocessing.Process(
                    target=worker_process_entry, 
                    args=(self.node_id, i),
                    daemon=True 
                )
                p.start()
                self.worker_processes.append(p)
        else:
            self.log.info(f"Worker Process spawning bypassed (Profile: {node_profile}). Node operating in lightweight mode.")

    async def shutdown(self):
        if not self.running: return

        self.log.warn("Shutdown sequence initiated...")
        self.running = False

        # 1. 자식 Worker Process 정리
        if self.worker_processes:
            self.log.info("Terminating and reaping child worker processes...")
            for p in self.worker_processes:
                if p.is_alive():
                    p.terminate()
                    p.join(timeout=2.0)
                    if p.is_alive():
                        p.kill() # timeout 시 강제 종료

        # 2. Master 자원 정리
        await self.deregister_node()
        await self.supervisor.shutdown()

        # Broker 내부의 백그라운드 리스너 리소스 누수 방지 (Graceful Close)
        if self.broker and hasattr(self.broker, 'close'):
            try:
                await self.broker.close()
                self.log.info("DphiBroker background loops safely closed.")
            except Exception as e:
                self.log.warning(f"Error while closing DphiBroker: {e}")

        if self.actuator: await self.actuator.close()
        if self.tunnel: await self.tunnel.close()
        self.log.info("Teardown complete.")
        self._stop_event.set()

    async def wait_until_stopped(self):
        await self._stop_event.wait()

    async def _recover_from_panic(self, toxic_psi: PsiCarrier, error: Exception):
        self.log.crit(f"Critical anomaly from Ψ({toxic_psi.symbol}). Reason: {error}")
        toxic_psi.kind = f"{toxic_psi.kind}:quarantined" 
        if self.actuator:
            await self.actuator.actuate_psi(toxic_psi)

        current_boundaries = getattr(self.interpreter.anchor, 'recept_boundaries', None)
        stable_anchor = AnchorFlow.bootstrap(current_boundaries)
        
        # 패닉 복구 시에도 inter.node 규격으로 재초기화
        self.interpreter = NodeInterpreter(broker=self.broker, anchor=stable_anchor)
        
        if self.dispatcher:
            self.dispatcher.default_handler = self._create_phase_handler() 
            
        self.log.signal(f"System restored to Phase: {self.interpreter.phase}")

    async def register_node(self):
        capabilities = {}
        for fqn, meta in self.local_manifold.items():
            contract = meta.contract
            capabilities[fqn] = {
                "requires": list(contract.requires),
                "emits": list(contract.emits)
            }
        data = {
            "node_id": self.node_id,
            "started_at": time.time(),
            "capabilities": json.dumps(capabilities)
        }
        if self.executor:
            data["executor_type"] = type(self.executor).__name__

        node_key = runtime_keys.get("node", node_id=self.node_id)
        await self.tunnel.hset(node_key, mapping=data)
        
        for fqn, cap in capabilities.items():
            for req_key in cap["requires"]:
                req_idx_key = runtime_keys.get("index_requires", req_key=req_key)
                await self.tunnel.sadd(req_idx_key, self.node_id)
            for emit_key in cap["emits"]:
                emit_idx_key = runtime_keys.get("index_emits", emit_key=emit_key)
                await self.tunnel.sadd(emit_idx_key, self.node_id)
                
        self.log.signal(f"Master Node registered with {len(capabilities)} capabilities.")

    async def deregister_node(self):
        if not self.tunnel: return
        
        node_key = runtime_keys.get("node", node_id=self.node_id)
        await self.tunnel.delete(node_key)
        
        for fqn, meta in self.local_manifold.items():
            contract = meta.contract
            for req_key in contract.requires:
                req_idx_key = runtime_keys.get("index_requires", req_key=req_key)
                await self.tunnel.srem(req_idx_key, self.node_id)
            for emit_key in contract.emits:
                emit_idx_key = runtime_keys.get("index_emits", emit_key=emit_key)
                await self.tunnel.srem(emit_idx_key, self.node_id)
                
        self.log.info("Node and capability indexes deregistered.")

_node_instance: Optional[NodeRuntime] = None

async def main_async():
    global _node_instance
    completion_signal = asyncio.Event()
    executor = SwarmExecutor(completion_signal)
    _node_instance = NodeRuntime(executor=executor)

    await _node_instance.start()
    await _node_instance.wait_until_stopped()

async def teardown():
    if _node_instance and getattr(_node_instance, 'running', False):
        await _node_instance.shutdown()