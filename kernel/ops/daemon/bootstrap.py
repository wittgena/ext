# xphi.kernel.ops.daemon.bootstrap
import os
import asyncio
import json
import time
from abc import ABC, abstractmethod
from typing import Optional, Callable, Awaitable, List, Tuple
from contextlib import suppress
from types import SimpleNamespace

from xphi.kernel.space.topos.tunnel.factory import UniversalFacade
from xphi.arch.event.psi import PsiEvent, PsiCarrier, CarrierType
from xphi.arch.event.bus import AsyncEventBus
from xphi.arch.event.next import next_id
from xphi.arch.contract.registry.unified import registry

from xphi.kernel.ops.daemon.base import AbstractDaemon
from xphi.kernel.ops.task.supervisor import TaskSupervisor, Dispatcher
from xphi.state.phase.runtime.context import RuntimeContext
from xphi.state.phase.runtime.sensor import SurfaceSensor
from xphi.watcher.plane.emitter import get_emitter
from xphi.state.phase.executor.cont import LoopCarrier, DynamicsXe

log = get_emitter("daemon.bootstrap")
SENSOR_INTERVAL = 1.0

TOPIC_BUS_STREAM        = "runtime:bus:stream"
GROUP_NODE_MANIFOLD     = "node_manifold_group"

KEY_HEARTBEAT_PREFIX    = "runtime:heartbeat:"
KEY_HEARTBEAT_PATTERN   = "runtime:heartbeat:*"
KEY_ACTIVE              = "runtime:active"

class EventBusDaemon(AbstractDaemon):
    def __init__(self, tunnel: UniversalFacade, dispatcher: Dispatcher, node_id: str, 
                 idle_timeout: float, shutdown_hook: Callable[[], Awaitable[None]], 
                 group_name: str = GROUP_NODE_MANIFOLD):
        super().__init__("EventBus")
        self.tunnel = tunnel
        self.dispatcher = dispatcher
        self.node_id = node_id
        self.shutdown_hook = shutdown_hook
        
        self.topic = TOPIC_BUS_STREAM
        self.group_name = group_name
        self.consumer_name = f"consumer-{self.node_id}"
        self.poll_timeout_ms = 1000

    async def _init_consumer_group(self):
        try:
            await self.tunnel.state_store.xgroup_create(name=self.topic, groupname=self.group_name, id='$', mkstream=True)
            self.log.info(f"Consumer Group '{self.group_name}' initialized.")
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                self.log.error(f"Failed to initialize Consumer Group: {e}")

    async def run(self):
        self.log.info(f"EventBusDaemon started. Waiting for Sticky Events (Topic: {self.topic}, Consumer: {self.consumer_name})")
        await self._init_consumer_group()
        
        while self.running:
            try:
                streams = await self.tunnel.stream_consume(
                    topic=self.topic, group=self.group_name, consumer=self.consumer_name,
                    count=1, block=self.poll_timeout_ms
                )

                if not streams:
                    continue

                for stream_name, messages in streams:
                    for message_id, msg_data in messages:
                        json_payload = msg_data.get("data", msg_data.get(b"data", b"{}"))
                        if isinstance(json_payload, bytes):
                            json_payload = json_payload.decode('utf-8')
                        
                        try:
                            event_dict = json.loads(json_payload)
                            if 'carrier' in event_dict and isinstance(event_dict['carrier'], dict):
                                event_dict['carrier'] = PsiCarrier(**event_dict['carrier'])
                                
                            event = PsiEvent(**event_dict)
                            if event.carrier.kind == "system:shutdown":
                                self.log.warn("Shutdown signal received via Event Stream. Evaporating...")
                                asyncio.create_task(self.shutdown_hook())
                                break
                                
                            self.dispatcher.dispatch(event)
                        except Exception as e:
                            self.log.error(f"Event parsing or dispatch error: {e}")
                        
                        # [FIX] 어떤 경우에도 ACK는 날려 파이프라인 정체 방지
                        await self.tunnel.stream_ack(self.topic, self.group_name, message_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Stream Consume Error: {e}")
                await asyncio.sleep(1)


class HeartbeatDaemon(AbstractDaemon):
    def __init__(self, tunnel: UniversalFacade, node_id: str, supervisor: TaskSupervisor, role: str = "master", capacity: int = 0):
        super().__init__("Heartbeat")
        self.tunnel = tunnel
        self.node_id = node_id
        self.supervisor = supervisor
        self.role = role
        self.capacity = capacity

    async def run(self):
        self.log.info(f"HeartbeatDaemon initiated. Node [{self.node_id}] emitting vital signs (Role: {self.role}, Capacity: {self.capacity})...")
        try:
            while self.running:
                meta_payload = {
                    "ts": int(time.time()),
                    "role": self.role,
                    "capacity": self.capacity
                }
                meta_json = json.dumps(meta_payload)
                
                try:
                    await asyncio.gather(
                        self.tunnel.set(f"{KEY_HEARTBEAT_PREFIX}{self.node_id}", meta_json, ex=10),
                        self.tunnel.set(KEY_ACTIVE, int(time.time()), ex=10) 
                    )
                except Exception as e:
                    self.log.error(f"Redis connection drop during heartbeat sync: {e}")
                
                try:
                    active_tasks = len(self.supervisor.get_active_tasks())
                    payload = {
                        "signal_id": f"node_load_{self.node_id}",
                        "value": float(active_tasks)
                    }
                    await self.tunnel.publish("meta.self:signals:phase_mutation", json.dumps(payload))
                except Exception as e:
                    self.log.warning(f"Failed to emit load metric: {e}")

                await asyncio.sleep(1.0) 
        except asyncio.CancelledError:
            self.log.warn("HeartbeatDaemon received cancellation signal.")
        except Exception as e:
            self.log.error(f"Fatal HeartbeatDaemon Error: {e}", exc_info=True)


class DynamicsDaemon(AbstractDaemon):
    def __init__(self, bus: AsyncEventBus, sensor: Optional[SurfaceSensor] = None):
        super().__init__("Dynamics")
        self.bus = bus
        self.sensor = sensor
        self.carriers: List[Tuple[str, LoopCarrier]] = []

    async def start(self) -> asyncio.Task:
        self.log.info("DynamicsDaemon scanning registry for sensor kernels...")
        kernels_map = getattr(registry, "_kernels", {})
        
        for name, kernel_class in kernels_map.items():
            if name.startswith("sensor."):
                try:
                    kernel_instance = kernel_class() 
                    xe_core = DynamicsXe(bound=kernel_instance, ex=f"genesis.{name}")
                    carrier = LoopCarrier(xe=xe_core, max_ticks=999999999, interval=0.5)
                    carrier.node = SimpleNamespace(bus=self.bus)
                    self.carriers.append((name, carrier))
                except Exception as e:
                    self.log.error(f"Failed to bootstrap kernel '{name}': {e}", exc_info=True)
        
        self.log.info(f"Discovered and bound {len(self.carriers)} kernels.")
        return await super().start()

    async def _legacy_sensor_loop(self):
        if not self.sensor: return
        self.log.info("Surface Sensor loop started.")
        while self.running:
            try:
                signals = await self.sensor.sense()
                for psi in signals:
                    await self.bus.publish(psi) 
                await asyncio.sleep(SENSOR_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Surface Sensor Error: {e}")
                await asyncio.sleep(2)

    async def run(self):
        tasks = []
        if self.sensor:
            tasks.append(asyncio.create_task(self._legacy_sensor_loop()))
            
        for name, carrier in self.carriers:
            initial_psi = self._create_genesis_psi(name)
            tasks.append(asyncio.create_task(carrier.execute(initial_psi)))
            
        if not tasks:
            self.log.warn("No sensors or dynamic kernels found. Idling...")
            while self.running:
                await asyncio.sleep(1)
            return

        try:
            # [FIX] return_exceptions=True 적용하여 하나의 센서가 죽어도 전체 데몬이 죽지 않도록 방어
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    self.log.error(f"[Fault Isolated] A dynamics sub-task crashed unexpectedly: {result}")
        except asyncio.CancelledError:
            self.log.warn("DynamicsDaemon execution cancelled.")
        except Exception as e:
            self.log.error(f"Fatal error in DynamicsDaemon run loop: {e}", exc_info=True)
            
    def _create_genesis_psi(self, name: str) -> PsiEvent:
        carrier = PsiCarrier(kind="genesis", tag=name, payload={}, carrier_type=CarrierType.FIXED)
        return PsiEvent(
            event_id=next_id(), parent_id=None, source_id=f"daemon.dynamics.{name}",
            scope="GLOBAL", tick=0, carrier=carrier, phase_id=0, context={"domain": "kernel.bootstrap"}
        )


# =========================================================================
# [개선] 노드 프로파일(profile) 파라미터를 수신하여 위상 분리 지원
# =========================================================================
def mount_master_layer(supervisor: TaskSupervisor, ctx: RuntimeContext, profile: str = "ALL"):
    """
    profile 매개변수를 기반으로 현재 노드에 필요한 인프라 데몬만 선별하여 마운트합니다.
    (기본값 "ALL"을 유지하여 기존 하위 호환성 100% 보장)
    """
    
    # 1. 공통 필수 데몬: Heartbeat는 프로파일과 무관하게 항상 동작 (네트워크 상태 보고용)
    # 기존 코드와의 사이드 이펙트 차단을 위해 "ALL"일 때는 기존 명칭인 "master"를 그대로 사용
    assigned_role = profile.lower() if profile != "ALL" else "master"
    
    master_daemons = [
        HeartbeatDaemon(
            tunnel=ctx.tunnel, 
            node_id=ctx.node_id,
            supervisor=supervisor,
            role=assigned_role,     
            capacity=0         
        )
    ]
    
    # 2. 제어/관측 노드 전용 데몬: 무거운 DynamicsDaemon은 EDGE/COMPUTE 노드에서는 스킵
    if profile in ["ALL", "CONTROL"]:
        master_daemons.append(
            DynamicsDaemon(
                bus=ctx.bus,
                sensor=ctx.sensor
            )
        )
    
    # [FIX] 코어 데몬(Infra) 마운트 실패 시에도 예외 격리
    for daemon in master_daemons:
        try:
            supervisor.mount_daemon(daemon)
        except Exception as e:
            log.error(f"Critical Failure: Could not mount master infra daemon '{daemon.name}': {e}", exc_info=True)
    
    log.info(f"Master Infra Layer mount attempt complete. (Profile: {profile})")

    active_daemons_str = os.getenv("KERNEL_DAEMONS", "rest_edge,gateway_edge,risk_vault")
    active_daemons = [d.strip() for d in active_daemons_str.split(",") if d.strip()]
    
    discovered_daemons = getattr(registry, "_daemons", {})
    
    for daemon_name in active_daemons:
        DaemonClass = discovered_daemons.get(daemon_name)
        if DaemonClass:
            try:
                # [FIX] 생성자 예외와 마운트 예외를 안전하게 커버
                daemon_instance = DaemonClass(ctx=ctx)
                supervisor.mount_daemon(daemon_instance)
                log.info(f"App Layer Daemon Mounted on MASTER: {daemon_name} -> {DaemonClass.__name__}")
            except Exception as e:
                log.error(f"[Fault Isolated] Failed to construct or mount daemon '{daemon_name}'. Boot continues. Reason: {e}")
        else:
            log.warning(f"Requested Daemon '{daemon_name}' not found in registry. Skipping.")


def mount_worker_layer(supervisor: TaskSupervisor, ctx: RuntimeContext):
    worker_capacity = 4
    worker_daemons = [
        HeartbeatDaemon(
            tunnel=ctx.tunnel, 
            node_id=ctx.node_id,
            supervisor=supervisor,
            role="worker",
            capacity=worker_capacity
        ),
        EventBusDaemon(
            tunnel=ctx.tunnel,
            dispatcher=ctx.dispatcher, 
            node_id=ctx.node_id,
            idle_timeout=ctx.idle_timeout,
            shutdown_hook=ctx.shutdown_hook,
            group_name=GROUP_NODE_MANIFOLD
        )
    ]
    
    for daemon in worker_daemons:
        try:
            supervisor.mount_daemon(daemon)
        except Exception as e:
            log.error(f"Critical Failure: Could not mount worker daemon '{daemon.name}': {e}", exc_info=True)

    try:
        from xphi.kernel.ops.task.wasm import TaskWasm
        wasm_daemon = TaskWasm(tunnel=ctx.tunnel, supervisor=supervisor)
        wasm_daemon.concurrency_limit = worker_capacity
        supervisor.mount_daemon(wasm_daemon)
        log.info("Worker Plugin Mounted: WasmTaskerDaemon")
    except ImportError as e:
        log.warn(f"WasmTaskerDaemon bypassed (Not installed or import error): {e}")
    except Exception as e:
        log.error(f"[Fault Isolated] Failed to mount WasmTaskerDaemon. Boot continues. Reason: {e}")

    log.info("Worker Data Layer mount attempt complete.")