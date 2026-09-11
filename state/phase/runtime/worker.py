# xphi.state.phase.runtime.worker
## @lineage: xphi.state.runtime.worker
## @lineage: xphi.kernel.phase.runtime.worker
import asyncio
import sys
import os
import importlib

from xphi.arch.event.bus import TunnelEventBus
from xphi.arch.event.psi import PsiEvent

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.kernel.ops.task.supervisor import TaskSupervisor, Dispatcher
from xphi.state.phase.runtime.context import RuntimeContext
from xphi.state.phase.runtime.sensor import SurfaceActuator
from xphi.kernel.ops.daemon.bootstrap import mount_worker_layer
from xphi.kernel.space.bind.resolver import find_current_self
from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.plane.sink import TunnelSink

def worker_process_entry(master_id: str, worker_idx: int):
    asyncio.run(_run_worker_loop(master_id, worker_idx))

async def _run_worker_loop(master_id: str, worker_idx: int):
    worker_id = f"{master_id}-w{worker_idx}"
    log = get_emitter(f"node.worker.{worker_idx}", phase="SYSTEM")
    log.info(f"🚀 Worker Process Booting: [{worker_id}] (PID: {os.getpid()})")

    supervisor = TaskSupervisor(source=f"Worker-{worker_id}")
    tunnel = await TunnelFactory.get_default()
    dispatcher = Dispatcher(supervisor=supervisor)
    
    def _worker_reload_handler(psi: PsiEvent):
        module_fqn = psi.carrier.payload.get("module_fqn")
        if module_fqn and module_fqn in sys.modules:
            try:
                importlib.reload(sys.modules[module_fqn])
                log.info(f"[Plasticity] Topology synchronized in Worker: {module_fqn}")
            except Exception as e:
                log.error(f"[Cleavage] Failed to sync topology in Worker: {e}")
                
    dispatcher.add_route(
        predicate=lambda p: p.carrier.kind == "system:topology" and p.carrier.tag == "reload",
        handler=_worker_reload_handler
    )

    def _ignore_cli_commands(psi: PsiEvent):
        pass 

    dispatcher.add_route(
        predicate=lambda p: hasattr(p, 'carrier') and p.carrier.kind == "COMMAND",
        handler=_ignore_cli_commands
    )

    ctx = RuntimeContext(
        node_id=worker_id,
        tunnel=tunnel,
        bus=TunnelEventBus(tunnel),
        dispatcher=dispatcher,
        sensor=None,
        actuator=SurfaceActuator(TunnelSink(tunnel=tunnel)),
        idle_timeout=0.0,
        watch_dir=find_current_self(),
        shutdown_hook=supervisor.shutdown
    )

    mount_worker_layer(supervisor, ctx)
    
    try:
        while not supervisor._closed:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        log.info(f"Worker {worker_id} received shutdown signal.")
    finally:
        await tunnel.close()
        log.info(f"Worker Process Terminated: [{worker_id}]")