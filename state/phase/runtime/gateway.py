# xphi.state.phase.runtime.gateway
## @lineage: xphi.state.runtime.gateway
## @lineage: xphi.kernel.phase.runtime.gateway
## @lineage: kernel.phase.runtime.gateway
## @lineage: kernel.phase.boot
import os
import sys
import asyncio
import subprocess

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.watcher.receptor.policy.router import RoutingPolicyEngine, ClusterStateMesh
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("runtime.gateway")

class RuntimeGateway:
    @classmethod
    async def assemble(cls, node) -> None:
        topology = os.getenv("GATEWAY_TOPOLOGY", "EMBEDDED_BYPASS")
        log.info(f"[Orchestrator] Assembling Unified Membrane in {topology} mode.")

        broker_facade = await TunnelFactory.get_default()
        policy_engine = RoutingPolicyEngine(broker_facade)
        state_mesh = ClusterStateMesh(broker_facade)
        await policy_engine.synchronize_initial_state()

        asyncio.create_task(policy_engine.watch_policy_updates())
        asyncio.create_task(state_mesh.start_mesh_sync())

        if topology == "EXT_PROC":
            stream_handler = ExtProcStreamHandler(policy_engine, state_mesh)
            asyncio.create_task(stream_handler.serve())
        else:
            log.info(f"[Orchestrator] Running in {topology} mode. (Ingress Receptor removed)")