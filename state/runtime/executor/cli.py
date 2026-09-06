# xphi.state.runtime.executor.cli
## @lineage: xphi.kernel.phase.runtime.executor.cli
## @lineage: kernel.phase.runtime.executor.cli
import os
import sys
import uuid
import json
import asyncio
import argparse
import subprocess
from dataclasses import asdict
from typing import Callable, Any
from pathlib import Path

from xphi.arch.event.psi import PsiEvent, PsiCarrier
from xphi.arch.event.next import next_id, LogEvent
from xphi.arch.contract.registry.unified import registry
from xphi.state.runtime.executor.base import BaseExecutor

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.kernel.space.bind.resolver import get_invoker
from xphi.kernel.ops.task.event import TaskSummaryEvent, TaskDetailRecord
from xphi.kernel.ops.daemon.bootstrap import TOPIC_BUS_STREAM, KEY_HEARTBEAT_PATTERN

from xphi.watcher.plane.emitter import get_emitter, flow_scope
from xphi.watcher.plane.regulator import console_surface

log = get_emitter("cli.executor")

def parse_local(argv):
    """Parse global CLI arguments including execution routing and dynamic timeouts."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--local", action="store_true", help="Force local execution")
    parser.add_argument("--timeout", type=float, default=60.0, help="Timeout in seconds for node reflection")
    return parser.parse_known_args(argv)

def dispatch_cli(command_name: str, entry_func: Callable, file_path: str):
    """
    @role: Universal Execution Router
    @desc: Routes CLI inputs to either local execution or injects them into the Topological Manifold (Tunnel).
    """
    bound_args, remain = parse_local(sys.argv[1:])
    
    if bound_args.local:
        ## @dimension.1: Local Physical Execution
        log.info(f"[Router] Routing {command_name} to Local Process.")
        task = entry_func(remain)
        task.run() 
    else:
        ## @dimension.2: Topological Event (Ψ) Injection
        log.info(f"[Router] Injecting {command_name} to Topological Manifold.")
        invoker, command = get_invoker(Path(file_path))
        
        ## @refined.payload: Inject dynamic timeout into context
        payload = { 
            "_context": {
                "invoker": str(invoker), 
                "command": command, 
                "cli_args": remain,
                "timeout": bound_args.timeout
            } 
        }
        
        task = entry_func(remain)
        execute_cli_task(task_instance=task, command_name=command_name, payload=payload)

class _GenericCliExecutor(BaseExecutor):
    """bound(transduction) - external CLI → internal Ψ execution"""
    def __init__(self, task_instance, completion_signal: asyncio.Event):
        super().__init__()
        self.task_instance = task_instance
        self.completion_signal = completion_signal
        self.node = None

    async def execute(self, psi) -> list:
        command = psi.carrier.tag if hasattr(psi, 'carrier') else "CLI_TASK"
        task_id = getattr(psi, 'event_id', f"task-{next_id()}")

        detail_key = f"{command.lower()}:cli:{task_id}"
        latest_pointer_key = f"{command.lower()}:cli:latest"
        
        self.log.info(f"[exec] Executing CLI task: {command} ({task_id})")
        
        # [개선] 노드 의존성 대신 범용 비동기 터널 획득 (공용 환경)
        tunnel = await TunnelFactory.get_default()
        
        with flow_scope(flow_id=task_id, phase="EXECUTION"):
            self.log.info(f"[exec] Starting flow: {task_id}")
            try:
                raw_result = self.task_instance.run() or {}
                detail_record = TaskDetailRecord(
                    task_id=task_id,
                    command=command,
                    status=raw_result.get("status", "SUCCESS"),
                    artifacts=raw_result.get("artifacts", {}),
                    metrics=raw_result.get("metrics", {})
                )

                summary_event = TaskSummaryEvent(
                    task_id=task_id,
                    command=command,
                    status=detail_record.status,
                    summary=raw_result.get("summary", f"Task {command} completed."),
                    detail_key=detail_key,
                    details=raw_result.get("details", {})
                )

                _project_to_stdout(summary_event)

                ## Store in Universal Tunnel and publish reflection
                # [개선] 인프라 백엔드(Redis/Kafka)와 무관하게 파사드를 통해 저장 및 발행
                await tunnel.set(detail_key, detail_record.to_json(), ex=3600)
                await tunnel.set(latest_pointer_key, detail_key, ex=3600)
                await asyncio.sleep(0.05)

                response_channel = psi.context.get("response_channel")
                if response_channel:
                    await tunnel.publish(response_channel, summary_event.to_json())
                    self.log.info(f"[exec] Reflection published to {response_channel}")
                
                self.log.info(f"[exec] Detailed artifacts saved -> Tunnel[{detail_key}]")

                ## Publish result event to internal bus for swarm synchronization
                result_carrier = PsiCarrier(kind="RESULT", tag=command, payload=asdict(summary_event))
                result_event = PsiEvent(
                    event_id=f"res-{task_id}",
                    parent_id=getattr(psi, 'event_id', None),
                    source_id="cli.executor",
                    scope="GLOBAL",
                    tick=1,
                    carrier=result_carrier
                )

                if self.node and getattr(self.node, 'bus', None):
                    await self.node.bus.publish(result_event)
            except Exception as e:
                self.log.error(f"[exec] Task Failed: {e}")
                import traceback; traceback.print_exc()
            finally:
                self.completion_signal.set()
                
        return []


def _project_to_stdout(summary_event: TaskSummaryEvent):
    """@topos.phase: Ψ → local surface projection (stdout)"""
    try:
        print(f"\n[{summary_event.status}] {summary_event.command}")
        print(f"{summary_event.summary}")
        print(f"detail_key: {summary_event.detail_key}")

        details = getattr(summary_event, "details", None)
        if isinstance(details, dict):
            print("\n## Grouped Result")
            for k, v in details.items():
                print(f"\n[{k}] ({len(v)})")
                for item in v[:5]:
                    print(f" - {item.get('namespace')}")
    except Exception:
        pass

class CliTaskAdapter:
    """Universal adapter converting raw business logic into standard executor dictionaries."""
    def __init__(self, target_func: Callable, **kwargs):
        self.target_func = target_func
        self.kwargs = kwargs

    def run(self) -> dict:
        try:
            raw_result = self.target_func(**self.kwargs)
            if isinstance(raw_result, dict) and "status" in raw_result:
                return raw_result
                
            return {
                "status": "SUCCESS",
                "summary": "Task completed successfully.",
                "artifacts": [],
                "details": raw_result
            }
        except Exception as e:
            return {
                "status": "FAILED",
                "summary": f"Task failed: {str(e)}",
                "details": {"error": str(e)}
            }


async def _async_run_in_node(task_instance, command_name: str, payload: dict):
    # [개선] 외부 주입용 독립형(Isolated) 비동기 터널 획득
    tunnel = await TunnelFactory.get_isolated()
    
    ## Extract dynamic timeout from payload (Defaults to 60s)
    timeout_sec = payload.get("_context", {}).get("timeout", 60.0)
    
    ## Ambient Node Spawning Logic
    ## @step.1: Assess swarm availability by counting registered nodes (Refined: use heartbeat)
    active_node_keys = await tunnel.keys(KEY_HEARTBEAT_PATTERN)
    node_count = len(active_node_keys)
    
    ## @step.2: Determine spawn conditions based on active nodes
    # [수정] Stream 환경에 맞춰 xlen 오판 로직을 제거하고, Cold Start(0대)일 때만 띄우도록 단순화
    should_spawn = (node_count == 0)
    
    if should_spawn:
        ## @step.3: Prevent spawn storms using a distributed lock (10s expiry)
        lock_acquired = await tunnel.set("runtime:spawn_lock", "LOCKED", nx=True, ex=10)
        
        if lock_acquired:
            log.info("[CLI] No active nodes detected. Spawning background Ambient Node...")
            
            # Spawn node process as a background daemon
            subprocess.Popen(
                [sys.executable, "-m", "plane.node.runtime"],
                stdout=subprocess.DEVNULL, 
                stderr=subprocess.DEVNULL,
                start_new_session=True,    
                env=os.environ.copy()      
            )
            
            ## @step.4: Deterministic boot wait for node registration (Max 7.5s)
            for _ in range(15):  
                await asyncio.sleep(0.5)
                current_nodes = await tunnel.keys(KEY_HEARTBEAT_PATTERN)
                if len(current_nodes) > node_count:
                    break
        else:
            # Wait if another process is currently spawning a node
            log.info("[CLI] Another process is spawning a node. Waiting for boot sequence...")
            for _ in range(15):
                await asyncio.sleep(0.5)
                if not await tunnel.exists("runtime:spawn_lock"):
                    break
    
    task_id = f"task-{uuid.uuid4().hex[:8]}"
    with flow_scope(flow_id=task_id, phase="CLI"):
        response_channel = f"res:{task_id}"
        log_channel = f"log:{task_id}"

        # [개선] 범용 PubSub 획득
        pubsub = tunnel.pubsub()
        await pubsub.subscribe(response_channel, log_channel)
        log.info(f"[CLI] Listening on {response_channel} & {log_channel}...")

        ## Inject event (Ψ) into the manifold
        trigger_event = PsiEvent(
            event_id=task_id,
            source_id=f"{command_name}:proxy",
            scope="GLOBAL",
            parent_id=None,
            tick=1,
            carrier=PsiCarrier(kind="COMMAND", tag=command_name.lower(), payload=payload),
            context={"response_channel": response_channel}
        )
        
        # [핵심 변경] 레거시 queue lpush 제거 -> Stream XADD 주입 방식으로 변경 (Double-hop 방지)
        event_json = json.dumps(asdict(trigger_event))
        await tunnel.state_store.xadd(TOPIC_BUS_STREAM, {"data": event_json})

        try:
            ## Wait for node reflection with Dynamic Timeout
            async with asyncio.timeout(timeout_sec):
                # [개선] UniversalPubSub의 제너레이터 사용
                async for msg in pubsub.listen():
                    if msg["type"] != "message":
                        continue

                    channel = msg["channel"]
                    if channel == log_channel:
                        try:
                            log_data = json.loads(msg["data"])
                            
                            ## Ignore self-emitted CLI logs to prevent echo
                            if log_data.get("context", {}).get("phase") == "CLI":
                                continue

                            ## Safely filter fields for LogEvent mapping
                            from dataclasses import fields
                            valid_fields = {f.name for f in fields(LogEvent)}
                            clean_data = {k: v for k, v in log_data.items() if k in valid_fields}
                            
                            event = LogEvent(**clean_data)
                            console_surface.update(event)
                        except Exception:
                            continue 
                            
                    elif channel == response_channel:
                        try:
                            data = json.loads(msg["data"])
                            print_formatted_result(data)
                            return 
                        except Exception as e:
                            log.error(f"[CLI] Failed to parse result data: {e}")
                            break 
                            
        except TimeoutError:
            log.error(f"[CLI] Task timed out. Node failed to reflect within {timeout_sec}s.")
        finally:
            # [개선] 안전한 자원 해제(Teardown)를 보장
            await pubsub.close()
            if hasattr(tunnel.state_store, 'aclose'):
                await tunnel.state_store.aclose()


def execute_cli_task(task_instance, command_name: str = "run", payload: dict = None):
    """@topos.entry: external trigger → Ψ injection"""
    payload = payload or {}
    try:
        asyncio.run(_async_run_in_node(task_instance, command_name, payload))
    except KeyboardInterrupt:
        log.info("[CLI] Task interrupted by user.")


def print_formatted_result(data: dict):
    """Render rich CLI output based on the response payload."""
    status = data.get("status", "UNKNOWN")
    command = data.get("command", "task")
    summary = data.get("summary", "")
    details = data.get("details", {})
    detail_key = data.get("detail_key", "")

    color_code = "\033[92m" if status == "SUCCESS" else "\033[91m"
    reset_code = "\033[0m"
    
    print(f"\n{color_code}[{status}]{reset_code} {command}")
    print(f"{summary}")

    if isinstance(details, dict) and details:
        print("\n## Execution Details")
        for category, items in details.items():
            if isinstance(items, list):
                print(f"\n └─ [{category}] ({len(items)} items)")
                for item in items[:5]:
                    if isinstance(item, dict):
                        ns = item.get('namespace') or item.get('path') or str(item)
                        print(f"    - {ns}")
                if len(items) > 5:
                    print(f"    ... and {len(items)-5} more.")
            else:
                print(f" └─ {category}: {items}")

    print(f"\n(Full artifacts saved at Tunnel State Store -> {detail_key})")