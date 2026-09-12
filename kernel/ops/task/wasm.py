# xphi.kernel.ops.task.wasm
import os
import json
import asyncio
import uuid
import time
from pathlib import Path
from typing import Optional
from contextlib import suppress

from xphi.kernel.ops.daemon.base import AbstractDaemon
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter, flow_scope
from xphi.kernel.wasm.cgroup import CgroupPolicy
from xphi.kernel.wasm.method import DphiMethod
from xphi.kernel.ops.task.strategy import ExecutionStrategy

TIME_ROOT = resolve_path("time")

REDIS_CONTROL_CHANNEL = "wasm:control:req"
REDIS_STREAM_TOPIC    = "wasm:execute:stream"
REDIS_GROUP_NAME      = "wasm_tasker_group"

DEFAULT_WASM_PATH     = "dphi.wasm"
DEFAULT_TIER          = "STANDARD"
DEFAULT_CONCURRENCY   = 5
POLL_TIMEOUT_MS       = 1000

class TaskWasm(AbstractDaemon):
    def __init__(self, tunnel, supervisor, node_id: str = None, default_wasm_path: str = DEFAULT_WASM_PATH):
        super().__init__("TaskWasm")
        self.tunnel = tunnel
        self.supervisor = supervisor
        
        self.node_id = node_id or f"tasker-{uuid.uuid4().hex[:6]}-{os.getpid()}"
        self.default_wasm_path = default_wasm_path
        
        self.control_channel = REDIS_CONTROL_CHANNEL
        self.topic = REDIS_STREAM_TOPIC
        self.group_name = REDIS_GROUP_NAME
        self.consumer_name = f"consumer-{self.node_id}"
        
        self.poll_timeout_ms = POLL_TIMEOUT_MS
        self.default_tier = DEFAULT_TIER
        self.concurrency_limit = DEFAULT_CONCURRENCY
        
        self.fetch_batch_size = 1 
        
        self._wasm_pool = asyncio.Queue(maxsize=self.concurrency_limit)
        self._pubsub_task: Optional[asyncio.Task] = None
        self.strategy = ExecutionStrategy(prewarm_pool_size=self.concurrency_limit)

    async def _init_consumer_group(self):
        try:
            await self.tunnel.state_store.xgroup_create(name=self.topic, groupname=self.group_name, id='0', mkstream=True)
            self.log.info(f"Consumer Group '{self.group_name}' initialized for Secure Execution.")
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                self.log.error(f"Failed to init Consumer Group: {e}")

    async def _init_wasm_pool(self):
        self.log.info(f"[{self.node_id}] Pre-warming {self.concurrency_limit} WASM instances...")
        from xphi.state.inter.wasm import WasmInterpreter
        
        for _ in range(self.concurrency_limit):
            interp = WasmInterpreter(
                wasm_module_path=str(self._resolve_wasm_path(self.default_wasm_path)),
                policy=self._get_policy_from_tier(self.default_tier)
            )
            self._wasm_pool.put_nowait(interp)
        self.log.info(f"[{self.node_id}] WASM Instance Pool initialized successfully.")

    def _is_expired(self, context: dict, default_timeout_sec: float = 15.0) -> bool:
        ts_val = float(context.get("timestamp", 0))
        if ts_val <= 0: return False
        
        req_ts = ts_val / 1000.0 if ts_val > 1e11 else ts_val
        elapsed = time.time() - req_ts
        
        timeout_sec = float(context.get("timeout", default_timeout_sec))
        if elapsed > 86400 * 365: return False 
        return elapsed > timeout_sec

    async def run(self):
        await self._init_consumer_group()
        await self._init_wasm_pool()
        
        self._pubsub_task = asyncio.create_task(self._listen_pubsub())
        self.log.info(f"TaskWasm listening: Stream[{self.topic}] | PubSub[{self.control_channel}] (Max Concurrency: {self.concurrency_limit}, PID: {os.getpid()})")
        
        try:
            while self.running:
                streams = await self.tunnel.stream_consume(
                    topic=self.topic, group=self.group_name,
                    consumer=self.consumer_name, count=self.fetch_batch_size, block=self.poll_timeout_ms
                )
                if not streams:
                    continue

                for stream_name, messages in streams:
                    for message_id, msg_data in messages:
                        json_payload = msg_data.get("data", msg_data.get(b"data", b"{}"))
                        if isinstance(json_payload, bytes):
                            json_payload = json_payload.decode('utf-8')
                        
                        try:
                            data = json.loads(json_payload)
                            job_id = data.get("job_id", "unknown")
                            context = data.get("context", {})
                            
                            if self._is_expired(context):
                                await self.tunnel.stream_ack(self.topic, self.group_name, message_id)
                                continue

                            interp_instance = await self._wasm_pool.get()

                            self.supervisor.create(
                                self._process_and_reply(data, message_id, interp_instance), 
                                name=f"ExecGate-{job_id[:8]}"
                            )
                            
                        except json.JSONDecodeError:
                            self.log.error(f"Invalid JSON payload: {json_payload}")
                            await self.tunnel.stream_ack(self.topic, self.group_name, message_id)
                        except Exception as e:
                            self.log.error(f"Failed to schedule WASM task: {e}")

        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.log.error(f"TaskWasm Main Loop Error: {e}", exc_info=True)
        finally:
            if self._pubsub_task and not self._pubsub_task.done():
                self._pubsub_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._pubsub_task
                    
            while not self._wasm_pool.empty():
                interp = self._wasm_pool.get_nowait()
                interp.shutdown()

    async def _listen_pubsub(self):
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe(self.control_channel)
        try:
            async for msg in pubsub.listen():
                if not self.running:
                    break
                if isinstance(msg, dict) and msg.get("type") == "message":
                    ctrl_data = json.loads(msg["data"])
                    await self._handle_control(ctrl_data)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.log.error(f"TaskWasm PubSub Error: {e}")
        finally:
            if hasattr(pubsub, 'unsubscribe'):
                await pubsub.unsubscribe(self.control_channel)
            await pubsub.close()

    def _resolve_wasm_path(self, wasm_path: str) -> Path:
        path = Path(wasm_path)
        return path if path.is_absolute() else TIME_ROOT / path

    async def _handle_control(self, payload: dict):
        job_id = payload.get("job_id", "unknown")
        response_channel = payload.get("response_channel")
        tier = payload.get("tier", self.default_tier).upper()
        context = payload.get("context", {})
        
        with flow_scope(**context):
            self.default_tier = tier
            self.log.info(f"[{job_id[:8]}] Global Cgroup default tier updated -> {tier}")
            if response_channel:
                await self.tunnel.publish(response_channel, json.dumps({"success": True, "job_id": job_id}))

    def _get_policy_from_tier(self, tier_str: str) -> CgroupPolicy:
        tier_str = (tier_str or self.default_tier).upper()
        if tier_str == "SYSTEM": return CgroupPolicy.system()
        if tier_str == "UNLIMITED": return CgroupPolicy.custom(mem_mb=1024, fuel=10_000_000_000)
        return CgroupPolicy.standard()

    async def _process_and_reply(self, payload: dict, message_id: str, interp_instance):
        response_channel = payload.get("response_channel")
        job_id = payload.get("job_id", "unknown")
        target_func = payload.get("target_func", DphiMethod.EXECUTE_CODE.value)

        try:
            if target_func in (DphiMethod.EXECUTE_CODE.value, DphiMethod.EXECUTE_DVM.value):
                response_data = await asyncio.to_thread(self._execute_heavy_sandbox, payload, interp_instance)
            else:
                response_data = self._execute_fast_wasm(payload, interp_instance)

            if isinstance(response_data, dict):
                response_data["job_id"] = job_id
                if "metrics" not in response_data:
                    response_data["metrics"] = {}
                response_data["metrics"]["handled_by_node"] = self.node_id
                response_data["metrics"]["handled_by_pid"] = os.getpid()
            
            if response_channel:
                await self.tunnel.publish(response_channel, json.dumps(response_data))
                
        except Exception as e:
            self.log.error(f"Failed to process execution payload: {e}", exc_info=True)
            if response_channel:
                err_payload = {"success": False, "error": f"Tasker Daemon Error: {str(e)}", "job_id": job_id}
                await self.tunnel.publish(response_channel, json.dumps(err_payload))
        finally:
            await self.tunnel.stream_ack(self.topic, self.group_name, message_id)
            self._wasm_pool.put_nowait(interp_instance)

    def _execute_fast_wasm(self, payload: dict, interp_instance) -> dict:
        job_id = payload.get("job_id", "unknown")
        target_func = payload.get("target_func")
        exec_data = payload.get("payload", payload.get("data", ""))
        context = payload.get("context", {})
        
        tier_str = payload.get("tier", self.default_tier)
        job_policy = self._get_policy_from_tier(tier_str)
        if hasattr(interp_instance, 'apply_policy'):
            interp_instance.apply_policy(job_policy)
            
        try:
            exec_payload = exec_data if isinstance(exec_data, str) else json.dumps(exec_data)
            res = interp_instance.invoke(
                target_func=target_func,
                payload=exec_payload,
                context=context
            )
            metrics = interp_instance.get_metrics()
            if res.success:
                return {"success": True, "output": res.output, "metrics": metrics}
            else:
                return {"success": False, "output": "", "error": str(res.error), "metrics": metrics}
        except Exception as e:
            return {"success": False, "output": "", "error": f"Invoke Error: {e}", "metrics": {}}

    def _execute_heavy_sandbox(self, payload: dict, interp_instance) -> dict:
        job_id = payload.get("job_id", "unknown")
        target_func = payload.get("target_func", DphiMethod.EXECUTE_CODE.value)
        wasm_path = payload.get("wasm_path", self.default_wasm_path)
        tier_str = payload.get("tier", self.default_tier)
        job_policy = self._get_policy_from_tier(tier_str)
        
        if hasattr(interp_instance, 'apply_policy'):
            interp_instance.apply_policy(job_policy)

        exec_data = payload.get("payload", payload.get("data", ""))
        context = payload.get("context", {})

        with flow_scope(**context):
            target_path = self._resolve_wasm_path(wasm_path)
            
            if not target_path.exists():
                error_msg = f"WASM binary not found: {target_path}"
                self.log.warning(f"[{job_id[:8]}] {error_msg}")
                return {"success": False, "output": "", "error": error_msg}

            safe_payload = exec_data
            if target_func == DphiMethod.EXECUTE_DVM.value:
                return self.strategy.run_dvm_sandbox(target_path, job_policy, safe_payload, context, job_id, self.log)
            else:
                return self.strategy.run_python_sandbox(job_policy, safe_payload, context, job_id, self.log)