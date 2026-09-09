# xphi.state.inter.dvm
## @lineage: xphi.kernel.phase.inter.dvm
## @lineage: kernel.phase.inter.dvm
import json
import threading
import os
from pathlib import Path
from typing import Any, Dict, Union, Optional
from contextlib import suppress

try:
    import wasmtime
except ImportError:
    wasmtime = None

from xphi.arch.contract.interpreter import ExecutionError, ExecutionResult
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.kernel.wasm.cgroup import WasmCgroup, CgroupPolicy, Tier
from xphi.state.inter.wasm import WasmInterpreter

from xphi.watcher.plane.emitter import get_emitter

TIME_ROOT = resolve_path("time")
log = get_emitter("inter.dvm", phase="SYSTEM")

_GLOBAL_ENGINE = None
_GLOBAL_MODULE_CACHE = {}
_CACHE_LOCK = threading.Lock()

def get_cached_module(wasm_path: str, cg_policy: WasmCgroup):
    global _GLOBAL_ENGINE, _GLOBAL_MODULE_CACHE
    
    with _CACHE_LOCK:
        if _GLOBAL_ENGINE is None:
            config = wasmtime.Config()
            cg_policy.apply_to_config(config)
            _GLOBAL_ENGINE = wasmtime.Engine(config)
            
        if wasm_path not in _GLOBAL_MODULE_CACHE:
            log.info(f"⚙️ [AOT Compile] Compiling {Path(wasm_path).name} (Only once per process)...")
            if not os.path.exists(wasm_path):
                raise FileNotFoundError(f"WASM Artifact not found: {wasm_path}")
            _GLOBAL_MODULE_CACHE[wasm_path] = wasmtime.Module.from_file(_GLOBAL_ENGINE, wasm_path)
            log.info(f"✅ [AOT Compile] {Path(wasm_path).name} cached successfully.")
            
        return _GLOBAL_ENGINE, _GLOBAL_MODULE_CACHE[wasm_path]
# ============================================================================

class DvmInterpreter:
    def __init__(
        self,
        wasm_module_name: str = "dvm.wasm",
        policy: Optional[CgroupPolicy] = None,
    ) -> None:
        if wasmtime is None:
            raise ImportError("The 'wasmtime' package is required. Please install it.")
            
        self.wasm_module_path = str(Path(TIME_ROOT) / wasm_module_name)
        self.policy = policy or CgroupPolicy.standard()
        
        self._execution_count = 0

        # Wasmtime Core
        self.engine = None
        self.store = None
        self.module = None
        self.instance = None
        self.memory = None
        
        # Wasm Exports (ABI)
        self._wasm_alloc = None
        self._wasm_dealloc = None
        self._wasm_execute_router = None
        self.cg = WasmCgroup(cgroup_name=f"multi-vm-worker-{id(self)}", policy=self.policy)
        
        self._ensure_engine_started()

    def _ensure_engine_started(self) -> None:
        if self.instance is not None:
             return
             
        try:
            self.engine, self.module = get_cached_module(self.wasm_module_path, self.cg)
            
            wasi_config = wasmtime.WasiConfig()
            wasi_config.inherit_stdout()
            wasi_config.inherit_stderr()
            
            self.store = wasmtime.Store(self.engine)
            self.store.set_wasi(wasi_config)
            self.cg.apply_to_store(self.store)
            
            linker = wasmtime.Linker(self.engine)
            linker.define_wasi()

            # ==================================================================
            # [Host Escape Hatch: Cross-VM Bridge]
            # ==================================================================
            def invoke_native_vm_callback(input_ptr: int) -> int:
                chunk_size = 256
                result_bytes = bytearray()
                curr_ptr = input_ptr
                while True:
                    chunk = self.memory.read(self.store, curr_ptr, curr_ptr + chunk_size)
                    null_idx = chunk.find(b'\x00')
                    if null_idx != -1:
                        result_bytes.extend(chunk[:null_idx])
                        break
                    result_bytes.extend(chunk)
                    curr_ptr += chunk_size
                    
                json_str = result_bytes.decode('utf-8')
                
                try:
                    payload = json.loads(json_str)
                    vm_target = payload.get("vm_target", "UNKNOWN").upper()
                    
                    if vm_target == "DPHI_KERNEL":
                        log.info("[Host Bridge] Cross-VM Call: dvm.wasm -> dphi.wasm")
                        dphi_method = payload.get("method", "evaluate_tension")
                        dphi_context = payload.get("context", {"injected_anchor": 1, "injected_tick": 0})
                        dphi_payload = payload.get("payload", {})
                        
                        dphi_wasm_path = str(Path(TIME_ROOT) / "dphi.wasm")
                        with WasmInterpreter(dphi_wasm_path, policy=CgroupPolicy.system()) as dphi_kernel:
                            res = dphi_kernel.invoke(dphi_method, json.dumps(dphi_payload), context=dphi_context)
                            
                            if res.success:
                                residue = json.loads(res.output)
                                native_result = {
                                    "success": residue.get("success", False),
                                    "gas_used": 5000,
                                    "output": residue.get("data", "0x"),
                                    "revert_reason": residue.get("error")
                                }
                            else:
                                native_result = {"success": False, "revert_reason": f"DPHI Kernel Panic: {res.error}"}

                    elif vm_target == "COSMWASM_EXTERNAL":
                        log.info("[Host Bridge] Cross-VM Call: dvm.wasm -> External CosmWasm")
                        wasm_file = payload.get("wasm_file", "unknown.wasm")
                        env_data = payload.get("env", {})
                        info_data = payload.get("info", {})
                        msg_data = payload.get("msg", {})
                        
                        state_snapshot = payload.get("state_snapshot") or {}
                        
                        try:
                            from xphi.state.inter.cosm import CosmWasmInterpreter
                            with CosmWasmInterpreter(wasm_module_name=wasm_file, policy=self.policy, initial_state=state_snapshot) as cosm_sandbox:
                                res = cosm_sandbox.execute(env_data, info_data, msg_data)
                                
                                if res.success:
                                    native_result = json.loads(res.output)
                                else:
                                    native_result = {"success": False, "revert_reason": str(res.error)}
                        except Exception as e:
                            native_result = {"success": False, "revert_reason": f"CosmWasm Host Crash: {str(e)}"}
                            
                    else:
                        raise ValueError(f"Unknown Native VM Target: {vm_target}")

                except Exception as e:
                    log.error(f"Cross-VM Execution Failed: {e}", exc_info=True)
                    native_result = {"success": False, "revert_reason": f"Host Bridge Error: {str(e)}"}
                    
                res_bytes = json.dumps(native_result).encode('utf-8') + b'\x00'
                res_ptr = self._wasm_alloc(self.store, len(res_bytes))
                self.memory.write(self.store, res_bytes, res_ptr)
                return res_ptr

            func_type = wasmtime.FuncType([wasmtime.ValType.i32()], [wasmtime.ValType.i32()])
            linker.define_func("env", "invoke_native_vm", func_type, invoke_native_vm_callback)

            self.instance = linker.instantiate(self.store, self.module)
            self.memory = self.instance.exports(self.store)["memory"]
            
            exports = self.instance.exports(self.store)
            self._wasm_alloc = exports.get("alloc")
            self._wasm_dealloc = exports.get("dealloc")
            self._wasm_execute_router = exports.get("execute_router")
            
            if not all([self._wasm_alloc, self._wasm_dealloc, self._wasm_execute_router]):
                raise ExecutionError("dvm.wasm missing required exports: 'alloc', 'dealloc', or 'execute_router'")
        except Exception as e:
            raise ExecutionError(f"Failed to initialize dvm.wasm engine: {e}")

    def execute(
        self,
        vm_target: str,
        target_address: str,
        calldata: str,
        state_snapshot: Union[Dict[str, Any], str],
        context: Union[Dict[str, Any], str, None] = None
    ) -> ExecutionResult:
        self._execution_count += 1
        
        # 0. Type Sanitization
        if isinstance(state_snapshot, str):
            try: state_snapshot = json.loads(state_snapshot)
            except: state_snapshot = {}
        state_snapshot = state_snapshot or {}

        if isinstance(context, str):
            try: context = json.loads(context)
            except: context = {}
        context = context or {}

        gas_limit = self.policy.cpu_fuel_quota if self.policy.tier == Tier.STANDARD else 30_000_000

        # 1. VM Target 에 따른 페이로드 조립 분기
        if vm_target.upper() == "EVM":
            caller = context.get("caller") or context.get("caller_address")
            value = str(context.get("value", "0"))
            block_info = context.get("block", {})
            block_context = None
            if block_info:
                block_context = {
                    "timestamp": int(block_info.get("timestamp", 0)),
                    "block_number": int(block_info.get("block_number", 0)),
                    "coinbase": block_info.get("coinbase"),
                    "chain_id": int(block_info.get("chain_id", 1)) if block_info.get("chain_id") else None
                }

            inner_payload = {
                "target_address": target_address,
                "calldata": calldata,
                "gas_limit": gas_limit,
                "state_snapshot": state_snapshot
            }
            if caller: inner_payload["caller_address"] = caller
            if value and value != "0": inner_payload["value"] = value
            if block_context: inner_payload["block_context"] = block_context

        elif vm_target.upper() == "COSMWASM_INTERNAL":
            inner_payload = {
                "contract_address": target_address,
                "sender": context.get("caller") or context.get("caller_address"),
                "msg": calldata,
                "state_snapshot": state_snapshot
            }
            with suppress(Exception):
                if isinstance(inner_payload["msg"], str):
                    inner_payload["msg"] = json.loads(inner_payload["msg"])
        else:
            return ExecutionResult(success=False, error=ExecutionError(f"Unsupported VM Target: {vm_target}"))

        # 2. 통합 라우팅 페이로드(Unified Input) 패키징
        unified_input = {
            "vm_target": vm_target.upper(),
            "payload": inner_payload
        }
        
        log.info(f"[dvm.wasm] Routing TX to {vm_target.upper()} on {target_address} (Gas Limit: {gas_limit})")
        
        # 🌟🌟🌟 [핵심 디버그 로그 추가] 🌟🌟🌟
        # Rust(WASM)로 넘어가기 직전의 JSON 구조 전체를 화면에 덤프합니다.
        log.info(f"🔥 [DEBUG FFI PAYLOAD] Unified Input to Rust:\n{json.dumps(unified_input, indent=2)}")
        
        try:
            self._ensure_engine_started()
            
            # 3. Rust CStr 포맷: JSON 문자열 끝에 널 바이트('\x00') 추가
            payload_bytes = json.dumps(unified_input).encode('utf-8') + b'\x00'
            req_len = len(payload_bytes)
            
            # 4. WASM Memory 에 할당 후 기록
            code_ptr = self._wasm_alloc(self.store, req_len)
            self.memory.write(self.store, payload_bytes, code_ptr)
            
            try:
                # 5. 라우터 실행
                res_ptr = self._wasm_execute_router(self.store, code_ptr)
                
                if res_ptr == 0:
                    raise ExecutionError("WASM Execution returned null pointer.")
                    
                # 6. WASM 메모리에서 결과 읽어오기 (C-String Read)
                chunk_size = 256
                result_bytes = bytearray()
                curr_ptr = res_ptr
                
                while True:
                    chunk = self.memory.read(self.store, curr_ptr, curr_ptr + chunk_size)
                    null_idx = chunk.find(b'\x00')
                    if null_idx != -1:
                        result_bytes.extend(chunk[:null_idx])
                        break
                    result_bytes.extend(chunk)
                    curr_ptr += chunk_size
                    
                result_str = result_bytes.decode('utf-8')
                
            finally:
                # 7. 파이썬이 할당했던 입력 메모리 공간 해제
                if code_ptr is not None:
                    self._wasm_dealloc(self.store, code_ptr, req_len)
                    
            # 8. 결과 파싱 및 표준화
            result_data = json.loads(result_str)
            success = result_data.get("success", False)
            gas_used = str(result_data.get("gas_used", 0))
            output_hex = result_data.get("output", "0x")
            revert_reason = result_data.get("revert_reason")
            
            logs = result_data.get("logs", [])
            state_diff = result_data.get("state_diff", {})
            state_root = result_data.get("state_root", "0x00...00")

            response_payload = {
                "success": success,
                "gas_used": gas_used,
                "state_root": state_root,
                "output": output_hex,
                "logs": logs,
                "state_diff": state_diff,
                "revert_reason": revert_reason
            }
            return ExecutionResult(success=success, output=json.dumps(response_payload))
            
        except Exception as e:
            return ExecutionResult(success=False, error=ExecutionError(f"dvm Fatal: {str(e)}"))

    def get_metrics(self) -> dict:
        if not self.store or not self.memory:
            return {}
        return self.cg.inspect_metrics(self.store, self.memory)

    def shutdown(self) -> None:
        self.engine = None
        self.store = None
        self.module = None
        self.instance = None
        self.memory = None
        
        self._wasm_alloc = None
        self._wasm_dealloc = None
        self._wasm_execute_router = None
        self._owner_thread = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.shutdown()