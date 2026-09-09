# xphi.state.inter.wasm
## @lineage: xphi.kernel.phase.inter.wasm
import json
import os
import threading
from os import PathLike
from pathlib import Path
from typing import Any, Callable, Mapping

try:
    import wasmtime
except ImportError:
    wasmtime = None

from xphi.arch.contract.interpreter import ExecutionError, ProtocolError, ExecutionResult
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.wasm.cgroup import WasmCgroup, CgroupPolicy

TIME_ROOT = resolve_path("time")

log = get_emitter("inter.wasm", phase="SYSTEM")

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

class WasmInterpreter:
    def __init__(
        self,
        wasm_module_path: str = "dphi.wasm",
        enable_read_paths: list[PathLike | str] | None = None,
        enable_write_paths: list[PathLike | str] | None = None,
        enable_env_vars: list[str] | None = None,
        sync_files: bool = True,
        policy: CgroupPolicy | None = None,
    ) -> None:
        if wasmtime is None:
            raise ImportError("The 'wasmtime' module is required. Please install it.")
        
        self.wasm_module_path = wasm_module_path
        self.enable_read_paths = enable_read_paths or []
        self.enable_write_paths = enable_write_paths or []
        self.enable_env_vars = enable_env_vars or []
        self.sync_files = sync_files

        self.engine = None
        self.store = None
        self.module = None
        self.instance = None
        self.memory = None
        
        self._wasm_alloc = None
        self._wasm_dealloc = None
        self._wasm_invoke = None
        
        self._invoke_shared = None
        self._get_ptr = None
        self._get_size = None
        self.shared_ptr = None
        self.shared_size = 0
        
        self.valid_methods = set()
        cg_policy = policy or CgroupPolicy.standard()
        self.cg = WasmCgroup(cgroup_name=f"worker-{id(self)}", policy=cg_policy)
        
        self.current_timestamp = 0.0 

    def apply_policy(self, policy: CgroupPolicy) -> None:
        self.cg.policy = policy
        if self.store is not None:
            if hasattr(self.cg, 'apply_to_store'):
                self.cg.apply_to_store(self.store)

    def _ensure_engine_started(self) -> None:
        if self.instance is not None:
             return
             
        try:
            self.engine, self.module = get_cached_module(self.wasm_module_path, self.cg)
            wasi_config = wasmtime.WasiConfig()
            wasi_config.inherit_stdout()
            wasi_config.inherit_stderr()
            
            for path in self.enable_read_paths:
                wasi_config.preopen_dir(str(path), f"/sandbox/read/{os.path.basename(str(path))}")
            for path in self.enable_write_paths:
                 wasi_config.preopen_dir(str(path), f"/sandbox/write/{os.path.basename(str(path))}")
            
            env = []
            for var in self.enable_env_vars:
                 if var in os.environ:
                     env.append((var, os.environ[var]))
            wasi_config.env = env
            
            self.store = wasmtime.Store(self.engine)
            self.store.set_wasi(wasi_config)
            self.cg.apply_to_store(self.store)
            
            linker = wasmtime.Linker(self.engine)
            linker.define_wasi()

            self.instance = linker.instantiate(self.store, self.module)
            self.memory = self.instance.exports(self.store)["memory"]
            
            exports = self.instance.exports(self.store)
            
            self._wasm_alloc = exports.get("alloc")
            self._wasm_dealloc = exports.get("dealloc")
            self._wasm_invoke = exports.get("invoke_wasm")
            
            if not self._wasm_alloc or not self._wasm_dealloc or not self._wasm_invoke:
                raise ProtocolError("WASM module missing legacy memory exports")

            self._get_ptr = exports.get("get_shared_buffer_ptr")
            self._get_size = exports.get("get_shared_buffer_size")
            self._invoke_shared = exports.get("invoke_shared")
            
            if self._get_ptr and self._get_size and self._invoke_shared:
                self.shared_ptr = self._get_ptr(self.store)
                self.shared_size = self._get_size(self.store)

        except Exception as e:
            raise ProtocolError(f"Failed to initialize Wasmtime engine: {e}")

    def _run_wasm_function(self, target_func_name: str, payload: Any, context: dict | None = None) -> str:
        self._ensure_engine_started()
        
        actual_payload = payload
        if isinstance(payload, str):
            try:
                actual_payload = json.loads(payload)
            except json.JSONDecodeError:
                pass

        context = context or {}
        if "seed" not in context:
            context["seed"] = "dphi_secure_fallback_seed"

        routed_request = {
            "method": target_func_name,
            "context": context,
            "payload": actual_payload
        }
        
        payload_bytes = json.dumps(routed_request).encode('utf-8')
        req_len = len(payload_bytes)
        
        if self.shared_ptr is not None and req_len <= self.shared_size:
            self.memory.write(self.store, payload_bytes, self.shared_ptr)
            res_len = self._invoke_shared(self.store, req_len)
            
            if res_len == 0:
                raise ExecutionError("WASM shared buffer execution failed.")
            result_bytes = self.memory.read(self.store, self.shared_ptr, self.shared_ptr + res_len)
            return result_bytes.decode('utf-8')
        else:
            code_ptr = self._wasm_alloc(self.store, req_len)
            res_ptr = None
            res_len_legacy = None
            try:
                self.memory.write(self.store, payload_bytes, code_ptr)
                result_packed = self._wasm_invoke(self.store, code_ptr, req_len)
                
                res_ptr = result_packed >> 32
                res_len_legacy = result_packed & 0xFFFFFFFF
                result_bytes = self.memory.read(self.store, res_ptr, res_ptr + res_len_legacy)
                return result_bytes.decode('utf-8')
            finally:
                if code_ptr is not None:
                    self._wasm_dealloc(self.store, code_ptr, req_len)
                if res_ptr is not None and res_len_legacy is not None:
                    self._wasm_dealloc(self.store, res_ptr, res_len_legacy)

    def invoke(self, target_func: str, payload: str, context: dict | None = None) -> ExecutionResult:
        self.current_timestamp = float(context.get("timestamp", 0.0)) if context else 0.0
        try:
            result_str = self._run_wasm_function(target_func, payload, context=context)
            result_data = json.loads(result_str)
            
            if result_data.get("success", False):
                return ExecutionResult(success=True, output=json.dumps(result_data.get("data", {})))
            else:
                return ExecutionResult(success=False, error=ExecutionError(result_data.get("error", "Failed")))
        except Exception as e:
            return ExecutionResult(success=False, error=ExecutionError(f"WASM Invoke Failed: {e}"))

    def execute(
        self,
        code: str,
        variables: Mapping[str, Any] | None = None,
        callables: Mapping[str, Callable[..., Any]] | None = None,
        context: dict | None = None,
    ) -> ExecutionResult:
        variables = variables or {}
        self.current_timestamp = float(context.get("timestamp", 0.0)) if context else 0.0
        
        try:
             result_str = self._run_wasm_function("execute_code", {"code": code, "variables": variables}, context=context)
             result_data = json.loads(result_str)
             
             if result_data.get("success", False):
                 inner_data = result_data.get("data", {})
                 return ExecutionResult(success=True, output=inner_data.get("output", ""))
             else:
                 return ExecutionResult(success=False, error=ExecutionError(result_data.get("error", "Error")))
        except Exception as e:
             return ExecutionResult(success=False, error=ExecutionError(f"WASM Execution Failed: {e}"))

    def get_metrics(self) -> dict:
        if not self.store or not self.memory: return {}
        return self.cg.inspect_metrics(self.store, self.memory)

    def shutdown(self) -> None:
        self.engine = self.store = self.module = self.instance = self.memory = None

    def __enter__(self): return self
    def __exit__(self, *_): self.shutdown()