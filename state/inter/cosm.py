# xphi.state.inter.cosm
## @lineage: xphi.kernel.phase.inter.cosm
## @lineage: kernel.phase.inter.cosm
## @lineage: kernel.bind.inter.cosm
"""@desc: Local interpreter for secure CosmWasm smart contract execution using Wasmtime"""
import json
import threading
import os
import struct
from pathlib import Path
from typing import Any, Dict, Optional, Union
from contextlib import suppress

try:
    import wasmtime
except ImportError:
    wasmtime = None

from arch.contract.interpreter import ExecutionError, ExecutionResult
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.kernel.wasm.cgroup import WasmCgroup, CgroupPolicy
from xphi.watcher.plane.emitter import get_emitter

TIME_ROOT = resolve_path("time")
log = get_emitter("inter.cosm", phase="SYSTEM")

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
            log.info(f"⚙️ [AOT Compile] Compiling CosmWasm Contract: {Path(wasm_path).name}...")
            if not os.path.exists(wasm_path):
                raise FileNotFoundError(f"CosmWasm Artifact not found: {wasm_path}")
            _GLOBAL_MODULE_CACHE[wasm_path] = wasmtime.Module.from_file(_GLOBAL_ENGINE, wasm_path)
            log.info(f"✅ [AOT Compile] {Path(wasm_path).name} cached successfully.")
            
        return _GLOBAL_ENGINE, _GLOBAL_MODULE_CACHE[wasm_path]

class CosmWasmInterpreter:
    def __init__(
        self,
        wasm_module_name: str,
        policy: Optional[CgroupPolicy] = None,
        initial_state: Optional[Dict[str, str]] = None
    ) -> None:
        if wasmtime is None:
            raise ImportError("The 'wasmtime' package is required.")
            
        self.wasm_module_path = str(Path(TIME_ROOT) / wasm_module_name)
        self.policy = policy or CgroupPolicy.standard()
        self.cg = WasmCgroup(cgroup_name=f"cosm-worker-{id(self)}", policy=self.policy)
        
        self.state_db: Dict[bytes, bytes] = {
            k.encode('utf-8'): v.encode('utf-8') for k, v in (initial_state or {}).items()
        }
        self.state_diff: Dict[str, Optional[str]] = {}
        
        self.engine = self.store = self.module = self.instance = self.memory = None
        self._wasm_allocate = self._wasm_deallocate = self._wasm_execute = None
        
    def _ensure_engine_started(self) -> None:
        if self.instance is not None:
             return
             
        try:
            self.engine, self.module = get_cached_module(self.wasm_module_path, self.cg)
            self.store = wasmtime.Store(self.engine)
            self.cg.apply_to_store(self.store)
            
            linker = wasmtime.Linker(self.engine)
            
            def db_read_cb(key_ptr: int) -> int:
                key_bytes = self._read_region(self.store, key_ptr)
                val_bytes = self.state_db.get(key_bytes)
                if val_bytes is None: return 0 
                return self._write_to_region(self.store, val_bytes)

            def db_write_cb(key_ptr: int, val_ptr: int):
                key_bytes = self._read_region(self.store, key_ptr)
                val_bytes = self._read_region(self.store, val_ptr)
                self.state_db[key_bytes] = val_bytes
                self.state_diff[key_bytes.decode('utf-8', errors='ignore')] = val_bytes.decode('utf-8', errors='ignore')

            def db_remove_cb(key_ptr: int):
                key_bytes = self._read_region(self.store, key_ptr)
                self.state_db.pop(key_bytes, None)
                self.state_diff[key_bytes.decode('utf-8', errors='ignore')] = None

            def db_scan_cb(s_ptr: int, e_ptr: int, order: int) -> int: return 0
            def db_next_cb(iter_id: int) -> int: return 0
            def query_chain_cb(req_ptr: int) -> int: return 0
            def debug_cb(msg_ptr: int): pass
            def abort_cb(msg_ptr: int): pass
            
            def addr_validate_cb(s_ptr: int) -> int: return 0
            def addr_canonicalize_cb(s_ptr: int, d_ptr: int) -> int: return 0
            def addr_humanize_cb(s_ptr: int, d_ptr: int) -> int: return 0
            
            def secp256k1_verify_cb(h_p: int, s_p: int, pk_p: int) -> int: return 0
            def secp256k1_recover_pubkey_cb(h_p: int, s_p: int, rp: int) -> int: return 0
            def ed25519_verify_cb(m_p: int, s_p: int, pk_p: int) -> int: return 0
            def ed25519_batch_verify_cb(m_p: int, s_p: int, pk_p: int) -> int: return 0

            i32 = wasmtime.ValType.i32()
            i64 = wasmtime.ValType.i64()
            
            linker.define_func("env", "db_read", wasmtime.FuncType([i32], [i32]), db_read_cb)
            linker.define_func("env", "db_write", wasmtime.FuncType([i32, i32], []), db_write_cb)
            linker.define_func("env", "db_remove", wasmtime.FuncType([i32], []), db_remove_cb)
            
            linker.define_func("env", "db_scan", wasmtime.FuncType([i32, i32, i32], [i32]), db_scan_cb)
            linker.define_func("env", "db_next", wasmtime.FuncType([i32], [i32]), db_next_cb)
            linker.define_func("env", "query_chain", wasmtime.FuncType([i32], [i32]), query_chain_cb)
            linker.define_func("env", "debug", wasmtime.FuncType([i32], []), debug_cb)
            linker.define_func("env", "abort", wasmtime.FuncType([i32], []), abort_cb)
            
            linker.define_func("env", "addr_validate", wasmtime.FuncType([i32], [i32]), addr_validate_cb)
            linker.define_func("env", "addr_canonicalize", wasmtime.FuncType([i32, i32], [i32]), addr_canonicalize_cb)
            linker.define_func("env", "addr_humanize", wasmtime.FuncType([i32, i32], [i32]), addr_humanize_cb)
            
            linker.define_func("env", "secp256k1_verify", wasmtime.FuncType([i32, i32, i32], [i32]), secp256k1_verify_cb)
            linker.define_func("env", "secp256k1_recover_pubkey", wasmtime.FuncType([i32, i32, i32], [i64]), secp256k1_recover_pubkey_cb)
            linker.define_func("env", "ed25519_verify", wasmtime.FuncType([i32, i32, i32], [i32]), ed25519_verify_cb)
            linker.define_func("env", "ed25519_batch_verify", wasmtime.FuncType([i32, i32, i32], [i32]), ed25519_batch_verify_cb)
            
            self.instance = linker.instantiate(self.store, self.module)
            exports = self.instance.exports(self.store)
            self.memory = exports["memory"]
            
            self._wasm_allocate = exports.get("allocate")
            self._wasm_deallocate = exports.get("deallocate")
            self._wasm_execute = exports.get("execute")
            
        except Exception as e:
            raise ExecutionError(f"Failed to initialize CosmWasm engine: {e}")

    def _read_region(self, caller: Union['wasmtime.Caller', 'wasmtime.Store'], ptr: int) -> bytes:
        memory = self.memory if isinstance(caller, wasmtime.Store) else caller.get("memory")
        region_header = bytes(memory.read(caller, ptr, ptr + 12))
        offset, capacity, length = struct.unpack("<III", region_header)
        return bytes(memory.read(caller, offset, offset + length))

    def _write_to_region(self, caller: Union['wasmtime.Caller', 'wasmtime.Store'], data: bytes) -> int:
        region_ptr = self._wasm_allocate(caller, len(data))
        memory = self.memory if isinstance(caller, wasmtime.Store) else caller.get("memory")
        
        region_header = bytes(memory.read(caller, region_ptr, region_ptr + 12))
        offset, capacity, length = struct.unpack("<III", region_header)
        
        memory.write(caller, data, offset)
        memory.write(caller, struct.pack("<I", len(data)), region_ptr + 8) 
        
        return region_ptr

    def _prepare_json_arg(self, data_dict: dict) -> int:
        json_bytes = json.dumps(data_dict).encode('utf-8')
        return self._write_to_region(self.store, json_bytes)

    def execute(self, env_data: dict, info_data: dict, msg_data: dict) -> ExecutionResult:
        try:
            self._ensure_engine_started()
            
            env_ptr = self._prepare_json_arg(env_data)
            info_ptr = self._prepare_json_arg(info_data)
            msg_ptr = self._prepare_json_arg(msg_data)
            
            res_ptr = self._wasm_execute(self.store, env_ptr, info_ptr, msg_ptr)
            
            result_bytes = self._read_region(self.store, res_ptr)
            result_json = json.loads(result_bytes.decode('utf-8'))
            
            if self._wasm_deallocate:
                self._wasm_deallocate(self.store, res_ptr)
                
            response_payload = {
                "success": True,
                "gas_used": 0,
                "output": result_json,
                "state_diff": self.state_diff,
                "logs": [],
                "revert_reason": None
            }
            
            return ExecutionResult(success=True, output=json.dumps(response_payload))
            
        except Exception as e:
            log.error(f"CosmWasm Execution Failed: {e}", exc_info=True)
            return ExecutionResult(success=False, error=ExecutionError(f"Execute Fatal: {str(e)}"))

    def shutdown(self) -> None:
        self.engine = self.store = self.module = self.instance = self.memory = None

    def __enter__(self): return self
    def __exit__(self, *_): self.shutdown()