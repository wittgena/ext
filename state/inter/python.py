# xphi.state.inter.python
## @lineage: xphi.kernel.phase.inter.python
## @lineage: kernel.phase.inter.python
import functools
import inspect
import json
import keyword
import os
import subprocess
import threading
import time
import select
import shutil
from os import PathLike
from pathlib import Path
from typing import Any, Callable, Mapping
from contextlib import suppress

from arch.contract.interpreter import PRIMITIVE_TYPES, ExecutionError, ProtocolError, ExecutionResult, JsonRpcMessage, JsonRpcErrorCode
from xphi.kernel.space.bind.resolver import find_current_self, resolve_path
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.wasm.cgroup import CgroupPolicy, Tier

log = get_emitter("inter.python", phase="SYSTEM")
TIME_ROOT = resolve_path("time")
LARGE_VAR_THRESHOLD = 100 * 1024 * 1024

class PythonInterpreter:
    def __init__(
        self,
        deno_command: list[str] | None = None,
        enable_read_paths: list[PathLike | str] | None = None,
        enable_write_paths: list[PathLike | str] | None = None,
        enable_env_vars: list[str] | None = None,
        enable_network_access: list[str] | None = None,
        sync_files: bool = True,
        policy: CgroupPolicy | None = None,
    ) -> None:
        if isinstance(deno_command, dict):
            raise TypeError("deno_command must be a list of strings")

        self.enable_read_paths = enable_read_paths or []
        self.enable_write_paths = enable_write_paths or []
        self.enable_env_vars = enable_env_vars or []
        self.enable_network_access = enable_network_access or []
        self.sync_files = sync_files
        self.policy = policy or CgroupPolicy.standard()

        if deno_command:
            self.deno_command = list(deno_command)
        else:
            args = ["deno", "run", "--no-prompt"]
            
            allowed_read_paths = [str(Path(self._get_runner_path()).resolve())]
            deno_dir = self._get_deno_dir()
            if deno_dir:
                allowed_read_paths.append(str(Path(deno_dir).resolve()))
            
            if self.enable_read_paths:
                allowed_read_paths.extend(str(Path(p).resolve()) for p in self.enable_read_paths)
            if self.enable_write_paths:
                allowed_read_paths.extend(str(Path(p).resolve()) for p in self.enable_write_paths)
                
            args.append(f"--allow-read={','.join(set(allowed_read_paths))}")

            if self.enable_write_paths:
                resolved_writes = [str(Path(p).resolve()) for p in self.enable_write_paths]
                args.append(f"--allow-write={','.join(set(resolved_writes))}")

            if self.enable_network_access:
                args.append(f"--allow-net={','.join(str(x) for x in self.enable_network_access)}")

            self._env_arg = ""
            if self.enable_env_vars:
                user_vars = [str(v).strip() for v in self.enable_env_vars]
                args.append("--allow-env=" + ",".join(user_vars))
                self._env_arg = ",".join(user_vars)

            args.append(self._get_runner_path())
            if self._env_arg:
                args.append(self._env_arg)
            self.deno_command = args

        self.deno_process = None
        self._mounted_files = False
        self._request_id = 0
        self._owner_thread: int | None = None
        self._pending_large_vars = {}
        
        self.execution_timeout = 15.0

    def _check_thread_ownership(self) -> None:
        current_thread = threading.current_thread().ident
        if self._owner_thread is None:
            self._owner_thread = current_thread
        elif self._owner_thread != current_thread:
            raise RuntimeError("PythonInterpreter is not thread-safe. Instantiate per thread.")

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def _get_deno_dir() -> str | None:
        if "DENO_DIR" in os.environ:
            return os.environ["DENO_DIR"]
        try:
            result = subprocess.run(
                ["deno", "info", "--json"], capture_output=True, text=True, check=False
            )
            if result.returncode == 0:
                info = json.loads(result.stdout)
                return info.get("denoDir")
        except Exception:
            log.warning("Unable to find the Deno cache dir.")
        return None

    def _get_runner_path(self) -> str:
        return os.path.join(TIME_ROOT, "pysand.ts")

    def _mount_files(self):
        if self._mounted_files:
            return
        paths_to_mount = []
        if self.enable_read_paths:
            paths_to_mount.extend(self.enable_read_paths)
        if self.enable_write_paths:
            paths_to_mount.extend(self.enable_write_paths)
        if not paths_to_mount:
            return
            
        for path in paths_to_mount:
            if not path:
                continue
            if not os.path.exists(path):
                if self.enable_write_paths and path in self.enable_write_paths:
                    open(path, "a").close()
                else:
                    raise ProtocolError(f"Cannot mount non-existent file: {path}")
            virtual_path = f"/sandbox/{os.path.basename(path)}"
            self._send_request("mount_file", {"host_path": str(path), "virtual_path": virtual_path}, f"mounting {path}")
        self._mounted_files = True

    def _sync_files(self):
        if not self.enable_write_paths or not self.sync_files:
            return
        for path in self.enable_write_paths:
            virtual_path = f"/sandbox/{os.path.basename(path)}"
            sync_msg = JsonRpcMessage.notification("sync_file", {"virtual_path": virtual_path, "host_path": str(path)})
            self.deno_process.stdin.write(sync_msg + "\n")
            self.deno_process.stdin.flush()

    def _extract_parameters(self, fn: Callable) -> list[dict]:
        sig = inspect.signature(fn)
        params = []
        for name, param in sig.parameters.items():
            p = {"name": name}
            if param.annotation != inspect.Parameter.empty:
                if param.annotation in PRIMITIVE_TYPES:
                    p["type"] = param.annotation.__name__
            if param.default != inspect.Parameter.empty:
                p["default"] = param.default
            params.append(p)
        return params

    def _register_callables(self, callables: Mapping[str, Callable[..., Any]] | None) -> None:
        if not callables:
            return

        callables_info = []
        for name, fn in callables.items():
            callables_info.append({
                "name": name,
                "parameters": self._extract_parameters(fn)
            })

        self._send_request("register", {"tools": callables_info}, "registering callables")

    def _handle_callable_call(self, request: dict, callables: Mapping[str, Callable[..., Any]]) -> None:
        request_id = request["id"]
        params = request.get("params", {})
        callable_name = params.get("name")
        kwargs = params.get("kwargs", {})

        try:
            if callable_name not in callables:
                raise ExecutionError(f"Security Violation: Unknown callable '{callable_name}' requested by Sandbox.")
            
            result = callables[callable_name](**kwargs)
            is_json = isinstance(result, (list, dict))
            response = JsonRpcMessage.result(
                {"value": json.dumps(result) if is_json else (str(result) if result is not None else ""), "type": "json" if is_json else "string"},
                request_id
            )
        except Exception as e:
            log.warning(f"Callable '{callable_name}' failed in host: {e}")
            error_type = type(e).__name__
            error_code = JsonRpcErrorCode.from_exception_type(error_type)
            response = JsonRpcMessage.error(error_code, str(e), request_id, {"type": error_type})

        try:
            if self.deno_process and self.deno_process.stdin:
                self.deno_process.stdin.write(response + "\n")
                self.deno_process.stdin.flush()
        except (BrokenPipeError, AttributeError):
            pass 

    def _apply_cgroup_policy(self) -> None:
        use_fuel = self.policy.cpu_fuel_quota if self.policy.tier == Tier.STANDARD else None
        params = {
            "fuel": use_fuel,
            "mem_bytes": self.policy.max_memory_bytes
        }
        self._send_request("apply_cgroup", params, "Applying Cgroup Policy")

    def apply_policy(self, policy: CgroupPolicy) -> None:
        """런타임에 Cgroup 정책(Tier, 리소스 제한 등)을 동적으로 변경하고 Deno 샌드박스에 즉시 적용합니다."""
        self.policy = policy
        if self.deno_process and self.deno_process.poll() is None:
            self._apply_cgroup_policy()

    def _ensure_deno_process(self) -> None:
        # 기존 프로세스가 깨졌거나 죽어있다면 완벽하게 수거(Cleanup) 후 재생성
        if self.deno_process is not None:
            if self.deno_process.poll() is not None:
                self.shutdown()
            else:
                return # 정상 동작 중

        self._mounted_files = False
        try:
            deno_exe = self.deno_command[0]
            if not os.path.isabs(deno_exe):
                resolved_exe = shutil.which(deno_exe)
                if not resolved_exe:
                    raise FileNotFoundError(f"{deno_exe} executable not found in host PATH")
                self.deno_command[0] = resolved_exe

            safe_env = {
                "TMPDIR": os.environ.get("TMPDIR", "/tmp")
            }
            
            deno_dir = self._get_deno_dir()
            if deno_dir:
                safe_env["DENO_DIR"] = deno_dir
            
            if self.enable_env_vars:
                for k in self.enable_env_vars:
                    if k in os.environ:
                        safe_env[k] = os.environ[k]
                        
            self.deno_process = subprocess.Popen(
                self.deno_command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, 
                text=True,
                encoding="UTF-8",
                env=safe_env
            )
        except FileNotFoundError as e:
            raise ProtocolError("Deno executable not found.") from e
        
        self._health_check()
        self._apply_cgroup_policy()

    _MAX_SKIP_LINES = 100

    def _read_response_line(self, context: str) -> str:
        start_time = time.time()
        
        while True:
            # 파이프가 닫혔을 경우를 대비한 검증
            if not self.deno_process or not self.deno_process.stdout:
                raise ProtocolError(f"Deno process or stdout is closed during {context}")

            ready, _, _ = select.select([self.deno_process.stdout], [], [], 0.1)
            
            if ready:
                response_line = self.deno_process.stdout.readline().strip()
                if response_line:
                    return response_line
            
            if time.time() - start_time > self.execution_timeout:
                log.debug(f"Deno execution timed out ({self.execution_timeout}s) during {context}. Forcing termination.")
                self.shutdown()
                raise ProtocolError(f"Execution Timeout ({self.execution_timeout}s) {context}")

            exit_code = self.deno_process.poll()
            if exit_code is not None:
                self.shutdown() # 즉시 자원 회수
                raise ProtocolError(f"Deno exited unexpectedly (code {exit_code}) {context}")

    def _parse_response_line(self, response_line: str, context: str) -> dict | None:
        if not response_line.startswith("{"):
            log.debug(f"[Deno Output Leak] {context}: {response_line}")
            return None
        try:
            return json.loads(response_line)
        except json.JSONDecodeError:
            log.error(f"[Deno JSON Error] {context}: {response_line[:500]}")
            return None

    def _send_request(self, method: str, params: dict, context: str) -> dict:
        self._request_id += 1
        request_id = self._request_id
        msg = JsonRpcMessage.request(method, params, request_id)
        self.deno_process.stdin.write(msg + "\n")
        self.deno_process.stdin.flush()

        skipped = 0
        while skipped <= self._MAX_SKIP_LINES:
            response_line = self._read_response_line(context)
            response = self._parse_response_line(response_line, context)
            if response is None:
                skipped += 1
                continue

            if response.get("id") != request_id:
                log.debug(f"Skipping mismatched JSON response {context}: {response}")
                skipped += 1
                continue
                
            if "error" in response:
                raise ProtocolError(f"Error {context}: {response['error'].get('message', 'Unknown error')}")
            return response

        raise ProtocolError(f"Too many non-JSON lines ({skipped}) {context}")

    def _health_check(self) -> None:
        response = self._send_request("execute", {"code": "print(1+1)"}, "during health check")
        if response.get("result", {}).get("output", "").strip() != "2":
            raise ProtocolError(f"Unexpected ping response: {response}")

    def _to_json_compatible(self, value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        elif isinstance(value, dict):
            return {k: self._to_json_compatible(v) for k, v in value.items()}
        elif isinstance(value, (list, tuple)):
            return [self._to_json_compatible(v) for v in value]
        elif isinstance(value, set):
            try:
                return sorted(self._to_json_compatible(v) for v in value)
            except TypeError:
                return [self._to_json_compatible(v) for v in value]
        else:
            raise ExecutionError(f"Unsupported value type: {type(value).__name__}")

    def _inject_variables(self, code: str, variables: Mapping[str, Any]) -> str:
        for key in variables:
            if not key.isidentifier() or keyword.iskeyword(key) or key == "json":
                raise ExecutionError(f"Invalid variable name: '{key}'")

        large_vars = {}
        small_assignments = []
        for k, v in variables.items():
            serialized = self._serialize_value(v)
            if len(serialized) > LARGE_VAR_THRESHOLD:
                large_vars[k] = json.dumps(self._to_json_compatible(v))
            else:
                small_assignments.append(f"{k} = {serialized}")

        self._pending_large_vars = large_vars

        if large_vars:
            large_assignments = [f"{k} = json.loads(open('/tmp/spi_vars/{k}.json').read())" for k in large_vars]
            assignments = ["import json"] + small_assignments + large_assignments
        else:
            assignments = small_assignments

        return "\n".join(assignments) + "\n" + code if assignments else code

    def _serialize_value(self, value: Any) -> str:
        if value is None:
            return "None"
        elif isinstance(value, str):
            return repr(value)
        elif isinstance(value, bool):
            return "True" if value else "False"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, (list, tuple)):
            items = ", ".join(self._serialize_value(item) for item in value)
            return f"[{items}]"
        elif isinstance(value, dict):
            items = ", ".join(f"{self._serialize_value(k)}: {self._serialize_value(v)}" for k, v in value.items())
            return f"{{{items}}}"
        elif isinstance(value, set):
            try:
                sorted_items = sorted(value)
            except TypeError:
                sorted_items = list(value)
            items = ", ".join(self._serialize_value(item) for item in sorted_items)
            return f"[{items}]"
        else:
            raise ExecutionError(f"Unsupported value type: {type(value).__name__}")

    def _inject_large_var(self, name: str, value: str) -> None:
        self._send_request("inject_var", {"name": name, "value": value}, f"injecting variable '{name}'")

    def get_metrics(self) -> dict:
        if not self.deno_process or self.deno_process.poll() is not None:
            return {}
        try:
            res = self._send_request("get_metrics", {}, "fetching metrics")
            if "result" in res:
                metrics = res["result"]
                metrics["tier"] = self.policy.tier.value
                return metrics
        except Exception as e:
            log.warning(f"Failed to fetch sandbox metrics: {e}")
        return {}

    def execute(
        self,
        code: str,
        variables: Mapping[str, Any] | None = None,
        callables: Mapping[str, Callable[..., Any]] | None = None,
        context: dict | None = None,
    ) -> ExecutionResult:
        self._check_thread_ownership()
        variables = variables or {}
        callables = callables or {}
        context = context or {}

        if "timestamp" not in context:
            context["timestamp"] = 0
        if "seed" not in context:
            context["seed"] = "dphi_secure_fallback_seed"
        
        try:
            code = self._inject_variables(code, variables)
        except ExecutionError as e:
            return ExecutionResult(success=False, error=e)

        self._ensure_deno_process()
        self._mount_files()
        
        self._register_callables(callables)

        for name, value in self._pending_large_vars.items():
            self._inject_large_var(name, value)

        self._request_id += 1
        execute_request_id = self._request_id
        payload = {
            "code": code,
            "context": context
        }
        input_data = JsonRpcMessage.request("execute", payload, execute_request_id)
        log.info(f"[Deno RPC Request] -> {input_data}")

        try:
            self.deno_process.stdin.write(input_data + "\n")
            self.deno_process.stdin.flush()
        except BrokenPipeError:
            log.warning("Broken pipe during execute. Restarting Deno process.")
            self.shutdown() # 기존 핸들 명시적 삭제 (핵심 누수 차단)
            self._ensure_deno_process()
            self._mount_files()
            self._register_callables(callables)
            for name, value in self._pending_large_vars.items():
                self._inject_large_var(name, value)
            
            try:
                self.deno_process.stdin.write(input_data + "\n")
                self.deno_process.stdin.flush()
            except BrokenPipeError as e:
                raise ProtocolError("Deno process crashed continuously during execution.") from e

        skipped = 0
        while skipped <= self._MAX_SKIP_LINES:
            output_line = self._read_response_line("during execution")
            msg = self._parse_response_line(output_line, "during execution")
            if msg is None:
                skipped += 1
                continue

            if "method" in msg:
                if msg["method"] == "tool_call":
                    self._handle_callable_call(msg, callables)
                    continue

            if "result" in msg:
                if msg.get("id") != execute_request_id:
                    log.debug(f"Skipping mismatched JSON response during execution: {msg}")
                    skipped += 1
                    continue
                
                result = msg["result"]
                self._sync_files()
                return ExecutionResult(success=True, output=result.get("output", ""))

            if "error" in msg:
                if msg.get("id") is not None and msg.get("id") != execute_request_id:
                    log.debug(f"Skipping mismatched JSON error during execution: {msg}")
                    skipped += 1
                    continue
                
                error = msg["error"]
                error_message = error.get("message", "Unknown error")
                error_data = error.get("data", {})
                error_type = error_data.get("type", "Error")
                return ExecutionResult(
                    success=False, 
                    error=ExecutionError(f"{error_type}: {error_data.get('args') or error_message}")
                )
            raise ProtocolError(f"Unexpected message format from sandbox: {msg}")
        raise ProtocolError(f"Too many non-JSON lines ({skipped}) during execution")

    def start(self) -> None:
        self._ensure_deno_process()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.shutdown()

    def __call__(
        self,
        code: str,
        variables=None,
        callables=None,
        context: dict | None = None
    ) -> ExecutionResult:
        return self.execute(code, variables, callables, context)

    def shutdown(self) -> None:
        """가장 강력한 방어선. 프로세스와 모든 파이프를 명시적으로 파기합니다."""
        if self.deno_process:
            if self.deno_process.poll() is None:
                try:
                    # 1. 우아한 종료 시도
                    if self.deno_process.stdin:
                        self.deno_process.stdin.write(JsonRpcMessage.notification("shutdown") + "\n")
                        self.deno_process.stdin.flush()
                        self.deno_process.stdin.close()
                    # 타임아웃을 1초로 짧게 주어 장애 상황에 빠르게 반응
                    self.deno_process.wait(timeout=1.0) 
                except (BrokenPipeError, subprocess.TimeoutExpired, OSError, AttributeError):
                    # 2. 실패 시 하드 킬(Hard Kill) 및 좀비 수거(Reap)
                    try:
                        self.deno_process.kill()
                        self.deno_process.wait(timeout=1.0)
                    except Exception as e:
                        log.error(f"Failed to force kill Deno process: {e}")
            
            # 3. 파이프 명시적 차단 (File Descriptor 누수 방지)
            if self.deno_process.stdout:
                with suppress(Exception):
                    self.deno_process.stdout.close()
                    
        self.deno_process = None
        self._owner_thread = None

    def __del__(self):
        """객체가 GC될 때, 닫히지 않은 Deno 프로세스가 있다면 동반 자살(Kill)시킵니다."""
        with suppress(Exception):
            self.shutdown()