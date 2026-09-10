# xphi.arch.contract.protocol.agent
import sys
import json
import logging
import asyncio
import contextvars
from typing import Dict, Any, Optional

## 표준 출력(stdout)을 표준 에러(stderr)로 강제 리다이렉션하여 일반적인 print()나 서드파티 라이브러리의 출력이 JSON-RPC 통신을 오염시키는 것을 방지
_REAL_STDOUT = sys.stdout
sys.stdout = sys.stderr

## 현재 비동기/스레드 컨텍스트에서 실행 중인 요청 ID 추적
current_request_id = contextvars.ContextVar("current_request_id", default="SYSTEM")

class JsonStderrFormatter(logging.Formatter):
    """표준 에러(stderr)로 나가는 로그를 JSON으로 구조화하는 포매터"""
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "req_id": current_request_id.get(),
            "message": record.getMessage()
        }
        # 워커에서 extra 딕셔너리로 넘긴 커스텀 필드 병합
        if hasattr(record, "extra_ctx"):
            log_record.update(record.extra_ctx)

        return json.dumps(log_record)

def _setup_json_logger(agent_name: str) -> logging.Logger:
    logger = logging.getLogger(agent_name)
    logger.setLevel(logging.INFO)

    # 중복 핸들러 방지
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonStderrFormatter())
        logger.addHandler(handler)
        logger.propagate = False
    return logger

class AgentProtocol:
    """@desc: 레거시(동기식) 워커를 위한 베이스 클래스 - 순차적 처리 및 YIELD 시 Blocking 발생"""
    def __init__(self, agent_name: str):
        self.agent_name = agent_name
        self.log = _setup_json_logger(self.agent_name)

    """Single Point of Egress"""
    def _emit_rpc_message(self, message: Dict[str, Any]):
        message["jsonrpc"] = "2.0"
        try:
            raw_out = json.dumps(message) + "\n"
            _REAL_STDOUT.write(raw_out)
            _REAL_STDOUT.flush()
        except TypeError as e:
            self.log.error(f"Payload Serialization Failed: {e}")
            if "error" not in message: 
                self.send_error(message.get("id"), -32603, "Internal Serialization Error")

    """표준 JSON-RPC Message Builders"""
    def send_response(self, req_id: Any, result: Any):
        self._emit_rpc_message({"id": req_id, "result": result})

    def send_error(self, req_id: Any, code: int, message: str, data: Optional[Any] = None):
        err_obj = {"code": code, "message": message}
        if data is not None:
            err_obj["data"] = data
        self._emit_rpc_message({"id": req_id, "error": err_obj})

    def send_request(self, req_id: Any, method: str, params: Optional[Dict[str, Any]] = None):
        msg = {"id": req_id, "method": method}
        if params is not None:
            msg["params"] = params
        self._emit_rpc_message(msg)

    def serve_forever(self):
        """무한 입력 대기 루프 (동기식)"""
        self.log.info(f"Sync Agent '{self.agent_name}' Ignited. Listening on stdin...")
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
                self._route_request(payload)
            except json.JSONDecodeError:
                self.send_error(None, -32700, "Parse error: Invalid JSON")
            except Exception as e:
                self.log.error(f"Internal Fracture: {e}", exc_info=True)
                self.send_error(payload.get("id") if isinstance(payload, dict) else None, -32603, "Internal Server Error")

    def _route_request(self, req: Dict[str, Any]):
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params", {})

        token = current_request_id.set(req_id)
        
        # [보강] 동기 워커 인입 성공 로그
        self.log.info(f"Incoming RPC payload recognized. Method: {method}")
        
        try:
            if method == "initialize":
                self.send_response(req_id, {"protocolVersion": "2026-09-04", "capabilities": {}})
            elif method == "tools/list":
                self.handle_tools_list(req_id)
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                meta = params.get("_meta", {})
                self.handle_tools_call(req_id, tool_name, arguments, meta)
            else:
                self.send_error(req_id, -32601, f"Unknown method: {method}")
        except Exception as e:
            self.log.error(f"Execution Fault in '{method}': {e}", exc_info=True)
            self.send_error(req_id, -32000, str(e))
        finally:
            # 컨텍스트 복원
            current_request_id.reset(token)

    """Abstract Handlers"""
    def handle_tools_list(self, req_id: Any):
        self.send_response(req_id, {"tools": []})

    def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        self.send_error(req_id, -32601, f"Tool '{tool_name}' not implemented")


class AsyncAgentProtocol:
    """@desc: 모던(비동기) 워커를 위한 베이스 클래스 - 코루틴 라우팅 및 Non-blocking I/O 지원"""
    def __init__(self, agent_name: str):
        self.agent_name = agent_name
        self.log = _setup_json_logger(self.agent_name)

        self._stdout_lock = asyncio.Lock()
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

    async def _initialize_streams(self):
        """파이썬 표준 STDIN/STDOUT을 비동기 스트림으로 래핑합니다."""
        loop = asyncio.get_running_loop()
        self._reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(self._reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)

        w_transport, w_protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, _REAL_STDOUT)
        self._writer = asyncio.StreamWriter(w_transport, w_protocol, self._reader, loop)

    """Single Point of Egress (Async)"""
    async def _emit_rpc_message_async(self, message: Dict[str, Any]):
        message["jsonrpc"] = "2.0"
        try:
            raw_out = (json.dumps(message) + "\n").encode('utf-8')
            async with self._stdout_lock:
                self._writer.write(raw_out)
                await self._writer.drain()
        except TypeError as e:
            self.log.error(f"Payload Serialization Failed: {e}")
            if "error" not in message: 
                await self.send_error(message.get("id"), -32603, "Internal Serialization Error")

    """표준 JSON-RPC Message Builders (Async)"""
    async def send_response(self, req_id: Any, result: Any):
        await self._emit_rpc_message_async({"id": req_id, "result": result})

    async def send_error(self, req_id: Any, code: int, message: str, data: Optional[Any] = None):
        err_obj = {"code": code, "message": message}
        if data is not None:
            err_obj["data"] = data
        await self._emit_rpc_message_async({"id": req_id, "error": err_obj})

    async def send_request(self, req_id: Any, method: str, params: Optional[Dict[str, Any]] = None):
        msg = {"id": req_id, "method": method}
        if params is not None:
            msg["params"] = params
        await self._emit_rpc_message_async(msg)

    async def serve_forever_async(self):
        """코루틴 기반 무한 입력 대기 루프"""
        await self._initialize_streams()
        self.log.info(f"Async Agent '{self.agent_name}' Ignited. Listening on async stdin...")

        while True:
            try:
                line_bytes = await self._reader.readline()
                if not line_bytes: # EOF
                    break

                line = line_bytes.decode('utf-8').strip()
                if not line:
                    continue

                payload = json.loads(line)
                asyncio.create_task(self._route_request_async(payload))
            except json.JSONDecodeError:
                await self.send_error(None, -32700, "Parse error: Invalid JSON")
            except Exception as e:
                self.log.error(f"Stream Reader Fracture: {e}", exc_info=True)

    async def _route_request_async(self, req: Dict[str, Any]):
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params", {})
        action = req.get("action") 

        # [핵심] 컨텍스트에 현재 req_id 할당
        token = current_request_id.set(req_id)
        
        # [보강 3] 인입 성공 로그 (이 로그가 안 찍히면 파이프 통신 파손을 의미)
        self.log.info(f"Incoming RPC payload recognized. Action/Method: {action or method}")

        try:
            if action == "RESUME":
                self.log.info(f"Initiating RESUME sequence for parked intent.")
                await self.handle_resume(req_id, req)
                return

            if method == "initialize":
                await self.send_response(req_id, {"protocolVersion": "2026-09-04", "capabilities": {}})
            elif method == "tools/list":
                await self.handle_tools_list(req_id)
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                meta = params.get("_meta", {})
                await self.handle_tools_call(req_id, tool_name, arguments, meta)
            else:
                await self.send_error(req_id, -32601, f"Unknown method: {method}")
        except Exception as e:
            self.log.error(f"Async Routing/Execution Fault: {e}", exc_info=True)
            await self.send_error(req_id, -32000, f"Execution failed: {str(e)}")
        finally:
            # 컨텍스트 복원 (다른 코루틴과 섞이지 않음)
            current_request_id.reset(token)

    """Abstract Async Handlers"""
    async def handle_tools_list(self, req_id: Any):
        await self.send_response(req_id, {"tools": []})

    async def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        await self.send_error(req_id, -32601, f"Tool '{tool_name}' not implemented")

    async def handle_resume(self, req_id: Any, payload: Dict[str, Any]):
        self.log.warning(f"RESUME payload received for {req_id}, but handle_resume is not implemented.")