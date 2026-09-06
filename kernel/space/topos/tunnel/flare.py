# xphi.kernel.space.topos.tunnel.flare
import json
import asyncio
import httpx
from typing import Optional, Any, List, Tuple

from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.topos.tunnel.config import BackendProtocol, resolve_default_config, parse_connection_urls
from xphi.kernel.wasm.method import DphiMethod  # DphiMethod 임포트

log = get_emitter("tunnel.flare")

class PayloadKey:
    """Matches dphi.broker.PayloadKey to minimize dependencies"""
    JOB_ID = "job_id"
    METHOD_FUNC = "target_func" 
    PAYLOAD = "payload"
    TIER = "tier"
    WASM_PATH = "wasm_path"
    CONTEXT = "context"

class ResultKey:
    SUCCESS = "success"
    OUTPUT = "output"
    ERROR = "error"

class _MockPubSub:
    """Adapter for broker's pubsub.listen() using an asyncio.Queue"""
    def __init__(self, response_queue: asyncio.Queue):
        self.q = response_queue

    async def subscribe(self, *args, **kwargs): pass
    async def unsubscribe(self, *args, **kwargs): pass
    async def close(self, *args, **kwargs): pass

    async def listen(self):
        while True:
            msg = await self.q.get()
            yield msg

class _MockStateStore:
    """Adapter to convert broker's Redis xadd() into Cloudflare HTTP POST requests"""
    def __init__(self, http_client: httpx.AsyncClient, response_queue: asyncio.Queue):
        self.http_client = http_client
        self.q = response_queue
        # 백그라운드 태스크 GC 방지용 Set
        self._background_tasks = set()

    async def xadd(self, name: str, fields: dict, *args, **kwargs):
        """
        [보안/통신 패치] Redis xadd처럼 즉시 반환(Non-blocking)되도록 설계 변경.
        HTTP 요청은 백그라운드 태스크로 분리하여 Broker의 타임아웃 타이머와 충돌하지 않게 합니다.
        """
        job_id = "unknown"
        try:
            # 1. 안전하게 페이로드 추출 (실패 시 즉시 종료)
            data_val = fields.get('data') or fields.get(b'data')
            if not data_val: 
                return b'0-0'
            
            payload = json.loads(data_val)
            job_id = payload.get(PayloadKey.JOB_ID, "unknown")
            
            # 2. HTTP 요청을 백그라운드 태스크로 스케줄링 (Fire-and-Forget)
            task = asyncio.create_task(self._process_edge_request(job_id, payload))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
            
            # Redis 메시지 ID처럼 임의의 ID 즉시 반환
            return b'edge-mock-id'
            
        except Exception as e:
            log.error(f"[FlareTunnel] Invalid payload format: {e}")
            return b'error-id'

    async def _process_edge_request(self, job_id: str, payload: dict):
        """실제 Cloudflare Edge로 HTTP 요청을 보내고 응답을 큐에 주입하는 비동기 워커"""
        try:
            method_func = payload.get(PayloadKey.METHOD_FUNC, "")
            wasm_path = payload.get(PayloadKey.WASM_PATH)
            base_payload = payload.get(PayloadKey.PAYLOAD, {})
            context = payload.get(PayloadKey.CONTEXT, {})
            
            # [라우팅 패치 1] DphiMethod를 기반으로 DPHI 커널 프로토콜 필터링
            dphi_kernel_methods = {
                m.value for m in DphiMethod 
                if m.value not in (DphiMethod.EXECUTE_CODE.value, DphiMethod.EXECUTE_DVM.value)
            }
            
            # [라우팅 패치 2] vm_target 정밀 분기
            if wasm_path == "dphi.wasm" or method_func in dphi_kernel_methods:
                vm_tgt = "DPHI"
            elif wasm_path == "dvm.wasm" or method_func == DphiMethod.EXECUTE_DVM.value:
                vm_tgt = "DVM"
            elif wasm_path: # 기타 wasm (cw20_base.wasm 등)
                vm_tgt = "COSMWASM_EXTERNAL"
            else:
                vm_tgt = "PYTHON"
            
            # [스키마 검증 패치] Rust JSON 스키마(PhaseDrift Envelope)에 맞춘 정확한 페이로드 조립
            if vm_tgt == "DPHI":
                actual_edge_payload = {
                    "method": method_func,     # Rust가 기대하는 Method Enum (예: "init_epoch")
                    "context": context,        # ToposContext 객체
                    "payload": base_payload    # InitEpochRequest, SealEpochPayload 등의 알맹이
                }
                rpc_method = "invoke_wasm"     # TS 라우터가 받을 HTTP RPC 메서드는 고정
            else:
                actual_edge_payload = base_payload
                rpc_method = method_func.replace("wasm:", "").split(":")[-1] if "wasm:" in method_func else "execute"
            
            # 최종 Edge API 파라미터 조립
            edge_params = {
                "vm_target": vm_tgt,
                "payload": actual_edge_payload, # 완성된 Envelope이 그대로 TS 라우터를 거쳐 WASM 메모리로 들어감
                "context": context,
            }
            
            if vm_tgt == "PYTHON" and isinstance(actual_edge_payload, dict):
                edge_params["code"] = actual_edge_payload.get("code", "")
                edge_params["callables"] = actual_edge_payload.get("callables", [])
                
            if payload.get(PayloadKey.TIER) == "SYSTEM":
                edge_params["fuel"] = None # Unlimited fuel for SYSTEM tier

            rpc_request = {
                "jsonrpc": "2.0",
                "method": rpc_method, 
                "params": edge_params,
                "id": job_id
            }
            
            # 3. Transmit via HTTP
            response = await self.http_client.post("/", json=rpc_request)
            response.raise_for_status()
            
            # 4. Parse HTTP response
            res_json = response.json()
            
            broker_result = {
                PayloadKey.JOB_ID: job_id,
                ResultKey.SUCCESS: "error" not in res_json,
            }
            
            if broker_result[ResultKey.SUCCESS]:
                broker_result[ResultKey.OUTPUT] = res_json.get("result", {}).get("output", "")
            else:
                err_data = res_json.get("error", {})
                broker_result[ResultKey.ERROR] = err_data.get("message", "Unknown Edge Error")
            
            # 5. Inject into queue
            await self._put_success(broker_result)
            
        except httpx.HTTPStatusError as e:
            # 502 Bad Gateway / 1102 Worker Exceeded Limit 등 Cloudflare 한도 초과 포착
            err_msg = f"Edge HTTP {e.response.status_code}: {e.response.text}"
            if e.response.status_code in (502, 503):
                err_msg = "Kinetic Trap triggered: Edge returned 502 Bad Gateway (CPU Limit Exceeded)"
            log.warning(f"[FlareTunnel] {err_msg} [{job_id}]")
            await self._put_error(job_id, err_msg)
            
        except httpx.TimeoutException:
            # 로컬 Deno/Wrangler 환경에서 워커가 무한 루프에 빠져 응답을 못하는 경우 포착
            err_msg = "Kinetic Trap triggered: Edge execution timeout (Local Simulator Hang)"
            log.warning(f"[FlareTunnel] {err_msg} [{job_id}]")
            await self._put_error(job_id, err_msg)
            
        except httpx.RemoteProtocolError:
            # V8 엔진이 뻗어서 호스트가 소켓을 강제로 끊어버린 경우 포착
            err_msg = "Kinetic Trap triggered: Edge connection severed forcefully"
            log.warning(f"[FlareTunnel] {err_msg} [{job_id}]")
            await self._put_error(job_id, err_msg)
            
        except httpx.RequestError as e:
            # 기타 네트워크 오류
            log.error(f"[FlareTunnel] Network Error [{job_id}]: {type(e).__name__} - {str(e)}")
            await self._put_error(job_id, f"Network Error: {str(e)}")
            
        except Exception as e:
            log.error(f"[FlareTunnel] Internal Error [{job_id}]: {e}", exc_info=True)
            await self._put_error(job_id, str(e))

    async def _put_success(self, broker_result: dict):
        msg = {
            'type': 'message',
            'data': json.dumps(broker_result).encode('utf-8')
        }
        await self.q.put(msg)

    async def _put_error(self, job_id: str, error_msg: str):
        msg = {
            'type': 'message',
            'data': json.dumps({
                PayloadKey.JOB_ID: job_id,
                ResultKey.SUCCESS: False,
                ResultKey.ERROR: error_msg
            }).encode('utf-8')
        }
        await self.q.put(msg)
        
    async def aclose(self):
        # 실행 중인 모든 백그라운드 태스크 취소 및 대기
        for task in list(self._background_tasks):
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

    async def close(self):
        await self.aclose()


class FlareFacade:
    """
    Stateless HTTP Bridge injected into DphiBroker.
    Appears as a Redis tunnel externally (Duck Typing), but executes CF HTTP calls internally.
    """
    def __init__(self, state_url: str, mq_url: str, mq_protocol: BackendProtocol, **kwargs):
        self.mq_protocol = mq_protocol
        self.mq_url = mq_url
        self.wasm_broker = None
        
        # Extended timeout to accommodate Edge Wasm Cold Boot
        pool_kwargs = {
            "timeout": httpx.Timeout(60.0, connect=5.0), 
            "verify": False
        }
        self.http_client = httpx.AsyncClient(base_url=self.mq_url, **pool_kwargs)
        self._response_queue = asyncio.Queue()
        self.state_store = _MockStateStore(self.http_client, self._response_queue)
        log.info(f"[FlareTunnel] Initialized Duck-Typed HTTP Bridge targeting Edge: {self.mq_url}")

    def pubsub(self):
        """Mock pubsub to satisfy broker's listener"""
        return _MockPubSub(self._response_queue)

    async def publish(self, channel: str, message: Any):
        """Handle Control Plane publish events to prevent Broker timeouts"""
        try:
            if isinstance(message, bytes): message = message.decode('utf-8')
            payload = json.loads(message)
            job_id = payload.get(PayloadKey.JOB_ID)
            
            if job_id:
                ack_msg = {
                    'type': 'message',
                    'data': json.dumps({
                        PayloadKey.JOB_ID: job_id,
                        ResultKey.SUCCESS: True,
                        ResultKey.OUTPUT: "Policy synchronized at edge boundary"
                    }).encode('utf-8')
                }
                await self._response_queue.put(ack_msg)
        except Exception:
            pass
        return 1

    def bind_wasm_broker(self, broker):
        self.wasm_broker = broker

    async def stream_produce(self, *args, **kwargs): return ""
    async def stream_consume(self, *args, **kwargs): return []
    async def stream_ack(self, *args, **kwargs): return 1

    async def close(self):
        await self.state_store.aclose()
        await self.http_client.aclose()


class FlareTunnelFactory:
    _async_instance: Optional[FlareFacade] = None

    @classmethod
    async def get_default(cls, **kwargs) -> FlareFacade:
        if cls._async_instance is None:
            config = resolve_default_config()
            scheme, state_url, mq_url = parse_connection_urls(config.default_url)
            
            mq_url = kwargs.pop("mq_url", mq_url)
            state_url = kwargs.pop("state_url", state_url)
            scheme = kwargs.pop("mq_protocol", scheme)
            
            cls._async_instance = FlareFacade(state_url, mq_url, scheme, **kwargs)
            log.info(f"[FlareTunnelFactory] Provisioned Async Flare Tunnel: {mq_url}")
        return cls._async_instance

    @classmethod
    async def get_isolated(cls, **kwargs) -> FlareFacade:
        return await cls.get_default(**kwargs)
        
    @classmethod
    async def get_provenant(cls, wasm_broker, **kwargs) -> FlareFacade:
        tunnel = await cls.get_default(**kwargs)
        tunnel.bind_wasm_broker(wasm_broker)
        return tunnel

    @classmethod
    async def close_all(cls):
        if cls._async_instance:
            await cls._async_instance.close()
            cls._async_instance = None