# xphi.state.phase.channel
## @lineage: xphi.state.network.channel
import json
import uuid
import asyncio
import logging
from typing import Any, List, Optional, Dict

from xphi.watcher.plane.emitter import get_emitter, flow_scope

# 로거 네임스페이스 분리 유지
pipeline_log = get_emitter("channel.pipeline")
codec_log = get_emitter("channel.codec")
bridge_log = get_emitter("rpc.bridge")

# =====================================================================
# 1. Pipeline Classes (Base Structures & Orchestration)
# =====================================================================

class ChannelContext:
    """핸들러 간의 이벤트 라우팅 및 전역 상태(AttributeMap) 공유"""
    def __init__(self, pipeline: 'ChannelPipeline', index: int):
        self.pipeline = pipeline
        self.index = index

    def set_attr(self, key: str, value: Any):
        self.pipeline.attributes[key] = value

    def get_attr(self, key: str, default: Any = None) -> Any:
        return self.pipeline.attributes.get(key, default)

    async def fire_channel_active(self):
        await self.pipeline._process_channel_active(self.index + 1)

    async def fire_channel_inactive(self):
        await self.pipeline._process_channel_inactive(self.index + 1)

    async def fire_channel_read(self, msg: Any):
        await self.pipeline._process_read(msg, self.index + 1)

    async def fire_write(self, msg: Any) -> asyncio.Future:
        return await self.pipeline._process_write(msg, self.index - 1)

    async def fire_exception_caught(self, exc: Exception):
        await self.pipeline._process_exception(exc, self.index + 1)

    async def fire_user_event_triggered(self, evt: Any):
        await self.pipeline._process_user_event(evt, self.index + 1)


class DuplexChannel:
    async def channel_active(self, ctx: ChannelContext): 
        await ctx.fire_channel_active()
        
    async def channel_inactive(self, ctx: ChannelContext): 
        await ctx.fire_channel_inactive()
        
    async def channel_read(self, ctx: ChannelContext, msg: Any): 
        await ctx.fire_channel_read(msg)
        
    async def write(self, ctx: ChannelContext, msg: Any): 
        await ctx.fire_write(msg)
        
    async def exception_caught(self, ctx: ChannelContext, exc: Exception): 
        await ctx.fire_exception_caught(exc)
        
    async def user_event_triggered(self, ctx: ChannelContext, evt: Any): 
        await ctx.fire_user_event_triggered(evt)


class ChannelPipeline:
    def __init__(self):
        self.handlers: List[DuplexChannel] = []
        self.transport: Optional[asyncio.Transport] = None
        self.attributes: Dict[str, Any] = {}  # 전역 상태 저장소
        
        self._pending_tasks = 0
        self.HIGH_WATERMARK = 1000
        self.LOW_WATERMARK = 500

    def add_last(self, handler: DuplexChannel) -> 'ChannelPipeline':
        self.handlers.append(handler)
        return self

    async def fire_channel_active(self):
        await self._process_channel_active(0)

    async def _process_channel_active(self, index: int = 0):
        if index < len(self.handlers):
            await self.handlers[index].channel_active(ChannelContext(self, index))

    async def fire_channel_inactive(self):
        await self._process_channel_inactive(0)

    async def _process_channel_inactive(self, index: int = 0):
        if index < len(self.handlers):
            await self.handlers[index].channel_inactive(ChannelContext(self, index))

    async def _process_read(self, msg: Any, index: int = 0):
        if index < len(self.handlers):
            await self.handlers[index].channel_read(ChannelContext(self, index), msg)
            
        if index == len(self.handlers) - 1:
            self._pending_tasks -= 1
            if self._pending_tasks == self.LOW_WATERMARK and self.transport:
                self.transport.resume_reading()

    async def _process_write(self, msg: Any, index: int) -> asyncio.Future:
        if index >= 0:
            return await self.handlers[index].write(ChannelContext(self, index), msg)
        else:
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            try:
                if self.transport and isinstance(msg, bytes):
                    self.transport.write(msg)
                    future.set_result(True)
                else:
                    future.set_exception(ValueError("Transport is not ready or msg is not bytes"))
            except Exception as e:
                future.set_exception(e)
            return future

    async def _process_exception(self, exc: Exception, index: int = 0):
        if index < len(self.handlers):
            await self.handlers[index].exception_caught(ChannelContext(self, index), exc)
        else:
            pipeline_log.error(f"[Pipeline] Unhandled Exception: {exc}")

    async def _process_user_event(self, evt: Any, index: int = 0):
        if index < len(self.handlers):
            await self.handlers[index].user_event_triggered(ChannelContext(self, index), evt)


# =====================================================================
# 2. Codec Classes (Serialization & Utilities)
# =====================================================================

class JsonMessageCodec(DuplexChannel):
    """Raw TCP Bytes(\n 구분) <-> JSON Dictionary 양방향 직렬화/역직렬화"""
    def __init__(self):
        self._buffer = bytearray()

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        if isinstance(msg, bytes):
            self._buffer.extend(msg)
            while b'\n' in self._buffer:
                frame, _, remainder = self._buffer.partition(b'\n')
                self._buffer = bytearray(remainder)
                try:
                    parsed = json.loads(frame.decode('utf-8'))
                    await ctx.fire_channel_read(parsed)
                except json.JSONDecodeError as e:
                    await ctx.fire_exception_caught(ValueError(f"JSON Parsing failed: {e}"))
        else:
            await ctx.fire_channel_read(msg)

    async def write(self, ctx: ChannelContext, msg: Any):
        if isinstance(msg, dict):
            encoded = (json.dumps(msg) + '\n').encode('utf-8')
            await ctx.fire_write(encoded)
        else:
            await ctx.fire_write(msg)


class XelogUniversalTracer(DuplexChannel):
    """모든 트래픽을 관측하는 로깅 미들웨어 (형식 무관)"""
    async def channel_read(self, ctx: ChannelContext, msg: Any):
        codec_log.trace(f"[RX_DUMP] {msg}")
        await ctx.fire_channel_read(msg)

    async def write(self, ctx: ChannelContext, msg: Any):
        codec_log.trace(f"[TX_DUMP] {msg}")
        await ctx.fire_write(msg)


# =====================================================================
# 3. Bridge Classes (RPC & Flow Propagation)
# =====================================================================

class FlowPropagator(DuplexChannel):
    def __init__(self, client_id: str = "SERVER_SIDE"):
        self.client_id = client_id

    async def channel_active(self, ctx: ChannelContext):
        ## 연결 성립 시 고유 flow_id 부여 및 AttributeMap 저장
        flow_id = f"flow_{self.client_id}_{uuid.uuid4().hex[:8]}"
        ctx.set_attr("flow_id", flow_id)
        
        with flow_scope(flow_id=flow_id, phase="NET_ACTIVE", client_id=self.client_id):
            bridge_log.info("채널 연결 완료 (Flow 생성됨)")
            await ctx.fire_channel_active()

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        flow_id = ctx.get_attr("flow_id", "UNKNOWN_FLOW")
        ## 수신 이벤트를 flow_scope로 래핑하여 하위 핸들러로 전달
        with flow_scope(flow_id=flow_id, phase="NET_RX", client_id=self.client_id):
            await ctx.fire_channel_read(msg)

    async def write(self, ctx: ChannelContext, msg: Any):
        flow_id = ctx.get_attr("flow_id", "UNKNOWN_FLOW")
        ## 송신 이벤트를 flow_scope로 래핑하여 Transport로 전달
        with flow_scope(flow_id=flow_id, phase="NET_TX", client_id=self.client_id):
            await ctx.fire_write(msg)


class RpcBridge(DuplexChannel):
    def __init__(self):
        self.pending_requests = {}
        self._seq = 0
        self.ctx = None  # 파이프라인이 연결되면 활성화됨

    async def channel_active(self, ctx: ChannelContext):
        self.ctx = ctx
        await ctx.fire_channel_active()

    async def request(self, payload: dict, timeout: float = 60.0) -> dict:
        if not self.ctx:
            raise ConnectionError("Pipeline is not active yet.")
            
        self._seq += 1
        req_id = f"req_{self._seq}"
        
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.pending_requests[req_id] = fut
        self.ctx.set_attr("current_req_id", req_id)
        
        write_future = await self.ctx.fire_write(payload)
        if isinstance(write_future, asyncio.Future) and write_future.exception():
            self.pending_requests.pop(req_id, None)
            raise write_future.exception()
            
        return await asyncio.wait_for(fut, timeout=timeout)

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        req_id = ctx.get_attr("current_req_id")
        if req_id and req_id in self.pending_requests:
            self.pending_requests.pop(req_id).set_result(msg)
            ctx.set_attr("current_req_id", None)  # 처리 후 초기화
            return
        elif isinstance(msg, dict) and "_req_id" in msg:
            fallback_req_id = msg["_req_id"]
            if fallback_req_id in self.pending_requests:
                self.pending_requests.pop(fallback_req_id).set_result(msg)
                return
        await ctx.fire_channel_read(msg)