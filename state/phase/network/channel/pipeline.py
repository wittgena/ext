# xphi.state.phase.network.channel.pipeline
## @lineage: xphi.kernel.phase.network.channel.pipeline
import asyncio
import logging
from typing import Any, List, Optional, Dict
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("channel.pipeline")

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
            log.error(f"[Pipeline] Unhandled Exception: {exc}")

    async def _process_user_event(self, evt: Any, index: int = 0):
        if index < len(self.handlers):
            await self.handlers[index].user_event_triggered(ChannelContext(self, index), evt)