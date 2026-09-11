# xphi.state.network.factory
## @lineage: xphi.state.phase.network.factory
## @lineage: xphi.kernel.phase.network.factory
import asyncio
import logging
from typing import Callable, List, Optional
from xphi.state.network.channel.pipeline import ChannelPipeline, ChannelContext, DuplexChannel

log = logging.getLogger("protocol.factory")

class ProtocolBridge(asyncio.Protocol):
    def __init__(self, pipeline: ChannelPipeline):
        self.pipeline = pipeline

    def connection_made(self, transport: asyncio.Transport):
        self.pipeline.transport = transport
        if self.pipeline.handlers:
            asyncio.create_task(self.pipeline.handlers[0].channel_active(ChannelContext(self.pipeline, 0)))

    def data_received(self, data: bytes):
        self.pipeline._pending_tasks += 1
        # 배압(Backpressure) 방어
        if self.pipeline._pending_tasks == self.pipeline.HIGH_WATERMARK:
            self.pipeline.transport.pause_reading()
            
        asyncio.create_task(self.pipeline._process_read(data))

    def connection_lost(self, exc: Optional[Exception]):
        if exc:
            asyncio.create_task(self.pipeline._process_exception(exc))
        if self.pipeline.handlers:
            asyncio.create_task(self.pipeline.handlers[0].channel_inactive(ChannelContext(self.pipeline, 0)))

class ProtocolFactory:
    def __init__(self):
        self.factories: List[Callable[[], DuplexChannel]] = []

    def child_handler(self, factory_func: Callable[[], DuplexChannel]) -> 'ProtocolFactory':
        """새로운 연결이 맺어질 때마다 파이프라인에 주입할 핸들러 팩토리 등록"""
        self.factories.append(factory_func)
        return self

    async def connect(self, host: str, port: int) -> ProtocolBridge:
        loop = asyncio.get_running_loop()
        
        def protocol_factory():
            pipeline = ChannelPipeline()
            for factory in self.factories:
                pipeline.add_last(factory())
            return ProtocolBridge(pipeline)

        log.info(f"[Bootstrap] Connecting to {host}:{port}...")
        transport, protocol = await loop.create_connection(protocol_factory, host, port)
        return protocol