# xphi.state.phase.network.channel.codec
## @lineage: xphi.kernel.phase.network.channel.codec
import json
from typing import Any
from xphi.state.phase.network.channel.pipeline import DuplexChannel, ChannelContext
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("channel.codec")

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
        log.trace(f"[RX_DUMP] {msg}")
        await ctx.fire_channel_read(msg)

    async def write(self, ctx: ChannelContext, msg: Any):
        log.trace(f"[TX_DUMP] {msg}")
        await ctx.fire_write(msg)