# xphi.state.phase.runtime.sensor
import asyncio
import random
import time
import json
import re
from pathlib import Path
from typing import Set, List, Optional

from xphi.kernel.space.topos.tunnel.factory import UniversalFacade
from xphi.arch.event.psi import PsiType
from xphi.kernel.space.bind.resolver import find_current_self, resolve_path, resolve_pattern
from xphi.watcher.plane.emitter import get_emitter

class SurfaceSensor:
    """
    @role: External state observer
    @desc: 스웜 외부 표면(Redis/Tunnel)의 상태 변화를 감지하고 이벤트를 발생시킵니다.
    """
    def __init__(self, tunnel: UniversalFacade):
        self.tunnel = tunnel
        self.prefix = "sensor:watcher"
        self.keylist = f"{self.prefix}:keylist"
        
        self.root_dir = resolve_path("surface") / "redis"
        self.pattern = resolve_pattern()
        self.regex = re.compile(self.pattern)
        self.log = get_emitter("surface.sensor", phase="SENSING")

    def _key_to_filename(self, key: str) -> str:
        return key.replace(":", "__") + ".json"

    async def fetch_keys(self) -> Set[str]:
        cursor = 0
        keys = set()
        while True:
            cursor, batch = await self.tunnel.scan(cursor=cursor, count=200)
            for k in batch:
                if self.regex.match(k):
                    keys.add(k)
            if int(cursor) == 0:
                break
        return keys

    async def dump_key(self, key: str):
        t = await self.tunnel.type(key)
        
        if t == "string":
            val = await self.tunnel.get(key)
            try: data = json.loads(val)
            except Exception: data = val
        elif t == "hash":
            data = await self.tunnel.hgetall(key)
        elif t == "list":
            data = await self.tunnel.lrange(key, 0, -1)
        elif t == "set":
            data = sorted(await self.tunnel.smembers(key))
        elif t == "zset":
            raw_zset = await self.tunnel.zrange(key, 0, -1, withscores=True)
            data = [{"member": m, "score": s} for m, s in raw_zset]
        else:
            data = f"[unsupported] type={t}"

        out_path = self.root_dir / self._key_to_filename(key)
        out_path.write_text(json.dumps({key: data}, indent=2), encoding="utf-8")

    async def sense(self) -> List[PsiType]:
        """주기적으로 표면의 변경사항을 확인하고 Signal(Psi)을 방출합니다."""
        self.root_dir.mkdir(parents=True, exist_ok=True)
        
        previous_raw = await self.tunnel.smembers(self.keylist)
        previous = set(previous_raw) if previous_raw else set()

        current = await self.fetch_keys()
        added = current - previous
        removed = previous - current

        signals: List[PsiType] = []
        for k in sorted(added):
            await self.dump_key(k)
            signals.append(PsiType(kind="watcher:key_added", tag=k, payload="source: tunnel"))

        for k in sorted(removed):
            (self.root_dir / self._key_to_filename(k)).unlink(missing_ok=True)
            signals.append(PsiType(kind="watcher:key_removed", tag="removed", payload=""))

        # 상태 갱신
        if hasattr(self.tunnel, 'pipeline'):
            pipe = self.tunnel.pipeline()
            pipe.delete(self.keylist)
            if current:
                pipe.sadd(self.keylist, *current)
            await pipe.execute()
        else:
            await self.tunnel.delete(self.keylist)
            if current:
                await self.tunnel.sadd(self.keylist, *current)

        return signals


class SurfaceActuator:
    """
    @role: External state projector
    @desc: 런타임 내부의 결정 및 상태(Psi)를 외부 표면에 투영(기록)합니다.
    """
    KEY_PARTS = ["intensity", "threshold", "state", "score", "generated", "flag", "signal"]
    CONTROL_KEY = "runtime:control:emit"

    def __init__(self, sinks):
        self.sinks = sinks if isinstance(sinks, list) else [sinks]
        self.emit_enabled = True
        self.processed = 0

        self.watcher_pattern = resolve_pattern()
        self.namespaces = self._extract_namespaces(self.watcher_pattern)
        self.log = get_emitter("surface.actuator", phase="ACTUATION") 

    def _extract_namespaces(self, pattern: str):
        inner = pattern.split("(")[1].split(")")[0]
        return inner.split("|")

    async def is_emit_enabled(self):
        for sink in self.sinks:
            try:
                val = await sink.get_control_flag(self.CONTROL_KEY)
                if val is not None:
                    return val != "off"
            except Exception:
                pass
        return self.emit_enabled

    async def _fanout_set(self, key, payload):
        await asyncio.gather(*(sink.set(key, payload) for sink in self.sinks), return_exceptions=True)

    async def _fanout_delete(self, key):
        await asyncio.gather(*(sink.delete(key) for sink in self.sinks), return_exceptions=True)

    async def actuate_psi(self, psi: PsiType):
        if not await self.is_emit_enabled():
            return

        key = psi.tag
        payload = {"ts": time.time(), "source": "runtime.loop", "psi": psi.symbol}

        try:
            if psi.kind.endswith("removed"):
                await self._fanout_delete(key)
                self.log.signal(f"[emit] delete {key}")
            else:
                await self._fanout_set(key, payload)
                if psi.kind.endswith("added") or psi.kind.endswith("generated"):
                    self.log.signal(f"[emit] set {key}")
                else:
                    self.log.signal(f"[emit] update {key}")
            self.processed += 1
        except Exception as e:
            self.log.error(f"[emit:error] {e}")

    async def close(self):
        for sink in self.sinks:
            try: await sink.close()
            except Exception: pass

    # --- [Mocking & Test Utilities] ---
    def random_key(self):
        ns = random.choice(self.namespaces)
        part = random.choice(self.KEY_PARTS)
        idx = random.randint(1, 5)
        return f"{ns}:{part}:{idx}"

    async def random_surface(self, duration: int = 60):
        print("## Runtime Psi Random Surface")
        created = set()
        start = time.time()
        while time.time() - start < duration:
            if random.random() < 0.65:
                key = self.random_key()
                payload = {"ts": time.time(), "source": "runtime.random", "v": random.random()}
                await self._fanout_set(key, payload)
                created.add(key)
                print(f"[ψ+] create {key}")
            else:
                if created:
                    key = random.choice(list(created))
                    await self._fanout_delete(key)
                    created.remove(key)
                    print(f"[ψ-] delete {key}")
            await asyncio.sleep(random.uniform(0.25, 0.8))
        print("\n[random surface finished]")