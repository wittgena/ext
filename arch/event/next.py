# xphi.arch.event.next
import os
import time
import threading
import random
import uuid as _std_uuid
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Annotated, Tuple, Protocol
from datetime import UTC, datetime

from pydantic import Field
from xphi.kernel.space.bind.resolver import resolve_identity

uuid = _std_uuid

"""Topos ID Generator (64-bit Snowflake)"""
class ToposGenerator:
    def __init__(self, vertex_id: int = 1, manifold_id: int = 1):
        self.worker_id = vertex_id & 0x1F
        self.datacenter_id = manifold_id & 0x1F
        self.sequence = 0
        self.epoch = 1767225600000 
        self.last_timestamp = -1
        self._lock = threading.Lock()

    def _timestamp(self) -> int:
        return int(time.time() * 1000)

    def generate(self) -> int:
        with self._lock:
            timestamp = self._timestamp()
            if timestamp < self.last_timestamp:
                offset = self.last_timestamp - timestamp
                if offset <= 5: 
                    time.sleep((offset + 1) / 1000.0) 
                    timestamp = self._timestamp() 
                else:
                    raise Exception(f"Clock moved backwards significantly! Refusing to generate id for {offset} milliseconds")

            if timestamp == self.last_timestamp:
                self.sequence = (self.sequence + 1) & 0xFFF
                if self.sequence == 0:
                    while timestamp <= self.last_timestamp:
                        timestamp = self._timestamp()
            else:
                self.sequence = 0
                
            self.last_timestamp = timestamp

            time_delta = (timestamp - self.epoch) & 0x1FFFFFFFFFF 
            topos_id = (
                (time_delta << 22) | 
                (self.datacenter_id << 17) | 
                (self.worker_id << 12) | 
                self.sequence
            )
            return topos_id

_manifold_id, _vertex_id = resolve_identity()
generator = ToposGenerator(vertex_id=_vertex_id, manifold_id=_manifold_id)

def next_id() -> str:
    """Returns Topos ID as string for precision safety."""
    return str(generator.generate())

def parse_id(snowflake_id: str):
    sid = int(snowflake_id)
    timestamp = ((sid >> 22) & 0x1FFFFFFFFFF) + 1767225600000
    datacenter_id = (sid >> 17) & 0x1F
    worker_id = (sid >> 12) & 0x1F
    sequence = sid & 0xFFF
    return {
        "timestamp_ms": timestamp,
        "worker_info": f"{datacenter_id}:{worker_id}",
        "seq": sequence
    }

def uuid4() -> _std_uuid.UUID:
    """
    - ToposId(64bit)를 상위 비트에, 랜덤 값(64bit)을 하위 비트에 결합하여 128비트 UUID 객체(8-4-4-4-12 포맷)로 변환하여 반환
    - ToposGenerator 실패 시 표준 UUIDv4로 안전하게 폴백(Fallback)
    """
    try:
        # 기존 체계의 ToposId (64비트 정수) 추출
        topos_int = generator.generate()
        
        # 상위 64비트에 ToposId를, 하위 64비트에 난수를 패킹 - 고유성을 보장, 시간순 정렬(Sortable) 특성을 유지
        uuid_int = (topos_int << 64) | random.getrandbits(64)
        
        # 표준 UUID 객체로 변환하여 반환 (호출부에서 str() 처리 호환)
        return _std_uuid.UUID(int=uuid_int)
        
    except Exception:
        # Clock backwards 등 ToposGenerator 예외 발생 시 표준 UUIDv4로 폴백
        return _std_uuid.uuid4()


"""Phase ID Generator (32-bit 차분 신호)"""
class PhaseIdGenerator:
    def __init__(self):
        self.prev_topo = 0
        self.prev_press = 0
        self.epoch = 0
        self.tick = 0
        self.current_anchor = 0
        self._lock = threading.Lock()

    def generate(self, current_topo: int, current_press: int, 
                 rupture: bool = False, injected_tick: Optional[int] = None) -> int:
        with self._lock:
            if rupture:
                self.epoch ^= 1
            
            if injected_tick is not None:
                self.tick = injected_tick & 0x7F
            else:
                self.tick = (self.tick + 1) & 0x7F
                
            d_topo = current_topo - self.prev_topo
            topo_sign = 1 if d_topo >= 0 else 0
            topo_mag = min(abs(d_topo), 0x7FF) # Rust 11bit mag 대응
            
            d_press = current_press - self.prev_press
            press_sign = 1 if d_press >= 0 else 0
            press_mag = min(abs(d_press), 0x7FF) # Rust 11bit mag 대응
            
            self.prev_topo = current_topo
            self.prev_press = current_press
            
            # (Epoch 1b | Tick 7b | TopoSign 1b + Mag 11b | PressSign 1b + Mag 11b) = 32-bit
            phase_id = (
                (self.epoch << 31) | 
                (self.tick << 24) |
                (topo_sign << 23) | 
                (topo_mag << 12) | 
                (press_sign << 11) | 
                press_mag
            )
            return phase_id & 0xFFFFFFFF

phase_generator = PhaseIdGenerator()

def next_phase_id(topo: int, press: int, rupture: bool = False, tick: Optional[int] = None) -> int:
    return phase_generator.generate(topo, press, rupture, tick)

def parse_phase_id(phase_id: int) -> Dict[str, Any]:
    """@desc: 32-bit Phase ID 디코딩 (Epoch, Tick, Topo 변위, Press 변위)"""
    epoch = (phase_id >> 31) & 0x1
    tick = (phase_id >> 24) & 0x7F
    
    topo_sign = (phase_id >> 23) & 0x1
    topo_mag = (phase_id >> 12) & 0x7FF
    d_topo = topo_mag if topo_sign else -topo_mag
    
    press_sign = (phase_id >> 11) & 0x1
    press_mag = phase_id & 0x7FF
    d_press = press_mag if press_sign else -press_mag
    
    return {
        "epoch": epoch,
        "tick": tick,
        "d_topo": d_topo,
        "d_press": d_press
    }


"""Nexus & Parity Management (Ternary XOR Logic)"""
def generate_nexus_id(topos_id: str | int, phase_id: int) -> int:
    """@desc: Topos ID의 하위 32비트와 Phase ID를 XOR하여 Nexus ID를 생성"""
    topos_low32 = int(topos_id) & 0xFFFFFFFF
    return topos_low32 ^ (phase_id & 0xFFFFFFFF)

def verify_parity(
    topos_id: Optional[str | int] = None, 
    phase_id: Optional[int] = None, 
    nexus_id: Optional[int] = None
) -> Dict[str, Any]:
    """@desc: 3개의 ID 중 최소 2개를 제공받아 무결성을 검증하거나 누락된 1개를 복원"""
    t_low32 = (int(topos_id) & 0xFFFFFFFF) if topos_id is not None else None
    p = (phase_id & 0xFFFFFFFF) if phase_id is not None else None
    n = (nexus_id & 0xFFFFFFFF) if nexus_id is not None else None

    present_count = sum(x is not None for x in (t_low32, p, n))

    if present_count == 3:
        is_valid = (t_low32 ^ p) == n
        return {"is_valid": is_valid}
        
    elif present_count == 2:
        if t_low32 is None:
            recovered = p ^ n
            missing_type = "topos_id_low32"
        elif p is None:
            recovered = t_low32 ^ n
            missing_type = "phase_id"
        else:
            recovered = t_low32 ^ p
            missing_type = "nexus_id"
            
        return {
            "is_valid": True,
            "recovered_missing": recovered,
            "recovered_type": missing_type
        }
    else:
        raise ValueError("At least two identification IDs are required for parity recovery")

def generate_parity_triplet(topo: int, press: int, rupture: bool = False) -> Dict[str, Any]:
    """@desc: Rust의 `init_epoch_ffi`와 동일하게 한 턴의 통신에 필요한 Topos ID, Phase ID, Nexus ID 세트를 일괄 생성하여 반환"""
    topos_id = generator.generate()
    phase_id = phase_generator.generate(topo, press, rupture)
    nexus_id = generate_nexus_id(topos_id, phase_id)
    return {
        "topos_id": str(topos_id),
        "phase_id": phase_id,
        "nexus_id": nexus_id
    }


"""Event Schema & Utilities"""
ToposId = Annotated[str, Field(description="Topological Snowflake ID (Replaces legacy UUID)")]

def utc_now():
    return datetime.now(UTC)

@dataclass
class LogEvent:
    """
    @event.contract: Telemetry counterpart to PsiEvent
    Identity(2) + Origin(3) + Content(3) + Metrics(3)
    """
    event_id: str = field(default_factory=next_id)
    phase_id: int = 0
    nexus_id: Optional[int] = None
    parent_id: Optional[str] = None
    
    source_id: str = "unknown"
    scope: str = "LOG"
    tick: Optional[int] = None
    
    level: str = "INFO"
    kind: str = "log"
    message: str = ""
    
    context: Dict[str, Any] = field(default_factory=dict)
    
    density: float = 0.0
    gain: float = 1.0
    fold_count: int = 1

class EventObserver(Protocol):
    def update(self, event: LogEvent) -> None:
        ...