# xphi.kernel.wasm.cgroup
## @lineage: xphi.kernel.dphi.cgroup
## @lineage: kernel.dphi.cgroup
## @lineage: watcher.dphi.cgroup
import logging
from dataclasses import dataclass
from enum import Enum
from xphi.watcher.plane.emitter import get_emitter

try:
    import wasmtime
except ImportError:
    wasmtime = None

log = get_emitter("dphi.cgroup")

class Tier(Enum):
    STANDARD = "STANDARD"   # 일반 유저 코드 및 비즈니스 로직
    SYSTEM = "SYSTEM"       # 관리자 작업 (무결성 검증, 대규모 해싱 등)
    UNLIMITED = "UNLIMITED" # 로컬 디버깅 및 프로파일링 전용

@dataclass
class CgroupPolicy:
    max_memory_bytes: int
    cpu_fuel_quota: int
    tier: Tier

    @classmethod
    def standard(cls) -> 'CgroupPolicy':
        return cls(
            max_memory_bytes=64 * 1024 * 1024,
            cpu_fuel_quota=10_000_000,
            tier=Tier.STANDARD
        )

    @classmethod
    def system(cls) -> 'CgroupPolicy':
        return cls(
            max_memory_bytes=256 * 1024 * 1024,
            cpu_fuel_quota=2_000_000_000,
            tier=Tier.SYSTEM
        )

    @classmethod
    def custom(cls, mem_mb: int, fuel: int) -> 'CgroupPolicy':
        return cls(
            max_memory_bytes=mem_mb * 1024 * 1024,
            cpu_fuel_quota=fuel,
            tier=Tier.UNLIMITED
        )

class WasmCgroup:
    """@desc: In-process resource controller (Data Plane) for a Wasm instance"""
    def __init__(self, cgroup_name: str, policy: CgroupPolicy = None):
        # [개선] WasmCgroup 인스턴스화 시점에만 wasmtime 존재 여부 검증
        if wasmtime is None:
            raise ImportError("The 'wasmtime' module is required to use WasmCgroup.")
            
        self.cgroup_name = cgroup_name
        self.policy = policy or CgroupPolicy.standard()

    def apply_to_config(self, config: 'wasmtime.Config') -> None:
        config.consume_fuel = True

    def apply_to_store(self, store: 'wasmtime.Store') -> None:
        store.set_limits(memory_size=self.policy.max_memory_bytes)
        store.set_fuel(self.policy.cpu_fuel_quota)
        
        mb = self.policy.max_memory_bytes // 1024 // 1024
        log.info(f"[{self.cgroup_name}] Cgroup enforced: Tier={self.policy.tier.value}, Mem={mb}MB, Fuel={self.policy.cpu_fuel_quota:,}")

    def inject_emergency_fuel(self, store: 'wasmtime.Store', additional_fuel: int) -> None:
        current_fuel = store.get_fuel()
        new_fuel = current_fuel + additional_fuel
        
        ## Wasmtime Store의 Fuel을 새로운 값으로 덮어씁니다.
        store.set_fuel(new_fuel)
        self.policy.cpu_fuel_quota += additional_fuel
        log.warning(f"[{self.cgroup_name}] Emergency Fuel Injected: +{additional_fuel:,} (New Total Quota: {self.policy.cpu_fuel_quota:,})")

    def inspect_metrics(self, store: 'wasmtime.Store', memory: 'wasmtime.Memory') -> dict:
        """Control Plane(브로커)으로 보낼 현재 리소스 상태를 계측합니다."""
        current_mem_bytes = memory.size(store) * 65536 
        try:
            fuel_remaining = store.get_fuel()
        except wasmtime.WasmtimeError:
            fuel_remaining = 0
            
        fuel_consumed = self.policy.cpu_fuel_quota - fuel_remaining
        return {
            "cgroup": self.cgroup_name,
            "tier": self.policy.tier.value,
            "mem_usage_bytes": current_mem_bytes,
            "mem_limit_bytes": self.policy.max_memory_bytes,
            "fuel_consumed": fuel_consumed,
            "fuel_remaining": fuel_remaining
        }