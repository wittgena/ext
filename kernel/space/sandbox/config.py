# xphi.kernel.space.sandbox.config
## @lineage: xphi.bound.space.sandbox.config
## @lineage: xphi.xor.space.sandbox.config
## @lineage: xphi.arch.eco.config
## @lineage: xphi.arch.eco.dphi.config
from pydantic import Field
from pydantic_settings import BaseSettings
from typing import Dict
from xphi.kernel.wasm.cgroup import Tier

class TierPolicySettings(BaseSettings):
    fuel_to_seconds_ratio: int = 100_000_000
    
    ## Premium (System)
    system_max_threads: int = 4
    system_max_node_capacity: int = 50
    system_max_simulation_ticks: int = 1000
    
    ## Degraded (Standard)
    standard_max_threads: int = 1
    standard_max_node_capacity: int = 3
    standard_max_simulation_ticks: int = 50

    ## Fallback
    fallback_mem_mb: int = 10
    fallback_fuel: int = 1_000_000

    class Config:
        env_prefix = "TIER_POLICY_"

class FuelSettings(BaseSettings):
    fuel_unit: int = 1_000_000
    usd_per_fuel_unit: float = 0.01
    tier_multipliers: Dict[str, float] = Field(
        default_factory=lambda: {
            "SYSTEM": 1.5,
            "STANDARD": 1.0,
            "FALLBACK": 0.5
        }
    )

    class Config:
        env_prefix = "FUEL_"

class TreasurySettings(BaseSettings):
    operator_share: float = 0.70      # 인프라 제공자
    gov_agent_share: float = 0.15     # 거버넌스 에이전트 풀
    sec_agent_share: float = 0.15     # 보안 검증 에이전트 풀

    class Config:
        env_prefix = "TREASURY_"

fuel_config = FuelSettings()
tier_config = TierPolicySettings()
treasury_config = TreasurySettings()