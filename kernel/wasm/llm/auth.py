# xphi.kernel.wasm.llm.auth
## @lineage: xphi.kernel.dphi.llm.auth
## @lineage: xphi.kernel.dphi.schema
## @lineage: kernel.dphi.schema
from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional

class DphiKey(str, Enum):
    """Pipeline 및 Metadata에 주입될 DPHI 도메인의 Key 상수 모음"""
    KERNEL_AUTH = "kernel_auth"
    X402_RECEIPT = "x402_receipt"
    CLIENT_HOST = "client_host"
    FUEL_BUDGET = "fuel_budget"
    FUEL_CONSUMED = "fuel_consumed"
    AUDIT_HASH = "audit_hash"

class DphiAction(str, Enum):
    """Broker Intent Action 상수"""
    LLM_COMPUTE = "LLM_COMPUTE"
    LLM_EMBEDDING = "LLM_EMBEDDING"

class KernelAuthPayload(BaseModel):
    """Broker에서 반환되는 Kernel Auth 데이터 규격"""
    fuel_budget: float = Field(default=float('inf'), description="할당된 최대 토큰/연료")
    audit_hash: Optional[str] = Field(default=None, description="영수증 봉합을 위한 해시")
    # 향후 권한 관련 필드 추가 시 여기에 집중