# xphi.kernel.space.contract
## @lineage: xphi.bound.space.contract
## @lineage: xphi.xor.space.contract
from enum import Enum
from typing import Optional, Dict, Any
from typing_extensions import Annotated
from pydantic import Field
from xphi.arch.model.surge.model import DynamicSurgeModel 

class CoherenceState(str, Enum):
    STREAMING = "STREAMING"
    COHERENT = "COHERENT"
    FRAGMENTED = "FRAGMENTED"

Uint32 = Annotated[int, Field(ge=0, le=4294967295, description="Rust FFI 호환 uint32")]
ToposId = Annotated[str, Field(pattern=r"^\d+$", description="Topological Snowflake ID (64-bit str)")]

class Contract(DynamicSurgeModel):
    topos_id: Optional[ToposId] = Field(default=None, description="소속 Topos 공간 ID")
    phase_id: Optional[Uint32] = Field(default=None, description="32-bit 차분 신호 (Epoch, Tick, Mag)")
    nexus_id: Optional[Uint32] = Field(default=None, description="Topos(Low32) ^ Phase XOR 결과값")

    kind: str = Field(description="이벤트 성격 (e.g., CORE, SYMLINK, state_transition, consensus_pulse)")
    source: str = Field(description="방출기 식별자 (e.g., wasm_tension, ledger_pulse)")
    state: CoherenceState = Field(default=CoherenceState.STREAMING, description="데이터 무결성 및 흐름 상태")
    payload: Dict[str, Any] = Field(default_factory=dict, description="WASM Residue, Inscribe Payload 등")