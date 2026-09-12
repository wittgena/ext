# xphi.arch.bound.adapter.pta
## @lineage: xphi.bound.adapter.pta
## @lineage: xphi.kernel.adapter.pta
import json
import time
import uuid
import hashlib
import base64
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field, asdict

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from xphi.state.ledger.consensus import KernelLedger, LogicStream
from xphi.state.ledger.oracle import LedgerOracle
from xphi.kernel.wasm.broker import DphiBroker
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("adapter.pta", phase="KERNEL")

class AgentWallet:
    """Ed25519 기반의 실제 암호학적 지갑 (서명 및 검증용)"""
    def __init__(self):
        self.private_key = ed25519.Ed25519PrivateKey.generate()
        self.public_key = self.private_key.public_key()
        raw_pub = self.public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        )
        self.address = f"cosm_{base64.urlsafe_b64encode(raw_pub).decode().rstrip('=')}"

    def sign_payload(self, payload: str) -> str:
        """주어진 페이로드를 Private Key로 서명"""
        signature = self.private_key.sign(payload.encode('utf-8'))
        return base64.urlsafe_b64encode(signature).decode()


def compute_merkle_root(tx_hashes: List[str]) -> str:
    if not tx_hashes:
        return ""
    
    current_level = tx_hashes
    while len(current_level) > 1:
        next_level = []
        for i in range(0, len(current_level), 2):
            left = current_level[i]
            right = current_level[i+1] if i+1 < len(current_level) else left
            combined = f"{left}{right}".encode('utf-8')
            next_level.append(hashlib.sha256(combined).hexdigest())
        current_level = next_level
        
    return current_level[0]


@dataclass
class PtaPointer:
    """이전 트랜잭션의 특정 Output을 가리키는 포인터 (OutPoint)"""
    tx_hash: str
    output_index: int

    def to_key(self) -> str:
        return f"{self.tx_hash}:{self.output_index}"


@dataclass
class PtaInput:
    """PTA를 소모하기 위한 입력값 (이전 Output의 포인터와 소유자 서명)"""
    pointer: PtaPointer
    signature: str  # 소모 권한 증명 (Ed25519 Signature)
    owner_address: str = "" # 서명 검증을 위한 퍼블릭 키(주소) 힌트


@dataclass
class PtaOutput:
    """새롭게 생성되는 가치의 단위"""
    amount: int
    owner: str
    asset_type: str = "fuel"


@dataclass
class PhaseAnchorOutput(PtaOutput):
    handle_id: str = ""
    phase_status: str = ""    # PENDING, YIELD, RESOLVED 등
    executable_payload: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self.asset_type = "mcp_state_anchor"

def create_state_anchor(handle_id: str, status: str, payload: Dict[str, Any] = None) -> PhaseAnchorOutput:
    if payload is None:
        payload = {}
        
    return PhaseAnchorOutput(
        amount=0,                              # 상태 앵커는 금융적 가치가 없으므로 0
        owner=f"mcp_bridge_{handle_id}",       # 이 핸들의 소유권은 해당 브릿지 세션에 귀속됨
        handle_id=handle_id,
        phase_status=status.upper(),
        executable_payload=payload
    )


@dataclass
class PtaTransaction:
    """입력들을 소모하여 새로운 출력들을 만들어내는 상태 전이의 최소 단위 (Split / Merge 지원)"""
    inputs: List[PtaInput]
    outputs: List[PtaOutput]
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    tx_hash: str = field(init=False)

    def __post_init__(self):
        self.tx_hash = self._compute_hash()

    def _compute_hash(self) -> str:
        payload = {
            "inputs": [{"tx": i.pointer.tx_hash, "idx": i.pointer.output_index} for i in self.inputs],
            "outputs": [asdict(o) for o in self.outputs],
            "meta": self.metadata,
            "ts": self.timestamp_ms
        }
        canonical_str = json.dumps(payload, sort_keys=True)
        return hashlib.sha256(canonical_str.encode('utf-8')).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tx_hash": self.tx_hash,
            "inputs": [{"pointer": i.pointer.to_key(), "signature": i.signature, "owner": i.owner_address} for i in self.inputs],
            "outputs": [asdict(o) for o in self.outputs],
            "metadata": self.metadata,
            "timestamp_ms": self.timestamp_ms
        }


class PtaAdapter:
    """PTA 모델 기반의 오프체인 마이크로 빌링 및 상태 병합 어댑터"""
    def __init__(self, broker: DphiBroker):
        self.broker = broker
        self.ledger = KernelLedger()
        self.oracle = LedgerOracle(broker=broker)
        self._unfold_pool: Dict[str, PtaOutput] = {}

    def _verify_ed25519_signature(self, payload: str, signature_b64: str, owner_address: str) -> bool:
        """어댑터 레벨에서 발생하는 실제 연산 로드 (타원곡선 암호학 검증)"""
        if not owner_address.startswith("cosm_"):
            return True
            
        try:
            ## Base64 패딩 복구 및 퍼블릭 키 추출
            b64_pub = owner_address.replace("cosm_", "")
            b64_pub += "=" * ((4 - len(b64_pub) % 4) % 4)
            raw_pub = base64.urlsafe_b64decode(b64_pub)
            pub_key = ed25519.Ed25519PublicKey.from_public_bytes(raw_pub)
            
            ## 서명 바이트 추출
            sig_b64 = signature_b64 + "=" * ((4 - len(signature_b64) % 4) % 4)
            sig_bytes = base64.urlsafe_b64decode(sig_b64)
            pub_key.verify(sig_bytes, payload.encode('utf-8'))
            return True
        except Exception as e:
            log.warning(f"[PtaAdapter] Signature verification failed for payload '{payload}': {e}")
            return False

    async def execute_transaction(self, tx: PtaTransaction) -> str:
        """PTA 상태 전이를 검증하고 Ledger에 제안 및 밀봉(Seal)"""
        for idx, current_input in enumerate(tx.inputs):
            if current_input.owner_address:
                payload_to_verify = current_input.pointer.to_key()
                is_valid = self._verify_ed25519_signature(payload_to_verify, current_input.signature, current_input.owner_address)
                if not is_valid:
                    raise PermissionError(f"Cryptographic Auth Failed for input index {idx}")

        stream_id = f"pta_tx_{tx.tx_hash[:8]}"
        stream = LogicStream(
            id=stream_id,
            action="PTA_STATE_TRANSITION",
            payload=tx.to_dict(),
            metadata=tx.metadata
        )
        log.debug(f"[PtaAdapter] Proposing PTA Tx: {tx.tx_hash[:8]}... (Inputs: {len(tx.inputs)}, Outputs: {len(tx.outputs)})")
        sealed_kernel = await self.ledger.propose_and_seal(stream)
        
        if sealed_kernel:
            receipt_signature = sealed_kernel.signature
            log.info(f"[PtaAdapter] Tx Sealed. Receipt: {receipt_signature[:8]}... | Hash: {tx.tx_hash[:8]}")
            
            ## 소모된 Input 제거 (Burn)
            for current_input in tx.inputs:
                pointer_key = current_input.pointer.to_key()
                self._unfold_pool.pop(pointer_key, None)
            
            ## 새롭게 생성된 Output 등록 (Mint)
            for idx, output in enumerate(tx.outputs):
                new_pointer_key = f"{tx.tx_hash}:{idx}"
                self._unfold_pool[new_pointer_key] = output
                
            return tx.tx_hash
        else:
            log.debug(f"[PtaAdapter] Tx Queued in Mempool. Stream: {stream_id}")
            return tx.tx_hash

    async def get_balance(self, owner_address: str, asset_type: str = "fuel") -> int:
        """@desc: 외부 Edge 레이어(API)에서 호출할 잔고 조회 접점"""
        total_balance = 0
        for output in self._unfold_pool.values():
            if output.owner == owner_address and output.asset_type == asset_type:
                total_balance += output.amount
                
        log.debug(f"[PtaAdapter] Balance read for {owner_address[:12]}... -> {total_balance} {asset_type}")
        return total_balance

    async def verify_and_collapse_receipt(self, tx_hash: str) -> Dict[str, Any]:
        try:
            log.info(f"[PtaAdapter] Requesting Oracle collapse for PTA Tx: {tx_hash[:8]}...")
            collapsed_state = await self.oracle.observe_nexus(tx_hash)
            return collapsed_state
        except Exception as e:
            log.error(f"[PtaAdapter] Receipt collapse failed: {e}")
            raise

    async def verify_lineage(self, tx_hash: str, depth: int = 5) -> bool:
        try:
            res = await self.oracle.verify_kernel_lineage(tx_hash, depth)
            is_valid = res.get("is_valid", False)
            if not is_valid:
                log.warning(f"[PtaAdapter] PTA Lineage verification failed for {tx_hash[:8]}")
            return is_valid
        except Exception as e:
            log.error(f"[PtaAdapter] Lineage verification error: {e}")
            return False

    def shutdown(self):
        self.oracle.close()