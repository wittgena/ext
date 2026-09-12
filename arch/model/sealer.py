# xphi.arch.model.sealer
## @lineage: arch.model.sealer
## @lineage: arch.bound.sealer
## @lineage: arch.topos.bound.sealer
import time
from typing import Dict, Any
from xphi.arch.bound.adapter.sign import NodeSigner
from xphi.arch.bound.adapter.state import StateAdapter
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("topos.sealer")

class EpochSealer:
    """
    @desc: Encapsulates the cryptographic sealing logic for WASM epochs.
           Coordinates NodeSigner (Identity) and StateAdapter (FFI Schema) 
           to generate deterministic, signed JCS payloads.
    """
    @staticmethod
    def generate_seal_payload(entangled_state: Dict[str, Any], parent_commit_id: str = "genesis") -> str:
        """
        주어진 상태 데이터를 바탕으로 WASM 'seal_epoch' 호출용 JCS 문자열을 생성합니다.
        """
        # 1. 노드 신원(Identity) 로드
        signer = NodeSigner.get_instance()
        pubkey = signer.pubkey_hex
        
        parity = entangled_state.get("parity", {})
        repos = entangled_state.get("repos", {})
        timestamp_now = time.time()
        
        # 2. 서명 대상이 될 Anchor Commit (데이터 패킷) 생성
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=parity,
            parent_nexus_id=0,
            parent_commit_id=parent_commit_id,
            repos=repos,
            cached_states={}
        )
        
        # 3. JCS 직렬화 후 서명 생성 (WASM 검증 규칙과 동일한 방식 적용)
        canonical_bytes = StateAdapter.to_canonical_bytes(anchor_commit)
        signature_hex = signer.sign_payload(canonical_bytes)
        
        log.debug(f"[Sealer] Generated Ed25519 signature for epoch (Signer: {pubkey[:8]}...)")
        
        # 4. 서명 데이터가 포함된 최종 Seal 페이로드 조립
        seal_payload_dict = StateAdapter.build_seal_epoch_payload(
            parity=parity,
            parent_nexus_id=0,
            self_parent_state=parent_commit_id,
            repos=repos,
            cached_states={},
            timestamp=timestamp_now,
            signers=[pubkey],             # 추출한 노드 공개키
            signatures=[signature_hex],   # 생성된 서명
            threshold=1,                  # 1-of-1 서명 요구
            allowed_signers=[pubkey]      # ACL(화이트리스트)
        )
        
        # 5. FFI 전달을 위한 JCS (Canonical JSON) 문자열 반환
        return StateAdapter.to_canonical_bytes(seal_payload_dict).decode('utf-8')