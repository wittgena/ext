# xphi.watcher.receptor.wasm
## @lineage: watcher.receptor.wasm
import json
import time
import hashlib
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional

from xphi.bound.adapter.sign import NodeSigner
from xphi.kernel.wasm.broker import DphiBroker
from xphi.bound.adapter.state import StateAdapter
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("receptor.wasm")

@dataclass
class ReceptorSignal:
    requester_id: str
    intent_action: str
    proposed_payload: Dict[str, Any]
    proof_of_compute: Optional[Dict[str, Any]] = None
    max_fuel_budget: int = 10_000_000
    timestamp: float = field(default_factory=time.time)


@dataclass
class ReceptorBindingResult:
    is_bound: bool
    receptor_id: str
    commit_hash: Optional[str] = None
    mutated_parity: Optional[Dict[str, Any]] = None
    fuel_consumed: int = 0
    rupture_reason: Optional[str] = None

class WasmReceptor:
    def __init__(self, broker: DphiBroker, receptor_id: str = "rec_core_v1"):
        self.broker = broker
        self.receptor_id = receptor_id
        self.signer = NodeSigner.get_instance()

    def _hash_canonical_payload(self, payload_dict: dict) -> str:
        """JCS (RFC 8785) 인코딩 후 SHA-256 해시 도출"""
        canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        return hashlib.sha256(canonical_bytes).hexdigest()

    async def transduce_signal(self, signal: ReceptorSignal) -> ReceptorBindingResult:
        log.info(f"[{self.receptor_id}] ⚡ Ingesting external signal: '{signal.intent_action}' from [{signal.requester_id}]")

        ## Phase 1: Intent Validation (의도 및 가스비 한도 검증)
        intent_payload = {
            "requester_id": signal.requester_id,
            "action": signal.intent_action,
            "max_fuel_budget": signal.max_fuel_budget,
            "timestamp": int(signal.timestamp * 1000)
        }
        
        ## dphi.wasm 내 validate_intent Entry 호출
        val_res = await self.broker.invoke("validate_intent", json.dumps(intent_payload))
        if not val_res.success:
            log.warn(f"[{self.receptor_id}] 🚫 Intent Validation Rejected: {val_res.error}")
            return ReceptorBindingResult(
                is_bound=False, 
                receptor_id=self.receptor_id, 
                rupture_reason=f"Intent Rejected: {val_res.error}"
            )

        ## Phase 2: Proof-of-Compute Audit (연산 증명 무결성 검사)
        if signal.proof_of_compute:
            proof_res = await self.broker.invoke("generate_proof", json.dumps(signal.proof_of_compute))
            if not proof_res.success:
                log.error(f"[{self.receptor_id}] 🚨 Invalid Proof-of-Compute Payload")
                return ReceptorBindingResult(
                    is_bound=False, 
                    receptor_id=self.receptor_id, 
                    rupture_reason=f"Proof Audit Failed: {proof_res.error}"
                )

        ## Phase 3: Cryptographic Inscription & Alignment (암호학적 서명 각인)
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=StateAdapter.build_parity_triplet(f"topos_{self.receptor_id}", 1, 0),
            parent_nexus_id=0,
            parent_commit_id="genesis",
            repos={signal.requester_id: self._hash_canonical_payload(signal.proposed_payload)},
            cached_states={}
        )

        canonical_bytes = StateAdapter.to_canonical_bytes(anchor_commit)
        signature_hex = self.signer.sign_payload(canonical_bytes)

        ## Inscribe Payload 생성 (1-of-1 Receptor Authority)
        inscribe_payload = StateAdapter.build_inscribe_payload(
            nexus_id=int(time.time()) % 4294967295,  # u32 호환
            parent_nexus_id=0,
            parent_commit_id="genesis",
            signers=[self.signer.pubkey_hex],
            signatures=[signature_hex],
            threshold=1,
            allowed_signers=[self.signer.pubkey_hex]
        )

        inscribe_json = StateAdapter.to_canonical_bytes(inscribe_payload).decode('utf-8')
        inscribe_res = await self.broker.invoke("inscribe_actor", inscribe_json)
        if not inscribe_res.success:
            log.error(f"[{self.receptor_id}] ❌ Inscription to Ledger failed: {inscribe_res.error}")
            return ReceptorBindingResult(
                is_bound=False, 
                receptor_id=self.receptor_id, 
                rupture_reason=f"Inscription Failed: {inscribe_res.error}"
            )

        inscribe_output = json.loads(inscribe_res.output)
        commit_hash = inscribe_output.get("commit_hash")

        ## Phase 4: Topological State Evolution (위상 상태 전이 확정)
        trans_rule = StateAdapter.build_trans_rule(
            src=signal.requester_id,
            dest=f"receptor:{self.receptor_id}",
            kind="CORE"
        )
        
        evolution_ctx = StateAdapter.build_evolution_context(
            phase_root=StateAdapter.adapt_swarm_to_phase_root(
                commit_hash=commit_hash,
                agents_dict={signal.requester_id: commit_hash}
            ),
            external_rules=[trans_rule]
        )

        transition_payload = StateAdapter.build_transition_payload(
            intent_action=signal.intent_action,
            intent_payload=signal.proposed_payload,
            evolution_ctx=evolution_ctx
        )

        transition_json = StateAdapter.to_canonical_bytes(transition_payload).decode('utf-8')
        trans_res = await self.broker.invoke("execute_transition", transition_json)

        if trans_res.success:
            trans_output = json.loads(trans_res.output)
            log.info(f"[{self.receptor_id}] ✨ Signal successfully bound to dphi.wasm! Commit: {commit_hash[:8]}...")
            
            return ReceptorBindingResult(
                is_bound=True,
                receptor_id=self.receptor_id,
                commit_hash=commit_hash,
                mutated_parity=trans_output.get("final_root"),
                fuel_consumed=getattr(trans_res, 'fuel_consumed', 0)
            )
        else:
            log.error(f"[{self.receptor_id}] 💥 Transition Collapsed: {trans_res.error}")
            return ReceptorBindingResult(
                is_bound=False,
                receptor_id=self.receptor_id,
                rupture_reason=f"Transition Collapsed: {trans_res.error}"
            )