# xphi.state.ledger.oracle
## @lineage: xphi.kernel.dphi.ledger.oracle
import json
from typing import Any, Dict, Optional, List
from rocksdict import Rdict, Options, AccessType

from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.wasm.broker import DphiBroker, DphiMethod
from xphi.kernel.wasm.adapter.state import StateAdapter

log = get_emitter("ledger.oracle", phase="KERNEL")
LEDGER_DB_PATH = resolve_path("ledger")

class LedgerOracle:
    def __init__(self, broker: DphiBroker, path: str = LEDGER_DB_PATH):
        self.broker = broker
        ro_opt = Options()
        try:
            self.db = Rdict(str(path), ro_opt, access_type=AccessType.read_only())
            log.info("[LedgerOracle] Mounted ledger DB in Read-Only mode. Canonical WASM Broker attached.")
        except Exception as e:
            log.error(f"[LedgerOracle] Failed to mount ledger DB: {e}")
            raise

    def close(self):
        if hasattr(self, 'db') and self.db is not None:
            self.db.close()

    def _get_raw_object(self, obj_type: str, obj_hash: str) -> Optional[Dict[str, Any]]:
        key = f"{obj_type}:{obj_hash}".encode('utf-8')
        if key in self.db:
            return json.loads(self.db[key].decode('utf-8'))
        return None

    async def observe_nexus(self, epoch_hash: str) -> Dict[str, Any]:
        snapshot = self._get_raw_object("commit", epoch_hash)
        if not snapshot:
            raise ValueError(f"Epoch not found in Ledger: {epoch_hash}")

        entangled_state = snapshot.get("entangled_state", {})
        if not entangled_state.get("has_contention", False):
            return {"epoch_hash": epoch_hash, "resolved_state": snapshot, "is_collapsed": False}

        raw_phase_root = snapshot.get("phase_root")
        if raw_phase_root:
            phase_root = StateAdapter.build_core_node(
                name=raw_phase_root.get("name", "root"), 
                content=raw_phase_root.get("content", ""), 
                children=raw_phase_root.get("children")
            )
        else:
            phase_root = StateAdapter.build_core_node("root", snapshot.get("parent_hash", "genesis"))

        evo_ctx = StateAdapter.build_evolution_context(phase_root=phase_root)
        transition_payload = StateAdapter.build_transition_payload(
            intent_action="COLLAPSE_ENTANGLEMENT",
            intent_payload=entangled_state,
            evolution_ctx=evo_ctx
        )

        canonical_payload = StateAdapter.to_canonical_bytes(transition_payload).decode('utf-8')
        log.info(f"[LedgerOracle] Injecting canonical entanglement into WASM for collapse. Epoch: {epoch_hash[:8]}")
        exec_result = await self.broker.invoke(
            target_func=DphiMethod.EXECUTE_TRANSITION,
            payload=canonical_payload,
            tier="KERNEL"
        )

        if not exec_result.success:
            log.error(f"[LedgerOracle] WASM Collapse Failed: {exec_result.error.msg}")
            raise RuntimeError(f"WASM State Transition Failed: {exec_result.error.msg}")

        collapsed_state = json.loads(exec_result.output)
        return {
            "epoch_hash": epoch_hash,
            "parent_hash": snapshot.get("parent_hash"),
            "resolved_state": collapsed_state.get("final_root"),
            "residues": collapsed_state.get("all_residues", []),
            "is_collapsed": True
        }

    async def verify_kernel_lineage(self, start_commit_hash: str, depth: int = 5) -> Dict[str, Any]:
        """StateAdapter를 통해 ParityTriplet을 조립하여 WASM 엔진에서 무결성을 검증합니다."""
        chain_trace = []
        current_hash = start_commit_hash
        traversed_depth = 0

        while current_hash and current_hash != "genesis" and traversed_depth < depth:
            snapshot = self._get_raw_object("commit", current_hash)
            if not snapshot:
                return {"is_valid": False, "rupture_hash": current_hash, "trace": chain_trace}
            
            parity_req = StateAdapter.build_parity_triplet(
                topos_id=str(snapshot.get("topos_id", "0")),
                phase_id=int(snapshot.get("phase_id", 0)),
                nexus_id=int(snapshot.get("nexus_id", 0))
            )
            
            canonical_parity = StateAdapter.to_canonical_bytes(parity_req).decode('utf-8')
            res = await self.broker.invoke(DphiMethod.VERIFY_PARITY, canonical_parity)
            if not res.success:
                log.warning(f"[LedgerOracle] WASM Parity verification crashed at {current_hash[:8]}")
                return {"is_valid": False, "rupture_hash": current_hash, "trace": chain_trace}

            output = json.loads(res.output)
            is_valid = output.get("is_valid", False)
            if not is_valid and "recovered_missing" in output:
                log.warning(f"[LedgerOracle] ⚠️ Parity fractured but recovered via XOR at {current_hash[:8]}")
                is_valid = True

            if not is_valid:
                log.warning(f"[LedgerOracle] Parity invalid at {current_hash[:8]}")
                return {"is_valid": False, "rupture_hash": current_hash, "trace": chain_trace}

            chain_trace.append(current_hash)
            current_hash = snapshot.get("parent_hash")
            traversed_depth += 1

        return {"is_valid": True, "reached_genesis": current_hash == "genesis", "trace": chain_trace}