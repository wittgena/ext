# xphi.state.ledger.consensus
import time
import json
import hashlib
import asyncio
from enum import Enum
from typing import Any, Dict, Optional, List
from dataclasses import dataclass, field, asdict
from pydantic import BaseModel, Field
from rocksdict import Rdict, Options, AccessType

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.kernel.space.bind.resolver import resolve_path

from xphi.kernel.wasm.broker import DphiBroker  
from xphi.arch.bound.adapter.sign import NodeSigner
from xphi.arch.bound.adapter.state import StateAdapter
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("kernel.ledger", phase="KERNEL")

LEDGER_DB_PATH = resolve_path("ledger")

class LedgerRole(Enum):
    LEADER = "SEALER"        # Acquired physical lock; can invoke WASM and seal states
    FOLLOWER = "PROPOSER"    # Read-Only mode; proposes raw streams to Mempool

def deterministic_hash(data: Dict[str, Any]) -> str:
    canonical_bytes = StateAdapter.to_canonical_bytes(data)
    return hashlib.sha256(canonical_bytes).hexdigest()

class LogicStream(BaseModel):
    """Ψ_open - The external request flowing into the system."""
    id: str
    action: str = "default_action"
    payload: Any  
    metadata: Dict[str, Any] = Field(default_factory=dict)

@dataclass
class ToposBlob:
    """OOB Log / Residue representation from WASM transitions or System Anomalies."""
    action: str
    from_state: str
    to_state: str
    tension: float
    details: str
    timestamp: float = field(default_factory=time.time)

@dataclass
class KernelCommit:
    """Sealed kernel snapshot verified by WASM or Multi-Sig Core."""
    stream_id: str
    executable_payload: Any
    tension_at_seal: float
    blob_hashes: List[str]
    parent_hash: Optional[str] = None
    sealed_at: float = field(default_factory=time.time)

class SealedKernel(BaseModel):
    """Ω_knot - The mathematically invariant, executable closed boundary."""
    kernel_id: str
    stream_id: str
    executable_payload: Any
    tension_at_seal: float
    signature: str 

class KernelLedger:
    _instance = None

    def __new__(cls, path=LEDGER_DB_PATH):
        if cls._instance is None:
            cls._instance = super(KernelLedger, cls).__new__(cls)
            cls._instance._initialize_consensus_node(path)
        return cls._instance

    def _initialize_consensus_node(self, target_path: str):
        """Proof of Lock: Determines LEADER/FOLLOWER role and binds system handlers."""
        opt = Options()
        opt.create_if_missing(True)
        
        try:
            self.db = Rdict(str(target_path), opt)
            self.role = LedgerRole.LEADER
            self.broker = None
            self.wasm = DphiBroker() 
            log.info(f"[Ledger] Acquired physical lock. Operating as {self.role.value}. WASM Kernel mounted.")
            
        except Exception as e:
            error_msg = str(e).lower()
            if "lock" in error_msg or "temporarily unavailable" in error_msg:
                self.role = LedgerRole.FOLLOWER
                self.wasm = None 
                log.info(f"[Ledger] Lock held by another node. Operating as {self.role.value} (Read-Only/Proposer).")
                
                ro_opt = Options()
                self.db = Rdict(str(target_path), ro_opt, access_type=AccessType.read_only())
                self.broker = TunnelFactory.get_sync()
            else:
                raise 

        # [순환 참조 해결] AuditWarden 지연 로딩 (Lazy Import)
        from xphi.watcher.receptor.warden import AuditWarden
        AuditWarden.register_anomaly_handler(self._handle_warden_anomaly)

    def _handle_warden_anomaly(self, action: str, details: str) -> None:
        blob = ToposBlob(
            action=f"SYS_WARDEN_GUARD::{action}", 
            from_state="host.python.sandbox", 
            to_state="host.os.kernel",
            tension=1.0, 
            details=details
        )
        self.save_transition(blob)

    def _put_object(self, obj_type: str, data: Dict[str, Any]) -> str:
        """Routes to either physical disk write (LEADER) or Mempool proposal (FOLLOWER)."""
        obj_hash = deterministic_hash(data)
        
        if self.role == LedgerRole.LEADER:
            key = f"{obj_type}:{obj_hash}".encode('utf-8')
            if key not in self.db:
                self.db[key] = json.dumps(data).encode('utf-8')
            return obj_hash
        else:
            payload = {"type": obj_type, "hash": obj_hash, "data": json.dumps(data)}
            try:
                self.broker.stream_produce(f"ledger:mempool:{obj_type}s", payload)
                log.debug(f"[Ledger:FOLLOWER] Proposed {obj_type} {obj_hash[:8]} to mempool.")
            except Exception as e:
                log.error(f"[Ledger:FOLLOWER] Mempool propose failed: {e}")
            return obj_hash

    def save_transition(self, blob: ToposBlob) -> str:
        return self._put_object("blob", asdict(blob))

    def update_head(self, stream_id: str, commit_hash: str) -> None:
        if self.role == LedgerRole.LEADER:
            key = f"ref:{stream_id}".encode('utf-8')
            self.db[key] = commit_hash.encode('utf-8')
        else:
            payload = {"type": "head_update", "stream_id": stream_id, "commit_hash": commit_hash}
            try:
                self.broker.stream_produce("ledger:mempool:heads", payload)
            except Exception:
                pass

    def get_head_hash(self, stream_id: str) -> Optional[str]:
        key = f"ref:{stream_id}".encode('utf-8')
        if key in self.db:
            return self.db[key].decode('utf-8')
        return None

    def _save_kernel_unsafe(self, commit: KernelCommit) -> str:
        return self._put_object("commit", asdict(commit))

    def seal_system_epoch(self, commit: KernelCommit, signatures: List[str], threshold: int = 1) -> str:
        # [순환 참조 해결] AuditWarden 지연 로딩 (Lazy Import)
        from xphi.watcher.receptor.warden import AuditWarden

        if not signatures:
            error_msg = "System Epoch Seal Rejected: No signatures provided."
            log.critical(f"[KernelStore: PRIVILEGE] {error_msg}")
            AuditWarden.record_anomaly("kernel.privilege_escalation_attempt", error_msg)
            raise PermissionError(error_msg)

        signer = NodeSigner.get_instance()
        canonical_bytes = StateAdapter.to_canonical_bytes(asdict(commit))
        valid_count = 0
        for sig in signatures:
            try:
                # NodeSigner verifies the signature against the canonical payload hash
                if signer.verify_signature(canonical_bytes, sig):
                    valid_count += 1
            except Exception as e:
                log.debug(f"[KernelStore] Invalid signature fragment detected: {e}")
                continue

        if valid_count < threshold:
            error_msg = f"Multi-Sig Threshold Failed: {valid_count}/{threshold} valid signatures."
            log.critical(f"[KernelStore: PRIVILEGE] {error_msg}")
            AuditWarden.record_anomaly("kernel.privilege_escalation_attempt", error_msg)
            raise PermissionError(error_msg)

        log.info(f"[KernelStore: PRIVILEGE] Multi-Sig Verified ({valid_count}/{threshold}). Authorized direct ledger commit.")
        return self._save_kernel_unsafe(commit)

    async def propose_and_seal(self, stream: LogicStream) -> Optional[SealedKernel]:
        if self.role == LedgerRole.FOLLOWER:
            stream_data = {
                "id": str(stream.id), 
                "action": str(stream.action), 
                "payload": json.dumps(stream.payload) if isinstance(stream.payload, (dict, list)) else str(stream.payload), 
                "metadata": json.dumps(stream.metadata) if isinstance(stream.metadata, dict) else str(stream.metadata)
            }
            self.broker.stream_produce("ledger:mempool:logic_streams", stream_data)
            return None

        parent_hash = self.get_head_hash(stream.id) or "genesis"
        transition_payload = {
            "intent_action": stream.action,
            "intent_payload": stream.payload,
            "evolution_ctx": {
                "phase_root": {
                    "name": f"root_{stream.id}",
                    "kind": "CORE",
                    "content": json.dumps(stream.metadata),
                    "ref_target": parent_hash,
                    "children": {}
                },
                "external_rules": [] 
            }
        }

        payload_json = json.dumps(transition_payload)
        wasm_res = await self.wasm.invoke("execute_transition", payload_json)

        if not wasm_res.success:
            log.error(f"[Ledger:WASM] FATAL: Kernel panicked or rejected execution. {wasm_res.error}")
            return None

        trans_result = json.loads(wasm_res.output)

        if not trans_result.get("is_authorized", False):
            err_msg = trans_result.get("error_msg", "Spatial Fence Denied")
            log.warning(f"[Ledger:WASM] REJECTED: {err_msg}")
            
            await asyncio.to_thread(
                self.save_transition, 
                ToposBlob(action="wasm.reject", from_state="logic.stream", to_state="fragmented", tension=1.0, details=err_msg)
            )
            return None

        final_root = trans_result.get("final_root")
        all_residues = trans_result.get("all_residues", [])
        tension_at_seal = len(all_residues) * 0.1 

        blob_hashes = []
        for residue in all_residues:
            blob = ToposBlob(
                action="wasm.mutation",
                from_state="logic.loop",
                to_state="sealed.kernel",
                tension=tension_at_seal,
                details=f"[{residue['kind']}] {residue['msg']}"
            )
            b_hash = await asyncio.to_thread(self.save_transition, blob)
            blob_hashes.append(b_hash)

        commit = KernelCommit(
            stream_id=stream.id,
            executable_payload=final_root,
            tension_at_seal=tension_at_seal,
            blob_hashes=blob_hashes,
            parent_hash=parent_hash
        )

        # Ring 3 bypasses the external Multi-Sig requirement because WASM inherently approved it
        signature = await asyncio.to_thread(self._save_kernel_unsafe, commit)
        await asyncio.to_thread(self.update_head, stream.id, signature)

        log.info(f"[Ledger:LEADER] ✨ Epoch Sealed by WASM. Stream: {stream.id} | Commit: {signature[:8]}")

        return SealedKernel(
            kernel_id=f"ker_{stream.id}",
            stream_id=stream.id,
            executable_payload=final_root,
            tension_at_seal=tension_at_seal,
            signature=signature
        )

    def close(self):
        if hasattr(self, 'db') and self.db is not None:
            self.db.close()
            role_msg = self.role.value if hasattr(self, 'role') else "UNKNOWN"
            log.info(f"[KernelStore] Ledger DB closed cleanly. ({role_msg})")