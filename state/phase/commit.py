# xphi.state.phase.commit
## @lineage: xphi.kernel.phase.commit
import json
import time
import asyncio
from typing import Dict, List, Optional, Callable, Any
from pathlib import Path

from xphi.kernel.wasm.broker import DphiBroker
from xphi.kernel.adapter.sign import LedgerAuthAdapter
from xphi.kernel.adapter.state import StateAdapter
from xphi.watcher.plane.emitter import get_emitter
from xphi.state.ledger.consensus import KernelLedger, ToposBlob

log = get_emitter("kernel.protocol", phase="KERNEL")

class Attractor:
    """@role: execution unit + lineage inscription node"""
    def __init__(self, name: str, path: str, runner: Callable):
        self.name = name
        self.path = Path(path).expanduser().resolve()
        self.runner = runner
        self.store = KernelLedger()

    def inscribe(
        self, 
        nexus_id: int, 
        parent_nexus_id: int, 
        parent_commit_id: str, 
        message: str, 
        apply: bool = False,
        tag: Optional[str] = None  # [추가] 글로벌 태그 수신
    ) -> str:
        """
        Models and inscribes the lineage of the current generation.
        @note: String anchor_id is replaced by integer nexus_id for Parity-based deterministic consensus.
        """
        # WASM의 RepoCommit 스키마와 동일한 형태의 딕셔너리 생성
        model_dict = {
            "nexus_id": nexus_id,
            "parent_nexus_id": parent_nexus_id,
            "parent_commit_id": parent_commit_id
        }
        
        # [추가] 커밋 페이로드에 태그 정보 각인 (암호학적 증명에 포함됨)
        if tag:
            model_dict["version_tag"] = tag

        # Git commit 메시지에 물리적으로 각인 (결정론적 직렬화)
        json_payload = json.dumps(model_dict, separators=(',', ':'), sort_keys=True)
        full_message = f"{message}\n\n{json_payload}"
        
        # Git Commit 및 Tag 실행 (Runner로 tag 파라미터 전달)
        new_commit_id = self.runner(self.path, full_message, apply, tag=tag)

        if apply:
            blob = ToposBlob(
                action=f"align.commit::{self.name}",
                from_state=parent_commit_id,
                to_state=new_commit_id,
                tension=1.0, 
                details=json_payload
            )
            self.store.save_transition(blob)
            self.store.update_head(self.name, new_commit_id)

        tag_info = f" | Tag: {tag}" if tag else ""
        print(f"  └─ [{self.name}] Inscribed. Nexus: {nexus_id} | State: {new_commit_id[:7]}{tag_info}")
        return new_commit_id


class EpochManager(Attractor):
    """@role: boundary (synchronization frame) + era manager"""
    ERA_DEPTH = 3

    def __init__(self, name: str, path: str, runner: Callable):
        super().__init__(name, path, runner)
        self.registry_key = f"legacy_registry:{self.name}".encode('utf-8')

    def load_history(self) -> List[Dict]:
        """
        @desc: Perfect backward compatibility method for external protocol.commit invocations.
               Extracts and returns the legacy .registry.json array format directly from RocksDB.
        """
        if self.registry_key in self.store.db:
            try:
                raw_data = self.store.db[self.registry_key].decode('utf-8')
                return json.loads(raw_data).get("history", [])
            except Exception:
                pass
        return []

    def resolve(self, repo_name: str) -> str:
        """Resolves the state based on historical consistency rather than the absolute HEAD from KernelStore."""
        history = self.load_history()
        for snapshot in reversed(history[-self.ERA_DEPTH:]):
            if repo_name in snapshot.get("repos", {}):
                return snapshot["repos"][repo_name]
            if repo_name in snapshot.get("cached_states", {}):
                return snapshot["cached_states"][repo_name]
        return "0000000"

    def project(self, states: Dict[str, str]) -> Dict[str, str]:
        return states

async def anchor_commit(
    repos: List[Attractor], 
    anchor: EpochManager, 
    broker: DphiBroker, 
    message: str, 
    apply: bool = False,
    tag: Optional[str] = None  # [추가] Commiter로부터 태그 수신
) -> None:
    mode = "APPLY" if apply else "DRY-RUN"
    log.info(f"## Era-based Alignment Cycle Initiated ({mode})")
    current_ts = int(time.time() * 1000)
    
    init_req: Dict[str, Any] = {
        "ts": current_ts,
        "topo": 1,
        "press": len(repos),
        "rupture": False,
        "injected_tick": None
    }
    
    # [추가] WASM이 초기 Parity 생성 시 태그를 인지할 수 있도록 주입
    if tag:
        init_req["version_tag"] = tag
        log.info(f"[Protocol] Global Version Tag '{tag}' will be cryptographically sealed.")
    
    log.info("[Protocol] Requesting Parity Triplet from WASM Engine...")
    init_payload_str = StateAdapter.to_canonical_bytes(init_req).decode('utf-8')
    init_res = await broker.invoke("init_epoch", init_payload_str)
    
    if not init_res.success:
        log.error(f"Failed to initialize Epoch via WASM: {str(init_res.error)}")
        return
        
    parity = json.loads(init_res.output)
    nexus_id = parity.get("nexus_id")
    log.info(f"[Protocol] Nexus ID Generated: {nexus_id}")

    # PHASE 2: Physical Commits (Thread Offloading)
    history = anchor.load_history()
    last_snapshot = history[-1] if history else None
    parent_nexus_id = last_snapshot.get("parity", {}).get("nexus_id", 0) if last_snapshot else 0
    current_aligned_states: Dict[str, str] = {}
    
    for r in repos:
        parent_state = anchor.resolve(r.name)
        commit_hash = await asyncio.to_thread(
            r.inscribe,
            nexus_id=nexus_id,
            parent_nexus_id=parent_nexus_id,
            parent_commit_id=parent_state,
            message=message,
            apply=apply,
            tag=tag  # [추가] 개별 리포지토리(Attractor)에 태그 전파
        )
        current_aligned_states[r.name] = commit_hash

    cached_states: Dict[str, str] = {}
    if last_snapshot:
        prev_total = {**last_snapshot.get("repos", {}), **last_snapshot.get("cached_states", {})}
        for name, last_hash in prev_total.items():
            if name not in current_aligned_states and name != anchor.name:
                cached_states[name] = last_hash

    anchor_commit_dict = StateAdapter.build_anchor_commit(
        parity=parity,
        parent_nexus_id=parent_nexus_id,
        parent_commit_id=anchor.resolve(anchor.name),
        repos=current_aligned_states,
        cached_states=cached_states
    )
    
    signature_hex = LedgerAuthAdapter.sign_state_payload(anchor_commit_dict)
    current_pubkey = LedgerAuthAdapter.get_signer_pubkey()
    seal_payload = StateAdapter.build_seal_epoch_payload(
        parity=parity,
        parent_nexus_id=parent_nexus_id,
        self_parent_state=anchor.resolve(anchor.name),
        repos=current_aligned_states,
        cached_states=cached_states,
        timestamp=float(current_ts),
        signers=[current_pubkey],        # Extracted via LedgerAuthAdapter
        signatures=[signature_hex],      # Extracted via LedgerAuthAdapter
        threshold=1                      # Single active signer threshold
    )

    log.info("[Protocol] Sealing Epoch cryptographically...")
    seal_payload_str = StateAdapter.to_canonical_bytes(seal_payload).decode('utf-8')
    seal_res = await broker.invoke("seal_epoch", seal_payload_str)
    
    if not seal_res.success:
        log.critical(f"## Epoch Seal Rejected by WASM: {str(seal_res.error)}")
        if apply:
            log.warning("Physical commits were made, but WASM seal failed. Requires subsequent sync to recover.")
        return

    if apply:
        sealed_data = json.loads(seal_res.output)
        kernel_commit_data = sealed_data.get("kernel_commit")
        
        from xphi.state.ledger.consensus import KernelCommit, LedgerRole
        from dataclasses import asdict
        
        commit_obj = KernelCommit(**kernel_commit_data)
        
        if hasattr(anchor.store, 'role') and anchor.store.role == LedgerRole.FOLLOWER:
            log.warning("[Protocol] Node is FOLLOWER. Proposing Epoch Seal to Mempool instead of direct disk write.")
            anchor.store._put_object("commit_proposal", asdict(commit_obj))
        else:
            try:
                commit_hash = anchor.store.seal_system_epoch(
                    commit=commit_obj, 
                    signatures=[signature_hex], 
                    threshold=1
                )
                
                anchor.store.update_head("global_era_anchor", sealed_data["anchor_result"]["commit_hash"])
                for repo_name, repo_hash in current_aligned_states.items():
                    anchor.store.update_head(repo_name, repo_hash)
                    
            except PermissionError as pe:
                log.critical(f"Physical state finalization aborted due to Kernel Store privilege denial: {pe}")
                return
            
        # Append to Legacy Registry for backwards compatibility
        full_history = history + [seal_payload]
        try:
            if hasattr(anchor.store, 'db') and anchor.store.db is not None:
                anchor.store.db[anchor.registry_key] = json.dumps({"history": full_history}).encode('utf-8')
        except Exception as e:
            log.warning(f"Failed to update legacy registry: {e}")

    log.info(f"## Era Fixed. Nexus: {nexus_id} (Aligned: {len(current_aligned_states)}, Lagged: {len(cached_states)})")