# xphi.kernel.space.sandbox.runner
## @lineage: xphi.bound.space.sandbox.runner
## @lineage: xphi.xor.space.sandbox.runner
## @lineage: xphi.xor.space.runner
import asyncio
import time
import hashlib
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable, Union
from enum import Enum

import httpx
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from xphi.kernel.space.contract import Contract, CoherenceState
from xphi.kernel.space.sandbox.resolver import SandboxExecutor, TaskContext, EffectResolver
from xphi.kernel.wasm.broker import DphiBroker
from xphi.arch.bound.adapter.state import StateAdapter
from xphi.state.ledger.consensus import KernelLedger, KernelCommit
from xphi.kernel.wasm.method import DphiMethod
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("space.runner")

class RecoveryMethod(str, Enum):
    SEAL_VOID_EPOCH = "seal_void_epoch"

# =====================================================================
# 1. Base Evaluation & Execution Runners
# =====================================================================

class BaseRunner:
    def __init__(self):
        self.success_count = 0
        self.fail_count = 0
        self.last_failed_context: List[str] = []
        self.failed_cases: List[Dict[str, str]] = []

    def report(self):
        log.info(f"\n=== [DONE] Scenarios Completed: {self.success_count} Passed, {self.fail_count} Failed ===")
        if self.fail_count > 0:
            log.warning("Review the following failed contexts:")
            for ctx in self.last_failed_context:
                log.warning(f" - {ctx}")

    def _record_success(self, elapsed_ms: float, msg: str):
        self.success_count += 1
        safe_msg = str(msg).replace('\n', ' ')[:150]
        log.info(f"  [PASS] Time: {elapsed_ms:.2f}ms | Output: {safe_msg}")

    def _record_fail(self, elapsed_ms: float, error_msg: str, context: str, title: str = "Unknown Test Case"):
        self.fail_count += 1
        log.error(f"  [FAIL] Time: {elapsed_ms:.2f}ms | Details: {error_msg}")
        self.last_failed_context.append(context)
        self.failed_cases.append({
            "title": title,
            "error": f"[{context}] {error_msg}"
        })

class SchemeRunner(BaseRunner):
    def __init__(self, broker: Any):
        super().__init__()
        self.broker = broker

    async def _set_worker_policy(self, tier_name: str):
        log.info(f"\n[Control Plane] Shifting WasmCgroup Policy Tier -> {tier_name}")
        if hasattr(self.broker, "update_policy"):
            await self.broker.update_policy(tier=tier_name)
            log.info(f"  └─ Policy successfully enforced to {tier_name}.")
        else:
            log.warning("  └─ Broker missing 'update_policy' API.")

    async def _run_case(
        self, 
        title: str, 
        target_func: Union[str, DphiMethod, RecoveryMethod],
        payload: Any, 
        expected_success: bool, 
        expected_match: Optional[str] = None,
        custom_validator: Optional[Callable[[str], bool]] = None,
        tier: Optional[str] = None
    ):
        func_name = target_func.value if isinstance(target_func, Enum) else target_func
        log.info(f"\n[TEST] {title} (Func: {func_name})")
        start_time = time.time()
        
        # 1회성 Tier 주입 실행
        result = await self.broker.invoke(target_func=func_name, payload=payload, tier=tier)
        elapsed_ms = (time.time() - start_time) * 1000
        
        output_str = str(result.output) if result.success else str(result.error)
        safe_payload_str = str(payload)[:50]

        if result.success != expected_success:
            self._record_fail(
                elapsed_ms, 
                f"Expected success={expected_success}, Got success={result.success}. Output: {output_str}",
                f"Function: {func_name} | Input: {safe_payload_str}...",
                title=title
            )
            return

        if expected_match and expected_match.lower() not in output_str.lower():
            self._record_fail(
                elapsed_ms, 
                f"Expected string '{expected_match}' not found in output. Output: {output_str}",
                f"Function: {func_name} | Input: {safe_payload_str}...",
                title=title
            )
            return

        if custom_validator and not custom_validator(output_str):
            self._record_fail(
                elapsed_ms, 
                f"Custom validation failed for output: {output_str}",
                f"Function: {func_name} | Input: {safe_payload_str}...",
                title=title
            )
            return

        self._record_success(elapsed_ms, output_str)

class WebRunner(BaseRunner):
    def __init__(self, base_url: str, client: Optional[httpx.AsyncClient] = None):
        super().__init__()
        self.base_url = base_url
        self._is_injected_client = client is not None
        self.client = client or httpx.AsyncClient(base_url=base_url, timeout=10.0)
        self.committee_keys = [ed25519.Ed25519PrivateKey.generate() for _ in range(3)]
        self.committee_pubs = [
            k.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            ).hex() for k in self.committee_keys
        ]

    async def teardown(self):
        if not self._is_injected_client and not self.client.is_closed:
            await self.client.aclose()

    async def _run_api_case(self, title: str, method: str, endpoint: str, payload: Dict[str, Any], expected_status: int = 200) -> Optional[httpx.Response]:
        log.info(f"\n[TEST] {title} ({method} {endpoint})")
        start_time = time.time()
        
        try:
            res = await self.client.request(method, endpoint, json=payload)
            elapsed_ms = (time.time() - start_time) * 1000
            
            if res.status_code == expected_status:
                self._record_success(elapsed_ms, res.text)
                return res
            else:
                self._record_fail(
                    elapsed_ms,
                    f"Expected: {expected_status}, Got: {res.status_code}. Response: {res.text[:100]}",
                    f"Endpoint: {endpoint} | Error: {res.text[:200]}",
                    title=title
                )
                return res
        except Exception as e:
            self.fail_count += 1
            log.error(f"  [CRITICAL FAIL] Network/Execution Error: {str(e)}")
            self.failed_cases.append({"title": title, "error": f"[CRITICAL] {str(e)}"})
            return None

    def _sign_payload(self, signers: List[ed25519.Ed25519PrivateKey], payload_dict: Dict[str, Any]) -> List[str]:
        raw_json_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        commit_hash = hashlib.sha256(raw_json_bytes).digest()
        return [k.sign(commit_hash).hex() for k in signers]


# =====================================================================
# 2. Reactive Runtime Runners (Event & Recovery Automation)
# =====================================================================

class RuntimeRunner(ABC):
    def __init__(self, broker: DphiBroker, resolvers: Optional[Dict[str, EffectResolver]] = None):
        self.broker = broker
        self.executor = SandboxExecutor(resolvers=resolvers)
        self.is_running = False

    async def watch_and_react(self, initial_context: TaskContext):
        self.is_running = True
        log.info(f"[RuntimeRunner] Activated pattern for task: {initial_context.task_type}")
        
        async for contract in self.executor.execute_stream(initial_context):
            if not self.is_running:
                break
            await self.on_contract_emitted(contract)

    @abstractmethod
    async def on_contract_emitted(self, contract: Contract):
        pass

    def stop(self):
        self.is_running = False


class RecoveryRunner(RuntimeRunner):
    def __init__(self, broker, resolvers: Optional[Dict[str, EffectResolver]] = None):
        super().__init__(broker, resolvers)
        self.auditor_keys = [ed25519.Ed25519PrivateKey.generate() for _ in range(3)]
        self.auditor_pubs = [
            k.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            ).hex() for k in self.auditor_keys
        ]
        self.store = KernelLedger()

    def _sign_multisig(self, signers: list, commit_dict: dict) -> list:
        canonical_bytes = StateAdapter.to_canonical_bytes(commit_dict)
        return [k.sign(canonical_bytes).hex() for k in signers]

    async def on_contract_emitted(self, contract: Contract):
        if contract.state == CoherenceState.STREAMING:
            log.trace(f"[RecoveryRunner] Node streaming normally. Topos: {contract.topos_id}")
            return
            
        if contract.state == CoherenceState.FRAGMENTED:
            log.warning(f"[RecoveryRunner] Anomaly detected! Phase lost at Topos: {contract.topos_id}")
            await self._trigger_parity_recovery(contract)
            
        elif contract.state == CoherenceState.COHERENT and getattr(contract, "kind", None) == "coherence":
            log.info(f"[RecoveryRunner] Task finalized coherently. Nexus: {contract.nexus_id}")

    async def _trigger_parity_recovery(self, failed_contract: Contract):
        log.info(f"--- [Reaction] Initiating Parity Recovery for Nexus {failed_contract.nexus_id} ---")
        topos_id_low32 = int(failed_contract.topos_id) & 0xFFFFFFFF if failed_contract.topos_id else None
        
        recovery_context = TaskContext(
            task_type=DphiMethod.VERIFY_PARITY,
            payload={
                "topos_id_low32": topos_id_low32,
                "nexus_id": failed_contract.nexus_id
            },
            tier="SYSTEM"
        )
        
        async for recovery_contract in self.executor.execute_stream(recovery_context):
            if recovery_contract.state == CoherenceState.COHERENT:
                recovered_phase = recovery_contract.payload.get("data", {}).get("recovered_missing")
                if recovered_phase:
                    log.info(f"  └─ [SUCCESS] Auditor mathematically recovered Phase ID: {recovered_phase}")
                    await self._seal_recovered_state(topos_id_low32, failed_contract.nexus_id, recovered_phase)
                break
            elif recovery_contract.state == CoherenceState.FRAGMENTED:
                reason = recovery_contract.payload.get("reason", "math_validation_failed")
                log.error(f"  └─ [FATAL] Parity recovery rejected by kernel: {reason}")
                break

    async def _seal_recovered_state(self, topos_id: Optional[int], nexus_id: int, recovered_phase: int):
        log.info("\n--- [Reaction] DAG Rebase & Roll-forward Sealing ---")
        restored_parity = StateAdapter.build_parity_triplet(
            topos_id=str(topos_id), 
            phase_id=recovered_phase, 
            nexus_id=nexus_id
        )
        
        failed_hash = "orphan_hash_45_aborted"
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=restored_parity,
            parent_nexus_id=nexus_id, 
            parent_commit_id=failed_hash, 
            repos={"recovery_status": "fully_healed"},
            cached_states={}
        )
        
        active_keys = self.auditor_keys[:2]
        active_pubs = self.auditor_pubs[:2]
        signatures = self._sign_multisig(active_keys, anchor_commit)
        
        seal_payload = StateAdapter.build_seal_epoch_payload(
            parity=restored_parity,
            parent_nexus_id=nexus_id,
            self_parent_state=failed_hash,
            repos={"recovery_status": "fully_healed"},
            cached_states={},
            timestamp=time.time(),
            signers=active_pubs,
            signatures=signatures,
            threshold=2, 
            allowed_signers=self.auditor_pubs
        )

        seal_context = TaskContext(task_type=DphiMethod.SEAL_EPOCH, payload=seal_payload, tier="SYSTEM")
        
        async for seal_contract in self.executor.execute_stream(seal_context):
            if seal_contract.state == CoherenceState.COHERENT:
                sealed_data = seal_contract.payload.get("data", {})
                kernel_commit = KernelCommit(**sealed_data.get("kernel_commit", {}))
                
                try:
                    commit_hash = self.store.seal_system_epoch(
                        commit=kernel_commit, 
                        signatures=signatures, 
                        threshold=2
                    )
                    self.store.update_head("global_era_anchor", commit_hash)
                    log.info(f"  └─ [PHYSICAL SEAL SUCCESS] Ledger Head updated: {commit_hash[:8]}")
                except Exception as e:
                    log.critical(f"  └─ [FATAL] WASM validated, but physical seal to DB failed: {e}")
                break
            elif seal_contract.state == CoherenceState.FRAGMENTED:
                rejection = seal_contract.payload.get("reason") or seal_contract.payload.get("detail", "unknown anomaly")
                log.error(f"  └─ [FATAL] WASM rejected recovery payload: {rejection}")
                break


class SyzygyResonator(RuntimeRunner):
    def __init__(self, broker, node_identity: str, resolvers: Optional[Dict[str, EffectResolver]] = None):
        super().__init__(broker, resolvers)
        self.node_identity = node_identity

    async def on_contract_emitted(self, contract: Contract):
        if contract.state == CoherenceState.STREAMING:
            return

        if getattr(contract, "kind", None) == "divergence":
            phase_id = getattr(contract, "phase_id", "UNKNOWN")
            log.warning(f"[SyzygyResonator] Topological Drift Detected! Parity Delta != 0 at Phase {phase_id}")
            await self._seal_void_nexus(contract)
            
        elif getattr(contract, "kind", None) == "coherence" and contract.payload.get("task_type") == RecoveryMethod.SEAL_VOID_EPOCH:
            log.info("[SyzygyResonator] Successfully rebased to Dominant Topos. Swarm Expansion Resumed.")

    async def _seal_void_nexus(self, drift_contract: Contract):
        log.info("--- [Reaction] Initiating Retrospective Entanglement & Void Nexus Sealing ---")
        
        payload = drift_contract.payload
        orphan_hash = payload.get("local_orphan_hash", f"orphan_drift_{self.node_identity[:8]}")
        dominant_topos = payload.get("dominant_topos_id", "macro_topos_alpha")
        phase_id = getattr(drift_contract, "phase_id", 12345)
        
        void_parity = StateAdapter.build_parity_triplet(
            topos_id=dominant_topos, 
            phase_id=phase_id, 
            nexus_id=777777  # Void Nexus 고유 ID
        )
        
        sig = f"sig_{self.node_identity}_void_sealed"
        seal_payload = StateAdapter.build_seal_epoch_payload(
            parity=void_parity, 
            parent_nexus_id=1000000, 
            self_parent_state=orphan_hash,
            repos={"void_sealed": True, "reason": "split_brain_convergence"}, 
            cached_states={},
            timestamp=time.time(), 
            signers=[self.node_identity], 
            signatures=[sig], 
            threshold=1, 
            allowed_signers=[self.node_identity]
        )
        
        rebase_context = TaskContext(
            task_type=DphiMethod.SEAL_EPOCH,
            payload=seal_payload,
            tier="SYSTEM"
        )
        
        log.info("  └─ Submitting Void Seal Task to Executor (Tier: SYSTEM)...")
        async for rebase_contract in self.executor.execute_stream(rebase_context):
            if rebase_contract.state == CoherenceState.COHERENT:
                log.info("  ├─ [Void Seal] Divergent history mathematically isolated.")
                break
            elif rebase_contract.state == CoherenceState.FRAGMENTED:
                reason = rebase_contract.payload.get("reason") or rebase_contract.payload.get("detail", "unknown anomaly")
                log.error(f"  ├─ [FATAL] Void Seal Rejected by Kernel: {reason}")
                break