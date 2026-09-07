# xphi.bound.space.sandbox.protocol
from enum import Enum
from dataclasses import dataclass
from typing import List, Optional

from xphi.kernel.wasm.adapter.dta import DtaTransaction, DtaPointer
from xphi.state.ledger.consensus import SealedKernel, ToposBlob, KernelLedger
from xphi.state.ledger.oracle import LedgerOracle
from xphi.kernel.wasm.adapter.dta import DtaAdapter

class TriadAxis(str, Enum):
    INTENT = "Intent"
    SUBSTRATE = "Substrate"
    SETTLEMENT = "Settlement"

@dataclass
class MsgIngressPledge:
    axis: TriadAxis
    actor_address: str
    pledge_tx: DtaTransaction  # The actual L1 transaction minting the initial fuel

@dataclass
class MsgDelegateTrust:
    delegator_address: str
    split_tx: DtaTransaction   # Consumes parent DTA, outputs multiple child DTAs

@dataclass
class MsgWasmExecution:
    worker_address: str
    target_wasm: str
    execution_tx: DtaTransaction    # Consumes worker's fuel, outputs to 0xDEAD

@dataclass
class MsgExecutionReceipt:
    worker_address: str
    execution_tx_hash: str
    sealed_kernel: SealedKernel # Proof of execution provided by ledger.consensus

@dataclass
class MsgSettlementSeal:
    aggregator_address: str
    rollup_blob: ToposBlob      # L1 state transition residue
    consolidated_root: str      # Root of all execution_tx_hashes
    l1_calldata: str            # Final formatted payload for target EVM/L1

class ProtocolValidator:
    def __init__(self, dta_adapter: DtaAdapter, ledger: KernelLedger, oracle: LedgerOracle):
        self.dta = dta_adapter
        self.ledger = ledger
        self.oracle = oracle

    async def apply_ingress(self, msg: MsgIngressPledge) -> str:
        tx_hash = await self.dta.execute_transaction(msg.pledge_tx)
        return tx_hash

    async def apply_delegation(self, msg: MsgDelegateTrust) -> str:
        tx_hash = await self.dta.execute_transaction(msg.split_tx)
        return tx_hash

    async def apply_wasm_execution(self, msg: MsgWasmExecution) -> str:
        tx_hash = await self.dta.execute_transaction(msg.execution_tx)
        return tx_hash

    async def verify_execution_receipt(self, receipt: MsgExecutionReceipt) -> bool:
        oracle_res = await self.oracle.verify_kernel_lineage(receipt.sealed_kernel.stream_id)
        if not oracle_res.get("is_valid", False):
            raise ValueError(f"Protocol Violation: L1 Oracle rejected execution receipt {receipt.execution_tx_hash}")
        return True

    def apply_settlement(self, msg: MsgSettlementSeal) -> str:
        sealed_hash = self.ledger.save_transition(msg.rollup_blob)
        return sealed_hash

class D3Protocol:
    async def publish_pledge(self, msg: MsgIngressPledge) -> str:
        raise NotImplementedError
        
    async def publish_delegation(self, msg: MsgDelegateTrust) -> str:
        raise NotImplementedError
        
    async def request_execution(self, msg: MsgWasmExecution) -> MsgExecutionReceipt:
        raise NotImplementedError
        
    async def publish_settlement(self, msg: MsgSettlementSeal) -> str:
        raise NotImplementedError