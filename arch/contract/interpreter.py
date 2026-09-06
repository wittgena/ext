# xphi.arch.contract.interpreter
from __future__ import annotations
import json
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any, Callable, Mapping, Protocol, runtime_checkable, 
    Optional, FrozenSet, Union
)

from xphi.arch.event.psi import PsiCarrier, PhaseField
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.wasm.method import DphiMethod

log = get_emitter("contract.interpreter")

PRIMITIVE_TYPES = (str, int, float, bool, list, dict, tuple, type(None))

"""Base Execution Interpreter Contracts & Errors"""
class SandboxError(RuntimeError):
    pass

class ExecutionError(SandboxError):
    pass

class ProtocolError(SandboxError):
    pass

@dataclass(frozen=True)
class ExecutionResult:
    success: bool
    output: Any | None = None
    error: ExecutionError | None = None
    metrics: Optional[dict] = None

@runtime_checkable
class CodeInterpreter(Protocol):
    def start(self) -> None:
        ...

    def execute(
        self,
        code: str,
        variables: Mapping[str, Any] | None = None,
        callables: Mapping[str, Callable[..., Any]] | None = None,
    ) -> ExecutionResult:
        ...

    def shutdown(self) -> None:
        ...

@runtime_checkable
class BrokerProtocol(Protocol):
    async def invoke(
        self, 
        target_func: Union[str, Enum], 
        payload: str, 
        wasm_path: Optional[str] = None, 
        tier: Optional[str] = None,
        context: Optional[dict] = None,
        timeout: Optional[float] = None
    ) -> ExecutionResult:
        ...

    async def execute(
        self, 
        code: Union[str, dict],  
        variables: Mapping[str, Any] | None = None, 
        tier: Optional[str] = None,
        context: Optional[dict] = None,
        timeout: Optional[float] = None
    ) -> ExecutionResult:
        ...

    async def update_policy(
        self, 
        tier: str, 
        context: Optional[dict] = None
    ) -> bool:
        ...

    async def close(self) -> None:
        ...

"""JSON-RPC 2.0 Message & Error Formatter"""
class JsonRpcErrorCode:
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603

    SYNTAX_ERROR = -32000
    NAME_ERROR = -32001
    TYPE_ERROR = -32002
    VALUE_ERROR = -32003
    ATTRIBUTE_ERROR = -32004
    INDEX_ERROR = -32005
    KEY_ERROR = -32006
    RUNTIME_ERROR = -32007
    UNKNOWN = -32099

    @classmethod
    def from_exception_type(cls, error_type: str) -> int:
        attr_name = error_type.replace("Error", "_ERROR").upper()
        return getattr(cls, attr_name, cls.UNKNOWN)

class JsonRpcMessage:
    VERSION = "2.0"

    @classmethod
    def request(cls, method: str, params: Mapping[str, Any], msg_id: int | str) -> str:
        return json.dumps({"jsonrpc": cls.VERSION, "method": method, "params": params, "id": msg_id})

    @classmethod
    def notification(cls, method: str, params: Mapping[str, Any] | None = None) -> str:
        msg: dict[str, Any] = {"jsonrpc": cls.VERSION, "method": method}
        if params: msg["params"] = params
        return json.dumps(msg)

    @classmethod
    def result(cls, result: Any, msg_id: int | str) -> str:
        return json.dumps({"jsonrpc": cls.VERSION, "result": result, "id": msg_id})

    @classmethod
    def error(cls, code: int, message: str, msg_id: int | str | None, data: Mapping[str, Any] | None = None) -> str:
        err: dict[str, Any] = {"code": code, "message": message}
        if data: err["data"] = data
        return json.dumps({"jsonrpc": cls.VERSION, "error": err, "id": msg_id})

"""Topological Phase Interpreter (Node Interpretation)"""
class PhaseAction(str, Enum):
    SPAWN = "RESONANCE:SPAWN"
    DROP = "INTERFERENCE:DROP"
    FIELD_MISMATCH = "INTERFERENCE:FIELD_MISMATCH"

@dataclass(frozen=True)
class PhaseJudgment:
    psi_symbol: str
    action: PhaseAction
    phase: str
    version: int
    is_resonance: bool

@dataclass(frozen=True)
class AnchoredIR:
    version: int
    recept_boundaries: FrozenSet[str]

class AnchorFlow:
    @staticmethod
    def bootstrap(recepts: Optional[FrozenSet[str]] = None) -> AnchoredIR:
        if not recepts:
            recepts = frozenset({"system:signal", "system:ping"})
        log.trace(f"[bootstrap] Constructing boundary with recepts: {recepts}")
        return AnchoredIR(version=1, recept_boundaries=recepts)

    @staticmethod
    def revise(anchor: AnchoredIR, new_recept: str) -> AnchoredIR:
        log.trace(f"[δ] revise → expanding boundary for {new_recept}")
        new_boundaries = frozenset(anchor.recept_boundaries | {new_recept})
        return AnchoredIR(version=anchor.version + 1, recept_boundaries=new_boundaries)

class NodeInterpreter:
    def __init__(self, broker: BrokerProtocol, anchor: AnchoredIR, field: PhaseField = PhaseField.COHERENT):
        self.broker = broker
        self.anchor = anchor
        self.current_field = field
        self._current_phase = "PHASE_IDLE"

    @property
    def phase(self) -> str:
        return self._current_phase

    async def process(self, carrier: PsiCarrier, context: Optional[dict] = None) -> PhaseJudgment:
        phase_root = {
            "kind": "ANCHOR",
            "name": "anchor_root",
            "content": json.dumps(list(self.anchor.recept_boundaries))
        }
        
        evolution_ctx = {
            "phase_root": phase_root,
            "external_rules": []
        }

        payload = {
            "evolution_ctx": evolution_ctx,
            "intent_action": "EVALUATE_CARRIER", 
            "intent_payload": {
                "tag": carrier.tag,
                "symbol": carrier.symbol,
                "field": self.current_field.value
            }
        }
        result = await self.broker.invoke(
            target_func=DphiMethod.EXECUTE_TRANSITION,
            payload=json.dumps(payload),
            context=context
        )
        
        if not result.success:
            log.error(f"[NodeInterpreter] WASM Transition Failed: {result.error}")
            return PhaseJudgment(
                psi_symbol=carrier.symbol,
                action=PhaseAction.DROP,
                phase="PHASE_ERROR",
                version=self.anchor.version,
                is_resonance=False
            )

        try:
            trans_result = json.loads(result.output)
            is_authorized = trans_result.get("is_authorized", False)
            residues = trans_result.get("all_residues") or []

            if is_authorized:
                action = PhaseAction.SPAWN
                self._current_phase = f"PHASE_ACTIVE::{carrier.symbol}"
            else:
                action = PhaseAction.DROP
                self._current_phase = "PHASE_IDLE"

            for residue in residues:
                if residue.get("kind") == "ERROR":
                    action = PhaseAction.FIELD_MISMATCH
                    self._current_phase = "PHASE_ERROR"

            return PhaseJudgment(
                psi_symbol=carrier.symbol,
                action=action,
                phase=self._current_phase,
                version=self.anchor.version,
                is_resonance=is_authorized
            )
            
        except json.JSONDecodeError as e:
            log.error(f"[NodeInterpreter] Invalid TransitionResult payload: {e}")
            return PhaseJudgment(
                psi_symbol=carrier.symbol,
                action=PhaseAction.DROP,
                phase="PHASE_ERROR",
                version=self.anchor.version,
                is_resonance=False
            )