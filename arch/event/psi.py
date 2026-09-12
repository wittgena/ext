# xphi.arch.event.psi
from __future__ import annotations

import time
import json
from enum import Enum
from typing import Dict, Tuple, FrozenSet, Optional, Any, Union, Generic, TypeVar, List
from dataclasses import dataclass, field, asdict
from abc import ABC, abstractmethod

from xphi.arch.event.next import LogEvent
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("event.psi")

# =====================================================================
# 1. Phase Space Enums
# =====================================================================

class CarrierType(str, Enum):
    """resonance (Ψ behavior)"""
    RECURSIVE = "recursive"      # self-referential resonance
    MODULATORY = "modulatory"    # indirect / delayed resonance
    FIXED = "fixed"              # constrained / invariant resonance
    DIFFUSE = "diffuse"          # distributed resonance

class PhaseField(str, Enum):
    """Φ Field Types"""
    LOCAL = "local"
    COHERENT = "coherent"
    INTERFERENCE = "interference"
    EVALUATION = "evaluation"


# =====================================================================
# 2. Psi Data Structures (Carrier & Event)
# =====================================================================

@dataclass(frozen=True)
class PsiCarrier:
    """The fundamental transport layer carrying the payload."""
    kind: str
    tag: str
    payload: Any
    carrier_type: Optional[CarrierType] = CarrierType.FIXED
    target_field: Optional[PhaseField] = PhaseField.LOCAL

    @property
    def symbol(self) -> str:
        """boundary routing symbol (∂Φ input)"""
        return f"{self.kind}:{self.tag}"

@dataclass
class PsiEvent:
    """Minimal unit of phase resonance in the event loop."""
    event_id: str  
    parent_id: Optional[str]
    source_id: str
    scope: str
    tick: int
    
    carrier: PsiCarrier
    phase_id: int = 0
    context: Dict[str, Any] = field(default_factory=dict)

    @property
    def payload(self) -> str:
        """Compatibility property for legacy systems."""
        return self.carrier.payload

    @property
    def event_type(self) -> str:
        """Compatibility property for legacy systems."""
        return self.carrier.kind
        
    @property
    def symbol(self) -> str:
        """Routing symbol delegation."""
        return self.carrier.symbol

    @property
    def tag(self) -> str:
        """Tag delegation."""
        return self.carrier.tag

    @property
    def kind(self) -> str:
        """Kind delegation."""
        return self.carrier.kind
    
    def to_json(self) -> str:
        """Serializes object to JSON string (For Surface Projection)."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str: str) -> 'PsiEvent':
        """Deserializes JSON string back to object (For Capture)."""
        data = json.loads(json_str)
        # Reconstruct the nested PsiCarrier object
        if 'carrier' in data and isinstance(data['carrier'], dict):
            # Enums reconstruction if needed
            c_data = data['carrier']
            if 'carrier_type' in c_data and c_data['carrier_type']:
                c_data['carrier_type'] = CarrierType(c_data['carrier_type'])
            if 'target_field' in c_data and c_data['target_field']:
                c_data['target_field'] = PhaseField(c_data['target_field'])
            data['carrier'] = PsiCarrier(**c_data)
            
        return cls(**data)


PsiType = Union[PsiCarrier, PsiEvent]


# =====================================================================
# 3. Network Abstract Patterns (Merged from event.network)
# =====================================================================

Psi_T = TypeVar('Psi_T')        ## psi: dynamic flow (task / message)
Phi_T = TypeVar('Phi_T')        ## phi: shared topology (state / memory)
R_T = TypeVar('R_T')            ## Representation
EventId_T = TypeVar('EventId_T')## psi identifier

class EventDisperser(ABC, Generic[Psi_T, R_T, Phi_T]):
    """@flow: ψ -> {ψ₁..ψₙ} -> {R₁..Rₙ} -> Φ_merged"""

    @abstractmethod
    def scatter(self, task: Psi_T) -> List[Psi_T]:
        """Split a task into parallel subflows."""
        pass

    @abstractmethod
    def gather(self, representations: List[R_T]) -> Phi_T:
        """Merge parallel representations into a unified state."""
        pass

class EventRouter(ABC, Generic[Psi_T, Phi_T, EventId_T]):
    """@flow: ψ_in → Φ_rules → ψ_out_k"""

    def __init__(self, rules_topology: Phi_T):
        self.rules = rules_topology  # routing decision topology

    @abstractmethod
    def dispatch(self, task: Psi_T) -> EventId_T:
        """Select the target ator based on routing rules."""
        pass

class EventAligner(ABC, Generic[Psi_T, Phi_T]):
    """@flow: ψ → ∂Φ → Φ′ → Φ"""

    @abstractmethod
    def detect_drift(self, event: Psi_T, current_state: Phi_T) -> Phi_T:
        """Detect state drift caused by an event."""
        pass

    @abstractmethod
    def reconcile(self, drift_analysis: Phi_T, current_state: Phi_T) -> Phi_T:
        """Reconcile drift and synchronize shared state."""
        pass

class EventResonator(ABC, Generic[Psi_T]):
    """@flow: ψ → interference → resonance → ψ_amplified"""

    @abstractmethod
    def interfere(self, flow_a: Psi_T, flow_b: Psi_T) -> Psi_T:
        """Create interference between two flows (e.g. generator vs critic)."""
        pass

    @abstractmethod
    def amplify(self, interference_result: Psi_T, iterations: int) -> Psi_T:
        """Iteratively refine and amplify the result."""
        pass

class EventTransductor(ABC, Generic[Psi_T]):
    """@flow: ψ₁ → transductor → ψ₂"""

    @abstractmethod
    def transduce(self, source_flow: Psi_T, target_format_rule: Any) -> Psi_T:
        """Convert flow representation for another ator (e.g. NL → JSON)."""
        pass

class MultiEventFlow(ABC, Generic[Psi_T, Phi_T]):
    """Hybrid control loop combining the five network primitives."""

    def __init__(self):
        self.shared_state: Optional[Phi_T] = None

    @abstractmethod
    def execute(self, initial_flow: Psi_T) -> Phi_T:
        """
        @flow: ψ → dispersion → routing → transduction → alignment → resonance → Φ
        - Execute the full multi-ator flow cycle.
        """
        pass