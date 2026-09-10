# xphi.bound.adapter.ator
## @lineage: xphi.kernel.adapter.ator
import math
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Optional

class NodeRole(str, Enum):
    NORMAL = "NORMAL"
    REFLECTOR = "REFLECTOR"
    ATTRACTOR = "ATTRACTOR"

class ToposActionType(str, Enum):
    EMIT_PULSE = "EmitPulse"
    EMIT_PROJECTION = "EmitProjection"
    EMIT_COLLAPSE = "EmitCollapse"
    EMIT_INVERSION = "EmitInversion"

@dataclass(slots=True)
class ToposSignal:
    type: str
    delta_time: Optional[float] = None
    amount: Optional[float] = None
    multiplier: Optional[float] = None

@dataclass(slots=True)
class ManifoldState:
    potential: float = 0.0
    threshold: float = 1.0
    reentry_multiplier: float = 1.0
    global_tick: int = 0
    void_gap: List[str] = field(default_factory=list)
    projection_flow: List[str] = field(default_factory=list)
    collapse_field: List[str] = field(default_factory=list)

@dataclass(slots=True)
class NodeState:
    phase: float
    omega: float
    state: str = NodeRole.NORMAL.value
    tension: float = 0.0
    
    velocity: Optional[float] = None
    recovery: Optional[float] = None
    is_spiking: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass(slots=True)
class KernelDelta:
    d_phase: float
    target_tension: float
    velocity: Optional[float] = None
    recovery: Optional[float] = None
    is_spiking: Optional[bool] = None

class AtorAdapter:
    """
    @spec: FFI & Internal Dynamics Adapter.
    @role: Enforces strict schema mapping and finite-float safety for WASM boundary.
    """

    @staticmethod
    def _assert_safe_float(val: Optional[float], name: str) -> Optional[float]:
        """Prevents NaN/Inf pollution from crashing the WASM math engine."""
        if val is None:
            return None
        f_val = float(val)
        if math.isnan(f_val) or math.isinf(f_val):
            raise ValueError(f"[FFI Boundary Error] '{name}' must be a finite float, got {val}")
        return f_val

    # --- [ Inputs to WASM: Signals ] ---
    @staticmethod
    def build_signal_tick(delta_time: float) -> ToposSignal:
        return ToposSignal(type="Tick", delta_time=AtorAdapter._assert_safe_float(delta_time, "delta_time"))

    @staticmethod
    def build_signal_inject_tension(amount: float) -> ToposSignal:
        return ToposSignal(type="InjectTension", amount=AtorAdapter._assert_safe_float(amount, "amount"))

    @staticmethod
    def build_signal_perturb() -> ToposSignal:
        return ToposSignal(type="Perturb")

    @staticmethod
    def build_signal_tune_reentry(multiplier: float) -> ToposSignal:
        return ToposSignal(type="TuneReentry", multiplier=AtorAdapter._assert_safe_float(multiplier, "multiplier"))

    # --- [ Inputs to WASM: Manifold & Payload ] ---
    @staticmethod
    def build_manifold_state(**kwargs) -> ManifoldState:
        if "potential" in kwargs: kwargs["potential"] = AtorAdapter._assert_safe_float(kwargs["potential"], "potential")
        if "threshold" in kwargs: kwargs["threshold"] = AtorAdapter._assert_safe_float(kwargs["threshold"], "threshold")
        if "reentry_multiplier" in kwargs: kwargs["reentry_multiplier"] = AtorAdapter._assert_safe_float(kwargs["reentry_multiplier"], "reentry_multiplier")
        return ManifoldState(**kwargs)

    @staticmethod
    def build_topos_ffi_payload(signal: ToposSignal, state: Optional[ManifoldState] = None) -> Dict[str, Any]:
        signal_dict = {k: v for k, v in asdict(signal).items() if v is not None}
        payload = {"signal": signal_dict}
        if state is not None:
            payload["state"] = asdict(state)
        return payload

    # --- [ Outputs from WASM: Action Parser ] ---
    @staticmethod
    def parse_topos_action(action_dict: Dict[str, Any]) -> Dict[str, Any]:
        action_type = action_dict.get("action")
        if not action_type:
            raise ValueError("[FFI Error] ToposAction missing 'action' tag.")
            
        parsed = {"action": action_type}
        
        # Validates against the strict Enum
        if action_type == ToposActionType.EMIT_PULSE.value:
            parsed["pulse_id"] = action_dict.get("pulse_id", "unknown_pulse")
        elif action_type == ToposActionType.EMIT_PROJECTION.value:
            parsed["vector_id"] = action_dict.get("vector_id")
            parsed["parent_id"] = action_dict.get("parent_id")
        elif action_type == ToposActionType.EMIT_COLLAPSE.value:
            parsed["phi_id"] = action_dict.get("phi_id")
            parsed["parent_id"] = action_dict.get("parent_id")
        elif action_type == ToposActionType.EMIT_INVERSION.value:
            parsed["count"] = int(action_dict.get("count", 0))
            parsed["parent_id"] = action_dict.get("parent_id")
        else:
            raise ValueError(f"[FFI Error] Unknown ToposAction received from WASM: {action_type}")
            
        return parsed

    # --- [ Internal Mechanics: Node State & Kernel Delta Standardization ] ---
    @staticmethod
    def build_node_state(phase: float, omega: float, state: str = NodeRole.NORMAL.value, tension: float = 0.0, **kwargs) -> NodeState:
        velocity = kwargs.pop("velocity", None)
        recovery = kwargs.pop("recovery", None)
        is_spiking = kwargs.pop("is_spiking", False)
        
        # Ensure role validity before creation
        if state not in [role.value for role in NodeRole]:
            raise ValueError(f"[Schema Error] Invalid NodeRole '{state}'")

        return NodeState(
            phase=AtorAdapter._assert_safe_float(phase, "phase"), 
            omega=AtorAdapter._assert_safe_float(omega, "omega"), 
            state=state, 
            tension=AtorAdapter._assert_safe_float(tension, "tension"),
            velocity=AtorAdapter._assert_safe_float(velocity, "velocity"), 
            recovery=AtorAdapter._assert_safe_float(recovery, "recovery"), 
            is_spiking=bool(is_spiking), 
            metadata=kwargs
        )

    @staticmethod
    def build_kernel_delta(d_phase: float, target_tension: float, velocity: Optional[float] = None, recovery: Optional[float] = None, is_spiking: Optional[bool] = None) -> KernelDelta:
        return KernelDelta(
            d_phase=AtorAdapter._assert_safe_float(d_phase, "d_phase"), 
            target_tension=AtorAdapter._assert_safe_float(target_tension, "target_tension"),
            velocity=AtorAdapter._assert_safe_float(velocity, "velocity"), 
            recovery=AtorAdapter._assert_safe_float(recovery, "recovery"), 
            is_spiking=is_spiking
        )

    # =====================================================================
    # 3. Dynamics Orchestration (Math Offloaded to Rust)
    # =====================================================================
    class Dynamics:
        @staticmethod
        def evolve_phase(current_phase: float, d_phase: float) -> float:
            return (current_phase + d_phase) % (2 * math.pi)

        @staticmethod
        def evolve_tension(current_tension: float, target_tension: float, dt: float, accumulate: bool = True) -> float:
            if accumulate:
                return current_tension + (target_tension * dt)
            return target_tension

        @staticmethod
        def apply_reflector(current_phase: float, boost: float) -> float:
            return (current_phase + boost) % (2 * math.pi)

        @staticmethod
        def apply_attractor(current_omega: float, gain: float) -> float:
            return current_omega * gain

        @staticmethod
        def build_dynamics_ffi_payload(states: Dict[str, NodeState], kernel_type: str, params: Dict[str, Any], dt: float) -> Dict[str, Any]:
            """Strictly builds the payload for Rust's process_field_dynamics."""
            return {
                "states": {k: asdict(v) for k, v in states.items()},
                "kernel_type": kernel_type,
                "params": params,
                "dt": AtorAdapter._assert_safe_float(dt, "dt")
            }

        @staticmethod
        def parse_dynamics_result(result_dict: Dict[str, Any]) -> Dict[str, KernelDelta]:
            """Safely parses Rust's returning Delta HashMaps."""
            return {
                node_id: AtorAdapter.build_kernel_delta(**delta_data)
                for node_id, delta_data in result_dict.items()
            }