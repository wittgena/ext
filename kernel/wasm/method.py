# xphi.kernel.wasm.method
## @lineage: xphi.kernel.dphi.method
from enum import Enum

class DphiMethod(str, Enum):
    EXECUTE_CODE = "execute_code"
    EXECUTE_DVM = "execute_dvm"
    COMPUTE_ROOT_FINGERPRINT = "compute_root_fingerprint"
    EVALUATE_TENSION = "evaluate_tension"
    GENERATE_PROOF = "generate_proof"
    GENERATE_TOPOS_ID = "generate_topos_id"
    GENERATE_PHASE_ID = "generate_phase_id"
    INIT_EPOCH = "init_epoch"
    PROCESS_EVOLUTION = "process_evolution"
    PROCESS_TOPOS_TICK = "process_topos_tick"
    INSCRIBE_ACTOR = "inscribe_actor"
    SEAL_EPOCH = "seal_epoch"
    VERIFY_BUILD_LINEAGE = "verify_build_lineage"
    VERIFY_PARITY = "verify_parity"
    EXECUTE_TRANSITION = "execute_transition"
    CONFIGURE_TOPOLOGY = "configure_topology"
    PROCESS_FIELD_DYNAMICS = "process_field_dynamics"