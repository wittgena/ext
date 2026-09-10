# xphi.bound.adapter.state
## @lineage: xphi.kernel.adapter.state
import canonicaljson

class StateAdapter:
    @staticmethod
    def to_canonical_bytes(data: dict) -> bytes:
        """Enforce RFC 8785 Canonical JSON (JCS) for deterministic hashing."""
        return canonicaljson.encode_canonical_json(data)

    @staticmethod
    def _assert_uint32(val: int, name: str):
        """Rust u32(0 ~ 4,294,967,295) 경계값 검증"""
        if val is not None and not (0 <= val <= 4294967295):
            raise ValueError(f"[FFI Type Error] '{name}' must be a uint32, got {val}")

    @staticmethod
    def build_configure_topology_payload(manifold_id: int, vertex_id: int) -> dict:
        """@target: Rust `ConfigureTopologyPayload`"""
        StateAdapter._assert_uint32(manifold_id, "manifold_id")
        StateAdapter._assert_uint32(vertex_id, "vertex_id")
        return {
            "manifold_id": manifold_id,
            "vertex_id": vertex_id
        }

    """Parity & Context Builders"""
    @staticmethod
    def build_topos_context(timestamp: int, injected_anchor: int = None, injected_tick: int = None) -> dict:
        """@target: Rust `ToposContext` (Influx Context)"""
        StateAdapter._assert_uint32(injected_tick, "injected_tick")
        return {
            "timestamp": timestamp,
            "injected_anchor": injected_anchor,
            "injected_tick": injected_tick
        }

    @staticmethod
    def build_parity_triplet(topos_id: str, phase_id: int, nexus_id: int) -> dict:
        """@target: Rust `ParityTriplet`"""
        StateAdapter._assert_uint32(phase_id, "phase_id")
        StateAdapter._assert_uint32(nexus_id, "nexus_id")
        return {
            "topos_id": topos_id,
            "phase_id": phase_id,
            "nexus_id": nexus_id
        }

    @staticmethod
    def build_parity_request(topos_id_low32: int = None, phase_id: int = None, nexus_id: int = None) -> dict:
        """@target: Rust `ParityRequest` struct"""
        if topos_id_low32 is not None: StateAdapter._assert_uint32(topos_id_low32, "topos_id_low32")
        if phase_id is not None: StateAdapter._assert_uint32(phase_id, "phase_id")
        if nexus_id is not None: StateAdapter._assert_uint32(nexus_id, "nexus_id")
        return {
            "topos_id_low32": topos_id_low32,
            "phase_id": phase_id,
            "nexus_id": nexus_id
        }

    """Ledger Struct Builders For Canonical Hashing"""
    @staticmethod
    def build_repo_commit(nexus_id: int, parent_nexus_id: int, parent_commit_id: str) -> dict:
        """@target: Rust `RepoCommit` struct"""
        StateAdapter._assert_uint32(nexus_id, "nexus_id")
        StateAdapter._assert_uint32(parent_nexus_id, "parent_nexus_id")
        return {
            "nexus_id": nexus_id,
            "parent_nexus_id": parent_nexus_id or 0,
            "parent_commit_id": parent_commit_id
        }

    @staticmethod
    def build_anchor_commit(parity: dict, parent_nexus_id: int, parent_commit_id: str, repos: dict, cached_states: dict = None) -> dict:
        """@target: Rust `AnchorCommit` struct"""
        StateAdapter._assert_uint32(parent_nexus_id, "parent_nexus_id")
        
        safe_repos = {str(k): str(v) for k, v in (repos or {}).items()}
        safe_cached_states = {str(k): str(v) for k, v in (cached_states or {}).items()}

        return {
            "parity": parity,
            "parent_nexus_id": parent_nexus_id or 0,
            "parent_commit_id": parent_commit_id,
            "repos": safe_repos,
            "cached_states": safe_cached_states
        }

    """FFI Entrypoint Payloads (Multi-Sig & Dynamic ACL)"""
    @staticmethod
    def build_inscribe_payload(
        nexus_id: int, 
        parent_nexus_id: int, 
        parent_commit_id: str, 
        signers: list, 
        signatures: list, 
        threshold: int, 
        allowed_signers: list = None
    ) -> dict:
        """@target: Rust `InscribePayload` (Membrane Entrypoint)"""
        StateAdapter._assert_uint32(nexus_id, "nexus_id")
        StateAdapter._assert_uint32(parent_nexus_id, "parent_nexus_id")
        StateAdapter._assert_uint32(threshold, "threshold")
        
        return {
            "nexus_id": nexus_id,
            "parent_nexus_id": parent_nexus_id,
            "parent_commit_id": parent_commit_id,
            "signers": signers,
            "signatures": signatures,
            "threshold": threshold,
            "allowed_signers": allowed_signers
        }

    @staticmethod
    def build_seal_epoch_payload(
        parity: dict, 
        parent_nexus_id: int, 
        self_parent_state: str, 
        repos: dict, 
        cached_states: dict, 
        timestamp: float, 
        signers: list, 
        signatures: list, 
        threshold: int, 
        allowed_signers: list = None
    ) -> dict:
        """@target: Rust `SealEpochPayload` (Membrane Entrypoint)"""
        StateAdapter._assert_uint32(parent_nexus_id, "parent_nexus_id")
        StateAdapter._assert_uint32(threshold, "threshold")
        
        safe_repos = {str(k): str(v) for k, v in (repos or {}).items()}
        safe_cached_states = {str(k): str(v) for k, v in (cached_states or {}).items()}

        return {
            "parity": parity,
            "parent_nexus_id": parent_nexus_id,
            "self_parent_state": self_parent_state,
            "repos": safe_repos,
            "cached_states": safe_cached_states,
            "timestamp": timestamp,
            "signers": signers,
            "signatures": signatures,
            "threshold": threshold,
            "allowed_signers": allowed_signers
        }

    @staticmethod
    def build_trans_rule(src: str, dest: str, kind: str) -> dict:
        """
        @target: Rust `TransRule` struct
        @constraint: `kind` MUST strictly be one of ["CORE", "SYMLINK", "ANCHOR"]
        """
        valid_kinds = {"CORE", "SYMLINK", "ANCHOR"}
        if kind not in valid_kinds:
            raise ValueError(f"[Schema Error] Invalid NodeType '{kind}'. Must be one of {valid_kinds}")
            
        return {
            "src": src,
            "dest": dest,
            "kind": kind
        }

    @staticmethod
    def build_evolution_context(phase_root: dict, external_rules: list = None) -> dict:
        """
        @target: Rust `EvolutionContext` struct
        @note: Rust의 StateNode는 `name`과 `kind`를 필수로 요구하므로, 
               빈 딕셔너리({})나 불완전한 형태가 들어올 경우 기본 ROOT 노드로 안전하게 래핑(Wrapping)합니다.
        """
        if not phase_root or "name" not in phase_root or "kind" not in phase_root:
            # 안전한 FFI 통신을 위해 유효한 StateNode 규격으로 자동 포장
            phase_root = StateAdapter.build_core_node(
                name="root", 
                content="default_empty_state",
                children=phase_root if isinstance(phase_root, dict) and phase_root else {}
            )

        return {
            "phase_root": phase_root,
            "external_rules": external_rules or []
        }

    @staticmethod
    def build_transition_payload(intent_action: str, intent_payload: dict, evolution_ctx: dict) -> dict:
        """@target: Rust `TransitionPayload` struct (Top-level FFI)"""
        return {
            "intent_action": intent_action,
            "intent_payload": intent_payload,
            "evolution_ctx": evolution_ctx
        }

    @staticmethod
    def adapt_swarm_to_phase_root(commit_hash: str, agents_dict: dict) -> dict:
        children = {
            agent_name: StateAdapter.build_core_node(agent_name, str(agent_hash))
            for agent_name, agent_hash in agents_dict.items()
        }
        return StateAdapter.build_core_node("swarm_root", commit_hash, children)

    @staticmethod
    def adapt_ecosystem_to_phase_root(epoch_state: str, proposal_ipfs_hash: str) -> dict:
        children = {
            "pending_proposal": StateAdapter.build_symlink_node("pending_proposal", proposal_ipfs_hash)
        }
        return StateAdapter.build_core_node("ecosystem_root", epoch_state, children)

    @staticmethod
    def adapt_provenance_to_phase_root(commit_hash: str, repos_dict: dict) -> dict:
        children = {
            repo_name: StateAdapter.build_core_node(repo_name, str(repo_hash))
            for repo_name, repo_hash in repos_dict.items()
        }
        return StateAdapter.build_core_node("provenance_root", commit_hash, children)
    
    """DAG Node Builders"""
    @staticmethod
    def build_core_node(name: str, content: str, children: dict = None) -> dict:
        """@target: Rust `StateNode` (CORE)"""
        return {
            "name": name, 
            "kind": "CORE", 
            "content": content, 
            "ref_target": None, 
            "children": children or {}
        }

    @staticmethod
    def build_symlink_node(name: str, ref_target: str, children: dict = None) -> dict:
        """@target: Rust `StateNode` (SYMLINK)"""
        return {
            "name": name, 
            "kind": "SYMLINK", 
            "content": None, 
            "ref_target": ref_target, 
            "children": children or {}
        }