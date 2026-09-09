# xphi.kernel.space.bind.resolver
import os
import json
import re
import argparse
import logging
from pathlib import Path
from functools import lru_cache

log = logging.getLogger(__name__)

def determine_anchor_name() -> str:
    """@resolve: Contextual anchor boundary ('.anchor' for USER, 'anchor' for DEV)."""
    if any(part in ["site-packages", "dist-packages"] for part in Path(__file__).parts):
        return ".anchor"
    return "anchor"

def get_anchor_dir(root: Path) -> Path:
    """@resolve: Active anchor node path."""
    if (root / ".anchor").is_dir():
        return root / ".anchor"
    if (root / "anchor").is_dir():
        return root / "anchor"
    return root / determine_anchor_name()

@lru_cache(maxsize=1)
def find_current_self(start: Path | None = None) -> Path:
    """
    @flow: Cascade upward to identify the topological root (self).
    @fallback: Returns start path (cwd) if unbound.
    """
    if start is None:
        start = Path.cwd()

    start = start.resolve()
    
    # 1. DEV 모드 탐색 (상위로 올라가며 anchor 찾기)
    for parent in [start] + list(start.parents):
        if (parent / "anchor").is_dir() or (parent / ".anchor").is_dir():
            return parent

    # 2. USER 모드 판별: site-packages 내에서 실행 중이라면
    # 파편화를 막기 위해 무조건 홈 디렉터리를 기준점(self)으로 반환
    if any(part in ["site-packages", "dist-packages"] for part in Path(__file__).parts):
        return Path.home()

    return start

@lru_cache(maxsize=1)
def _get_around_context() -> tuple[str, dict]:
    """@delegate: SSOT constants resolution via 'around' module."""
    try:
        import bind.around as around_mod
    except ImportError:
        try:
            import xphi.kernel.space.bind.around as around_mod
        except ImportError as e:
            raise RuntimeError(f"Critical dependency missing: 'around' module not found. ({e})")
    
    bound_name = getattr(around_mod, 'BOUND', 'bound.json')
    skeleton = getattr(around_mod, 'DEFAULT_BOUND_SKELETON', {})
    return bound_name, skeleton

@lru_cache(maxsize=1)
def resolve_identity(start: Path | None = None) -> tuple[int, int]:
    """@extract: Topological identifiers (Manifold, Vertex) from bound state."""
    self_root = find_current_self(start)
    bound = load_bound(self_root)
    identity = bound.get("identity", {})
    manifold_id = identity.get("manifold_id", 1) & 0x1F
    vertex_id = identity.get("vertex_id", 1) & 0x1F
    return manifold_id, vertex_id

def get_invoker(path: Path):
    """@extract: Invoker FQN from path relative to topological root."""
    try:
        self_root = find_current_self()
        rel = path.relative_to(self_root).with_suffix("")
        parts = rel.parts[1:] if rel.parts and rel.parts[0] == self_root.name else rel.parts
        invoker = ".".join(parts)
        command = ".".join(parts[-2:])
    except Exception as e: 
        log.info(f"## get_invoker fail: {e}")
        return "", ""
    return invoker, command

@lru_cache(maxsize=1)
def load_bound(self_root: Path) -> dict:
    """
    @flow: Load topology state (bound.json).
    @fallback: Bootstraps default skeleton if boundary is uninitialized.
    """
    bound_name, default_skeleton = _get_around_context()
    anchor_dir = get_anchor_dir(self_root)
    bound_path = anchor_dir / bound_name 
    
    if not bound_path.exists():
        anchor_dir.mkdir(parents=True, exist_ok=True)
        try:
            bound_path.write_text(json.dumps(default_skeleton, indent=2), encoding="utf-8")
        except Exception:
            pass
        return default_skeleton.copy()

    try:
        data = json.loads(bound_path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise RuntimeError(f"{bound_name} must be a JSON object")
        return data
    except Exception as e:
        raise RuntimeError(f"Invalid bound in {bound_path}: {e}")

def _clean_subpath(root_name: str, sub_path_str: str) -> str:
    """@normalize: Prevent root directory duplication in subpaths."""
    pure_path = Path(sub_path_str)
    if pure_path.parts and pure_path.parts[0] == root_name:
        return os.path.join(*pure_path.parts[1:]) if len(pure_path.parts) > 1 else "."
    return sub_path_str

def _track_io_usage(name: str, target_path: Path):
    """@hook: Telemetry for IO boundary access."""
    try:
        from xphi.arch.contract.registry.path import path_registry
        path_registry.log_access(name, target_path)
    except ImportError:
        pass

@lru_cache(maxsize=32)
def resolve_path(name: str, start: Path | None = None) -> Path:
    """@flow: Resolve logical name -> physical topological path."""
    ## @phase: Context alignment
    effective_start = start.resolve() if start else Path.cwd().resolve()
    self_root = find_current_self(effective_start)
    anchor_dir = get_anchor_dir(self_root)
    
    clean_name = _clean_subpath(self_root.name, name)
    direct_candidate = (self_root / clean_name).resolve()
    if direct_candidate.exists() and direct_candidate.is_dir():
        return direct_candidate

    ## @phase: Path mapping & Substitution
    bound = load_bound(self_root)
    paths = bound.get("paths", {})
    
    # around 토폴로지(site-packages 절대 경로 맵) 로드
    around_topology = bound.get("around", {})
    
    if name in paths:
        raw_mapped = paths[name]
        substitutions = {"self": self_root, "anchor": anchor_dir}
        
        ## @merge: Substitution syntax resolution
        for prefix_key, rel_path in bound.get("substitution", {}).items():
            safe_rel_path = rel_path.lstrip("/") if isinstance(rel_path, str) else str(rel_path)
            base_module = safe_rel_path.split("/")[0] # 예: "xphi", "anchor"
            
            # 1. USER 모드 코어 패키지 (fiber, xphi 등 - site-packages 절대 경로 매핑)
            if base_module in around_topology:
                base_abs_path = Path(around_topology[base_module]["path"])
                sub_path_parts = safe_rel_path.split("/")[1:]
                sub_path = "/".join(sub_path_parts) if sub_path_parts else ""
                substitutions[prefix_key] = (base_abs_path / sub_path).resolve()
            
            # 2. 앵커 데이터 경로 (USER 모드의 .anchor 이름 변경 규칙 적용)
            elif base_module == "anchor":
                sub_path_parts = safe_rel_path.split("/")[1:]
                sub_path = "/".join(sub_path_parts) if sub_path_parts else ""
                # self_root / "anchor" 가 아닌 계산된 anchor_dir 사용
                substitutions[prefix_key] = (anchor_dir / sub_path).resolve()
                
            # 3. 기타 런타임 데이터 및 DEV 모드 (self_root 기준 상대 매핑)
            else:
                substitutions[prefix_key] = (self_root / safe_rel_path).resolve()
        
        ## @match: Pattern :prefix:/sub_path
        match = re.match(r"^:([^:]+):(?:/(.*))?$", raw_mapped)
        if match:
            prefix = match.group(1)
            sub_path = match.group(2) or ""
            if prefix in substitutions:
                target_path = (substitutions[prefix] / sub_path).resolve()
            else:
                raise RuntimeError(f"Unknown substitution prefix ':{prefix}:' in paths mapping '{name}: {raw_mapped}'")
        else:
            mapped_subpath = _clean_subpath(self_root.name, raw_mapped)
            target_path = (self_root / mapped_subpath).resolve()
    else:
        ## @fallback: Isolate unbound paths into anchor namespace
        target_path = (anchor_dir / clean_name).resolve()

    # 안전장치: site-packages(소스 코드) 내부 경로를 반환할 때는 mkdir을 강제로 실행하지 않음
    if not any(part in ["site-packages", "dist-packages"] for part in target_path.parts):
        target_path.mkdir(parents=True, exist_ok=True)
        
    _track_io_usage(name, target_path)
    return target_path

def resolve_channel(name: str, start: Path | None = None) -> str:
    """
    @flow: Resolve logical channel -> physical redis channel.
    @rule: Anchor override -> Namespace validation.
    """
    self_root = find_current_self(start)
    bound = load_bound(self_root)

    channels = bound.get("channels", {})
    anchor = channels.get("anchor", {})
    namespaces = channels.get("namespaces", [])
    
    if not namespaces:
        bound_name, _ = _get_around_context()
        raise RuntimeError(f"channels.namespaces not defined in {bound_name}")

    if name in anchor:
        return anchor[name]

    if ":" not in name:
        raise RuntimeError(f"Invalid channel '{name}'. Format: '<namespace>:<name>'")

    prefix = name.split(":")[0]
    if prefix not in namespaces:
        raise RuntimeError(f"Channel namespace '{prefix}' not allowed. Allowed: {', '.join(namespaces)}")
    
    return name

def resolve_pattern(start: Path | None = None) -> str:
    """@resolve: Global subscription pattern (xphi)."""
    self_root = find_current_self(start)
    bound = load_bound(self_root)
    channels = bound.get("channels", {})
    xphi = channels.get("xphi", {})
    pattern = xphi.get("pattern")
    
    if not pattern:
        bound_name, _ = _get_around_context()
        raise RuntimeError(f"xphi pattern not defined in {bound_name}")

    return pattern

def run_around():
    """@delegate: Trigger topology generation (Φ → ∂Φ) via SSOT module."""
    self_root = find_current_self()
    anchor_dir = get_anchor_dir(self_root)
    
    try:
        from bind.around import discover_repos, update_bound_config
    except ImportError:
        try:
            from anchor.bind.around import discover_repos, update_bound_config
        except ImportError as e:
            log.info(f"[Error] Failed to delegate: Could not import 'around' module. ({e})")
            return

    log.info("[Phase: Delegate] Delegating topology update to 'around' module...")
    repos = discover_repos(self_root, max_depth=2)
    update_bound_config(anchor_dir, repos)

def ensure_bound_initialized():
    """
    @flow: Lazy-initialization of topology state.
    @route: Forks mapping logic based on DEV/USER mode detection.
    """
    self_root = find_current_self()
    anchor_dir = get_anchor_dir(self_root)
    bound_path = anchor_dir / "bound.json"
    
    if not bound_path.exists():
        log.info(f"[System Bootstrap] Initializing topology state in {anchor_dir}...")
        
        try:
            from anchor.bind.around import (
                determine_execution_mode,
                resolve_workspace_root,
                project_dev_mode,
                project_user_mode
            )
            
            mode = determine_execution_mode()
            
            if mode == "DEV":
                log.info("[System Bootstrap] DEV Mode detected. Running local workspace projection...")
                workspace_root = resolve_workspace_root()
                project_dev_mode(workspace_root, anchor_dir)
            else:
                log.info("[System Bootstrap] USER Mode detected. Running installed package projection...")
                project_user_mode(anchor_dir)
                
        except ImportError as e:
            log.warning(f"[System Bootstrap] Failed to auto-initialize topology: {e}")
        except Exception as e:
            log.error(f"[System Bootstrap] Unexpected error during initialization: {e}")

"""@trigger: Auto-binding hook on module load"""
# ensure_bound_initialized()

def run_test():
    """@test: Verify topology resolution mechanics."""
    try:
        log.info("## bound RESOLVER TEST")
        self_root = find_current_self(Path("."))
        log.info(f"[SELF ROOT] {self_root}")
        log.info(f"[ANCHOR] {get_anchor_dir(self_root)}")

        bound = load_bound(self_root)
        if bound:
            log.info("\n[BOUND FOUND / GENERATED]")
            log.info(json.dumps(bound, indent=2))

        log.info("\n## @path.resolution.test")
        paths = bound.get("paths", {})
        for name in paths.keys():
            try:
                resolved = resolve_path(name)
                log.info(f" ✔ {name} → {resolved}")
            except Exception as e:
                log.info(f" ✘ {name} → ERROR: {e}")

        log.info("\n## @channel.resolution.test")
        channels = bound.get("channels", {})
        anchor = channels.get("anchor", {})

        tested = set(anchor.keys())
        for name in tested:
            try:
                resolved = resolve_channel(name)
                log.info(f" ✔ {name} -> {resolved}")
            except Exception as e:
                log.info(f" ✘ {name} -> ERROR: {e}")

        namespace_tests = [
            "psi:test_signal", "delta:new_branch", "execution:done",
            "xor:score", "loop:trigger"
        ]

        for name in namespace_tests:
            try:
                resolved = resolve_channel(name)
                log.info(f" ✔ {name} → {resolved}")
            except Exception as e:
                log.info(f" ✘ {name} → ERROR: {e}")

        invalid_tests = ["unknown:test", "badformat", "psi"]
        log.info("\n## @invalid.channel.test")
        for name in invalid_tests:
            try:
                resolved = resolve_channel(name)
                log.info(f" ✘ {name} → SHOULD FAIL but returned {resolved}")
            except Exception as e:
                log.info(f" ✔ {name} → correctly rejected ({e})")

        log.info("\n## @xphi.pattern")
        pattern = resolve_pattern()
        log.info(f"xphi pattern -> {pattern}")

        test_channels = [
            "psi:intensity", "delta:generated", "execution:completed",
            "xor:similarity_score", "loop:stabilized", "xphi:heartbeat"
        ]
        compiled = re.compile(pattern)
        for ch in test_channels:
            match = bool(compiled.match(ch))
            log.info(f"   {ch} → {'MATCH' if match else 'NO MATCH'}")

        log.info("\n## TEST COMPLETE")
    except Exception as e:
        log.info(f"[FATAL ERROR] {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Anchor Resolver CLI")
    parser.add_argument("--around", action="store_true", help="Run around script to update repository bounds")
    parser.add_argument("--test", action="store_true", help="Run manual verification tests")
    
    args = parser.parse_args()
    if args.around:
        run_around()
    elif args.test:
        run_test()
    else:
        parser.print_help()