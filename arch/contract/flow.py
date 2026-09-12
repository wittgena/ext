# xphi.arch.contract.flow
## @lineage: xphi.arch.contract.phase.flow
import uuid
import asyncio
import enum
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Tuple, Callable, Union, Type, TypeVar
from dataclasses import dataclass

from xphi.watcher.plane.emitter import get_logger
from xphi.arch.contract.registry.unified import contract, registry

log = get_logger('phase.flow')

PhaseElement = Union[str, Type[Any]]

@dataclass(frozen=True)
class Proto:
    sequence: Tuple[PhaseElement, ...]
    kind: str = "phase"

def proto(p: Proto) -> Callable:
    """
    @invariant:
    - execution != proto
    """
    def wrap(obj: Any) -> Any:
        setattr(obj, "__proto__", p)
        return obj
    return wrap

@proto(Proto(
    sequence=("Φ", "kernel", "ΔΦ", "Φ"),
    kind="evolution"
))
def evolve(self, dt: float):
    pass

def get_proto(obj: Any) -> Proto:
    return getattr(obj, "__proto__", None)

T = TypeVar('T')

def extend_proto(p: Proto, *seq: str, kind: str = None) -> Proto:
    """기존 Proto를 변형하지 않고 확장된 새로운 Proto를 반환 (순수 함수)"""
    return Proto(
        sequence=p.sequence + seq,
        kind=kind or p.kind
    )

BASE_LOOP = Proto(("Ψ", "Φ′", "Φ", "Ψ′"), "loop")

@proto(BASE_LOOP)
async def interpret(self):
    pass


# ==========================================
# Core Flow & State Models
# ==========================================

class PhaseFlow:
    def __init__(self, payload=None, id=None, aspect=None, root=None):
        self.payload = payload
        self.id = id or str(uuid.uuid4())
        self.aspect = aspect or "default"
        self.root = root or self.id

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())[:6]


class FlowState:
    """coupling of ψ and Φ during runtime traversal"""
    def __init__(self, flow: PhaseFlow, state: Dict[str, Any]):
        self.flow = flow
        self.state = state


# ==========================================
# Phase Operators
# ==========================================

class Dispersion:
    """
    @flow: ψ → {ψ₁..ψₙ}
    @phase: dispersion / fan-out
    """
    def scatter(self, flow: PhaseFlow, aspects: List[str]) -> List[PhaseFlow]:
        log.info(f"  [Dispersion] '{flow.id}'를 {aspects}로 분화합니다.")
        return [PhaseFlow(payload=flow.payload, aspect=a, id=f"{flow.id}_{a}") for a in aspects]


class Judgment:
    """@flow: ψ → judgment → ψ_k"""
    def judge(self, flow: PhaseFlow, rules: List[Dict]) -> str:
        for rule in rules:
            cond = rule["if"]
            if "aspect" in cond and cond["aspect"] == flow.aspect:
                return rule["next"]
            if "contains" in cond and cond["contains"] in str(flow.payload):
                return rule["next"]
        return rules[-1]["next"]


class Transduction:
    """
    @flow: ψ_open → (project) → (close ⊕ kernel) → ψ_closed
    @desc: 상위 레벨에서의 폐합(Closure) 연산자
    """
    def transduce(self, flow: PhaseFlow, ator_node: Any) -> PhaseFlow:
        log.debug(f"## Current Instance Type: {type(self)}")
        projected_payload = self._project(flow, ator_node)
        return self._close(projected_payload, flow, ator_node)

    def _project(self, flow: PhaseFlow, ator_node: Any) -> dict:
        """기본형은 변화 없이(Identity) 페이로드를 반환"""
        return flow.payload

    def _close(self, projected, flow, ator_node):
        transformed = self._execute_transformation(
            projected,
            ator_node.node_context.get("instruction", "")
        )
        log.info(f"  [Closure] Binding kernel at the moment of closing ψ:{flow.id}")
        return PhaseFlow(
            payload=transformed,
            id=flow.id,
            aspect=f"transduced_{ator_node.role}",
            root=flow.root
        )

    def _execute_transformation(self, data, instruction):
        """커널 로직: 입력과 지침을 결합하여 실질적인 변화를 생성"""
        return f"Result({data} ⊕ {instruction})"


class Align:
    """
    @flow: ψ → ∂Φ → Φ'
    @phase: state alignment
    """
    def align(self, flow: PhaseFlow, state: Dict[str, Any]):
        log.info(f"  [Alignment] '{flow.aspect}' 결과를 상태 공간에 동기화합니다.")
        state[flow.id] = flow.payload
        return state


class Resonance:
    """
    @flow: ψ₁ ⊕ ψ₂ → ψ*
    @phase: resonance / interference
    """
    def interfere(self, a, b):
        return f"[Resonance] {a} ⊕ {b}"


class Gather:
    """
    @flow: {ψ₁..ψₙ} → gather → ψ_merged
    @phase: synchronization / fan-in
    """
    def merge(self, flows: List[PhaseFlow], root: str) -> PhaseFlow:
        """동일한 root를 가진 여러 ProtoFlow들을 하나로 병합 - 각 flow의 aspect를 key로 하여 payload를 딕셔너리 형태"""
        log.info(f"  [Gather] {len(flows)}개의 흐름을 하나로 병합 (root: {root})")
        
        ## 각 파편의 aspect(예: 'tech', 'market')를 키로 사용하여 페이로드 병합
        merged_payload = {f.aspect: f.payload for f in flows}
        return PhaseFlow(
            payload=merged_payload,
            id=root,
            aspect="merged",
            root=root
        )


# ==========================================
# Phase Ators
# ==========================================

class PhaseAtor(ABC):
    @abstractmethod
    async def run(self, flow: PhaseFlow, operator: Any, ctx: FlowState) -> List[Tuple[str, FlowState]]:
        raise NotImplementedError()


@contract.ator("ator", role="phase_node")
@proto(Proto((PhaseFlow, Transduction, "List[Tuple]"), kind="transduction"))
class TransAtor(PhaseAtor):
    def __init__(self, spec):
        self.role = spec["role"]
        self.next = spec["next"]
        self.node_context = spec.get("context", {})
        self.spec = spec

    async def run(self, flow: PhaseFlow, operator: Transduction, ctx: FlowState):
        log.info(f"    [TransAtor] '{self.role}' initiates self-transmutation")
        injected_state = {k: ctx.state.get(k) for k in self.node_context.get("inject_state", [])}
        flow.payload = {
            "raw_input": flow.payload,
            "instructions": self.node_context.get("instruction"),
            "injected_state": injected_state,
            "hyperparams": {"temperature": self.node_context.get("temperature", 0.7)}
        }

        loop = asyncio.get_running_loop()
        new_flow = await loop.run_in_executor(None, operator.transduce, flow, self)
        ctx.flow = new_flow
        return [(self.next, ctx)]


@contract.ator("aligner", role="phase_node")
@proto(Proto((PhaseFlow, Align, "State"), kind="aligner"))
class AlignAtor(PhaseAtor):
    def __init__(self, spec):
        self.next = spec["next"]
        self.spec = spec

    async def run(self, flow: PhaseFlow, operator: Align, ctx: FlowState):
        log.info(f"    [AlignAtor] reconcile ψ:{flow.id}")
        result = operator.align(flow, self.spec)
        ctx.state.update(result.get("state", {}))
        flow.payload = result.get("payload", flow.payload)
        self.next = result.get("next", self.next)
        return [(self.next, ctx)]


@contract.ator("judgment", role="phase_node")
@proto(Proto((PhaseFlow, Judgment, "str"), kind="judgment"))
class JudgmentAtor(PhaseAtor):
    def __init__(self, spec):
        self.rules = spec["rules"]
        op_name = spec.get("operator", "default_judgment")
        
        # [개선됨] 카테고리 인자("ator") 삭제 - 단일 풀에서 자동 탐색
        self.custom_op = registry.create_component({"type": op_name})

    async def run(self, flow: PhaseFlow, operator: Judgment, ctx: FlowState):
        # [버그 수정] 존재하지 않는 base_operator 대신 주입받은 operator 사용
        ator = self.custom_op or operator
        target = ator.dispatch(flow, self.rules)
        log.info(f"    [JudgmentAtor] ψ:{flow.id} → {target}")
        return [(target, ctx)]


@contract.ator("dispersion", role="phase_node")
@proto(Proto((PhaseFlow, Dispersion, "List[ProtoFlow]"), kind="dispersion"))
class DispersionAtor(PhaseAtor):
    def __init__(self, spec):
        self.aspects = spec["aspects"]
        self.next = spec["next"]

    async def run(self, flow: PhaseFlow, operator: Dispersion, ctx: FlowState):
        log.info(f"    [DispersionAtor] scatter ψ:{flow.id}")
        flows = operator.scatter(flow, self.aspects)
        return [(self.next, FlowState(f, ctx.state)) for f in flows]


@contract.ator("gather", role="phase_node")
@proto(Proto((PhaseFlow, Gather, "List[ProtoFlow]"), kind="gather"))
class GatherAtor(PhaseAtor):
    def __init__(self, spec):
        self.next = spec["next"]
        self.buffer = {}
        self.expected = spec.get("expected_count", 2)

    async def run(self, flow: PhaseFlow, operator: Gather, ctx: FlowState):
        root = flow.root
        slot = self.buffer.setdefault(root, [])
        slot.append(flow)

        log.info(f"    [GatherAtor] buffer ψ:{flow.id} {len(slot)}/{self.expected}")
        if len(slot) < self.expected:
            return []

        flows = self.buffer.pop(root)
        new_flow = operator.merge(flows, root)
        return [(self.next, FlowState(new_flow, ctx.state))]


@contract.ator("resonance", role="phase_node")
@proto(Proto((PhaseFlow, Resonance, "ProtoFlow"), kind="resonance"))
class ResonanceAtor(PhaseAtor):
    def __init__(self, spec):
        self.next = spec["next"]
        self.buffer = {}
        op_name = spec.get("operator", "default_resonance")
        self.custom_op = registry.create_component({"type": op_name})

    async def run(self, flow: PhaseFlow, operator: Resonance, ctx: FlowState):
        root = flow.root
        tag = flow.aspect

        slot = self.buffer.setdefault(root, {})
        slot[tag] = flow.payload
        log.info(f"    [ResonanceAtor] interference update {tag}")
        if "code" not in slot or "logic" not in slot:
            return []

        data = self.buffer.pop(root)
        active_operator = self.custom_op or operator
        payload = active_operator.interfere(data["code"], data["logic"])
        
        new_flow = PhaseFlow(payload=payload, id=root, aspect="resonated", root=root)
        return [(self.next, FlowState(new_flow, ctx.state))]