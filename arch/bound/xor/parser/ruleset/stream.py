# xphi.arch.bound.xor.parser.ruleset.stream
## @lineage: xphi.bound.xor.parser.ruleset.stream
## @lineage: xphi.bound.parser.ruleset.stream
## @lineage: xphi.xor.parser.ruleset.stream
## @lineage: xphi.arch.xor.parser.stream
## @lineage: arch.xor.parser.stream
import json
from typing import List, Tuple, Dict, Any, Optional, Callable
from elasticsearch.dsl import Q

from xphi.arch.bound.xor.parser.ruleset.engine import AbstractRulesetParser, CompiledEngine
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("parser.stream")

class ElasticDSLRulesetParser(AbstractRulesetParser[List[Tuple[Q, str]]]):
    """@desc: 룰셋을 Elasticsearch DSL 쿼리 객체 리스트로 컴파일합니다."""
    
    def _dict_to_q(self, kv_dict: Dict[str, Any], is_exclusion: bool = False) -> Optional[Q]:
        if not kv_dict: return None
        
        queries = []
        for key, value in kv_dict.items():
            if isinstance(value, list):
                q = Q("terms", **{key: value})
            else:
                q = Q("term", **{key: value})
            queries.append(~q if is_exclusion else q)
        if not queries: return None
        
        # 생성된 여러 Q 객체를 & (AND) 연산자로 결합하여 단일 Q 반환
        combined_q = queries[0]
        for q in queries[1:]:
            combined_q &= q
            
        return combined_q

    def _keywords_to_q(self, keywords: List[Dict[str, List[str]]], search_field: str = "message") -> Optional[Q]:
        """
        @desc: 키워드 리스트를 받아 로그의 message 필드를 검색하는 match_phrase 쿼리로 변환합니다.
        """
        if not keywords: return None
        
        components = []
        for group in keywords:
            if "AND" in group and group["AND"]:
                # AND 그룹: Q() & Q() 결합
                q = Q("match_phrase", **{search_field: group["AND"][0]})
                for val in group["AND"][1:]:
                    q &= Q("match_phrase", **{search_field: val})
                components.append(q)
                
            elif "OR" in group and group["OR"]:
                # OR 그룹: Q() | Q() 결합
                q = Q("match_phrase", **{search_field: group["OR"][0]})
                for val in group["OR"][1:]:
                    q |= Q("match_phrase", **{search_field: val})
                components.append(q)

        if not components: return None
        
        # 각 키워드 그룹(리스트의 요소)은 최종적으로 & (AND)로 묶임
        combined_q = components[0]
        for q in components[1:]:
            combined_q &= q
            
        return combined_q

    def parse_ruleset(self, ruleset: Dict[str, Any], target_tags: Optional[List[str]] = None) -> List[Tuple[Q, str]]:
        global_config = ruleset.get("global_config", {})
        
        base_q = self._dict_to_q(global_config.get("base_query", {}))
        excl_q = self._dict_to_q(global_config.get("noise_exclusions", {}), is_exclusion=True)
        
        compiled_queries = []
        
        for target in ruleset.get("targets", []):
            tag = target.get("tag")
            if target_tags and tag not in target_tags: continue
                
            cond_q = self._dict_to_q(target.get("condition", {}))
            kw_q = self._keywords_to_q(target.get("keywords", []))
            apply_exclusions = target.get("apply_exclusions", False)
            
            ## 파이썬 객체인 Q를 결합하기만 하면, Elasticsearch DSL이 내부적으로 트리를 구성함
            final_q = None
            for q_obj in [base_q, cond_q, excl_q if apply_exclusions else None, kw_q]:
                if q_obj is not None:
                    final_q = q_obj if final_q is None else (final_q & q_obj)
            
            if final_q:
                compiled_queries.append((final_q, tag))
                
        return compiled_queries


# =====================================================================
# 2. 로컬 스트림 엔진 및 파서 (도커 컨테이너/에이전트 로그 실시간 평가용)
# =====================================================================
class LocalStreamEngine(CompiledEngine[str, List[str]]):
    """
    @desc: 기존 Python Callable들을 순회(Sequential)하며 실시간 평가하는 엔진.
           UniversalLogAuditor 등에서 .execute(line) 형태로 단일 호출됩니다.
    """
    def __init__(self, evaluators: List[Tuple[Callable[[str], bool], str]]):
        self.evaluators = evaluators

    def execute(self, payload: str) -> List[str]:
        return [tag for eval_fn, tag in self.evaluators if eval_fn(payload)]


class LocalStreamRulesetParser(AbstractRulesetParser[LocalStreamEngine]):
    """@desc: JSON 룰셋을 로컬 스트림 평가 엔진(LocalStreamEngine)으로 컴파일합니다."""
    
    def _keywords_to_evaluator(self, keywords: List[Dict[str, List[str]]]) -> Optional[Callable[[str], bool]]:
        if not keywords: return None
        
        def evaluator(line: str) -> bool:
            line_lower = line.lower()
            for group in keywords:
                if "AND" in group and group["AND"]:
                    if not all(val.lower() in line_lower for val in group["AND"]):
                        return False
                elif "OR" in group and group["OR"]:
                    if not any(val.lower() in line_lower for val in group["OR"]):
                        return False
            return True
            
        return evaluator

    def parse_ruleset(self, ruleset: Dict[str, Any], target_tags: Optional[List[str]] = None) -> LocalStreamEngine:
        compiled_evaluators = []
        for target in ruleset.get("targets", []):
            tag = target.get("tag")
            if target_tags and tag not in target_tags: continue
            
            kw_evaluator = self._keywords_to_evaluator(target.get("keywords", []))
            if kw_evaluator:
                compiled_evaluators.append((kw_evaluator, tag))
                
        return LocalStreamEngine(compiled_evaluators)