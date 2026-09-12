# xphi.arch.bound.xor.parser.ruleset.engine
## @lineage: xphi.bound.xor.parser.ruleset.engine
## @lineage: xphi.bound.parser.ruleset.engine
## @lineage: xphi.xor.parser.ruleset.engine
import json
import re
import orjson
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Generic, TypeVar, Tuple
from xphi.arch.bound.xor.secret.redact import redact_string, sanitize_payload
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("ruleset.engine")

InputT = TypeVar('InputT')
OutputT = TypeVar('OutputT')
EngineT = TypeVar('EngineT')

class CompiledEngine(ABC, Generic[InputT, OutputT]):
    @abstractmethod
    def execute(self, payload: InputT) -> OutputT:
        pass

class AbstractRulesetParser(ABC, Generic[EngineT]):
    @abstractmethod
    def parse_ruleset(self, ruleset: Dict[str, Any]) -> EngineT:
        pass

class FastRegexRedactionEngine(CompiledEngine[bytes, bytes]):
    """@desc: 외부 모듈(redact_string) 또는 주입된 정규식을 통해 바이트 스트림 마스킹"""
    def __init__(self, patterns: Optional[List[str]] = None):
        self._use_custom_patterns = bool(patterns)
        if self._use_custom_patterns:
            self._compiled_regexes = [re.compile(p.encode('utf-8'), re.IGNORECASE) for p in patterns]
            self.mask_token = b"***REDACTED***"

    def execute(self, payload: bytes) -> bytes:
        if self._use_custom_patterns:
            redacted = payload
            for r in self._compiled_regexes:
                redacted = r.sub(self.mask_token, redacted)
            return redacted
        else:
            try:
                return redact_string(payload.decode('utf-8')).encode('utf-8')
            except UnicodeDecodeError:
                return payload

class StructuralRedactionEngine(CompiledEngine[bytes, bytes]):
    """@desc: JSON 파싱 후 마스킹. 실패 시 컴파일 시 주입된 fallback 엔진으로 위임"""
    def __init__(self, fallback_engine: CompiledEngine[bytes, bytes]):
        self._fallback = fallback_engine

    def execute(self, payload: bytes) -> bytes:
        try:
            parsed_obj = orjson.loads(payload)
            sanitized_obj = sanitize_payload(parsed_obj)
            return orjson.dumps(sanitized_obj)
        except orjson.JSONDecodeError:
            return self._fallback.execute(payload)

class StructuralExtractionEngine(CompiledEngine[bytes, Dict[str, Any]]):
    def __init__(self, extract_paths: Dict[str, List[str]]):
        self.paths = extract_paths

    def execute(self, payload: bytes) -> Dict[str, Any]:
        try:
            parsed = orjson.loads(payload)
            result = {}
            for field_name, path_list in self.paths.items():
                val = parsed
                for p in path_list:
                    if isinstance(val, dict) and p in val:
                        val = val[p]
                    else:
                        val = None
                        break
                result[field_name] = val
            return result
        except orjson.JSONDecodeError:
            return {}

class StructuralExtractionParser(AbstractRulesetParser[StructuralExtractionEngine]):
    def parse_ruleset(self, ruleset: Dict[str, Any]) -> StructuralExtractionEngine:
        extract_paths = {}
        for target in ruleset.get("targets", []):
            field_name = target.get("tag")
            path_str = target.get("path", "")
            if field_name and path_str:
                extract_paths[field_name] = path_str.split(".")
                
        log.info(f"[Parser] Compiled StructuralExtractionEngine targeting: {list(extract_paths.keys())}")
        return StructuralExtractionEngine(extract_paths)

class AuditRulesetParser(AbstractRulesetParser[CompiledEngine[bytes, bytes]]):
    def parse_ruleset(self, ruleset: Dict[str, Any]) -> CompiledEngine[bytes, bytes]:
        regex_engine = FastRegexRedactionEngine()
        inspection_level = ruleset.get("global_config", {}).get("inspection_level", "structural")
        if inspection_level == "structural":
            log.info("[Parser] Compiling StructuralRedactionEngine (Tree Traversal Active).")
            return StructuralRedactionEngine(fallback_engine=regex_engine)
        
        log.info("[Parser] Compiling FastRegexRedactionEngine (Flat Regex Scan Active).")
        return regex_engine

class FastLifecycleEngine(CompiledEngine[str, List[str]]):
    """@desc: C 레벨 정규식으로 컴파일된 패턴을 활용해 O(1)에 가까운 속도로 스트림을 분류합니다."""
    def __init__(self, compiled_rules: List[Tuple[re.Pattern, str]]):
        self.rules = compiled_rules

    def execute(self, payload: str) -> List[str]:
        # C 언어로 작성된 re.search만 호출하므로 Python 레벨의 연산 최소화
        return [tag for pattern, tag in self.rules if pattern.search(payload)]


class LifecycleRegexParser(AbstractRulesetParser[FastLifecycleEngine]):
    """@desc: JSON 룰셋의 AND/OR 조건을 정규식 패턴으로 변환 후 컴파일합니다."""
    
    def parse_ruleset(self, ruleset: Dict[str, Any]) -> FastLifecycleEngine:
        compiled_rules = []
        for target in ruleset.get("targets", []):
            tag = target.get("tag")
            keywords = target.get("keywords", [])
            if not tag or not keywords: continue

            # AND 조건을 정규식의 전방 탐색(Positive Lookahead)으로 변환
            # 예: {"AND": ["Netty started", "port"]} -> (?=.*Netty started)(?=.*port)
            regex_parts = []
            for group in keywords:
                if "AND" in group:
                    # 대소문자 무시 및 순서 무관 매칭
                    lookaheads = "".join(f"(?=.*{re.escape(word)})" for word in group["AND"])
                    regex_parts.append(f"^{lookaheads}.*")
                elif "OR" in group:
                    # OR 조건 변환
                    ors = "|".join(re.escape(word) for word in group["OR"])
                    regex_parts.append(f"(?:{ors})")

            if regex_parts:
                # 각 키워드 그룹을 결합하여 단일 정규식으로 컴파일 (IGNORECASE 플래그 적용)
                final_regex = "|".join(regex_parts)
                compiled_pattern = re.compile(final_regex, re.IGNORECASE)
                compiled_rules.append((compiled_pattern, tag))

        return FastLifecycleEngine(compiled_rules)