# xphi.arch.bound.xor.parser.ruleset.otlp
## @lineage: xphi.bound.xor.parser.ruleset.otlp
## @lineage: xphi.bound.parser.ruleset.otlp
## @lineage: xphi.xor.parser.ruleset.otlp
## @lineage: xphi.arch.xor.parser.otlp
## @lineage: arch.xor.parser.otlp
import orjson
from typing import List, Dict, Any
from xphi.arch.bound.xor.parser.ruleset.engine import AbstractRulesetParser, CompiledEngine
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("parser.otlp")

class StrictOtlpExtractionEngine(CompiledEngine[bytes, Dict[str, Any]]):
    def __init__(self, required_root_keys: List[str], extract_paths: Dict[str, List[str]]):
        self.required_root_keys = required_root_keys
        self.paths = extract_paths

    def execute(self, payload: bytes) -> Dict[str, Any]:
        try:
            parsed = orjson.loads(payload)
            for req_key in self.required_root_keys:
                if req_key not in parsed:
                    raise ValueError(f"Strict parsing failed: Missing required OTLP root key '{req_key}'")

            clean_result = {}
            for field_name, path_list in self.paths.items():
                val = parsed
                for p in path_list:
                    if isinstance(val, dict) and p in val:
                        val = val[p]
                    elif isinstance(val, list) and isinstance(p, int) and len(val) > p:
                        val = val[p] # 리스트 인덱싱 지원 (옵션)
                    else:
                        val = None
                        break
                clean_result[field_name] = val
            return clean_result
        except orjson.JSONDecodeError as e:
            raise ValueError(f"Malformed JSON payload: {str(e)}")

class StrictOtlpRulesetParser(AbstractRulesetParser[StrictOtlpExtractionEngine]):
    def parse_ruleset(self, ruleset: Dict[str, Any]) -> StrictOtlpExtractionEngine:
        global_config = ruleset.get("global_config", {})
        required_root_keys = global_config.get("required_root_keys", ["resourceLogs"])
        extract_paths = {}
        for target in ruleset.get("targets", []):
            field_name = target.get("tag")
            path_str = target.get("path", "")
            if field_name and path_str:
                extract_paths[field_name] = [int(p) if p.isdigit() else p for p in path_str.split(".")]
                
        log.info(f"[Parser] Compiled StrictOtlpExtractionEngine. Enforcing keys: {required_root_keys}")
        return StrictOtlpExtractionEngine(required_root_keys, extract_paths)