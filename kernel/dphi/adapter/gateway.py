# xphi.kernel.dphi.adapter.gateway
import math
import re
from typing import Dict, Any, List

class GatewayAdapter:
    """
    @spec: FFI & Intent Sanitization Adapter for Gateway WASM.
    @role: Enforces strict schema mapping, finite-float safety, and payload sanitization for WASM boundary.
    """
    @staticmethod
    def _assert_safe_float(val: Any, name: str) -> float:
        f_val = float(val)
        if math.isnan(f_val) or math.isinf(f_val):
            raise ValueError(f"[FFI Boundary Error] '{name}' must be a finite float, got {val}")
        return f_val

    @staticmethod
    def _assert_uint16(val: int, name: str) -> int:
        if not isinstance(val, int) or not (0 <= val <= 65535):
            raise ValueError(f"[FFI Boundary Error] '{name}' must be a uint16 (0~65535), got {val}")
        return val

    @staticmethod
    def _assert_uint8_list(vec: List[int], expected_len: int, name: str) -> List[int]:
        if not isinstance(vec, (list, tuple)) or len(vec) != expected_len:
            raise ValueError(f"[FFI Boundary Error] '{name}' length mismatch. Expected {expected_len}, got {len(vec)}")
        for v in vec:
            if not isinstance(v, int) or not (0 <= v <= 255):
                raise ValueError(f"[FFI Boundary Error] '{name}' elements must be uint8 (0~255), got {v}")
        return list(vec)

    @staticmethod
    def sanitize_payload(raw_payload: str) -> str:
        """
        WASM Pest 파서로 넘어가기 전 파이썬 단에서 문자열을 1차 살균합니다.
        
        [주의] Base64 및 Hex 페이로드의 무결성을 위해 절대 대소문자를 변경(upper/lower)하지 않습니다.
        공백 압축(Whitespace squashing)은 Rust Pest 파서가 더 효율적으로 처리하므로 파이썬에서는 생략합니다.
        """
        if not raw_payload:
            return ""
            
        # 눈에 보이지 않는 쓰레기 문자(BOM: \uFEFF, ZWS: \u200B 등) 완벽 제거
        clean_str = re.sub(r'[\uFEFF\u200B\u200C\u200D]', '', str(raw_payload))
        
        # 양끝 공백만 제거하여 반환 (나머지 구문 검증은 WASM에게 위임)
        return clean_str.strip()

    @staticmethod
    def build_evaluate_payload(dimension: int, base_friction: float, raw_payload: str, state_vector: List[int]) -> Dict[str, Any]:
        """
        @target: Rust `GatewayInput` struct
        Rust 영역이 기대하는 정확한 타입과 살균된 데이터로 페이로드를 조립합니다.
        """
        safe_dimension = GatewayAdapter._assert_uint16(dimension, "dimension")
        safe_friction = GatewayAdapter._assert_safe_float(base_friction, "base_friction")
        safe_vector = GatewayAdapter._assert_uint8_list(state_vector, safe_dimension, "state_vector")
        safe_payload = GatewayAdapter.sanitize_payload(raw_payload)

        return {
            "dimension": safe_dimension,
            "base_friction": safe_friction,
            "raw_payload": safe_payload,
            "state_vector": safe_vector
        }