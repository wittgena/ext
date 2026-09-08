# xphi.kernel.adapter.gateway
import time
import json
import base64
import uuid
import asyncio
import math
import re
from functools import lru_cache
from typing import Dict, Any, Tuple, Optional, List

from pydantic import BaseModel, AnyUrl, IPvAnyAddress

from cryptography.hazmat.primitives.asymmetric import ed25519, ec, rsa
from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePublicNumbers
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding

from xphi.kernel.space.topos.tunnel.factory import UniversalFacade
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("gateway.adapter")


# ==========================================
# Identity & Security Models (fiber.infra)
# ==========================================

class AgentIdentity(BaseModel):
    target_server_id: str
    agent_uri: AnyUrl
    proof_of_possession: Optional[str] = None
    receipt: Optional[str] = None
    client_ip: IPvAnyAddress
    nonce: str
    idempotency_key: str


class IdempotencyMapper:
    def __init__(self, tunnel: UniversalFacade):
        self.tunnel = tunnel

    async def get_or_create_handle(self, target_id: str, idempotency_key: str) -> Tuple[str, bool]:
        redis_key = f"mcp:idem:{target_id}:{idempotency_key}"
        try:
            # 1차 조회
            existing_handle = await self.tunnel.get(redis_key)
            if existing_handle:
                return str(existing_handle), False
                
            # 캐시 미스 시 새로운 핸들 생성
            entropy = uuid.uuid4().hex[:12]
            new_handle = f"txn_{int(time.time())}_{entropy}"
            
            # 원자적 기록 시도 (Check-Then-Act 레이스 방어)
            is_set = await self.tunnel.set(redis_key, new_handle, ex=86400, nx=True)
            
            if is_set:
                return new_handle, True
            else:
                await asyncio.sleep(0.01)
                winner_handle = await self.tunnel.get(redis_key)
                if winner_handle:
                    return str(winner_handle), False
                else:
                    raise RuntimeError("Idempotency Race Condition: Lost lock but key is gone.")
        except Exception as e:
            log.critical(f"Tunnel Idempotency Check Failed: {e}")
            raise RuntimeError("Distributed state storage unavailable")


class JwkAdapter:
    @staticmethod
    def _b64_decode(data: str) -> bytes:
        padding_needed = '=' * ((4 - len(data) % 4) % 4)
        return base64.urlsafe_b64decode(data + padding_needed)

    @classmethod
    def parse_public_key(cls, jwk: Dict[str, Any]):
        jwk_str = json.dumps(jwk, sort_keys=True)
        return cls._cached_parse(jwk_str)

    @staticmethod
    @lru_cache(maxsize=1024)
    def _cached_parse(jwk_str: str):
        jwk = json.loads(jwk_str)
        kty = jwk.get("kty")
        try:
            if kty == "OKP" and jwk.get("crv") == "Ed25519":
                return ed25519.Ed25519PublicKey.from_public_bytes(JwkAdapter._b64_decode(jwk["x"]))
            elif kty == "RSA":
                n = int.from_bytes(JwkAdapter._b64_decode(jwk["n"]), byteorder="big")
                e = int.from_bytes(JwkAdapter._b64_decode(jwk["e"]), byteorder="big")
                return RSAPublicNumbers(e, n).public_key()
            elif kty == "EC":
                curves = {"P-256": ec.SECP256R1(), "P-384": ec.SECP384R1(), "P-521": ec.SECP521R1()}
                crv = curves.get(jwk.get("crv"))
                if not crv: raise ValueError("Unsupported Elliptic Curve")
                x = int.from_bytes(JwkAdapter._b64_decode(jwk["x"]), byteorder="big")
                y = int.from_bytes(JwkAdapter._b64_decode(jwk["y"]), byteorder="big")
                return EllipticCurvePublicNumbers(x, y, crv).public_key()
            raise ValueError(f"Unsupported JWK kty: {kty}")
        except Exception as e:
            raise ValueError(f"Malformed JWK structure: {e}")


class DPoPValidator:
    @staticmethod
    def _b64_decode_str(data: str) -> str:
        return JwkAdapter._b64_decode(data).decode('utf-8')

    @classmethod
    def verify_token(cls, jwt_str: str, expected_nonce: str, htu: str, htm: str) -> bool:
        try:
            parts = jwt_str.split('.')
            if len(parts) != 3: return False
            
            header = json.loads(cls._b64_decode_str(parts[0]))
            payload = json.loads(cls._b64_decode_str(parts[1]))
            signature = JwkAdapter._b64_decode(parts[2])
            
            # Replay 공격 방어 (시간 검증)
            if payload.get("nonce") != expected_nonce: return False
            if payload.get("htm") != htm or payload.get("htu") != htu: return False
            if abs(time.time() - payload.get("iat", 0)) > 60: return False
            
            jwk = header.get("jwk")
            if not jwk: return False
            
            # 캐싱된 공개키 객체 로드
            public_key = JwkAdapter.parse_public_key(jwk)
            signed_data = f"{parts[0]}.{parts[1]}".encode('utf-8')
            
            if isinstance(public_key, ed25519.Ed25519PublicKey):
                public_key.verify(signature, signed_data)
            elif isinstance(public_key, rsa.RSAPublicKey):
                public_key.verify(signature, signed_data, padding.PKCS1v15(), hashes.SHA256())
            elif isinstance(public_key, ec.EllipticCurvePublicKey):
                public_key.verify(signature, signed_data, ec.ECDSA(hashes.SHA256()))
                
            return True
        except (InvalidSignature, Exception) as e:
            log.warning(f"DPoP Verification Failed: {e}")
            return False


class NonceReplayProtector:
    def __init__(self, tunnel: UniversalFacade):
        self.tunnel = tunnel
        
    async def validate_and_lock_nonce(self, nonce: str, ttl: int = 300) -> bool:
        """일회성 논스(Nonce)를 Tunnel에 Lock 처리하여 A2A 트랜잭션의 Replay Attack을 차단"""
        try:
            return bool(await self.tunnel.set(f"sec:nonce:{nonce}", "1", ex=ttl, nx=True))
        except Exception as e:
            log.critical(f"Tunnel Nonce Verification Failed: {e}")
            return False


# ==========================================
# WASM/FFI Sanitization (xphi.kernel)
# ==========================================

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
        if not raw_payload:
            return ""
            
        clean_str = re.sub(r'[\uFEFF\u200B\u200C\u200D]', '', str(raw_payload))
        return clean_str.strip()

    @staticmethod
    def build_evaluate_payload(dimension: int, base_friction: float, raw_payload: str, state_vector: List[int]) -> Dict[str, Any]:
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