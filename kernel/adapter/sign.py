# xphi.kernel.adapter.sign
import os
from pathlib import Path
import hashlib
import nacl.signing
import nacl.encoding
import nacl.exceptions
from cryptography.hazmat.primitives import serialization
from typing import Dict, Any, Optional

from xphi.kernel.adapter.state import StateAdapter
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("adapter.sign")

class NodeSigner:
    """Singleton identity manager responsible for hierarchical key loading and deterministic cryptographic signature generation for the node"""
    _instance = None

    def __init__(self):
        self.signing_key = self._load_key_chain()
        self.verify_key = self.signing_key.verify_key
        self.pubkey_hex = self.verify_key.encode(encoder=nacl.encoding.HexEncoder).decode('utf-8')
        log.info(f"[Crypto] Node identity loaded. PubKey: {self.pubkey_hex[:8]}...")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _load_key_chain(self) -> nacl.signing.SigningKey:
        # Priority 1: Load a 64-character raw hex seed from environment variables
        env_seed = os.environ.get("XPHI_NODE_SEED")
        if env_seed and len(env_seed) == 64:
            return nacl.signing.SigningKey(env_seed, encoder=nacl.encoding.HexEncoder)

        # Priority 2: Fallback to the local SSH key
        ssh_key_path = Path.home() / ".ssh" / "id_ed25519"
        if ssh_key_path.exists():
            try:
                with open(ssh_key_path, "rb") as key_file:
                    private_key = serialization.load_ssh_private_key(
                        key_file.read(),
                        password=None
                    )
                raw_bytes = private_key.private_bytes(
                    encoding=serialization.Encoding.Raw,
                    format=serialization.PrivateFormat.Raw,
                    encryption_algorithm=serialization.NoEncryption()
                )
                return nacl.signing.SigningKey(raw_bytes)
            except Exception as e:
                log.warning(f"Failed to load SSH key ({e}). Falling back to ephemeral key generation...")

        # Priority 3: Generate an ephemeral key if no persistent identity is found
        log.warning("[Crypto] No persistent identity found. Using ephemeral (in-memory) key.")
        return nacl.signing.SigningKey.generate()

    def sign_payload(self, canonical_bytes: bytes) -> str:
        """Generates an Ed25519 signature for the given payload"""
        payload_hash_str = hashlib.sha256(canonical_bytes).hexdigest()
        signed = self.signing_key.sign(payload_hash_str.encode('utf-8'))
        return signed.signature.hex()

    def verify_signature(self, canonical_bytes: bytes, signature_hex: str, pubkey_hex: Optional[str] = None) -> bool:
        """Verifies an Ed25519 signature against the given payload"""
        try:
            ## 서명할 때와 동일하게 Canonical Hash String 생성
            payload_hash_str = hashlib.sha256(canonical_bytes).hexdigest()
            message_bytes = payload_hash_str.encode('utf-8')
            signature_bytes = bytes.fromhex(signature_hex)

            ## 공개키가 명시적으로 주어지면 해당 키 사용, 없으면 자기 자신의 공개키 사용
            if pubkey_hex:
                vk = nacl.signing.VerifyKey(pubkey_hex, encoder=nacl.encoding.HexEncoder)
            else:
                vk = self.verify_key

            ## 서명 검증 (실패 시 nacl.exceptions.BadSignatureError 발생)
            vk.verify(message_bytes, signature_bytes)
            return True
        except (nacl.exceptions.BadSignatureError, ValueError) as e:
            log.debug(f"[Crypto] Invalid signature detected: {e}")
            return False
        except Exception as e:
            log.error(f"[Crypto] Verification process failed: {e}")
            return False

class LedgerAuthAdapter:
    @staticmethod
    def sign_state_payload(payload_dict: Dict[str, Any]) -> str:
        canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        signer = NodeSigner.get_instance()
        return signer.sign_payload(canonical_bytes)
        
    @staticmethod
    def get_signer_pubkey() -> str:
        return NodeSigner.get_instance().pubkey_hex