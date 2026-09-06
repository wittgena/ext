# xphi.bound.secret.cipher
## @lineage: xphi.xor.secret.cipher
## @lineage: xphi.arch.xor.secret.cipher
## @lineage: arch.xor.secret.cipher
import hashlib
from base64 import b64encode
from cryptography.fernet import Fernet, InvalidToken
from pydantic import SecretStr
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class Cipher:
    def __init__(self, secret_key: str):
        self.secret_key = secret_key
        self._fernet: Fernet | None = None

    def encrypt(self, secret: SecretStr | None) -> str | None:
        if secret is None:
            return None
        
        # 명시적으로 utf-8 인코딩 사용
        secret_value = secret.get_secret_value().encode("utf-8")
        fernet = self._get_fernet()
        return fernet.encrypt(secret_value).decode("utf-8")

    def decrypt(self, secret: str | None) -> SecretStr | None:
        if secret is None:
            return None
            
        try:
            fernet = self._get_fernet()
            decrypted = fernet.decrypt(secret.encode("utf-8")).decode("utf-8")
            return SecretStr(decrypted)
        except InvalidToken as e:
            # 암호화 키가 다르거나 토큰이 유효하지 않을 때 발생하는 구체적인 예외 처리
            log.warning(
                f"Failed to decrypt secret value (setting to None): {e}. "
                "This may occur when loading conversations encrypted with a different "
                "key or when upgrading from older versions."
            )
            return None
        except Exception as e:
            # 기타 예상치 못한 에러 로깅 추가
            log.error(f"Unexpected error during decryption: {e}")
            return None

    def _get_fernet(self) -> Fernet:
        # 캐싱된 Fernet 객체가 없다면 생성
        if self._fernet is None:
            secret_key = self.secret_key.encode("utf-8")
            fernet_key = b64encode(hashlib.sha256(secret_key).digest())
            
            # 불필요한 object.__setattr__ 대신 파이썬의 표준적인 속성 할당 방식 사용
            self._fernet = Fernet(fernet_key)
            
        return self._fernet