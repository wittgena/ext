# xphi.bound.xor.secret.client
import base64
import os
import binascii
import httpx
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union

from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("secret.client")

class KMSVendor:
    AZURE_KEY_VAULT = "azure_key_vault"
    GOOGLE_KMS = "google_kms"
    AWS_KMS = "aws_kms"
    AWS_SECRET_MANAGER = "aws_secret_manager"
    GOOGLE_SECRET_MANAGER = "google_secret_manager"
    HASHICORP_VAULT = "hashicorp_vault"
    CYBERARK = "cyberark"
    CUSTOM = "custom"
    LOCAL = "local"


class BaseSecretManager(ABC):
    @abstractmethod
    async def async_read_secret(
        self, secret_name: str, optional_params: Optional[dict] = None, timeout: Optional[Union[float, httpx.Timeout]] = None,
    ) -> Optional[str]: ...

    @abstractmethod
    def sync_read_secret(
        self, secret_name: str, optional_params: Optional[dict] = None, timeout: Optional[Union[float, httpx.Timeout]] = None,
    ) -> Optional[str]: ...

    @abstractmethod
    async def async_write_secret(
        self, secret_name: str, secret_value: str, description: Optional[str] = None, optional_params: Optional[dict] = None, timeout: Optional[Union[float, httpx.Timeout]] = None, tags: Optional[Union[dict, list]] = None,
    ) -> Dict[str, Any]: ...

    @abstractmethod
    async def async_delete_secret(
        self, secret_name: str, recovery_window_in_days: Optional[int] = 7, optional_params: Optional[dict] = None, timeout: Optional[Union[float, httpx.Timeout]] = None,
    ) -> dict: ...

    async def async_rotate_secret(
        self, current_secret_name: str, new_secret_name: str, new_secret_value: str, optional_params: Optional[dict] = None, timeout: Optional[Union[float, httpx.Timeout]] = None,
    ) -> dict:
        try:
            old_secret = await self.async_read_secret(current_secret_name, optional_params, timeout)
            if old_secret is None:
                raise ValueError(f"Current secret {current_secret_name} not found")

            create_response = await self.async_write_secret(
                new_secret_name, new_secret_value, f"Rotated from {current_secret_name}", optional_params, timeout
            )

            new_secret = await self.async_read_secret(new_secret_name, optional_params, timeout)
            if new_secret is None:
                raise ValueError(f"Failed to verify new secret {new_secret_name}")

            await self.async_delete_secret(
                current_secret_name, recovery_window_in_days=7, optional_params=optional_params, timeout=timeout
            )
            return create_response

        except httpx.HTTPStatusError as err:
            log.exception(f"Error rotating secret: {err.response.text}")
            raise ValueError(f"HTTP error occurred: {err.response.text}")
        except Exception as e:
            log.exception(f"Error rotating secret: {e}")
            raise


def _is_base64(s: str) -> bool:
    try:
        return base64.b64encode(base64.b64decode(s)).decode() == s
    except binascii.Error:
        return False


def get_secret_from_vendor(
    client: Any,
    key_manager: str,
    secret_name: str,
    key_management_settings: Optional[Any] = None,
    google_kms_resource_name: Optional[str] = None,  # config 의존성 대체를 위해 추가
) -> Optional[str]:
    """Vendor 별로 분기하여 Secret을 가져옵니다."""
    if key_manager == KMSVendor.LOCAL:
        return os.getenv(secret_name)

    if key_manager == KMSVendor.AZURE_KEY_VAULT or type(client).__name__ == "SecretClient":
        return client.get_secret(secret_name).value

    if key_manager == KMSVendor.GOOGLE_KMS or client.__class__.__name__ == "KeyManagementServiceClient":
        encrypted_secret = os.getenv(secret_name)
        if not encrypted_secret:
            raise ValueError("Google KMS requires the encrypted secret to be in the environment!")
        if not _is_base64(encrypted_secret):
            raise ValueError("Google KMS requires the encrypted secret to be encoded in base64")
        
        response = client.decrypt(
            request={
                "name": google_kms_resource_name,
                "ciphertext": base64.b64decode(encrypted_secret),
            }
        )
        return response.plaintext.decode("utf-8")

    if key_manager == KMSVendor.AWS_KMS:
        encrypted_value = os.getenv(secret_name)
        if not encrypted_value:
            raise Exception(f"AWS KMS - Encrypted Value of Key={secret_name} is None")
        
        response = client.decrypt(CiphertextBlob=base64.b64decode(encrypted_value))
        secret = response["Plaintext"].decode("utf-8")
        return secret.strip() if isinstance(secret, str) else secret

    if key_manager == KMSVendor.AWS_SECRET_MANAGER:
        from config.secret_managers.aws_secret_manager_v2 import AWSSecretsManagerV2
        if isinstance(client, AWSSecretsManagerV2):
            primary_secret_name = key_management_settings.primary_secret_name if key_management_settings else None
            return client.sync_read_secret(secret_name=secret_name, primary_secret_name=primary_secret_name)

    if key_manager == KMSVendor.GOOGLE_SECRET_MANAGER:
        secret = client.get_secret_from_google_secret_manager(secret_name)
        if secret is None:
            raise ValueError(f"No secret found in Google Secret Manager for {secret_name}")
        return secret

    if key_manager in (KMSVendor.HASHICORP_VAULT, KMSVendor.CYBERARK):
        secret = client.sync_read_secret(secret_name=secret_name)
        if secret is None:
            raise ValueError(f"No secret found in {key_manager} for {secret_name}")
        return secret

    if key_manager == KMSVendor.CUSTOM:
        if not isinstance(client, BaseSecretManager):
            raise ValueError(f"Custom secret manager client must be an instance of BaseSecretManager, got {type(client).__name__}")
        
        optional_params = key_management_settings.model_dump() if hasattr(key_management_settings, 'model_dump') else None
        secret = client.sync_read_secret(secret_name=secret_name, optional_params=optional_params)
        if secret is None:
            raise ValueError(f"No secret found in Custom Secret Manager for {secret_name}")
        return secret
    return client.get_secret(secret_name).secret_value