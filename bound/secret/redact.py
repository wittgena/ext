# xphi.bound.secret.redact
## @lineage: xphi.xor.secret.redact
## @lineage: xphi.arch.xor.secret.redact
## @lineage: arch.xor.secret.redact
import copy
import re
from collections.abc import Mapping
from typing import Any, List
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
import httpx

_REDACTED = "<redacted>"

SECRET_KEY_PATTERNS = frozenset(
    {
        "AUTHORIZATION",
        "CERTIFICATE",
        "COOKIE",
        "CREDENTIAL",
        "JWT",
        "KEY",
        "OAUTH",
        "PASSPHRASE",
        "PASSWORD",
        "PRIVATE",
        "SECRET",
        "SESSION",
        "SIGNATURE",
        "TOKEN",
    }
)

REDACT_ALL_VALUES_KEYS = frozenset({"environment", "env", "headers", "acp_env"})
SENSITIVE_URL_PARAMS = frozenset(
    {
        "tavilyapikey",
        "apikey",
        "api_key",
        "token",
        "access_token",
        "secret",
        "key",
    }
)


def is_secret_key(key: str) -> bool:
    key_upper = key.upper()
    return any(pattern in key_upper for pattern in SECRET_KEY_PATTERNS)


def _build_secret_patterns() -> "re.Pattern[str]":
    patterns: List[str] = [
        # Standard & Cloud Providers
        r"(?:AKIA|ASIA)[0-9A-Z]{16}",
        r"(?:aws_secret_access_key|aws_session_token|aws_access_key_id)\s*[:=]\s*[A-Za-z0-9/+=]{20,}",
        r"AIza[0-9A-Za-z\-_]{35}",
        r"\bya29\.[A-Za-z0-9_.~+/-]+",
        
        # Common Tokens & Keys
        r"Bearer\s+[A-Za-z0-9\-._~+/]{10,}=*",
        r"Basic\s+[A-Za-z0-9+/]{10,}={0,2}",
        r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*",  # JWT
        r"-----BEGIN[A-Z \-]*PRIVATE KEY-----[\s\S]*?-----END[A-Z \-]*PRIVATE KEY-----",
        
        # Service Specific (GitHub, Stripe, Slack, etc.)
        r"gh[pousr]_[A-Za-z0-9_]{20,}",  # GitHub (Classic, Fine-grained, OAuth, Server, Refresh)
        r"(?:sk|rk)_(?:test|live)_[a-zA-Z0-9]{24,}",  # Stripe
        r"hooks\.slack\.com/services/T[a-zA-Z0-9_]+/B[a-zA-Z0-9_]+/[a-zA-Z0-9_]+",  # Slack Webhooks
        r"xox[bp]-[A-Za-z0-9_-]{20,}",  # Slack bot/user tokens
        
        # LLM & Third-Party APIs
        r"sk-(?:or-v1|proj|ant-(?:api|oat)\d{2})-[A-Za-z0-9_-]{20,}",  # OpenAI, Anthropic, OpenRouter
        r"sk-[A-Za-z0-9\-_]{20,}",
        r"sk-oh-[A-Za-z0-9]{20,}",
        r"gsk_[A-Za-z0-9]{20,}",  # GROQ
        r"hf_[A-Za-z0-9]{20,}",  # HuggingFace
        r"tgp_v1_[A-Za-z0-9_-]{20,}",  # Together AI
        r"ctx7sk-[A-Za-z0-9_-]{10,}",  # Context7
        r"cla_[A-Za-z0-9_-]{20,}",  # Claude.ai
        r"sntryu_[A-Za-z0-9]{10,}",  # Sentry
        r"lin_api_[A-Za-z0-9]{10,}",  # Linear
        r"tvly-[A-Za-z0-9_-]{10,}",  # Tavily
        r"ATATT3x[A-Za-z0-9_-]{10,}",  # Jira
        r"dapi[0-9a-f]{32}",
        
        # Key-Value / Generic formats (Tolerant spacing)
        r"(?:client_secret|azure_password|azure_username)['\"]?\s*[:=]\s*['\"]?[^\s,'\"})\]{}>]+",
        r"(?:api[_-]?key|x-api-key|x-ak-[A-Za-z0-9\-_]{20,})['\"]?\s*[:=]\s*['\"]?[^\s,'\"})\]{}>]{8,}",
        r"(?:^|(?<=\W))\w*(?:password|passwd|client_secret|secret_key|_secret)['\"]?\s*[:=]\s*['\"]?[^\s,'\"})\]{}>]+",
        r"(?:master_key|xai_key|database_url|db_url|connection_string|signing_key|encryption_key|auth_token|access_token|refresh_token|slack_webhook_url|webhook_url|database_connection_string|huggingface_token|jwt_secret|tavilyApiKey)['\"]?\s*[:=]\s*['\"]?[^\s,'\"})\]{}>]+",
        r"(?<=[?&])(?:key|token|secret|api_key|access_token|tavilyApiKey)=[^\s&'\"]{8,}",
        
        # Auth Headers & Service Accounts
        r"(?<=://)[^\s'\"]*:[^\s'\"@]+(?=@)",
        r'\{[^{}]*"type"\s*:\s*"service_account"[^{}]*(?:\{[^{}]*\}[^{}]*)*\}',
        r"""private_key['\"]?\s*[:=]\s*['\"]?(?:-----BEGIN[A-Z \-]*PRIVATE KEY-----[\s\S]*?-----END[A-Z \-]*PRIVATE KEY-----|[^\s,'\"})\]{}>]+)""",
    ]
    return re.compile("|".join(patterns), re.IGNORECASE)


_SECRET_RE = _build_secret_patterns()


def redact_string(value: str) -> str:
    """Scrub known secret/credential patterns from a string and return the result."""
    return _SECRET_RE.sub(_REDACTED, value)


def redact_url_params(url: str) -> str:
    try:
        parsed = urlparse(url)
    except Exception:
        return url

    if not parsed.query:
        return url

    params = parse_qs(parsed.query, keep_blank_values=True)
    redacted_params: dict[str, list[str]] = {}
    for param_name, values in params.items():
        if param_name.lower() in SENSITIVE_URL_PARAMS or is_secret_key(param_name):
            redacted_params[param_name] = [_REDACTED] * len(values)
        else:
            redacted_params[param_name] = values

    # doseq=True tells urlencode to unpack the value lists correctly.
    redacted_query = urlencode(redacted_params, doseq=True)
    return urlunparse(parsed._replace(query=redacted_query))


def _redact_all_values(value: Any) -> Any:
    """Recursively redact all values while preserving structure (key names)."""
    if isinstance(value, Mapping):
        return {k: _redact_all_values(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_all_values(item) for item in value]
    return _REDACTED


def sanitize_payload(obj: Any) -> Any:
    """Single-pass traversal to redact sensitive dictionary keys, URL queries, and raw string secrets."""
    if isinstance(obj, Mapping):
        sanitized = {}
        for key, value in obj.items():
            key_str = str(key)
            if key_str.lower() in REDACT_ALL_VALUES_KEYS:
                sanitized[key] = _redact_all_values(value)
            elif is_secret_key(key_str):
                sanitized[key] = _REDACTED
            else:
                sanitized[key] = sanitize_payload(value)
        return sanitized

    if isinstance(obj, list):
        return [sanitize_payload(item) for item in obj]

    if isinstance(obj, str):
        # 1. URL 쿼리 파라미터 우선 처리
        if "?" in obj:
            obj = redact_url_params(obj)
        # 2. 통합 정규식 마스킹 처리
        return redact_string(obj)

    return obj


def sanitize_config(config: dict[str, Any]) -> dict[str, Any]:
    """Deepcopy and apply a single-pass redaction logic."""
    return sanitize_payload(copy.deepcopy(config))


def http_error_log_content(response: httpx.Response) -> str | dict:
    try:
        return sanitize_payload(response.json())
    except Exception:
        body_len = len(response.text or "")
        return f"<non-JSON response body omitted ({body_len} chars)>"