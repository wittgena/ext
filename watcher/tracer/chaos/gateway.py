# xphi.watcher.tracer.chaos.gateway
## @lineage: xphi.watcher.receptor.policy.gateway
import uuid
import time
import json
import re
from collections import defaultdict
from typing import Any, Dict, Optional

from xphi.watcher.server.stream.schema import LogicStream as IngressLogicStream
from xphi.state.ledger.consensus import KernelLedger, LogicStream as KernelLogicStream, SealedKernel, LedgerRole
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("mesh.gateway", phase="KERNEL")

class TokenBucketLimiter:
    """메모리 기반의 유동적 토큰 버킷"""
    def __init__(self, capacity: int = 100, refill_rate: float = 10.0):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens: Dict[str, float] = defaultdict(lambda: float(capacity))
        self.last_refill: Dict[str, float] = defaultdict(time.time)

    def consume(self, identity_id: str, cost: int) -> bool:
        now = time.time()
        
        elapsed = now - self.last_refill[identity_id]
        self.tokens[identity_id] = min(
            self.capacity, 
            self.tokens[identity_id] + elapsed * self.refill_rate
        )
        self.last_refill[identity_id] = now

        if self.tokens[identity_id] >= cost:
            self.tokens[identity_id] -= cost
            return True
        return False


class PayloadSanitizer:
    OS_INJECTION_PATTERN = re.compile(r"(?:;|\||&&|`|\$\()")
    TRAVERSAL_PATTERN = re.compile(r"(?:\.\./|\.\.\\|%2e%2e%2f)", re.IGNORECASE)
    PROMPT_INJECTION_KEYWORDS = [
        "ignore previous instructions", 
        "ignore all previous instructions",
        "system override", 
        "forget all instructions",
        "bypass security"
    ]

    @classmethod
    def inspect(cls, payload: Any) -> bool:
        if not payload:
            return True
            
        try:
            raw_text = json.dumps(payload).lower()
            
            if cls.OS_INJECTION_PATTERN.search(raw_text):
                log.warning("[Sanitizer] OS Command Injection signature detected.")
                return False
                
            if cls.TRAVERSAL_PATTERN.search(raw_text):
                log.warning("[Sanitizer] Path Traversal signature detected.")
                return False
                
            if any(kw in raw_text for kw in cls.PROMPT_INJECTION_KEYWORDS):
                log.warning("[Sanitizer] Prompt Injection / Jailbreak keyword detected.")
                return False
                
            return True
        except Exception as e:
            log.error(f"[Sanitizer] Failed to parse payload: {e}. Defaulting to BLOCKED.")
            return False 

class ToposGateway:
    """@desc: Compliant middleware & Adapter bridging external Ingress to the unified KernelStore"""
    def __init__(self, store: Optional[KernelLedger] = None):
        self.store = store or KernelLedger()
        self.limiter = TokenBucketLimiter(capacity=100, refill_rate=5.0)
        self.action_costs = {
            "READ_RESOURCE": 1,
            "INVOKE_TOOL": 5,
            "INJECT_QUARANTINE_RULE": 10,
            "INJECT_META_RULE": 10,
            "LOGSTREAM_BULK_INSERT": 1, # 내부 시스템 로깅 액션
            "SECURITY_TENSION_ALERT": 0  
        }

    async def authorize_ingress(self, stream: IngressLogicStream) -> bool:
        action_id = str(stream.meta.stream_id)
        action = stream.payload.parameters.get("action", stream.payload.intent.value) 
        payload = stream.payload.parameters.get("data", stream.payload.parameters)
        
        metadata = {
            "is_authenticated": stream.identity.is_authenticated,
            "stateless_token": stream.identity.stateless_token_id,
            "client_ip": stream.meta.client_ip,
            "protocol_version": stream.meta.original_protocol.value,
        }
        
        # 추가 메타데이터 병합
        if "meta" in stream.payload.parameters:
            metadata.update(stream.payload.parameters["meta"])

        return await self.authorize(action_id=action_id, action=action, payload=payload, metadata=metadata)

    async def authorize(self, action_id: str, action: str, payload: Any, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        @desc: The single choke-point for agent action validation.
        """
        if metadata is None:
            metadata = {}

        ## Step 1: Quota Check (Rate Limiting)
        identity_id = metadata.get("stateless_token") or metadata.get("client_ip") or "anonymous_agent"
        cost = self.action_costs.get(action, 1)
        
        if not self.limiter.consume(identity_id, cost):
            log.warning(f"[Gateway] BLOCKED (Quota Exceeded): Identity '{identity_id}' exhausted tokens for action '{action}'.")
            return False

        ## Step 2: Deep Content Inspection (Payload Sanitizer)
        if not PayloadSanitizer.inspect(payload):
            log.critical(f"[Gateway] BLOCKED (Semantic Breach): Malicious signature detected in payload for action '{action}'.")
            return False

        ## Step 3: Adapt & Forward to WASM Kernel
        kernel_stream = KernelLogicStream(
            id=action_id or str(uuid.uuid4()),
            action=action,
            payload=payload,
            metadata=metadata
        )

        log.debug(f"[Gateway] Forwarding stream {kernel_stream.id} to KernelStore for WASM validation.")
        try:
            sealed_kernel: Optional[SealedKernel] = await self.store.propose_and_seal(kernel_stream)
            if sealed_kernel is not None:
                log.info(f"[Gateway] AUTHORIZED: Stream {kernel_stream.id} successfully sealed into {sealed_kernel.kernel_id}.")
                return True
            else:
                if hasattr(self.store, 'role') and self.store.role == LedgerRole.FOLLOWER:
                    log.info(f"[Gateway] PROPOSED: Stream {kernel_stream.id} delegated to Mempool (FOLLOWER mode).")
                    return True
                else:
                    log.warning(f"[Gateway] BLOCKED: Stream {kernel_stream.id} rejected by WASM Kernel Spatial Fence.")
                    return False
        except Exception as e:
            log.error(f"[Gateway] Kernel pipeline failed with exception: {e}. Defaulting to BLOCKED.")
            return False

class BypassGateway(ToposGateway):
    async def authorize(self, action_id: str, action: str, payload: Any, metadata: Optional[Dict[str, Any]] = None) -> bool:
        log.debug(f"[Gateway: Bypass] Auto-authorizing action {action_id} (DEV MODE).")
        return True