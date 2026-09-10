# xphi.arch.model.edge.receipt
from typing import Any, ClassVar, List, Optional, Dict
from pydantic import BaseModel, ConfigDict, Field

class AgentMandateRequest(BaseModel):
    """에이전트가 DPHI에 제출하는 오프체인 과금 허용 서명 (EIP-712/AP2)"""
    client_id: str = Field(..., description="Agent DID or Wallet Address")
    max_spend_usdc: str = Field(..., description="최대 허용 과금액 (예: '100.0')")
    expiration_ts: int = Field(..., description="서명 만료 Timestamp")
    signature: str = Field(..., description="Agent의 프라이빗 키로 서명된 무결성 증명")

class CapabilityReceiptResponse(BaseModel):
    receipt_id: str = Field(..., description="발급된 X402 영수증 해시 (Capability Token)")
    status: str = Field(..., description="ACTIVE 또는 REJECTED")
    budget_usdc: str = Field(..., description="승인된 롤업 내부 예산")
    issued_at: str = Field(..., description="발급 시간 (ISO-8601)")

class SandboxIntent(BaseModel):
    client_id: str
    responder_id: Optional[str] = Field(default=None, description="타겟 실행 노드 ID (없을 시 Gateway가 할당)")
    action: str
    source_code: str
    max_fuel: int = Field(..., description="최대 허용 CPU 사이클 (가스 리밋)")
    signature: str

class AuditReceipt(BaseModel):
    receipt_id: str
    receipt_type: str
    status: str
    fuel_consumed: int
    metered_cost_usd: float
    state_root: str
    audit_trail: List[str]

class AuditEvent(BaseModel):
    message: str
    actor: str | None = None
    action: str | None = None
    source: str | None = None
    target: str | None = None
    status: str | None = None

class AuditLogRequest(BaseModel):
    event: AuditEvent
    verbose: bool = False
    sign_local: bool = False

class AuditEnvelope(BaseModel):
    event: AuditEvent
    received_at: str

class AuditResult(BaseModel):
    envelope: AuditEnvelope
    hash: str
    membership_proof: str | None = None
    consistency_proof: list[str] | None = None

class AuditLogResponse(BaseModel):
    request_id: str
    request_time: str | None = None
    response_time: str | None = None
    status: str
    result: AuditResult

class BilledExecutionRequest(BaseModel):
    sandbox_schema: Dict[str, Any]
    context_depth: int = 2
    target_entry: str

class BilledExecutionResponse(BaseModel):
    status: str
    tier_applied: str
    fuel_billed: int
    billed_cost_usd: float
    reason: Optional[str] = None

class KernelExecutionRecord(BaseModel):
    action: str = "record_agent_execution"
    receipt_id: str
    repos: Dict[str, str]
    signatures: List[str]
    timestamp: float

class KernelOtlpRecord(BaseModel):
    action: str = "seal_otlp_transaction"
    content_hash: str
    metrics_summary: Dict[str, Any]
    receipt_ref: Optional[str] = None

class KernelLedgerAppendRecord(BaseModel):
    stream_name: str
    timestamp: int
    events: List[Dict[str, Any]]

class LogstEvent(BaseModel):
    timestamp: str = Field(description="ISO 8601 format timestamp")
    level: str | None = Field(default=None, description="Log severity level")
    message: str | None = Field(default=None, description="Log message content")
    trace_id: str | None = Field(default=None, description="Trace identifier")
    model_config: ClassVar[ConfigDict] = ConfigDict(extra="allow")

class LogstUsage(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

class LogstResponseBody(BaseModel):
    usage: LogstUsage | None = None
    model_config: ClassVar[ConfigDict] = ConfigDict(extra="allow")

class LogstRequest(BaseModel):
    time: str
    uri: str
    verb: str
    api_version: str | None = None
    ip_address: str | None = None
    headers: dict[str, str] | None = None
    body: Any = None

class LogstResponse(BaseModel):
    time: str
    status: int
    body: LogstResponseBody | None = None

class LogstEventPayload(BaseModel):
    request: LogstRequest
    response: LogstResponse
    user_id: str | None = None
    company_id: str | None = None
    metadata: dict[str, Any] | None = None

class KeyValue(BaseModel):
    key: str
    value: dict[str, Any]
    
    @property
    def get_value(self) -> Any:
        if not self.value: return None
        return next(iter(self.value.values()), None)

class LogRecord(BaseModel):
    timeUnixNano: str
    severityNumber: Optional[int] = None
    severityText: Optional[str] = None
    body: dict[str, Any] = Field(default_factory=dict)
    attributes: List[KeyValue] = Field(default_factory=list)
    traceId: Optional[str] = None
    spanId: Optional[str] = None

class ScopeLogs(BaseModel):
    scope: dict[str, Any] = Field(default_factory=dict)
    logRecords: List[LogRecord] = Field(default_factory=list)

class ResourceLogs(BaseModel):
    resource: dict[str, Any] = Field(default_factory=dict)
    scopeLogs: List[ScopeLogs] = Field(default_factory=list)

class ExportLogsServiceRequest(BaseModel):
    resourceLogs: List[ResourceLogs]