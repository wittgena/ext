# xphi.arch.model.edge.stream
## @lineage: xphi.watcher.server.stream.schema
from pydantic import BaseModel, Field
from typing import Any, Dict
from enum import Enum
from uuid import UUID, uuid4

class ProtocolSource(str, Enum):
    MCP_1_0 = "1.0"
    MCP_2_0 = "2.0"
    UNKNOWN = "unknown"

class ActionIntent(str, Enum):
    INITIALIZE = "initialize"
    INVOKE_TOOL = "invoke_tool"
    READ_RESOURCE = "read_resource"

class StreamIdentity(BaseModel):
    is_authenticated: bool = Field(default=False)
    stateless_token_id: str | None = None
    granted_scopes: list[str] = Field(default_factory=list)

class StreamMetadata(BaseModel):
    stream_id: UUID = Field(default_factory=uuid4)
    original_protocol: ProtocolSource
    content_length: int
    client_ip: str

class LogicPayload(BaseModel):
    intent: ActionIntent
    parameters: Dict[str, Any] = Field(default_factory=dict)

class LogicStream(BaseModel):
    meta: StreamMetadata
    identity: StreamIdentity
    payload: LogicPayload
    model_config = {"frozen": True, "extra": "forbid"}