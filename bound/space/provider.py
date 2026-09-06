# xphi.bound.space.provider
## @lineage: xphi.xor.space.provider
import time
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Type

from xphi.bound.space.manager import SessionContainerManager, CoreSandboxWorkspace
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

# =====================================================================
# 1. Infrastructure Data Models
# =====================================================================
@dataclass
class ProviderExecutionResult:
    """순수 인프라 관점에서의 실행 결과 (Fuel이나 도메인 로직 제외)"""
    success: bool
    exit_code: int
    stdout: str
    stderr: str
    elapsed_ms: float
    raw_metrics: Dict[str, Any] = field(default_factory=dict)
    error_detail: Optional[str] = None

@dataclass
class ResourceLimits:
    """컨테이너/프로세스에 부여할 물리적 제약 조건"""
    max_memory_bytes: Optional[int] = None
    cpu_quota: Optional[int] = None
    timeout_seconds: float = 30.0


# =====================================================================
# 2. Abstract Provider Interface
# =====================================================================
class BaseSandboxProvider(ABC):
    """
    모든 샌드박스 환경이 구현해야 하는 표준 인터페이스.
    Kernel 계층은 이 인터페이스만 바라보고 명령을 내립니다.
    """
    
    @abstractmethod
    async def provision(self, session_id: str, limits: Optional[ResourceLimits] = None) -> Any:
        """환경 할당 및 초기화 (컨테이너 생성, 폴더 마운트 등)"""
        pass

    @abstractmethod
    async def execute_code(
        self, 
        session_id: str, 
        code: str, 
        env_vars: Optional[Dict[str, str]] = None,
        limits: Optional[ResourceLimits] = None
    ) -> ProviderExecutionResult:
        """할당된 환경에서 코드를 실행하고 결과를 반환"""
        pass

    @abstractmethod
    async def teardown(self, session_id: str) -> None:
        """환경 파괴 및 자원 회수"""
        pass


# =====================================================================
# 3. Concrete Implementations
# =====================================================================
class DockerSandboxProvider(BaseSandboxProvider):
    """
    xphi.xor.space.manager의 SessionContainerManager를 활용하는 Docker 기반 프로바이더
    """
    def __init__(self, base_workdir: str = "/tmp/sandbox"):
        self.manager = SessionContainerManager()
        self.base_workdir = base_workdir
        self._workspaces: Dict[str, CoreSandboxWorkspace] = {}

    async def provision(self, session_id: str, limits: Optional[ResourceLimits] = None) -> CoreSandboxWorkspace:
        # Note: ResourceLimits는 향후 SessionContainerManager.allocate에 주입하여
        # Docker Cgroup(--memory, --cpu-quota) 설정에 활용할 수 있습니다.
        container = await self.manager.allocate(session_id, working_dir=self.base_workdir)
        workspace = CoreSandboxWorkspace(working_dir=self.base_workdir, container=container)
        self._workspaces[session_id] = workspace
        return workspace

    async def execute_code(
        self, 
        session_id: str, 
        code: str, 
        env_vars: Optional[Dict[str, str]] = None,
        limits: Optional[ResourceLimits] = None
    ) -> ProviderExecutionResult:
        workspace = self._workspaces.get(session_id)
        if not workspace:
            # 프로비저닝이 안 되어 있으면 자동 프로비저닝
            workspace = await self.provision(session_id, limits)
            
        timeout = limits.timeout_seconds if limits else 30.0
        
        # 임시 파일로 코드를 작성하여 실행 (echo 파이프라인 우회 방지)
        # 보안을 위해 실제로는 셸 이스케이프 또는 별도 entrypoint.sh 구성을 권장합니다.
        safe_code = code.replace("'", "'\\''")
        command = f"python3 -c '{safe_code}'"

        start_time = time.time()
        # 환경변수가 주입된 경우 CoreSandboxWorkspace 인스턴스에 일시 설정
        original_env = workspace.env_vars
        if env_vars:
            workspace.env_vars = {**original_env, **env_vars}
            
        try:
            result = workspace.execute_command(command, timeout=timeout)
        finally:
            # 실행 후 환경변수 복구
            workspace.env_vars = original_env

        elapsed_ms = (time.time() - start_time) * 1000

        return ProviderExecutionResult(
            success=(result.exit_code == 0 and not result.timeout_occurred),
            exit_code=result.exit_code,
            stdout=result.stdout,
            stderr=result.stderr,
            elapsed_ms=elapsed_ms,
            error_detail="Command timed out" if result.timeout_occurred else None
        )

    async def teardown(self, session_id: str) -> None:
        workspace = self._workspaces.pop(session_id, None)
        if workspace and workspace.get('container'):
            await self.manager.release(session_id, workspace.container, destroy=True)


class LocalSandboxProvider(BaseSandboxProvider):
    """
    디버깅 및 경량 테스트를 위한 Local 프로세스 기반 프로바이더 (격리 수준이 낮음)
    """
    async def provision(self, session_id: str, limits: Optional[ResourceLimits] = None) -> Any:
        log.debug(f"[LocalSandboxProvider] Provisioned local environment for {session_id}")
        return True

    async def execute_code(
        self, 
        session_id: str, 
        code: str, 
        env_vars: Optional[Dict[str, str]] = None,
        limits: Optional[ResourceLimits] = None
    ) -> ProviderExecutionResult:
        timeout = limits.timeout_seconds if limits else 30.0
        start_time = time.time()
        
        proc = await asyncio.create_subprocess_exec(
            "python3", "-c", code,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env_vars
        )
        
        try:
            async with asyncio.timeout(timeout):
                stdout, stderr = await proc.communicate()
                timeout_occurred = False
        except asyncio.TimeoutError:
            proc.kill()
            stdout, stderr = b"", b""
            timeout_occurred = True
            
        elapsed_ms = (time.time() - start_time) * 1000
        
        return ProviderExecutionResult(
            success=(proc.returncode == 0 and not timeout_occurred),
            exit_code=proc.returncode if proc.returncode is not None else -1,
            stdout=stdout.decode('utf-8', errors='replace'),
            stderr=stderr.decode('utf-8', errors='replace'),
            elapsed_ms=elapsed_ms,
            error_detail="Local execution timed out" if timeout_occurred else None
        )

    async def teardown(self, session_id: str) -> None:
        log.debug(f"[LocalSandboxProvider] Teardown local environment for {session_id}")


# =====================================================================
# 4. Provider Registry (Factory)
# =====================================================================
class SandboxProviderRegistry:
    """
    이름(Enum 또는 문자열)을 기반으로 적절한 Provider를 반환하는 레지스트리.
    어플리케이션 시작 시점에 외부에서 커스텀 프로바이더를 주입할 수도 있습니다.
    """
    def __init__(self):
        self._providers: Dict[str, BaseSandboxProvider] = {
            "docker": DockerSandboxProvider(),
            "local": LocalSandboxProvider(),
            # "wasm": WasmSandboxProvider() # 향후 추가
        }

    def register(self, env_name: str, provider: BaseSandboxProvider) -> None:
        self._providers[env_name] = provider

    def get_provider(self, env_name: str) -> BaseSandboxProvider:
        provider = self._providers.get(env_name)
        if not provider:
            raise ValueError(f"Sandbox environment '{env_name}' is not registered.")
        return provider

# 전역 레지스트리 싱글톤 (필요시 의존성 주입으로 대체 가능)
provider_registry = SandboxProviderRegistry()