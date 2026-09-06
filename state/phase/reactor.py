# xphi.state.phase.reactor
## @lineage: xphi.kernel.phase.reactor
## @lineage: kernel.phase.reactor
import asyncio
import sys
import os
import signal
import logging
from typing import Callable, Coroutine, Any, Optional
import traceback

log = logging.getLogger("kernel.reactor")

class PhaseReactor:
    _policy_applied = False
    _pinned_core = None

    @classmethod
    def _apply_kernel_optimizations(cls) -> None:
        if cls._policy_applied:
            return

        worker_idx = int(os.environ.get("DPHI_WORKER_IDX", os.getpid()))
        if sys.platform == 'darwin':
            log.info("[Reactor] ⚡ macOS detected. Native CPU Affinity pinning is bypassed (OS constraint).")
            cls._pinned_core = f"Virtual-{worker_idx}"
        else:
            if hasattr(os, 'sched_setaffinity') and hasattr(os, 'sched_getaffinity'):
                try:
                    available_cores = list(os.sched_getaffinity(0))
                    if available_cores:
                        target_core = available_cores[worker_idx % len(available_cores)]
                        os.sched_setaffinity(0, {target_core})
                        cls._pinned_core = target_core
                        log.info(f"[Reactor] ⚡ CPU Affinity Pinned to Core: {target_core} (Idx: {worker_idx})")
                except Exception as e:
                    log.warning(f"[Reactor] ⚠️ Failed to pin CPU: {e}")

        if sys.platform not in ('win32', 'cygwin', 'cli'):
            try:
                import uvloop
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
                log.info("[Reactor] 🚀 UVLoop (epoll) Policy injected.")
            except ImportError:
                log.warning("[Reactor] ⚠️ UVLoop unavailable. Using standard asyncio.")
                
        cls._policy_applied = True

    @classmethod
    def _global_exception_handler(cls, loop: asyncio.AbstractEventLoop, context: dict) -> None:
        # 1. 기본 메시지 추출
        msg = context.get("message", "Unhandled exception in event loop")
        exc = context.get("exception")

        # 2. 메타데이터 안전 추출 (가독성을 위한 포맷팅)
        meta = {}
        for k, v in context.items():
            if k in ("message", "exception"):
                continue
            
            # Task 식별자 추출
            if k == "task":
                if hasattr(v, "get_name"):
                    meta[k] = f"Task({v.get_name()})"
                elif hasattr(v, "get_coro"):
                    meta[k] = f"Coroutine({v.get_coro().__qualname__})"
                else:
                    meta[k] = str(v)
            
            # Future, Handle, Protocol 등 기타 객체의 타입과 메모리 주소를 포함한 문자열화
            elif k in ("future", "handle", "protocol", "transport"):
                meta[k] = repr(v)
                
            # source_traceback: 'Task was destroyed' 에러 시 태스크가 생성된 위치 추적
            elif k == "source_traceback":
                meta[k] = "\n" + "".join(traceback.format_list(v))
            else:
                meta[k] = str(v)

        # 3. 로깅 메시지 조합
        log_msg = f"💥 {msg}"
        if meta:
            # 딕셔너리를 예쁘게 줄바꿈하여 출력 (가독성 극대화)
            meta_str = " | ".join(f"{k}: {v}" for k, v in meta.items())
            log_msg += f"\n    [Context] {meta_str}"

        # 4. 레벨 결정 및 출력
        # Unclosed 관련 경고는 ERROR 레벨로 낮춤 (애플리케이션 크래시가 아니므로)
        if "Unclosed" in msg or "Task was destroyed" in msg:
            if exc:
                log.error(f"[Reactor] {log_msg}", exc_info=exc)
            else:
                log.error(f"[Reactor] {log_msg}")
        else:
            if exc:
                log.critical(f"[Reactor] {log_msg}", exc_info=exc)
            else:
                log.critical(f"[Reactor] {log_msg}")


    @classmethod
    async def _lifecycle_manager(
        cls, 
        main_coro_func: Callable[[], Coroutine[Any, Any, None]],
        teardown_hook: Optional[Callable[[], Coroutine[Any, Any, None]]] = None,
        shutdown_timeout: float = 10.0
    ) -> None:
        loop = asyncio.get_running_loop()
        
        # [중요] 디버그 모드를 켜야 source_traceback이 context에 포함됩니다.
        if os.environ.get("DPHI_DEBUG") == "1":
            loop.set_debug(True)
            
        loop.set_exception_handler(cls._global_exception_handler)
        shutdown_event = asyncio.Event()
        
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, shutdown_event.set)
            except (NotImplementedError, RuntimeError):
                pass

        main_task = asyncio.create_task(main_coro_func(), name="Reactor-Main")
        shutdown_waiter = asyncio.create_task(shutdown_event.wait(), name="Reactor-Shutdown-Waiter")

        try:
            done, pending = await asyncio.wait([main_task, shutdown_waiter], return_when=asyncio.FIRST_COMPLETED)
            if shutdown_waiter in done:
                log.info("\n[Reactor] 🛑 OS Signal received. Initiating graceful shutdown...")
                main_task.cancel()
                try:
                    await asyncio.wait_for(main_task, timeout=shutdown_timeout)
                except asyncio.TimeoutError:
                    pass
                except asyncio.CancelledError:
                    pass
            else:
                shutdown_waiter.cancel()

        except asyncio.CancelledError:
            log.warning("\n[Reactor] 🛑 Reactor itself was cancelled. Cascading shutdown...")
            main_task.cancel()
        finally:
            if teardown_hook:
                log.info("[Reactor] 🧹 Executing user-defined teardown hook...")
                try:
                    await teardown_hook()
                except Exception as e:
                    pass

            log.info("[Reactor] 🏁 Lifecycle terminated cleanly.")

    @classmethod
    def ignite(
        cls, 
        main_coro_func: Callable[[], Coroutine[Any, Any, None]],
        teardown_hook: Optional[Callable[[], Coroutine[Any, Any, None]]] = None,
        shutdown_timeout: float = 10.0
    ) -> None:
        cls._apply_kernel_optimizations()
        policy_name = asyncio.get_event_loop_policy().__class__.__name__
        pin_status = f"Core {cls._pinned_core}" if cls._pinned_core is not None else "Floating"
        log.info(f"[Reactor] Ignition... Loop: {policy_name} | CPU: {pin_status}")
        
        try:
            asyncio.run(cls._lifecycle_manager(main_coro_func, teardown_hook, shutdown_timeout))
        except KeyboardInterrupt:
            pass