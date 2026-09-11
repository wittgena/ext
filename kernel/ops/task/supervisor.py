# xphi.kernel.ops.task.supervisor
from __future__ import annotations
import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Callable, List, Tuple, Any, Optional, Set, Protocol

from xphi.arch.event.psi import PsiEvent
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter(__name__)
ErrorHandler = Callable[[asyncio.Task[Any], BaseException], None]

class MountableDaemon(Protocol):
    name: str
    def start(self) -> Awaitable[asyncio.Task]: ...
    async def stop(self) -> None: ...

class TaskSupervisor:
    def __init__(self, *, source: str) -> None:
        self._source = source
        self._tasks: set[asyncio.Task[Any]] = set()
        self._daemons: set[MountableDaemon] = set()
        self._closed = False
        self._error_handlers: list[ErrorHandler] = []

    def add_error_handler(self, handler: ErrorHandler) -> None:
        self._error_handlers.append(handler)

    def get_active_tasks(self) -> list[asyncio.Task[Any]]:
        return list(self._tasks)

    def create(
        self,
        coroutine: Awaitable[Any],
        *,
        name: str | None = None,
        on_error: ErrorHandler | None = None,
    ) -> asyncio.Task[Any]:
        if self._closed:
            msg = f"TaskSupervisor for {self._source} already closed"
            raise RuntimeError(msg)
        
        task = asyncio.create_task(coroutine, name=name)
        self._tasks.add(task)
        task.add_done_callback(lambda t: self._on_done(t, on_error))
        return task

    def create_thread_task(
        self,
        func: Callable[..., Any],
        *args: Any,
        name: str | None = None,
        on_error: ErrorHandler | None = None,
    ) -> asyncio.Task[Any]:
        """@cpu_bound: 동기 함수(WASM 등)를 이벤트 루프 블로킹 없이 스레드 풀에서 실행"""
        coro = asyncio.to_thread(func, *args)
        return self.create(coro, name=name, on_error=on_error)

    def mount_daemon(self, daemon: MountableDaemon, on_error: ErrorHandler | None = None) -> asyncio.Task:
        if self._closed:
            raise RuntimeError(f"Cannot mount daemon {daemon.name}: Supervisor closed.")
        
        async def _daemon_anchored_runner():
            inner_task = await daemon.start()
            await inner_task

        self._daemons.add(daemon)
        task = self.create(
            _daemon_anchored_runner(), 
            name=f"Daemon-{daemon.name}", 
            on_error=on_error
        )
        log.info(f"[Supervisor] Mounted Daemon: {daemon.name}")
        return task

    def _on_done(self, task: asyncio.Task[Any], on_error: ErrorHandler | None) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
            
        try:
            task.result()
        except Exception as exc:
            handled = False
            if on_error is not None:
                try:
                    on_error(task, exc)
                    handled = True
                except Exception:
                    log.exception("Error in %s task-specific error handler", self._source)
                    
            if not handled:
                for handler in self._error_handlers:
                    try:
                        handler(task, exc)
                        handled = True
                    except Exception:
                        log.exception("Error in %s supervisor error handler", self._source)
                        
            if not handled:
                log.exception(f"Unhandled error completely isolated in {self._source} task: {task.get_name()}")

    async def shutdown(self) -> None:
        self._closed = True
        if self._daemons:
            log.info(f"[Supervisor] Gracefully stopping {len(self._daemons)} daemons concurrently...")
            stop_tasks = []
            for daemon in self._daemons:
                stop_tasks.append(asyncio.create_task(daemon.stop(), name=f"Stop-{daemon.name}"))
            await asyncio.gather(*stop_tasks, return_exceptions=True)
            self._daemons.clear()
        
        if not self._tasks:
            return
        
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
            
        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        log.info(f"[Supervisor] {self._source} shutdown complete.")


class Dispatcher:
    def __init__(
        self,
        supervisor: TaskSupervisor,
        default_handler: Optional[Callable] = None,
        executor=None,
        actuator=None,
        emitter=None,
        broadcaster=None,
    ):
        self.supervisor = supervisor
        self.default_handler = default_handler
        self.executor = executor
        self.actuator = actuator
        self.emitter = emitter
        self.broadcaster = broadcaster
        self._routes: List[Tuple[Callable[[PsiEvent], bool], Callable]] = []

    def add_route(self, predicate: Callable[[PsiEvent], bool], handler: Callable):
        self._routes.append((predicate, handler))

    def _resolve_handler(self, psi: PsiEvent) -> Callable | None:
        for predicate, route_handler in self._routes:
            if predicate(psi):
                return route_handler
        return self.default_handler

    def dispatch(self, psi: PsiEvent):
        """큐 대기 없이 즉시 핸들러를 결정하고, 파이프라인의 실행을 Supervisor에 위임합니다."""
        target_handler = self._resolve_handler(psi)
        if not target_handler:
            log.warn(f"[Dispatcher] No handler resolved for {psi.symbol}")
            return

        task_name = f"Dispatch-{psi.symbol}"
        self.supervisor.create(self._process_flow(target_handler, psi), name=task_name)

    async def _process_flow(self, handler: Callable, psi: PsiEvent):
        """기존 Dispatcher가 담당하던 Executor -> Handler -> Actuator 로직을 복원"""
        try:
            if self.executor:
                psi_batch = await self.executor.execute(psi)
            else:
                psi_batch = [psi]

            for p in psi_batch:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        result = await handler(p)
                    else:
                        result = await asyncio.to_thread(handler, p)

                    if result and self.actuator:
                        await self.actuator.actuate_psi(p)

                    if result and self.broadcaster:
                        self.broadcaster.broadcast(result)

                    if self.emitter:
                        await self.emitter.emit_psi(p)
                except Exception as inner_e:
                    log.error(f"[Dispatcher:ItemError] Failed on {p.symbol}: {type(inner_e).__name__}: {inner_e}")
                    raise inner_e
        except Exception as e:
            log.error(f"[Dispatcher:FlowError] Pipeline broken for {psi.symbol}: {type(e).__name__}: {e}")
            raise