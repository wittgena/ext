# xphi.watcher.plane.surface.tunnel
## @lineage: watcher.plane.surface.tunnel
import json
import time
import sys
import threading
import queue
from dataclasses import asdict
from typing import Optional
from xphi.arch.event.next import LogEvent
from xphi.arch.event.next import EventObserver

class TunnelSurface(EventObserver):
    def __init__(self, max_batch_size: int = 50, flush_interval: float = 0.2):
        self.max_batch_size = max_batch_size
        self.flush_interval = flush_interval
        self._queue = queue.Queue()
        self._worker_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._ensure_worker()

    def _ensure_worker(self):
        with self._lock:
            if self._worker_thread is None or not self._worker_thread.is_alive():
                self._worker_thread = threading.Thread(
                    target=self._publish_worker, 
                    daemon=True,
                    name="TunnelSurfaceWorker"
                )
                self._worker_thread.start()

    def _publish_worker(self):
        """백그라운드에서 동작하며, 설정된 버퍼 규칙에 따라 Redis Pipeline으로 로그를 묶어 전송합니다."""
        from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
        tunnel = None
        
        while True:
            try:
                if tunnel is None:
                    tunnel = TunnelFactory.get_sync()
                
                batch = []
                try:
                    channel, msg = self._queue.get(timeout=self.flush_interval)
                    batch.append((channel, msg))
                except queue.Empty:
                    continue

                start_time = time.time()
                while len(batch) < self.max_batch_size:
                    elapsed = time.time() - start_time
                    remaining = self.flush_interval - elapsed
                    if remaining <= 0:
                        break
                        
                    try:
                        ch, m = self._queue.get(timeout=remaining)
                        batch.append((ch, m))
                    except queue.Empty:
                        break
                
                if batch:
                    pipeline = tunnel.pipeline()
                    for ch, m in batch:
                        pipeline.publish(ch, m)
                    pipeline.execute()
                    
                    for _ in range(len(batch)):
                        self._queue.task_done()

            except Exception as e:
                sys.stderr.write(f"[TunnelSurface Worker Anomaly] {e}\n")
                tunnel = None
                time.sleep(1.0)

    def update(self, event: LogEvent):
        flow_id = event.context.get("flow_id") or "global"
        if flow_id == "global":
            return

        channel = f"log:{flow_id}"
        msg = json.dumps(asdict(event), ensure_ascii=False)
        self._ensure_worker()
        self._queue.put((channel, msg))