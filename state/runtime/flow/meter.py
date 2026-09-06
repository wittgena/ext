# xphi.state.runtime.flow.meter
## @lineage: xphi.kernel.phase.runtime.flow.meter
## @lineage: kernel.phase.runtime.flow.meter
## @lineage: watcher.plane.flow.meter
import time
import datetime
import logging
from collections import defaultdict, deque
from typing import Dict, Any, List

from xphi.watcher.plane.metric.trajectory import Point, WindowedTrajectory, DefaultBoundLensStrategy

log = logging.getLogger("flow.meter")

class PressureMeter:
    """
    @role: L1 Data Collector
    @desc: Collects raw event timestamps, measures density via a sliding window, 
           and autonomously prevents memory leaks.
    """
    def __init__(self, window: float = 2.0):
        self.window = window
        self.history = defaultdict(deque)

    def record(self, key: str) -> float:
        """Records an event and returns the current density."""
        now = time.time()
        q = self.history[key]
        
        # 1. 만료된 타임스탬프 제거 (Sliding Window)
        while q and now - q[0] > self.window:
            q.popleft()
            
        # 2. 현재 이벤트 기록
        q.append(now)
        
        # 3. 가비지 컬렉션: 완전히 비어버린 큐는 딕셔너리에서 삭제하여 메모리 누수 방지
        self._prune_empty_keys()
        
        return len(q) / self.window

    def get_history(self, key: str) -> deque:
        return self.history[key]

    def _prune_empty_keys(self):
        """Removes keys with empty deques to free memory."""
        empty_keys = [k for k, v in self.history.items() if not v]
        for k in empty_keys:
            del self.history[k]


class MeterProjector:
    """
    @role: Architecture Bridge
    @desc: Converts 1D timestamp queues into multi-dimensional time-series trajectories.
    """
    def __init__(self, bins: int = 10):
        self.bins = bins

    def project(self, identity: str, timestamps: deque, window_size: float) -> WindowedTrajectory:
        now = time.time()
        start_time = now - window_size
        bin_size = window_size / self.bins
        
        binned_counts = [0.0] * self.bins
        
        # 1. 마이크로 빈(Bin) 할당 연산
        for ts in timestamps:
            if ts < start_time: 
                continue
            
            bin_idx = int((ts - start_time) / bin_size)
            if 0 <= bin_idx < self.bins:
                binned_counts[bin_idx] += 1.0
                
        # 2. Trajectory Point 변환
        points = [
            Point(
                timestamp=datetime.datetime.fromtimestamp(start_time + (i * bin_size)),
                value=count / bin_size  # 마이크로 밀도(events/sec)
            )
            for i, count in enumerate(binned_counts)
        ]
            
        return WindowedTrajectory(
            identity=identity,
            start_time=datetime.datetime.fromtimestamp(start_time),
            end_time=datetime.datetime.fromtimestamp(now),
            points=points
        )


class TelemetryEngine:
    """
    @role: Orchestrator
    @desc: Integrates Collection (Meter) -> Transformation (Projector) -> Analysis (Lens).
    """
    def __init__(
        self, 
        window: float = 2.0, 
        resolution_bins: int = 10,
        burst_density_threshold: float = 3.0,
        burst_accel_threshold: float = 1.5
    ):
        self.meter = PressureMeter(window=window)
        self.projector = MeterProjector(bins=resolution_bins)
        self.kinematic_lens = DefaultBoundLensStrategy(preset_name="kinematic")
        self.burst_density_threshold = burst_density_threshold
        self.burst_accel_threshold = burst_accel_threshold
        self._last_burst_state = False

    def analyze(self, key: str) -> Dict[str, Any]:
        """Records an event and returns diagnosed turbulence metrics."""
        current_density = self.meter.record(key)
        raw_history = self.meter.get_history(key)
        trajectory = self.projector.project(key, raw_history, self.meter.window)
        
        lens_result = self.kinematic_lens.scan(trajectory)
        metrics = lens_result.get("metrics", {})
        acceleration = metrics.get("acceleration", 0.0)
        
        is_bursting = current_density > self.burst_density_threshold and acceleration > self.burst_accel_threshold
        if is_bursting and not self._last_burst_state:
            log.warning(f"[Telemetry] 🚨 Burst Alert Triggered on '{key}' (Acc: {acceleration:.2f}) Den: {current_density:.2f})")
        elif not is_bursting and self._last_burst_state:
            log.info(f"[Telemetry] 🟢 System Stabilized on '{key}'")
            
        self._last_burst_state = is_bursting
        
        return {
            "key": key,
            "density": round(current_density, 2),
            "is_bursting": is_bursting,
            "metrics": metrics,
            "status": lens_result.get("status")
        }

## Global singleton instance
default_telemetry = TelemetryEngine(
    window=2.0, 
    resolution_bins=10,
    burst_density_threshold=3.0,
    burst_accel_threshold=1.5
)