# xphi.watcher.plane.regulator
import os
import sys
import time
import atexit
import logging
import datetime
from collections import defaultdict, deque
from dataclasses import replace
from typing import Dict, List, Optional, Any
from pathlib import Path

# --- Architecture & Kernel Imports ---
from xphi.arch.event.next import LogEvent, next_phase_id, EventObserver
from xphi.kernel.space.bind.resolver import resolve_path

# --- Metric & Trajectory Imports ---
from xphi.watcher.plane.metric.trajectory import Point, WindowedTrajectory, DefaultBoundLensStrategy

# --- Surface Imports ---
from xphi.watcher.plane.surface.tunnel import TunnelSurface
from xphi.watcher.plane.surface.console import ConsoleSurface
from xphi.watcher.plane.surface.file import TextFileSurface, JsonFileSurface

log = logging.getLogger("flow.meter")


# =====================================================================
# 1. Telemetry & Flow Meter Layer
# =====================================================================

class PressureMeter:
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

# Global singleton instance for Telemetry
default_telemetry = TelemetryEngine(
    window=2.0, 
    resolution_bins=10,
    burst_density_threshold=3.0,
    burst_accel_threshold=1.5
)


# =====================================================================
# 2. Regulator Layer
# =====================================================================

class PlaneRegulator:
    """@desc: Event backpressure regulator and telemetry/phase orchestrator."""
    PRIORITY_LEVELS = {"CRIT", "SIGNAL"}

    def __init__(self, threshold: float = 5.0, telemetry_engine=None):
        # Uses the default_telemetry defined above if none provided
        self.telemetry = telemetry_engine or default_telemetry
        self.threshold = threshold
        self.fold_cache: Dict[str, LogEvent] = {}
        self._observers: List[EventObserver] = []

    def handle(self, event: LogEvent):
        phase_val = event.context.get("phase") if event.context else None
        phase_str = str(phase_val if phase_val is not None else "SYSTEM")
        source_str = str(event.source_id or "UNKNOWN")
        
        # 1. 측정용 키 (메시지 배제: 정확한 출처별 부하 측정)
        telemetry_key = f"{phase_str}:{source_str}"
        
        # 2. 텔레메트리 엔진 측정 (밀도 및 가속도)
        tel_result = self.telemetry.analyze(telemetry_key)
        density_val = tel_result.get("density", 0.0)
        is_bursting = tel_result.get("is_bursting", False)
        metrics = tel_result.get("metrics", {})

        # 3. [phase_id 자동 생성 및 주입]
        acceleration = metrics.get("acceleration", 0.0)
        topo_val = int(abs(acceleration) * 100)
        press_val = int(density_val * 100)
        
        current_phase_id = next_phase_id(
            topo=topo_val, 
            press=press_val, 
            rupture=is_bursting
        )
        
        # 이벤트 객체에 측정된 데이터 덮어쓰기 (불변성 유지)
        event = replace(event, phase_id=current_phase_id, density=density_val)

        # 4. 폴딩 캐시 키 생성
        fold_key = telemetry_key if is_bursting else f"{telemetry_key}:{event.message}"

        # 5. 우선순위 레벨 즉시 방출
        if event.level in self.PRIORITY_LEVELS:
            self._notify(event)
            return

        # 6. 배압 제어 (Backpressure) 및 폴딩 로직
        if density_val > self.threshold or is_bursting:
            gain_val = 0.1 if is_bursting else self.threshold / (density_val or 1)
            enriched_context = {**(event.context or {}), **metrics}

            if fold_key in self.fold_cache:
                old_event = self.fold_cache[fold_key]
                new_fold_count = getattr(old_event, "fold_count", 1) + 1
                
                # 폭주 통폐합 시 메시지 덮어쓰기
                display_msg = f"[{source_str}] Bursting logs suppressed..." if is_bursting else old_event.message
                
                self.fold_cache[fold_key] = replace(
                    old_event, 
                    message=display_msg,
                    fold_count=new_fold_count, 
                    gain=gain_val,
                    context=enriched_context,
                    phase_id=current_phase_id # 최신 위상 상태 반영
                )
                return
                
            summary_event = replace(
                event, 
                kind="summary", 
                fold_count=1, 
                gain=gain_val,
                context=enriched_context
            )
            self.fold_cache[fold_key] = summary_event
            self._notify(summary_event)
        else:
            # 정상 트래픽
            if fold_key in self.fold_cache:
                folded_event = self.fold_cache.pop(fold_key)
                if getattr(folded_event, "fold_count", 0) > 1:
                    self._notify(folded_event)
            
            self._notify(event)

    def _notify(self, event: LogEvent):
        for observer in self._observers:
            try:
                observer.update(event)
            except Exception as e:
                sys.stderr.write(f"[PlaneRegulator-Notify Anomaly] {e}\n")
            
    def attach(self, observer: EventObserver):
        if observer not in self._observers:
            self._observers.append(observer)

    def flush(self):
        for key in list(self.fold_cache.keys()):
            folded_event = self.fold_cache.pop(key, None)
            if folded_event and getattr(folded_event, "fold_count", 0) > 1:
                self._notify(folded_event)


# =====================================================================
# 3. Ecosystem Assembly & Deployment Region
# =====================================================================

default_plane = PlaneRegulator(telemetry_engine=default_telemetry)
console_surface = ConsoleSurface(
    mode="NORMAL", 
    min_level=os.environ.get("LOG_LEVEL", "INFO").upper()
)

def _assemble_collector_network():
    global_log_level = os.environ.get("LOG_LEVEL", "INFO").upper()

    ## 터미널 출력 (콘솔)
    default_plane.attach(console_surface)

    ## 사람을 위한 텍스트 파일 저장소 (log 디렉토리)
    log_base_dir = resolve_path("log")
    text_min_level = "TRACE" if global_log_level == "TRACE" else "DEBUG"
    text_surface = TextFileSurface(base_dir=log_base_dir, min_level=text_min_level)
    default_plane.attach(text_surface)

    ## AI 분석을 위한 JSON 파일 저장소
    ailog_base_dir = resolve_path("ailog")
    json_surface = JsonFileSurface(base_dir=ailog_base_dir, min_level="INFO", unified=True)
    default_plane.attach(json_surface)

    ## 원격 파이프라인 전송 (Tunnel)
    tunnel_streamer = TunnelSurface()
    default_plane.attach(tunnel_streamer)
    
    ## 프로세스 종료 시 잔여 이벤트 Flush 등록
    atexit.register(default_plane.flush)

# Initialize the network
_assemble_collector_network()