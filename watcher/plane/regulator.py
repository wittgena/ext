# xphi.watcher.plane.regulator
## @lineage: watcher.plane.regulator
import os
import sys
import atexit
from dataclasses import replace
from typing import Dict, List, Optional
from pathlib import Path

from xphi.arch.event.next import LogEvent, next_phase_id
from xphi.kernel.space.bind.resolver import resolve_path

from xphi.state.phase.flow.meter import default_telemetry
from xphi.watcher.plane.observer.event import EventObserver
from xphi.watcher.plane.surface.tunnel import TunnelSurface
from xphi.watcher.plane.surface.console import ConsoleSurface
from xphi.watcher.plane.surface.file import TextFileSurface, JsonFileSurface

class PlaneRegulator:
    """@desc: Event backpressure regulator and telemetry/phase orchestrator."""
    PRIORITY_LEVELS = {"CRIT", "SIGNAL"}

    def __init__(self, threshold: float = 5.0, telemetry_engine=None):
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
        # 밀도(Density) -> 압력(Press)
        # 가속도(Acceleration) -> 위상/궤적(Topo)
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

        # 4. 폴딩 캐시 키 생성 (폭주 시엔 소스 단위 통폐합, 평시엔 메시지 단위 폴딩)
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

"""Ecosystem Collector Network: Assembly & Deployment Region"""
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

    ailog_base_dir = resolve_path("ailog")
    json_surface = JsonFileSurface(base_dir=ailog_base_dir, min_level="INFO", unified=True)
    default_plane.attach(json_surface)

    tunnel_streamer = TunnelSurface()
    default_plane.attach(tunnel_streamer)
    atexit.register(default_plane.flush)

_assemble_collector_network()