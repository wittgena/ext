# xphi.watcher.plane.surface.file
## @lineage: watcher.plane.surface.file
import os
import json
import time
import sys
from pathlib import Path
from dataclasses import asdict
from datetime import datetime, timezone
from xphi.arch.event.next import LogEvent
from xphi.arch.event.next import EventObserver
from xphi.watcher.plane.surface.console import ConsoleSurface

def _safe_json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)

class TextFileSurface(EventObserver):
    """
    @role: Human-Readable File Logger
    @desc: 개발자가 터미널이나 에디터에서 직접 열어보고 흐름을 파악하기 위한 순수 텍스트(.log) 서페이스
    """
    def __init__(self, base_dir: str | Path, min_level: str = "DEBUG"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.min_level = min_level.upper()
        self.min_weight = ConsoleSurface.LEVEL_WEIGHTS.get(self.min_level, 20)

    def update(self, event: LogEvent):
        event_level = str(event.level or "INFO").upper()
        current_weight = ConsoleSurface.LEVEL_WEIGHTS.get(event_level, 30)
        
        if event_level not in ConsoleSurface.BYPASS_LEVELS and current_weight < self.min_weight:
            return

        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        ctx_phase = event.context.get("phase") if event.context else None
        phase_str = str(ctx_phase if ctx_phase is not None else "SYSTEM")
        
        safe_phase = "".join(c for c in phase_str if c.isalnum() or c in "_-").lower() or "system"
        target_file = self.base_dir / f"{safe_phase}.log"
        
        source_str = str(event.source_id or "UNKNOWN")
        fold_val = getattr(event, "fold_count", 0)
        fold = f" (x{fold_val})" if fold_val and fold_val > 1 else ""
        
        # 개발자가 읽기 편한 전통적인 텍스트 포맷
        log_line = f"[{timestamp}] [{event_level:^5}] [{phase_str:^8}] {source_str}: {event.message}{fold}\n"
        
        try:
            with open(target_file, "a", encoding="utf-8") as f:
                f.write(log_line)
        except Exception as e:
            sys.stderr.write(f"[TextFileSurface Error] {e}\n")


class JsonFileSurface(EventObserver):
    """
    @role: Machine-Readable Event Store
    @desc: AI 요약(LogtailDaemon) 및 향후 데이터 파이프라인(ES) 연동을 위한 구조화 데이터(.jsonl) 서페이스
    """
    def __init__(self, base_dir: str | Path, min_level: str = "DEBUG", unified: bool = False):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.min_level = min_level.upper()
        self.min_weight = ConsoleSurface.LEVEL_WEIGHTS.get(self.min_level, 20)
        self.unified = unified

    def update(self, event: LogEvent):
        event_level = str(event.level or "INFO").upper()
        current_weight = ConsoleSurface.LEVEL_WEIGHTS.get(event_level, 30)
        
        if event_level not in {"CRIT", "SIGNAL"} and current_weight < self.min_weight:
            return

        if self.unified:
            target_file = self.base_dir / "unified.jsonl"
        else:
            ctx_phase = event.context.get("phase") if event.context else None
            phase_str = str(ctx_phase if ctx_phase is not None else "SYSTEM")
            safe_phase = "".join(c for c in phase_str if c.isalnum() or c in "_-").lower() or "system"
            target_file = self.base_dir / f"{safe_phase}.jsonl"

        event_dict = asdict(event)
        event_dict["@timestamp"] = datetime.now(timezone.utc).isoformat()
        
        try:
            log_line = json.dumps(event_dict, ensure_ascii=False, default=_safe_json_serializer) + "\n"
            with open(target_file, "a", encoding="utf-8") as f:
                f.write(log_line)
        except Exception as e:
            sys.stderr.write(f"[JsonFileSurface Error] {e}\n")