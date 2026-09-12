# xphi.watcher.plane.surface.console
## @lineage: watcher.plane.surface.console
import os
import json
import time
import asyncio
import sys
import atexit
from dataclasses import replace, asdict
from typing import Dict, List, Protocol, Optional
from pathlib import Path
from xphi.arch.event.next import LogEvent
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.arch.event.next import EventObserver

class ConsoleSurface(EventObserver):
    """@desc: Handles standard output with level-based filtering and rich formatting."""
    BYPASS_LEVELS = {"CRIT", "SIGNAL"}
    
    LEVEL_WEIGHTS = {
        "TRACE": 10,
        "DEBUG": 20,
        "INFO": 30,
        "WARN": 40,
        "ERROR": 50,
        "CRIT": 60,
        "SIGNAL": 70
    }

    def __init__(self, mode: str = "NORMAL", min_level: str = "INFO"):
        self.mode = mode.upper()
        self.min_level = min_level.upper()
        self.min_weight = self.LEVEL_WEIGHTS.get(self.min_level, 30)

    def update(self, event: LogEvent):
        event_level = str(event.level or "INFO").upper()
        
        if event_level not in self.BYPASS_LEVELS:
            current_weight = self.LEVEL_WEIGHTS.get(event_level, 30)
            if current_weight < self.min_weight:
                return

        if self.mode == "FULL":
            print(f"DEBUG_EVENT: {event}")
            return

        p_mark = "🔥" if event.kind == "summary" else ""
        gain_val = getattr(event, "gain", None)
        gain = f" [G:{gain_val:.1f}]" if gain_val is not None and gain_val < 1.0 else ""
        
        acc_val = event.context.get("acceleration") if event.context else None
        acc_str = f" [Acc:{acc_val:.1f}]" if acc_val else ""
        
        fold_val = getattr(event, "fold_count", 0)
        fold = f" (x{fold_val})" if fold_val and fold_val > 1 else ""
        
        phase_val = event.context.get("phase") if event.context else None
        phase_str = str(phase_val if phase_val is not None else "SYSTEM")
        kind_str = str(event.kind or "LOG").upper()
        source_str = str(event.source_id or "UNKNOWN")

        if self.mode == "SLIM":
            print(f"[{event_level:^5}] {source_str}: {event.message}{fold}")
        elif self.mode == "MINIMAL":
            print(f"{event.message}{fold}")
        else:
            try:
                prefix = f"T-{int(event.tick):04d}" if getattr(event, "tick", None) is not None else f"{kind_str:^5}"
            except (ValueError, TypeError):
                prefix = f"{kind_str:^5}"
                
            print(f"{prefix}{p_mark}| {phase_str:^6} | {event_level:^5} | {gain}{acc_str} {source_str}: {event.message}{fold}")