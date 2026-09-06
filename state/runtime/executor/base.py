# xphi.state.runtime.executor.base
## @lineage: xphi.kernel.phase.runtime.executor.base
## @lineage: xphi.arch.contract.executor
from __future__ import annotations
import asyncio
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any, Protocol
from xphi.arch.event.psi import PsiType, PsiEvent
from xphi.watcher.plane.emitter import get_emitter

class BaseExecutor(ABC):
    """@executor: ψ → {ψ'} (execution / dispersion / transduction)"""
    def __init__(self):
        self.log = get_emitter("base.executor", phase="EXECUTION")

    @abstractmethod
    async def execute(self, psi: PsiType) -> List[PsiType]:
        pass