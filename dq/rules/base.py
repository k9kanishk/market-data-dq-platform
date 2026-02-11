from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List
import pandas as pd

@dataclass(frozen=True)
class Issue:
    rule: str
    obs_date: date
    severity: int
    suggested_action: str
    details: Dict[str, Any]

class Rule:
    name: str
    def run(self, series: pd.Series, **kwargs) -> List[Issue]:
        raise NotImplementedError
