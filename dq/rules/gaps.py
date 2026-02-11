from __future__ import annotations
from typing import List, Set, Optional
from datetime import date
import pandas as pd
from .base import Rule, Issue

class MissingBdaysRule(Rule):
    name = "gaps.missing_bdays"

    def run(self, series: pd.Series, **kwargs) -> List[Issue]:
        expected: Optional[Set[date]] = kwargs.get("expected_dates")
        s = series.dropna().sort_index()
        if s.empty:
            return []
        if expected is None:
            expected = set(pd.bdate_range(min(s.index), max(s.index)).date)
        observed = set(s.index)
        missing = sorted(d for d in expected if d not in observed)

        return [
            Issue(self.name, d, 55, "interpolate", {"reason": "missing_expected_date"})
            for d in missing
        ]

class StaleRule(Rule):
    name = "gaps.stale"

    def __init__(self, min_streak: int = 3, atol: float = 0.0):
        self.min_streak = min_streak
        self.atol = atol

    def run(self, series: pd.Series, **kwargs) -> List[Issue]:
        s = series.dropna().sort_index()
        if len(s) < self.min_streak + 1:
            return []
        unchanged = (s.diff().abs() <= self.atol).fillna(False)

        out: List[Issue] = []
        streak = 0
        for d, flag in unchanged.items():
            if flag:
                streak += 1
                if streak >= self.min_streak:
                    sev = int(min(100, 30 + 12 * streak))
                    out.append(Issue(self.name, d, sev, "review", {"streak": streak}))
            else:
                streak = 0
        return out
