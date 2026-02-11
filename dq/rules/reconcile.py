from __future__ import annotations
import pandas as pd
from typing import List
from .base import Rule, Issue

class ReconcileRule(Rule):
    name = "reconcile.abs_pct"

    def __init__(self, abs_tol: float = 0.0005, pct_tol: float = 0.002):
        self.abs_tol = abs_tol
        self.pct_tol = pct_tol

    def run(self, series: pd.Series, **kwargs) -> List[Issue]:
        other: pd.Series = kwargs.get("other_series")
        if other is None:
            return []
        df = pd.DataFrame({"a": series, "b": other}).dropna().sort_index()
        if df.empty:
            return []

        abs_diff = (df["a"] - df["b"]).abs()
        pct_diff = abs_diff / df["a"].abs().replace(0.0, pd.NA)

        out: List[Issue] = []
        for d in df.index:
            ad = float(abs_diff.loc[d])
            pdiff = float(pct_diff.loc[d]) if pd.notna(pct_diff.loc[d]) else ad
            if ad > self.abs_tol and pdiff > self.pct_tol:
                sev = int(min(100, 70 + 30 * min(1.0, pdiff / (5 * self.pct_tol))))
                out.append(Issue(
                    self.name, d, sev, "source_switch",
                    {"abs_diff": ad, "pct_diff": pdiff, "abs_tol": self.abs_tol, "pct_tol": self.pct_tol}
                ))
        return out
