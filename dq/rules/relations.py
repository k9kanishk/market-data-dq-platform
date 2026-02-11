from __future__ import annotations
import numpy as np
import pandas as pd
from typing import List
from .base import Rule, Issue

class CorrBreakRule(Rule):
    name = "relations.corr_break"
    def __init__(self, window: int = 60, min_abs_corr: float = 0.2):
        self.window = window
        self.min_abs_corr = min_abs_corr

    def run(self, series: pd.Series, **kwargs) -> List[Issue]:
        peer: pd.Series = kwargs.get("peer_series")
        if peer is None:
            return []
        df = pd.DataFrame({"x": series, "y": peer}).dropna().sort_index()
        if len(df) < self.window + 10:
            return []
        corr = df["x"].rolling(self.window).corr(df["y"])
        out: List[Issue] = []
        for d, c in corr.dropna().items():
            if abs(c) < self.min_abs_corr:
                sev = int(min(100, 70 + 50 * (self.min_abs_corr - abs(c)) / max(1e-6, self.min_abs_corr)))
                out.append(Issue(self.name, d, sev, "review", {"rolling_corr": float(c), "window": self.window}))
        return out

class FXTriangleRule(Rule):
    name = "relations.fx_triangle"
    def __init__(self, rel_tol: float = 0.002):
        self.rel_tol = rel_tol

    def run(self, series: pd.Series, **kwargs) -> List[Issue]:
        ab: pd.Series = kwargs.get("ab")  # EURUSD
        bc: pd.Series = kwargs.get("bc")  # USDGBP
        ac: pd.Series = kwargs.get("ac")  # EURGBP
        if ab is None or bc is None or ac is None:
            return []
        df = pd.DataFrame({"ab": ab, "bc": bc, "ac": ac}).dropna().sort_index()
        if df.empty:
            return []
        implied = df["ab"] * df["bc"]
        rel_err = (df["ac"] - implied).abs() / implied.abs().replace(0.0, np.nan)

        out: List[Issue] = []
        for d, e in rel_err.dropna().items():
            if e > self.rel_tol:
                sev = int(min(100, 60 + 200 * float(e)))
                out.append(Issue(self.name, d, sev, "source_switch", {"rel_error": float(e), "rel_tol": self.rel_tol}))
        return out
