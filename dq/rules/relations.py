from __future__ import annotations
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

    def __init__(
        self,
        threshold_abs_pct: float = 0.005,
        consecutive: int = 3,
        rule_suffix: str | None = None,
    ):
        self.threshold_abs_pct = threshold_abs_pct
        self.consecutive = max(1, int(consecutive))
        self.rule_suffix = rule_suffix

    def run(self, series: pd.Series, **kwargs) -> List[Issue]:
        ab: pd.Series = kwargs.get("ab")  # EURUSD
        bc: pd.Series = kwargs.get("bc")  # USDGBP (GBP per USD)
        ac: pd.Series = kwargs.get("ac")  # EURGBP

        if ab is None or bc is None or ac is None:
            return []

        def _dedup(s: pd.Series) -> pd.Series:
            s = s.dropna().sort_index()
            if not s.index.is_unique:
                s = s.groupby(level=0).last()
            return s

        ab, bc, ac = _dedup(ab), _dedup(bc), _dedup(ac)

        df = pd.DataFrame({"ab": ab, "bc": bc, "ac": ac}).dropna().sort_index()
        if df.empty:
            return []

        implied = df["ab"] * df["bc"]
        abs_pct = (implied / df["ac"] - 1.0).abs()

        rule = self.name
        if self.rule_suffix:
            rule = f"{rule}.{self.rule_suffix}"

        out: List[Issue] = []
        breach = abs_pct > self.threshold_abs_pct
        streak = breach.copy()
        for _ in range(self.consecutive - 1):
            streak = streak & streak.shift(1).fillna(False)

        bad = abs_pct[streak]
        for d, v in bad.items():
            ratio = float(v / self.threshold_abs_pct)
            sev = int(min(100, 60 + 40 * min(1.0, (ratio - 1.0) / 4.0)))  # 60 @ 1x, 100 @ 5x
            out.append(
                Issue(
                    rule,
                    d,
                    sev,
                    "source_switch",
                    {
                        "abs_pct": float(v),
                        "threshold": float(self.threshold_abs_pct),
                        "ratio": ratio,
                        "consecutive": self.consecutive,
                        "implied": float(implied.loc[d]),
                        "observed": float(df["ac"].loc[d]),
                    },
                )
            )
        return out
