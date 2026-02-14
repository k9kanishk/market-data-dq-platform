from __future__ import annotations

import pandas as pd
import numpy as np
from typing import List, Optional
from .base import Rule, Issue


class ReconcileRule(Rule):
    """
    Vendor-vendor reconciliation.

    - For FX / equities / commodities: compare RETURN differences (robust to close-time differences)
    - For rates: compare LEVEL differences (yields, etc.)
    """

    name = "reconcile.abs_pct"

    def __init__(
        self,
        # Level thresholds (used for rates, or if forced)
        abs_tol: float = 0.0005,
        pct_tol: float = 0.002,
        # Returns threshold (used for fx/equities/commodities by default)
        ret_tol: float = 0.003,  # 30 bps return-diff
        # Persistence filter: require N consecutive breaches
        consecutive: int = 3,
        # Prefer log-returns when series is strictly positive
        use_log_returns: bool = True,
    ):
        self.abs_tol = abs_tol
        self.pct_tol = pct_tol
        self.ret_tol = ret_tol
        self.consecutive = max(1, int(consecutive))
        self.use_log_returns = bool(use_log_returns)

    @staticmethod
    def _dedup(s: pd.Series) -> pd.Series:
        s = s.dropna().sort_index()
        if not s.index.is_unique:
            # keep last observation per date (typical vendor overwrite behavior)
            s = s.groupby(level=0).last()
        return s

    def _returns(self, s: pd.Series) -> pd.Series:
        s = self._dedup(s)
        if self.use_log_returns and (s > 0).all():
            return pd.Series(np.log(s.astype(float)), index=s.index).diff()
        return s.astype(float).pct_change()  # fallback

    def run(self, series: pd.Series, **kwargs) -> List[Issue]:
        other: Optional[pd.Series] = kwargs.get("other_series")
        asset_class: str = str(kwargs.get("asset_class") or "").lower()
        mode: str = str(kwargs.get("mode") or "").lower().strip()
        src_a = kwargs.get("source_a")
        src_b = kwargs.get("source_b")

        # Calibrate by asset class
        if asset_class == "fx":
            ret_tol = 0.004
            consecutive = 3
        elif asset_class == "equities":
            ret_tol = 0.006
            consecutive = 2
        else:
            ret_tol = self.ret_tol
            consecutive = self.consecutive

        if other is None:
            return []

        # Default mode by asset class
        if not mode:
            if asset_class in {"fx", "equities", "commodities"}:
                mode = "returns"
            else:
                mode = "level"

        a = self._dedup(series)
        b = self._dedup(other)

        # Align
        df = pd.DataFrame({"a": a, "b": b}).dropna().sort_index()
        if df.empty:
            return []

        out: List[Issue] = []

        if mode == "returns":
            ra = self._returns(df["a"])
            rb = self._returns(df["b"])
            rdiff = (ra - rb).abs().dropna()

            if rdiff.empty:
                return []

            breach = rdiff > ret_tol
            if consecutive > 1:
                # require N consecutive True; flag the last day of the streak
                streak = breach.copy()
                for _ in range(consecutive - 1):
                    streak = streak & streak.shift(1).fillna(False)
                breach = streak

            rule_name = "reconcile.returns_diff"

            for d in breach[breach].index:
                dv = float(rdiff.loc[d])
                sev = int(min(100, 70 + 30 * min(1.0, dv / (5 * ret_tol))))
                out.append(
                    Issue(
                        rule_name,
                        d,
                        sev,
                        "source_switch",
                        {
                            "mode": "returns",
                            "ret_diff": dv,
                            "ret_tol": float(ret_tol),
                            "a_ret": float(ra.loc[d]) if d in ra.index and pd.notna(ra.loc[d]) else None,
                            "b_ret": float(rb.loc[d]) if d in rb.index and pd.notna(rb.loc[d]) else None,
                            "consecutive": consecutive,
                            "source_a": src_a,
                            "source_b": src_b,
                        },
                    )
                )

            return out

        # mode == "level" (current behavior, but dedup-safe)
        abs_diff = (df["a"] - df["b"]).abs()
        pct_diff = abs_diff / df["a"].abs().replace(0.0, pd.NA)

        breach = (abs_diff > self.abs_tol) & (pct_diff > self.pct_tol)
        if self.consecutive > 1:
            streak = breach.copy()
            for _ in range(self.consecutive - 1):
                streak = streak & streak.shift(1).fillna(False)
            breach = streak

        for d in breach[breach].index:
            ad = float(abs_diff.loc[d])
            pdiff = float(pct_diff.loc[d]) if pd.notna(pct_diff.loc[d]) else ad
            sev = int(min(100, 70 + 30 * min(1.0, pdiff / (5 * self.pct_tol))))
            out.append(
                Issue(
                    self.name,
                    d,
                    sev,
                    "source_switch",
                    {
                        "mode": "level",
                        "abs_diff": ad,
                        "pct_diff": pdiff,
                        "abs_tol": float(self.abs_tol),
                        "pct_tol": float(self.pct_tol),
                        "consecutive": self.consecutive,
                    },
                )
            )

        return out
