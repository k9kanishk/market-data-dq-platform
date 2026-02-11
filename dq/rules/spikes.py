from __future__ import annotations
import numpy as np
import pandas as pd
from typing import List
from .base import Rule, Issue

class HampelRule(Rule):
    name = "spikes.hampel"

    def __init__(self, window: int = 21, n_sigmas: float = 6.0):
        self.window = window
        self.n_sigmas = n_sigmas

    def run(self, series: pd.Series, **kwargs) -> List[Issue]:
        x = series.dropna().astype(float)
        if len(x) < 30:
            return []
        med = x.rolling(self.window, center=True, min_periods=max(10, self.window // 3)).median()
        mad = (x - med).abs().rolling(self.window, center=True, min_periods=max(10, self.window // 3)).median()
        scale = 1.4826 * mad.replace(0.0, np.nan)
        z = (x - med) / scale

        out: List[Issue] = []
        for d, zz in z.dropna().items():
            if abs(zz) >= self.n_sigmas:
                sev = int(min(100, 40 + 10 * abs(zz)))
                out.append(Issue(
                    rule=self.name,
                    obs_date=d,
                    severity=sev,
                    suggested_action="winsorize" if abs(zz) < 12 else "remove",
                    details={"z_robust": float(zz), "window": self.window, "n_sigmas": self.n_sigmas},
                ))
        return out
