from __future__ import annotations
from dataclasses import dataclass
from datetime import date
import pandas as pd

@dataclass(frozen=True)
class SeriesData:
    df: pd.DataFrame   # index=date, column value
    provider: str
    symbol: str

class ProviderError(RuntimeError):
    pass

class Provider:
    name: str
    def fetch(self, symbol: str, start: date, end: date, **kwargs) -> SeriesData:
        raise NotImplementedError
