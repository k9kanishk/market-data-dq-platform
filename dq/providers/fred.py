from __future__ import annotations
from datetime import date
import pandas as pd
from pandas_datareader.data import DataReader
from .base import Provider, SeriesData, ProviderError

class FREDProvider(Provider):
    name = "fred"
    def fetch(self, symbol: str, start: date, end: date, **kwargs) -> SeriesData:
        try:
            df = DataReader(symbol, "fred", start, end)
        except Exception as e:
            raise ProviderError(f"FRED fetch failed for {symbol}: {e}") from e
        if df.empty:
            raise ProviderError(f"FRED returned empty for {symbol}")
        out = df.rename(columns={symbol: "value"}).dropna()
        out.index = pd.to_datetime(out.index).date
        return SeriesData(df=out[["value"]], provider=self.name, symbol=symbol)
