from __future__ import annotations
from datetime import date
import pandas as pd
from pandas_datareader.data import DataReader
from .base import Provider, SeriesData, ProviderError

class StooqProvider(Provider):
    name = "stooq"
    def fetch(self, symbol: str, start: date, end: date, **kwargs) -> SeriesData:
        try:
            df = DataReader(symbol, "stooq", start, end)
        except Exception as e:
            raise ProviderError(f"Stooq fetch failed for {symbol}: {e}") from e
        if df is None or df.empty:
            raise ProviderError(f"Stooq returned empty for {symbol}")
        df = df.sort_index()
        if "Close" not in df.columns:
            raise ProviderError(f"Stooq missing Close for {symbol}. cols={list(df.columns)}")
        out = df[["Close"]].rename(columns={"Close": "value"}).dropna()
        out.index = pd.to_datetime(out.index).date
        return SeriesData(df=out, provider=self.name, symbol=symbol)
