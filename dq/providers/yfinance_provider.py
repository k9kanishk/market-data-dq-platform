from __future__ import annotations
from datetime import date
import pandas as pd
import yfinance as yf
from .base import Provider, SeriesData, ProviderError

class YFinanceProvider(Provider):
    name = "yfinance"
    def fetch(self, symbol: str, start: date, end: date, **kwargs) -> SeriesData:
        try:
            df = yf.download(symbol, start=str(start), end=str(end), progress=False, auto_adjust=False)
        except Exception as e:
            raise ProviderError(f"yfinance fetch failed for {symbol}: {e}") from e
        if df is None or df.empty:
            raise ProviderError(f"yfinance returned empty for {symbol}")
        df = df.sort_index()
        col = "Adj Close" if "Adj Close" in df.columns else "Close"
        out = df[[col]].rename(columns={col: "value"}).dropna()
        out.index = pd.to_datetime(out.index).date
        return SeriesData(df=out, provider=self.name, symbol=symbol)
