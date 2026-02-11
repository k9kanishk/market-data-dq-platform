from __future__ import annotations

from datetime import date
from io import StringIO

import pandas as pd
import requests

from .base import Provider, SeriesData, ProviderError

# Keyless CSV endpoint (full history), filter locally
# Example: https://stooq.com/q/d/l/?s=10yusy.b&i=d
STOOQ_CSV = "https://stooq.com/q/d/l/"


class StooqProvider(Provider):
    name = "stooq"

    def fetch(self, symbol: str, start: date, end: date, **kwargs) -> SeriesData:
        try:
            r = requests.get(STOOQ_CSV, params={"s": symbol, "i": "d"}, timeout=30)
            r.raise_for_status()
        except Exception as e:
            raise ProviderError(f"Stooq download failed for {symbol}: {e}") from e

        try:
            df = pd.read_csv(StringIO(r.text))
        except Exception as e:
            raise ProviderError(f"Stooq CSV parse failed for {symbol}: {e}") from e

        if df.empty or "Date" not in df.columns:
            raise ProviderError(f"Stooq returned empty/unexpected CSV for {symbol}: cols={list(df.columns)}")

        if "Close" not in df.columns:
            raise ProviderError(f"Stooq missing Close for {symbol}: cols={list(df.columns)}")

        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        out = df[["Date", "Close"]].rename(columns={"Close": "value"}).dropna()
        out = out.set_index("Date").sort_index()

        out = out.loc[(out.index >= start) & (out.index <= end)]
        if out.empty:
            raise ProviderError(f"Stooq empty after filtering for {symbol}")

        return SeriesData(df=out, provider=self.name, symbol=symbol)
