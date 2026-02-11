from __future__ import annotations

from datetime import date
from io import StringIO

import pandas as pd
import requests

from .base import Provider, SeriesData, ProviderError

# Keyless CSV endpoint
# Example: https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10
FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv"


class FREDProvider(Provider):
    name = "fred"

    def fetch(self, symbol: str, start: date, end: date, **kwargs) -> SeriesData:
        try:
            r = requests.get(FRED_CSV, params={"id": symbol}, timeout=30)
            r.raise_for_status()
        except Exception as e:
            raise ProviderError(f"FRED download failed for {symbol}: {e}") from e

        try:
            df = pd.read_csv(StringIO(r.text))
        except Exception as e:
            raise ProviderError(f"FRED CSV parse failed for {symbol}: {e}") from e

        if df.empty or "DATE" not in df.columns or symbol not in df.columns:
            raise ProviderError(f"FRED returned unexpected CSV for {symbol}: cols={list(df.columns)}")

        df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
        # FRED uses '.' for missing
        s = pd.to_numeric(df[symbol].replace(".", pd.NA), errors="coerce")
        out = pd.DataFrame({"value": s.values}, index=df["DATE"]).dropna()

        out = out.loc[(out.index >= start) & (out.index <= end)]
        if out.empty:
            raise ProviderError(f"FRED empty after filtering for {symbol}")

        return SeriesData(df=out, provider=self.name, symbol=symbol)
