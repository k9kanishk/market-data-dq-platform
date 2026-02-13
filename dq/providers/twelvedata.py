from __future__ import annotations

from datetime import date
import os
from typing import Any

import pandas as pd
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .base import Provider, ProviderError, SeriesData

TD_URL = "https://api.twelvedata.com/time_series"


def _get_td_key() -> str | None:
    key = os.getenv("TWELVE_DATA_API_KEY")
    if key:
        return key.strip()

    try:
        import streamlit as st  # type: ignore

        secret_key = st.secrets.get("TWELVE_DATA_API_KEY", None)  # type: ignore[attr-defined]
        if secret_key:
            return str(secret_key).strip()
    except Exception:
        pass

    return None


class TwelveDataProvider(Provider):
    name = "twelvedata"

    @retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1.0, min=1, max=12),
        retry=retry_if_exception_type(
            (requests.Timeout, requests.ConnectionError, ProviderError)
        ),
    )
    def _call(self, params: dict[str, Any]) -> dict[str, Any]:
        try:
            response = requests.get(TD_URL, params=params, timeout=25)
        except (requests.Timeout, requests.ConnectionError):
            raise

        if response.status_code in (429, 502, 503, 504):
            raise ProviderError(f"Twelve Data temporary error HTTP {response.status_code}")

        try:
            payload = response.json()
        except Exception as exc:
            raise ProviderError(f"Twelve Data returned non-JSON: {exc}")

        if isinstance(payload, dict) and payload.get("status") == "error":
            code = payload.get("code", "")
            message = payload.get("message", "unknown error")
            if str(code) in ("429", "500", "502", "503", "504") or "rate" in str(message).lower():
                raise ProviderError(f"Twelve Data rate/temporary error: {code} {message}")
            raise ProviderError(f"Twelve Data error: {code} {message}")

        return payload

    def fetch(self, symbol: str, start: date, end: date, **kwargs: Any) -> SeriesData:
        key = _get_td_key()
        if not key:
            raise ProviderError(
                "Missing Twelve Data API key. Set TWELVE_DATA_API_KEY as env var or Streamlit secret."
            )

        interval = kwargs.get("interval", "1day")
        params = {
            "symbol": symbol,
            "interval": interval,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "apikey": key,
            "format": "JSON",
        }

        payload = self._call(params)
        values = payload.get("values")
        if not values or not isinstance(values, list):
            raise ProviderError(
                f"Twelve Data empty/invalid response for {symbol}: keys={list(payload.keys())}"
            )

        df = pd.DataFrame(values)
        if "datetime" not in df.columns or "close" not in df.columns:
            raise ProviderError(
                f"Twelve Data response missing datetime/close for {symbol}: cols={list(df.columns)}"
            )

        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        df = df.dropna(subset=["datetime"])

        df["value"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["value"])

        df["date"] = df["datetime"].dt.date
        out = (
            df[["date", "value"]]
            .drop_duplicates("date", keep="last")
            .set_index("date")
            .sort_index()
        )

        out = out.loc[(out.index >= start) & (out.index <= end)]
        if out.empty:
            raise ProviderError(f"Twelve Data returned no rows after filtering for {symbol}")

        return SeriesData(df=out, provider=self.name, symbol=symbol)
