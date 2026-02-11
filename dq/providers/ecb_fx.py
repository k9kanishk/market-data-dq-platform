from __future__ import annotations
from datetime import date
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from .base import Provider, SeriesData, ProviderError

ECB_HIST_XML = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml"

class ECBFXProvider(Provider):
    name = "ecb_fx"

    def fetch(self, symbol: str, start: date, end: date, **kwargs) -> SeriesData:
        try:
            r = requests.get(ECB_HIST_XML, timeout=30)
            r.raise_for_status()
        except Exception as e:
            raise ProviderError(f"ECB download failed: {e}") from e

        try:
            root = ET.fromstring(r.text)
        except Exception as e:
            raise ProviderError(f"ECB XML parse failed: {e}") from e

        records = []
        for node in root.iter():
            if node.tag.endswith("Cube") and "time" in node.attrib:
                d = pd.to_datetime(node.attrib["time"]).date()
                rate = None
                for child in list(node):
                    if child.tag.endswith("Cube") and child.attrib.get("currency") == symbol:
                        rate = float(child.attrib["rate"])
                        break
                if rate is not None:
                    records.append((d, rate))

        if not records:
            raise ProviderError(f"ECB FX no data for {symbol}")

        s = pd.Series({d: v for d, v in records}, name="value").sort_index()
        df = s.to_frame()
        df = df.loc[(df.index >= start) & (df.index <= end)]
        if df.empty:
            raise ProviderError(f"ECB FX empty after filtering for {symbol}")
        return SeriesData(df=df, provider=self.name, symbol=symbol)

class ECBFXCrossProvider(Provider):
    name = "ecb_fx_cross"
    def __init__(self):
        self.leg = ECBFXProvider()

    def fetch(self, symbol: str, start: date, end: date, **kwargs) -> SeriesData:
        legs = kwargs.get("legs")
        if not legs or len(legs) != 2:
            raise ProviderError("ecb_fx_cross requires meta legs: [EURUSD, EURGBP]-style")

        def infer_ccy(rf_id: str) -> str:
            if rf_id.startswith("EUR") and len(rf_id) == 6:
                return rf_id[3:6]
            raise ProviderError(f"Can't infer currency from {rf_id}, expected EURXXX")

        ccy1 = infer_ccy(legs[0])  # USD
        ccy2 = infer_ccy(legs[1])  # GBP

        eur_ccy1 = self.leg.fetch(ccy1, start, end).df.rename(columns={"value": "eur_ccy1"})
        eur_ccy2 = self.leg.fetch(ccy2, start, end).df.rename(columns={"value": "eur_ccy2"})

        df = eur_ccy2.join(eur_ccy1, how="inner")
        df["value"] = df["eur_ccy2"] / df["eur_ccy1"]  # GBP per USD
        out = df[["value"]]
        if out.empty:
            raise ProviderError("ECB cross produced empty series")
        return SeriesData(df=out, provider=self.name, symbol=symbol)
