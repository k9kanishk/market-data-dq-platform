from __future__ import annotations
from datetime import date
import pandas as pd
from sqlmodel import select
from .db import session
from .models import RiskFactor, DataSource, Observation
from .providers.registry import get_provider
from .universe import RiskFactorSpec

def upsert_risk_factor(rf: RiskFactorSpec):
    with session() as s:
        obj = s.get(RiskFactor, rf.id) or RiskFactor(
            id=rf.id, asset_class=rf.asset_class, description=rf.description, unit=rf.unit
        )
        obj.asset_class = rf.asset_class
        obj.description = rf.description
        obj.unit = rf.unit
        s.add(obj)
        s.commit()

def upsert_data_source(name: str, symbol: str, field: str, meta: dict) -> int:
    with session() as s:
        q = select(DataSource).where(
            DataSource.name == name, DataSource.symbol == symbol, DataSource.field == field
        )
        ds = s.exec(q).first()
        if ds is None:
            ds = DataSource(name=name, symbol=symbol, field=field, meta=meta or {})
            s.add(ds)
            s.commit()
            s.refresh(ds)
        return int(ds.id)

def ingest_series(rf_id: str, provider_name: str, symbol: str, start: date, end: date, field: str, meta: dict):
    provider = get_provider(provider_name)
    data = provider.fetch(symbol, start, end, **(meta or {}))

    source_id = upsert_data_source(provider_name, symbol, field, meta or {})
    df = data.df
    if "value" not in df.columns:
        raise ValueError(f"{provider_name} did not return 'value' column")

    inserted = 0
    with session() as s:
        existing = set(
            d for (d,) in s.exec(
                select(Observation.obs_date).where(
                    Observation.risk_factor_id == rf_id,
                    Observation.source_id == source_id
                )
            ).all()
        )
        for d, v in df["value"].items():
            if d < start or d > end:
                continue
            if d in existing:
                continue
            s.add(Observation(risk_factor_id=rf_id, source_id=source_id, obs_date=d, value=float(v)))
            inserted += 1
        s.commit()
    return inserted

def ingest_risk_factor(rf: RiskFactorSpec, start: date, end: date) -> dict:
    upsert_risk_factor(rf)
    results = []
    for src in rf.sources:
        meta = src.meta or {}
        try:
            inserted = ingest_series(rf.id, src.name, src.symbol, start, end, src.field, meta)
            results.append({"source": src.name, "symbol": src.symbol, "inserted": inserted, "error": ""})
        except Exception as e:
            # critical for DQ platforms: never die because one feed is broken
            results.append({"source": src.name, "symbol": src.symbol, "inserted": 0, "error": str(e)})
    return {"risk_factor": rf.id, "results": results}
