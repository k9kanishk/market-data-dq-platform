from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from sqlmodel import select

from .db import init_db, session
from .ingest import ingest_risk_factor
from .models import RiskFactor
from .universe import load_universe
from .engine import run_dq


UNIVERSE_PATH = Path("dq/config/universe.yml")


def ingest_universe(start: date, end: date, universe_path: Path = UNIVERSE_PATH) -> list[dict[str, Any]]:
    init_db()
    rfs = load_universe(universe_path)
    results: list[dict[str, Any]] = []
    for rf in rfs:
        results.append(ingest_risk_factor(rf, start, end))
    return results


def list_risk_factors() -> list[RiskFactor]:
    with session() as s:
        return list(s.exec(select(RiskFactor).order_by(RiskFactor.asset_class, RiskFactor.id)).all())


def run_dq_for_all(asof: date, lookback_days: int = 400) -> list[int]:
    init_db()
    rfs = list_risk_factors()
    run_ids: list[int] = []
    for rf in rfs:
        run_ids.append(run_dq(asset_class=rf.asset_class, risk_factor_id=rf.id, asof=asof, lookback_days=lookback_days))
    return run_ids
