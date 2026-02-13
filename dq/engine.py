from __future__ import annotations
from datetime import date
import pandas as pd
from sqlmodel import select
from .db import session
from .models import DQRun, DQException, RiskFactor
from .rules.spikes import HampelRule
from .rules.gaps import MissingBdaysRule, StaleRule
from .rules.reconcile import ReconcileRule
from .rules.relations import CorrBreakRule, FXTriangleRule
from .calendars import expected_dates

SPIKE = HampelRule()
MISSING = MissingBdaysRule()
STALE = StaleRule()
RECON = ReconcileRule()


def _pick_sources(asset_class: str, series_by_src: dict[str, pd.Series]) -> tuple[str, str | None]:
    prefs_primary = {
        "rates": ["fred", "stooq"],
        "fx": ["ecb_fx", "twelvedata", "stooq"],
        "equities": ["twelvedata", "stooq", "yfinance"],
        "commodities": ["twelvedata", "stooq", "yfinance"],
    }
    prefs_secondary = {
        "rates": ["stooq", "twelvedata"],
        "fx": ["twelvedata", "stooq"],
        "equities": ["stooq", "twelvedata"],
        "commodities": ["stooq", "twelvedata"],
    }

    keys = list(series_by_src.keys())

    primary = None
    for preferred in prefs_primary.get(asset_class, []):
        if preferred in series_by_src:
            primary = preferred
            break
    if primary is None:
        primary = sorted(keys)[0]

    secondary = None
    for preferred in prefs_secondary.get(asset_class, []):
        if preferred in series_by_src and preferred != primary:
            secondary = preferred
            break

    return primary, secondary

def load_series(rf_id: str) -> dict[str, pd.Series]:
    from .models import Observation, DataSource
    with session() as s:
        rows = s.exec(
            select(Observation.obs_date, Observation.value, DataSource.name)
            .join(DataSource, Observation.source_id == DataSource.id)
            .where(Observation.risk_factor_id == rf_id)
        ).all()
    df = pd.DataFrame(rows, columns=["date", "value", "source"])
    out = {}
    if df.empty:
        return out
    for src, g in df.groupby("source"):
        ser = pd.Series(g["value"].values, index=pd.to_datetime(g["date"]).dt.date, name=rf_id).sort_index()
        out[str(src)] = ser
    return out


def run_dq(asset_class: str, risk_factor_id: str, asof: date, lookback_days: int = 400) -> int:
    # Create run + capture run_id immediately (no detached ORM objects)
    with session() as s:
        rf = s.get(RiskFactor, risk_factor_id)
        if not rf:
            raise ValueError(f"Unknown risk factor {risk_factor_id}. Did you ingest?")

        run = DQRun(
            asset_class=asset_class,
            risk_factor_id=risk_factor_id,
            asof=asof,
            parameters={"lookback_days": lookback_days},
        )
        s.add(run)
        s.commit()
        s.refresh(run)  # ensure ID is populated
        run_id = int(run.id)

    series_by_src = load_series(risk_factor_id)
    if not series_by_src:
        raise ValueError(f"No observations for {risk_factor_id}")

    start = (pd.Timestamp(asof) - pd.Timedelta(days=lookback_days)).date()
    expected = expected_dates(asset_class, start, asof)

    primary_src, other_src = _pick_sources(asset_class, series_by_src)
    primary = series_by_src[primary_src].loc[
        (series_by_src[primary_src].index >= start) & (series_by_src[primary_src].index <= asof)
    ]

    issues = []
    issues += SPIKE.run(primary)
    issues += MISSING.run(primary, expected_dates=expected)
    issues += STALE.run(primary)

    if other_src is not None:
        other = series_by_src[other_src].loc[
            (series_by_src[other_src].index >= start) & (series_by_src[other_src].index <= asof)
        ]
        issues += RECON.run(primary, other_series=other)

    if asset_class == "rates" and risk_factor_id == "US10Y":
        peers = load_series("US2Y")
        if peers:
            s2 = peers[sorted(peers.keys())[0]]
            issues += CorrBreakRule().run(primary, peer_series=s2)

    if asset_class == "fx":
        eurusd = load_series("EURUSD")
        usdgbp = load_series("USDGBP")
        eurgbp = load_series("EURGBP")
        if eurusd and usdgbp and eurgbp:
            issues += FXTriangleRule().run(
                primary,
                ab=eurusd[sorted(eurusd.keys())[0]],
                bc=usdgbp[sorted(usdgbp.keys())[0]],
                ac=eurgbp[sorted(eurgbp.keys())[0]],
            )

    # Persist exceptions + mark run finished using run_id (not run object)
    with session() as s:
        for iss in issues:
            s.add(
                DQException(
                    dq_run_id=run_id,
                    risk_factor_id=risk_factor_id,
                    rule=iss.rule,
                    obs_date=iss.obs_date,
                    severity=int(iss.severity),
                    status="open",
                    suggested_action=iss.suggested_action,
                    details=iss.details,
                )
            )
        run2 = s.get(DQRun, run_id)
        run2.finished_at = pd.Timestamp.utcnow().to_pydatetime()
        s.add(run2)
        s.commit()

    return run_id
