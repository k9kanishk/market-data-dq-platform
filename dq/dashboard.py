from __future__ import annotations
from datetime import date, timedelta

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from sqlmodel import select

from .db import session, init_db
from .models import DQException, ExceptionAction, Observation, DataSource, RiskFactor
from .bootstrap import ingest_universe, run_dq_for_all


def _load_exceptions(from_date: date, to_date: date, rf: str, status: str) -> pd.DataFrame:
    with session() as s:
        q = select(DQException).where(DQException.obs_date >= from_date, DQException.obs_date <= to_date)
        if rf != "ALL":
            q = q.where(DQException.risk_factor_id == rf)
        if status != "ALL":
            q = q.where(DQException.status == status)
        rows = s.exec(q).all()
    return pd.DataFrame([r.model_dump() for r in rows]) if rows else pd.DataFrame()


def _list_rfs() -> list[str]:
    with session() as s:
        rows = s.exec(select(RiskFactor.id).order_by(RiskFactor.id)).all()
    return [r[0] for r in rows]


def _load_series(rf_id: str) -> dict[str, pd.Series]:
    with session() as s:
        rows = s.exec(
            select(Observation.obs_date, Observation.value, DataSource.name, DataSource.symbol)
            .join(DataSource, Observation.source_id == DataSource.id)
            .where(Observation.risk_factor_id == rf_id)
        ).all()
    df = pd.DataFrame(rows, columns=["date", "value", "source", "symbol"])
    out: dict[str, pd.Series] = {}
    if df.empty:
        return out
    for (src, sym), g in df.groupby(["source", "symbol"]):
        ser = pd.Series(
            g["value"].values,
            index=pd.to_datetime(g["date"]).dt.date,
            name=f"{src}:{sym}",
        ).sort_index()
        out[f"{src}:{sym}"] = ser
    return out


def _plot(series_dict: dict[str, pd.Series], ex: pd.DataFrame):
    fig = go.Figure()
    for name, s in series_dict.items():
        fig.add_trace(go.Scatter(x=list(s.index), y=s.values, mode="lines", name=name))
    if ex is not None and not ex.empty and series_dict:
        first = next(iter(series_dict.values()))
        ex_dates = ex["obs_date"].tolist()
        ex_y = [first.get(d, None) for d in ex_dates]
        fig.add_trace(go.Scatter(x=ex_dates, y=ex_y, mode="markers", name="exceptions"))
    st.plotly_chart(fig, width="stretch")


def main():
    st.set_page_config(page_title="Market Data DQ Platform", layout="wide")
    init_db()

    st.title("Market Data DQ Platform")

    # -----------------------------
    # Setup / Run panel (Cloud needs this)
    # -----------------------------
    with st.sidebar.expander("Setup / Run (first time in Streamlit Cloud)", expanded=False):
        years = st.slider("Ingest lookback (years)", 1, 10, 5)
        dq_lookback_days = st.slider("DQ lookback (days)", 60, 900, 400, step=20)
        asof = st.date_input("As-of date", value=date.today())

        c1, c2 = st.columns(2)
        with c1:
            if st.button("1) Ingest universe"):
                start = date.today() - timedelta(days=365 * int(years))
                end = date.today()
                with st.status("Ingesting market data...", expanded=True) as status:
                    results = ingest_universe(start, end)
                    status.update(label="Ingestion complete", state="complete")
                # Show summary
                total_inserted = 0
                errors = 0
                for r in results:
                    for row in r["results"]:
                        total_inserted += int(row.get("inserted", 0))
                        if row.get("error"):
                            errors += 1
                st.success(f"Ingested rows: {total_inserted}. Source-errors: {errors}.")
                st.rerun()

        with c2:
            if st.button("2) Run DQ (all RFs)"):
                with st.status("Running DQ checks...", expanded=True) as status:
                    run_ids = run_dq_for_all(asof=asof, lookback_days=int(dq_lookback_days))
                    status.update(label="DQ runs complete", state="complete")
                st.success(f"Completed DQ runs: {len(run_ids)}")
                st.rerun()

        st.caption("Note: Streamlit Cloud containers are ephemeral; data may reset on redeploy.")

    # -----------------------------
    # Main filters
    # -----------------------------
    rf_list = ["ALL"] + _list_rfs()
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        rf = st.selectbox("Risk Factor", rf_list)
    with col2:
        status = st.selectbox("Status", ["ALL", "open", "triaged", "closed"], index=1)
    with col3:
        lookback = st.selectbox("Lookback (days)", [7, 30, 90, 180, 365], index=2)

    to_d = date.today()
    from_d = to_d - timedelta(days=int(lookback))
    df = _load_exceptions(from_d, to_d, rf, status)

    left, right = st.columns([1, 1])
    with left:
        st.subheader("Exception Queue")
        if df.empty:
            st.info("No exceptions in this window. Use the sidebar Setup/Run to ingest data and run DQ.")
            return

        view = df.sort_values(["severity", "obs_date"], ascending=[False, True])[
            ["id", "obs_date", "risk_factor_id", "rule", "severity", "status", "suggested_action"]
        ]
        st.dataframe(view, width="stretch", hide_index=True)
        ex_id = st.selectbox("Drilldown exception id", view["id"].tolist())

    with right:
        ex_row = df[df["id"] == ex_id].iloc[0].to_dict()
        st.subheader("Drilldown")
        st.markdown(
            f"**{ex_row['risk_factor_id']}** • {ex_row['obs_date']} • **{ex_row['rule']}** • sev **{ex_row['severity']}**"
        )
        st.json(ex_row["details"])

        series = _load_series(ex_row["risk_factor_id"])
        win_from = ex_row["obs_date"] - timedelta(days=60)
        win_to = ex_row["obs_date"] + timedelta(days=10)
        series = {k: v.loc[(v.index >= win_from) & (v.index <= win_to)] for k, v in series.items()}
        ex2 = df[
            (df["risk_factor_id"] == ex_row["risk_factor_id"])
            & (df["obs_date"] >= win_from)
            & (df["obs_date"] <= win_to)
        ]
        _plot(series, ex2)

        st.divider()
        st.subheader("Resolution / Audit Trail")
        action = st.selectbox(
            "Action",
            ["accept", "remove", "winsorize", "interpolate", "source_switch", "close_as_false_positive"],
        )
        comment = st.text_input("Comment")
        actor = st.text_input("Actor", "analyst")

        if st.button("Record action"):
            with session() as s:
                s.add(ExceptionAction(exception_id=int(ex_row["id"]), action=action, comment=comment, actor=actor))
                ex_obj = s.get(DQException, int(ex_row["id"]))
                ex_obj.status = "closed" if action in {"accept", "close_as_false_positive"} else "triaged"
                s.add(ex_obj)
                s.commit()
            st.success("Recorded.")
            st.rerun()
