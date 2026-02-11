from __future__ import annotations
from datetime import date, timedelta
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from sqlmodel import select
from .db import session, init_db
from .models import DQException, ExceptionAction, Observation, DataSource, RiskFactor

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
    df = pd.DataFrame(rows, columns=["date","value","source","symbol"])
    out = {}
    if df.empty:
        return out
    for (src, sym), g in df.groupby(["source","symbol"]):
        ser = pd.Series(g["value"].values, index=pd.to_datetime(g["date"]).dt.date, name=f"{src}:{sym}").sort_index()
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
    st.plotly_chart(fig, use_container_width=True)

def main():
    st.set_page_config(page_title="Market Data DQ Platform", layout="wide")
    init_db()

    st.title("Market Data DQ Platform")

    rf_list = ["ALL"] + _list_rfs()
    col1, col2, col3 = st.columns([2,1,1])
    with col1:
        rf = st.selectbox("Risk Factor", rf_list)
    with col2:
        status = st.selectbox("Status", ["ALL","open","triaged","closed"], index=1)
    with col3:
        lookback = st.selectbox("Lookback (days)", [7,30,90,180,365], index=2)

    to_d = date.today()
    from_d = to_d - timedelta(days=int(lookback))

    df = _load_exceptions(from_d, to_d, rf, status)

    left, right = st.columns([1,1])
    with left:
        st.subheader("Exception Queue")
        if df.empty:
            st.info("No exceptions in this window.")
            return
        view = df.sort_values(["severity","obs_date"], ascending=[False, True])[
            ["id","obs_date","risk_factor_id","rule","severity","status","suggested_action"]
        ]
        st.dataframe(view, use_container_width=True, hide_index=True)
        ex_id = st.selectbox("Drilldown exception id", view["id"].tolist())

    with right:
        ex_row = df[df["id"] == ex_id].iloc[0].to_dict()
        st.subheader("Drilldown")
        st.markdown(f"**{ex_row['risk_factor_id']}** • {ex_row['obs_date']} • **{ex_row['rule']}** • sev **{ex_row['severity']}**")
        st.json(ex_row["details"])

        series = _load_series(ex_row["risk_factor_id"])
        win_from = ex_row["obs_date"] - timedelta(days=60)
        win_to = ex_row["obs_date"] + timedelta(days=10)
        series = {k: v.loc[(v.index >= win_from) & (v.index <= win_to)] for k, v in series.items()}
        ex2 = df[(df["risk_factor_id"] == ex_row["risk_factor_id"]) &
                 (df["obs_date"] >= win_from) & (df["obs_date"] <= win_to)]
        _plot(series, ex2)

        st.divider()
        st.subheader("Resolution / Audit Trail")
        action = st.selectbox("Action", ["accept","remove","winsorize","interpolate","source_switch","close_as_false_positive"])
        comment = st.text_input("Comment")
        actor = st.text_input("Actor", "analyst")

        if st.button("Record action"):
            with session() as s:
                s.add(ExceptionAction(exception_id=int(ex_row["id"]), action=action, comment=comment, actor=actor))
                ex_obj = s.get(DQException, int(ex_row["id"]))
                ex_obj.status = "closed" if action in {"accept","close_as_false_positive"} else "triaged"
                s.add(ex_obj)
                s.commit()
            st.success("Recorded.")
