from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from typing import Any

import pandas as pd
from sqlmodel import select

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

from .db import session
from .models import DQException, ExceptionAction, DQRun


@dataclass(frozen=True)
class DQPack:
    html_bytes: bytes
    pdf_bytes: bytes
    html_name: str
    pdf_name: str


def _utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _safe_json(x: Any) -> str:
    if x is None:
        return ""
    # details is often dict/json; stringify cleanly
    try:
        import json

        return json.dumps(x, sort_keys=True)
    except Exception:
        return str(x)


def _fetch_exceptions(from_date: date, to_date: date, status: str | None = None) -> pd.DataFrame:
    with session() as s:
        q = select(DQException).where(DQException.obs_date >= from_date, DQException.obs_date <= to_date)
        if status and status.lower() != "all":
            q = q.where(DQException.status == status)
        rows = s.exec(q).all()

    if not rows:
        return pd.DataFrame(
            columns=[
                "id",
                "dq_run_id",
                "risk_factor_id",
                "obs_date",
                "rule",
                "severity",
                "status",
                "suggested_action",
                "details",
            ]
        )

    df = pd.DataFrame([r.model_dump() for r in rows])

    # normalize columns
    if "details" in df.columns:
        df["details"] = df["details"].map(_safe_json)
    if "suggested_action" not in df.columns:
        df["suggested_action"] = ""
    if "dq_run_id" not in df.columns:
        df["dq_run_id"] = None

    # ordering
    if "severity" in df.columns:
        df = df.sort_values(["severity", "obs_date"], ascending=[False, False])
    else:
        df = df.sort_values(["obs_date"], ascending=[False])

    return df


def _fetch_actions(exception_ids: list[int]) -> pd.DataFrame:
    if not exception_ids:
        return pd.DataFrame(columns=["exception_id", "action", "comment", "actor", "created_at", "ts", "id"])

    with session() as s:
        q = select(ExceptionAction).where(ExceptionAction.exception_id.in_(exception_ids))
        rows = s.exec(q).all()

    if not rows:
        return pd.DataFrame(columns=["exception_id", "action", "comment", "actor", "created_at", "ts", "id"])

    df = pd.DataFrame([r.model_dump() for r in rows])
    return df


def _latest_run_meta() -> dict[str, Any]:
    # Used just for “latest run” context in the pack header
    with session() as s:
        run = s.exec(select(DQRun).order_by(DQRun.id.desc())).first()
    if not run:
        return {}
    return run.model_dump()


def _df_to_html_table(df: pd.DataFrame, max_rows: int = 50) -> str:
    if df.empty:
        return "<p><em>No rows.</em></p>"
    view = df.head(max_rows).copy()
    return view.to_html(index=False, escape=True)


def _pdf_table_from_df(df: pd.DataFrame, max_rows: int = 40) -> Table:
    if df.empty:
        df = pd.DataFrame([{"info": "No rows."}])

    view = df.head(max_rows).copy()
    data = [list(view.columns)] + view.astype(str).values.tolist()

    t = Table(data, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
            ]
        )
    )
    return t


def generate_dq_pack(asof: date, lookback_days: int, status: str = "open") -> DQPack:
    """
    Generates HTML + PDF DQ pack for exceptions in [asof-lookback_days, asof].
    'status' can be 'open', 'triaged', 'closed', or 'all'.
    """
    from_date = asof - timedelta(days=int(lookback_days))
    to_date = asof

    ex_df = _fetch_exceptions(from_date, to_date, status=status)

    # KPIs
    total = len(ex_df)
    by_rule = ex_df.groupby("rule", dropna=False).size().reset_index(name="count").sort_values("count", ascending=False)
    by_rf = (
        ex_df.groupby("risk_factor_id", dropna=False).size().reset_index(name="count").sort_values("count", ascending=False)
    )

    if "severity" in ex_df.columns and not ex_df.empty:
        top = ex_df[["id", "obs_date", "risk_factor_id", "rule", "severity", "status", "suggested_action"]].head(50)
    else:
        top = ex_df[["id", "obs_date", "risk_factor_id", "rule", "status", "suggested_action"]].head(50)

    # Actions summary
    actions_df = _fetch_actions(ex_df["id"].astype(int).tolist() if "id" in ex_df.columns and not ex_df.empty else [])
    action_kpi = (
        actions_df.groupby("action", dropna=False).size().reset_index(name="count").sort_values("count", ascending=False)
        if not actions_df.empty
        else pd.DataFrame(columns=["action", "count"])
    )

    run_meta = _latest_run_meta()

    # ---------------- HTML ----------------
    html = f"""
    <html>
    <head>
      <meta charset="utf-8"/>
      <style>
        body {{ font-family: Arial, sans-serif; margin: 24px; }}
        h1 {{ margin-bottom: 0; }}
        .meta {{ color: #444; margin-top: 6px; }}
        .kpi {{ display: flex; gap: 12px; margin: 18px 0; }}
        .card {{ border: 1px solid #ddd; border-radius: 10px; padding: 12px 14px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 10px 0 22px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 6px 8px; font-size: 12px; }}
        th {{ background: #f3f3f3; text-align: left; }}
        .small {{ font-size: 12px; color: #444; }}
      </style>
    </head>
    <body>
      <h1>Market Data DQ Pack</h1>
      <div class="meta">
        Generated: {_utc_now_str()}<br/>
        Window: {from_date.isoformat()} → {to_date.isoformat()} ({lookback_days} days)<br/>
        Status filter: {status}<br/>
      </div>

      <div class="kpi">
        <div class="card"><b>Total exceptions</b><br/>{total}</div>
        <div class="card"><b>Top risk factor</b><br/>{(by_rf.iloc[0]["risk_factor_id"] if len(by_rf) else "-")}</div>
        <div class="card"><b>Top rule</b><br/>{(by_rule.iloc[0]["rule"] if len(by_rule) else "-")}</div>
      </div>

      <h2>Top Exceptions (by severity)</h2>
      {_df_to_html_table(top, max_rows=50)}

      <h2>Exceptions by Rule</h2>
      {_df_to_html_table(by_rule, max_rows=50)}

      <h2>Exceptions by Risk Factor</h2>
      {_df_to_html_table(by_rf, max_rows=50)}

      <h2>Actions Recorded (Audit Trail)</h2>
      {_df_to_html_table(action_kpi, max_rows=50)}

      <h2>Latest Run Metadata</h2>
      <div class="small"><pre>{_safe_json(run_meta)}</pre></div>
    </body>
    </html>
    """.strip()

    html_bytes = html.encode("utf-8")

    # ---------------- PDF ----------------
    pdf_buf = BytesIO()
    doc = SimpleDocTemplate(pdf_buf, pagesize=A4, title="DQ Pack")
    styles = getSampleStyleSheet()

    story = []
    story.append(Paragraph("Market Data DQ Pack", styles["Title"]))
    story.append(Paragraph(f"Generated: {_utc_now_str()}", styles["Normal"]))
    story.append(Paragraph(f"Window: {from_date.isoformat()} → {to_date.isoformat()} ({lookback_days} days)", styles["Normal"]))
    story.append(Paragraph(f"Status filter: {status}", styles["Normal"]))
    story.append(Spacer(1, 12))

    # KPI table
    kpi_df = pd.DataFrame(
        [
            {
                "total_exceptions": total,
                "top_risk_factor": (by_rf.iloc[0]["risk_factor_id"] if len(by_rf) else "-"),
                "top_rule": (by_rule.iloc[0]["rule"] if len(by_rule) else "-"),
            }
        ]
    )
    story.append(Paragraph("KPI Summary", styles["Heading2"]))
    story.append(_pdf_table_from_df(kpi_df, max_rows=5))
    story.append(Spacer(1, 12))

    story.append(Paragraph("Top Exceptions (by severity)", styles["Heading2"]))
    story.append(_pdf_table_from_df(top, max_rows=40))
    story.append(Spacer(1, 12))

    story.append(Paragraph("Exceptions by Rule", styles["Heading2"]))
    story.append(_pdf_table_from_df(by_rule, max_rows=40))
    story.append(Spacer(1, 12))

    story.append(Paragraph("Exceptions by Risk Factor", styles["Heading2"]))
    story.append(_pdf_table_from_df(by_rf, max_rows=40))
    story.append(Spacer(1, 12))

    story.append(Paragraph("Actions Recorded (Audit Trail)", styles["Heading2"]))
    story.append(_pdf_table_from_df(action_kpi, max_rows=40))
    story.append(Spacer(1, 12))

    story.append(Paragraph("Latest Run Metadata", styles["Heading2"]))
    story.append(Paragraph(f"<pre>{_safe_json(run_meta)}</pre>", styles["Code"]))

    doc.build(story)
    pdf_bytes = pdf_buf.getvalue()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    html_name = f"dq_pack_{stamp}.html"
    pdf_name = f"dq_pack_{stamp}.pdf"

    return DQPack(
        html_bytes=html_bytes,
        pdf_bytes=pdf_bytes,
        html_name=html_name,
        pdf_name=pdf_name,
    )
