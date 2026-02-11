from __future__ import annotations
from datetime import date, datetime
from pathlib import Path
import pandas as pd
from jinja2 import Template
from sqlmodel import select
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table
from reportlab.lib.styles import getSampleStyleSheet
from .settings import settings
from .db import session
from .models import DQException

HTML = Template("""
<!doctype html><html><head><meta charset="utf-8"><title>DQ Pack</title>
<style>
body{font-family:Arial;margin:24px} table{border-collapse:collapse;width:100%}
th,td{border:1px solid #ddd;padding:6px;font-size:12px} th{background:#f5f5f5}
</style></head><body>
<h1>Market Data DQ Pack</h1>
<p><b>Window:</b> {{ f }} → {{ t }}</p>
<p>Total: <b>{{ total }}</b> | High (>=80): <b>{{ high }}</b> | Open: <b>{{ open_ }}</b></p>
<table>
<thead><tr><th>Date</th><th>RF</th><th>Rule</th><th>Sev</th><th>Status</th><th>Suggested</th></tr></thead>
<tbody>
{% for r in rows %}
<tr><td>{{ r.obs_date }}</td><td>{{ r.risk_factor_id }}</td><td>{{ r.rule }}</td>
<td>{{ r.severity }}</td><td>{{ r.status }}</td><td>{{ r.suggested_action }}</td></tr>
{% endfor %}
</tbody></table>
</body></html>
""")

def make_pack(from_date: date, to_date: date) -> dict:
    out_dir = Path(settings.outputs_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    html_path = out_dir / f"dq_pack_{from_date}_{to_date}_{stamp}.html"
    pdf_path = out_dir / f"dq_pack_{from_date}_{to_date}_{stamp}.pdf"

    with session() as s:
        rows = s.exec(
            select(DQException).where(DQException.obs_date >= from_date, DQException.obs_date <= to_date)
        ).all()

    df = pd.DataFrame([r.model_dump() for r in rows]) if rows else pd.DataFrame()
    total = 0 if df.empty else len(df)
    high = 0 if df.empty else int((df["severity"] >= 80).sum())
    open_ = 0 if df.empty else int((df["status"] == "open").sum())

    html_path.write_text(HTML.render(f=str(from_date), t=str(to_date), total=total, high=high, open_=open_, rows=rows), encoding="utf-8")

    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(str(pdf_path), pagesize=A4)
    story = [
        Paragraph("Market Data DQ Pack", styles["Title"]),
        Paragraph(f"Window: {from_date} → {to_date}", styles["Normal"]),
        Spacer(1, 12),
        Paragraph(f"Total: {total} | High (>=80): {high} | Open: {open_}", styles["Normal"]),
        Spacer(1, 12),
    ]
    if rows:
        top = sorted(rows, key=lambda x: (-x.severity, x.obs_date))[:50]
        table_data = [["Date","RF","Rule","Sev","Status","Suggested"]] + [
            [str(r.obs_date), r.risk_factor_id, r.rule, str(r.severity), r.status, r.suggested_action] for r in top
        ]
        story.append(Table(table_data, hAlign="LEFT"))
    else:
        story.append(Paragraph("No exceptions.", styles["Normal"]))
    doc.build(story)

    return {"html": str(html_path), "pdf": str(pdf_path), "count": total}
