# Market Data DQ Platform (VaR Risk Factors)

A practical market data DQ platform that:
- Ingests time-series risk factors from **multiple public sources**
- Runs layered controls: spikes, gaps/staleness, relationship checks, and source reconciliation
- Stores an **audit trail** in SQL (runs, exceptions, analyst actions)
- Provides a **Streamlit triage dashboard**
- Generates a weekly **DQ Pack** (HTML + PDF)

## Quickstart

```bash
python -m venv .venv
# Windows: .\.venv\Scripts\activate
source .venv/bin/activate

pip install -U pip
pip install -e .

dq db init
dq ingest universe --start 2018-01-01 --end 2026-02-10
dq run --asset-class rates --risk-factor US10Y --asof 2026-01-20
dq serve
```

Notes

Public data feeds are imperfect by nature (format changes, throttling). That’s fine: the value here is
the control framework + auditability. Swap providers later for Bloomberg/Refinitiv/internal sources.
