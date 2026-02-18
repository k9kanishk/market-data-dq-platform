# Market Data DQ Platform for VaR Risk Factors

A Python + SQL market data quality platform that ingests real historical time series from multiple sources,
runs layered DQ controls (spikes, gaps/staleness, cross-checks, vendor reconciliation), supports triage/audit trail,
and generates a weekly DQ Pack (HTML/PDF).

## Why this matters
VaR and stress testing depend on clean market risk factor time series. This tool flags spikes/gaps, stale prints,
and vendor disagreements, producing an exception queue + working-group-ready reporting.

## Architecture
- Ingestion: multi-provider (ECB/FRED/Stooq/TwelveData optional)
- Storage: SQLModel + SQLite (swap to Postgres easily)
- DQ Engine: rules for spikes/gaps/reconcile/relations
- Workflow: exception queue + actions (audit trail)
- Reporting: one-click DQ Pack (PDF/HTML)
- Automation: weekly GitHub Action creates artifacts

## Data sources
- ECB FX (no key)
- FRED rates (no key)
- Stooq (no key)
- TwelveData (optional key) — set `TWELVEDATA_API_KEY`

## Quickstart (local)
```bash
poetry install
poetry run dq db init
poetry run dq ingest universe --start 2023-01-01 --end 2026-02-18
poetry run streamlit run streamlit_app.py
