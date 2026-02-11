from __future__ import annotations
from datetime import date
from pathlib import Path
import typer
from rich import print as rprint
from rich.table import Table

from .db import init_db
from .universe import load_universe
from .ingest import ingest_risk_factor
from .engine import run_dq
from .pack import make_pack

app = typer.Typer(add_completion=False)

def parse_date(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except Exception as e:
        raise typer.BadParameter(f"Invalid date {s}. Use YYYY-MM-DD") from e

@app.command("db")
def db_cmd(action: str = typer.Argument(..., help="init")):
    if action != "init":
        raise typer.BadParameter("Only 'init' is supported")
    init_db()
    rprint("[green]DB initialized.[/green]")

@app.command()
def ingest(
    what: str = typer.Argument(..., help="universe"),
    start: str = typer.Option(..., help="YYYY-MM-DD"),
    end: str = typer.Option(..., help="YYYY-MM-DD"),
    universe_path: Path = typer.Option(Path("dq/config/universe.yml")),
):
    if what != "universe":
        raise typer.BadParameter("Only 'universe' is supported")
    start_d, end_d = parse_date(start), parse_date(end)

    rfs = load_universe(universe_path)
    t = Table(title="Ingestion results")
    t.add_column("Risk Factor")
    t.add_column("Source")
    t.add_column("Symbol")
    t.add_column("Inserted", justify="right")
    t.add_column("Error")

    for rf in rfs:
        res = ingest_risk_factor(rf, start_d, end_d)
        for row in res["results"]:
            t.add_row(res["risk_factor"], row["source"], row["symbol"], str(row["inserted"]), row["error"][:60])
    rprint(t)

@app.command()
def run(
    asset_class: str = typer.Option(..., "--asset-class"),
    risk_factor: str = typer.Option(..., "--risk-factor"),
    asof: str = typer.Option(..., "--asof"),
    lookback_days: int = typer.Option(400, "--lookback-days"),
):
    run_id = run_dq(asset_class, risk_factor, parse_date(asof), lookback_days)
    rprint(f"[green]DQ run complete.[/green] dq_run_id={run_id}")

@app.command()
def pack(from_date: str = typer.Option(..., "--from"), to_date: str = typer.Option(..., "--to")):
    out = make_pack(parse_date(from_date), parse_date(to_date))
    rprint(f"[green]DQ Pack generated.[/green] count={out['count']}")
    rprint(out["html"])
    rprint(out["pdf"])

@app.command()
def serve():
    import subprocess, sys
    raise SystemExit(subprocess.call([sys.executable, "-m", "streamlit", "run", "streamlit_app.py"]))
