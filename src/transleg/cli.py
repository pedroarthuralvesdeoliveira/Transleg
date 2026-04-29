from __future__ import annotations

from datetime import date

import typer
from rich.console import Console
from rich.table import Table

from transleg.application.orchestrator import SyncOrchestrator
from transleg.core.config import get_settings
from transleg.core.logging import configure_logging
from transleg.domain.models import ReportName
from transleg.infrastructure.report_catalog import list_report_specs

app = typer.Typer(help="CLI do pipeline Transleg.")
console = Console()


def _build_orchestrator() -> SyncOrchestrator:
    settings = get_settings()
    configure_logging(settings.log_level)
    return SyncOrchestrator(settings)


@app.command("reports")
def reports() -> None:
    table = Table(title="Catalogo de relatorios")
    table.add_column("Slug")
    table.add_column("Nome")
    table.add_column("Tabela destino")

    for spec in list_report_specs():
        table.add_row(spec.report_name.value, spec.display_name, spec.table_name)

    console.print(table)


@app.command("sync")
def sync(
    report_name: ReportName = typer.Argument(..., help="Slug do relatorio."),
    start_date: date = typer.Option(..., "--start", formats=["%Y-%m-%d"]),
    end_date: date = typer.Option(..., "--end", formats=["%Y-%m-%d"]),
    load_to_db: bool = typer.Option(True, "--load-to-db/--no-load-to-db"),
) -> None:
    outcome = _build_orchestrator().sync_report(
        report_name=report_name,
        start_date=start_date,
        end_date=end_date,
        load_to_db=load_to_db,
    )
    console.print(
        {
            "report": outcome.report_name.value,
            "status": outcome.status.value,
            "rows_downloaded": outcome.rows_downloaded,
            "rows_loaded": outcome.rows_loaded,
            "source_file": str(outcome.source_file) if outcome.source_file else None,
            "message": outcome.message,
        }
    )


@app.command("incremental")
def incremental(
    report_name: ReportName = typer.Argument(..., help="Slug do relatorio."),
    default_start_date: date = typer.Option(
        ...,
        "--default-start",
        formats=["%Y-%m-%d"],
    ),
    overlap_days: int = typer.Option(2, "--overlap-days"),
) -> None:
    outcome = _build_orchestrator().sync_incremental(
        report_name=report_name,
        default_start_date=default_start_date,
        overlap_days=overlap_days,
    )
    console.print(
        {
            "report": outcome.report_name.value,
            "status": outcome.status.value,
            "window": f"{outcome.data_window.start} -> {outcome.data_window.end}",
            "rows_downloaded": outcome.rows_downloaded,
            "rows_loaded": outcome.rows_loaded,
        }
    )


@app.command("backfill")
def backfill(
    report_name: ReportName = typer.Argument(..., help="Slug do relatorio."),
    start_date: date = typer.Option(..., "--start", formats=["%Y-%m-%d"]),
    end_date: date = typer.Option(..., "--end", formats=["%Y-%m-%d"]),
    chunk_days: int = typer.Option(180, "--chunk-days"),
) -> None:
    outcomes = _build_orchestrator().backfill(
        report_name=report_name,
        start_date=start_date,
        end_date=end_date,
        chunk_days=chunk_days,
    )
    console.print(
        [
            {
                "window": f"{item.data_window.start} -> {item.data_window.end}",
                "status": item.status.value,
                "rows_loaded": item.rows_loaded,
            }
            for item in outcomes
        ]
    )


if __name__ == "__main__":
    app()

