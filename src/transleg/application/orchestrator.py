from __future__ import annotations

from datetime import date, datetime, timedelta

from transleg.core.config import Settings
from transleg.domain.exceptions import ConfigurationError
from transleg.domain.models import (
    DateWindow,
    PipelineOutcome,
    ReportName,
    RunStatus,
)
from transleg.infrastructure.browser import BrowserSession
from transleg.infrastructure.portal import AleffPortalClient
from transleg.infrastructure.postgres import PostgresWarehouseRepository
from transleg.infrastructure.report_catalog import get_report_spec
from transleg.services.transformations import DataFrameTransformer


class SyncOrchestrator:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.transformer = DataFrameTransformer()
        self.repository = (
            PostgresWarehouseRepository(settings.database_url)
            if settings.database_url
            else None
        )

    def sync_report(
        self,
        report_name: ReportName,
        start_date: date,
        end_date: date,
        load_to_db: bool = True,
    ) -> PipelineOutcome:
        started_at = datetime.now()
        window = DateWindow(start=start_date, end=end_date)
        spec = get_report_spec(report_name)

        if load_to_db and self.repository is None:
            raise ConfigurationError(
                "TRANSLEG_DATABASE_URL e obrigatoria quando --load-to-db estiver ativo."
            )

        with BrowserSession(self.settings) as browser:
            portal = AleffPortalClient(browser, self.settings)
            portal.login()
            downloaded_report = portal.download_report(spec, window)

        if not downloaded_report.downloaded:
            outcome = PipelineOutcome(
                report_name=report_name,
                status=RunStatus.NO_DATA,
                data_window=window,
                rows_downloaded=0,
                rows_loaded=0,
                message=downloaded_report.message,
                source_file=None,
                started_at=started_at,
                finished_at=datetime.now(),
            )
            self._record_outcome(outcome)
            return outcome

        dataframe = self.transformer.transform_file(downloaded_report.file_path, spec)
        rows_loaded = 0

        if load_to_db and self.repository is not None:
            load_stats = self.repository.load_dataframe(spec, dataframe)
            rows_loaded = load_stats.inserted

        outcome = PipelineOutcome(
            report_name=report_name,
            status=RunStatus.SUCCESS,
            data_window=window,
            rows_downloaded=len(dataframe),
            rows_loaded=rows_loaded,
            message=downloaded_report.message,
            source_file=downloaded_report.file_path,
            started_at=started_at,
            finished_at=datetime.now(),
        )
        self._record_outcome(outcome)
        return outcome

    def sync_incremental(
        self,
        report_name: ReportName,
        default_start_date: date,
        overlap_days: int = 2,
        end_date: date | None = None,
    ) -> PipelineOutcome:
        if self.repository is None:
            raise ValueError("sync_incremental requer TRANSLEG_DATABASE_URL configurada.")

        last_success = self.repository.last_successful_date(
            report_name.value,
            default_start_date,
        )
        start_date = max(last_success - timedelta(days=overlap_days), default_start_date)
        return self.sync_report(
            report_name=report_name,
            start_date=start_date,
            end_date=end_date or date.today(),
            load_to_db=True,
        )

    def backfill(
        self,
        report_name: ReportName,
        start_date: date,
        end_date: date,
        chunk_days: int = 180,
    ) -> list[PipelineOutcome]:
        outcomes: list[PipelineOutcome] = []
        current_start = start_date

        while current_start <= end_date:
            current_end = min(current_start + timedelta(days=chunk_days - 1), end_date)
            outcomes.append(
                self.sync_report(
                    report_name=report_name,
                    start_date=current_start,
                    end_date=current_end,
                    load_to_db=True,
                )
            )
            current_start = current_end + timedelta(days=1)

        return outcomes

    def _record_outcome(self, outcome: PipelineOutcome) -> None:
        if self.repository is not None:
            self.repository.record_run(outcome)
