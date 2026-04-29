from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from pathlib import Path


class ReportName(str, Enum):
    ISSUED_DOCUMENTS = "issued-documents"
    RECEIVED_INVOICES = "received-invoices"
    PAYABLE_TITLES = "payable-titles"
    MANUAL_RELEASES = "manual-releases"


class RunStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    NO_DATA = "no_data"
    PARTIAL = "partial"


@dataclass(slots=True, frozen=True)
class DateWindow:
    start: date
    end: date

    def __post_init__(self) -> None:
        if self.end < self.start:
            raise ValueError("end date must be greater than or equal to start date")


@dataclass(slots=True, frozen=True)
class ReportSpec:
    report_name: ReportName
    display_name: str
    menu_module_id: str
    report_code_id: str
    report_link_fragment: str
    monitor_description: str
    file_prefix: str
    table_name: str
    conflict_columns: tuple[str, ...]
    column_mapping: dict[str, str]
    click_fields: tuple[str, ...] = ()
    clear_fields: tuple[str, ...] = ()
    select_fields: dict[str, str] = field(default_factory=dict)
    radio_button_id: str | None = None
    tail_rows_to_drop: int = 0
    date_columns: tuple[str, ...] = ()
    numeric_br_columns: tuple[str, ...] = ()
    integer_ranges: dict[str, tuple[int, int]] = field(default_factory=dict)

    @property
    def all_target_columns(self) -> list[str]:
        return list(self.column_mapping.values())


@dataclass(slots=True, frozen=True)
class DownloadedReport:
    downloaded: bool
    message: str
    file_path: Path | None = None


@dataclass(slots=True, frozen=True)
class LoadStats:
    processed: int = 0
    inserted: int = 0
    ignored: int = 0


@dataclass(slots=True, frozen=True)
class PipelineOutcome:
    report_name: ReportName
    status: RunStatus
    data_window: DateWindow
    rows_downloaded: int
    rows_loaded: int
    message: str
    source_file: Path | None
    started_at: datetime
    finished_at: datetime

