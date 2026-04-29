from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd
from sqlalchemy import (
    BIGINT,
    DATE,
    DATETIME,
    NUMERIC,
    TEXT,
    Column,
    Identity,
    MetaData,
    Table,
    UniqueConstraint,
    create_engine,
    inspect,
    select,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.sql import func

from transleg.domain.exceptions import DataLoadError
from transleg.domain.models import LoadStats, PipelineOutcome, ReportSpec


class PostgresWarehouseRepository:
    def __init__(self, database_url: str) -> None:
        self.engine: Engine = create_engine(database_url, future=True)
        self.metadata = MetaData()
        self._ensure_etl_runs_table()

    def _ensure_etl_runs_table(self) -> None:
        table = Table(
            "etl_runs",
            self.metadata,
            Column("id", BIGINT, Identity(always=False), primary_key=True),
            Column("process_name", TEXT, nullable=False),
            Column("report_name", TEXT, nullable=False),
            Column("status", TEXT, nullable=False),
            Column("started_at", DATETIME(timezone=True), nullable=False, server_default=func.now()),
            Column("finished_at", DATETIME(timezone=True), nullable=True),
            Column("data_start", DATE, nullable=True),
            Column("data_end", DATE, nullable=True),
            Column("rows_downloaded", BIGINT, nullable=True),
            Column("rows_loaded", BIGINT, nullable=True),
            Column("source_file", TEXT, nullable=True),
            Column("message", TEXT, nullable=True),
            schema="public",
            extend_existing=True,
        )
        self.metadata.create_all(self.engine, tables=[table], checkfirst=True)

    def load_dataframe(self, spec: ReportSpec, dataframe: pd.DataFrame) -> LoadStats:
        if dataframe.empty:
            return LoadStats()

        schema, table_name = self._split_table_name(spec.table_name)
        table = self._ensure_domain_table(schema, table_name, spec, dataframe)
        payload = [self._sanitize_record(record) for record in dataframe.to_dict("records")]

        statement = pg_insert(table).values(payload)
        if spec.conflict_columns:
            statement = statement.on_conflict_do_nothing(
                index_elements=list(spec.conflict_columns)
            )

        try:
            with self.engine.begin() as connection:
                result = connection.execute(statement)
                inserted = max(result.rowcount or 0, 0)
        except Exception as exc:
            raise DataLoadError(
                f"Falha ao inserir dados em {spec.table_name}: {exc}"
            ) from exc

        processed = len(payload)
        return LoadStats(
            processed=processed,
            inserted=inserted,
            ignored=max(processed - inserted, 0),
        )

    def last_successful_date(
        self,
        report_name: str,
        default: date,
    ) -> date:
        table = Table("etl_runs", MetaData(), autoload_with=self.engine, schema="public")
        statement = (
            select(table.c.data_end)
            .where(table.c.report_name == report_name)
            .where(table.c.status == "success")
            .order_by(table.c.finished_at.desc())
            .limit(1)
        )
        with self.engine.begin() as connection:
            result = connection.execute(statement).scalar_one_or_none()
        return result or default

    def record_run(self, outcome: PipelineOutcome) -> None:
        table = Table("etl_runs", MetaData(), autoload_with=self.engine, schema="public")
        payload = {
            "process_name": "transleg.sync",
            "report_name": outcome.report_name.value,
            "status": outcome.status.value,
            "started_at": outcome.started_at,
            "finished_at": outcome.finished_at,
            "data_start": outcome.data_window.start,
            "data_end": outcome.data_window.end,
            "rows_downloaded": outcome.rows_downloaded,
            "rows_loaded": outcome.rows_loaded,
            "source_file": str(outcome.source_file) if outcome.source_file else None,
            "message": outcome.message,
        }
        with self.engine.begin() as connection:
            connection.execute(table.insert().values(payload))

    def _ensure_domain_table(
        self,
        schema: str,
        table_name: str,
        spec: ReportSpec,
        dataframe: pd.DataFrame,
    ) -> Table:
        inspector = inspect(self.engine)
        if inspector.has_table(table_name, schema=schema):
            return Table(table_name, MetaData(), autoload_with=self.engine, schema=schema)

        columns = [
            Column("id", BIGINT, Identity(always=False), primary_key=True),
        ]
        for column_name in dataframe.columns:
            columns.append(Column(column_name, self._infer_sqlalchemy_type(spec, column_name)))
        columns.append(
            Column("loaded_at", DATETIME(timezone=True), nullable=False, server_default=func.now())
        )

        constraints: list[Any] = []
        if spec.conflict_columns:
            constraints.append(
                UniqueConstraint(*spec.conflict_columns, name=f"uq_{table_name}_natural_key")
            )

        table = Table(
            table_name,
            MetaData(),
            *columns,
            *constraints,
            schema=schema,
        )
        table.create(self.engine, checkfirst=True)
        return table

    def _infer_sqlalchemy_type(self, spec: ReportSpec, column_name: str):
        if column_name in spec.date_columns:
            return DATE
        if column_name in spec.numeric_br_columns:
            return NUMERIC(18, 4)
        if column_name in spec.integer_ranges:
            return BIGINT
        return TEXT

    def _sanitize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        sanitized: dict[str, Any] = {}
        for key, value in record.items():
            if pd.isna(value):
                sanitized[key] = None
            elif isinstance(value, pd.Timestamp):
                sanitized[key] = value.to_pydatetime()
            elif isinstance(value, Decimal):
                sanitized[key] = value
            else:
                sanitized[key] = value
        return sanitized

    def _split_table_name(self, table_name: str) -> tuple[str, str]:
        if "." not in table_name:
            return "public", table_name
        schema, simple_name = table_name.split(".", maxsplit=1)
        return schema, simple_name

