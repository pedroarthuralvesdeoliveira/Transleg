from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pandas as pd
from pandas.api.types import is_object_dtype, is_string_dtype

from transleg.domain.models import ReportSpec


class DataFrameTransformer:
    def transform_file(self, file_path: Path, spec: ReportSpec) -> pd.DataFrame:
        dataframe = self._read_excel(file_path)
        return self.transform_dataframe(dataframe, spec)

    def transform_dataframe(self, dataframe: pd.DataFrame, spec: ReportSpec) -> pd.DataFrame:
        working = dataframe.copy()
        if spec.tail_rows_to_drop:
            working = working.iloc[:-spec.tail_rows_to_drop]

        working = working.dropna(axis=1, how="all")
        working = working.drop_duplicates()
        working = self._trim_strings(working)
        working = working.replace(
            {
                "None": None,
                "none": None,
                "NULL": None,
                "null": None,
                "nan": None,
                "NaN": None,
                "": None,
            }
        )

        working = working.rename(columns=spec.column_mapping)
        selected_columns = [column for column in spec.all_target_columns if column in working.columns]
        working = working[selected_columns].copy()
        working = working.dropna(how="all")

        for column_name in spec.numeric_br_columns:
            if column_name in working.columns:
                working[column_name] = self._normalize_locale_decimal(working[column_name])

        for column_name in spec.date_columns:
            if column_name in working.columns:
                working[column_name] = pd.to_datetime(
                    working[column_name],
                    dayfirst=True,
                    errors="coerce",
                ).dt.date

        for column_name, bounds in spec.integer_ranges.items():
            if column_name in working.columns:
                working[column_name] = self._coerce_integer(working[column_name], *bounds)

        return working.reset_index(drop=True)

    def _read_excel(self, file_path: Path) -> pd.DataFrame:
        try:
            return pd.read_excel(file_path, engine="calamine")
        except ValueError:
            return pd.read_excel(file_path)

    def _trim_strings(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        for column_name in dataframe.columns:
            if is_object_dtype(dataframe[column_name]) or is_string_dtype(
                dataframe[column_name]
            ):
                dataframe[column_name] = dataframe[column_name].apply(
                    lambda value: value.strip() if isinstance(value, str) else value
                )
        return dataframe

    def _normalize_locale_decimal(self, series: pd.Series) -> pd.Series:
        normalized = (
            series.astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .replace({"None": None, "nan": None})
        )
        return normalized.map(self._to_decimal_or_none)

    def _to_decimal_or_none(self, value: object) -> Decimal | None:
        if value is None:
            return None
        value_str = str(value).strip()
        if not value_str:
            return None
        try:
            return Decimal(value_str)
        except Exception:
            return None

    def _coerce_integer(self, series: pd.Series, min_value: int, max_value: int) -> pd.Series:
        numeric = pd.to_numeric(series, errors="coerce")
        clipped = numeric.clip(lower=min_value, upper=max_value)
        return clipped.astype("Int64")
