from __future__ import annotations

from transleg.infrastructure.report_catalog import list_report_specs


def test_all_report_specs_have_table_and_conflict_columns() -> None:
    specs = list_report_specs()

    assert len(specs) == 4
    for spec in specs:
        assert spec.table_name
        assert spec.conflict_columns
        assert spec.file_prefix

