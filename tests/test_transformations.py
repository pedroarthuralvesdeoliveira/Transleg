from __future__ import annotations

from decimal import Decimal

import pandas as pd

from transleg.domain.models import ReportName
from transleg.infrastructure.report_catalog import get_report_spec
from transleg.services.transformations import DataFrameTransformer


def test_transformer_normalizes_received_invoices_dataframe() -> None:
    transformer = DataFrameTransformer()
    spec = get_report_spec(ReportName.RECEIVED_INVOICES)
    raw = pd.DataFrame(
        [
            {
                "Duplicata": " FAT-1 ",
                "Valor Total": "1.234,56",
                "Multa": "10,00",
                "Data Emissão": "21/01/2026",
                "Status": " Pago ",
            }
        ]
    )

    transformed = transformer.transform_dataframe(raw, spec)

    assert transformed.loc[0, "duplicata"] == "FAT-1"
    assert transformed.loc[0, "valor_total"] == Decimal("1234.56")
    assert transformed.loc[0, "multa"] == Decimal("10.00")
    assert transformed.loc[0, "data_emissao"].isoformat() == "2026-01-21"
    assert transformed.loc[0, "status"] == "Pago"


def test_transformer_drops_manual_release_footer_rows() -> None:
    transformer = DataFrameTransformer()
    spec = get_report_spec(ReportName.MANUAL_RELEASES)
    raw = pd.DataFrame(
        [
            {"Nr Lancto": "1", "Valor": "100,00", "Data Lancto": "01/02/2026"},
            {"Nr Lancto": "2", "Valor": "200,00", "Data Lancto": "02/02/2026"},
            {"Nr Lancto": None, "Valor": None, "Data Lancto": None},
            {"Nr Lancto": None, "Valor": None, "Data Lancto": None},
            {"Nr Lancto": None, "Valor": None, "Data Lancto": None},
            {"Nr Lancto": None, "Valor": None, "Data Lancto": None},
        ]
    )

    transformed = transformer.transform_dataframe(raw, spec)

    assert len(transformed) == 2
    assert transformed.loc[0, "valor"] == Decimal("100.00")
    assert transformed.loc[1, "nr_lancto"] == 2

