from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from pipeline.fetch_data import fetch_financial_data_from_public_sources, select_disclosed_indicator_row
from pipeline.models import RunContext


class FetchDataTestCase(unittest.TestCase):
    """数据采集辅助逻辑测试。"""

    def test_select_disclosed_indicator_row_ignores_future_announcements(self) -> None:
        indicator_df = pd.DataFrame(
            [
                {"ann_date": "20260421", "end_date": "20260331", "roe": 12.0},
                {"ann_date": "20260418", "end_date": "20251231", "roe": 10.0},
            ]
        )
        selected = select_disclosed_indicator_row(indicator_df, date(2026, 4, 20))
        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(str(selected["ann_date"].date()), "2026-04-18")

    def test_public_financial_fallback_keeps_schema_when_all_symbols_miss(self) -> None:
        class EmptyAkshare:
            @staticmethod
            def stock_financial_abstract_ths(symbol: str) -> pd.DataFrame:
                return pd.DataFrame()

        with TemporaryDirectory() as temp_dir:
            context = RunContext(
                asof_date=date(2026, 4, 20),
                project_root=Path(temp_dir),
                output_dir=Path(temp_dir),
                raw_dir=Path(temp_dir),
                processed_dir=Path(temp_dir),
            )
            with patch("pipeline.fetch_data.ak", EmptyAkshare()):
                financial_df = fetch_financial_data_from_public_sources(["000001", "000002"], context)

        self.assertEqual(
            financial_df.columns.tolist(),
            [
                "stock_code",
                "pe",
                "pb",
                "turnover_rate",
                "roe",
                "net_profit_growth",
                "revenue_growth",
                "debt_to_asset",
                "ann_date",
                "report_period",
                "snapshot_trade_date",
            ],
        )
        self.assertEqual(financial_df["stock_code"].tolist(), ["000001", "000002"])
        self.assertTrue(financial_df[["pe", "pb", "turnover_rate", "roe"]].isna().all().all())


if __name__ == "__main__":
    unittest.main()
