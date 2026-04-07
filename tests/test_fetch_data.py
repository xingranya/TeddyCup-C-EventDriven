from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from pipeline.fetch_data import (
    describe_tushare_trade_calendar_error,
    fetch_financial_data_from_public_sources,
    fetch_trading_calendar,
    load_imported_event_records,
    select_disclosed_indicator_row,
)
from pipeline.models import AppConfig, RunContext


class FetchDataTestCase(unittest.TestCase):
    """数据采集辅助逻辑测试。"""

    @staticmethod
    def build_config() -> AppConfig:
        """构造最小可用配置。"""

        return AppConfig(
            raw={
                "project": {
                    "timezone": "Asia/Shanghai",
                    "initial_capital": 100000,
                    "market_close_time": "15:00:00",
                },
                "data": {
                    "lookback_days": 14,
                    "benchmark_code": "000300.SH",
                    "trading_calendar_source": "tushare",
                },
                "tushare": {
                    "token": "test-token",
                },
                "events": {
                    "qstock_enabled": False,
                    "import_paths": {},
                },
                "strategy": {
                    "max_positions": 3,
                    "single_position_max": 0.5,
                    "single_position_min": 0.2,
                    "min_listing_days": 60,
                    "min_avg_turnover_million": 80,
                    "positive_score_threshold": 0.02,
                    "min_prediction_score_threshold": -0.01,
                },
            }
        )

    def test_describe_tushare_trade_calendar_error_returns_readable_message(self) -> None:
        message = describe_tushare_trade_calendar_error(
            Exception("抱歉，您没有接口访问权限，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。")
        )
        self.assertIn("无访问权限", message)
        self.assertIn("trade_cal", message)

    def test_fetch_trading_calendar_uses_akshare_compat_when_py_mini_racer_breaks(self) -> None:
        class FakeTushareClient:
            @staticmethod
            def trade_cal(**_: object) -> pd.DataFrame:
                raise Exception("抱歉，您没有接口访问权限，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。")

        class BrokenAkshare:
            @staticmethod
            def tool_trade_date_hist_sina() -> pd.DataFrame:
                raise AttributeError("dlsym(0x1, mr_eval_context): symbol not found")

        compat_df = pd.DataFrame(
            {"trade_date": [date(2026, 3, 30), date(2026, 3, 31), date(2026, 4, 1)]}
        )
        config = self.build_config()

        with patch("pipeline.fetch_data.require_tushare_client", return_value=FakeTushareClient()):
            with patch("pipeline.fetch_data.ak", BrokenAkshare()):
                with patch(
                    "pipeline.fetch_data.fetch_trading_calendar_from_akshare_compat",
                    return_value=compat_df,
                ):
                    with self.assertLogs("pipeline.fetch_data", level="WARNING") as captured:
                        artifacts = fetch_trading_calendar(date(2026, 3, 30), date(2026, 4, 1), config)

        self.assertEqual(artifacts.source_name, "akshare_compat")
        self.assertEqual(
            artifacts.calendar,
            [date(2026, 3, 30), date(2026, 3, 31), date(2026, 4, 1)],
        )
        joined = "\n".join(captured.output)
        self.assertIn("Tushare 交易日历接口无访问权限", joined)
        self.assertIn("Akshare 交易日历依赖的 py_mini_racer 原生库与当前环境不兼容", joined)

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

    def test_load_imported_event_records_skips_bad_rows_instead_of_raising(self) -> None:
        with TemporaryDirectory() as temp_dir:
            import_dir = Path(temp_dir)
            (import_dir / "events.json").write_text(
                """
[
  {
    "raw_id": "good-1",
    "title": "有效事件",
    "content": "这里有完整内容",
    "published_at": "2026-04-18 10:00:00",
    "source_name": "测试来源",
    "source_url": "https://example.com/good"
  },
  {
    "raw_id": "bad-1",
    "title": "坏事件",
    "content": "缺少发布时间",
    "published_at": ""
  }
]
                """.strip(),
                encoding="utf-8",
            )
            records = load_imported_event_records(
                import_dir=import_dir,
                source_type="industry",
                stock_names=[],
                collected_at="2026-04-20 09:00:00",
                lookback_start=date(2026, 4, 6),
                asof_date=date(2026, 4, 20),
            )

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["raw_id"], "good-1")


if __name__ == "__main__":
    unittest.main()
