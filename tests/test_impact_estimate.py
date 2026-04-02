from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from pipeline.models import AppConfig
from pipeline.task3_impact_estimate import run_impact_estimation


class ImpactEstimateTestCase(unittest.TestCase):
    """影响评估锚点口径测试。"""

    def test_after_close_prediction_uses_next_trade_date_anchor(self) -> None:
        config = AppConfig(
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
                    "stock_whitelist_path": "",
                    "stock_blacklist_path": "",
                },
                "tushare": {"token": ""},
                "strategy": {
                    "max_positions": 3,
                    "single_position_max": 0.5,
                    "single_position_min": 0.1,
                    "min_listing_days": 60,
                    "min_avg_turnover_million": 80,
                    "positive_score_threshold": 0.02,
                },
                "scoring": {
                    "prediction": {
                        "expected_car_4d": 0.5,
                        "association_score": 0.2,
                        "event_score": 0.2,
                        "liquidity_score": 0.2,
                        "risk_penalty": 0.1,
                    }
                },
            }
        )
        trading_calendar = [
            pd.Timestamp("2026-04-20").date(),
            pd.Timestamp("2026-04-21").date(),
            pd.Timestamp("2026-04-22").date(),
        ]
        event_df = pd.DataFrame(
            [
                {
                    "event_id": "evt-1",
                    "event_name": "收盘后公告",
                    "published_at": "2026-04-20 15:01:00",
                    "subject_type": "公司类事件",
                    "industry_type": "电子",
                    "sentiment_score": 0.8,
                    "heat_score": 0.7,
                    "intensity_score": 0.6,
                    "scope_score": 0.5,
                    "confidence_score": 0.9,
                }
            ]
        )
        relation_df = pd.DataFrame(
            [
                {
                    "event_id": "evt-1",
                    "event_name": "收盘后公告",
                    "stock_code": "000001",
                    "stock_name": "平安银行",
                    "association_score": 0.8,
                    "relation_type": "直接关联",
                }
            ]
        )
        stock_df = pd.DataFrame(
            [
                {
                    "stock_code": "000001",
                    "stock_name": "平安银行",
                    "industry": "银行",
                    "avg_turnover_million": 300.0,
                }
            ]
        )
        price_df = pd.DataFrame(
            [
                {"stock_code": "000001", "trade_date": "2026-04-20", "close": 10.0},
                {"stock_code": "000001", "trade_date": "2026-04-21", "close": 10.2},
                {"stock_code": "000001", "trade_date": "2026-04-22", "close": 10.3},
            ]
        )
        benchmark_df = pd.DataFrame(
            [
                {"stock_code": "000300.SH", "trade_date": "2026-04-20", "close": 4000.0},
                {"stock_code": "000300.SH", "trade_date": "2026-04-21", "close": 4010.0},
                {"stock_code": "000300.SH", "trade_date": "2026-04-22", "close": 4020.0},
            ]
        )
        financial_df = pd.DataFrame()

        with tempfile.TemporaryDirectory() as temp_dir:
            prediction_df = run_impact_estimation(
                event_df=event_df,
                relation_df=relation_df,
                stock_df=stock_df,
                price_df=price_df,
                benchmark_df=benchmark_df,
                trading_calendar=trading_calendar,
                financial_df=financial_df,
                output_dir=Path(temp_dir),
                config=config,
            )

        self.assertEqual(prediction_df.iloc[0]["anchor_trade_date"], "2026-04-21")


if __name__ == "__main__":
    unittest.main()
