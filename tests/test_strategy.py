from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

from pipeline.models import AppConfig
from pipeline.task4_strategy import allocate_positions, is_tradeable, next_trading_date, week_last_trading_date


class StrategyRuleTestCase(unittest.TestCase):
    """策略交易规则测试。"""

    def _build_config(self) -> AppConfig:
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
                    "stock_whitelist_path": "",
                    "stock_blacklist_path": "",
                },
                "tushare": {"token": ""},
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

    def test_next_trading_date_can_fall_on_friday(self) -> None:
        calendar = [
            date(2026, 4, 20),
            date(2026, 4, 24),
        ]
        self.assertEqual(next_trading_date(calendar, date(2026, 4, 20), 1), date(2026, 4, 24))

    def test_open_interval_suspend_is_not_tradeable(self) -> None:
        calendar = [
            date(2026, 4, 20),
            date(2026, 4, 21),
            date(2026, 4, 22),
            date(2026, 4, 23),
            date(2026, 4, 24),
        ]
        suspend_resume_df = pd.DataFrame(
            [
                {
                    "stock_code": "600760",
                    "suspend_date": "2026-04-21",
                    "resume_date": "",
                    "suspend_reason": "重大事项停牌",
                }
            ]
        )
        self.assertFalse(is_tradeable("600760", date(2026, 4, 20), calendar, suspend_resume_df))

    def test_week_last_trading_date_uses_last_open_day(self) -> None:
        calendar = [
            date(2026, 4, 20),
            date(2026, 4, 21),
            date(2026, 4, 22),
            date(2026, 4, 23),
        ]
        self.assertEqual(week_last_trading_date(calendar, date(2026, 4, 20)), date(2026, 4, 23))

    def test_allocate_positions_respects_bounds_and_sum(self) -> None:
        config = self._build_config()
        selected = pd.DataFrame(
            [
                {"event_name": "事件A", "stock_code": "600001", "stock_name": "股票A", "final_score": 0.92, "prediction_score": 0.08},
                {"event_name": "事件B", "stock_code": "600002", "stock_name": "股票B", "final_score": 0.55, "prediction_score": 0.05},
                {"event_name": "事件C", "stock_code": "600003", "stock_name": "股票C", "final_score": 0.31, "prediction_score": 0.03},
            ]
        )

        picks = allocate_positions(selected, config)

        self.assertAlmostEqual(picks["capital_ratio"].sum(), 1.0, places=4)
        self.assertTrue((picks["capital_ratio"] >= config.position_floor).all())
        self.assertTrue((picks["capital_ratio"] <= config.position_cap).all())


if __name__ == "__main__":
    unittest.main()
