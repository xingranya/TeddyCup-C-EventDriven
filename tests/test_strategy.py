from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

from pipeline.task4_strategy import is_tradeable, next_trading_date, week_last_trading_date


class StrategyRuleTestCase(unittest.TestCase):
    """策略交易规则测试。"""

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


if __name__ == "__main__":
    unittest.main()
