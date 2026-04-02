from __future__ import annotations

import unittest
from datetime import date, datetime, time

import pandas as pd

from pipeline.event_study_enhanced import _build_event_window, _resolve_anchor_trade_date


class EventStudyTestCase(unittest.TestCase):
    """事件研究模块测试。"""

    def test_after_close_event_moves_to_next_trade_date(self) -> None:
        calendar = [date(2026, 4, 20), date(2026, 4, 21), date(2026, 4, 22)]
        anchor = _resolve_anchor_trade_date(
            calendar,
            datetime(2026, 4, 20, 18, 0, 0),
            time(15, 0, 0),
        )
        self.assertEqual(anchor, date(2026, 4, 21))

    def test_same_day_event_before_close_keeps_trade_date(self) -> None:
        calendar = [date(2026, 4, 20), date(2026, 4, 21), date(2026, 4, 22)]
        anchor = _resolve_anchor_trade_date(
            calendar,
            datetime(2026, 4, 20, 10, 0, 0),
            time(15, 0, 0),
        )
        self.assertEqual(anchor, date(2026, 4, 20))

    def test_event_at_market_close_moves_to_next_trade_date(self) -> None:
        calendar = [date(2026, 4, 20), date(2026, 4, 21), date(2026, 4, 22)]
        anchor = _resolve_anchor_trade_date(
            calendar,
            datetime(2026, 4, 20, 15, 0, 0),
            time(15, 0, 0),
        )
        self.assertEqual(anchor, date(2026, 4, 21))

    def test_car_0_2_excludes_day_minus_one(self) -> None:
        calendar = [
            date(2026, 4, 20),
            date(2026, 4, 21),
            date(2026, 4, 22),
            date(2026, 4, 23),
            date(2026, 4, 24),
            date(2026, 4, 27),
        ]
        stock_history = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(calendar),
                "return": [0.10, 0.20, 0.30, 0.40, 0.50, 0.60],
            }
        )
        benchmark_returns = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(calendar),
                "return": [0.0] * len(calendar),
            }
        )
        window_df = _build_event_window(
            stock_history=stock_history,
            benchmark_returns=benchmark_returns,
            market_calendar=calendar,
            anchor_date=date(2026, 4, 21),
            start_offset=-1,
            end_offset=4,
            market_model={"alpha": 0.0, "beta": 0.0, "use_market_adjusted": 0.0},
        )
        car_row = window_df[window_df["day_offset"] == 2].iloc[0]
        self.assertAlmostEqual(car_row["cumulative_abnormal_return"], 1.0, places=6)
        self.assertAlmostEqual(car_row["cumulative_abnormal_return_0_2"], 0.9, places=6)


if __name__ == "__main__":
    unittest.main()
