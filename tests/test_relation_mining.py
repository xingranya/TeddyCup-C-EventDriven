from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from pipeline.models import AppConfig
from pipeline.task2_relation_mining import run_relation_mining


class RelationMiningTestCase(unittest.TestCase):
    """关联挖掘配置驱动测试。"""

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
                },
                "scoring": {
                    "association": {
                        "direct_mention": 1.0,
                        "business_match": 0.0,
                        "industry_overlap": 0.0,
                        "historical_co_move": 0.0,
                    },
                    "association_profiles": {
                        "default": {
                            "direct_mention": 1.0,
                            "business_match": 1.0,
                            "industry_overlap": 1.0,
                            "historical_co_move": 1.0,
                        }
                    },
                },
            }
        )

    def test_relation_score_reads_association_weights_from_config(self) -> None:
        event_df = pd.DataFrame(
            [
                {
                    "event_id": "evt-1",
                    "event_name": "平安银行获重大订单",
                    "raw_evidence": "平安银行获重大订单",
                    "industry_type": "金融类事件",
                    "subject_type": "公司类事件",
                }
            ]
        )
        stock_df = pd.DataFrame(
            [
                {
                    "stock_code": "000001.SZ",
                    "stock_name": "平安银行",
                    "industry": "银行",
                    "concept_tags": "",
                    "main_business": "商业银行",
                }
            ]
        )
        price_df = pd.DataFrame(
            [{"stock_code": "000001", "trade_date": "2026-04-20", "close": 10.0}]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            output_dir = project_root / "outputs"
            output_dir.mkdir(parents=True, exist_ok=True)
            manual_dir = project_root / "data" / "manual"
            manual_dir.mkdir(parents=True, exist_ok=True)
            (manual_dir / "industry_relation_map.json").write_text(json.dumps({}), encoding="utf-8")

            with patch("pipeline.task2_relation_mining.compute_business_match", return_value=0.0), patch(
                "pipeline.task2_relation_mining.compute_industry_overlap", return_value=0.0
            ), patch("pipeline.task2_relation_mining.compute_historical_co_move", return_value=0.0):
                relation_df, _ = run_relation_mining(
                    event_df=event_df,
                    stock_df=stock_df,
                    price_df=price_df,
                    project_root=project_root,
                    output_dir=output_dir,
                    config=self._build_config(),
                )

        self.assertEqual(len(relation_df), 1)
        self.assertEqual(relation_df.iloc[0]["stock_code"], "000001")
        self.assertAlmostEqual(float(relation_df.iloc[0]["association_score"]), 1.0, places=4)


if __name__ == "__main__":
    unittest.main()
