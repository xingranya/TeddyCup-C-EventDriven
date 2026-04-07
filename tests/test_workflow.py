from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from pipeline.event_study_enhanced import EventStudyArtifacts
from pipeline.fetch_data import FetchArtifacts
from pipeline.industry_chain_enhanced import IndustryChainArtifacts
from pipeline.models import AppConfig
from pipeline.workflow import run_weekly_pipeline


class WorkflowTestCase(unittest.TestCase):
    """工作流配置透传测试。"""

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
                "tushare": {"token": "dummy-token"},
                "events": {"qstock_enabled": False, "import_paths": {}},
                "strategy": {
                    "max_positions": 3,
                    "single_position_max": 0.5,
                    "single_position_min": 0.2,
                    "min_listing_days": 60,
                    "min_avg_turnover_million": 80,
                    "positive_score_threshold": 0.02,
                },
                "report": {
                    "top_event_count": 5,
                    "top_relation_count": 12,
                },
                "event_taxonomy": {
                    "duration_type": {"自定义周期": ["规划"]},
                    "subject_type": {"自定义主体": ["政策"]},
                    "predictability": {"自定义可预测性": ["预期"]},
                    "industry_type": {"自定义行业": ["科技"]},
                },
            }
        )

    def test_workflow_passes_event_taxonomy_into_task1(self) -> None:
        config = self._build_config()
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            fetch_artifacts = FetchArtifacts(
                news_df=pd.DataFrame([{"title": "新闻", "content": "内容", "published_at": "2026-04-20 10:00:00", "source": "policy", "source_name": "来源", "raw_id": "1", "entity_candidates": ""}]),
                stock_df=pd.DataFrame(columns=["stock_code", "stock_name", "industry", "concept_tags", "main_business", "listed_date", "is_st"]),
                price_df=pd.DataFrame(columns=["stock_code", "trade_date", "close"]),
                benchmark_df=pd.DataFrame(columns=["stock_code", "trade_date", "close"]),
                trading_calendar=[date(2026, 4, 20)],
                trading_calendar_source_name="tushare",
                trading_calendar_status_note="交易日历来自 Tushare 实盘接口",
            )

            with patch("pipeline.workflow.load_config", return_value=config), patch(
                "pipeline.workflow.run_fetch_pipeline", return_value=fetch_artifacts
            ), patch("pipeline.workflow.run_event_identification", return_value=pd.DataFrame()) as mock_identify, patch(
                "pipeline.workflow.run_relation_mining", return_value=(pd.DataFrame(), [])
            ), patch("pipeline.workflow.fetch_financial_data", return_value=pd.DataFrame()), patch(
                "pipeline.workflow.fetch_suspend_resume_data", return_value=pd.DataFrame()
            ), patch("pipeline.workflow.run_impact_estimation", return_value=pd.DataFrame()), patch(
                "pipeline.workflow.run_event_study_enhanced",
                return_value=EventStudyArtifacts(
                    detail_df=pd.DataFrame(),
                    stats_df=pd.DataFrame(),
                    joint_mean_car_df=pd.DataFrame(),
                    output_dir=project_root / "outputs" / "weekly" / "2026-04-20" / "event_study",
                    joint_mean_car_path=project_root / "outputs" / "weekly" / "2026-04-20" / "event_study" / "joint_mean_car.png",
                ),
            ), patch(
                "pipeline.workflow.run_industry_chain_enhanced",
                return_value=IndustryChainArtifacts(
                    relation_df=pd.DataFrame(),
                    summary_path=project_root / "outputs" / "weekly" / "2026-04-20" / "kg_visual" / "summary.md",
                    combined_png_path=project_root / "outputs" / "weekly" / "2026-04-20" / "kg_visual" / "graph.png",
                    combined_html_path=project_root / "outputs" / "weekly" / "2026-04-20" / "kg_visual" / "graph.html",
                    selected_events=[],
                ),
            ), patch(
                "pipeline.workflow.run_strategy_construction",
                return_value=(pd.DataFrame(), {"asof_date": "2026-04-20", "fallback_used": False, "candidate_count": 0, "selected_count": 0, "buy_date": "", "sell_date": ""}),
            ), patch(
                "pipeline.workflow.build_weekly_report",
                return_value=project_root / "outputs" / "weekly" / "2026-04-20" / "report.md",
            ):
                run_weekly_pipeline(project_root, "2026-04-20")

        _, kwargs = mock_identify.call_args
        self.assertEqual(kwargs["event_taxonomy"], config.event_taxonomy)


if __name__ == "__main__":
    unittest.main()
