from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from pipeline.event_study_enhanced import EventStudyArtifacts
from pipeline.industry_chain_enhanced import IndustryChainArtifacts
from pipeline.models import AppConfig
from pipeline.report_builder import build_weekly_report


class ReportBuilderTestCase(unittest.TestCase):
    """报告构建回归测试。"""

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
            }
        )

    def test_report_contains_typical_case_and_model_performance_sections(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            output_dir = project_root / "outputs" / "weekly" / "2026-04-20"
            study_dir = output_dir / "event_study"
            kg_dir = output_dir / "kg_visual"
            study_dir.mkdir(parents=True, exist_ok=True)
            kg_dir.mkdir(parents=True, exist_ok=True)

            summary_path = kg_dir / "industry_chain_summary.md"
            summary_path.write_text("图谱说明", encoding="utf-8")
            png_path = kg_dir / "industry_chain_graph.png"
            png_path.write_bytes(b"png")
            html_path = kg_dir / "industry_chain_graph.html"
            html_path.write_text("<html></html>", encoding="utf-8")
            joint_plot_path = study_dir / "joint_mean_car.png"
            joint_plot_path.write_bytes(b"png")

            event_df = pd.DataFrame(
                [
                    {
                        "event_id": "evt-1",
                        "event_name": "政策催化事件",
                        "subject_type": "政策类事件",
                        "duration_type": "中期型事件",
                        "predictability_type": "预披露型事件",
                        "industry_type": "科技类事件",
                        "heat_score": 0.8,
                        "intensity_score": 0.7,
                        "scope_score": 0.6,
                        "confidence_score": 0.9,
                    }
                ]
            )
            relation_df = pd.DataFrame(
                [
                    {
                        "event_id": "evt-1",
                        "event_name": "政策催化事件",
                        "stock_code": "000001",
                        "stock_name": "平安银行",
                        "relation_type": "业务匹配",
                        "association_score": 0.82,
                        "relation_path": "政策催化 -> 金融 IT 投入 -> 平安银行",
                    }
                ]
            )
            prediction_df = pd.DataFrame(
                [
                    {
                        "event_id": "evt-1",
                        "event_name": "政策催化事件",
                        "stock_code": "000001",
                        "stock_name": "平安银行",
                        "subject_type": "政策类事件",
                        "relation_type": "业务匹配",
                        "association_score": 0.82,
                        "car_4d": 0.036,
                        "prediction_score": 0.128,
                        "logic_chain": "政策催化 -> 业务匹配 -> 预期 CAR 上升",
                    }
                ]
            )
            final_picks = pd.DataFrame(
                [
                    {
                        "event_name": "政策催化事件",
                        "stock_code": "000001",
                        "stock_name": "平安银行",
                        "capital_ratio": 1.0,
                        "rank": 1,
                        "reason": "事件热度高，预测收益为正。",
                    }
                ]
            )
            detail_df = pd.DataFrame(
                [
                    {
                        "event_id": "evt-1",
                        "event_name": "政策催化事件",
                        "stock_code": "000001",
                        "stock_name": "平安银行",
                        "anchor_trade_date": "2026-04-21",
                        "trade_date": "2026-04-25",
                        "day_offset": 4,
                        "actual_return": 0.03,
                        "expected_return": 0.01,
                        "abnormal_return": 0.02,
                        "cumulative_abnormal_return": 0.04,
                        "cumulative_abnormal_return_0_2": 0.03,
                        "cumulative_abnormal_return_0_4": 0.04,
                        "sentiment_group": "正向事件",
                    }
                ]
            )
            stats_df = pd.DataFrame(
                [
                    {
                        "event_id": "evt-1",
                        "event_name": "政策催化事件",
                        "sample_size": 1,
                        "mean_ar_1d": 0.01,
                        "mean_car_0_2": 0.03,
                        "mean_car_0_4": 0.04,
                        "positive_ratio_0_4": 1.0,
                        "status_note": "窗口完整",
                    }
                ]
            )
            joint_df = pd.DataFrame(
                [
                    {
                        "group_label": "正向事件",
                        "day_offset": 4,
                        "mean_car": 0.04,
                        "sample_size": 1,
                        "note": "",
                    }
                ]
            )

            report_path = build_weekly_report(
                project_root=project_root,
                asof_date=pd.Timestamp("2026-04-20").date(),
                event_df=event_df,
                relation_df=relation_df,
                prediction_df=prediction_df,
                final_picks=final_picks,
                graph_paths=[],
                output_dir=output_dir,
                config=self._build_config(),
                event_study_artifacts=EventStudyArtifacts(
                    detail_df=detail_df,
                    stats_df=stats_df,
                    joint_mean_car_df=joint_df,
                    output_dir=study_dir,
                    joint_mean_car_path=joint_plot_path,
                ),
                industry_chain_artifacts=IndustryChainArtifacts(
                    relation_df=pd.DataFrame(),
                    summary_path=summary_path,
                    combined_png_path=png_path,
                    combined_html_path=html_path,
                    selected_events=["evt-1"],
                ),
                summary={
                    "event_count": 1,
                    "relation_count": 1,
                    "prediction_count": 1,
                    "selected_count": 1,
                    "trading_calendar_source": "tushare",
                    "trading_calendar_status_note": "交易日历来自 Tushare 实盘接口",
                },
            )

            content = report_path.read_text(encoding="utf-8")

        self.assertIn("典型事件完整展示", content)
        self.assertIn("模型性能实验", content)
        self.assertIn("数据来源与限制", content)

    def test_model_performance_excludes_future_weekly_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            output_dir = project_root / "outputs" / "weekly" / "2026-04-20"
            study_dir = output_dir / "event_study"
            kg_dir = output_dir / "kg_visual"
            study_dir.mkdir(parents=True, exist_ok=True)
            kg_dir.mkdir(parents=True, exist_ok=True)

            future_dir = project_root / "outputs" / "weekly" / "2026-04-27"
            (future_dir / "event_study").mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                [
                    {
                        "event_id": "evt-future",
                        "stock_code": "000002",
                        "subject_type": "公司类事件",
                        "car_4d": 0.12,
                        "prediction_score": 0.33,
                    }
                ]
            ).to_csv(future_dir / "predictions.csv", index=False)
            pd.DataFrame(
                [
                    {
                        "event_id": "evt-future",
                        "stock_code": "000002",
                        "day_offset": 4,
                        "cumulative_abnormal_return_0_4": 0.12,
                    }
                ]
            ).to_csv(future_dir / "event_study" / "event_study_detail.csv", index=False)

            summary_path = kg_dir / "industry_chain_summary.md"
            summary_path.write_text("图谱说明", encoding="utf-8")
            png_path = kg_dir / "industry_chain_graph.png"
            png_path.write_bytes(b"png")
            html_path = kg_dir / "industry_chain_graph.html"
            html_path.write_text("<html></html>", encoding="utf-8")
            joint_plot_path = study_dir / "joint_mean_car.png"
            joint_plot_path.write_bytes(b"png")

            event_df = pd.DataFrame(
                [
                    {
                        "event_id": "evt-1",
                        "event_name": "政策催化事件",
                        "subject_type": "政策类事件",
                        "duration_type": "中期型事件",
                        "predictability_type": "预披露型事件",
                        "industry_type": "科技类事件",
                        "heat_score": 0.8,
                        "intensity_score": 0.7,
                        "scope_score": 0.6,
                        "confidence_score": 0.9,
                    }
                ]
            )
            relation_df = pd.DataFrame(
                [
                    {
                        "event_id": "evt-1",
                        "event_name": "政策催化事件",
                        "stock_code": "000001",
                        "stock_name": "平安银行",
                        "relation_type": "业务匹配",
                        "association_score": 0.82,
                        "relation_path": "政策催化 -> 金融 IT 投入 -> 平安银行",
                    }
                ]
            )
            prediction_df = pd.DataFrame(
                [
                    {
                        "event_id": "evt-1",
                        "event_name": "政策催化事件",
                        "stock_code": "000001",
                        "stock_name": "平安银行",
                        "subject_type": "政策类事件",
                        "relation_type": "业务匹配",
                        "association_score": 0.82,
                        "car_4d": 0.036,
                        "prediction_score": 0.128,
                        "logic_chain": "政策催化 -> 业务匹配 -> 预期 CAR 上升",
                    }
                ]
            )
            detail_df = pd.DataFrame(
                [
                    {
                        "event_id": "evt-1",
                        "event_name": "政策催化事件",
                        "stock_code": "000001",
                        "stock_name": "平安银行",
                        "anchor_trade_date": "2026-04-21",
                        "trade_date": "2026-04-25",
                        "day_offset": 4,
                        "actual_return": 0.03,
                        "expected_return": 0.01,
                        "abnormal_return": 0.02,
                        "cumulative_abnormal_return": 0.04,
                        "cumulative_abnormal_return_0_2": 0.03,
                        "cumulative_abnormal_return_0_4": 0.04,
                        "sentiment_group": "正向事件",
                    }
                ]
            )

            report_path = build_weekly_report(
                project_root=project_root,
                asof_date=pd.Timestamp("2026-04-20").date(),
                event_df=event_df,
                relation_df=relation_df,
                prediction_df=prediction_df,
                final_picks=pd.DataFrame(),
                graph_paths=[],
                output_dir=output_dir,
                config=self._build_config(),
                event_study_artifacts=EventStudyArtifacts(
                    detail_df=detail_df,
                    stats_df=pd.DataFrame(),
                    joint_mean_car_df=pd.DataFrame(),
                    output_dir=study_dir,
                    joint_mean_car_path=joint_plot_path,
                ),
                industry_chain_artifacts=IndustryChainArtifacts(
                    relation_df=pd.DataFrame(),
                    summary_path=summary_path,
                    combined_png_path=png_path,
                    combined_html_path=html_path,
                    selected_events=[],
                ),
                summary={
                    "event_count": 1,
                    "relation_count": 1,
                    "prediction_count": 1,
                    "selected_count": 0,
                    "trading_calendar_source": "tushare",
                    "trading_calendar_status_note": "交易日历来自 Tushare 实盘接口",
                },
            )

            content = report_path.read_text(encoding="utf-8")

        self.assertIn("当前仅基于本次运行样本进行评估。", content)
        self.assertNotIn("已存在的 1 个周度样本", content)

    def test_report_does_not_list_missing_enhancement_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            output_dir = project_root / "outputs" / "weekly" / "2026-04-20"
            study_dir = output_dir / "event_study"
            kg_dir = output_dir / "kg_visual"
            output_dir.mkdir(parents=True, exist_ok=True)

            report_path = build_weekly_report(
                project_root=project_root,
                asof_date=pd.Timestamp("2026-04-20").date(),
                event_df=pd.DataFrame(),
                relation_df=pd.DataFrame(),
                prediction_df=pd.DataFrame(),
                final_picks=pd.DataFrame(),
                graph_paths=[],
                output_dir=output_dir,
                config=self._build_config(),
                event_study_artifacts=EventStudyArtifacts(
                    detail_df=pd.DataFrame(),
                    stats_df=pd.DataFrame(),
                    joint_mean_car_df=pd.DataFrame(),
                    output_dir=study_dir,
                    joint_mean_car_path=study_dir / "joint_mean_car.png",
                ),
                industry_chain_artifacts=IndustryChainArtifacts(
                    relation_df=pd.DataFrame(),
                    summary_path=kg_dir / "industry_chain_summary.md",
                    combined_png_path=kg_dir / "industry_chain_graph.png",
                    combined_html_path=kg_dir / "industry_chain_graph.html",
                    selected_events=[],
                ),
                summary={
                    "event_count": 0,
                    "relation_count": 0,
                    "prediction_count": 0,
                    "selected_count": 0,
                    "trading_calendar_source": "tushare",
                    "trading_calendar_status_note": "交易日历来自 Tushare 实盘接口",
                },
            )

            content = report_path.read_text(encoding="utf-8")

        self.assertIn("本次未生成事件研究产物。", content)
        self.assertIn("本次未生成产业链增强产物。", content)
        self.assertNotIn("joint_mean_car.png", content)
        self.assertNotIn("industry_chain_graph.png", content)


if __name__ == "__main__":
    unittest.main()
