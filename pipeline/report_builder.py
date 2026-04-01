from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.event_study_enhanced import EventStudyArtifacts
from pipeline.industry_chain_enhanced import IndustryChainArtifacts
from pipeline.models import AppConfig


def build_weekly_report(
    asof_date,
    event_df: pd.DataFrame,
    relation_df: pd.DataFrame,
    prediction_df: pd.DataFrame,
    final_picks: pd.DataFrame,
    graph_paths: list[Path],
    output_dir: Path,
    config: AppConfig,
    event_study_artifacts: EventStudyArtifacts,
    industry_chain_artifacts: IndustryChainArtifacts,
    backtest_summary: pd.DataFrame | None = None,
) -> Path:
    """生成 Markdown 报告。"""

    report_path = output_dir / "report.md"
    top_events = event_df.head(config.raw["report"]["top_event_count"])
    top_relations = relation_df.head(config.raw["report"]["top_relation_count"])
    top_predictions = prediction_df.head(10)

    graph_lines = "\n".join(
        f"- 图谱文件：`{path.name}`" for path in graph_paths
    ) or "- 本次未生成图谱文件"
    chain_graph_lines = "\n".join(
        [
            f"- 增强图谱 PNG：`{industry_chain_artifacts.combined_png_path.name}`",
            f"- 增强图谱 HTML：`{industry_chain_artifacts.combined_html_path.name}`",
            f"- 图谱说明：`{industry_chain_artifacts.summary_path.name}`",
        ]
    )
    event_study_lines = "\n".join(
        [
            f"- 事件研究明细：`{(event_study_artifacts.output_dir / 'event_study_detail.csv').name}`",
            f"- 事件研究统计：`{(event_study_artifacts.output_dir / 'event_study_stats.csv').name}`",
            f"- 联合均值CAR图：`{event_study_artifacts.joint_mean_car_path.name}`",
            f"- 联合均值CAR数据：`{(event_study_artifacts.output_dir / 'joint_mean_car.csv').name}`",
        ]
    )

    backtest_section = "本周运行未附带历史回测摘要。"
    if backtest_summary is not None and not backtest_summary.empty:
        backtest_section = backtest_summary.to_markdown(index=False)

    event_study_stats_section = "暂无事件研究统计结果。"
    if not event_study_artifacts.stats_df.empty:
        event_study_stats_section = event_study_artifacts.stats_df.head(8).to_markdown(index=False)

    joint_mean_car_section = "暂无联合均值CAR结果。"
    if not event_study_artifacts.joint_mean_car_df.empty:
        joint_mean_car_section = event_study_artifacts.joint_mean_car_df.to_markdown(index=False)

    chain_relation_section = "暂无产业链图谱增强结果。"
    if not industry_chain_artifacts.relation_df.empty:
        chain_relation_section = industry_chain_artifacts.relation_df.head(10)[
            ["event_name", "theme_name", "link_name", "stock_name", "association_score", "chain_confidence", "relation_path"]
        ].to_markdown(index=False)

    chain_summary_text = "暂无图谱说明。"
    if industry_chain_artifacts.summary_path.exists():
        chain_summary_text = industry_chain_artifacts.summary_path.read_text(encoding="utf-8")

    content = f"""# 事件驱动型股市投资策略周报

## 1. 运行概览
- 分析基准日：{asof_date.isoformat()}
- 候选事件数：{len(event_df)}
- 关联关系数：{len(relation_df)}
- 最终入选股票数：{len(final_picks)}

## 2. 事件识别结果
{top_events[['event_name', 'subject_type', 'duration_type', 'predictability_type', 'industry_type', 'heat_score', 'intensity_score', 'scope_score', 'confidence_score']].to_markdown(index=False)}

## 3. 事件分类说明
- 事件按“持续周期、驱动主体、可预测性、行业属性”四个维度进行规则分类。
- 热度由事件聚类条数、来源权重和时间新鲜度综合计算。
- 强度重点考虑强刺激词、是否政策原文或公告、是否包含订单和业绩信息。
- 影响范围通过涉及公司数、行业广度和是否宏观/政策级事件综合刻画。

## 4. 关联图谱与关联公司
{top_relations[['event_name', 'stock_name', 'relation_type', 'association_score', 'relation_path']].to_markdown(index=False)}

{graph_lines}

## 5. 影响预测方法
- 估计窗口采用事件日前 60 到 6 个交易日。
- 市场基准采用沪深 300 指数。
- 对每个候选股票估计 beta、残差波动率、流动性得分和事件综合得分。
- 最终预测得分融合预期 4 日 CAR、关联强度、事件强度、流动性和风险惩罚。

## 6. 事件研究增强结果
- 正常收益模型采用“市场调整模型 + 单因子市场模型”，主表默认输出单因子市场模型结果，市场调整模型作为对照列保留在明细表中。
- 事件窗口用于可视化扩展到 `[-1, +10]`，汇总统计重点使用 `+1/+2/+4` 日的 AR/CAR。

{event_study_lines}

### 6.1 事件研究统计表
{event_study_stats_section}

### 6.2 联合均值 CAR 汇总
{joint_mean_car_section}

## 7. 产业链图谱增强结果
{chain_graph_lines}

### 7.1 产业链关系表
{chain_relation_section}

### 7.2 图谱说明
{chain_summary_text}

## 8. 逻辑链条说明
{top_predictions[['event_name', 'stock_name', 'car_4d', 'prediction_score', 'logic_chain']].to_markdown(index=False)}

## 9. 本周投资决策
{final_picks[['event_name', 'stock_code', 'capital_ratio', 'rank', 'reason']].to_markdown(index=False)}

## 10. 历史回测摘要
{backtest_section}
"""
    report_path.write_text(content, encoding="utf-8")
    return report_path
