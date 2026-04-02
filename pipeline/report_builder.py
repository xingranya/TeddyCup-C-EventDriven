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
    top_relations = relation_df.head(
        config.raw["report"]["top_relation_count"])
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
        event_study_stats_section = event_study_artifacts.stats_df.head(
            8).to_markdown(index=False)

    joint_mean_car_section = "暂无联合均值CAR结果。"
    if not event_study_artifacts.joint_mean_car_df.empty:
        joint_mean_car_section = event_study_artifacts.joint_mean_car_df.to_markdown(
            index=False)

    chain_relation_section = "暂无产业链图谱增强结果。"
    if not industry_chain_artifacts.relation_df.empty:
        chain_relation_section = industry_chain_artifacts.relation_df.head(10)[
            ["event_name", "theme_name", "link_name", "stock_name",
                "association_score", "chain_confidence", "relation_path"]
        ].to_markdown(index=False)

    chain_summary_text = "暂无图谱说明。"
    if industry_chain_artifacts.summary_path.exists():
        chain_summary_text = industry_chain_artifacts.summary_path.read_text(
            encoding="utf-8")

    # 生成投资决策推理过程
    reasoning_section = _generate_reasoning_section(
        final_picks, prediction_df, event_df)

    content = f"""# 事件驱动型股市投资策略周报

## 1. 运行概览
- 分析基准日：{asof_date.isoformat()}
- 候选事件数：{len(event_df)}
- 关联关系数：{len(relation_df)}
- 最终入选股票数：{len(final_picks)}

## 2. 研究方法论

### 事件识别与分类
本系统采用多源新闻聚合与文本聚类方法进行事件识别。新闻数据经标准化处理后，基于文本Jaccard相似度（阈值0.18）、
标题相似度（阈值0.35）、关键词共现和实体共现进行36小时滑窗聚类，将相关报道合并为独立事件。事件分类采用四维正交体系：
影响持续周期（脉冲/中期/长尾）、驱动主体（政策/公司/行业/宏观/地缘）、可预测性（突发/预披露）和行业属性。
每个事件提取热度、强度、范围和置信度四项量化特征。

### 事件关联挖掘
关联强度通过四维指标综合评估：直接提及（文本命中）、业务匹配（主营业务关键词重叠）、行业重叠（申万行业分类匹配）
和历史共振（价格相关性）。权重根据事件驱动主体类型动态调整，构建"事件-上市公司"知识图谱。

### 影响预测模型
采用市场模型（Market Model）进行事件研究：以[-60,-6]日为估计窗口，通过OLS回归拟合α和β，
计算事件窗口期[0,+4]日的异常收益(AR)和累计异常收益(CAR)。预测得分融合事件特征、关联强度、
基本面评分、流动性和风险惩罚五个维度。

### 投资策略
策略构建遵循"筛选-评分-分配"三步流程：首先进行基础过滤（排除ST股、低流动性股、次新股），
然后基于综合预测得分排序选取不超过3只标的，最后通过带约束的仓位分配算法确定资金比例。

## 3. 事件识别结果
{top_events[['event_name', 'subject_type', 'duration_type', 'predictability_type', 'industry_type', 'heat_score', 'intensity_score', 'scope_score', 'confidence_score']].to_markdown(index=False)}

## 4. 事件分类说明
- 事件按"持续周期、驱动主体、可预测性、行业属性"四个维度进行规则分类。
- 热度由事件聚类条数、来源权重和时间新鲜度综合计算。
- 强度重点考虑强刺激词、是否政策原文或公告、是否包含订单和业绩信息。
- 影响范围通过涉及公司数、行业广度和是否宏观/政策级事件综合刻画。

## 5. 关联图谱与关联公司
{top_relations[['event_name', 'stock_name', 'relation_type', 'association_score', 'relation_path']].to_markdown(index=False)}

{graph_lines}

## 6. 影响预测方法
- 估计窗口采用事件日前 60 到 6 个交易日。
- 市场基准采用沪深 300 指数。
- 对每个候选股票估计 beta、残差波动率、流动性得分和事件综合得分。
- 最终预测得分融合预期 4 日 CAR、关联强度、事件强度、流动性和风险惩罚。

## 7. 事件研究增强结果
- 正常收益模型采用“市场调整模型 + 单因子市场模型”，主表默认输出单因子市场模型结果。
- 事件锚点按完整发布时间与收盘时点共同确定，收盘后事件顺延到下一交易日。
- 事件窗口用于可视化扩展到 `[-1, +10]`，汇总统计严格使用 `AR(+1)`、`CAR(0,2)`、`CAR(0,4)`。

{event_study_lines}

### 7.1 事件研究统计表
{event_study_stats_section}

### 7.2 联合均值 CAR 汇总
{joint_mean_car_section}

## 8. 产业链图谱增强结果
{chain_graph_lines}

### 8.1 产业链关系表
{chain_relation_section}

### 8.2 图谱说明
{chain_summary_text}

## 9. 逻辑链条说明
{top_predictions[['event_name', 'stock_name', 'car_4d', 'prediction_score', 'logic_chain']].to_markdown(index=False)}

## 10. 本周投资决策
{final_picks[['event_name', 'stock_code', 'capital_ratio', 'rank', 'reason']].to_markdown(index=False)}

### 10.1 投资决策推理过程
{reasoning_section}

## 11. 历史回测摘要
{backtest_section}
"""
    report_path.write_text(content, encoding="utf-8")
    return report_path


def _generate_reasoning_section(
    final_picks: pd.DataFrame,
    prediction_df: pd.DataFrame,
    event_df: pd.DataFrame,
) -> str:
    """生成每只选中股票的详细推理链。"""
    if final_picks.empty:
        return "暂无选股结果。"

    lines = []
    for _, pick in final_picks.iterrows():
        stock_code = pick.get("stock_code", "N/A")
        stock_name = pick.get("stock_name", "N/A")
        event_name = pick.get("event_name", "N/A")
        capital_ratio = pick.get("capital_ratio", 0.0)
        reason = pick.get("reason", "")

        # 从 prediction_df 查找对应记录
        pred_match = prediction_df[
            (prediction_df["stock_code"] == stock_code) |
            (prediction_df["stock_name"] == stock_name)
        ]
        if not pred_match.empty:
            pred = pred_match.iloc[0]
            car_4d = pred.get("car_4d", 0.0)
            prediction_score = pred.get("prediction_score", 0.0)
            association_score = pred.get("association_score", 0.0)
            relation_type = pred.get("relation_type", "N/A")
        else:
            car_4d = 0.0
            prediction_score = 0.0
            association_score = 0.0
            relation_type = "N/A"

        # 从 event_df 查找事件特征
        event_match = event_df[event_df["event_name"] == event_name]
        if not event_match.empty:
            evt = event_match.iloc[0]
            heat_score = evt.get("heat_score", 0.0)
            intensity_score = evt.get("intensity_score", 0.0)
            scope_score = evt.get("scope_score", 0.0)
        else:
            heat_score = 0.0
            intensity_score = 0.0
            scope_score = 0.0

        # 格式化关联强度描述
        relation_desc = relation_type
        if relation_type == "direct_mention":
            relation_desc = "直接提及"
        elif relation_type == "business_match":
            relation_desc = "业务匹配"
        elif relation_type == "industry_overlap":
            relation_desc = "行业重叠"
        elif relation_type == "historical_resonance":
            relation_desc = "历史共振"

        section = f"""**{stock_name} ({stock_code})**
- 关联事件：{event_name}
- 事件特征：热度 {heat_score:.2f}，强度 {intensity_score:.2f}，范围 {scope_score:.2f}
- 关联强度：{association_score:.2f}（{relation_desc}）
- 预期异常收益(CAR)：{car_4d:+.2f}%
- 综合评分：{prediction_score:.3f}
- 资金分配：{capital_ratio*100:.0f}%
- 选股理由：{reason}
"""
        lines.append(section)

    return "\n".join(lines)
