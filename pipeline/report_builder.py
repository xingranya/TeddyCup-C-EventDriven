from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.event_study_enhanced import EventStudyArtifacts
from pipeline.industry_chain_enhanced import IndustryChainArtifacts
from pipeline.models import AppConfig
from pipeline.utils import normalize_stock_code


def build_weekly_report(
    project_root: Path,
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
    summary: dict,
    backtest_summary: pd.DataFrame | None = None,
) -> Path:
    """生成 Markdown 报告。"""

    report_path = output_dir / "report.md"
    top_events = event_df.head(config.raw["report"]["top_event_count"])
    top_relations = relation_df.head(config.raw["report"]["top_relation_count"])
    top_predictions = prediction_df.head(10)

    graph_lines = "\n".join(
        f"- 图谱文件：`{path.name}`" for path in graph_paths if path.exists()
    ) or "- 本次未生成图谱文件"
    chain_graph_lines = _build_artifact_lines(
        [
            ("增强图谱 PNG", industry_chain_artifacts.combined_png_path),
            ("增强图谱 HTML", industry_chain_artifacts.combined_html_path),
            ("图谱说明", industry_chain_artifacts.summary_path),
        ],
        "本次未生成产业链增强产物。",
    )
    event_study_lines = _build_artifact_lines(
        [
            ("事件研究明细", event_study_artifacts.output_dir / "event_study_detail.csv"),
            ("事件研究统计", event_study_artifacts.output_dir / "event_study_stats.csv"),
            ("联合均值CAR图", event_study_artifacts.joint_mean_car_path),
            ("联合均值CAR数据", event_study_artifacts.output_dir / "joint_mean_car.csv"),
        ],
        "本次未生成事件研究产物。",
    )
    backtest_section = "本周运行未附带历史回测摘要。"
    if backtest_summary is not None and not backtest_summary.empty:
        backtest_section = backtest_summary.to_markdown(index=False)

    event_study_stats_section = _to_markdown(
        event_study_artifacts.stats_df.head(8),
        ["event_name", "sample_size", "mean_ar_1d", "mean_car_0_2", "mean_car_0_4", "positive_ratio_0_4", "status_note"],
        "暂无事件研究统计结果。",
    )
    joint_mean_car_section = _to_markdown(
        event_study_artifacts.joint_mean_car_df,
        ["group_label", "day_offset", "mean_car", "sample_size", "note"],
        "暂无联合均值CAR结果。",
    )
    chain_relation_section = _to_markdown(
        industry_chain_artifacts.relation_df.head(10),
        ["event_name", "theme_name", "link_name", "stock_name", "association_score", "chain_confidence", "relation_path"],
        "暂无产业链图谱增强结果。",
    )
    chain_summary_text = "暂无图谱说明。"
    if industry_chain_artifacts.summary_path.exists():
        chain_summary_text = industry_chain_artifacts.summary_path.read_text(encoding="utf-8")

    reasoning_section = _generate_reasoning_section(final_picks, prediction_df, relation_df, event_df)
    typical_event_section = _build_typical_event_section(event_df, relation_df, prediction_df, final_picks)
    performance_section = _build_model_performance_section(
        project_root=project_root,
        asof_date=asof_date,
        prediction_df=prediction_df,
        event_study_detail_df=event_study_artifacts.detail_df,
        max_positions=config.max_positions,
    )
    data_source_section = _build_data_source_section(config, summary)

    content = f"""# 事件驱动型股市投资策略周报

## 1. 运行概览
- 分析基准日：{asof_date.isoformat()}
- 候选事件数：{summary.get('event_count', len(event_df))}
- 关联关系数：{summary.get('relation_count', len(relation_df))}
- 预测结果数：{summary.get('prediction_count', len(prediction_df))}
- 最终入选股票数：{summary.get('selected_count', len(final_picks))}
- 交易日历来源：{summary.get('trading_calendar_source', 'unknown')}
- 交易日历说明：{summary.get('trading_calendar_status_note', '未记录')}

## 2. 研究方法论

### 事件识别与分类
本系统采用多源新闻聚合与文本聚类方法进行事件识别。新闻数据经标准化处理后，基于文本 Jaccard 相似度、标题相似度、
关键词共现和实体共现进行 36 小时滑窗聚类，将相关报道合并为独立事件。事件分类采用四维正交体系：
影响持续周期、驱动主体、可预测性和行业属性。每个事件提取热度、强度、范围和置信度四项量化特征。

### 事件关联挖掘
关联强度通过四维指标综合评估：直接提及、业务匹配、行业重叠和历史共振。基础权重由配置文件统一控制，
若按事件主体类型做动态调整，则通过配置中的倍率 profile 进行归一化，不再依赖代码硬编码。

### 影响预测模型
采用市场模型（Market Model）进行事件研究：以 [-60,-6] 日为估计窗口，通过 OLS 回归拟合 α 和 β，
计算事件窗口期 [0,+4] 日的异常收益（AR）和累计异常收益（CAR）。预测得分融合预期 4 日 CAR、关联强度、
事件特征、流动性和风险惩罚。

### 投资策略
策略构建遵循“筛选-评分-分配”三步流程：先进行基础过滤和停牌约束，再基于综合预测得分排序选取不超过 3 只标的，
最后通过带上下限约束的仓位分配与最大余数法舍入生成提交仓位。

## 3. 事件识别结果
{_to_markdown(top_events, ['event_name', 'subject_type', 'duration_type', 'predictability_type', 'industry_type', 'heat_score', 'intensity_score', 'scope_score', 'confidence_score'], '暂无事件识别结果。')}

## 4. 典型事件完整展示
{typical_event_section}

## 5. 关联图谱与关联公司
{_to_markdown(top_relations, ['event_name', 'stock_name', 'relation_type', 'association_score', 'relation_path'], '暂无关联关系结果。')}

{graph_lines}

## 6. 影响预测与逻辑链条
{_to_markdown(top_predictions, ['event_name', 'stock_name', 'association_score', 'car_4d', 'prediction_score', 'logic_chain'], '暂无影响预测结果。')}

## 7. 事件研究增强结果
- 正常收益模型采用“市场调整模型 + 单因子市场模型”，主表默认输出单因子市场模型结果。
- 事件锚点按完整发布时间与收盘时间共同确定，收盘后事件顺延到下一交易日。
- 事件窗口用于可视化扩展到 `[-1, +10]`，汇总统计严格使用 `AR(+1)`、`CAR(0,2)`、`CAR(0,4)`。

{event_study_lines}

### 7.1 事件研究统计表
{event_study_stats_section}

### 7.2 联合均值 CAR 汇总
{joint_mean_car_section}

## 8. 模型性能实验
{performance_section}

## 9. 产业链图谱增强结果
{chain_graph_lines}

### 9.1 产业链关系表
{chain_relation_section}

### 9.2 图谱说明
{chain_summary_text}

## 10. 本周投资决策
{_to_markdown(final_picks, ['event_name', 'stock_code', 'capital_ratio', 'rank', 'reason'], '本周未形成投资决策。')}

### 10.1 投资决策推理过程
{reasoning_section}

## 11. 数据来源与限制
{data_source_section}

## 12. 历史回测摘要
{backtest_section}
"""
    report_path.write_text(content, encoding="utf-8")
    return report_path


def _to_markdown(df: pd.DataFrame, columns: list[str], empty_text: str) -> str:
    """将数据表安全转换为 Markdown。"""

    if df is None or df.empty:
        return empty_text
    available_columns = [column for column in columns if column in df.columns]
    if not available_columns:
        return empty_text
    return df[available_columns].to_markdown(index=False)


def _build_artifact_lines(entries: list[tuple[str, Path]], empty_text: str) -> str:
    """仅列出真实存在的产物文件。"""

    existing_lines = [
        f"- {label}：`{path.name}`"
        for label, path in entries
        if path.exists()
    ]
    return "\n".join(existing_lines) or empty_text


def _build_typical_event_section(
    event_df: pd.DataFrame,
    relation_df: pd.DataFrame,
    prediction_df: pd.DataFrame,
    final_picks: pd.DataFrame,
) -> str:
    """构建典型事件完整展示章节。"""

    if event_df.empty:
        return "暂无可展示的典型事件。"

    event_meta = event_df.copy()
    event_meta["event_score"] = (
        0.30 * event_meta["heat_score"]
        + 0.35 * event_meta["intensity_score"]
        + 0.20 * event_meta["scope_score"]
        + 0.15 * event_meta["confidence_score"]
    ).round(4)
    relation_score = (
        relation_df.groupby("event_id")["association_score"].max().rename("max_association_score")
        if not relation_df.empty and "association_score" in relation_df.columns
        else pd.Series(dtype="float64")
    )
    prediction_score = (
        prediction_df.groupby("event_id")["prediction_score"].max().rename("max_prediction_score")
        if not prediction_df.empty and "prediction_score" in prediction_df.columns
        else pd.Series(dtype="float64")
    )
    event_meta = event_meta.merge(relation_score, on="event_id", how="left")
    event_meta = event_meta.merge(prediction_score, on="event_id", how="left")
    event_meta["max_association_score"] = event_meta["max_association_score"].fillna(0.0)
    event_meta["max_prediction_score"] = event_meta["max_prediction_score"].fillna(0.0)
    event_meta["case_score"] = (
        0.5 * event_meta["event_score"]
        + 0.3 * event_meta["max_association_score"]
        + 0.2 * event_meta["max_prediction_score"]
    ).round(4)

    if "event_name" in final_picks.columns:
        picked_event_names = set(final_picks["event_name"].dropna().astype(str).tolist())
    else:
        picked_event_names = set()
    if picked_event_names:
        preferred = event_meta[event_meta["event_name"].isin(picked_event_names)].copy()
        chosen = preferred.sort_values("case_score", ascending=False).head(1)
        if chosen.empty:
            chosen = event_meta.sort_values("case_score", ascending=False).head(1)
    else:
        chosen = event_meta.sort_values("case_score", ascending=False).head(1)
    if chosen.empty:
        return "暂无可展示的典型事件。"

    event_row = chosen.iloc[0]
    event_id = event_row["event_id"]
    event_name = event_row["event_name"]
    related_companies = relation_df[relation_df["event_id"] == event_id].copy().head(5)
    prediction_rows = prediction_df[prediction_df["event_id"] == event_id].copy().head(5)
    if "event_name" in final_picks.columns:
        selected_rows = final_picks[final_picks["event_name"] == event_name].copy()
    else:
        selected_rows = pd.DataFrame()
    selected_text = _to_markdown(
        selected_rows,
        ["stock_code", "stock_name", "capital_ratio", "rank"],
        "该典型事件本周未进入最终持仓，但已进入候选评估链路。",
    )

    return f"""### 4.1 典型事件
- 事件名称：{event_name}
- 事件分类：{event_row.get('subject_type', '未知')} / {event_row.get('duration_type', '未知')} / {event_row.get('predictability_type', '未知')} / {event_row.get('industry_type', '未知')}
- 事件量化特征：热度 {event_row.get('heat_score', 0):.2f}，强度 {event_row.get('intensity_score', 0):.2f}，范围 {event_row.get('scope_score', 0):.2f}，置信度 {event_row.get('confidence_score', 0):.2f}
- 选择原因：该事件在事件强度、关联强度和预测分数三方面的综合得分最高，适合作为赛题要求的典型事件展示。

### 4.2 关联公司挖掘
{_to_markdown(related_companies, ['stock_name', 'relation_type', 'association_score', 'relation_path'], '暂无关联公司结果。')}

### 4.3 股价影响预测
{_to_markdown(prediction_rows, ['stock_name', 'association_score', 'car_4d', 'prediction_score', 'logic_chain'], '暂无该事件的预测结果。')}

### 4.4 是否进入最终投资决策
{selected_text}
"""


def _build_model_performance_section(
    project_root: Path,
    asof_date,
    prediction_df: pd.DataFrame,
    event_study_detail_df: pd.DataFrame,
    max_positions: int,
) -> str:
    """构建模型性能实验章节。"""

    evaluation_frames: list[pd.DataFrame] = []
    weekly_root = project_root / "outputs" / "weekly"
    asof_date_value = pd.Timestamp(asof_date).date()
    if weekly_root.exists():
        for week_dir in sorted(weekly_root.iterdir()):
            if not week_dir.is_dir():
                continue
            try:
                week_date = pd.Timestamp(week_dir.name).date()
            except Exception:
                continue
            if week_date > asof_date_value:
                continue
            pred_path = week_dir / "predictions.csv"
            detail_path = week_dir / "event_study" / "event_study_detail.csv"
            if not pred_path.exists() or not detail_path.exists():
                continue
            try:
                history_prediction_df = pd.read_csv(pred_path)
                history_detail_df = pd.read_csv(detail_path)
            except Exception:
                continue
            evaluation_df = _prepare_prediction_evaluation(
                history_prediction_df,
                history_detail_df,
                week_dir.name,
            )
            if not evaluation_df.empty:
                evaluation_frames.append(evaluation_df)

    if not evaluation_frames:
        evaluation_df = _prepare_prediction_evaluation(
            prediction_df,
            event_study_detail_df,
            "current_run",
        )
        if evaluation_df.empty:
            return "暂无可用于性能实验的预测-实现对照样本。"
        data_scope_text = "当前仅基于本次运行样本进行评估。"
    else:
        evaluation_df = pd.concat(evaluation_frames, ignore_index=True)
        data_scope_text = f"基于 `outputs/weekly` 中已存在的 {evaluation_df['week_label'].nunique()} 个周度样本进行评估。"

    weekly_rows = []
    for week_label, week_group in evaluation_df.groupby("week_label"):
        top_group = week_group.sort_values("prediction_score", ascending=False).head(max_positions)
        weekly_rows.append(
            {
                "week_label": week_label,
                "sample_pairs": int(len(week_group)),
                "direction_accuracy": round(float(week_group["direction_correct"].mean()), 4),
                "top_k_hit_rate": round(float((top_group["actual_car_0_4"] > 0).mean()), 4),
                "score_actual_spearman": _safe_spearman(week_group["prediction_score"], week_group["actual_car_0_4"]),
            }
        )
    weekly_summary_df = pd.DataFrame(weekly_rows).sort_values("week_label")

    subject_summary_df = (
        evaluation_df.groupby("subject_type")
        .agg(
            sample_pairs=("event_id", "count"),
            direction_accuracy=("direction_correct", "mean"),
            mean_predicted_car_4d=("car_4d", "mean"),
            mean_actual_car_0_4=("actual_car_0_4", "mean"),
        )
        .reset_index()
    )
    subject_summary_df["direction_accuracy"] = subject_summary_df["direction_accuracy"].round(4)
    subject_summary_df["mean_predicted_car_4d"] = subject_summary_df["mean_predicted_car_4d"].round(4)
    subject_summary_df["mean_actual_car_0_4"] = subject_summary_df["mean_actual_car_0_4"].round(4)

    overall_direction_accuracy = round(float(evaluation_df["direction_correct"].mean()), 4)
    overall_top_k = round(
        float(
            evaluation_df.sort_values(["week_label", "prediction_score"], ascending=[True, False])
            .groupby("week_label")
            .head(max_positions)["actual_car_0_4"]
            .gt(0)
            .mean()
        ),
        4,
    )
    overall_spearman = _safe_spearman(evaluation_df["prediction_score"], evaluation_df["actual_car_0_4"])

    return f"""- 样本说明：{data_scope_text}
- 方向准确率：{overall_direction_accuracy:.2%}
- Top-{max_positions} 命中率：{overall_top_k:.2%}
- 预测分数与实现 CAR(0,4) 的秩相关：{_format_metric(overall_spearman)}

### 8.1 周度实验汇总
{_to_markdown(weekly_summary_df, ['week_label', 'sample_pairs', 'direction_accuracy', 'top_k_hit_rate', 'score_actual_spearman'], '暂无周度实验汇总。')}

### 8.2 按事件主体类型分组表现
{_to_markdown(subject_summary_df, ['subject_type', 'sample_pairs', 'direction_accuracy', 'mean_predicted_car_4d', 'mean_actual_car_0_4'], '暂无按主体类型分组的实验结果。')}
"""


def _prepare_prediction_evaluation(
    prediction_df: pd.DataFrame,
    detail_df: pd.DataFrame,
    week_label: str,
) -> pd.DataFrame:
    """构建预测值与实现值的对照样本。"""

    if prediction_df is None or prediction_df.empty or detail_df is None or detail_df.empty:
        return pd.DataFrame()
    required_prediction_cols = {"event_id", "stock_code", "car_4d", "prediction_score"}
    required_detail_cols = {"event_id", "stock_code", "day_offset", "cumulative_abnormal_return_0_4"}
    if not required_prediction_cols.issubset(prediction_df.columns) or not required_detail_cols.issubset(detail_df.columns):
        return pd.DataFrame()

    prediction_meta = prediction_df.copy()
    prediction_meta["stock_code"] = prediction_meta["stock_code"].map(normalize_stock_code)
    detail_meta = detail_df.copy()
    detail_meta["stock_code"] = detail_meta["stock_code"].map(normalize_stock_code)
    detail_meta["day_offset"] = pd.to_numeric(detail_meta["day_offset"], errors="coerce")
    actual_df = detail_meta[detail_meta["day_offset"] == 4][
        ["event_id", "stock_code", "cumulative_abnormal_return_0_4"]
    ].rename(columns={"cumulative_abnormal_return_0_4": "actual_car_0_4"})
    merged = prediction_meta.merge(actual_df, on=["event_id", "stock_code"], how="inner")
    if merged.empty:
        return pd.DataFrame()
    merged["subject_type"] = merged.get("subject_type", pd.Series("未知", index=merged.index)).fillna("未知")
    merged["direction_correct"] = merged["car_4d"].apply(_direction_sign) == merged["actual_car_0_4"].apply(_direction_sign)
    merged["week_label"] = week_label
    return merged[
        ["week_label", "event_id", "subject_type", "stock_code", "car_4d", "prediction_score", "actual_car_0_4", "direction_correct"]
    ].copy()


def _build_data_source_section(config: AppConfig, summary: dict) -> str:
    """生成数据来源与限制章节。"""

    qstock_status = "启用" if config.qstock_enabled else "关闭"
    return f"""- 事件数据：优先读取项目内“采集 -> 标准化 -> 审核 -> 发布”流程生成的 `data/events/policy|announcement|industry|macro` 正式事件文件；qstock 自动采集当前默认 {qstock_status}。
- 行情与基准：正式运行优先使用 Tushare，公开接口仅作为降级备选。
- 财务与停复牌：依赖 Tushare / Akshare 可见口径，实际覆盖度受接口权限、公告披露时点和缓存完整性影响。
- 交易日历：本次运行使用 `{summary.get('trading_calendar_source', 'unknown')}`，说明为“{summary.get('trading_calendar_status_note', '未记录')}”。
- 结果限制：当前策略仍以启发式事件评分和事件研究为主，模型性能实验依赖历史输出样本，不等同于严格的样本外机器学习回测。
"""


def _generate_reasoning_section(
    final_picks: pd.DataFrame,
    prediction_df: pd.DataFrame,
    relation_df: pd.DataFrame,
    event_df: pd.DataFrame,
) -> str:
    """生成每只选中股票的详细推理链。"""

    if final_picks.empty:
        return "暂无选股结果。"

    prediction_meta = prediction_df.copy()
    if "stock_code" in prediction_meta.columns:
        prediction_meta["stock_code"] = prediction_meta["stock_code"].map(normalize_stock_code)
    relation_meta = relation_df.copy()
    if "stock_code" in relation_meta.columns:
        relation_meta["stock_code"] = relation_meta["stock_code"].map(normalize_stock_code)

    lines = []
    for _, pick in final_picks.iterrows():
        stock_code = normalize_stock_code(pick.get("stock_code"))
        stock_name = pick.get("stock_name", "N/A")
        event_name = pick.get("event_name", "N/A")
        capital_ratio = float(pick.get("capital_ratio", 0.0) or 0.0)
        reason = pick.get("reason", "")

        pred_match = prediction_meta[
            (prediction_meta.get("stock_code") == stock_code)
            & (prediction_meta.get("event_name") == event_name)
        ]
        if not pred_match.empty:
            pred = pred_match.iloc[0]
            car_4d = float(pred.get("car_4d", 0.0) or 0.0)
            prediction_score = float(pred.get("prediction_score", 0.0) or 0.0)
        else:
            car_4d = 0.0
            prediction_score = 0.0

        relation_match = relation_meta[
            (relation_meta.get("stock_code") == stock_code)
            & (relation_meta.get("event_name") == event_name)
        ].sort_values("association_score", ascending=False)
        if not relation_match.empty:
            relation_row = relation_match.iloc[0]
            association_score = float(relation_row.get("association_score", 0.0) or 0.0)
            relation_type = _relation_type_label(str(relation_row.get("relation_type", "未知")))
        else:
            association_score = 0.0
            relation_type = "未知"

        event_match = event_df[event_df["event_name"] == event_name]
        if not event_match.empty:
            evt = event_match.iloc[0]
            heat_score = float(evt.get("heat_score", 0.0) or 0.0)
            intensity_score = float(evt.get("intensity_score", 0.0) or 0.0)
            scope_score = float(evt.get("scope_score", 0.0) or 0.0)
        else:
            heat_score = 0.0
            intensity_score = 0.0
            scope_score = 0.0

        section = f"""**{stock_name} ({stock_code})**
- 关联事件：{event_name}
- 事件特征：热度 {heat_score:.2f}，强度 {intensity_score:.2f}，范围 {scope_score:.2f}
- 关联强度：{association_score:.2f}（{relation_type}）
- 预期异常收益 CAR(0,4)：{car_4d:+.2%}
- 综合评分：{prediction_score:.3f}
- 资金分配：{capital_ratio:.2%}
- 选股理由：{reason}
"""
        lines.append(section)

    return "\n".join(lines)


def _relation_type_label(relation_type: str) -> str:
    """将关系类型标签转成报告可读文本。"""

    mapping = {
        "direct_mention": "直接提及",
        "business_match": "业务匹配",
        "industry_overlap": "行业重叠",
        "historical_resonance": "历史共振",
    }
    return mapping.get(relation_type, relation_type or "未知")


def _direction_sign(value: float) -> int:
    """返回方向符号。"""

    if pd.isna(value):
        return 0
    if float(value) > 0:
        return 1
    if float(value) < 0:
        return -1
    return 0


def _safe_spearman(left: pd.Series, right: pd.Series) -> float | None:
    """安全计算 Spearman 秩相关。"""

    valid_df = pd.DataFrame({"left": left, "right": right}).dropna()
    if len(valid_df) < 2:
        return None
    if valid_df["left"].nunique() < 2 or valid_df["right"].nunique() < 2:
        return None
    correlation = valid_df["left"].corr(valid_df["right"], method="spearman")
    if pd.isna(correlation):
        return None
    return round(float(correlation), 4)


def _format_metric(value: float | None) -> str:
    """格式化可选数值。"""

    if value is None:
        return "样本不足"
    return f"{value:.4f}"
