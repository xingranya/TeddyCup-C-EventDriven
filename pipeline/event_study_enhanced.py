from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from pipeline.models import AppConfig
from pipeline.utils import ensure_directory, save_dataframe


@dataclass(slots=True)
class EventStudyArtifacts:
    """事件研究增强阶段产物。"""

    detail_df: pd.DataFrame
    stats_df: pd.DataFrame
    joint_mean_car_df: pd.DataFrame
    output_dir: Path
    joint_mean_car_path: Path


def run_event_study_enhanced(
    event_df: pd.DataFrame,
    relation_df: pd.DataFrame,
    price_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
    output_dir: Path,
    config: AppConfig,
) -> EventStudyArtifacts:
    """生成标准化事件研究明细、统计表和联合均值 CAR 图。"""

    study_dir = ensure_directory(output_dir / "event_study")
    if relation_df.empty:
        empty_detail = pd.DataFrame(
            columns=[
                "event_id",
                "event_name",
                "stock_code",
                "stock_name",
                "day_offset",
                "actual_return",
                "expected_return",
                "abnormal_return",
                "cumulative_abnormal_return",
            ]
        )
        empty_stats = pd.DataFrame(
            columns=[
                "event_id",
                "event_name",
                "sample_size",
                "mean_ar_1d",
                "mean_car_2d",
                "mean_car_4d",
                "std_car_4d",
                "positive_ratio",
            ]
        )
        empty_joint = pd.DataFrame(columns=["group_label", "day_offset", "mean_car", "sample_size", "note"])
        save_dataframe(empty_detail, study_dir / "event_study_detail")
        save_dataframe(empty_stats, study_dir / "event_study_stats")
        save_dataframe(empty_joint, study_dir / "joint_mean_car")
        joint_mean_car_path = study_dir / "joint_mean_car.png"
        _render_empty_joint_plot(joint_mean_car_path)
        return EventStudyArtifacts(empty_detail, empty_stats, empty_joint, study_dir, joint_mean_car_path)

    benchmark_returns = _prepare_return_series(benchmark_df)
    stock_returns = _prepare_return_series(price_df)
    event_meta = event_df.set_index("event_id")[
        ["event_name", "publish_time", "sentiment_score", "subject_type", "industry_type"]
    ].copy()
    event_meta["publish_time"] = pd.to_datetime(event_meta["publish_time"])

    detail_rows: list[dict] = []
    for _, row in relation_df.iterrows():
        event_id = row["event_id"]
        if event_id not in event_meta.index:
            continue
        meta = event_meta.loc[event_id]
        event_date = meta["publish_time"].date()
        stock_history = stock_returns[stock_returns["stock_code"] == row["stock_code"]].copy()
        common_calendar = sorted(
            set(pd.to_datetime(stock_history["trade_date"]).dt.date.tolist())
            & set(pd.to_datetime(benchmark_returns["trade_date"]).dt.date.tolist())
        )
        if not common_calendar:
            continue
        anchor_date = _locate_anchor_trade_date(common_calendar, event_date)
        if anchor_date is None:
            continue
        market_model = _estimate_market_model(stock_history, benchmark_returns, anchor_date)
        event_window_df = _build_event_window(
            stock_history=stock_history,
            benchmark_returns=benchmark_returns,
            market_calendar=common_calendar,
            anchor_date=anchor_date,
            start_offset=-1,
            end_offset=10,
            market_model=market_model,
        )
        if event_window_df.empty:
            continue
        event_window_df["event_id"] = event_id
        event_window_df["event_name"] = meta["event_name"]
        event_window_df["stock_code"] = row["stock_code"]
        event_window_df["stock_name"] = row["stock_name"]
        event_window_df["sentiment_group"] = "正向事件" if float(meta["sentiment_score"]) >= 0 else "负向事件"
        detail_rows.extend(event_window_df.to_dict(orient="records"))

    detail_df = pd.DataFrame(detail_rows)
    if detail_df.empty:
        detail_df = pd.DataFrame(
            columns=[
                "event_id",
                "event_name",
                "stock_code",
                "stock_name",
                "day_offset",
                "actual_return",
                "expected_return",
                "abnormal_return",
                "cumulative_abnormal_return",
                "sentiment_group",
            ]
        )

    detail_df = detail_df[
        [
            "event_id",
            "event_name",
            "stock_code",
            "stock_name",
            "day_offset",
            "actual_return",
            "expected_return",
            "abnormal_return",
            "cumulative_abnormal_return",
            "sentiment_group",
        ]
    ].sort_values(["event_id", "stock_code", "day_offset"]).reset_index(drop=True)
    save_dataframe(detail_df, study_dir / "event_study_detail")

    stats_df = _build_event_study_stats(detail_df)
    save_dataframe(stats_df, study_dir / "event_study_stats")

    joint_mean_car_df, plot_note = _build_joint_mean_car(detail_df)
    save_dataframe(joint_mean_car_df, study_dir / "joint_mean_car")
    joint_mean_car_path = study_dir / "joint_mean_car.png"
    _render_joint_mean_car_plot(joint_mean_car_df, joint_mean_car_path, plot_note)

    return EventStudyArtifacts(detail_df, stats_df, joint_mean_car_df, study_dir, joint_mean_car_path)


def _prepare_return_series(price_df: pd.DataFrame) -> pd.DataFrame:
    """准备收益率序列。"""

    ordered = price_df.sort_values(["stock_code", "trade_date"]).copy()
    ordered["trade_date"] = pd.to_datetime(ordered["trade_date"])
    ordered["return"] = ordered.groupby("stock_code")["close"].pct_change().fillna(0.0)
    return ordered


def _locate_anchor_trade_date(calendar: list[date], event_date: date) -> date | None:
    """确定事件锚点交易日。"""

    for trade_date in calendar:
        if trade_date >= event_date:
            return trade_date
    return calendar[-1] if calendar else None


def _estimate_market_model(stock_history: pd.DataFrame, benchmark_returns: pd.DataFrame, anchor_date: date) -> dict[str, float]:
    """估计单因子市场模型。"""

    estimation_start = pd.Timestamp(anchor_date) + pd.Timedelta(days=-60)
    estimation_end = pd.Timestamp(anchor_date) + pd.Timedelta(days=-6)
    stock_window = stock_history[
        (stock_history["trade_date"] >= estimation_start) & (stock_history["trade_date"] <= estimation_end)
    ][["trade_date", "return"]]
    benchmark_window = benchmark_returns[
        (benchmark_returns["trade_date"] >= estimation_start) & (benchmark_returns["trade_date"] <= estimation_end)
    ][["trade_date", "return"]].rename(columns={"return": "benchmark_return"})
    merged = stock_window.merge(benchmark_window, on="trade_date", how="inner")
    if len(merged) < 15:
        return {"alpha": 0.0, "beta": 1.0, "use_market_adjusted": 1.0}

    x = merged["benchmark_return"].to_numpy()
    y = merged["return"].to_numpy()
    beta, alpha = np.polyfit(x, y, 1)
    return {"alpha": float(alpha), "beta": float(beta), "use_market_adjusted": 0.0}


def _build_event_window(
    stock_history: pd.DataFrame,
    benchmark_returns: pd.DataFrame,
    market_calendar: list[date],
    anchor_date: date,
    start_offset: int,
    end_offset: int,
    market_model: dict[str, float],
) -> pd.DataFrame:
    """按事件窗口构造收益率明细。"""

    if anchor_date not in market_calendar:
        return pd.DataFrame()
    anchor_index = market_calendar.index(anchor_date)
    stock_returns_map = {
        pd.Timestamp(row["trade_date"]).date(): float(row["return"])
        for _, row in stock_history.iterrows()
    }
    benchmark_returns_map = {
        pd.Timestamp(row["trade_date"]).date(): float(row["return"])
        for _, row in benchmark_returns.iterrows()
    }

    rows: list[dict] = []
    cumulative_ar = 0.0
    for day_offset in range(start_offset, end_offset + 1):
        target_index = anchor_index + day_offset
        if target_index < 0 or target_index >= len(market_calendar):
            continue
        trade_date = market_calendar[target_index]
        actual_return = stock_returns_map.get(trade_date)
        benchmark_return = benchmark_returns_map.get(trade_date)
        if actual_return is None or benchmark_return is None:
            continue

        market_adjusted = benchmark_return
        single_factor_expected = market_model["alpha"] + market_model["beta"] * benchmark_return
        expected_return = market_adjusted if market_model["use_market_adjusted"] else single_factor_expected
        abnormal_return = actual_return - expected_return
        cumulative_ar += abnormal_return
        rows.append(
            {
                "trade_date": trade_date.isoformat(),
                "day_offset": int(day_offset),
                "actual_return": round(actual_return, 6),
                "expected_return": round(expected_return, 6),
                "abnormal_return": round(abnormal_return, 6),
                "cumulative_abnormal_return": round(cumulative_ar, 6),
                "market_adjusted_return": round(market_adjusted, 6),
                "single_factor_expected_return": round(single_factor_expected, 6),
            }
        )

    return pd.DataFrame(rows)


def _build_event_study_stats(detail_df: pd.DataFrame) -> pd.DataFrame:
    """构建事件研究汇总表。"""

    if detail_df.empty:
        return pd.DataFrame(
            columns=[
                "event_id",
                "event_name",
                "sample_size",
                "mean_ar_1d",
                "mean_car_2d",
                "mean_car_4d",
                "std_car_4d",
                "positive_ratio",
            ]
        )

    stats_rows: list[dict] = []
    for (event_id, event_name), group_df in detail_df.groupby(["event_id", "event_name"]):
        ar_1d = group_df[group_df["day_offset"] == 1]["abnormal_return"]
        car_2d = group_df[group_df["day_offset"] == 2]["cumulative_abnormal_return"]
        car_4d = group_df[group_df["day_offset"] == 4]["cumulative_abnormal_return"]
        sample_size = int(group_df["stock_code"].nunique())
        stats_rows.append(
            {
                "event_id": event_id,
                "event_name": event_name,
                "sample_size": sample_size,
                "mean_ar_1d": round(float(ar_1d.mean()) if not ar_1d.empty else 0.0, 6),
                "mean_car_2d": round(float(car_2d.mean()) if not car_2d.empty else 0.0, 6),
                "mean_car_4d": round(float(car_4d.mean()) if not car_4d.empty else 0.0, 6),
                "std_car_4d": round(float(car_4d.std(ddof=0)) if len(car_4d) > 0 else 0.0, 6),
                "positive_ratio": round(float((car_4d > 0).mean()) if len(car_4d) > 0 else 0.0, 4),
            }
        )
    return pd.DataFrame(stats_rows).sort_values("mean_car_4d", ascending=False).reset_index(drop=True)


def _build_joint_mean_car(detail_df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """构建联合均值 CAR 汇总。"""

    if detail_df.empty:
        return pd.DataFrame(columns=["group_label", "day_offset", "mean_car", "sample_size", "note"]), "无可用事件样本"

    available_groups = [
        group_name for group_name, group_df in detail_df.groupby("sentiment_group") if group_df["event_id"].nunique() >= 1
    ]
    if len(available_groups) < 2:
        joint = (
            detail_df.groupby("day_offset")
            .agg(
                mean_car=("cumulative_abnormal_return", "mean"),
                sample_size=("stock_code", "nunique"),
            )
            .reset_index()
        )
        joint["group_label"] = "单组聚合"
        joint["note"] = "样本不足，使用单组聚合"
        joint = joint[["group_label", "day_offset", "mean_car", "sample_size", "note"]]
        return joint, "样本不足，使用单组聚合"

    joint = (
        detail_df.groupby(["sentiment_group", "day_offset"])
        .agg(
            mean_car=("cumulative_abnormal_return", "mean"),
            sample_size=("stock_code", "nunique"),
        )
        .reset_index()
        .rename(columns={"sentiment_group": "group_label"})
    )
    joint["note"] = ""
    joint = joint[["group_label", "day_offset", "mean_car", "sample_size", "note"]]
    return joint, "联合均值 CAR 图"


def _render_joint_mean_car_plot(joint_df: pd.DataFrame, output_path: Path, title_note: str) -> None:
    """绘制联合均值 CAR 图。"""

    plt.figure(figsize=(10, 6))
    if joint_df.empty:
        plt.text(0.5, 0.5, "暂无可用事件研究样本", ha="center", va="center", fontsize=14)
        plt.axis("off")
    else:
        color_map = {
            "正向事件": "#d1495b",
            "负向事件": "#2f6690",
            "单组聚合": "#6c757d",
        }
        for group_label, group_df in joint_df.groupby("group_label"):
            plt.plot(
                group_df["day_offset"],
                group_df["mean_car"],
                marker="o",
                linewidth=2,
                label=group_label,
                color=color_map.get(group_label, "#6c757d"),
            )
        plt.axhline(0, linestyle="--", linewidth=1, color="#999999")
        plt.xlabel("day_offset")
        plt.ylabel("mean_car")
        plt.legend()
    plt.title(f"联合均值 CAR 图：{title_note}", fontfamily="Arial Unicode MS")
    plt.tight_layout()
    plt.savefig(output_path, dpi=180)
    plt.close()


def _render_empty_joint_plot(output_path: Path) -> None:
    """绘制空图。"""

    plt.figure(figsize=(10, 6))
    plt.text(0.5, 0.5, "暂无可用事件研究样本", ha="center", va="center", fontsize=14)
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(output_path, dpi=180)
    plt.close()
