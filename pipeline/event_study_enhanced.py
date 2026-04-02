from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

from pipeline.models import AppConfig
from pipeline.utils import ensure_directory, resolve_event_anchor_trade_date, save_dataframe, configure_matplotlib_chinese

# 配置 matplotlib 支持中文显示
configure_matplotlib_chinese()


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
    trading_calendar: list[date],
    output_dir: Path,
    config: AppConfig,
) -> EventStudyArtifacts:
    """生成标准化事件研究明细、统计表和联合均值 CAR 图。"""

    study_dir = ensure_directory(output_dir / "event_study")
    if relation_df.empty:
        empty_detail = _build_empty_detail_df()
        empty_stats = _build_empty_stats_df()
        empty_joint = pd.DataFrame(
            columns=["group_label", "day_offset", "mean_car", "sample_size", "note"])
        save_dataframe(empty_detail, study_dir / "event_study_detail")
        save_dataframe(empty_stats, study_dir / "event_study_stats")
        save_dataframe(empty_joint, study_dir / "joint_mean_car")
        joint_mean_car_path = study_dir / "joint_mean_car.png"
        _render_empty_joint_plot(joint_mean_car_path)
        return EventStudyArtifacts(empty_detail, empty_stats, empty_joint, study_dir, joint_mean_car_path)

    benchmark_returns = _prepare_return_series(benchmark_df)
    stock_returns = _prepare_return_series(price_df)
    event_meta = event_df.set_index("event_id")[
        ["event_name", "published_at", "sentiment_score",
            "subject_type", "industry_type"]
    ].copy()
    event_meta["published_at"] = pd.to_datetime(event_meta["published_at"])

    detail_rows: list[dict[str, object]] = []
    availability_rows: list[dict[str, object]] = []
    for _, relation in relation_df.iterrows():
        event_id = relation["event_id"]
        if event_id not in event_meta.index:
            continue
        meta = event_meta.loc[event_id]
        published_at = pd.Timestamp(meta["published_at"]).to_pydatetime()
        stock_history = stock_returns[stock_returns["stock_code"]
                                      == relation["stock_code"]].copy()
        common_calendar = sorted(
            set(pd.to_datetime(stock_history["trade_date"]).dt.date.tolist())
            & set(pd.to_datetime(benchmark_returns["trade_date"]).dt.date.tolist())
            & set(trading_calendar)
        )
        if not common_calendar:
            availability_rows.append(
                {
                    "event_id": event_id,
                    "event_name": meta["event_name"],
                    "stock_code": relation["stock_code"],
                    "stock_name": relation["stock_name"],
                    "has_ar_1d": False,
                    "has_car_0_2": False,
                    "has_car_0_4": False,
                    "availability_note": "无共同交易日样本",
                }
            )
            continue

        anchor_date = _resolve_anchor_trade_date(
            common_calendar, published_at, config.market_close_time)
        if anchor_date is None or anchor_date not in common_calendar:
            availability_rows.append(
                {
                    "event_id": event_id,
                    "event_name": meta["event_name"],
                    "stock_code": relation["stock_code"],
                    "stock_name": relation["stock_name"],
                    "has_ar_1d": False,
                    "has_car_0_2": False,
                    "has_car_0_4": False,
                    "availability_note": "事件锚点晚于当前可用行情窗口",
                }
            )
            continue

        market_model = _estimate_market_model(
            stock_history, benchmark_returns, anchor_date)
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
            availability_rows.append(
                {
                    "event_id": event_id,
                    "event_name": meta["event_name"],
                    "stock_code": relation["stock_code"],
                    "stock_name": relation["stock_name"],
                    "has_ar_1d": False,
                    "has_car_0_2": False,
                    "has_car_0_4": False,
                    "availability_note": "事件窗口无可用收益率样本",
                }
            )
            continue

        event_window_df["event_id"] = event_id
        event_window_df["event_name"] = meta["event_name"]
        event_window_df["stock_code"] = relation["stock_code"]
        event_window_df["stock_name"] = relation["stock_name"]
        event_window_df["anchor_trade_date"] = anchor_date.isoformat()
        event_window_df["sentiment_group"] = "正向事件" if float(
            meta["sentiment_score"]) >= 0 else "负向事件"
        detail_rows.extend(event_window_df.to_dict(orient="records"))
        availability_rows.append(_build_availability_row(event_window_df))

    detail_df = pd.DataFrame(detail_rows)
    if detail_df.empty:
        detail_df = _build_empty_detail_df()
    else:
        detail_df = detail_df[
            [
                "event_id",
                "event_name",
                "stock_code",
                "stock_name",
                "anchor_trade_date",
                "trade_date",
                "day_offset",
                "actual_return",
                "expected_return",
                "abnormal_return",
                "cumulative_abnormal_return",
                "cumulative_abnormal_return_0_2",
                "cumulative_abnormal_return_0_4",
                "sentiment_group",
            ]
        ].sort_values(["event_id", "stock_code", "day_offset"]).reset_index(drop=True)
    save_dataframe(detail_df, study_dir / "event_study_detail")

    availability_df = pd.DataFrame(availability_rows)
    stats_df = _build_event_study_stats(detail_df, availability_df)
    save_dataframe(stats_df, study_dir / "event_study_stats")

    joint_mean_car_df, plot_note = _build_joint_mean_car(detail_df)
    save_dataframe(joint_mean_car_df, study_dir / "joint_mean_car")
    joint_mean_car_path = study_dir / "joint_mean_car.png"
    _render_joint_mean_car_plot(
        joint_mean_car_df, joint_mean_car_path, plot_note)

    return EventStudyArtifacts(detail_df, stats_df, joint_mean_car_df, study_dir, joint_mean_car_path)


def _build_empty_detail_df() -> pd.DataFrame:
    """返回空的明细表结构。"""

    return pd.DataFrame(
        columns=[
            "event_id",
            "event_name",
            "stock_code",
            "stock_name",
            "anchor_trade_date",
            "trade_date",
            "day_offset",
            "actual_return",
            "expected_return",
            "abnormal_return",
            "cumulative_abnormal_return",
            "cumulative_abnormal_return_0_2",
            "cumulative_abnormal_return_0_4",
            "sentiment_group",
        ]
    )


def _build_empty_stats_df() -> pd.DataFrame:
    """返回空的统计表结构。"""

    return pd.DataFrame(
        columns=[
            "event_id",
            "event_name",
            "sample_size",
            "mean_ar_1d",
            "mean_car_0_2",
            "mean_car_0_4",
            "std_car_0_4",
            "positive_ratio_0_4",
            "t_stat",
            "p_value",
            "status_note",
        ]
    )


def _prepare_return_series(price_df: pd.DataFrame) -> pd.DataFrame:
    """准备收益率序列。"""

    ordered = price_df.sort_values(["stock_code", "trade_date"]).copy()
    ordered["trade_date"] = pd.to_datetime(ordered["trade_date"])
    ordered["return"] = ordered.groupby(
        "stock_code")["close"].pct_change().fillna(0.0)
    return ordered


def _resolve_anchor_trade_date(
    calendar: list[date],
    published_at: datetime,
    market_close_time,
) -> date | None:
    """按完整时间戳与收盘时间确定事件锚点交易日。"""

    return resolve_event_anchor_trade_date(calendar, published_at, market_close_time)


def _estimate_market_model(stock_history: pd.DataFrame, benchmark_returns: pd.DataFrame, anchor_date: date) -> dict[str, float]:
    """估计单因子市场模型。"""

    estimation_start = pd.Timestamp(anchor_date) + pd.Timedelta(days=-60)
    estimation_end = pd.Timestamp(anchor_date) + pd.Timedelta(days=-6)
    stock_window = stock_history[
        (stock_history["trade_date"] >= estimation_start) & (
            stock_history["trade_date"] <= estimation_end)
    ][["trade_date", "return"]]
    benchmark_window = benchmark_returns[
        (benchmark_returns["trade_date"] >= estimation_start) & (
            benchmark_returns["trade_date"] <= estimation_end)
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

    rows: list[dict[str, object]] = []
    cumulative_all = 0.0
    cumulative_0_2 = 0.0
    cumulative_0_4 = 0.0
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
        single_factor_expected = market_model["alpha"] + \
            market_model["beta"] * benchmark_return
        expected_return = market_adjusted if market_model[
            "use_market_adjusted"] else single_factor_expected
        abnormal_return = actual_return - expected_return
        cumulative_all += abnormal_return
        if 0 <= day_offset <= 2:
            cumulative_0_2 += abnormal_return
        if 0 <= day_offset <= 4:
            cumulative_0_4 += abnormal_return
        rows.append(
            {
                "trade_date": trade_date.isoformat(),
                "day_offset": int(day_offset),
                "actual_return": round(actual_return, 6),
                "expected_return": round(expected_return, 6),
                "abnormal_return": round(abnormal_return, 6),
                "cumulative_abnormal_return": round(cumulative_all, 6),
                "cumulative_abnormal_return_0_2": round(cumulative_0_2, 6),
                "cumulative_abnormal_return_0_4": round(cumulative_0_4, 6),
            }
        )

    return pd.DataFrame(rows)


def _build_availability_row(event_window_df: pd.DataFrame) -> dict[str, object]:
    """构建窗口可用性摘要。"""

    required_offsets = set(event_window_df["day_offset"].astype(int).tolist())
    return {
        "event_id": event_window_df.iloc[0]["event_id"],
        "event_name": event_window_df.iloc[0]["event_name"],
        "stock_code": event_window_df.iloc[0]["stock_code"],
        "stock_name": event_window_df.iloc[0]["stock_name"],
        "has_ar_1d": 1 in required_offsets,
        "has_car_0_2": all(offset in required_offsets for offset in (0, 1, 2)),
        "has_car_0_4": all(offset in required_offsets for offset in (0, 1, 2, 3, 4)),
        "availability_note": "窗口完整" if all(offset in required_offsets for offset in (0, 1, 2, 3, 4)) else "窗口部分可用",
    }


def _build_event_study_stats(detail_df: pd.DataFrame, availability_df: pd.DataFrame) -> pd.DataFrame:
    """构建事件研究汇总表。"""

    if availability_df.empty:
        return _build_empty_stats_df()

    stats_rows: list[dict[str, object]] = []
    for (event_id, event_name), availability_group in availability_df.groupby(["event_id", "event_name"]):
        event_detail_df = detail_df[detail_df["event_id"] == event_id].copy()
        ar_1d = event_detail_df[event_detail_df["day_offset"]
                                == 1]["abnormal_return"]
        car_0_2 = event_detail_df[event_detail_df["day_offset"]
                                  == 2]["cumulative_abnormal_return_0_2"]
        car_0_4 = event_detail_df[event_detail_df["day_offset"]
                                  == 4]["cumulative_abnormal_return_0_4"]
        complete_0_4_count = int(availability_group["has_car_0_4"].sum())
        partial_count = int(len(availability_group) - complete_0_4_count)
        status_note = "窗口完整"
        if complete_0_4_count == 0:
            status_note = "缺少完整 CAR(0,4) 窗口"
        elif partial_count > 0:
            status_note = f"部分样本窗口不足：{partial_count}"

        # 计算t-statistic和p-value
        car_values = car_0_4.dropna().tolist()
        if len(car_values) >= 3:
            t_stat, p_value = stats.ttest_1samp(car_values, 0)
        else:
            t_stat, p_value = float('nan'), float('nan')

        stats_rows.append(
            {
                "event_id": event_id,
                "event_name": event_name,
                "sample_size": int(availability_group["stock_code"].nunique()),
                "mean_ar_1d": round(float(ar_1d.mean()) if not ar_1d.empty else 0.0, 6),
                "mean_car_0_2": round(float(car_0_2.mean()) if not car_0_2.empty else 0.0, 6),
                "mean_car_0_4": round(float(car_0_4.mean()) if not car_0_4.empty else 0.0, 6),
                "std_car_0_4": round(float(car_0_4.std(ddof=0)) if len(car_0_4) > 0 else 0.0, 6),
                "positive_ratio_0_4": round(float((car_0_4 > 0).mean()) if len(car_0_4) > 0 else 0.0, 4),
                "t_stat": round(float(t_stat), 4) if not pd.isna(t_stat) else None,
                "p_value": round(float(p_value), 4) if not pd.isna(p_value) else None,
                "status_note": status_note,
            }
        )

    return pd.DataFrame(stats_rows).sort_values("mean_car_0_4", ascending=False).reset_index(drop=True)


def _build_joint_mean_car(detail_df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """构建联合均值 CAR 汇总。"""

    if detail_df.empty:
        return pd.DataFrame(columns=["group_label", "day_offset", "mean_car", "sample_size", "note"]), "无可用事件样本"

    available_groups = [
        group_name
        for group_name, group_df in detail_df.groupby("sentiment_group")
        if group_df["event_id"].nunique() >= 1
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
        return joint[["group_label", "day_offset", "mean_car", "sample_size", "note"]], "样本不足，使用单组聚合"

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
    return joint[["group_label", "day_offset", "mean_car", "sample_size", "note"]], "联合均值 CAR 图"


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
