from __future__ import annotations

from datetime import date, timedelta
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from pipeline.fetch_data import fetch_price_history, fetch_trading_calendar
from pipeline.settings import load_config
from pipeline.task4_strategy import next_trading_date, week_last_trading_date
from pipeline.workflow import run_weekly_pipeline
from pipeline.utils import configure_logging, configure_matplotlib_chinese, ensure_directory, parse_date, save_dataframe

# 配置 matplotlib 支持中文显示
configure_matplotlib_chinese()

logger = logging.getLogger(__name__)


def run_backtest(project_root: Path, start_value: str | date, end_value: str | date) -> pd.DataFrame:
    """按赛题规则执行日频周度回测。"""

    configure_logging()
    start_date = parse_date(start_value)
    end_date = parse_date(end_value)
    config = load_config(project_root)
    trading_calendar_artifacts = fetch_trading_calendar(
        start_date - timedelta(days=7), end_date + timedelta(days=7), config)
    trading_calendar = trading_calendar_artifacts.calendar
    logger.info("回测使用交易日历来源：%s", trading_calendar_artifacts.source_name)
    results: list[dict] = []
    joint_mean_car_frames: list[pd.DataFrame] = []
    event_study_stats_frames: list[pd.DataFrame] = []
    cursor = start_date

    while cursor <= end_date:
        monday = cursor - timedelta(days=cursor.weekday())
        tuesday = monday + timedelta(days=1)
        friday = monday + timedelta(days=4)
        if tuesday > end_date:
            break

        artifacts = run_weekly_pipeline(project_root, monday)
        if not artifacts.event_study_artifacts.joint_mean_car_df.empty:
            joint_df = artifacts.event_study_artifacts.joint_mean_car_df.copy()
            joint_df["week_monday"] = monday.isoformat()
            joint_mean_car_frames.append(joint_df)
        if not artifacts.event_study_artifacts.stats_df.empty:
            stats_df = artifacts.event_study_artifacts.stats_df.copy()
            stats_df["week_monday"] = monday.isoformat()
            event_study_stats_frames.append(stats_df)
        buy_date = next_trading_date(
            trading_calendar, monday, target_weekday=1)
        sell_date = week_last_trading_date(trading_calendar, monday)
        week_prices = pd.DataFrame(
            columns=["stock_code", "trade_date", "open", "close"])
        if buy_date is not None and sell_date is not None and not artifacts.final_picks.empty:
            week_prices = fetch_price_history(
                stock_codes=artifacts.final_picks["stock_code"].astype(
                    str).tolist(),
                start_date=buy_date,
                end_date=sell_date,
                config=config,
                trading_calendar=trading_calendar,
            )
            week_prices["trade_date"] = pd.to_datetime(
                week_prices["trade_date"]).dt.date
        week_return = 0.0
        trade_rows = []

        for _, pick in artifacts.final_picks.iterrows():
            pick_code = str(pick["stock_code"]).zfill(6) if str(
                pick["stock_code"]).isdigit() else str(pick["stock_code"])
            stock_quotes = week_prices[week_prices["stock_code"] == pick_code]
            buy_row = stock_quotes[stock_quotes["trade_date"] >= buy_date].sort_values(
                "trade_date").head(1) if buy_date else pd.DataFrame()
            sell_row = stock_quotes[stock_quotes["trade_date"] <= sell_date].sort_values(
                "trade_date").tail(1) if sell_date else pd.DataFrame()
            if buy_row.empty or sell_row.empty:
                continue
            buy_price = float(buy_row.iloc[0]["open"])
            sell_price = float(sell_row.iloc[0]["close"])
            # 计算交易成本：佣金0.1% + 滑点0.05%，买卖各一次
            commission_rate = 0.001
            slippage = 0.0005
            total_cost = commission_rate * 2 + slippage * 2
            trade_return = (sell_price / buy_price) - 1 - total_cost
            weighted_return = trade_return * float(pick["capital_ratio"])
            week_return += weighted_return
            trade_rows.append(
                {
                    "week_monday": monday.isoformat(),
                    "event_name": pick["event_name"],
                    "stock_code": pick_code,
                    "buy_date": buy_row.iloc[0]["trade_date"].isoformat(),
                    "buy_price": buy_price,
                    "sell_date": sell_row.iloc[0]["trade_date"].isoformat(),
                    "sell_price": sell_price,
                    "capital_ratio": pick["capital_ratio"],
                    "weighted_return": round(weighted_return, 6),
                }
            )

        results.append(
            {
                "week_monday": monday.isoformat(),
                "buy_date": buy_date.isoformat() if buy_date else "",
                "sell_date": sell_date.isoformat() if sell_date else "",
                "weekly_return": round(week_return, 6),
                "pick_count": int(len(artifacts.final_picks)),
            }
        )

        if trade_rows:
            trade_df = pd.DataFrame(trade_rows)
            save_dataframe(
                trade_df,
                ensure_directory(project_root / "outputs/backtest" /
                                 monday.isoformat()) / "trade_details",
            )
        cursor = monday + timedelta(days=7)

    summary_df = pd.DataFrame(results)
    if not summary_df.empty:
        summary_df["net_value"] = (
            1 + summary_df["weekly_return"]).cumprod().round(6)
    backtest_dir = ensure_directory(project_root / "outputs/backtest")
    save_dataframe(summary_df, backtest_dir / "weekly_summary")

    if joint_mean_car_frames:
        historical_joint_df = pd.concat(
            joint_mean_car_frames, ignore_index=True)
        historical_joint_summary = (
            historical_joint_df.groupby(["group_label", "day_offset"])
            .agg(
                mean_car=("mean_car", "mean"),
                sample_size=("sample_size", "sum"),
            )
            .reset_index()
        )
        historical_joint_summary["note"] = "历史窗口联合均值CAR聚合"
        save_dataframe(historical_joint_summary,
                       backtest_dir / "historical_joint_mean_car")
        _render_historical_joint_mean_car(
            historical_joint_summary, backtest_dir / "historical_joint_mean_car.png")

    if event_study_stats_frames:
        historical_stats_df = pd.concat(
            event_study_stats_frames, ignore_index=True)
        save_dataframe(historical_stats_df, backtest_dir /
                       "historical_event_study_stats")
    return summary_df


def _render_historical_joint_mean_car(joint_df: pd.DataFrame, output_path: Path) -> None:
    """绘制历史窗口联合均值 CAR 图。"""

    plt.figure(figsize=(10, 6))
    if joint_df.empty:
        plt.text(0.5, 0.5, "暂无历史窗口 CAR 聚合结果", ha="center", va="center")
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
    plt.title("历史窗口联合均值 CAR 图", fontfamily="Arial Unicode MS")
    plt.tight_layout()
    plt.savefig(output_path, dpi=180)
    plt.close()
