from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from pipeline.workflow import run_weekly_pipeline
from pipeline.utils import ensure_directory, parse_date, save_dataframe


def run_backtest(project_root: Path, start_value: str | date, end_value: str | date) -> pd.DataFrame:
    """按赛题规则执行日频周度回测。"""

    start_date = parse_date(start_value)
    end_date = parse_date(end_value)
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
        prices = pd.read_csv(project_root / "data/raw" / monday.isoformat() / f"prices_{monday.isoformat()}.csv")
        prices["stock_code"] = prices["stock_code"].astype(str).apply(lambda code: code.zfill(6) if code.isdigit() else code)
        prices["trade_date"] = pd.to_datetime(prices["trade_date"]).dt.date
        week_return = 0.0
        trade_rows = []

        for _, pick in artifacts.final_picks.iterrows():
            pick_code = str(pick["stock_code"]).zfill(6) if str(pick["stock_code"]).isdigit() else str(pick["stock_code"])
            stock_quotes = prices[prices["stock_code"] == pick_code]
            buy_row = stock_quotes[stock_quotes["trade_date"] >= tuesday].sort_values("trade_date").head(1)
            sell_row = stock_quotes[stock_quotes["trade_date"] <= friday].sort_values("trade_date").tail(1)
            if buy_row.empty or sell_row.empty:
                continue
            buy_price = float(buy_row.iloc[0]["open"])
            sell_price = float(sell_row.iloc[0]["close"])
            trade_return = (sell_price / buy_price) - 1
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
                "buy_date": tuesday.isoformat(),
                "sell_date": friday.isoformat(),
                "weekly_return": round(week_return, 6),
                "pick_count": int(len(artifacts.final_picks)),
            }
        )

        if trade_rows:
            trade_df = pd.DataFrame(trade_rows)
            save_dataframe(
                trade_df,
                ensure_directory(project_root / "outputs/backtest" / monday.isoformat()) / "trade_details",
            )
        cursor = monday + timedelta(days=7)

    summary_df = pd.DataFrame(results)
    if not summary_df.empty:
        summary_df["net_value"] = (1 + summary_df["weekly_return"]).cumprod().round(6)
    backtest_dir = ensure_directory(project_root / "outputs/backtest")
    save_dataframe(summary_df, backtest_dir / "weekly_summary")

    if joint_mean_car_frames:
        historical_joint_df = pd.concat(joint_mean_car_frames, ignore_index=True)
        historical_joint_summary = (
            historical_joint_df.groupby(["group_label", "day_offset"])
            .agg(
                mean_car=("mean_car", "mean"),
                sample_size=("sample_size", "sum"),
            )
            .reset_index()
        )
        historical_joint_summary["note"] = "历史窗口联合均值CAR聚合"
        save_dataframe(historical_joint_summary, backtest_dir / "historical_joint_mean_car")
        _render_historical_joint_mean_car(historical_joint_summary, backtest_dir / "historical_joint_mean_car.png")

    if event_study_stats_frames:
        historical_stats_df = pd.concat(event_study_stats_frames, ignore_index=True)
        save_dataframe(historical_stats_df, backtest_dir / "historical_event_study_stats")
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
