from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pandas as pd

from pipeline.models import AppConfig
from pipeline.utils import save_dataframe


def run_strategy_construction(
    asof_date: date,
    event_df: pd.DataFrame,
    prediction_df: pd.DataFrame,
    stock_df: pd.DataFrame,
    price_df: pd.DataFrame,
    financial_df: pd.DataFrame,
    suspend_resume_df: pd.DataFrame,
    output_dir,
    config: AppConfig,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """生成周度投资决策。"""

    merged = prediction_df.merge(
        stock_df[["stock_code", "stock_name", "listed_date", "is_st", "avg_turnover_million"]],
        on=["stock_code", "stock_name"],
        how="left",
    )
    if financial_df is not None and not financial_df.empty:
        merged = merged.merge(
            financial_df[["stock_code", "pe", "pb", "roe", "net_profit_growth"]],
            on="stock_code",
            how="left",
        )
    else:
        for col in ["pe", "pb", "roe", "net_profit_growth"]:
            merged[col] = None

    merged["listed_date"] = pd.to_datetime(merged["listed_date"])
    merged["listing_days"] = (pd.Timestamp(asof_date) - merged["listed_date"]).dt.days
    merged["passes_filter"] = merged.apply(
        lambda row: pass_basic_filter(row, config) and pass_fundamental_filter(row),
        axis=1,
    )

    tradable = merged[merged["passes_filter"]].copy()
    tradable["can_trade"] = tradable.apply(
        lambda row: is_tradeable(row["stock_code"], asof_date, price_df, suspend_resume_df),
        axis=1,
    )
    tradable = tradable[tradable["can_trade"]].copy()
    tradable["final_score"] = tradable["prediction_score"]
    tradable = tradable.sort_values(["final_score", "pseudoconfidence"], ascending=[False, False]).reset_index(drop=True)
    tradable = tradable.drop_duplicates(subset=["stock_code"], keep="first").reset_index(drop=True)

    selected = tradable[tradable["final_score"] >= config.positive_score_threshold].head(config.max_positions).copy()
    fallback_used = False
    if selected.empty:
        fallback_used = True
        selected = build_fallback_pool(tradable, config)

    final_picks = allocate_positions(selected, config)
    if final_picks.empty:
        final_picks["reason"] = pd.Series(dtype="object")
    else:
        final_picks["reason"] = final_picks.apply(
            lambda row: build_pick_reason(row, tradable, event_df),
            axis=1,
        )
    save_dataframe(tradable, output_dir / "strategy_candidates")
    save_dataframe(final_picks, output_dir / "final_picks")

    summary = {
        "asof_date": asof_date.isoformat(),
        "fallback_used": fallback_used,
        "candidate_count": int(len(tradable)),
        "selected_count": int(len(final_picks)),
    }
    return final_picks, summary


def pass_basic_filter(row: pd.Series, config: AppConfig) -> bool:
    """执行股票基础过滤。"""

    if bool(row["is_st"]):
        return False
    if float(row["avg_turnover_million"]) < config.min_avg_turnover_million:
        return False
    if int(row["listing_days"]) < config.min_listing_days:
        return False
    return True


def pass_fundamental_filter(row: pd.Series) -> bool:
    """执行基本面过滤（PE极值、ROE、净利润增长）。"""

    pe_val = row.get("pe")
    if pe_val is not None:
        try:
            pe = float(pe_val)
            if pe > 100 or pe < 0:
                return False
        except (TypeError, ValueError):
            pass

    roe_val = row.get("roe")
    if roe_val is not None:
        try:
            roe = float(roe_val)
            if roe < 0.05:
                return False
        except (TypeError, ValueError):
            pass

    growth_val = row.get("net_profit_growth")
    if growth_val is not None:
        try:
            growth = float(growth_val)
            if growth < -0.2:
                return False
        except (TypeError, ValueError):
            pass

    return True


def is_tradeable(stock_code: str, asof_date: date, price_df: pd.DataFrame, suspend_resume_df: pd.DataFrame) -> bool:
    """检查周二是否可能正常成交。"""

    buy_date = next_trading_date(price_df, stock_code, asof_date, target_weekday=1)
    if buy_date is None:
        return False

    if suspend_resume_df is not None and not suspend_resume_df.empty:
        stock_suspend = suspend_resume_df[suspend_resume_df["stock_code"] == stock_code]
        for _, sr in stock_suspend.iterrows():
            s_date = sr.get("suspend_date")
            r_date = sr.get("resume_date")
            if s_date and r_date:
                try:
                    s_dt = pd.Timestamp(s_date)
                    r_dt = pd.Timestamp(r_date)
                    if s_dt <= pd.Timestamp(buy_date) <= r_dt:
                        return False
                except (ValueError, TypeError):
                    pass

    quote = price_df[(price_df["stock_code"] == stock_code) & (pd.to_datetime(price_df["trade_date"]) == pd.Timestamp(buy_date))]
    if quote.empty:
        return False
    row = quote.iloc[0]
    if float(row["open"]) == float(row["high"]) == float(row["low"]) and float(row["pct_chg"]) > 9:
        return False
    return True


def next_trading_date(price_df: pd.DataFrame, stock_code: str, asof_date: date, target_weekday: int) -> date | None:
    """找到目标周的第一个交易日。"""

    target = asof_date
    while target.weekday() != target_weekday:
        target += timedelta(days=1)

    stock_dates = set(pd.to_datetime(price_df[price_df["stock_code"] == stock_code]["trade_date"]).dt.date.tolist())
    for offset in range(3):
        candidate = target + timedelta(days=offset)
        if candidate in stock_dates:
            return candidate
    return None


def build_fallback_pool(tradable: pd.DataFrame, config: AppConfig) -> pd.DataFrame:
    """无明显正分标的时的兜底池。"""

    fallback = tradable.copy()
    fallback["stability_score"] = fallback["liquidity_score"] + fallback["pseudoconfidence"] - fallback["risk_penalty"]
    fallback = fallback.sort_values(["stability_score", "final_score"], ascending=[False, False]).head(config.max_positions)
    return fallback


def allocate_positions(selected: pd.DataFrame, config: AppConfig) -> pd.DataFrame:
    """按规则分配仓位。"""

    if selected.empty:
        return pd.DataFrame(columns=["event_name", "stock_code", "capital_ratio", "rank", "stock_name"])

    picks = selected[["event_name", "stock_code", "stock_name", "final_score"]].copy()
    scores = picks["final_score"].clip(lower=0.0001)
    picks["capital_ratio"] = scores / scores.sum()

    if len(picks) > 1:
        picks["capital_ratio"] = picks["capital_ratio"].clip(lower=config.position_floor, upper=config.position_cap)
        picks["capital_ratio"] = picks["capital_ratio"] / picks["capital_ratio"].sum()
        picks["capital_ratio"] = picks["capital_ratio"].clip(lower=config.position_floor, upper=config.position_cap)
        picks["capital_ratio"] = picks["capital_ratio"] / picks["capital_ratio"].sum()
    else:
        picks["capital_ratio"] = 1.0

    picks = picks.sort_values("capital_ratio", ascending=False).reset_index(drop=True)
    picks["rank"] = range(1, len(picks) + 1)
    picks["capital_ratio"] = picks["capital_ratio"].round(4)
    diff = round(1 - picks["capital_ratio"].sum(), 4)
    if not picks.empty and diff != 0:
        picks.loc[0, "capital_ratio"] = round(picks.loc[0, "capital_ratio"] + diff, 4)
    return picks[["event_name", "stock_code", "capital_ratio", "rank", "stock_name"]]


def build_pick_reason(row: pd.Series, tradable: pd.DataFrame, event_df: pd.DataFrame) -> str:
    """构造选股理由。"""

    candidate = tradable[tradable["stock_code"] == row["stock_code"]].iloc[0]
    event_row = event_df[event_df["event_name"] == row["event_name"]].iloc[0]
    return (
        f"事件热度{event_row['heat_score']:.2f}、强度{event_row['intensity_score']:.2f}，"
        f"该股预测得分{candidate['final_score']:.4f}，"
        f"预期4日CAR为{candidate['car_4d']:.4f}，具备较强的短线交易价值。"
    )
