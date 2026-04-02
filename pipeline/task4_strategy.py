from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pandas as pd

from pipeline.models import AppConfig
from pipeline.utils import logistic, save_dataframe


def _compute_momentum(stock_code: str, price_df: pd.DataFrame, asof_date, n_days: int = 5) -> float:
    """计算股票近n日涨幅。"""
    if price_df is None or price_df.empty:
        return 0.0
    stock_prices = price_df[price_df['stock_code']
                            == stock_code].sort_values('trade_date')
    # 取asof_date之前的数据
    stock_prices = stock_prices[stock_prices['trade_date'] <= str(asof_date)]
    if len(stock_prices) < n_days + 1:
        return 0.0
    recent = stock_prices.tail(n_days + 1)
    return (recent['close'].iloc[-1] / recent['close'].iloc[0]) - 1


def run_strategy_construction(
    asof_date: date,
    event_df: pd.DataFrame,
    prediction_df: pd.DataFrame,
    stock_df: pd.DataFrame,
    trading_calendar: list[date],
    financial_df: pd.DataFrame,
    suspend_resume_df: pd.DataFrame,
    output_dir,
    config: AppConfig,
    price_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """生成周度投资决策。"""

    merged = prediction_df.merge(
        stock_df[["stock_code", "stock_name", "listed_date",
                  "is_st", "avg_turnover_million"]],
        on=["stock_code", "stock_name"],
        how="left",
    )
    if financial_df is not None and not financial_df.empty:
        merged = merged.merge(
            financial_df[["stock_code", "pe",
                          "pb", "roe", "net_profit_growth"]],
            on="stock_code",
            how="left",
        )
    else:
        for col in ["pe", "pb", "roe", "net_profit_growth"]:
            merged[col] = None

    merged["listed_date"] = pd.to_datetime(merged["listed_date"])
    merged["listing_days"] = (pd.Timestamp(
        asof_date) - merged["listed_date"]).dt.days
    merged["passes_filter"] = merged.apply(
        lambda row: pass_basic_filter(
            row, config) and pass_fundamental_filter(row),
        axis=1,
    )

    tradable = merged[merged["passes_filter"]].copy()
    tradable["can_trade"] = tradable.apply(
        lambda row: is_tradeable(
            row["stock_code"], asof_date, trading_calendar, suspend_resume_df),
        axis=1,
    )
    tradable = tradable[tradable["can_trade"]].copy()

    # 计算动量因子并融入排序得分
    tradable["momentum_5d"] = tradable.apply(
        lambda row: _compute_momentum(
            row["stock_code"], price_df, asof_date, n_days=5),
        axis=1,
    )
    # momentum_score 使用 logistic 归一化，默认 0.5
    tradable["momentum_score"] = tradable["momentum_5d"].apply(
        lambda x: logistic(x * 10) if x != 0 else 0.5)
    # 最终得分：85% prediction_score + 15% momentum_score
    tradable["final_score"] = 0.85 * tradable["prediction_score"] + \
        0.15 * tradable["momentum_score"]

    tradable = tradable.sort_values(["final_score", "pseudoconfidence"], ascending=[
                                    False, False]).reset_index(drop=True)
    tradable = tradable.drop_duplicates(
        subset=["stock_code"], keep="first").reset_index(drop=True)

    selected = tradable[tradable["final_score"] >= config.positive_score_threshold].head(
        config.max_positions).copy()
    fallback_used = False
    if selected.empty:
        fallback_used = True
        selected = build_fallback_pool(tradable, config)

    final_picks = allocate_positions(selected, config)

    # 空仓保护：如果最终选中标的的预期收益全为负，不操作
    if not final_picks.empty:
        min_score_threshold = -0.01  # 可配置
        if final_picks["prediction_score"].max() < min_score_threshold:
            print(
                f"[STRATEGY] 所有候选标的预期收益为负(max={final_picks['prediction_score'].max():.4f})，本周空仓")
            final_picks = final_picks.iloc[0:0]  # 清空但保留列结构

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
        "buy_date": next_trading_date(trading_calendar, asof_date, target_weekday=1).isoformat()
        if next_trading_date(trading_calendar, asof_date, target_weekday=1)
        else "",
        "sell_date": week_last_trading_date(trading_calendar, asof_date).isoformat()
        if week_last_trading_date(trading_calendar, asof_date)
        else "",
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


def is_tradeable(stock_code: str, asof_date: date, trading_calendar: list[date], suspend_resume_df: pd.DataFrame) -> bool:
    """检查本周是否满足交易日与停牌约束。"""

    buy_date = next_trading_date(trading_calendar, asof_date, target_weekday=1)
    if buy_date is None:
        return False
    sell_date = week_last_trading_date(trading_calendar, asof_date)
    if sell_date is None or sell_date < buy_date:
        return False

    if suspend_resume_df is not None and not suspend_resume_df.empty:
        stock_suspend = suspend_resume_df[suspend_resume_df["stock_code"] == stock_code]
        for _, sr in stock_suspend.iterrows():
            s_date = sr.get("suspend_date")
            r_date = sr.get("resume_date")
            if not s_date:
                continue
            try:
                s_dt = pd.Timestamp(s_date).date()
                r_dt = pd.Timestamp(r_date).date() if r_date else None
            except (ValueError, TypeError):
                continue
            if r_dt is None and s_dt <= buy_date:
                return False
            if r_dt is not None and s_dt <= buy_date <= r_dt:
                return False
    return True


def next_trading_date(trading_calendar: list[date], asof_date: date, target_weekday: int) -> date | None:
    """找到目标周的第一个交易日。"""

    target = asof_date
    while target.weekday() != target_weekday:
        target += timedelta(days=1)

    calendar_set = set(trading_calendar)
    week_end = target + timedelta(days=4)
    for offset in range(5):
        candidate = target + timedelta(days=offset)
        if candidate > week_end:
            break
        if candidate in calendar_set:
            return candidate
    return None


def week_last_trading_date(trading_calendar: list[date], asof_date: date) -> date | None:
    """找到当前周最后一个交易日。"""

    monday = asof_date - timedelta(days=asof_date.weekday())
    week_end = monday + timedelta(days=4)
    candidates = [
        trade_date for trade_date in trading_calendar if monday <= trade_date <= week_end]
    if not candidates:
        return None
    return max(candidates)


def build_fallback_pool(tradable: pd.DataFrame, config: AppConfig) -> pd.DataFrame:
    """无明显正分标的时的兜底池。"""

    fallback = tradable.copy()
    # 流动性归一化
    max_liquidity = fallback["liquidity_score"].max()
    if max_liquidity > 0:
        fallback["liquidity_norm"] = fallback["liquidity_score"] / max_liquidity
    else:
        fallback["liquidity_norm"] = 0.5

    # 改进的稳定性得分：40% 流动性 + 35% 置信度 + 25% (1 - 风险惩罚)
    fallback["stability_score"] = (
        0.40 * fallback["liquidity_norm"]
        + 0.35 * fallback["pseudoconfidence"]
        + 0.25 * (1 - fallback["risk_penalty"])
    )
    fallback = fallback.sort_values(["stability_score", "final_score"], ascending=[
                                    False, False]).head(config.max_positions)
    return fallback


def allocate_positions(selected: pd.DataFrame, config: AppConfig) -> pd.DataFrame:
    """按规则分配仓位。"""

    if selected.empty:
        return pd.DataFrame(columns=["event_name", "stock_code", "capital_ratio", "rank", "stock_name", "prediction_score"])

    picks = selected[["event_name", "stock_code",
                      "stock_name", "final_score", "prediction_score"]].copy()
    scores = picks["final_score"].clip(lower=0.0001)
    picks["capital_ratio"] = scores / scores.sum()

    # 置信度加权：如果最高分标的的 prediction_score > 第二名的 1.5 倍，给最高分额外 5% 权重
    if len(picks) > 1:
        sorted_scores = picks["prediction_score"].sort_values(
            ascending=False).reset_index(drop=True)
        if sorted_scores.iloc[0] > sorted_scores.iloc[1] * 1.5:
            # 找到最高分标的的索引
            top_idx = picks["prediction_score"].idxmax()
            # 给其他标的等比扣除权重
            other_indices = picks.index[picks.index != top_idx]
            if len(other_indices) > 0:
                total_other = picks.loc[other_indices, "capital_ratio"].sum()
                if total_other > 0:
                    # 从其他标的扣除 5%，加到最高分
                    picks.loc[other_indices,
                              "capital_ratio"] *= (total_other - 0.05) / total_other
                    picks.loc[top_idx, "capital_ratio"] += 0.05

    if len(picks) > 1:
        # 仓位下限从 20% 调整为 15%，上限保持 50%
        position_floor = getattr(config, "position_floor_new", 0.15)
        picks["capital_ratio"] = picks["capital_ratio"].clip(
            lower=position_floor, upper=config.position_cap)
        picks["capital_ratio"] = picks["capital_ratio"] / \
            picks["capital_ratio"].sum()
        picks["capital_ratio"] = picks["capital_ratio"].clip(
            lower=position_floor, upper=config.position_cap)
        picks["capital_ratio"] = picks["capital_ratio"] / \
            picks["capital_ratio"].sum()
    else:
        picks["capital_ratio"] = 1.0

    picks = picks.sort_values(
        "capital_ratio", ascending=False).reset_index(drop=True)
    picks["rank"] = range(1, len(picks) + 1)
    picks["capital_ratio"] = picks["capital_ratio"].round(4)
    diff = round(1 - picks["capital_ratio"].sum(), 4)
    if not picks.empty and diff != 0:
        picks.loc[0, "capital_ratio"] = round(
            picks.loc[0, "capital_ratio"] + diff, 4)
    return picks[["event_name", "stock_code", "capital_ratio", "rank", "stock_name", "prediction_score"]]


def build_pick_reason(row: pd.Series, tradable: pd.DataFrame, event_df: pd.DataFrame) -> str:
    """构造选股理由。"""

    # 获取候选股票信息
    candidate_rows = tradable[tradable["stock_code"] == row["stock_code"]]
    if candidate_rows.empty:
        return f"基于事件{row.get('event_name', '未知事件')}选中的股票，具备短线交易价值。"
    candidate = candidate_rows.iloc[0]

    # 获取事件信息，添加 empty 检查避免 IndexError
    event_rows = event_df[event_df["event_name"] == row["event_name"]]
    if event_rows.empty:
        return (
            f"该股预测得分{candidate.get('final_score', 0):.4f}，"
            f"预期4日CAR为{candidate.get('car_4d', 0):.4f}，具备较强的短线交易价值。"
        )
    event_row = event_rows.iloc[0]

    return (
        f"事件热度{event_row.get('heat_score', 0):.2f}、强度{event_row.get('intensity_score', 0):.2f}，"
        f"该股预测得分{candidate.get('final_score', 0):.4f}，"
        f"预期4日CAR为{candidate.get('car_4d', 0):.4f}，具备较强的短线交易价值。"
    )
