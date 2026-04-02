from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd

from pipeline.models import AppConfig
from pipeline.utils import logistic, normalize_text, resolve_event_anchor_trade_date, save_dataframe


def run_impact_estimation(
    event_df: pd.DataFrame,
    relation_df: pd.DataFrame,
    stock_df: pd.DataFrame,
    price_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
    trading_calendar: list[date],
    financial_df: pd.DataFrame,
    output_dir,
    config: AppConfig,
) -> pd.DataFrame:
    """基于 A 股简化事件研究思想估计未来影响。"""

    merged = relation_df.merge(
        event_df[
            [
                "event_id",
                "event_name",
                "published_at",
                "subject_type",
                "industry_type",
                "sentiment_score",
                "heat_score",
                "intensity_score",
                "scope_score",
                "confidence_score",
            ]
        ],
        on=["event_id", "event_name"],
        how="left",
    ).merge(
        stock_df[["stock_code", "stock_name",
                  "industry", "avg_turnover_million"]],
        on=["stock_code", "stock_name"],
        how="left",
    )
    if financial_df is not None and not financial_df.empty:
        merged = merged.merge(
            financial_df[
                [
                    "stock_code",
                    "pe",
                    "pb",
                    "turnover_rate",
                    "roe",
                    "net_profit_growth",
                    "revenue_growth",
                    "debt_to_asset",
                    "ann_date",
                    "report_period",
                    "snapshot_trade_date",
                ]
            ],
            on="stock_code",
            how="left",
        )
    else:
        for col in [
            "pe",
            "pb",
            "turnover_rate",
            "roe",
            "net_profit_growth",
            "revenue_growth",
            "debt_to_asset",
            "ann_date",
            "report_period",
            "snapshot_trade_date",
        ]:
            merged[col] = None

    merged["published_at"] = pd.to_datetime(merged["published_at"])
    benchmark_returns = prepare_return_series(benchmark_df)
    stock_returns = prepare_return_series(price_df)

    # 计算自适应缩放因子
    historical_car_std = _estimate_historical_car_volatility(
        price_df, benchmark_df)
    adaptive_scale = min(0.25, max(0.10, historical_car_std * 2.5)
                         ) if historical_car_std > 0 else 0.18

    predictions: list[dict[str, Any]] = []
    for _, row in merged.iterrows():
        anchor_date = resolve_event_anchor_trade_date(
            trading_calendar,
            pd.Timestamp(row["published_at"]).to_pydatetime(),
            config.market_close_time,
        )
        if anchor_date is None:
            continue
        stock_history = stock_returns[stock_returns["stock_code"]
                                      == row["stock_code"]].copy()
        benchmark_history = benchmark_returns[benchmark_returns["stock_code"]
                                              == config.benchmark_code].copy()

        regression_stats = estimate_market_model(
            stock_history, benchmark_history, anchor_date)
        event_score = round(
            0.3 * row["heat_score"] + 0.35 * row["intensity_score"] +
            0.2 * row["scope_score"] + 0.15 * row["confidence_score"],
            4,
        )
        liquidity_score = min(1.0, float(row["avg_turnover_million"]) / 600)
        sentiment_direction = 1 if row["sentiment_score"] >= 0 else -1
        market_state = compute_market_state(benchmark_history, anchor_date)
        subject_multiplier = subject_bias(row["subject_type"])
        residual_risk = regression_stats["residual_volatility"]
        fundamental_score = compute_fundamental_score(row)
        expected_car_4d = round(
            sentiment_direction
            * event_score
            * row["association_score"]
            * subject_multiplier
            * (0.55 + market_state)
            * max(0.15, 1 - residual_risk)
            * (1 + fundamental_score * 0.15)
            * adaptive_scale,
            4,
        )
        ar_1d = round(expected_car_4d * (0.22 + 0.18 * liquidity_score), 4)
        car_2d = round(expected_car_4d * 0.65, 4)
        pseudoconfidence = round(
            min(
                0.99,
                0.2
                + 0.25 * row["confidence_score"]
                + 0.2 * row["association_score"]
                + 0.2 * liquidity_score
                + 0.15 * regression_stats["data_sufficiency"]
                + 0.1 * max(0.0, 1 - residual_risk),
            ),
            4,
        )
        risk_penalty = round(residual_risk * 0.6 +
                             max(0.0, 0.2 - market_state), 4)
        prediction_score = round(
            config.raw["scoring"]["prediction"]["expected_car_4d"] *
            expected_car_4d
            + config.raw["scoring"]["prediction"]["association_score"] *
            row["association_score"]
            + config.raw["scoring"]["prediction"]["event_score"] * event_score
            + config.raw["scoring"]["prediction"]["liquidity_score"] *
            liquidity_score
            - config.raw["scoring"]["prediction"]["risk_penalty"] *
            risk_penalty,
            4,
        )
        # 生成包含详细数值的逻辑链
        logic_chain = (
            f"事件「{row['event_name']}」(热度{row['heat_score']:.2f}, 强度{row['intensity_score']:.2f}) → "
            f"关联「{row['stock_name']}」(关联度{row['association_score']:.2f}) → "
            f"预期CAR {expected_car_4d:+.2%} → "
            f"综合评分 {prediction_score:.3f}"
        )

        predictions.append(
            {
                "event_id": row["event_id"],
                "event_name": row["event_name"],
                "stock_code": row["stock_code"],
                "stock_name": row["stock_name"],
                "anchor_trade_date": anchor_date.isoformat(),
                "ar_1d": ar_1d,
                "car_2d": car_2d,
                "car_4d": expected_car_4d,
                "direction": "正向" if expected_car_4d >= 0 else "负向",
                "prediction_score": prediction_score,
                "event_score": event_score,
                "fundamental_score": fundamental_score,
                "liquidity_score": round(liquidity_score, 4),
                "risk_penalty": risk_penalty,
                "pseudoconfidence": pseudoconfidence,
                "logic_chain": logic_chain,
                "beta": regression_stats["beta"],
                "residual_volatility": regression_stats["residual_volatility"],
            }
        )

    prediction_df = pd.DataFrame(predictions)
    if prediction_df.empty:
        prediction_df = _build_empty_prediction_df()
    else:
        prediction_df = prediction_df.sort_values(
            ["prediction_score", "pseudoconfidence"], ascending=[False, False]
        ).reset_index(drop=True)
    save_dataframe(prediction_df, output_dir / "predictions")
    return prediction_df


def _build_empty_prediction_df() -> pd.DataFrame:
    """返回空的预测结果结构。"""

    return pd.DataFrame(
        columns=[
            "event_id",
            "event_name",
            "stock_code",
            "stock_name",
            "anchor_trade_date",
            "ar_1d",
            "car_2d",
            "car_4d",
            "direction",
            "prediction_score",
            "event_score",
            "fundamental_score",
            "liquidity_score",
            "risk_penalty",
            "pseudoconfidence",
            "logic_chain",
            "beta",
            "residual_volatility",
        ]
    )


def prepare_return_series(price_df: pd.DataFrame) -> pd.DataFrame:
    """构建收益率序列。"""

    ordered = price_df.sort_values(["stock_code", "trade_date"]).copy()
    ordered["trade_date"] = pd.to_datetime(ordered["trade_date"])
    ordered["return"] = ordered.groupby(
        "stock_code")["close"].pct_change().fillna(0.0)
    return ordered


def estimate_market_model(stock_history: pd.DataFrame, benchmark_history: pd.DataFrame, event_date) -> dict[str, float]:
    """利用估计窗口做简化市场模型回归。"""

    estimation_end = pd.Timestamp(event_date + timedelta(days=-6))
    estimation_start = pd.Timestamp(event_date + timedelta(days=-60))

    stock_window = stock_history[(stock_history["trade_date"] >= estimation_start) & (
        stock_history["trade_date"] <= estimation_end)]
    benchmark_window = benchmark_history[
        (benchmark_history["trade_date"] >= estimation_start) & (
            benchmark_history["trade_date"] <= estimation_end)
    ][["trade_date", "return"]].rename(columns={"return": "benchmark_return"})
    merged = stock_window.merge(benchmark_window, on="trade_date", how="inner")
    if len(merged) < 15:
        return {"alpha": 0.0, "beta": 1.0, "residual_volatility": 0.25, "data_sufficiency": 0.2}

    x = merged["benchmark_return"].to_numpy()
    y = merged["return"].to_numpy()
    beta, alpha = np.polyfit(x, y, 1)
    residual = y - (alpha + beta * x)
    residual_volatility = float(
        np.clip(np.std(residual) * np.sqrt(252), 0.02, 0.8))
    data_sufficiency = min(1.0, len(merged) / 50)
    return {
        "alpha": round(float(alpha), 6),
        "beta": round(float(beta), 6),
        "residual_volatility": round(residual_volatility, 4),
        "data_sufficiency": round(data_sufficiency, 4),
    }


def compute_market_state(benchmark_history: pd.DataFrame, event_date) -> float:
    """估算当前市场状态。"""

    target_date = pd.Timestamp(event_date)
    window = benchmark_history[benchmark_history["trade_date"] <= target_date].tail(
        10)
    if window.empty:
        return 0.5
    recent_return = window["return"].mean()
    return round(float(np.clip(0.5 + recent_return * 8, 0.1, 0.9)), 4)


def subject_bias(subject_type: str) -> float:
    """不同事件主体的偏置因子。"""

    mapping = {
        "政策类事件": 1.08,
        "公司类事件": 1.12,
        "行业类事件": 1.0,
        "宏观类事件": 0.92,
        "地缘类事件": 1.15,
    }
    return mapping.get(subject_type, 1.0)


def _normalize_pe(pe: float | None, sector_median_pe: float | None = None) -> float:
    """PE合理区间得分，0~1。

    Args:
        pe: 市盈率值
        sector_median_pe: 行业中位数PE，用于相对评分。如果为None则使用绝对阈值
    """
    if pe is None or pd.isna(pe):
        return 0.5
    try:
        v = float(pe)
        if v < 0:
            return 0.1  # 亏损企业
        # 如果有行业中位数，使用相对评分
        if sector_median_pe is not None and sector_median_pe > 0:
            ratio = v / sector_median_pe
            if 0.5 <= ratio <= 1.5:  # 合理区间
                return 0.6 + 0.4 * (1 - abs(ratio - 1.0) / 0.5)
            elif ratio > 3.0:
                return 0.1
            elif ratio > 1.5:
                return 0.4
            else:
                return 0.5
        # fallback到现有逻辑（保留不变）
        if v > 150:
            return 0.0
        if v < 10:
            return 0.3 + (10 - v) / 10 * 0.3
        if v <= 30:
            return 0.6 + (30 - v) / 20 * 0.4
        if v <= 50:
            return 0.4 + (50 - v) / 20 * 0.2
        return 0.2
    except (TypeError, ValueError):
        return 0.5


def _normalize_pb(pb: float | None, sector_median_pb: float | None = None) -> float:
    """PB得分，0~1。

    Args:
        pb: 市净率值
        sector_median_pb: 行业中位数PB，用于相对评分。如果为None则使用绝对阈值
    """
    if pb is None or pd.isna(pb):
        return 0.5
    try:
        v = float(pb)
        if v <= 0:
            return 0.0
        # 如果有行业中位数，使用相对评分
        if sector_median_pb is not None and sector_median_pb > 0:
            ratio = v / sector_median_pb
            if 0.5 <= ratio <= 1.5:  # 合理区间
                return 0.6 + 0.4 * (1 - abs(ratio - 1.0) / 0.5)
            elif ratio > 3.0:
                return 0.1
            elif ratio > 1.5:
                return 0.4
            else:
                return 0.5
        # fallback到现有逻辑（保留不变）
        if v <= 2:
            return 0.7 + (2 - v) / 2 * 0.3
        if v <= 4:
            return 0.4 + (4 - v) / 2 * 0.3
        if v <= 8:
            return 0.2 + (8 - v) / 4 * 0.2
        return 0.1
    except (TypeError, ValueError):
        return 0.5


def _normalize_roe(roe: float | None, sector_median_roe: float | None = None) -> float:
    """ROE得分，0~1。

    Args:
        roe: 净资产收益率
        sector_median_roe: 行业中位数ROE，用于相对评分。如果为None则使用绝对阈值
    """
    if roe is None or pd.isna(roe):
        return 0.5
    try:
        v = float(roe)
        if v <= 0:
            return 0.0
        # 如果有行业中位数，使用相对评分
        if sector_median_roe is not None and sector_median_roe > 0:
            ratio = v / sector_median_roe
            if ratio >= 1.5:  # 显著高于行业中位数
                return 1.0
            elif ratio >= 1.0:  # 高于行业中位数
                return 0.7 + (ratio - 1.0) / 0.5 * 0.3
            elif ratio >= 0.5:  # 低于行业中位数但尚可
                return 0.4 + (ratio - 0.5) / 0.5 * 0.3
            else:  # 显著低于行业中位数
                return ratio / 0.5 * 0.4
        # fallback到现有逻辑（保留不变）
        if v >= 0.15:
            return 1.0
        if v >= 0.10:
            return 0.7 + (v - 0.10) / 0.05 * 0.3
        if v >= 0.05:
            return 0.4 + (v - 0.05) / 0.05 * 0.3
        return v / 0.05 * 0.4
    except (TypeError, ValueError):
        return 0.5


def _normalize_growth(growth: float | None) -> float:
    """净利润增长率得分，0~1。"""
    if growth is None:
        return 0.5
    try:
        v = float(growth)
        if v <= -0.3:
            return 0.0
        if v <= 0:
            return 0.2 + (v + 0.3) / 0.3 * 0.3
        if v <= 0.3:
            return 0.5 + v / 0.3 * 0.5
        return 1.0
    except (TypeError, ValueError):
        return 0.5


def compute_fundamental_score(row: pd.Series) -> float:
    """基本面综合得分，0~1，越高越好。"""
    pe_score = _normalize_pe(row.get("pe"))
    pb_score = _normalize_pb(row.get("pb"))
    roe_score = _normalize_roe(row.get("roe"))
    growth_score = _normalize_growth(row.get("net_profit_growth"))
    return round(0.3 * pe_score + 0.25 * pb_score + 0.25 * roe_score + 0.2 * growth_score, 4)


def _estimate_historical_car_volatility(price_df: pd.DataFrame, benchmark_df: pd.DataFrame, window: int = 4) -> float:
    """估算历史4日累计异常收益的标准差，用于自适应缩放。

    Args:
        price_df: 股票价格数据
        benchmark_df: 基准指数数据
        window: CAR计算窗口天数，默认4日

    Returns:
        历史CAR标准差，如果数据不足返回0.0
    """
    try:
        # 选择价格数据中出现最多的几只股票
        stock_counts = price_df['stock_code'].value_counts()
        sample_stocks = stock_counts.head(
            min(10, len(stock_counts))).index.tolist()

        all_car = []
        for sc in sample_stocks:
            stock_prices = price_df[price_df['stock_code']
                                    == sc].sort_values('trade_date')
            if len(stock_prices) < 30:
                continue
            stock_prices['trade_date'] = pd.to_datetime(
                stock_prices['trade_date'])
            stock_ret = stock_prices.set_index(
                'trade_date')['close'].pct_change()
            # 简单市场调整：用benchmark的return
            bm = benchmark_df.sort_values('trade_date').copy()
            bm['trade_date'] = pd.to_datetime(bm['trade_date'])
            bm = bm.set_index('trade_date')['close'].pct_change()
            # 对齐日期并计算AR
            common = stock_ret.index.intersection(bm.index)
            if len(common) < 20:
                continue
            ar = stock_ret[common] - bm[common]
            car_4d = ar.rolling(window).sum().dropna()
            all_car.extend(car_4d.tolist())

        if len(all_car) > 50:
            return float(pd.Series(all_car).std())
        return 0.0
    except Exception:
        return 0.0


def build_logic_chain(row: pd.Series) -> str:
    """输出可解释逻辑链。"""

    industry = row["industry_type"]
    return (
        f"{row['event_name']}发生后，首先强化了{industry}相关需求或情绪预期；"
        f"随后通过{row['relation_type']}传导至{row['stock_name']}所处业务环节；"
        f"由于该公司在{row['industry']}中具备直接或高相关敞口，市场预期其短期股价会出现{('正向' if row['sentiment_score'] >= 0 else '负向')}响应。"
    )
