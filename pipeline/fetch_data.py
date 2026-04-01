from __future__ import annotations

import hashlib
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.models import AppConfig, RunContext
from pipeline.utils import (
    dump_json,
    ensure_directory,
    load_json,
    parse_date,
    parse_datetime,
    save_dataframe,
)

if sys.version_info >= (3, 14):
    qs = None
else:
    try:
        import qstock as qs
    except Exception:  # pragma: no cover - 环境缺依赖时兜底
        qs = None

try:
    import tushare as ts
except Exception:  # pragma: no cover - 环境缺依赖时兜底
    ts = None


@dataclass(slots=True)
class FetchArtifacts:
    """数据采集阶段产物。"""

    news_df: pd.DataFrame
    stock_df: pd.DataFrame
    price_df: pd.DataFrame
    benchmark_df: pd.DataFrame
    financial_df: pd.DataFrame
    suspend_resume_df: pd.DataFrame


def run_fetch_pipeline(context: RunContext, config: AppConfig) -> FetchArtifacts:
    """采集周度运行所需的所有基础数据。"""

    news_df = fetch_news(context, config)
    stock_df = fetch_stock_universe(context.project_root)
    start_date = context.asof_date - timedelta(days=180)
    end_date = context.asof_date + timedelta(days=20)

    price_df = fetch_price_history(stock_df["stock_code"].tolist(), start_date, end_date, context, config)
    benchmark_df = fetch_benchmark_history(config.benchmark_code, start_date, end_date, context, config)
    financial_df = fetch_financial_data(stock_df["stock_code"].tolist(), context, config)
    suspend_resume_df = fetch_suspend_resume_data(stock_df["stock_code"].tolist(), context, config)

    save_dataframe(news_df, context.raw_dir / f"news_{context.asof_date.isoformat()}")
    save_dataframe(stock_df, context.raw_dir / "stock_universe")
    save_dataframe(price_df, context.raw_dir / f"prices_{context.asof_date.isoformat()}")
    save_dataframe(benchmark_df, context.raw_dir / f"benchmark_{context.asof_date.isoformat()}")
    save_dataframe(financial_df, context.raw_dir / f"financial_{context.asof_date.isoformat()}")
    save_dataframe(suspend_resume_df, context.raw_dir / f"suspend_resume_{context.asof_date.isoformat()}")

    return FetchArtifacts(
        news_df=news_df,
        stock_df=stock_df,
        price_df=price_df,
        benchmark_df=benchmark_df,
        financial_df=financial_df,
        suspend_resume_df=suspend_resume_df,
    )


def fetch_news(context: RunContext, config: AppConfig) -> pd.DataFrame:
    """获取新闻与公告数据。"""

    lookback_start = context.asof_date - timedelta(days=config.lookback_days)
    rows: list[dict[str, Any]] = []

    if qs is not None:
        try:
            qstock_df = qs.news_data(start=lookback_start.strftime("%Y%m%d"), end=context.asof_date.strftime("%Y%m%d"))
            if isinstance(qstock_df, pd.DataFrame) and not qstock_df.empty:
                renamed = qstock_df.rename(
                    columns={
                        "title": "title",
                        "content": "content",
                        "发布时间": "publish_time",
                        "时间": "publish_time",
                        "date": "publish_time",
                        "source": "source",
                    }
                )
                for record in renamed.to_dict(orient="records"):
                    title = str(record.get("title") or record.get("内容") or "").strip()
                    content = str(record.get("content") or record.get("正文") or title).strip()
                    publish_time = record.get("publish_time") or record.get("日期") or context.asof_date.isoformat()
                    rows.append(
                        {
                            "title": title,
                            "content": content,
                            "source": str(record.get("source") or "qstock"),
                            "publish_time": parse_datetime(publish_time).strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    )
        except Exception:
            rows = []

    if not rows:
        manual_news: list[dict[str, Any]] = load_json(context.project_root / "data/manual/sample_news.json")  # type: ignore[assignment]
        for item in manual_news:
            publish_dt = parse_datetime(item["publish_time"])
            if lookback_start <= publish_dt.date() <= context.asof_date:
                rows.append(item)

    news_df = pd.DataFrame(rows).drop_duplicates(subset=["title", "publish_time"]).copy()
    if news_df.empty:
        raise RuntimeError("未获取到新闻数据，无法继续执行。")

    news_df["publish_time"] = pd.to_datetime(news_df["publish_time"])
    news_df = news_df.sort_values("publish_time").reset_index(drop=True)
    news_df["news_id"] = news_df.apply(
        lambda row: hashlib.md5(f"{row['title']}-{row['publish_time']}".encode("utf-8")).hexdigest()[:12],
        axis=1,
    )
    return news_df[["news_id", "title", "content", "source", "publish_time"]]


def fetch_stock_universe(project_root: Path) -> pd.DataFrame:
    """读取本地股票池。"""

    stock_df = pd.read_csv(project_root / "data/manual/stock_universe.csv")
    stock_df["stock_code"] = stock_df["stock_code"].astype(str).str.zfill(6)
    stock_df["listed_date"] = pd.to_datetime(stock_df["listed_date"])
    stock_df["is_st"] = stock_df["is_st"].astype(bool)
    return stock_df


def fetch_price_history(
    stock_codes: list[str],
    start_date: date,
    end_date: date,
    context: RunContext,
    config: AppConfig,
) -> pd.DataFrame:
    """获取股票历史行情，优先走 Tushare，失败则生成可复现的样例行情。"""

    if ts is not None and config.raw.get("tushare", {}).get("token"):
        try:
            pro = ts.pro_api(config.raw["tushare"]["token"])
            frames: list[pd.DataFrame] = []
            for code in stock_codes:
                ts_code = to_tushare_code(code)
                daily = pro.daily(
                    ts_code=ts_code,
                    start_date=start_date.strftime("%Y%m%d"),
                    end_date=end_date.strftime("%Y%m%d"),
                )
                if daily.empty:
                    continue
                daily = daily.rename(
                    columns={
                        "trade_date": "trade_date",
                        "open": "open",
                        "high": "high",
                        "low": "low",
                        "close": "close",
                        "vol": "volume",
                        "pct_chg": "pct_chg",
                        "amount": "amount",
                    }
                )
                daily["stock_code"] = code
                frames.append(daily[["stock_code", "trade_date", "open", "high", "low", "close", "volume", "amount", "pct_chg"]])
            if frames:
                price_df = pd.concat(frames, ignore_index=True)
                price_df["trade_date"] = pd.to_datetime(price_df["trade_date"], format="%Y%m%d")
                price_df = price_df.sort_values(["stock_code", "trade_date"]).reset_index(drop=True)
                return extend_price_history_with_sample(price_df, stock_codes, start_date, end_date)
        except Exception:
            pass

    return generate_sample_price_history(stock_codes, start_date, end_date)


def fetch_benchmark_history(
    benchmark_code: str,
    start_date: date,
    end_date: date,
    context: RunContext,
    config: AppConfig,
) -> pd.DataFrame:
    """获取基准指数行情。"""

    if ts is not None and config.raw.get("tushare", {}).get("token"):
        try:
            pro = ts.pro_api(config.raw["tushare"]["token"])
            index_daily = pro.index_daily(
                ts_code="399300.SZ",
                start_date=start_date.strftime("%Y%m%d"),
                end_date=end_date.strftime("%Y%m%d"),
            )
            if not index_daily.empty:
                index_daily = index_daily.rename(columns={"trade_date": "trade_date"})
                index_daily["stock_code"] = benchmark_code
                index_daily["trade_date"] = pd.to_datetime(index_daily["trade_date"], format="%Y%m%d")
                benchmark_df = index_daily[["stock_code", "trade_date", "open", "high", "low", "close", "vol", "amount", "pct_chg"]].rename(
                    columns={"vol": "volume"}
                )
                return extend_price_history_with_sample(benchmark_df, [benchmark_code], start_date, end_date)
        except Exception:
            pass

    return generate_sample_price_history([benchmark_code], start_date, end_date, base_price=3800.0)


def generate_sample_price_history(
    stock_codes: list[str],
    start_date: date,
    end_date: date,
    base_price: float | None = None,
) -> pd.DataFrame:
    """生成可复现的样例行情，保证无外部依赖时主链路仍可运行。"""

    rows: list[dict[str, Any]] = []
    dates = pd.bdate_range(start_date, end_date)
    for stock_code in stock_codes:
        stock_code = str(stock_code).zfill(6) if str(stock_code).isdigit() else str(stock_code)
        seed = int(hashlib.md5(stock_code.encode("utf-8")).hexdigest()[:8], 16)
        price = base_price or 20 + (seed % 3000) / 100
        trend = ((seed % 19) - 9) / 1000
        amplitude = 0.012 + (seed % 7) / 500

        for index, trade_date in enumerate(dates):
            wave = ((index % 9) - 4) / 1500
            day_return = trend + wave + (((seed >> (index % 16)) & 3) - 1) * amplitude / 6
            open_price = max(1.0, round(price * (1 + day_return / 3), 2))
            close_price = max(1.0, round(price * (1 + day_return), 2))
            high_price = round(max(open_price, close_price) * 1.015, 2)
            low_price = round(min(open_price, close_price) * 0.985, 2)
            volume = int(5_000_000 + (seed % 500_000) + index * 6_000)
            amount = round((open_price + close_price) / 2 * volume / 10000, 2)
            pct_chg = round((close_price / price - 1) * 100, 4)

            rows.append(
                {
                    "stock_code": stock_code,
                    "trade_date": trade_date,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": volume,
                    "amount": amount,
                    "pct_chg": pct_chg,
                }
            )
            price = close_price

    return pd.DataFrame(rows).sort_values(["stock_code", "trade_date"]).reset_index(drop=True)


def extend_price_history_with_sample(
    price_df: pd.DataFrame,
    stock_codes: list[str],
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """在真实数据末尾补齐样例未来价格，便于未来比赛周预演。"""

    completed_frames: list[pd.DataFrame] = []
    normalized_codes = [str(code).zfill(6) if str(code).isdigit() else str(code) for code in stock_codes]
    price_df = price_df.copy()
    price_df["stock_code"] = price_df["stock_code"].astype(str).apply(lambda code: code.zfill(6) if code.isdigit() else code)
    price_df["trade_date"] = pd.to_datetime(price_df["trade_date"])

    for stock_code in normalized_codes:
        subset = price_df[price_df["stock_code"] == stock_code].sort_values("trade_date").copy()
        if subset.empty:
            completed_frames.append(generate_sample_price_history([stock_code], start_date, end_date))
            continue

        completed_frames.append(subset)
        last_trade_date = subset["trade_date"].max().date()
        if last_trade_date >= end_date:
            continue

        seed = int(hashlib.md5(stock_code.encode("utf-8")).hexdigest()[:8], 16)
        price = float(subset.iloc[-1]["close"])
        trend = ((seed % 19) - 9) / 1000
        amplitude = 0.012 + (seed % 7) / 500
        rows: list[dict[str, Any]] = []
        future_dates = pd.bdate_range(last_trade_date + timedelta(days=1), end_date)
        for index, trade_date in enumerate(future_dates, start=1):
            wave = ((index % 9) - 4) / 1500
            day_return = trend + wave + (((seed >> (index % 16)) & 3) - 1) * amplitude / 6
            open_price = max(1.0, round(price * (1 + day_return / 3), 2))
            close_price = max(1.0, round(price * (1 + day_return), 2))
            high_price = round(max(open_price, close_price) * 1.015, 2)
            low_price = round(min(open_price, close_price) * 0.985, 2)
            volume = int(5_000_000 + (seed % 500_000) + index * 6_000)
            amount = round((open_price + close_price) / 2 * volume / 10000, 2)
            pct_chg = round((close_price / price - 1) * 100, 4)
            rows.append(
                {
                    "stock_code": stock_code,
                    "trade_date": trade_date,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": volume,
                    "amount": amount,
                    "pct_chg": pct_chg,
                }
            )
            price = close_price

        completed_frames.append(pd.DataFrame(rows))

    combined = pd.concat(completed_frames, ignore_index=True)
    combined["trade_date"] = pd.to_datetime(combined["trade_date"])
    return combined.sort_values(["stock_code", "trade_date"]).reset_index(drop=True)


def to_tushare_code(stock_code: str) -> str:
    """将裸代码转换为 Tushare 代码。"""

    if stock_code.startswith(("6", "9")):
        return f"{stock_code}.SH"
    return f"{stock_code}.SZ"


def fetch_financial_data(
    stock_codes: list[str],
    context: RunContext,
    config: AppConfig,
) -> pd.DataFrame:
    """获取公司财务指标，优先走 Tushare，失败则用样例数据。"""

    rows: list[dict[str, Any]] = []

    if ts is not None and config.raw.get("tushare", {}).get("token"):
        try:
            pro = ts.pro_api(config.raw["tushare"]["token"])
            for code in stock_codes:
                ts_code = to_tushare_code(code)
                try:
                    fi = pro.fina_indicator(ts_code=ts_code, period=context.asof_date.strftime("%Y%m%d"))
                    if fi is None or fi.empty:
                        continue
                    row = fi.iloc[0]
                    rows.append(
                        {
                            "stock_code": code,
                            "pe": float(row.get("pe", 0) or 0),
                            "pb": float(row.get("pb", 0) or 0),
                            "roe": float(row.get("roe", 0) or 0),
                            "net_profit_growth": float(row.get("netprofit_ratio", 0) or 0),
                            "revenue_growth": float(row.get("revenue_ratio", 0) or 0),
                            "debt_to_asset": float(row.get("debt_to_assets", 0) or 0),
                        }
                    )
                except Exception:
                    continue
            if rows:
                df = pd.DataFrame(rows)
                df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
                return df.sort_values("stock_code").reset_index(drop=True)
        except Exception:
            pass

    sample: list[dict[str, Any]] = load_json(context.project_root / "data/manual/stock_financial_sample.json")
    rows = []
    code_set = set(str(c).zfill(6) for c in stock_codes)
    for item in sample:
        if item["stock_code"] in code_set:
            rows.append(item)
    if not rows:
        rows = sample
    df = pd.DataFrame(rows)
    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
    return df


def fetch_suspend_resume_data(
    stock_codes: list[str],
    context: RunContext,
    config: AppConfig,
) -> pd.DataFrame:
    """获取个股停牌/复牌信息。"""

    rows: list[dict[str, Any]] = []

    if ts is not None and config.raw.get("tushare", {}).get("token"):
        try:
            pro = ts.pro_api(config.raw["tushare"]["token"])
            for code in stock_codes:
                ts_code = to_tushare_code(code)
                try:
                    sus = pro.suspend(ts_code=ts_code, start_date=context.asof_date.strftime("%Y%m%d"))
                    if sus is None or sus.empty:
                        continue
                    for _, row in sus.iterrows():
                        trade_date = row.get("trade_date", "")
                        if isinstance(trade_date, str) and len(trade_date) == 8:
                            trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
                        rows.append(
                            {
                                "stock_code": code,
                                "suspend_date": trade_date,
                                "resume_date": "",
                                "suspend_reason": str(row.get("reason", "") or ""),
                            }
                        )
                except Exception:
                    continue
            if rows:
                df = pd.DataFrame(rows)
                df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
                return df
        except Exception:
            pass

    sample: list[dict[str, Any]] = load_json(context.project_root / "data/manual/suspend_resume_sample.json")
    rows = []
    code_set = set(str(c).zfill(6) for c in stock_codes)
    for item in sample:
        if item["stock_code"] in code_set:
            rows.append(item)
    if not rows:
        rows = sample
    df = pd.DataFrame(rows)
    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
    return df
