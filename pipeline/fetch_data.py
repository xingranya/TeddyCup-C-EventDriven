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
    ensure_directory,
    load_json,
    normalize_text,
    parse_datetime,
    read_code_list,
    save_dataframe,
)

try:
    import tushare as ts
except Exception:  # pragma: no cover - 环境缺依赖时兜底
    ts = None

try:
    import akshare as ak
except Exception:  # pragma: no cover - 环境缺依赖时兜底
    ak = None


@dataclass(slots=True)
class FetchArtifacts:
    """数据采集阶段产物。"""

    news_df: pd.DataFrame
    stock_df: pd.DataFrame
    price_df: pd.DataFrame
    benchmark_df: pd.DataFrame
    trading_calendar: list[date]


def run_fetch_pipeline(context: RunContext, config: AppConfig) -> FetchArtifacts:
    """采集周度运行所需的真实基础数据。"""

    start_date = context.asof_date - timedelta(days=180)
    trading_calendar = fetch_trading_calendar(start_date, context.asof_date + timedelta(days=20), config)
    all_stock_df = fetch_stock_universe(context.project_root, context.asof_date, config)
    news_df = fetch_news(context, config, all_stock_df)
    stock_df = narrow_stock_universe(context.project_root, all_stock_df, news_df)
    price_df = fetch_price_history(
        stock_codes=stock_df["stock_code"].tolist(),
        start_date=start_date,
        end_date=context.asof_date,
        config=config,
        trading_calendar=trading_calendar,
    )
    stock_df = attach_liquidity_metrics(stock_df, price_df, context.asof_date)
    benchmark_df = fetch_benchmark_history(
        benchmark_code=config.benchmark_code,
        start_date=start_date,
        end_date=context.asof_date,
        config=config,
        price_df=price_df,
    )

    save_dataframe(news_df, context.raw_dir / f"news_{context.asof_date.isoformat()}")
    save_dataframe(stock_df, context.raw_dir / "stock_universe")
    save_dataframe(price_df, context.raw_dir / f"prices_{context.asof_date.isoformat()}")
    save_dataframe(benchmark_df, context.raw_dir / f"benchmark_{context.asof_date.isoformat()}")
    save_dataframe(
        pd.DataFrame({"trade_date": pd.to_datetime(trading_calendar)}),
        context.raw_dir / f"trading_calendar_{context.asof_date.isoformat()}",
    )

    return FetchArtifacts(
        news_df=news_df,
        stock_df=stock_df,
        price_df=price_df,
        benchmark_df=benchmark_df,
        trading_calendar=trading_calendar,
    )


def require_tushare_client(config: AppConfig):
    """创建可用的 Tushare 客户端。"""

    if ts is None:
        raise RuntimeError("当前环境未安装 tushare，无法执行竞赛模式数据采集。")
    if not config.tushare_token:
        raise RuntimeError("未提供 Tushare 凭证，无法执行竞赛模式数据采集。")
    return ts.pro_api(config.tushare_token)


def load_qstock_module():
    """按需加载 qstock，避免默认运行时引入额外不稳定依赖。"""

    if sys.version_info >= (3, 14):
        return None
    try:
        import qstock as qstock_module
        # qstock 内部设置了 SimHei 字体，在 macOS 上会导致 findfont 警告
        # 导入后重新配置中文字体
        from pipeline.utils import configure_matplotlib_chinese
        configure_matplotlib_chinese()
    except Exception:
        return None
    return qstock_module


def fetch_trading_calendar(start_date: date, end_date: date, config: AppConfig) -> list[date]:
    """获取交易日历。"""

    if config.trading_calendar_source != "tushare":
        raise RuntimeError(f"暂不支持的交易日历来源：{config.trading_calendar_source}")

    # 1. 尝试 tushare
    try:
        pro = require_tushare_client(config)
        calendar_df = pro.trade_cal(
            exchange="SSE",
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
            is_open="1",
        )
        if calendar_df is not None and not calendar_df.empty:
            calendar_df["trade_date"] = pd.to_datetime(calendar_df["cal_date"], format="%Y%m%d").dt.date
            return sorted(calendar_df["trade_date"].unique().tolist())
    except Exception as e1:
        print(f"[WARN] tushare trading calendar failed: {e1}")

    # 2. 尝试 akshare（捕获 py_mini_racer / 网络等错误）
    if ak is not None:
        try:
            calendar_df = ak.tool_trade_date_hist_sina()
            if calendar_df is not None and not calendar_df.empty:
                calendar_df["trade_date"] = pd.to_datetime(calendar_df["trade_date"]).dt.date
                calendar_df = calendar_df[
                    (calendar_df["trade_date"] >= start_date)
                    & (calendar_df["trade_date"] <= end_date)
                ].copy()
                return sorted(calendar_df["trade_date"].unique().tolist())
        except Exception as e2:
            print(f"[WARN] akshare trading calendar failed: {e2}")

    # 3. 从本地缓存加载（扫描 data/raw/*/trading_calendar_*.csv）
    try:
        import glob as _glob
        # 尝试当前工作目录 / 脚本所在目录
        _search_roots = [Path.cwd(), Path(__file__).parent.parent]
        cache_files: list[str] = []
        for _root in _search_roots:
            _pattern = str(_root / "data" / "raw" / "*" / "trading_calendar_*.csv")
            _found = sorted(_glob.glob(_pattern))
            if _found:
                cache_files = _found
                break
        if cache_files:
            # 读取所有缓存文件，合并去重
            frames: list[pd.DataFrame] = []
            for cache_file in cache_files:
                try:
                    df = pd.read_csv(cache_file)
                    frames.append(df)
                except Exception:
                    continue
            if frames:
                combined = pd.concat(frames, ignore_index=True)
                combined["trade_date"] = pd.to_datetime(combined["trade_date"]).dt.date
                combined = combined[
                    (combined["trade_date"] >= start_date)
                    & (combined["trade_date"] <= end_date)
                ].copy()
                result = sorted(combined["trade_date"].unique().tolist())
                if result:
                    print(f"[INFO] 使用本地缓存交易日历，共 {len(result)} 个交易日")
                    return result
    except Exception as e3:
        print(f"[WARN] local calendar cache failed: {e3}")

    # 4. 最终兜底：使用 pandas bdate_range 生成工作日列表（不含周末）
    print("[WARN] 所有交易日历数据源均不可用，使用工作日列表作为兜底（不含节假日调整）")
    bdate_series = pd.bdate_range(start=start_date, end=end_date)
    return [d.date() for d in bdate_series]


def fetch_news(context: RunContext, config: AppConfig, stock_df: pd.DataFrame) -> pd.DataFrame:
    """获取多源事件数据并标准化。"""

    lookback_start = context.asof_date - timedelta(days=config.lookback_days)
    stock_names = stock_df["stock_name"].dropna().astype(str).unique().tolist()
    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows: list[dict[str, Any]] = []

    if config.qstock_enabled:
        qstock_module = load_qstock_module()
    else:
        qstock_module = None

    if qstock_module is not None:
        try:
            qstock_df = qstock_module.news_data(
                start=lookback_start.strftime("%Y%m%d"),
                end=context.asof_date.strftime("%Y%m%d"),
            )
        except Exception:  # pragma: no cover - 外部接口稳定性不可控
            qstock_df = None

        if qstock_df is not None and not qstock_df.empty:
            for record in qstock_df.to_dict(orient="records"):
                raw_time_val = (
                    record.get("发布时间")
                    or record.get("时间")
                    or record.get("date")
                    or record.get("publish_time")
                    or record.get("日期")
                )
                if not raw_time_val:
                    continue
                raw_time_str = str(raw_time_val).strip()
                # 如果只有时间部分（如 '19:54:50'），补充当日日期
                if len(raw_time_str) <= 8 and ":" in raw_time_str and "-" not in raw_time_str:
                    raw_time_str = f"{context.asof_date.isoformat()} {raw_time_str}"
                try:
                    published_at = parse_datetime(raw_time_str)
                except Exception:
                    continue
                if not (lookback_start <= published_at.date() <= context.asof_date):
                    continue
                title = str(record.get("title") or record.get("标题") or "").strip()
                content = str(record.get("content") or record.get("正文") or title).strip()
                if not title or not content:
                    continue
                rows.append(
                    _build_event_record(
                        title=title,
                        content=content,
                        published_at=published_at,
                        source="qstock",
                        source_type="industry",
                        source_name="qstock.news_data",
                        source_url=str(record.get("url") or record.get("链接") or ""),
                        raw_id=str(record.get("id") or ""),
                        stock_names=stock_names,
                        collected_at=collected_at,
                    )
                )

    for source_type, relative_path in config.event_import_paths.items():
        import_dir = context.project_root / relative_path
        rows.extend(
            load_imported_event_records(
                import_dir=import_dir,
                source_type=source_type,
                stock_names=stock_names,
                collected_at=collected_at,
                lookback_start=lookback_start,
                asof_date=context.asof_date,
            )
        )

    # 在 qstock 采集失败或禁用后，尝试 akshare
    if not rows:
        try:
            rows.extend(_fetch_akshare_news(context, config, stock_df))
        except Exception as e:
            print(f"[WARN] akshare news fetch failed: {e}")

    if not rows:
        raise RuntimeError(
            "未获取到任何事件数据。请选择以下方式之一：\n"
            "1. 在 data/events/ 目录下准备事件JSON文件（参见 data/events/ 已有文件格式）\n"
            "2. 确保网络连接正常以使用 akshare 自动采集\n"
            "3. 检查 config.yaml 中 events.import_paths 配置是否正确"
        )

    news_df = pd.DataFrame(rows).drop_duplicates(subset=["content_hash"]).copy()
    news_df["published_at"] = pd.to_datetime(news_df["published_at"])
    news_df = news_df.sort_values(["published_at", "source_name", "raw_id"]).reset_index(drop=True)
    news_df["news_id"] = news_df["content_hash"].str[:12]
    return news_df[
        [
            "news_id",
            "raw_id",
            "title",
            "content",
            "source",
            "source_type",
            "source_name",
            "source_url",
            "published_at",
            "entity_candidates",
            "content_hash",
            "collected_at",
        ]
    ]


def load_imported_event_records(
    import_dir: Path,
    source_type: str,
    stock_names: list[str],
    collected_at: str,
    lookback_start: date,
    asof_date: date,
) -> list[dict[str, Any]]:
    """加载规范化导入事件文件。"""

    if not import_dir.exists():
        return []

    rows: list[dict[str, Any]] = []
    for path in sorted(import_dir.iterdir()):
        if path.suffix.lower() == ".json":
            payload = load_json(path)
            if isinstance(payload, dict):
                raw_records = payload.get("records", [])
            else:
                raw_records = payload
        elif path.suffix.lower() == ".csv":
            raw_records = pd.read_csv(path).to_dict(orient="records")
        else:
            continue

        for raw_record in raw_records:
            title = str(raw_record.get("title") or raw_record.get("标题") or "").strip()
            content = str(raw_record.get("content") or raw_record.get("正文") or title).strip()
            published_value = (
                raw_record.get("published_at")
                or raw_record.get("publish_time")
                or raw_record.get("发布时间")
                or raw_record.get("date")
                or raw_record.get("日期")
            )
            if not title or not content or not published_value:
                raise RuntimeError(f"导入事件文件缺少必填字段：{path}")
            published_at = parse_datetime(str(published_value))
            if not (lookback_start <= published_at.date() <= asof_date):
                continue
            rows.append(
                _build_event_record(
                    title=title,
                    content=content,
                    published_at=published_at,
                    source=source_type if source_type in {"policy", "announcement", "industry", "macro"} else "import",
                    source_type=source_type,
                    source_name=str(raw_record.get("source_name") or path.name),
                    source_url=str(raw_record.get("source_url") or raw_record.get("url") or ""),
                    raw_id=str(raw_record.get("raw_id") or raw_record.get("id") or f"{path.stem}-{len(rows) + 1}"),
                    stock_names=stock_names,
                    collected_at=collected_at,
                )
            )
    return rows


def _build_event_record(
    title: str,
    content: str,
    published_at: datetime,
    source: str,
    source_type: str,
    source_name: str,
    source_url: str,
    raw_id: str,
    stock_names: list[str],
    collected_at: str,
) -> dict[str, Any]:
    """构造统一事件记录。"""

    entity_candidates = extract_entity_candidates(f"{title} {content}", stock_names)
    content_hash = hashlib.md5(
        f"{title}|{content}|{published_at.isoformat()}|{source_url}".encode("utf-8")
    ).hexdigest()
    return {
        "raw_id": raw_id or content_hash[:16],
        "title": title,
        "content": content,
        "source": source,
        "source_type": source_type,
        "source_name": source_name,
        "source_url": source_url,
        "published_at": published_at.strftime("%Y-%m-%d %H:%M:%S"),
        "entity_candidates": "、".join(entity_candidates),
        "content_hash": content_hash,
        "collected_at": collected_at,
    }


def extract_entity_candidates(text: str, stock_names: list[str]) -> list[str]:
    """从文本中抽取股票名称候选。"""

    normalized = normalize_text(text)
    matched: list[str] = []
    for stock_name in stock_names:
        token = normalize_text(stock_name)
        if token and token in normalized:
            matched.append(stock_name)
    return sorted(set(matched))[:12]


def _fetch_akshare_news(
    context: RunContext, config: AppConfig, stock_df: pd.DataFrame
) -> list[dict[str, Any]]:
    """使用 akshare 采集财经新闻作为 qstock 的降级备选。"""

    if ak is None:
        return []

    lookback_start = context.asof_date - timedelta(days=config.lookback_days)
    stock_names = stock_df["stock_name"].dropna().astype(str).unique().tolist()
    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows: list[dict[str, Any]] = []

    # 尝试东方财富全球财经快讯
    try:
        em_df = ak.stock_info_global_em()
        if em_df is not None and not em_df.empty:
            for record in em_df.to_dict(orient="records"):
                published_at = parse_datetime(
                    record.get("发布时间")
                    or record.get("时间")
                    or record.get("date")
                )
                if not (lookback_start <= published_at.date() <= context.asof_date):
                    continue
                title = str(record.get("标题") or "").strip()
                content = str(record.get("摘要") or title).strip()
                if not title or not content:
                    continue
                rows.append(
                    _build_event_record(
                        title=title,
                        content=content,
                        published_at=published_at,
                        source="akshare",
                        source_type="industry",
                        source_name="akshare.stock_info_global_em",
                        source_url=str(record.get("链接") or ""),
                        raw_id=str(record.get("id") or f"em-{len(rows) + 1}"),
                        stock_names=stock_names,
                        collected_at=collected_at,
                    )
                )
    except Exception as e:
        print(f"[WARN] akshare stock_info_global_em failed: {e}")

    # 尝试财联社新闻
    try:
        cls_df = ak.stock_info_global_cls()
        if cls_df is not None and not cls_df.empty:
            for record in cls_df.to_dict(orient="records"):
                # 财联社新闻有发布日期和发布时间两个字段
                pub_date = record.get("发布日期") or ""
                pub_time = record.get("发布时间") or ""
                if pub_date and pub_time:
                    published_at = parse_datetime(f"{pub_date} {pub_time}")
                elif pub_date:
                    published_at = parse_datetime(str(pub_date))
                else:
                    published_at = parse_datetime(str(pub_time))
                if not (lookback_start <= published_at.date() <= context.asof_date):
                    continue
                title = str(record.get("标题") or "").strip()
                content = str(record.get("内容") or title).strip()
                if not title or not content:
                    continue
                rows.append(
                    _build_event_record(
                        title=title,
                        content=content,
                        published_at=published_at,
                        source="akshare",
                        source_type="industry",
                        source_name="akshare.stock_info_global_cls",
                        source_url="",
                        raw_id=f"cls-{len(rows) + 1}",
                        stock_names=stock_names,
                        collected_at=collected_at,
                    )
                )
    except Exception as e:
        print(f"[WARN] akshare stock_info_global_cls failed: {e}")

    # 尝试新浪财经
    try:
        sina_df = ak.stock_info_global_sina()
        if sina_df is not None and not sina_df.empty:
            for record in sina_df.to_dict(orient="records"):
                raw_time = record.get("时间") or record.get("时间")
                if not raw_time:
                    continue
                raw_time_str = str(raw_time).strip()
                # 如果只有时间部分（如 '19:54:50'），补充当日日期
                if len(raw_time_str) <= 8 and ":" in raw_time_str and "-" not in raw_time_str:
                    raw_time_str = f"{context.asof_date.isoformat()} {raw_time_str}"
                try:
                    published_at = parse_datetime(raw_time_str)
                except Exception:
                    continue
                if not (lookback_start <= published_at.date() <= context.asof_date):
                    continue
                content = str(record.get("内容") or "").strip()
                if not content:
                    continue
                # 新浪财经新闻没有独立标题，使用内容前50字作为标题
                title = content[:50] + "..." if len(content) > 50 else content
                rows.append(
                    _build_event_record(
                        title=title,
                        content=content,
                        published_at=published_at,
                        source="akshare",
                        source_type="industry",
                        source_name="akshare.stock_info_global_sina",
                        source_url="",
                        raw_id=f"sina-{len(rows) + 1}",
                        stock_names=stock_names,
                        collected_at=collected_at,
                    )
                )
    except Exception as e:
        print(f"[WARN] akshare stock_info_global_sina failed: {e}")

    return rows


def fetch_stock_universe(project_root: Path, asof_date: date, config: AppConfig) -> pd.DataFrame:
    """获取全量在市 A 股股票池。"""

    stock_df = pd.DataFrame()
    try:
        pro = require_tushare_client(config)
        basic_df = pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,industry,list_date",
        )
        if basic_df is not None and not basic_df.empty:
            company_frames: list[pd.DataFrame] = []
            for exchange in ("SSE", "SZSE", "BSE"):
                try:
                    company_df = pro.stock_company(
                        exchange=exchange,
                        fields="ts_code,main_business,business_scope",
                    )
                except Exception:
                    continue
                if company_df is not None and not company_df.empty:
                    company_frames.append(company_df)

            company_df = (
                pd.concat(company_frames, ignore_index=True)
                .drop_duplicates(subset=["ts_code"])
                if company_frames
                else pd.DataFrame(columns=["ts_code", "main_business", "business_scope"])
            )

            stock_df = basic_df.merge(company_df, on="ts_code", how="left")
            stock_df["stock_code"] = stock_df["symbol"].astype(str).str.zfill(6)
            stock_df["stock_name"] = stock_df["name"].astype(str)
            stock_df["industry"] = stock_df["industry"].fillna("未知行业")
            stock_df["concept_tags"] = ""
            stock_df["main_business"] = (
                stock_df["main_business"]
                .fillna(stock_df["business_scope"])
                .fillna(stock_df["industry"])
                .astype(str)
            )
            stock_df["listed_date"] = pd.to_datetime(stock_df["list_date"], format="%Y%m%d", errors="coerce")
            stock_df["is_st"] = stock_df["stock_name"].str.contains("ST", case=False, na=False)
            stock_df = stock_df[stock_df["listed_date"].notna()].copy()
            stock_df = stock_df[stock_df["listed_date"].dt.date <= asof_date].copy()
    except Exception:
        stock_df = pd.DataFrame()

    if stock_df.empty:
        manual_path = project_root / "data/manual/stock_universe.csv"
        if not manual_path.exists():
            raise RuntimeError("未获取到股票池数据，且本地无可用候选池缓存。")
        stock_df = pd.read_csv(manual_path)
        stock_df["stock_code"] = stock_df["stock_code"].astype(str).str.zfill(6)
        stock_df["stock_name"] = stock_df["stock_name"].astype(str)
        stock_df["industry"] = stock_df["industry"].fillna("未知行业")
        stock_df["concept_tags"] = stock_df["concept_tags"].fillna("")
        stock_df["main_business"] = stock_df["main_business"].fillna(stock_df["industry"])
        stock_df["listed_date"] = pd.to_datetime(stock_df["listed_date"], errors="coerce")
        stock_df["is_st"] = stock_df["is_st"].astype(bool)

    whitelist_codes = read_code_list(project_root / config.stock_whitelist_path) if config.stock_whitelist_path else set()
    blacklist_codes = read_code_list(project_root / config.stock_blacklist_path) if config.stock_blacklist_path else set()
    if whitelist_codes:
        stock_df = stock_df[stock_df["stock_code"].isin(whitelist_codes)].copy()
    if blacklist_codes:
        stock_df = stock_df[~stock_df["stock_code"].isin(blacklist_codes)].copy()

    return stock_df[
        [
            "stock_code",
            "stock_name",
            "industry",
            "concept_tags",
            "main_business",
            "listed_date",
            "is_st",
        ]
    ].sort_values("stock_code").reset_index(drop=True)


def fetch_price_history(
    stock_codes: list[str],
    start_date: date,
    end_date: date,
    config: AppConfig,
    trading_calendar: list[date] | None = None,
) -> pd.DataFrame:
    """获取个股历史行情。"""

    pro = require_tushare_client(config)
    frames: list[pd.DataFrame] = []
    code_set = {
        str(code).zfill(6)
        for code in stock_codes
    }

    for stock_code in sorted(code_set):
        ts_code = to_tushare_code(stock_code)
        daily_df = pro.daily(
            ts_code=ts_code,
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
        )
        if daily_df is None or daily_df.empty:
            continue
        daily_df["stock_code"] = stock_code
        daily_df["trade_date"] = pd.to_datetime(daily_df["trade_date"], format="%Y%m%d")
        frames.append(
            daily_df[
                ["stock_code", "trade_date", "open", "high", "low", "close", "vol", "amount", "pct_chg"]
            ].rename(columns={"vol": "volume"})
        )

    if not frames:
        raise RuntimeError("未获取到个股行情数据，无法继续执行。")

    return pd.concat(frames, ignore_index=True).sort_values(
        ["stock_code", "trade_date"]
    ).reset_index(drop=True)


def narrow_stock_universe(project_root: Path, stock_df: pd.DataFrame, news_df: pd.DataFrame) -> pd.DataFrame:
    """根据事件文本与产业映射收缩候选股票池。"""

    relation_map = load_json(project_root / "data/manual/industry_relation_map.json")
    combined_text = normalize_text(" ".join((news_df["title"] + " " + news_df["content"]).tolist()))
    selected_codes: set[str] = set()

    for entities in news_df["entity_candidates"].fillna("").astype(str):
        for stock_name in filter(None, entities.split("、")):
            matched_df = stock_df[stock_df["stock_name"] == stock_name]
            if not matched_df.empty:
                selected_codes.update(matched_df["stock_code"].astype(str).tolist())

    for payload in relation_map.values():
        keywords = [normalize_text(keyword) for keyword in payload.get("keywords", [])]
        if any(keyword and keyword in combined_text for keyword in keywords):
            selected_codes.update(
                str(item["stock_code"]).zfill(6)
                for item in payload.get("stocks", [])
            )

    if not selected_codes:
        selected_codes.update(
            stock_df.head(50)["stock_code"].astype(str).tolist()
        )

    narrowed_df = stock_df[stock_df["stock_code"].astype(str).isin(selected_codes)].copy()
    manual_path = project_root / "data/manual/stock_universe.csv"
    if manual_path.exists():
        manual_df = pd.read_csv(manual_path)
        manual_df["stock_code"] = manual_df["stock_code"].astype(str).str.zfill(6)
        narrowed_df = narrowed_df.merge(
            manual_df[["stock_code", "concept_tags", "main_business"]],
            on="stock_code",
            how="left",
            suffixes=("", "_manual"),
        )
        narrowed_df["concept_tags"] = narrowed_df["concept_tags_manual"].fillna(narrowed_df["concept_tags"]).fillna("")
        narrowed_df["main_business"] = narrowed_df["main_business_manual"].fillna(narrowed_df["main_business"]).fillna(narrowed_df["industry"])
        narrowed_df = narrowed_df.drop(columns=["concept_tags_manual", "main_business_manual"])

    return narrowed_df.sort_values("stock_code").reset_index(drop=True)


def attach_liquidity_metrics(stock_df: pd.DataFrame, price_df: pd.DataFrame, asof_date: date) -> pd.DataFrame:
    """基于真实行情补充流动性指标。"""

    history_df = price_df[pd.to_datetime(price_df["trade_date"]).dt.date <= asof_date].copy()
    trailing_df = history_df.groupby("stock_code").tail(20).copy()
    liquidity_df = (
        trailing_df.groupby("stock_code")["amount"]
        .mean()
        .reset_index(name="avg_amount_thousand")
    )
    liquidity_df["avg_turnover_million"] = (
        liquidity_df["avg_amount_thousand"].astype(float) / 1000
    ).round(4)
    enriched_df = stock_df.merge(
        liquidity_df[["stock_code", "avg_turnover_million"]],
        on="stock_code",
        how="left",
    )
    enriched_df["avg_turnover_million"] = enriched_df["avg_turnover_million"].fillna(0.0)
    return enriched_df


def fetch_benchmark_history(
    benchmark_code: str,
    start_date: date,
    end_date: date,
    config: AppConfig,
    price_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """获取基准指数行情。"""

    try:
        pro = require_tushare_client(config)
        benchmark_df = pro.index_daily(
            ts_code=benchmark_code,
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
        )
        if benchmark_df is not None and not benchmark_df.empty:
            benchmark_df["stock_code"] = benchmark_code
            benchmark_df["trade_date"] = pd.to_datetime(benchmark_df["trade_date"], format="%Y%m%d")
            return benchmark_df[
                ["stock_code", "trade_date", "open", "high", "low", "close", "vol", "amount", "pct_chg"]
            ].rename(columns={"vol": "volume"}).sort_values("trade_date").reset_index(drop=True)
    except Exception:
        pass

    if price_df is None or price_df.empty:
        raise RuntimeError(f"未获取到基准指数 {benchmark_code}，且无法从候选股票池构造市场代理序列。")
    return build_proxy_benchmark_from_prices(price_df, benchmark_code)


def fetch_financial_data(
    stock_codes: list[str],
    context: RunContext,
    config: AppConfig,
    trading_calendar: list[date],
) -> pd.DataFrame:
    """获取真实财务快照。"""

    if not stock_codes:
        return pd.DataFrame(
            columns=[
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
        )

    rows: list[dict[str, Any]] = []
    daily_basic_df = pd.DataFrame()
    try:
        pro = require_tushare_client(config)
        latest_trade_date = max(trade_date for trade_date in trading_calendar if trade_date <= context.asof_date)
        daily_basic_df = pro.daily_basic(
            trade_date=latest_trade_date.strftime("%Y%m%d"),
            fields="ts_code,trade_date,pe,pb,turnover_rate",
        )
        if daily_basic_df is not None and not daily_basic_df.empty:
            daily_basic_df["stock_code"] = daily_basic_df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
            daily_basic_df["snapshot_trade_date"] = pd.to_datetime(daily_basic_df["trade_date"], format="%Y%m%d")
            daily_basic_df = daily_basic_df[
                daily_basic_df["stock_code"].isin({str(code).zfill(6) for code in stock_codes})
            ].copy()
            for stock_code in sorted(daily_basic_df["stock_code"].unique().tolist()):
                ts_code = to_tushare_code(stock_code)
                indicator_df = pro.fina_indicator(ts_code=ts_code)
                latest_row = select_disclosed_indicator_row(indicator_df, context.asof_date)
                if latest_row is None:
                    rows.append({"stock_code": stock_code})
                    continue
                rows.append(
                    {
                        "stock_code": stock_code,
                        "roe": normalize_ratio_value(latest_row.get("roe")),
                        "net_profit_growth": normalize_ratio_value(
                            latest_row.get("q_dtprofit_yoy")
                            or latest_row.get("netprofit_yoy")
                            or latest_row.get("q_netprofit_yoy")
                        ),
                        "revenue_growth": normalize_ratio_value(
                            latest_row.get("q_sales_yoy")
                            or latest_row.get("tr_yoy")
                            or latest_row.get("or_yoy")
                        ),
                        "debt_to_asset": normalize_ratio_value(latest_row.get("debt_to_assets")),
                        "ann_date": latest_row["ann_date"].strftime("%Y-%m-%d"),
                        "report_period": str(latest_row.get("end_date") or ""),
                    }
                )
    except Exception:
        daily_basic_df = pd.DataFrame()
        rows = []

    if daily_basic_df.empty:
        return fetch_financial_data_from_public_sources(stock_codes, context)

    indicator_snapshot_df = pd.DataFrame(rows)
    financial_df = daily_basic_df.merge(indicator_snapshot_df, on="stock_code", how="left")
    financial_df["pe"] = pd.to_numeric(financial_df["pe"], errors="coerce")
    financial_df["pb"] = pd.to_numeric(financial_df["pb"], errors="coerce")
    financial_df["turnover_rate"] = pd.to_numeric(financial_df["turnover_rate"], errors="coerce")
    return financial_df[
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
    ].sort_values("stock_code").reset_index(drop=True)


def select_disclosed_indicator_row(indicator_df: pd.DataFrame | None, asof_date: date) -> pd.Series | None:
    """选择指定日期前最新披露的财务指标。"""

    if indicator_df is None or indicator_df.empty:
        return None
    working_df = indicator_df.copy()
    working_df["ann_date"] = pd.to_datetime(working_df["ann_date"], format="%Y%m%d", errors="coerce")
    working_df = working_df[working_df["ann_date"].notna()].copy()
    working_df = working_df[working_df["ann_date"].dt.date <= asof_date].copy()
    if working_df.empty:
        return None
    working_df = working_df.sort_values(["ann_date", "end_date"], ascending=[False, False])
    return working_df.iloc[0]


def fetch_financial_data_from_public_sources(stock_codes: list[str], context: RunContext) -> pd.DataFrame:
    """从公开源补齐财务指标。"""

    if ak is None:
        raise RuntimeError("未获取到财务快照数据，且当前环境缺少可用公开财务源。")

    allowed_period = latest_allowed_report_period(context.asof_date)
    rows: list[dict[str, Any]] = []
    for stock_code in stock_codes:
        try:
            abstract_df = ak.stock_financial_abstract_ths(symbol=stock_code)
        except Exception:
            abstract_df = pd.DataFrame()
        if abstract_df is None or abstract_df.empty:
            rows.append(build_empty_financial_snapshot_row(stock_code, context.asof_date))
            continue

        abstract_df = abstract_df.copy()
        abstract_df["报告期"] = pd.to_datetime(abstract_df["报告期"], errors="coerce")
        abstract_df = abstract_df[abstract_df["报告期"].notna()].copy()
        abstract_df = abstract_df[abstract_df["报告期"].dt.date <= allowed_period].copy()
        if abstract_df.empty:
            rows.append(build_empty_financial_snapshot_row(stock_code, context.asof_date))
            continue
        latest_row = abstract_df.sort_values("报告期", ascending=False).iloc[0]
        rows.append(
            {
                "stock_code": stock_code,
                "pe": None,
                "pb": None,
                "turnover_rate": None,
                "roe": parse_percent_or_number(latest_row.get("净资产收益率-摊薄") or latest_row.get("净资产收益率")),
                "net_profit_growth": parse_percent_or_number(latest_row.get("净利润同比增长率")),
                "revenue_growth": parse_percent_or_number(latest_row.get("营业总收入同比增长率")),
                "debt_to_asset": parse_percent_or_number(latest_row.get("资产负债率")),
                "ann_date": "",
                "report_period": latest_row["报告期"].strftime("%Y%m%d"),
                "snapshot_trade_date": pd.Timestamp(context.asof_date),
            }
        )

    financial_df = pd.DataFrame(rows)
    for column in [
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
    ]:
        if column not in financial_df.columns:
            financial_df[column] = None
    for column in ["pe", "pb", "turnover_rate", "roe", "net_profit_growth", "revenue_growth", "debt_to_asset"]:
        financial_df[column] = pd.to_numeric(financial_df[column], errors="coerce")
    return financial_df[
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
    ].sort_values("stock_code").reset_index(drop=True)


def build_empty_financial_snapshot_row(stock_code: str, asof_date: date) -> dict[str, Any]:
    """构造空的财务快照行，保证兜底路径 schema 稳定。"""

    return {
        "stock_code": stock_code,
        "pe": None,
        "pb": None,
        "turnover_rate": None,
        "roe": None,
        "net_profit_growth": None,
        "revenue_growth": None,
        "debt_to_asset": None,
        "ann_date": "",
        "report_period": "",
        "snapshot_trade_date": pd.Timestamp(asof_date),
    }


def build_proxy_benchmark_from_prices(price_df: pd.DataFrame, benchmark_code: str) -> pd.DataFrame:
    """用候选股票池横截面收益构造市场代理序列。"""

    ordered_df = price_df.sort_values(["stock_code", "trade_date"]).copy()
    ordered_df["trade_date"] = pd.to_datetime(ordered_df["trade_date"])
    ordered_df["return"] = ordered_df.groupby("stock_code")["close"].pct_change()
    market_df = (
        ordered_df.groupby("trade_date")
        .agg(
            market_return=("return", "median"),
            open=("open", "median"),
            high=("high", "median"),
            low=("low", "median"),
            close=("close", "median"),
        )
        .reset_index()
        .sort_values("trade_date")
    )
    market_df["market_return"] = market_df["market_return"].fillna(0.0)
    base_price = 100.0
    closes: list[float] = []
    current_price = base_price
    for daily_return in market_df["market_return"].tolist():
        current_price = current_price * (1 + float(daily_return))
        closes.append(round(current_price, 4))
    market_df["close"] = closes
    market_df["open"] = market_df["close"].shift(1).fillna(base_price)
    market_df["high"] = market_df[["open", "close"]].max(axis=1)
    market_df["low"] = market_df[["open", "close"]].min(axis=1)
    market_df["volume"] = 0.0
    market_df["amount"] = 0.0
    market_df["pct_chg"] = (market_df["market_return"] * 100).round(4)
    market_df["stock_code"] = benchmark_code
    return market_df[
        ["stock_code", "trade_date", "open", "high", "low", "close", "volume", "amount", "pct_chg"]
    ].reset_index(drop=True)


def latest_allowed_report_period(asof_date: date) -> date:
    """按披露时点给出保守可见的最近报告期。"""

    year = asof_date.year
    if asof_date < date(year, 4, 30):
        return date(year - 1, 9, 30)
    if asof_date < date(year, 8, 31):
        return date(year, 3, 31)
    if asof_date < date(year, 10, 31):
        return date(year, 6, 30)
    return date(year, 9, 30)


def parse_percent_or_number(value: Any) -> float | None:
    """解析东方财富/同花顺财务字段。"""

    if value in {None, "", "False", False, "nan"}:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text.lower() == "false":
        return None
    if text.endswith("%"):
        try:
            return round(float(text[:-1]) / 100, 6)
        except ValueError:
            return None
    try:
        return normalize_ratio_value(text)
    except Exception:
        return None


def fetch_suspend_resume_data(
    stock_codes: list[str],
    context: RunContext,
    config: AppConfig,
) -> pd.DataFrame:
    """获取停复牌信息，允许开区间停牌。"""

    if not stock_codes:
        return pd.DataFrame(
            columns=[
                "stock_code",
                "suspend_date",
                "resume_date",
                "suspend_reason",
                "source_name",
                "collected_at",
            ]
        )

    rows: list[dict[str, Any]] = []
    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_date = (context.asof_date - timedelta(days=30)).strftime("%Y%m%d")
    end_date = (context.asof_date + timedelta(days=10)).strftime("%Y%m%d")
    try:
        pro = require_tushare_client(config)
        for stock_code in stock_codes:
            ts_code = to_tushare_code(stock_code)
            try:
                suspend_df = pro.suspend(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                )
            except TypeError:
                suspend_df = pro.suspend(ts_code=ts_code, start_date=start_date)
            except Exception:
                suspend_df = None
            if suspend_df is None or suspend_df.empty:
                continue
            for _, row in suspend_df.iterrows():
                rows.append(
                    {
                        "stock_code": stock_code,
                        "suspend_date": normalize_tushare_date(row.get("suspend_date") or row.get("trade_date")),
                        "resume_date": normalize_tushare_date(row.get("resume_date") or row.get("resump_date")),
                        "suspend_reason": str(row.get("suspend_reason") or row.get("reason") or ""),
                        "source_name": "tushare.suspend",
                        "collected_at": collected_at,
                    }
                )
    except Exception:
        rows = []

    if not rows:
        return pd.DataFrame(
            columns=[
                "stock_code",
                "suspend_date",
                "resume_date",
                "suspend_reason",
                "source_name",
                "collected_at",
            ]
        )

    return pd.DataFrame(rows).sort_values(["stock_code", "suspend_date"]).reset_index(drop=True)


def normalize_ratio_value(value: Any) -> float | None:
    """将百分比指标统一归一为小数。"""

    if value in {None, ""}:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if abs(numeric) > 1.5:
        return round(numeric / 100, 6)
    return round(numeric, 6)


def normalize_tushare_date(value: Any) -> str:
    """规范化 Tushare 日期字段。"""

    if value in {None, ""}:
        return ""
    text = str(value)
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return str(pd.Timestamp(text).date())


def to_tushare_code(stock_code: str) -> str:
    """将裸代码转换为 Tushare 代码。"""

    normalized_code = str(stock_code).zfill(6)
    if normalized_code.startswith(("6", "9")):
        return f"{normalized_code}.SH"
    if normalized_code.startswith("8"):
        return f"{normalized_code}.BJ"
    return f"{normalized_code}.SZ"
