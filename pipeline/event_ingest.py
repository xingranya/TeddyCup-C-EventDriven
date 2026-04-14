from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import re
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

import pandas as pd

from pipeline.utils import ensure_directory, normalize_text, parse_date, parse_datetime


logger = logging.getLogger(__name__)

COLLECTOR_NAME = "scripts/event_ingest.py"
COLLECTOR_VERSION = "2026-04-v1"
RAW_FILENAME = "records.jsonl"
GLOBAL_REVIEW_QUEUE = Path("data/staging/events/review_queue.csv")
DETAIL_FETCH_TIMEOUT_SECONDS = 5


@dataclass(frozen=True)
class SourceProfile:
    """事件来源配置。"""

    source: str
    source_type: str
    source_name: str
    mode: str
    default_urls: tuple[str, ...] = ()
    default_feed_urls: tuple[str, ...] = ()
    allowed_domains: tuple[str, ...] = ()


SOURCE_PROFILES: dict[str, SourceProfile] = {
    "gov_cn": SourceProfile(
        source="gov_cn",
        source_type="policy",
        source_name="中国政府网",
        mode="auto_web",
        default_urls=("https://www.gov.cn/zhengce/zuixin/",),
        default_feed_urls=("https://www.gov.cn/zhengce/zuixin/ZUIXINZHENGCE.json",),
        allowed_domains=("gov.cn",),
    ),
    "ndrc": SourceProfile(
        source="ndrc",
        source_type="policy",
        source_name="国家发展改革委",
        mode="auto_web",
        default_urls=("https://www.ndrc.gov.cn/xwdt/tzgg/",),
        allowed_domains=("ndrc.gov.cn",),
    ),
    "csrc": SourceProfile(
        source="csrc",
        source_type="policy",
        source_name="中国证监会",
        mode="auto_web",
        default_urls=("https://www.csrc.gov.cn/csrc/c100027/common_list.shtml",),
        allowed_domains=("csrc.gov.cn",),
    ),
    "cninfo": SourceProfile(
        source="cninfo",
        source_type="announcement",
        source_name="巨潮资讯网",
        mode="manual_input",
        allowed_domains=("cninfo.com.cn",),
    ),
    "eastmoney_industry": SourceProfile(
        source="eastmoney_industry",
        source_type="industry",
        source_name="东方财富行业频道",
        mode="manual_input",
        allowed_domains=("eastmoney.com",),
    ),
    "36kr_manual": SourceProfile(
        source="36kr_manual",
        source_type="industry",
        source_name="36氪产业板块",
        mode="manual_input",
        allowed_domains=("36kr.com",),
    ),
    "yicai_manual": SourceProfile(
        source="yicai_manual",
        source_type="macro",
        source_name="第一财经",
        mode="manual_input",
        allowed_domains=("yicai.com",),
    ),
    "macro_manual": SourceProfile(
        source="macro_manual",
        source_type="macro",
        source_name="宏观/地缘人工整理",
        mode="manual_input",
    ),
}

FEED_SUMMARY_SOURCES = {"gov_cn", "ndrc", "csrc"}


@dataclass
class CollectedRecord:
    """原始采集记录。"""

    raw_id: str
    source: str
    source_type: str
    source_name: str
    source_url: str
    title: str
    content: str
    published_at: str
    collected_at: str
    collector_name: str
    collector_version: str
    batch: str


@dataclass
class CandidateRecord:
    """标准化候选事件。"""

    dedupe_key: str
    raw_id: str
    source: str
    source_type: str
    source_name: str
    source_url: str
    title: str
    content: str
    published_at: str
    collected_at: str
    collector_name: str
    review_status: str
    review_note: str
    suggested_status: str
    entity_hits: str
    entity_hit_count: int
    duplicate_suspect: bool
    batch: str


REVIEW_QUEUE_COLUMNS = [field.name for field in CandidateRecord.__dataclass_fields__.values()]


def collect_events(
    project_root: Path,
    source: str,
    since_value: str | date,
    until_value: str | date,
    input_path: str | Path | None = None,
    seed_urls: list[str] | None = None,
    limit: int = 30,
) -> Path:
    """抓取原始事件记录并写入 inbox。"""

    profile = _get_source_profile(source)
    since = parse_date(since_value)
    until = parse_date(until_value)
    batch = until.isoformat()
    collector = _build_collector(profile)
    records = collector.collect(
        since=since,
        until=until,
        input_path=Path(input_path) if input_path else None,
        seed_urls=seed_urls or [],
        limit=limit,
        batch=batch,
    )
    output_dir = _raw_batch_dir(project_root, source, batch)
    ensure_directory(output_dir)
    output_path = output_dir / RAW_FILENAME
    _write_jsonl(output_path, [asdict(record) for record in records])
    logger.info("原始采集完成：source=%s batch=%s rows=%s", source, batch, len(records))
    return output_path


def normalize_events(project_root: Path, source: str, batch: str) -> tuple[Path, Path]:
    """将原始采集结果标准化并生成审阅清单。"""

    profile = _get_source_profile(source)
    raw_path = _raw_batch_dir(project_root, source, batch) / RAW_FILENAME
    if not raw_path.exists():
        raise RuntimeError(f"未找到原始采集文件：{raw_path}")

    raw_records = _read_jsonl(raw_path)
    stock_names = _load_stock_names(project_root)
    existing_keys = _load_existing_event_keys(project_root)
    candidates: list[CandidateRecord] = []
    for item in raw_records:
        candidate = _normalize_raw_record(item, stock_names, existing_keys, batch)
        candidates.append(candidate)
        existing_keys.add(candidate.dedupe_key)

    staging_dir = project_root / "data" / "staging" / "events" / profile.source_type
    ensure_directory(staging_dir)
    staging_path = staging_dir / f"{batch}_{source}.jsonl"
    _write_jsonl(staging_path, [asdict(candidate) for candidate in candidates])

    review_queue_path = project_root / GLOBAL_REVIEW_QUEUE
    _upsert_review_queue(review_queue_path, candidates)
    logger.info("标准化完成：source=%s batch=%s rows=%s", source, batch, len(candidates))
    return staging_path, review_queue_path


def publish_events(project_root: Path, source_type: str, batch: str) -> list[Path]:
    """将已确认的候选事件发布到正式事件目录。"""

    review_queue_path = project_root / GLOBAL_REVIEW_QUEUE
    if not review_queue_path.exists():
        raise RuntimeError(f"未找到审阅清单：{review_queue_path}")

    try:
        review_df = pd.read_csv(review_queue_path)
    except pd.errors.EmptyDataError as exc:
        raise RuntimeError(
            "审阅清单当前没有任何候选事件。请先运行 collect 和 normalize，或检查本次抓取是否实际命中了来源内容。"
        ) from exc
    if review_df.empty:
        raise RuntimeError("审阅清单为空，无法发布事件。请先确认 normalize 是否产生了候选事件。")
    source_type_df = review_df[
        (review_df["source_type"] == source_type)
        & (review_df["batch"] == batch)
        & (review_df["review_status"] == "accepted")
    ].copy()
    if source_type_df.empty:
        raise RuntimeError(f"未找到可发布的 accepted 事件：source_type={source_type} batch={batch}")

    accepted_keys = set(source_type_df["dedupe_key"].astype(str).tolist())
    staging_dir = project_root / "data" / "staging" / "events" / source_type
    if not staging_dir.exists():
        raise RuntimeError(f"未找到 staging 目录：{staging_dir}")

    accepted_records: list[dict[str, Any]] = []
    for staging_path in sorted(staging_dir.glob(f"{batch}_*.jsonl")):
        for item in _read_jsonl(staging_path):
            dedupe_key = str(item.get("dedupe_key", ""))
            if dedupe_key in accepted_keys:
                accepted_records.append(_build_event_export_record(item))

    if not accepted_records:
        raise RuntimeError(f"accepted 记录为空：source_type={source_type} batch={batch}")

    by_month: dict[str, list[dict[str, Any]]] = {}
    for record in accepted_records:
        month_key = str(record["published_at"])[:7].replace("-", "")
        by_month.setdefault(month_key, []).append(record)

    output_paths: list[Path] = []
    for month_key, records in sorted(by_month.items()):
        target_path = project_root / "data" / "events" / source_type / f"{source_type}_{month_key}.json"
        ensure_directory(target_path.parent)
        existing = _load_event_array(target_path)
        merged = _merge_event_records(existing, records)
        target_path.write_text(
            json.dumps(merged, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        output_paths.append(target_path)

    logger.info("正式发布完成：source_type=%s batch=%s files=%s", source_type, batch, len(output_paths))
    return output_paths


def build_arg_parser() -> argparse.ArgumentParser:
    """构建命令行参数。"""

    parser = argparse.ArgumentParser(description="事件采集、标准化与发布工具")
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect_parser = subparsers.add_parser("collect", help="抓取原始事件")
    collect_parser.add_argument("--source", required=True, choices=sorted(SOURCE_PROFILES))
    collect_parser.add_argument("--since", required=True, help="开始日期 YYYY-MM-DD")
    collect_parser.add_argument("--until", required=True, help="结束日期 YYYY-MM-DD")
    collect_parser.add_argument("--input", default=None, help="手动导入文件路径（CSV/JSON/JSONL/TXT）")
    collect_parser.add_argument("--seed-url", action="append", default=[], help="覆盖默认列表页 URL，可传多次")
    collect_parser.add_argument("--limit", type=int, default=30, help="单次抓取最多处理多少条详情")

    normalize_parser = subparsers.add_parser("normalize", help="标准化候选事件并生成审阅队列")
    normalize_parser.add_argument("--source", required=True, choices=sorted(SOURCE_PROFILES))
    normalize_parser.add_argument("--batch", required=True, help="批次日期 YYYY-MM-DD")

    publish_parser = subparsers.add_parser("publish", help="发布已确认事件到正式目录")
    publish_parser.add_argument("--source-type", required=True, choices=["policy", "announcement", "industry", "macro"])
    publish_parser.add_argument("--batch", required=True, help="批次日期 YYYY-MM-DD")
    return parser


def main(argv: list[str] | None = None) -> int:
    """脚本入口。"""

    parser = build_arg_parser()
    args = parser.parse_args(argv)
    project_root = Path(__file__).resolve().parent.parent
    try:
        if args.command == "collect":
            output_path = collect_events(
                project_root=project_root,
                source=args.source,
                since_value=args.since,
                until_value=args.until,
                input_path=args.input,
                seed_urls=args.seed_url,
                limit=args.limit,
            )
            print(output_path)
            return 0
        if args.command == "normalize":
            staging_path, queue_path = normalize_events(
                project_root=project_root,
                source=args.source,
                batch=args.batch,
            )
            print(staging_path)
            print(queue_path)
            return 0
        if args.command == "publish":
            output_paths = publish_events(
                project_root=project_root,
                source_type=args.source_type,
                batch=args.batch,
            )
            for path in output_paths:
                print(path)
            return 0
        parser.error(f"未知命令：{args.command}")
        return 1
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1


class BaseCollector:
    """采集器基类。"""

    def __init__(self, profile: SourceProfile) -> None:
        self.profile = profile

    def collect(
        self,
        since: date,
        until: date,
        input_path: Path | None,
        seed_urls: list[str],
        limit: int,
        batch: str,
    ) -> list[CollectedRecord]:
        raise NotImplementedError


class AutoWebCollector(BaseCollector):
    """自动网页抓取采集器。"""

    def collect(
        self,
        since: date,
        until: date,
        input_path: Path | None,
        seed_urls: list[str],
        limit: int,
        batch: str,
    ) -> list[CollectedRecord]:
        if input_path is not None:
            rows = _load_input_rows(input_path)
            return self._collect_from_rows(rows, since, until, batch, limit)

        if self.profile.default_feed_urls:
            records = self._collect_from_feed_urls(since, until, batch, limit)
            if records:
                return records

        urls = seed_urls or list(self.profile.default_urls)
        if not urls:
            raise RuntimeError(f"来源 {self.profile.source} 没有默认 seed url，请通过 --seed-url 或 --input 提供。")

        records: list[CollectedRecord] = []
        seen_urls: set[str] = set()
        for list_url in urls:
            try:
                html = _fetch_url_text(list_url)
            except Exception as exc:
                logger.warning("抓取列表页失败：%s", list_url, exc_info=exc)
                continue
            links = _extract_candidate_links(html, list_url, self.profile.allowed_domains)
            for link in links:
                if link in seen_urls:
                    continue
                seen_urls.add(link)
                if len(records) >= limit:
                    break
                record = _build_record_from_url(
                    source=self.profile.source,
                    source_type=self.profile.source_type,
                    source_name=self.profile.source_name,
                    source_url=link,
                    batch=batch,
                )
                if record is None:
                    continue
                published_at = parse_datetime(record.published_at).date()
                if since <= published_at <= until:
                    records.append(record)
            if len(records) >= limit:
                break
        return records

    def _collect_from_feed_urls(
        self,
        since: date,
        until: date,
        batch: str,
        limit: int,
    ) -> list[CollectedRecord]:
        records: list[CollectedRecord] = []
        for feed_url in self.profile.default_feed_urls:
            try:
                payload = _fetch_json(feed_url)
            except Exception as exc:
                logger.warning("抓取 JSON feed 失败：%s", feed_url, exc_info=exc)
                continue
            for row in payload if isinstance(payload, list) else payload.get("records", []):
                if len(records) >= limit:
                    break
                record = _build_record_from_row(
                    row=row,
                    source=self.profile.source,
                    source_type=self.profile.source_type,
                    source_name=self.profile.source_name,
                    batch=batch,
                )
                if record is None:
                    continue
                published_at = parse_datetime(record.published_at).date()
                if since <= published_at <= until:
                    records.append(record)
            if len(records) >= limit:
                break
        return records

    def _collect_from_rows(
        self,
        rows: list[dict[str, Any]],
        since: date,
        until: date,
        batch: str,
        limit: int,
    ) -> list[CollectedRecord]:
        records: list[CollectedRecord] = []
        for row in rows:
            if len(records) >= limit:
                break
            record = _build_record_from_row(
                row=row,
                source=self.profile.source,
                source_type=self.profile.source_type,
                source_name=self.profile.source_name,
                batch=batch,
            )
            if record is None:
                continue
            published_at = parse_datetime(record.published_at).date()
            if since <= published_at <= until:
                records.append(record)
        return records


class ManualInputCollector(BaseCollector):
    """手动输入型采集器。"""

    def collect(
        self,
        since: date,
        until: date,
        input_path: Path | None,
        seed_urls: list[str],
        limit: int,
        batch: str,
    ) -> list[CollectedRecord]:
        rows: list[dict[str, Any]]
        if input_path is not None:
            rows = _load_input_rows(input_path)
        elif seed_urls:
            rows = [{"source_url": url} for url in seed_urls]
        else:
            raise RuntimeError(f"来源 {self.profile.source} 需要通过 --input 或 --seed-url 提供导入素材。")

        records: list[CollectedRecord] = []
        for row in rows:
            if len(records) >= limit:
                break
            record = _build_record_from_row(
                row=row,
                source=self.profile.source,
                source_type=self.profile.source_type,
                source_name=self.profile.source_name,
                batch=batch,
            )
            if record is None:
                continue
            published_at = parse_datetime(record.published_at).date()
            if since <= published_at <= until:
                records.append(record)
        return records


def _build_collector(profile: SourceProfile) -> BaseCollector:
    """根据来源配置创建采集器。"""

    if profile.mode == "auto_web":
        return AutoWebCollector(profile)
    return ManualInputCollector(profile)


def _get_source_profile(source: str) -> SourceProfile:
    """获取来源配置。"""

    try:
        return SOURCE_PROFILES[source]
    except KeyError as exc:
        raise RuntimeError(f"不支持的事件来源：{source}") from exc


def _raw_batch_dir(project_root: Path, source: str, batch: str) -> Path:
    """原始采集批次目录。"""

    return project_root / "data" / "inbox" / "events_raw" / source / batch


def _load_input_rows(input_path: Path) -> list[dict[str, Any]]:
    """加载手动导入的素材文件。"""

    if not input_path.exists():
        raise RuntimeError(f"输入文件不存在：{input_path}")
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(input_path).fillna("").to_dict(orient="records")
    if suffix == ".json":
        payload = json.loads(input_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            payload = payload.get("records", [])
        return list(payload)
    if suffix == ".jsonl":
        return _read_jsonl(input_path)
    if suffix == ".txt":
        rows = []
        for line in input_path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text or text.startswith("#"):
                continue
            rows.append({"source_url": text})
        return rows
    raise RuntimeError(f"不支持的输入文件类型：{input_path}")


def _build_record_from_row(
    row: dict[str, Any],
    source: str,
    source_type: str,
    source_name: str,
    batch: str,
) -> CollectedRecord | None:
    """从导入行构建原始记录。"""

    source_url = str(row.get("source_url") or row.get("url") or "").strip()
    source_url = source_url or str(row.get("URL") or row.get("链接") or "").strip()
    title = str(row.get("title") or row.get("标题") or row.get("TITLE") or "").strip()
    content = str(row.get("content") or row.get("正文") or row.get("SUB_TITLE") or "").strip()
    published_value = (
        row.get("published_at")
        or row.get("publish_time")
        or row.get("发布时间")
        or row.get("date")
        or row.get("日期")
        or row.get("DOCRELPUBTIME")
    )
    # 对官方政策 feed，首版优先用标题/副标题快速生成候选，避免逐条详情补抓导致 collect 长时间阻塞。
    if source in FEED_SUMMARY_SOURCES and title and published_value and not content:
        content = title
    if source_url and (not title or not content or not published_value):
        fetched = _build_record_from_url(
            source=source,
            source_type=source_type,
            source_name=str(row.get("source_name") or source_name),
            source_url=source_url,
            batch=batch,
        )
        if fetched is not None:
            title = title or fetched.title
            content = content or fetched.content
            published_value = published_value or fetched.published_at
    if not title or not content or not published_value:
        logger.warning("跳过原始素材：缺少标题/正文/发布时间。source=%s url=%s", source, source_url)
        return None

    try:
        published_at = parse_datetime(str(published_value))
    except Exception as exc:
        logger.warning("跳过原始素材：发布时间无法解析。source=%s value=%s", source, published_value, exc_info=exc)
        return None

    raw_id = _build_raw_id(source, source_url or title, published_at)
    return CollectedRecord(
        raw_id=raw_id,
        source=source,
        source_type=str(row.get("source_type") or source_type),
        source_name=str(row.get("source_name") or source_name),
        source_url=source_url,
        title=title,
        content=content,
        published_at=published_at.strftime("%Y-%m-%d %H:%M:%S"),
        collected_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        collector_name=COLLECTOR_NAME,
        collector_version=COLLECTOR_VERSION,
        batch=batch,
    )


def _build_record_from_url(
    source: str,
    source_type: str,
    source_name: str,
    source_url: str,
    batch: str,
) -> CollectedRecord | None:
    """从详情页 URL 抓取原始记录。"""

    try:
        html = _fetch_url_text(source_url)
    except Exception as exc:
        logger.warning("抓取详情页失败：%s", source_url, exc_info=exc)
        return None

    title = _extract_article_title(html)
    content = _extract_article_content(html)
    published_at = _extract_article_datetime(html)
    if not title or not content or published_at is None:
        logger.warning("详情页内容不完整，跳过：%s", source_url)
        return None

    raw_id = _build_raw_id(source, source_url, published_at)
    return CollectedRecord(
        raw_id=raw_id,
        source=source,
        source_type=source_type,
        source_name=source_name,
        source_url=source_url,
        title=title,
        content=content,
        published_at=published_at.strftime("%Y-%m-%d %H:%M:%S"),
        collected_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        collector_name=COLLECTOR_NAME,
        collector_version=COLLECTOR_VERSION,
        batch=batch,
    )


def _fetch_url_text(url: str) -> str:
    """抓取网页文本。"""

    request = Request(url, headers={"User-Agent": "Mozilla/5.0 TeddyCup event collector"})
    with urlopen(request, timeout=DETAIL_FETCH_TIMEOUT_SECONDS) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        payload = response.read()
    return payload.decode(charset, errors="ignore")


def _fetch_json(url: str) -> Any:
    """抓取 JSON 数据。"""

    text = _fetch_url_text(url)
    return json.loads(text)


def _extract_candidate_links(html: str, base_url: str, allowed_domains: tuple[str, ...]) -> list[str]:
    """从列表页提取候选文章链接。"""

    pattern = re.compile(r"<a[^>]+href=['\"](?P<href>[^'\"]+)['\"][^>]*>(?P<text>.*?)</a>", re.IGNORECASE | re.DOTALL)
    links: list[str] = []
    seen: set[str] = set()
    for match in pattern.finditer(html):
        href = unescape(match.group("href").strip())
        if href.startswith("javascript:") or href.startswith("#"):
            continue
        text = _clean_html_text(match.group("text"))
        if len(text) < 8:
            continue
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        if allowed_domains and not any(parsed.netloc.endswith(domain) for domain in allowed_domains):
            continue
        if full_url in seen:
            continue
        seen.add(full_url)
        links.append(full_url)
    return links


def _extract_article_title(html: str) -> str:
    """提取文章标题。"""

    for pattern in (
        r"<meta[^>]+property=['\"]og:title['\"][^>]+content=['\"](?P<value>[^'\"]+)['\"]",
        r"<meta[^>]+name=['\"]Title['\"][^>]+content=['\"](?P<value>[^'\"]+)['\"]",
        r"<title>(?P<value>.*?)</title>",
        r"<h1[^>]*>(?P<value>.*?)</h1>",
    ):
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            value = _clean_html_text(match.group("value"))
            if value:
                return value
    return ""


def _extract_article_content(html: str) -> str:
    """提取正文。"""

    paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", html, flags=re.IGNORECASE | re.DOTALL)
    blocks = [_clean_html_text(item) for item in paragraphs]
    blocks = [item for item in blocks if len(item) >= 20]
    if blocks:
        return "\n".join(blocks[:12])

    body_match = re.search(r"<body[^>]*>(?P<value>.*?)</body>", html, flags=re.IGNORECASE | re.DOTALL)
    if body_match:
        body_text = _clean_html_text(body_match.group("value"))
        return body_text[:2000]
    return ""


def _extract_article_datetime(html: str) -> datetime | None:
    """提取发布时间。"""

    for pattern in (
        r"(?P<value>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})",
        r"(?P<value>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})",
        r"(?P<value>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})",
        r"(?P<value>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2})",
        r"(?P<value>\d{4}-\d{2}-\d{2})",
        r"(?P<value>\d{4}/\d{2}/\d{2})",
    ):
        match = re.search(pattern, html)
        if not match:
            continue
        try:
            return parse_datetime(match.group("value"))
        except Exception:
            continue
    return None


def _clean_html_text(value: str) -> str:
    """清理 HTML 片段。"""

    text = re.sub(r"<script.*?>.*?</script>", " ", value, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style.*?>.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _build_raw_id(source: str, seed: str, published_at: datetime) -> str:
    """构建稳定 raw_id。"""

    digest = hashlib.md5(f"{source}|{seed}|{published_at.isoformat()}".encode("utf-8")).hexdigest()[:12]
    return f"{source}-{published_at.strftime('%Y%m%d')}-{digest}"


def _normalize_raw_record(
    item: dict[str, Any],
    stock_names: list[str],
    existing_keys: set[str],
    batch: str,
) -> CandidateRecord:
    """将原始记录转换为候选事件。"""

    title = str(item.get("title") or "").strip()
    content = str(item.get("content") or "").strip()
    published_at = parse_datetime(str(item.get("published_at")))
    source_url = str(item.get("source_url") or "").strip()
    source_type = str(item.get("source_type") or "")
    dedupe_key = _build_dedupe_key(source_type, title, published_at, source_url)
    entity_hits = _extract_entity_hits(f"{title} {content}", stock_names)
    duplicate_suspect = dedupe_key in existing_keys
    suggested_status = _suggest_review_status(
        title=title,
        content=content,
        duplicate_suspect=duplicate_suspect,
    )
    return CandidateRecord(
        dedupe_key=dedupe_key,
        raw_id=str(item.get("raw_id") or ""),
        source=str(item.get("source") or ""),
        source_type=source_type,
        source_name=str(item.get("source_name") or ""),
        source_url=source_url,
        title=title,
        content=content,
        published_at=published_at.strftime("%Y-%m-%d %H:%M:%S"),
        collected_at=str(item.get("collected_at") or ""),
        collector_name=str(item.get("collector_name") or COLLECTOR_NAME),
        review_status="pending",
        review_note="",
        suggested_status=suggested_status,
        entity_hits="、".join(entity_hits),
        entity_hit_count=len(entity_hits),
        duplicate_suspect=duplicate_suspect,
        batch=batch,
    )


def _build_dedupe_key(source_type: str, title: str, published_at: datetime, source_url: str) -> str:
    """生成去重键。"""

    signature = source_url or f"{title}|{published_at.date().isoformat()}"
    payload = f"{source_type}|{normalize_text(title)}|{normalize_text(signature)}"
    return hashlib.md5(payload.encode("utf-8")).hexdigest()[:16]


def _extract_entity_hits(text: str, stock_names: list[str]) -> list[str]:
    """提取命中的股票名称。"""

    normalized = normalize_text(text)
    hits = []
    for stock_name in stock_names:
        token = normalize_text(stock_name)
        if token and token in normalized:
            hits.append(stock_name)
    return sorted(set(hits))[:12]


def _suggest_review_status(title: str, content: str, duplicate_suspect: bool) -> str:
    """给出建议状态。"""

    if duplicate_suspect:
        return "pending"
    if len(title) < 8:
        return "rejected"
    if len(content) < 40:
        return "pending"
    return "accepted"


def _load_stock_names(project_root: Path) -> list[str]:
    """加载股票名称列表。"""

    candidates = [
        project_root / "data" / "manual" / "stock_universe.csv",
        project_root / "data" / "raw" / "stock_universe.csv",
    ]
    for path in candidates:
        if not path.exists():
            continue
        df = pd.read_csv(path)
        if "stock_name" not in df.columns:
            continue
        return df["stock_name"].dropna().astype(str).unique().tolist()
    return []


def _load_existing_event_keys(project_root: Path) -> set[str]:
    """加载已有正式事件的 dedupe_key 集合。"""

    keys: set[str] = set()
    events_root = project_root / "data" / "events"
    if not events_root.exists():
        return keys
    for path in events_root.rglob("*.json"):
        payload = _load_event_array(path)
        for item in payload:
            title = str(item.get("title") or "").strip()
            published_at_value = item.get("published_at")
            source_type = str(item.get("source_type") or path.parent.name)
            source_url = str(item.get("source_url") or "").strip()
            if not title or not published_at_value:
                continue
            try:
                published_at = parse_datetime(str(published_at_value))
            except Exception:
                continue
            keys.add(_build_dedupe_key(source_type, title, published_at, source_url))
    return keys


def _upsert_review_queue(review_queue_path: Path, candidates: list[CandidateRecord]) -> None:
    """更新全局审阅队列。"""

    ensure_directory(review_queue_path.parent)
    new_df = pd.DataFrame([asdict(candidate) for candidate in candidates], columns=REVIEW_QUEUE_COLUMNS)
    if review_queue_path.exists():
        try:
            existing_df = pd.read_csv(review_queue_path)
        except pd.errors.EmptyDataError:
            existing_df = pd.DataFrame(columns=REVIEW_QUEUE_COLUMNS)
    else:
        existing_df = pd.DataFrame(columns=REVIEW_QUEUE_COLUMNS)

    if existing_df.empty:
        merged_df = new_df.copy()
    else:
        preserved = existing_df.set_index("dedupe_key").to_dict(orient="index")
        rows = []
        for candidate in candidates:
            row = asdict(candidate)
            old_row = preserved.get(candidate.dedupe_key)
            if old_row is not None:
                row["review_status"] = str(old_row.get("review_status") or row["review_status"])
                row["review_note"] = str(old_row.get("review_note") or row["review_note"])
            rows.append(row)
        refresh_df = pd.DataFrame(rows)
        untouched_df = existing_df[~existing_df["dedupe_key"].isin(refresh_df["dedupe_key"])].copy()
        merged_df = pd.concat([untouched_df, refresh_df], ignore_index=True)

    sort_cols = [column for column in ["batch", "source_type", "published_at", "source_name"] if column in merged_df.columns]
    if sort_cols:
        merged_df = merged_df.sort_values(sort_cols).reset_index(drop=True)
    merged_df.to_csv(review_queue_path, index=False, encoding="utf-8-sig")


def _build_event_export_record(item: dict[str, Any]) -> dict[str, Any]:
    """构造正式事件文件记录。"""

    return {
        "raw_id": item["raw_id"],
        "title": item["title"],
        "content": item["content"],
        "published_at": item["published_at"],
        "source_name": item["source_name"],
        "source_url": item["source_url"],
        "source_type": item["source_type"],
        "collector_name": item.get("collector_name", COLLECTOR_NAME),
        "collected_at": item.get("collected_at", ""),
        "review_status": item.get("review_status", "accepted"),
        "dedupe_key": item.get("dedupe_key", ""),
    }


def _load_event_array(path: Path) -> list[dict[str, Any]]:
    """加载正式事件 JSON 数组。"""

    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return list(payload.get("records", []))
    return list(payload)


def _merge_event_records(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """合并正式事件记录并按时间排序。"""

    merged: dict[str, dict[str, Any]] = {}
    for item in existing + incoming:
        key = str(item.get("dedupe_key") or "")
        if not key:
            try:
                key = _build_dedupe_key(
                    str(item.get("source_type") or ""),
                    str(item.get("title") or ""),
                    parse_datetime(str(item.get("published_at"))),
                    str(item.get("source_url") or ""),
                )
            except Exception:
                continue
        merged[key] = item
    result = list(merged.values())
    result.sort(key=lambda item: str(item.get("published_at") or ""))
    return result


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    """写入 JSONL 文件。"""

    ensure_directory(path.parent)
    with path.open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False))
            file.write("\n")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    """读取 JSONL 文件。"""

    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        rows.append(json.loads(text))
    return rows
