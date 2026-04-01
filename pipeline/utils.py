from __future__ import annotations

import json
import math
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd


SOURCE_WEIGHTS = {
    "manual_policy": 1.0,
    "manual_announcement": 0.95,
    "manual_research": 0.85,
    "manual_news": 0.8,
    "manual_market": 0.7,
    "qstock": 0.8,
    "fallback": 0.6,
}


def ensure_directory(path: Path) -> Path:
    """确保目录存在。"""

    path.mkdir(parents=True, exist_ok=True)
    return path


def parse_date(value: str | date | datetime) -> date:
    """将字符串或 datetime 统一转为 date。"""

    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.fromisoformat(str(value)).date()


def parse_datetime(value: str | datetime) -> datetime:
    """解析时间字符串。"""

    if isinstance(value, datetime):
        return value
    text = str(value).replace("/", "-")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y%m%d", "%Y%m%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return datetime.fromisoformat(text)


def daterange(start: date, end: date) -> list[date]:
    """生成闭区间日期序列。"""

    cursor = start
    values: list[date] = []
    while cursor <= end:
        values.append(cursor)
        cursor += timedelta(days=1)
    return values


def next_weekday(target: date, weekday: int) -> date:
    """找到指定日期之后最近的某个星期。weekday: 周一=0。"""

    days_ahead = weekday - target.weekday()
    if days_ahead < 0:
        days_ahead += 7
    return target + timedelta(days=days_ahead)


def previous_weekday(target: date, weekday: int) -> date:
    """找到指定日期之前最近的某个星期。"""

    days_back = target.weekday() - weekday
    if days_back < 0:
        days_back += 7
    return target - timedelta(days=days_back)


def normalize_text(text: str) -> str:
    """清洗文本中的噪声字符。"""

    value = str(text or "").strip().lower()
    value = re.sub(r"\s+", "", value)
    value = re.sub(r"[^\w\u4e00-\u9fff]", "", value)
    return value


def extract_keywords(text: str, keywords: Sequence[str]) -> list[str]:
    """抽取命中的关键词。"""

    return [keyword for keyword in keywords if keyword and keyword in text]


def text_similarity(left: str, right: str) -> float:
    """基于 token 重叠的轻量相似度。"""

    left_tokens = set(re.findall(r"[\u4e00-\u9fffA-Za-z0-9]+", left))
    right_tokens = set(re.findall(r"[\u4e00-\u9fffA-Za-z0-9]+", right))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def min_max_scale(value: float, lower: float, upper: float) -> float:
    """将数值缩放到 0 到 1。"""

    if upper <= lower:
        return 0.0
    return max(0.0, min(1.0, (value - lower) / (upper - lower)))


def logistic(value: float) -> float:
    """逻辑函数。"""

    return 1.0 / (1.0 + math.exp(-value))


def source_weight(source: str) -> float:
    """新闻来源权重。"""

    return SOURCE_WEIGHTS.get(source, 0.7)


def save_dataframe(df: pd.DataFrame, base_path: Path) -> None:
    """只保存 CSV，不保存 Parquet。"""

    ensure_directory(base_path.parent)
    csv_path = base_path.with_suffix(".csv")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def load_json(path: Path) -> dict | list:
    """读取 JSON。"""

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def dump_json(payload: dict | list, path: Path) -> None:
    """写入 JSON。"""

    ensure_directory(path.parent)
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def weighted_average(values: Iterable[tuple[float, float]]) -> float:
    """计算加权平均值。"""

    total_weight = 0.0
    total_score = 0.0
    for score, weight in values:
        total_score += score * weight
        total_weight += weight
    if total_weight == 0:
        return 0.0
    return total_score / total_weight


def build_event_id(title: str, publish_time: datetime) -> str:
    """生成稳定的事件 ID。"""

    prefix = normalize_text(title)[:24] or "event"
    return f"{publish_time.strftime('%Y%m%d')}_{prefix}"
