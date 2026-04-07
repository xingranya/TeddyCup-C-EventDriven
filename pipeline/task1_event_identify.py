from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

import pandas as pd

from pipeline.models import DEFAULT_EVENT_TAXONOMY
from pipeline.utils import build_event_id, logistic, normalize_text, source_weight, text_similarity


POSITIVE_WORDS = ["加快", "超预期", "提升", "回升",
                  "增长", "预增", "催化", "景气", "受益", "饱满", "突破"]
NEGATIVE_WORDS = ["下滑", "亏损", "风险", "终止", "减值", "下跌", "波动加大", "承压"]
INTENSITY_WORDS = ["重大", "核心", "超预期", "显著", "快速", "重点", "加快", "强烈", "高景气"]

# 全局变量，将在运行时被配置覆盖
EVENT_TAXONOMY: dict[str, dict[str, list[str]]] = DEFAULT_EVENT_TAXONOMY.copy()


def set_event_taxonomy(taxonomy: dict[str, dict[str, list[str]]] | None) -> None:
    """设置全局事件分类体系，用于从配置注入。"""
    global EVENT_TAXONOMY
    if taxonomy:
        EVENT_TAXONOMY = taxonomy
    else:
        EVENT_TAXONOMY = DEFAULT_EVENT_TAXONOMY


def run_event_identification(news_df: pd.DataFrame, event_taxonomy: dict[str, dict[str, list[str]]] | None = None) -> pd.DataFrame:
    """从新闻中聚合出候选事件。

    Args:
        news_df: 新闻数据框
        event_taxonomy: 事件分类体系配置，为 None 时使用默认值
    """
    # 设置全局分类体系
    set_event_taxonomy(event_taxonomy)

    grouped_indices: list[list[int]] = []
    consumed: set[int] = set()

    for idx, row in news_df.iterrows():
        if idx in consumed:
            continue
        anchor_time: datetime = pd.Timestamp(
            row["published_at"]).to_pydatetime()
        anchor_text = f"{row['title']} {row['content']}"
        anchor_title = str(row['title'])
        anchor_entities = set(
            filter(None, str(row.get("entity_candidates") or "").split("、")))
        cluster = [idx]
        consumed.add(idx)
        for other_idx, other in news_df.iloc[idx + 1:].iterrows():
            if other_idx in consumed:
                continue
            other_time: datetime = pd.Timestamp(
                other["published_at"]).to_pydatetime()
            if abs((other_time - anchor_time).total_seconds()) > 36 * 3600:
                continue
            other_text = f"{other['title']} {other['content']}"
            other_title = str(other['title'])
            similarity = text_similarity(anchor_text, other_text)
            # 标题相似度判断（捕获标题高度相似但正文差异较大的重复报道）
            title_similarity = text_similarity(anchor_title, other_title)
            shared_keywords = len(set(extract_all_keywords(anchor_text)) & set(
                extract_all_keywords(other_text)))
            other_entities = set(
                filter(None, str(other.get("entity_candidates") or "").split("、")))
            shared_entities = len(anchor_entities & other_entities)
            if similarity >= 0.18 or title_similarity >= 0.35 or shared_keywords >= 2 or shared_entities >= 1:
                cluster.append(other_idx)
                consumed.add(other_idx)
        grouped_indices.append(cluster)

    events: list[dict[str, Any]] = []
    for cluster_indices in grouped_indices:
        cluster_df = news_df.loc[cluster_indices].copy(
        ).sort_values("published_at")
        evidence_text = "；".join(cluster_df["title"].tolist()[:3])
        aggregated_text = " ".join(
            (cluster_df["title"] + " " + cluster_df["content"]).tolist())
        published_at = pd.Timestamp(
            cluster_df["published_at"].min()).to_pydatetime()
        category_map = classify_event(aggregated_text)
        sentiment_score = compute_sentiment_score(aggregated_text)
        heat_score = compute_heat_score(cluster_df)
        intensity_score = compute_intensity_score(aggregated_text, cluster_df)
        scope_score = compute_scope_score(aggregated_text, category_map)
        sentiment_abs = abs(sentiment_score)
        # 使用 logistic 变换提升区分度
        raw = 0.3 * heat_score + 0.35 * intensity_score + \
            0.2 * scope_score + 0.15 * sentiment_abs
        confidence_score = round(logistic(6 * (raw - 0.5)), 4)

        title = choose_event_name(cluster_df)
        events.append(
            {
                "event_id": build_event_id(title, published_at),
                "event_name": title,
                "source": ",".join(cluster_df["source"].unique().tolist()),
                "published_at": published_at.strftime("%Y-%m-%d %H:%M:%S"),
                "subject_type": category_map["subject_type"],
                "duration_type": category_map["duration_type"],
                "predictability_type": category_map["predictability_type"],
                "industry_type": category_map["industry_type"],
                "sentiment_score": sentiment_score,
                "heat_score": heat_score,
                "intensity_score": intensity_score,
                "scope_score": scope_score,
                "confidence_score": confidence_score,
                "raw_evidence": evidence_text,
                "cluster_size": int(len(cluster_df)),
                "cluster_member_ids": ",".join(cluster_df["raw_id"].astype(str).tolist()),
                "source_names": " | ".join(cluster_df["source_name"].astype(str).unique().tolist()),
            }
        )

    event_df = pd.DataFrame(events).sort_values(
        ["heat_score", "intensity_score", "published_at"], ascending=[False, False, False]
    ).reset_index(drop=True)
    return event_df


def extract_all_keywords(text: str) -> list[str]:
    """抽取所有分类维度中命中的关键词。"""

    normalized = normalize_text(text)
    keywords: list[str] = []
    for dimension in EVENT_TAXONOMY.values():
        for token_list in dimension.values():
            for token in token_list:
                if normalize_text(token) in normalized:
                    keywords.append(token)
    return keywords


def classify_event(text: str) -> dict[str, str]:
    """事件分类，按关键词命中优先，未命中时给出稳妥兜底。"""

    normalized = normalize_text(text)
    result: dict[str, str] = {}
    for dimension, categories in EVENT_TAXONOMY.items():
        best_label = next(iter(categories.keys()))
        best_score = -1
        for label, keywords in categories.items():
            score = sum(1 for keyword in keywords if normalize_text(
                keyword) in normalized)
            if score > best_score:
                best_score = score
                best_label = label
        # 将 predictability 映射为 predictability_type 以保持一致性
        result_key = f"{dimension}_type" if dimension == "predictability" else dimension
        result[result_key] = best_label
    return result


def compute_sentiment_score(text: str) -> float:
    """情绪强度评分，范围约为 -1 到 1。"""

    normalized = normalize_text(text)
    positive_hits = sum(
        1 for word in POSITIVE_WORDS if normalize_text(word) in normalized)
    negative_hits = sum(
        1 for word in NEGATIVE_WORDS if normalize_text(word) in normalized)
    total = positive_hits + negative_hits
    if total == 0:
        return 0.1
    return round((positive_hits - negative_hits) / total, 4)


def compute_heat_score(cluster_df: pd.DataFrame) -> float:
    """热度得分。"""

    source_score = cluster_df["source"].map(source_weight).mean()
    cluster_size = len(cluster_df)
    freshness_days = max(0.0, (cluster_df["published_at"].max(
    ) - cluster_df["published_at"].min()).total_seconds() / 86400)
    value = min(1.0, 0.18 * cluster_size + 0.55 * source_score +
                max(0.0, 0.2 - 0.03 * freshness_days))
    return round(value, 4)


def compute_intensity_score(text: str, cluster_df: pd.DataFrame) -> float:
    """事件强度得分。"""

    normalized = normalize_text(text)
    keyword_hits = sum(
        1 for word in INTENSITY_WORDS if normalize_text(word) in normalized)
    official_bonus = 0.15 if any(
        source in {"policy", "announcement"} for source in cluster_df["source"]) else 0.0
    amount_bonus = 0.1 if any(token in text for token in [
                              "订单", "利润", "净利润", "预增"]) else 0.0
    value = min(1.0, 0.25 + keyword_hits * 0.12 +
                official_bonus + amount_bonus)
    return round(value, 4)


def compute_scope_score(text: str, category_map: dict[str, str]) -> float:
    """影响范围得分。"""

    normalized = normalize_text(text)
    stock_mentions = len({token for token in [
                         "中航沈飞", "晨曦航空", "国科军工", "通宇通讯", "中际旭创", "中科曙光", "阳光电源"] if token in text})
    category_bonus = 0.18 if category_map["subject_type"] in {
        "政策类事件", "地缘类事件"} else 0.06
    breadth = len(set(extract_all_keywords(normalized)))
    value = min(1.0, 0.18 + stock_mentions * 0.08 +
                breadth * 0.05 + category_bonus)
    return round(value, 4)


def choose_event_name(cluster_df: pd.DataFrame) -> str:
    """从聚类内选择最具代表性的标题作为事件名。

    优先选择长度在 8-60 字符之间且来源权重最高的标题。
    """
    title_scores: Counter[str] = Counter()
    for _, row in cluster_df.iterrows():
        score = 1.0 + source_weight(row["source"])
        title_scores[row["title"]] += score

    # 过滤掉长度<8或>60字符的标题
    filtered_titles = [(title, score) for title,
                       score in title_scores.items() if 8 <= len(title) <= 60]

    if filtered_titles:
        # 在剩余标题中选取来源权重最高的
        return max(filtered_titles, key=lambda x: x[1])[0]
    else:
        # 如果过滤后为空，回退到原逻辑
        return title_scores.most_common(1)[0][0]
