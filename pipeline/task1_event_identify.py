from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

import pandas as pd

from pipeline.utils import build_event_id, normalize_text, source_weight, text_similarity


EVENT_TAXONOMY = {
    "subject_type": {
        "政策类事件": ["政策", "方案", "通知", "会议", "国务院", "发改委", "监管", "试点"],
        "公司类事件": ["公告", "订单", "业绩", "预增", "合同", "回购", "中标", "增持"],
        "行业类事件": ["行业", "景气", "产业链", "需求", "涨价", "扩产"],
        "宏观类事件": ["宏观", "经济", "财政", "货币", "基建"],
        "地缘类事件": ["空战", "冲突", "军贸", "地缘", "国防", "实战"]
    },
    "duration_type": {
        "脉冲型事件": ["突发", "空战", "冲突", "发布", "订单", "中标", "快报", "预告"],
        "中期型事件": ["扩产", "设备更新", "景气", "订单饱满"],
        "长尾型事件": ["政策", "规划", "基础设施", "国产替代", "长期", "试点"]
    },
    "predictability_type": {
        "突发型事件": ["突发", "空战", "冲突", "发布", "紧急"],
        "预披露型事件": ["公告", "年报", "一季报", "业绩预增", "业绩快报", "会议", "政策"]
    },
    "industry_type": {
        "军工": ["军工", "导弹", "战机", "无人机", "火箭", "国防", "空战"],
        "科技": ["算力", "AI", "人工智能", "芯片", "半导体", "服务器", "光模块"],
        "新能源": ["新能源", "光伏", "储能", "电池", "逆变器"],
        "低空经济": ["低空经济", "低空", "飞行器", "空域", "通航"],
        "业绩预告": ["业绩预增", "业绩快报", "一季报", "年报", "净利润", "超预期"]
    }
}

POSITIVE_WORDS = ["加快", "超预期", "提升", "回升", "增长", "预增", "催化", "景气", "受益", "饱满", "突破"]
NEGATIVE_WORDS = ["下滑", "亏损", "风险", "终止", "减值", "下跌", "波动加大", "承压"]
INTENSITY_WORDS = ["重大", "核心", "超预期", "显著", "快速", "重点", "加快", "强烈", "高景气"]


def run_event_identification(news_df: pd.DataFrame) -> pd.DataFrame:
    """从新闻中聚合出候选事件。"""

    grouped_indices: list[list[int]] = []
    consumed: set[int] = set()

    for idx, row in news_df.iterrows():
        if idx in consumed:
            continue
        anchor_time: datetime = pd.Timestamp(row["publish_time"]).to_pydatetime()
        anchor_text = f"{row['title']} {row['content']}"
        cluster = [idx]
        consumed.add(idx)
        for other_idx, other in news_df.iloc[idx + 1 :].iterrows():
            if other_idx in consumed:
                continue
            other_time: datetime = pd.Timestamp(other["publish_time"]).to_pydatetime()
            if abs((other_time - anchor_time).total_seconds()) > 36 * 3600:
                continue
            similarity = text_similarity(anchor_text, f"{other['title']} {other['content']}")
            shared_keywords = len(set(extract_all_keywords(anchor_text)) & set(extract_all_keywords(f"{other['title']} {other['content']}")))
            if similarity >= 0.18 or shared_keywords >= 2:
                cluster.append(other_idx)
                consumed.add(other_idx)
        grouped_indices.append(cluster)

    events: list[dict[str, Any]] = []
    for cluster_indices in grouped_indices:
        cluster_df = news_df.loc[cluster_indices].copy().sort_values("publish_time")
        evidence_text = "；".join(cluster_df["title"].tolist()[:3])
        aggregated_text = " ".join((cluster_df["title"] + " " + cluster_df["content"]).tolist())
        publish_time = pd.Timestamp(cluster_df["publish_time"].min()).to_pydatetime()
        category_map = classify_event(aggregated_text)
        sentiment_score = compute_sentiment_score(aggregated_text)
        heat_score = compute_heat_score(cluster_df)
        intensity_score = compute_intensity_score(aggregated_text, cluster_df)
        scope_score = compute_scope_score(aggregated_text, category_map)
        confidence_score = round(min(1.0, 0.25 + heat_score * 0.25 + intensity_score * 0.25 + scope_score * 0.25), 4)

        title = choose_event_name(cluster_df)
        events.append(
            {
                "event_id": build_event_id(title, publish_time),
                "event_name": title,
                "source": ",".join(cluster_df["source"].unique().tolist()),
                "publish_time": publish_time.strftime("%Y-%m-%d %H:%M:%S"),
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
            }
        )

    event_df = pd.DataFrame(events).sort_values(
        ["heat_score", "intensity_score", "publish_time"], ascending=[False, False, False]
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
            score = sum(1 for keyword in keywords if normalize_text(keyword) in normalized)
            if score > best_score:
                best_score = score
                best_label = label
        result[dimension] = best_label
    return result


def compute_sentiment_score(text: str) -> float:
    """情绪强度评分，范围约为 -1 到 1。"""

    normalized = normalize_text(text)
    positive_hits = sum(1 for word in POSITIVE_WORDS if normalize_text(word) in normalized)
    negative_hits = sum(1 for word in NEGATIVE_WORDS if normalize_text(word) in normalized)
    total = positive_hits + negative_hits
    if total == 0:
        return 0.1
    return round((positive_hits - negative_hits) / total, 4)


def compute_heat_score(cluster_df: pd.DataFrame) -> float:
    """热度得分。"""

    source_score = cluster_df["source"].map(source_weight).mean()
    cluster_size = len(cluster_df)
    freshness_days = max(0.0, (cluster_df["publish_time"].max() - cluster_df["publish_time"].min()).total_seconds() / 86400)
    value = min(1.0, 0.18 * cluster_size + 0.55 * source_score + max(0.0, 0.2 - 0.03 * freshness_days))
    return round(value, 4)


def compute_intensity_score(text: str, cluster_df: pd.DataFrame) -> float:
    """事件强度得分。"""

    normalized = normalize_text(text)
    keyword_hits = sum(1 for word in INTENSITY_WORDS if normalize_text(word) in normalized)
    official_bonus = 0.15 if any(source in {"manual_policy", "manual_announcement"} for source in cluster_df["source"]) else 0.0
    amount_bonus = 0.1 if any(token in text for token in ["订单", "利润", "净利润", "预增"]) else 0.0
    value = min(1.0, 0.25 + keyword_hits * 0.12 + official_bonus + amount_bonus)
    return round(value, 4)


def compute_scope_score(text: str, category_map: dict[str, str]) -> float:
    """影响范围得分。"""

    normalized = normalize_text(text)
    stock_mentions = len({token for token in ["中航沈飞", "晨曦航空", "国科军工", "通宇通讯", "中际旭创", "中科曙光", "阳光电源"] if token in text})
    category_bonus = 0.18 if category_map["subject_type"] in {"政策类事件", "地缘类事件"} else 0.06
    breadth = len(set(extract_all_keywords(normalized)))
    value = min(1.0, 0.18 + stock_mentions * 0.08 + breadth * 0.05 + category_bonus)
    return round(value, 4)


def choose_event_name(cluster_df: pd.DataFrame) -> str:
    """从聚类内选择最具代表性的标题作为事件名。"""

    title_scores: Counter[str] = Counter()
    for _, row in cluster_df.iterrows():
        score = 1.0 + source_weight(row["source"])
        title_scores[row["title"]] += score
    return title_scores.most_common(1)[0][0]
