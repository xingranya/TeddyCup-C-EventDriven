from __future__ import annotations

from pathlib import Path
import re
from typing import Any

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd

from pipeline.models import AppConfig
from pipeline.utils import load_json, normalize_text, save_dataframe, configure_matplotlib_chinese

# 配置 matplotlib 支持中文显示
configure_matplotlib_chinese()

# 行业标签映射：将事件识别后的 industry_type 映射到 relation_map 的键
INDUSTRY_LABEL_MAP = {
    "军工类事件": "军工",
    "科技类事件": "科技",
    "新能源类事件": "新能源",
    "低空类事件": "低空经济",
    "消费类事件": "消费",
    "医药类事件": "医药",
    "金融类事件": "金融",
    "农业类事件": "农业",
    "地产类事件": "地产",
    "业绩类事件": "业绩",
}


# 关联权重配置：根据事件驱动主体类型动态调整
WEIGHT_PROFILES = {
    "政策类事件": {"direct": 0.30, "business": 0.25, "industry": 0.35, "co_move": 0.10},
    "公司类事件": {"direct": 0.55, "business": 0.20, "industry": 0.15, "co_move": 0.10},
    "行业类事件": {"direct": 0.35, "business": 0.30, "industry": 0.25, "co_move": 0.10},
    "宏观类事件": {"direct": 0.25, "business": 0.20, "industry": 0.35, "co_move": 0.20},
    "地缘类事件": {"direct": 0.30, "business": 0.20, "industry": 0.35, "co_move": 0.15},
}
DEFAULT_WEIGHTS = {"direct": 0.45, "business": 0.25,
                   "industry": 0.20, "co_move": 0.10}


def run_relation_mining(
    event_df: pd.DataFrame,
    stock_df: pd.DataFrame,
    price_df: pd.DataFrame,
    project_root: Path,
    output_dir: Path,
    config: AppConfig,
) -> tuple[pd.DataFrame, list[Path]]:
    """基于事件与股票池构建关联关系和图谱。"""

    relation_map = load_json(
        project_root / "data/manual/industry_relation_map.json")
    price_returns = compute_returns(price_df)
    relations: list[dict[str, Any]] = []

    for _, event in event_df.iterrows():
        event_text = f"{event['event_name']} {event['raw_evidence']}"
        normalized_event_text = normalize_text(event_text)
        raw_industry = event.get("industry_type", "")
        theme_key = INDUSTRY_LABEL_MAP.get(raw_industry, raw_industry)
        industry_payload = relation_map.get(theme_key, {})
        priority_stocks = {
            item["stock_code"]: item
            for item in industry_payload.get("stocks", [])
        }

        for _, stock in stock_df.iterrows():
            direct_mention = 1.0 if normalize_text(
                stock["stock_name"]) in normalized_event_text else 0.0
            business_match = compute_business_match(event_text, stock)
            industry_overlap = compute_industry_overlap(
                event["industry_type"], stock)
            historical_co_move = compute_historical_co_move(
                stock["stock_code"], event["industry_type"], price_returns, stock_df
            )
            if stock["stock_code"] in priority_stocks:
                direct_mention = max(direct_mention, 0.85)
                business_match = max(business_match, 0.75)
                industry_overlap = max(industry_overlap, 0.8)

            # 根据事件的 subject_type 获取动态权重
            subject_type = event.get("subject_type", "")
            weights = WEIGHT_PROFILES.get(subject_type, DEFAULT_WEIGHTS)

            association_score = round(
                weights["direct"] * direct_mention
                + weights["business"] * business_match
                + weights["industry"] * industry_overlap
                + weights["co_move"] * historical_co_move,
                4,
            )
            if association_score < 0.2:
                continue

            relation_type = priority_stocks.get(
                stock["stock_code"], {}).get("relation_type", "文本与业务关联")
            relation_path = priority_stocks.get(stock["stock_code"], {}).get(
                "relation_path",
                f"{event['event_name']} -> {stock['industry']}需求变化 -> {stock['stock_name']}受影响",
            )

            relations.append(
                {
                    "event_id": event["event_id"],
                    "event_name": event["event_name"],
                    "stock_code": stock["stock_code"],
                    "stock_name": stock["stock_name"],
                    "relation_type": relation_type,
                    "evidence_text": build_evidence_text(direct_mention, business_match, industry_overlap, event_text, stock),
                    "association_score": association_score,
                    "relation_path": relation_path,
                    "direct_mention": round(direct_mention, 4),
                    "business_match": round(business_match, 4),
                    "industry_overlap": round(industry_overlap, 4),
                    "historical_co_move": round(historical_co_move, 4),
                }
            )

    relation_df = pd.DataFrame(relations).sort_values(
        ["event_id", "association_score"], ascending=[True, False]
    ).reset_index(drop=True)
    save_dataframe(relation_df, output_dir / "company_relations")

    graph_paths = render_relation_graphs(relation_df, output_dir)
    return relation_df, graph_paths


def compute_returns(price_df: pd.DataFrame) -> pd.DataFrame:
    """计算收益率序列。"""

    ordered = price_df.sort_values(["stock_code", "trade_date"]).copy()
    ordered["return"] = ordered.groupby(
        "stock_code")["close"].pct_change().fillna(0.0)
    return ordered


def compute_business_match(event_text: str, stock: pd.Series) -> float:
    """主营业务与事件关键词匹配（渐进式匹配）。

    完全匹配（关键词完整出现在文本中）：每个命中得 0.30 分
    部分匹配（关键词的子串出现、或文本包含关键词的变体）：每个命中得 0.15 分
    总分上限 1.0
    """

    normalized_event_text = normalize_text(event_text)
    tags = [
        token
        for token in re.split(r"[,，、/；;\s|]+", f"{stock['concept_tags']},{stock['main_business']},{stock['industry']}")
        if token
    ]

    full_match_score = 0.0
    partial_match_score = 0.0

    for tag in tags:
        normalized_tag = normalize_text(tag)
        if not normalized_tag:
            continue

        # 完全匹配
        if normalized_tag in normalized_event_text:
            full_match_score += 0.30
        # 部分匹配：标签长度>=3时，检查标签前2/3是否出现在文本中
        elif len(normalized_tag) >= 3:
            partial_tag = normalized_tag[:int(len(normalized_tag) * 2 / 3)]
            if partial_tag in normalized_event_text:
                partial_match_score += 0.15

    return min(1.0, full_match_score + partial_match_score)


def compute_industry_overlap(industry_type: str, stock: pd.Series) -> float:
    """事件行业与股票行业重合度（增强版）。

    保留硬编码匹配作为 base_score，新增行业大类映射匹配给予额外加分。
    """

    # 行业大类到申万行业的映射字典
    INDUSTRY_GROUP_MAP = {
        "科技": ["电子", "计算机", "通信", "传媒", "半导体", "软件", "信息技术"],
        "军工": ["国防军工", "航空", "航天", "兵器", "船舶"],
        "新能源": ["电力设备", "新能源", "光伏", "风电", "锂电"],
        "消费": ["食品饮料", "家用电器", "商贸零售", "纺织服装", "轻工制造"],
        "医药": ["医药生物", "生物制品", "化学制药", "医疗器械", "中药"],
        "金融": ["银行", "非银金融", "保险", "券商"],
        "低空": ["国防军工", "航空", "机械设备", "电子"],
        "地产": ["房地产", "建筑装饰", "建筑材料"],
        "农业": ["农林牧渔", "种植业", "养殖业"],
    }

    stock_text = f"{stock['industry']} {stock['concept_tags']}"
    base_score = 0.15

    # 硬编码匹配规则作为 base_score
    if industry_type in stock_text:
        base_score = 1.0
    elif industry_type == "科技" and any(token in stock_text for token in ["AI算力", "半导体", "芯片"]):
        base_score = 0.85
    elif industry_type == "军工" and any(token in stock_text for token in ["军工", "导弹", "航空"]):
        base_score = 0.9

    # 新增：从事件的 industry_type 中提取行业大类关键词，匹配细分行业列表
    additional_score = 0.0
    stock_industry = stock.get("industry", "")

    for group_name, sub_industries in INDUSTRY_GROUP_MAP.items():
        # 检查事件行业类型是否包含该行业大类关键词
        if group_name in industry_type:
            # 如果股票的 industry 字段匹配到细分行业列表中的任一项
            for sub_industry in sub_industries:
                if sub_industry in stock_industry:
                    additional_score = 0.3
                    break
            if additional_score > 0:
                break

    return min(1.0, base_score + additional_score)


def compute_historical_co_move(
    stock_code: str,
    industry_type: str,
    price_returns: pd.DataFrame,
    stock_df: pd.DataFrame,
) -> float:
    """计算股票与同行业股票的历史价格相关性。

    从 price_returns 中提取该股票的日收益率序列，计算与同行业股票平均收益率的皮尔逊相关系数。
    如果数据不足（<20个共同交易日），返回默认值0.5。
    结果映射到 [0.3, 1.0] 区间。
    """
    if price_returns.empty or stock_df.empty:
        return 0.5

    # 提取该股票的收益率序列
    stock_returns = price_returns[price_returns["stock_code"] == stock_code][[
        "trade_date", "return"]].copy()
    if stock_returns.empty or len(stock_returns) < 20:
        return 0.5

    # 获取同行业股票列表
    industry_stocks = stock_df[
        (stock_df["industry"].str.contains(industry_type, na=False))
        | (stock_df["concept_tags"].str.contains(industry_type, na=False))
    ]["stock_code"].tolist()

    # 如果没有找到同行业股票，或者只有当前股票自己，返回默认值
    peer_stocks = [code for code in industry_stocks if code != stock_code]
    if not peer_stocks:
        return 0.5

    # 计算同行业股票的平均收益率
    peer_returns = price_returns[price_returns["stock_code"].isin(
        peer_stocks)][["trade_date", "return", "stock_code"]]
    if peer_returns.empty:
        return 0.5

    # 按日期计算同行业平均收益率
    peer_avg_returns = peer_returns.groupby(
        "trade_date")["return"].mean().reset_index()
    peer_avg_returns.columns = ["trade_date", "peer_avg_return"]

    # 合并该股票收益率与同行业平均收益率
    merged = stock_returns.merge(
        peer_avg_returns, on="trade_date", how="inner")

    # 检查共同交易日数量
    if len(merged) < 20:
        return 0.5

    # 计算皮尔逊相关系数
    try:
        correlation = merged["return"].corr(merged["peer_avg_return"])
        if pd.isna(correlation):
            return 0.5
    except Exception:
        return 0.5

    # 将相关性映射到 [0.3, 1.0] 区间
    return min(1.0, 0.3 + max(0.0, correlation) * 0.7)


def build_evidence_text(
    direct_mention: float,
    business_match: float,
    industry_overlap: float,
    event_text: str,
    stock: pd.Series,
) -> str:
    """输出关系证据摘要。"""

    parts = []
    if direct_mention >= 0.8:
        parts.append("新闻或证据文本直接提及该公司")
    if business_match >= 0.35:
        parts.append("主营业务与事件关键词存在明显匹配")
    if industry_overlap >= 0.8:
        parts.append("行业属性与事件主线高度重合")
    if not parts:
        parts.append("通过行业与概念映射获得弱关联")
    return "；".join(parts)


def render_relation_graphs(relation_df: pd.DataFrame, output_dir: Path) -> list[Path]:
    """为得分最高的典型事件绘制图谱。"""

    graph_paths: list[Path] = []
    if relation_df.empty:
        return graph_paths

    top_event_ids = relation_df.groupby("event_id")["association_score"].max(
    ).sort_values(ascending=False).head(2).index.tolist()
    graph_dir = output_dir / "kg_visual"
    graph_dir.mkdir(parents=True, exist_ok=True)

    for event_id in top_event_ids:
        subset = relation_df[relation_df["event_id"] == event_id].head(8)
        if subset.empty:
            continue
        graph = nx.Graph()
        event_name = subset.iloc[0]["event_name"]
        graph.add_node(event_name, node_type="event")
        for _, row in subset.iterrows():
            graph.add_node(row["stock_name"], node_type="stock")
            graph.add_edge(
                event_name, row["stock_name"], weight=row["association_score"])

        positions = nx.spring_layout(graph, seed=42, k=0.8)
        plt.figure(figsize=(10, 6))
        colors = ["#d1495b" if node ==
                  event_name else "#2d6a4f" for node in graph.nodes]
        nx.draw_networkx(
            graph,
            pos=positions,
            with_labels=True,
            node_color=colors,
            node_size=1900,
            font_family="Arial Unicode MS",
            font_size=10,
            width=[graph[u][v]["weight"] * 3 for u, v in graph.edges],
        )
        plt.title(f"事件主体-上市公司图谱：{event_name}", fontfamily="Arial Unicode MS")
        plt.axis("off")
        png_path = graph_dir / f"{event_id}.png"
        plt.tight_layout()
        plt.savefig(png_path, dpi=180)
        plt.close()
        graph_paths.append(png_path)

    return graph_paths
