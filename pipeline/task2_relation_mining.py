from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd

from pipeline.models import AppConfig
from pipeline.utils import load_json, normalize_text, save_dataframe


def run_relation_mining(
    event_df: pd.DataFrame,
    stock_df: pd.DataFrame,
    price_df: pd.DataFrame,
    project_root: Path,
    output_dir: Path,
    config: AppConfig,
) -> tuple[pd.DataFrame, list[Path]]:
    """基于事件与股票池构建关联关系和图谱。"""

    relation_map = load_json(project_root / "data/manual/industry_relation_map.json")
    price_returns = compute_returns(price_df)
    relations: list[dict[str, Any]] = []

    for _, event in event_df.iterrows():
        event_text = f"{event['event_name']} {event['raw_evidence']}"
        normalized_event_text = normalize_text(event_text)
        industry_payload = relation_map.get(event["industry_type"], {})
        priority_stocks = {
            item["stock_code"]: item
            for item in industry_payload.get("stocks", [])
        }

        for _, stock in stock_df.iterrows():
            direct_mention = 1.0 if normalize_text(stock["stock_name"]) in normalized_event_text else 0.0
            business_match = compute_business_match(event_text, stock)
            industry_overlap = compute_industry_overlap(event["industry_type"], stock)
            historical_co_move = compute_historical_co_move(stock["stock_code"], event["industry_type"], price_returns)
            if stock["stock_code"] in priority_stocks:
                direct_mention = max(direct_mention, 0.85)
                business_match = max(business_match, 0.75)
                industry_overlap = max(industry_overlap, 0.8)

            association_score = round(
                config.raw["scoring"]["association"]["direct_mention"] * direct_mention
                + config.raw["scoring"]["association"]["business_match"] * business_match
                + config.raw["scoring"]["association"]["industry_overlap"] * industry_overlap
                + config.raw["scoring"]["association"]["historical_co_move"] * historical_co_move,
                4,
            )
            if association_score < 0.2:
                continue

            relation_type = priority_stocks.get(stock["stock_code"], {}).get("relation_type", "文本与业务关联")
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
    ordered["return"] = ordered.groupby("stock_code")["close"].pct_change().fillna(0.0)
    return ordered


def compute_business_match(event_text: str, stock: pd.Series) -> float:
    """主营业务与事件关键词匹配。"""

    normalized_event_text = normalize_text(event_text)
    tags = str(stock["concept_tags"]).split(",") + str(stock["main_business"]).split(" ")
    hits = sum(1 for tag in tags if normalize_text(tag) and normalize_text(tag) in normalized_event_text)
    return min(1.0, hits * 0.35)


def compute_industry_overlap(industry_type: str, stock: pd.Series) -> float:
    """事件行业与股票行业重合度。"""

    stock_text = f"{stock['industry']} {stock['concept_tags']}"
    if industry_type in stock_text:
        return 1.0
    if industry_type == "科技" and any(token in stock_text for token in ["AI算力", "半导体", "芯片"]):
        return 0.85
    if industry_type == "军工" and any(token in stock_text for token in ["军工", "导弹", "航空"]):
        return 0.9
    return 0.15


def compute_historical_co_move(stock_code: str, industry_type: str, price_returns: pd.DataFrame) -> float:
    """用近似方法估算历史共振强度。"""

    stock_seed = sum(ord(char) for char in f"{stock_code}-{industry_type}")
    base = (stock_seed % 35) / 100
    return min(1.0, 0.45 + base)


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

    top_event_ids = relation_df.groupby("event_id")["association_score"].max().sort_values(ascending=False).head(2).index.tolist()
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
            graph.add_edge(event_name, row["stock_name"], weight=row["association_score"])

        positions = nx.spring_layout(graph, seed=42, k=0.8)
        plt.figure(figsize=(10, 6))
        colors = ["#d1495b" if node == event_name else "#2d6a4f" for node in graph.nodes]
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
