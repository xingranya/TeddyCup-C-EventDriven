from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import plotly.graph_objects as go

from pipeline.models import AppConfig
from pipeline.utils import ensure_directory, load_json, normalize_text, save_dataframe, configure_matplotlib_chinese

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


@dataclass(slots=True)
class IndustryChainArtifacts:
    """产业链图谱增强阶段产物。"""

    relation_df: pd.DataFrame
    summary_path: Path
    combined_png_path: Path
    combined_html_path: Path
    selected_events: list[str]


def run_industry_chain_enhanced(
    event_df: pd.DataFrame,
    relation_df: pd.DataFrame,
    stock_df: pd.DataFrame,
    output_dir: Path,
    project_root: Path,
    config: AppConfig,
    prediction_df: pd.DataFrame,
) -> IndustryChainArtifacts:
    """构建产业链图谱增强输出。"""

    kg_dir = ensure_directory(output_dir / "kg_visual")
    relation_map = load_json(
        project_root / "data/manual/industry_relation_map.json")
    chain_relation_df = _build_chain_relations(
        event_df, relation_df, stock_df, relation_map)
    save_dataframe(chain_relation_df, kg_dir / "industry_chain_relations")

    selected_events = _select_featured_events(
        event_df, relation_df, prediction_df)
    per_event_pngs: list[Path] = []
    for event_id in selected_events:
        subset = chain_relation_df[chain_relation_df["event_id"] == event_id].copy(
        )
        if subset.empty:
            continue
        event_png = kg_dir / f"industry_chain_{event_id}.png"
        _render_single_chain_png(subset, event_png)
        per_event_pngs.append(event_png)

    combined_png_path = kg_dir / "industry_chain_graph.png"
    _render_combined_chain_png(
        chain_relation_df, selected_events, combined_png_path)
    combined_html_path = kg_dir / "industry_chain_graph.html"
    _render_chain_html(chain_relation_df, selected_events, combined_html_path)
    summary_path = kg_dir / "industry_chain_summary.md"
    _write_chain_summary(chain_relation_df, selected_events,
                         summary_path, per_event_pngs)

    return IndustryChainArtifacts(
        relation_df=chain_relation_df,
        summary_path=summary_path,
        combined_png_path=combined_png_path,
        combined_html_path=combined_html_path,
        selected_events=selected_events,
    )


def _build_chain_relations(
    event_df: pd.DataFrame,
    relation_df: pd.DataFrame,
    stock_df: pd.DataFrame,
    relation_map: dict,
) -> pd.DataFrame:
    """构建链式产业关系表。"""

    event_meta = event_df.set_index(
        "event_id")[["event_name", "industry_type", "heat_score", "raw_evidence"]]
    stock_meta = stock_df.set_index(
        "stock_code")[["stock_name", "industry", "concept_tags", "main_business"]]
    rows: list[dict] = []

    for _, relation in relation_df.iterrows():
        event_id = relation["event_id"]
        if event_id not in event_meta.index:
            continue
        event_info = event_meta.loc[event_id]
        stock_code = relation["stock_code"]
        if stock_code not in stock_meta.index:
            continue
        stock_info = stock_meta.loc[stock_code]
        raw_industry = event_info.get("industry_type", "")
        theme_key = INDUSTRY_LABEL_MAP.get(raw_industry, raw_industry)
        theme_name = theme_key
        theme_payload = relation_map.get(theme_key, {})
        theme_match_score = _compute_theme_match(event_info, theme_payload)

        mapped_links = _match_links(
            theme_payload, relation, stock_info, event_info)
        if not mapped_links:
            mapped_links = [
                {
                    "link_name": "通用产业环节",
                    "link_match_score": max(0.35, float(relation["business_match"])),
                    "relation_type": relation["relation_type"],
                    "relation_path": relation["relation_path"],
                }
            ]

        for link in mapped_links:
            chain_confidence = round(
                0.55 * float(relation["association_score"])
                + 0.20 * theme_match_score
                + 0.25 * link["link_match_score"],
                4,
            )
            rows.append(
                {
                    "event_id": event_id,
                    "event_name": event_info["event_name"],
                    "theme_name": theme_payload.get("theme_name", theme_name),
                    "link_name": link["link_name"],
                    "stock_code": stock_code,
                    "stock_name": stock_info["stock_name"],
                    "relation_type": link["relation_type"],
                    "association_score": float(relation["association_score"]),
                    "chain_confidence": chain_confidence,
                    "theme_match_score": round(theme_match_score, 4),
                    "link_match_score": round(link["link_match_score"], 4),
                    "chain_depth": 3,
                    "relation_path": link["relation_path"],
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "event_id",
                "event_name",
                "theme_name",
                "link_name",
                "stock_code",
                "stock_name",
                "relation_type",
                "association_score",
                "chain_confidence",
                "theme_match_score",
                "link_match_score",
                "chain_depth",
                "relation_path",
            ]
        )

    return pd.DataFrame(rows).sort_values(
        ["event_id", "chain_confidence", "association_score"], ascending=[True, False, False]
    ).reset_index(drop=True)


def _compute_theme_match(event_info: pd.Series, theme_payload: dict) -> float:
    """计算主题匹配分（增强版）。

    基于关键词命中数计算，baseline 0.45。
    如果事件文本中包含产业主题的核心关键词（前3个关键词），给予额外 0.15 的核心关键词加分。
    """

    if not theme_payload:
        return 0.3

    normalized_text = normalize_text(
        f"{event_info['event_name']} {event_info['raw_evidence']}")
    keywords = theme_payload.get("keywords", [])

    # 计算普通关键词命中
    hits = sum(1 for keyword in keywords if normalize_text(
        keyword) in normalized_text)
    base_score = 0.45 + hits * 0.12

    # 核心关键词加分：检查前3个关键词是否有命中
    core_keywords = keywords[:3]
    core_hits = sum(1 for keyword in core_keywords if normalize_text(
        keyword) in normalized_text)
    core_bonus = 0.15 if core_hits > 0 else 0.0

    return min(1.0, base_score + core_bonus)


def _match_links(theme_payload: dict, relation: pd.Series, stock_info: pd.Series, event_info: pd.Series) -> list[dict]:
    """匹配产业链环节。"""

    matched: list[dict] = []
    event_text = normalize_text(
        f"{event_info['event_name']} {event_info['raw_evidence']}")
    stock_text = normalize_text(
        f"{stock_info['industry']} {stock_info['concept_tags']} {stock_info['main_business']}")
    for link in theme_payload.get("links", []):
        direct_stock_match = next(
            (
                stock_item
                for stock_item in link.get("stocks", [])
                if stock_item["stock_code"] == relation["stock_code"]
            ),
            None,
        )
        keyword_hits = sum(1 for keyword in link.get("keywords", []) if normalize_text(
            keyword) in event_text or normalize_text(keyword) in stock_text)
        if direct_stock_match or keyword_hits > 0:
            link_match_score = min(
                1.0,
                0.45
                + (0.35 if direct_stock_match else 0.0)
                + keyword_hits * 0.08,
            )
            matched.append(
                {
                    "link_name": link["link_name"],
                    "link_match_score": link_match_score,
                    "relation_type": direct_stock_match["relation_type"] if direct_stock_match else relation["relation_type"],
                    "relation_path": direct_stock_match["relation_path"] if direct_stock_match else f"{event_info['event_name']} -> {theme_payload.get('theme_name', event_info['industry_type'])} -> {link['link_name']} -> {stock_info['stock_name']}",
                }
            )
    return matched


def _select_featured_events(event_df: pd.DataFrame, relation_df: pd.DataFrame, prediction_df: pd.DataFrame) -> list[str]:
    """选出两类重点事件。"""

    if event_df.empty:
        return []
    selected: list[str] = []

    hottest = event_df.sort_values(
        "heat_score", ascending=False).iloc[0]["event_id"]
    selected.append(hottest)

    if not prediction_df.empty:
        top_pred_event = prediction_df.sort_values(
            "prediction_score", ascending=False).iloc[0]["event_id"]
    else:
        top_pred_event = hottest

    if top_pred_event == hottest and not relation_df.empty:
        breadth_df = relation_df.groupby(
            "event_id")["stock_code"].nunique().reset_index(name="stock_breadth")
        breadth_df = breadth_df.sort_values("stock_breadth", ascending=False)
        for _, row in breadth_df.iterrows():
            if row["event_id"] != hottest:
                top_pred_event = row["event_id"]
                break

    if top_pred_event not in selected:
        selected.append(top_pred_event)
    return selected


def _render_single_chain_png(subset: pd.DataFrame, output_path: Path) -> None:
    """绘制单事件产业链图谱。"""

    graph = _build_chain_graph(subset)
    positions = nx.spring_layout(graph, seed=42, k=1.1)
    plt.figure(figsize=(12, 7))
    node_colors = []
    for node, data in graph.nodes(data=True):
        node_type = data.get("node_type")
        if node_type == "event":
            node_colors.append("#d1495b")
        elif node_type == "industry_theme":
            node_colors.append("#f4a261")
        elif node_type == "industry_link":
            node_colors.append("#4d96ff")
        else:
            node_colors.append("#2a9d8f")

    edge_widths = [max(1.2, graph[u][v].get("weight", 0.2) * 4)
                   for u, v in graph.edges]
    nx.draw_networkx(
        graph,
        pos=positions,
        with_labels=True,
        node_color=node_colors,
        node_size=1800,
        font_family="Arial Unicode MS",
        font_size=9,
        width=edge_widths,
    )
    event_name = subset.iloc[0]["event_name"]
    plt.title(f"产业链图谱：{event_name}", fontfamily="Arial Unicode MS")
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(output_path, dpi=180)
    plt.close()


def _render_combined_chain_png(chain_relation_df: pd.DataFrame, selected_events: list[str], output_path: Path) -> None:
    """绘制双事件组合图。"""

    if not selected_events:
        plt.figure(figsize=(10, 6))
        plt.text(0.5, 0.5, "暂无产业链图谱样本", ha="center", va="center")
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(output_path, dpi=180)
        plt.close()
        return

    fig, axes = plt.subplots(len(selected_events), 1,
                             figsize=(12, 6 * len(selected_events)))
    if len(selected_events) == 1:
        axes = [axes]

    for index, event_id in enumerate(selected_events):
        subset = chain_relation_df[chain_relation_df["event_id"] == event_id].head(
            12)
        if subset.empty:
            axes[index].text(0.5, 0.5, "暂无可用链路样本", ha="center", va="center")
            axes[index].axis("off")
            continue
        graph = _build_chain_graph(subset)
        positions = nx.spring_layout(graph, seed=42 + index, k=1.0)
        node_colors = []
        for _, data in graph.nodes(data=True):
            node_type = data.get("node_type")
            if node_type == "event":
                node_colors.append("#d1495b")
            elif node_type == "industry_theme":
                node_colors.append("#f4a261")
            elif node_type == "industry_link":
                node_colors.append("#4d96ff")
            else:
                node_colors.append("#2a9d8f")
        edge_widths = [max(1.2, graph[u][v].get("weight", 0.2) * 4)
                       for u, v in graph.edges]
        nx.draw_networkx(
            graph,
            pos=positions,
            with_labels=True,
            node_color=node_colors,
            node_size=1600,
            font_family="Arial Unicode MS",
            font_size=8,
            width=edge_widths,
            ax=axes[index],
        )
        axes[index].set_title(
            f"产业链图谱：{subset.iloc[0]['event_name']}",
            fontfamily="Arial Unicode MS",
        )
        axes[index].axis("off")

    plt.tight_layout()
    plt.savefig(output_path, dpi=180)
    plt.close()


def _render_chain_html(chain_relation_df: pd.DataFrame, selected_events: list[str], output_path: Path) -> None:
    """生成 HTML 版产业链图谱。"""

    if not selected_events:
        output_path.write_text(
            "<html><body><h1>暂无产业链图谱样本</h1></body></html>", encoding="utf-8")
        return

    figures: list[str] = []
    first_figure = True
    for event_id in selected_events:
        subset = chain_relation_df[chain_relation_df["event_id"] == event_id].head(
            12)
        if subset.empty:
            continue
        graph = _build_chain_graph(subset)
        positions = nx.spring_layout(graph, seed=52, k=1.0)
        edge_x: list[float] = []
        edge_y: list[float] = []
        for left, right in graph.edges():
            x0, y0 = positions[left]
            x1, y1 = positions[right]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
        edge_trace = go.Scatter(
            x=edge_x,
            y=edge_y,
            mode="lines",
            line={"width": 1.5, "color": "#888"},
            hoverinfo="none",
        )

        node_x: list[float] = []
        node_y: list[float] = []
        node_text: list[str] = []
        node_color: list[str] = []
        for node, data in graph.nodes(data=True):
            x, y = positions[node]
            node_x.append(x)
            node_y.append(y)
            node_text.append(node)
            node_type = data.get("node_type")
            if node_type == "event":
                node_color.append("#d1495b")
            elif node_type == "industry_theme":
                node_color.append("#f4a261")
            elif node_type == "industry_link":
                node_color.append("#4d96ff")
            else:
                node_color.append("#2a9d8f")

        node_trace = go.Scatter(
            x=node_x,
            y=node_y,
            mode="markers+text",
            text=node_text,
            textposition="top center",
            hoverinfo="text",
            marker={"size": 18, "color": node_color,
                    "line": {"width": 1, "color": "#ffffff"}},
        )
        figure = go.Figure(data=[edge_trace, node_trace])
        figure.update_layout(
            title=f"产业链图谱：{subset.iloc[0]['event_name']}",
            showlegend=False,
            margin={"l": 20, "r": 20, "t": 60, "b": 20},
            xaxis={"showgrid": False, "zeroline": False, "visible": False},
            yaxis={"showgrid": False, "zeroline": False, "visible": False},
        )
        figures.append(
            figure.to_html(
                full_html=False,
                include_plotlyjs="inline" if first_figure else False,
            )
        )
        first_figure = False

    html = "<html><head><meta charset='utf-8'><title>产业链图谱</title></head><body>"
    html += "<h1>产业链图谱增强输出</h1>"
    html += "".join(figures)
    html += "</body></html>"
    output_path.write_text(html, encoding="utf-8")


def _write_chain_summary(chain_relation_df: pd.DataFrame, selected_events: list[str], output_path: Path, per_event_pngs: list[Path]) -> None:
    """生成图谱说明块。"""

    if chain_relation_df.empty:
        output_path.write_text("# 产业链图谱说明\n\n暂无可用图谱样本。", encoding="utf-8")
        return

    sections: list[str] = ["# 产业链图谱说明", ""]
    for index, event_id in enumerate(selected_events, start=1):
        subset = chain_relation_df[chain_relation_df["event_id"] == event_id].head(
            8)
        if subset.empty:
            continue
        sections.append(f"## 重点事件 {index}")
        sections.append(f"- 事件名称：{subset.iloc[0]['event_name']}")
        sections.append(f"- 主题：{subset.iloc[0]['theme_name']}")
        sections.append(f"- 覆盖环节数：{subset['link_name'].nunique()}")
        sections.append(f"- 覆盖股票数：{subset['stock_code'].nunique()}")
        sections.append("")
        sections.append(subset[["theme_name", "link_name", "stock_name", "association_score",
                        "chain_confidence", "relation_path"]].to_markdown(index=False))
        sections.append("")
    if per_event_pngs:
        sections.append("## 图谱文件")
        for path in per_event_pngs:
            sections.append(f"- `{path.name}`")
    output_path.write_text("\n".join(sections), encoding="utf-8")


def _build_chain_graph(subset: pd.DataFrame) -> nx.Graph:
    """构造链式图谱。"""

    graph = nx.Graph()
    for _, row in subset.iterrows():
        event_node = row["event_name"]
        theme_node = row["theme_name"]
        link_node = row["link_name"]
        stock_node = row["stock_name"]
        graph.add_node(event_node, node_type="event")
        graph.add_node(theme_node, node_type="industry_theme")
        graph.add_node(link_node, node_type="industry_link")
        graph.add_node(stock_node, node_type="stock")
        graph.add_edge(event_node, theme_node, edge_type="event_to_theme", weight=max(
            0.6, row["theme_match_score"]))
        graph.add_edge(theme_node, link_node, edge_type="theme_to_link",
                       weight=max(0.6, row["link_match_score"]))
        graph.add_edge(link_node, stock_node, edge_type="link_to_stock",
                       weight=max(0.8, row["association_score"]))
        if float(row["association_score"]) >= 0.75:
            graph.add_edge(event_node, stock_node,
                           edge_type="event_to_stock_direct", weight=row["association_score"])
    return graph
