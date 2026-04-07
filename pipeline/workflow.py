from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import logging
from pathlib import Path

import pandas as pd

from pipeline.event_study_enhanced import EventStudyArtifacts, run_event_study_enhanced
from pipeline.fetch_data import fetch_financial_data, fetch_suspend_resume_data, run_fetch_pipeline
from pipeline.industry_chain_enhanced import IndustryChainArtifacts, run_industry_chain_enhanced
from pipeline.models import RunContext
from pipeline.report_builder import build_weekly_report
from pipeline.settings import load_config
from pipeline.task1_event_identify import run_event_identification
from pipeline.task2_relation_mining import run_relation_mining
from pipeline.task3_impact_estimate import run_impact_estimation
from pipeline.task4_strategy import run_strategy_construction
from pipeline.utils import configure_logging, ensure_directory, parse_date, save_dataframe


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class WorkflowArtifacts:
    """完整周度流程产物。"""

    context: RunContext
    event_df: pd.DataFrame
    relation_df: pd.DataFrame
    prediction_df: pd.DataFrame
    final_picks: pd.DataFrame
    report_path: Path
    graph_paths: list[Path]
    event_study_artifacts: EventStudyArtifacts
    industry_chain_artifacts: IndustryChainArtifacts
    summary: dict


def run_weekly_pipeline(project_root: Path, asof_value: str | date, config_path: str | None = None) -> WorkflowArtifacts:
    """执行一轮完整周度流程。"""

    configure_logging()
    config = load_config(project_root, config_path)
    asof_date = parse_date(asof_value)
    output_dir = ensure_directory(
        project_root / "outputs/weekly" / asof_date.isoformat())
    raw_dir = ensure_directory(
        project_root / "data/raw" / asof_date.isoformat())
    processed_dir = ensure_directory(
        project_root / "data/processed" / asof_date.isoformat())
    context = RunContext(
        asof_date=asof_date,
        project_root=project_root,
        output_dir=output_dir,
        raw_dir=raw_dir,
        processed_dir=processed_dir,
    )

    # fetch step
    try:
        fetch_artifacts = run_fetch_pipeline(context, config)
    except Exception as e:
        logger.exception("fetch 阶段失败。")
        raise  # fetch 是基础步骤，失败则无法继续

    # event_identify step
    try:
        event_df = run_event_identification(
            fetch_artifacts.news_df,
            event_taxonomy=config.event_taxonomy,
        )
    except Exception as e:
        logger.exception("event_identify 阶段失败。")
        event_df = pd.DataFrame()
    save_dataframe(event_df, processed_dir / "event_candidates")
    logger.info("event_identify 阶段完成：事件数=%s", len(event_df))

    # relation_mining step
    try:
        relation_df, graph_paths = run_relation_mining(
            event_df=event_df,
            stock_df=fetch_artifacts.stock_df,
            price_df=fetch_artifacts.price_df,
            project_root=project_root,
            output_dir=output_dir,
            config=config,
        )
    except Exception as e:
        logger.exception("relation_mining 阶段失败。")
        relation_df = pd.DataFrame()
        graph_paths = []
    logger.info("relation_mining 阶段完成：关联数=%s，图谱文件数=%s", len(relation_df), len(graph_paths))

    # financial_data step
    related_stock_codes = []
    if not relation_df.empty and "stock_code" in relation_df.columns:
        related_stock_codes = relation_df["stock_code"].dropna().astype(
            str).unique().tolist()

    try:
        financial_df = fetch_financial_data(
            stock_codes=related_stock_codes,
            context=context,
            config=config,
            trading_calendar=fetch_artifacts.trading_calendar,
        )
    except Exception as e:
        logger.exception("financial_data 阶段失败。")
        financial_df = pd.DataFrame()

    try:
        suspend_resume_df = fetch_suspend_resume_data(
            stock_codes=related_stock_codes,
            context=context,
            config=config,
        )
    except Exception as e:
        logger.exception("suspend_resume_data 阶段失败。")
        suspend_resume_df = pd.DataFrame()

    save_dataframe(financial_df, context.raw_dir /
                   f"financial_{context.asof_date.isoformat()}")
    save_dataframe(suspend_resume_df, context.raw_dir /
                   f"suspend_resume_{context.asof_date.isoformat()}")

    # impact_estimate step
    try:
        prediction_df = run_impact_estimation(
            event_df=event_df,
            relation_df=relation_df,
            stock_df=fetch_artifacts.stock_df,
            price_df=fetch_artifacts.price_df,
            benchmark_df=fetch_artifacts.benchmark_df,
            trading_calendar=fetch_artifacts.trading_calendar,
            financial_df=financial_df,
            output_dir=output_dir,
            config=config,
        )
    except Exception as e:
        logger.exception("impact_estimate 阶段失败。")
        prediction_df = pd.DataFrame()
    logger.info("impact_estimate 阶段完成：预测数=%s", len(prediction_df))

    # event_study step
    study_dir = output_dir / "event_study"
    try:
        event_study_artifacts = run_event_study_enhanced(
            event_df=event_df,
            relation_df=relation_df,
            price_df=fetch_artifacts.price_df,
            benchmark_df=fetch_artifacts.benchmark_df,
            trading_calendar=fetch_artifacts.trading_calendar,
            output_dir=output_dir,
            config=config,
        )
    except Exception as e:
        logger.exception("event_study 阶段失败。")
        from pipeline.event_study_enhanced import EventStudyArtifacts
        event_study_artifacts = EventStudyArtifacts(
            detail_df=pd.DataFrame(),
            stats_df=pd.DataFrame(),
            joint_mean_car_df=pd.DataFrame(),
            output_dir=study_dir,
            joint_mean_car_path=study_dir / "joint_mean_car.png",
        )

    # industry_chain step
    kg_dir = output_dir / "kg_visual"
    try:
        industry_chain_artifacts = run_industry_chain_enhanced(
            event_df=event_df,
            relation_df=relation_df,
            stock_df=fetch_artifacts.stock_df,
            output_dir=output_dir,
            project_root=project_root,
            config=config,
            prediction_df=prediction_df,
        )
    except Exception as e:
        logger.exception("industry_chain 阶段失败。")
        from pipeline.industry_chain_enhanced import IndustryChainArtifacts
        industry_chain_artifacts = IndustryChainArtifacts(
            relation_df=pd.DataFrame(),
            summary_path=kg_dir / "industry_chain_summary.md",
            combined_png_path=kg_dir / "industry_chain_graph.png",
            combined_html_path=kg_dir / "industry_chain_graph.html",
            selected_events=[],
        )

    # strategy step
    try:
        final_picks, summary = run_strategy_construction(
            asof_date=asof_date,
            event_df=event_df,
            prediction_df=prediction_df,
            stock_df=fetch_artifacts.stock_df,
            trading_calendar=fetch_artifacts.trading_calendar,
            financial_df=financial_df,
            suspend_resume_df=suspend_resume_df,
            output_dir=output_dir,
            config=config,
            price_df=fetch_artifacts.price_df,
        )
    except Exception as e:
        logger.exception("strategy 阶段失败。")
        final_picks = pd.DataFrame()
        summary = {
            "asof_date": asof_date.isoformat(),
            "fallback_used": False,
            "candidate_count": 0,
            "selected_count": 0,
            "buy_date": "",
            "sell_date": "",
        }
    summary.update(
        {
            "event_count": int(len(event_df)),
            "relation_count": int(len(relation_df)),
            "prediction_count": int(len(prediction_df)),
            "trading_calendar_source": fetch_artifacts.trading_calendar_source_name,
            "trading_calendar_status_note": fetch_artifacts.trading_calendar_status_note,
        }
    )
    logger.info(
        "strategy 阶段完成：候选=%s，入选=%s，fallback=%s，交易日历=%s",
        summary["candidate_count"],
        summary["selected_count"],
        summary["fallback_used"],
        summary["trading_calendar_source"],
    )

    # report_builder step
    try:
        report_path = build_weekly_report(
            project_root=project_root,
            asof_date=asof_date,
            event_df=event_df,
            relation_df=relation_df,
            prediction_df=prediction_df,
            final_picks=final_picks,
            graph_paths=graph_paths,
            output_dir=output_dir,
            config=config,
            event_study_artifacts=event_study_artifacts,
            industry_chain_artifacts=industry_chain_artifacts,
            summary=summary,
        )
    except Exception as e:
        logger.exception("report_builder 阶段失败。")
        report_path = output_dir / "report.md"

    logger.info(
        "周度流程结束：日期=%s，事件=%s，关联=%s，预测=%s，持仓=%s",
        summary["asof_date"],
        summary["event_count"],
        summary["relation_count"],
        summary["prediction_count"],
        summary["selected_count"],
    )

    return WorkflowArtifacts(
        context=context,
        event_df=event_df,
        relation_df=relation_df,
        prediction_df=prediction_df,
        final_picks=final_picks,
        report_path=report_path,
        graph_paths=graph_paths,
        event_study_artifacts=event_study_artifacts,
        industry_chain_artifacts=industry_chain_artifacts,
        summary=summary,
    )
