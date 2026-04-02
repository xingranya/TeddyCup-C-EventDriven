from __future__ import annotations

from dataclasses import dataclass
from datetime import date
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
from pipeline.utils import ensure_directory, parse_date, save_dataframe


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
        print(f"[ERROR] fetch step failed: {e}")
        raise  # fetch 是基础步骤，失败则无法继续

    # event_identify step
    try:
        event_df = run_event_identification(fetch_artifacts.news_df)
    except Exception as e:
        print(f"[ERROR] event_identify step failed: {e}")
        event_df = pd.DataFrame()
    save_dataframe(event_df, processed_dir / "event_candidates")

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
        print(f"[ERROR] relation_mining step failed: {e}")
        relation_df = pd.DataFrame()
        graph_paths = []

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
        print(f"[ERROR] financial_data step failed: {e}")
        financial_df = pd.DataFrame()

    try:
        suspend_resume_df = fetch_suspend_resume_data(
            stock_codes=related_stock_codes,
            context=context,
            config=config,
        )
    except Exception as e:
        print(f"[ERROR] suspend_resume_data step failed: {e}")
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
        print(f"[ERROR] impact_estimate step failed: {e}")
        prediction_df = pd.DataFrame()

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
        print(f"[ERROR] event_study step failed: {e}")
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
        print(f"[ERROR] industry_chain step failed: {e}")
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
        print(f"[ERROR] strategy step failed: {e}")
        final_picks = pd.DataFrame()
        summary = {
            "asof_date": asof_date.isoformat(),
            "fallback_used": False,
            "candidate_count": 0,
            "selected_count": 0,
            "buy_date": "",
            "sell_date": "",
        }

    # report_builder step
    try:
        report_path = build_weekly_report(
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
        )
    except Exception as e:
        print(f"[ERROR] report_builder step failed: {e}")
        report_path = output_dir / "report.md"

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
