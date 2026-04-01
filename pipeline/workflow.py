from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline.event_study_enhanced import EventStudyArtifacts, run_event_study_enhanced
from pipeline.fetch_data import run_fetch_pipeline
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
    output_dir = ensure_directory(project_root / "outputs/weekly" / asof_date.isoformat())
    raw_dir = ensure_directory(project_root / "data/raw" / asof_date.isoformat())
    processed_dir = ensure_directory(project_root / "data/processed" / asof_date.isoformat())
    context = RunContext(
        asof_date=asof_date,
        project_root=project_root,
        output_dir=output_dir,
        raw_dir=raw_dir,
        processed_dir=processed_dir,
    )

    fetch_artifacts = run_fetch_pipeline(context, config)
    event_df = run_event_identification(fetch_artifacts.news_df)
    save_dataframe(event_df, processed_dir / "event_candidates")

    relation_df, graph_paths = run_relation_mining(
        event_df=event_df,
        stock_df=fetch_artifacts.stock_df,
        price_df=fetch_artifacts.price_df,
        project_root=project_root,
        output_dir=output_dir,
        config=config,
    )
    prediction_df = run_impact_estimation(
        event_df=event_df,
        relation_df=relation_df,
        stock_df=fetch_artifacts.stock_df,
        price_df=fetch_artifacts.price_df,
        benchmark_df=fetch_artifacts.benchmark_df,
        financial_df=fetch_artifacts.financial_df,
        output_dir=output_dir,
        config=config,
    )
    event_study_artifacts = run_event_study_enhanced(
        event_df=event_df,
        relation_df=relation_df,
        price_df=fetch_artifacts.price_df,
        benchmark_df=fetch_artifacts.benchmark_df,
        output_dir=output_dir,
        config=config,
    )
    industry_chain_artifacts = run_industry_chain_enhanced(
        event_df=event_df,
        relation_df=relation_df,
        stock_df=fetch_artifacts.stock_df,
        output_dir=output_dir,
        project_root=project_root,
        config=config,
        prediction_df=prediction_df,
    )
    final_picks, summary = run_strategy_construction(
        asof_date=asof_date,
        event_df=event_df,
        prediction_df=prediction_df,
        stock_df=fetch_artifacts.stock_df,
        price_df=fetch_artifacts.price_df,
        financial_df=fetch_artifacts.financial_df,
        suspend_resume_df=fetch_artifacts.suspend_resume_df,
        output_dir=output_dir,
        config=config,
    )
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
