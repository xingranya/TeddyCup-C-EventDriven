# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the TeddyCup C Competition project ("Event-Driven Stock Market Investment Strategy Construction"). It implements a quantitative investment pipeline that identifies events from financial news, mines event-stock relationships, estimates event impact via event study methodology, and constructs weekly trading strategies.

## Commands

```bash
# Activate virtual environment
source .venv/bin/activate

# Run weekly pipeline (primary use case)
python main_weekly.py --asof 2026-04-20

# Run historical backtest
python main_backtest.py --start 2025-12-08 --end 2025-12-26
```

## Architecture

The pipeline runs as a sequential 4-task workflow:

```
run_weekly_pipeline (workflow.py)
├── fetch_data → news_df, stock_df, price_df, benchmark_df
├── task1_event_identify → event_df
├── task2_relation_mining → relation_df, graph_paths
├── task3_impact_estimate → prediction_df
├── task4_strategy → final_picks
└── report_builder → report.md
```

**Key modules:**
- `pipeline/workflow.py` - Orchestrates the full weekly pipeline, returns `WorkflowArtifacts`
- `pipeline/backtest.py` - Iterates `run_weekly_pipeline` week-by-week from start to end, calculates weekly returns with Tuesday buy/Friday sell logic
- `pipeline/models.py` - `RunContext` (directories, asof_date) and `AppConfig` (all strategy parameters)
- `pipeline/task1_event_identify.py` - Extract events from news data
- `pipeline/task2_relation_mining.py` - Build event-stock relations using association scoring (direct_mention, business_match, industry_overlap, historical_co_move)
- `pipeline/task3_impact_estimate.py` - Compute expected CAR using event study methodology (estimation window -60 to -6 days, event window -1 to +4 days)
- `pipeline/task4_strategy.py` - Construct final stock picks with position sizing
- `pipeline/report_builder.py` - Generate markdown report with visualizations
- `pipeline/fetch_data.py` - Fetch news/stock prices; falls back to `data/manual/` sample data if APIs unavailable

**Data flow:**
- Raw data cached in `data/raw/<asof_date>/`
- Intermediate results in `data/processed/<asof_date>/`
- Outputs written to `outputs/weekly/<asof_date>/`
- Backtest results in `outputs/backtest/`

## Configuration

All parameters are in `config/config.yaml` (strategy thresholds, scoring weights, event study windows, tushare token). The `AppConfig` class exposes these as typed properties.

## Dependencies

Managed via `requirements.txt`. Uses `.venv` with Python 3.12.13 (configured in `.python-version`).
