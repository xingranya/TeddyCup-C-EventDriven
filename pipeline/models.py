from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class RunContext:
    """一次运行所需的上下文信息。"""

    asof_date: date
    project_root: Path
    output_dir: Path
    raw_dir: Path
    processed_dir: Path


@dataclass(slots=True)
class AppConfig:
    """项目配置。"""

    raw: dict[str, Any]

    @property
    def lookback_days(self) -> int:
        return int(self.raw["data"]["lookback_days"])

    @property
    def benchmark_code(self) -> str:
        return str(self.raw["data"]["benchmark_code"])

    @property
    def initial_capital(self) -> float:
        return float(self.raw["project"]["initial_capital"])

    @property
    def max_positions(self) -> int:
        return int(self.raw["strategy"]["max_positions"])

    @property
    def position_cap(self) -> float:
        return float(self.raw["strategy"]["single_position_max"])

    @property
    def position_floor(self) -> float:
        return float(self.raw["strategy"]["single_position_min"])

    @property
    def min_listing_days(self) -> int:
        return int(self.raw["strategy"]["min_listing_days"])

    @property
    def min_avg_turnover_million(self) -> float:
        return float(self.raw["strategy"]["min_avg_turnover_million"])

    @property
    def positive_score_threshold(self) -> float:
        return float(self.raw["strategy"]["positive_score_threshold"])

