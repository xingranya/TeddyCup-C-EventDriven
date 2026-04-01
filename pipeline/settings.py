from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from pipeline.models import AppConfig


DEFAULT_CONFIG_PATH = Path("config/config.yaml")


def load_config(project_root: Path, config_path: str | None = None) -> AppConfig:
    """读取 YAML 配置，并注入环境变量中的动态参数。"""

    target = project_root / (config_path or DEFAULT_CONFIG_PATH)
    with target.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)

    token_env = raw.get("tushare", {}).get("token_env", "TUSHARE_TOKEN")
    if token_env and not raw.get("tushare", {}).get("token"):
        raw.setdefault("tushare", {})["token"] = os.getenv(token_env, "")

    return AppConfig(raw=raw)
