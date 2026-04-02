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
    raw.setdefault("tushare", {})
    config_token = str(raw["tushare"].get("token", "") or "").strip()
    env_token = os.getenv(token_env, "").strip() if token_env else ""
    raw["tushare"]["token"] = config_token or env_token
    if not raw["tushare"]["token"]:
        raise RuntimeError(
            f"未检测到 Tushare 凭证，请在配置文件中设置 tushare.token 或先设置环境变量 {token_env}。"
        )

    return AppConfig(raw=raw)
