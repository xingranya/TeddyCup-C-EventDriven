from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from pipeline.settings import load_config


class SettingsTestCase(unittest.TestCase):
    """配置加载兼容性测试。"""

    def _write_config(self, root: Path, token: str = "") -> None:
        config_path = root / "config"
        config_path.mkdir(parents=True, exist_ok=True)
        payload = {
            "project": {
                "name": "TeddyCup-C-EventDriven",
                "timezone": "Asia/Shanghai",
                "initial_capital": 100000,
                "market_close_time": "15:00:00",
            },
            "data": {
                "lookback_days": 14,
                "benchmark_code": "000300.SH",
                "trading_calendar_source": "tushare",
                "stock_whitelist_path": "",
                "stock_blacklist_path": "",
            },
            "tushare": {
                "token_env": "TUSHARE_TOKEN",
                "token": token,
            },
            "events": {
                "qstock_enabled": False,
                "import_paths": {},
            },
            "strategy": {
                "max_positions": 3,
                "single_position_max": 0.5,
                "single_position_min": 0.1,
                "min_listing_days": 60,
                "min_avg_turnover_million": 80,
                "positive_score_threshold": 0.02,
            },
        }
        with (config_path / "config.yaml").open("w", encoding="utf-8") as file:
            yaml.safe_dump(payload, file, allow_unicode=True)

    def test_load_config_keeps_token_from_config_when_env_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._write_config(root, token="config-token")
            with patch.dict(os.environ, {}, clear=True):
                config = load_config(root)
        self.assertEqual(config.tushare_token, "config-token")

    def test_load_config_uses_env_token_when_config_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._write_config(root, token="")
            with patch.dict(os.environ, {"TUSHARE_TOKEN": "env-token"}, clear=True):
                config = load_config(root)
        self.assertEqual(config.tushare_token, "env-token")

    def test_load_config_raises_when_config_and_env_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._write_config(root, token="")
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaisesRegex(RuntimeError, "tushare.token|环境变量 TUSHARE_TOKEN"):
                    load_config(root)


if __name__ == "__main__":
    unittest.main()
