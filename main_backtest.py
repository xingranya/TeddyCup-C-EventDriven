from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

# 屏蔽 py_mini_racer 在解释器关闭时的脏数据异常
try:
    import py_mini_racer
    _orig_mr_del = py_mini_racer.MiniRacer.__del__
    def _safe_mr_del(self):
        try:
            _orig_mr_del(self)
        except Exception:
            pass
    py_mini_racer.MiniRacer.__del__ = _safe_mr_del
except Exception:
    pass

from pipeline.backtest import run_backtest

warnings.filterwarnings("ignore")
warnings.filterwarnings("ignore", "Glyph.*missing from font", UserWarning)


def main() -> None:
    """历史回测入口。"""

    parser = argparse.ArgumentParser(description="泰迪杯 C 题历史回测脚本")
    parser.add_argument("--start", required=True, help="回测开始日期，格式为 YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="回测结束日期，格式为 YYYY-MM-DD")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent
    summary_df = run_backtest(project_root, args.start, args.end)
    print(f"回测完成，共 {len(summary_df)} 个周度样本。")
    print(f"结果目录：{project_root / 'outputs/backtest'}")


if __name__ == "__main__":
    main()
