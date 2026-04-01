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

from generate_result_xlsx import generate_result_xlsx
from pipeline.workflow import run_weekly_pipeline

warnings.filterwarnings("ignore")
warnings.filterwarnings("ignore", "Glyph.*missing from font", UserWarning)


def main() -> None:
    """周度运行入口。"""

    parser = argparse.ArgumentParser(description="泰迪杯 C 题周度运行脚本")
    parser.add_argument("--asof", required=True, help="分析基准日，格式为 YYYY-MM-DD")
    parser.add_argument("--config", default=None, help="配置文件路径")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent
    artifacts = run_weekly_pipeline(project_root=project_root, asof_value=args.asof, config_path=args.config)
    final_csv = artifacts.context.output_dir / "final_picks.csv"
    result_path = artifacts.context.output_dir / "result.xlsx"
    generate_result_xlsx(final_csv, result_path)

    print(f"周度流程完成：{artifacts.context.output_dir}")
    print(f"报告路径：{artifacts.report_path}")
    print(f"提交文件：{result_path}")


if __name__ == "__main__":
    main()
