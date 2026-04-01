from __future__ import annotations

from pathlib import Path

import pandas as pd


def generate_result_xlsx(input_path: str | Path, output_path: str | Path) -> Path:
    """根据最终选股 CSV 生成竞赛要求的 Excel。"""

    input_file = Path(input_path)
    output_file = Path(output_path)
    df = pd.read_csv(input_file)
    export_df = df.rename(
        columns={
            "event_name": "事件名称",
            "stock_code": "标的（股票）代码",
            "capital_ratio": "资金比例",
        }
    )[["事件名称", "标的（股票）代码", "资金比例"]]
    export_df["标的（股票）代码"] = export_df["标的（股票）代码"].astype(str).str.zfill(6)
    export_df.to_excel(output_file, index=False)
    return output_file


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="生成竞赛提交用 result.xlsx")
    parser.add_argument("--input", required=True, help="final_picks.csv 路径")
    parser.add_argument("--output", default="result.xlsx", help="输出 xlsx 路径")
    args = parser.parse_args()
    path = generate_result_xlsx(args.input, args.output)
    print(f"已生成：{path}")
