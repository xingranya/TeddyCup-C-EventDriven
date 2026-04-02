from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from generate_result_xlsx import generate_result_xlsx


class ResultXlsxTestCase(unittest.TestCase):
    """提交文件生成测试。"""

    def test_result_xlsx_schema_matches_competition_requirements(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            input_path = temp_path / "final_picks.csv"
            output_path = temp_path / "result.xlsx"
            pd.DataFrame(
                [
                    {"event_name": "事件A", "stock_code": 688327, "capital_ratio": 0.5},
                    {"event_name": "事件B", "stock_code": "2792", "capital_ratio": 0.5},
                ]
            ).to_csv(input_path, index=False)

            generate_result_xlsx(input_path, output_path)
            result_df = pd.read_excel(output_path, dtype={"标的（股票）代码": str})

            self.assertEqual(result_df.columns.tolist(), ["事件名称", "标的（股票）代码", "资金比例"])
            self.assertEqual(result_df["标的（股票）代码"].tolist(), ["688327", "002792"])


if __name__ == "__main__":
    unittest.main()
