# 更新日志

## 2026-04-01

### 新增功能

- **财务数据采集**：`fetch_financial_data()` 从 Tushare 拉取 PE/PB/ROE/净利润增长率等指标，fallback 到 `stock_financial_sample.json`
- **停复牌数据采集**：`fetch_suspend_resume_data()` 从 Tushare 拉取个股停牌/复牌信息
- **基本面预测增强**：`task3_impact_estimate.py` 新增 `fundamental_score`，`expected_car_4d` 公式引入基本面因子 `(1 + fundamental_score × 0.15)`
- **基本面过滤**：`task4_strategy.py` 新增 `pass_fundamental_filter()`，排除 PE 极值、ROE<5%、净利润下滑超 20% 的标的
- **停复牌交易判断**：`is_tradeable()` 增加停复牌日期区间过滤
- **数据配置开关**：`config.yaml` 新增 `strict_real_data` 和 `allow_synthetic_future_extension`

### 代码重构

- `FetchArtifacts` 扩展，新增 `financial_df` 和 `suspend_resume_df` 字段
- `workflow.py` 完成所有数据串联：财务数据传入 Task3/Task4，停复牌数据传入 Task4
- `utils.py` 的 `save_dataframe()` 移除 Parquet 输出，仅保留 CSV

### 清理

- 删除所有历史 Parquet 文件（`data/` 和 `outputs/` 下）
- `main_weekly.py` 和 `main_backtest.py` 增加 py_mini_racer 异常屏蔽
- 新增 `.gitignore` 忽略运行时生成的数据文件
