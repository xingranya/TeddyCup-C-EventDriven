# 策略构建（Task 4）

## 交易规则

按竞赛要求：
- **买入日**：周二开盘价买入
- **卖出日**：当周最后一个交易日收盘价全部卖出
- **初始资金**：100,000 元
- **持仓上限**：≤ 3 只股票

## 过滤流程

```
候选预测
  ├─ pass_basic_filter      — ST / 上市天数<60 / 成交额<80万
  ├─ pass_fundamental_filter — PE>100或<0 / ROE<5% / 净利润增长<-20%
  ├─ is_tradeable           — 持续停牌 / 当周无有效买卖交易日
  └─ positive_score_threshold ≥ 0.02
→ 最终候选池 → 按 prediction_score 排序取前 3
```

若周二休市，则顺延到该周周二之后的首个交易日买入。

## 仓位分配

当前仓位分配不是简单的“算完比例直接 round”，而是：

```python
1. 先按得分归一化
2. 再做 single_position_min / single_position_max 约束分配
3. 最后用最大余数法舍入，保证资金比例求和稳定为 1
```

这样做的原因，是为了解决此前“配置值和实际下限不一致、四舍五入后总和可能漂移”的问题，使提交到 `result.xlsx` 的资金比例更稳定地符合竞赛要求。

## 回测机制

`backtest.py` 按周迭代运行 `run_weekly_pipeline`：

```bash
python main_backtest.py --start 2025-12-08 --end 2025-12-26
```

输出：
- `outputs/backtest/weekly_summary.csv` — 每周收益与净值
- `outputs/backtest/<week>/trade_details.csv` — 每笔交易明细

回测收益计算：
```
周收益率 = Σ(卖出价/买入价 - 1) × 资金比例
净值 = (1 + 周收益率1) × (1 + 周收益率2) × ...
```
