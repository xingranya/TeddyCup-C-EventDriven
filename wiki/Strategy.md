# 策略构建（Task 4）

## 交易规则

按竞赛要求：
- **买入日**：周二开盘价买入
- **卖出日**：当周周五收盘价全部卖出
- **初始资金**：100,000 元
- **持仓上限**：≤ 3 只股票

## 过滤流程

```
候选预测
  ├─ pass_basic_filter      — ST / 上市天数<60 / 成交额<80万
  ├─ pass_fundamental_filter — PE>100或<0 / ROE<5% / 净利润增长<-20%
  ├─ is_tradeable           — 停牌中 / 涨跌停
  └─ positive_score_threshold ≥ 0.02
→ 最终候选池 → 按 prediction_score 排序取前 3
```

## 仓位分配

按预测得分加权分配：

```python
capital_ratio = prediction_score / Σ(prediction_scores)
# 限制每只股票比例在 20%~50% 之间
```

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
