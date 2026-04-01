# TeddyCup-C-EventDriven

面向**泰迪杯 C 题"事件驱动型股市投资策略构建"**的完整解决方案，覆盖从事件感知到策略落地的全链路分析。

## 快速开始

```bash
# 激活虚拟环境（Python 3.12）
source .venv/bin/activate

# 周度实测（竞赛提交）
python main_weekly.py --asof 2026-04-20

# 历史回测（验证策略有效性）
python main_backtest.py --start 2025-12-08 --end 2025-12-26
```

## 竞赛说明

| 任务 | 内容 |
|---|---|
| Task 1 | 从海量数据中识别事件，完成四维分类与五大量化特征提取 |
| Task 2 | 挖掘事件关联公司，构建"事件-上市公司"关联图谱 |
| Task 3 | 用事件研究法量化事件影响，给出传导逻辑链条 |
| Task 4 | 构建投资策略，以 10 万元初始资金在 2026-04-20 ~ 2026-05-03 间实测 |

**提交时间**：
- 第一周：2026-04-20 15:00 ~ 2026-04-21 09:00
- 第二周：2026-04-27 15:00 ~ 2026-04-28 09:00

## 项目结构

```
pipeline/
├── fetch_data.py              # 数据采集（新闻/行情/财务/停复牌）
├── task1_event_identify.py     # Task 1：事件识别与分类
├── task2_relation_mining.py   # Task 2：事件-公司关联关系
├── task3_impact_estimate.py   # Task 3：CAR 影响预测
├── event_study_enhanced.py     # 事件研究增强（标准化 AR/CAR 输出）
├── industry_chain_enhanced.py  # 产业链图谱（三层链式）
├── task4_strategy.py          # Task 4：策略构建与仓位分配
├── report_builder.py          # 周报生成
├── workflow.py                # 完整流水线编排
├── backtest.py                # 回测引擎
├── models.py                 # 数据模型
└── utils.py                  # 通用工具

config/
└── config.yaml                # 全部策略参数

data/manual/                   # 样例数据（提交仓库）
├── sample_news.json           # 新闻样例
├── stock_universe.csv         # 股票池
├── industry_relation_map.json  # 产业链映射
├── stock_financial_sample.json # 财务指标样例
└── suspend_resume_sample.json  # 停复牌样例
```

## 数据来源

| 数据类型 | 优先来源 | Fallback |
|---|---|---|
| 新闻/事件 | `qstock.news_data()` | `sample_news.json` |
| 个股行情 | `tushare.daily()` | 伪随机生成 |
| 基准指数 | `tushare.index_daily()` | 伪随机生成 |
| 财务指标 | `tushare.fina_indicator()` | `stock_financial_sample.json` |
| 停复牌 | `tushare.suspend()` | `suspend_resume_sample.json` |

所有原始数据缓存在 `data/raw/<asof_date>/`，中间结果保存在 `data/processed/<asof_date>/`。

## 流水线架构

```
FetchArtifacts
  → task1: event_df        (四维分类 + 五大量化特征)
  → task2: relation_df     (四维关联评分 + 知识图谱)
  → task3: prediction_df    (CAR 预测 + 基本面增强)
  → event_study_enhanced:   (标准化 AR/CAR 明细与统计)
  → industry_chain_enhanced: (事件→主题→产业环节→公司 三层图谱)
  → task4: final_picks      (过滤 + 仓位分配)
  → report: report.md + result.xlsx
```

## 关键参数（`config.yaml`）

| 参数 | 默认值 | 说明 |
|---|---|---|
| `max_positions` | 3 | 最大持仓股票数 |
| `single_position_max` | 0.5 | 单股最大仓位比例 |
| `single_position_min` | 0.2 | 单股最小仓位比例 |
| `min_listing_days` | 60 | 最短上市天数 |
| `min_avg_turnover_million` | 80 | 最低日均成交额（万元） |
| `positive_score_threshold` | 0.02 | 入选预测得分门槛 |
| `estimation_window` | -60 ~ -6 | 事件研究估计窗口 |
| `event_window` | -1 ~ +4 | CAR 计算事件窗口 |

## 输出内容

| 文件 | 说明 |
|---|---|
| `result.xlsx` | 投资决策（事件名、股票代码、资金比例） |
| `report.md` | 周度分析报告（含事件特征、关联图谱、CAR分析、决策理由） |
| `predictions.csv` | 所有事件-股票组合的预测得分与 CAR |
| `company_relations.csv` | 事件-公司关联关系 |
| `event_study/` | 事件研究明细/统计/联合均值CAR |
| `kg_visual/` | 产业链图谱（PNG + HTML） |
| `weekly_summary.csv` | 回测每周收益与净值 |

## 基本面过滤规则

Task 4 排除了以下标的：
- ST 股票
- 上市不足 60 天
- 日均成交额低于 80 万元
- PE > 100 或 PE < 0（极值）
- ROE < 5%
- 净利润同比增长 < -20%
- 停牌中或涨跌停无法买入
