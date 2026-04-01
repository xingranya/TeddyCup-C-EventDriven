# TeddyCup-C-EventDriven

面向泰迪杯 C 题“事件驱动型股市投资策略构建”的 MVP 主链路实现。

## 目录说明
- `config/`：项目配置（`config.yaml` 含策略参数、Tushare token 及数据开关）。
- `data/manual/`：手工维护的行业关系、样例事件、股票池、财务指标和停复牌样例数据。
- `data/raw/`：原始抓取结果缓存（含 news、prices、benchmark、financial、suspend_resume）。
- `data/processed/`：中间计算结果（事件候选、关联关系等）。
- `pipeline/`：数据采集、事件识别、关联挖掘、影响估计、策略构建、事件研究增强、产业链图谱和报告生成模块。
- `outputs/weekly/`：周度运行输出。
- `outputs/backtest/`：历史回测输出。

## 运行方式
项目默认 Python 版本固定为 `3.12.13`，根目录下的 `.python-version` 与 `.venv` 已对应这一版本。

推荐先激活项目虚拟环境：
```bash
source .venv/bin/activate
python --version
```

```bash
python main_weekly.py --asof 2026-04-20
python main_backtest.py --start 2025-12-08 --end 2025-12-26
```

若本地尚未配置 `qstock` 或 `Tushare`，系统会自动退回到 `data/manual/` 下的样例数据，仍可跑通完整流程。

## 数据来源

| 数据类型 | 采集接口 | Fallback |
|---|---|---|
| 新闻/事件数据 | `qstock.news_data()` | `data/manual/sample_news.json` |
| 股票行情（日频 OHLCV） | `tushare.daily()` | `generate_sample_price_history()` 生成伪随机样例 |
| 基准指数（沪深300） | `tushare.index_daily()` | 同上 |
| **财务指标（PE/PB/ROE/净利润增长率等）** | `tushare.fina_indicator()` | `data/manual/stock_financial_sample.json` |
| **停复牌信息** | `tushare.suspend()` | `data/manual/suspend_resume_sample.json` |

**配置开关**（`config/config.yaml`）：
- `strict_real_data: false` — API 失败时是否抛异常（默认关闭，允许样例兜底）
- `allow_synthetic_future_extension: true` — 是否允许真实数据末尾接样例延伸（用于比赛周预演）

## 输出内容

## 输出内容
- `outputs/weekly/<asof_date>/result.xlsx` — 投资决策（事件名、股票代码、资金比例）
- `outputs/weekly/<asof_date>/report.md` — 周度分析报告
- `outputs/weekly/<asof_date>/company_relations.csv` — 事件-公司关联关系
- `outputs/weekly/<asof_date>/predictions.csv` — 含 `fundamental_score` 的 CAR 预测
- `outputs/weekly/<asof_date>/event_study/` — 事件研究明细/统计/联合均值CAR
- `outputs/weekly/<asof_date>/kg_visual/` — 产业链图谱（PNG + HTML）
- `outputs/backtest/weekly_summary.csv` — 回测周收益汇总
- `data/raw/<asof_date>/financial_<asof_date>.csv` — 财务指标原始数据
- `data/raw/<asof_date>/suspend_resume_<asof_date>.csv` — 停复牌原始数据
