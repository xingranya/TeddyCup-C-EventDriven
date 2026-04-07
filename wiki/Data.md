# 数据说明

## 数据来源

| 数据类型 | 正式来源 | 说明 |
|---|---|---|
| 新闻/事件 | `data/events/*` + 可选 `qstock.news_data()` | 当前默认关闭 qstock，以导入事件为主 |
| 股票池 | `tushare.stock_basic()` + `tushare.stock_company()` | 接口频控受限时回退到本地竞赛候选池 |
| 交易日历 | `tushare.trade_cal()` / `akshare.tool_trade_date_hist_sina()` | 所有事件窗口和交易日统一以此为准 |
| 个股行情（日频 OHLCV） | `tushare.daily()` | 不再生成伪随机行情 |
| 基准指数（沪深300） | `tushare.index_daily()` / 候选股票池市场代理序列 | 指数权限缺失时使用市场代理基准 |
| 估值快照 | `tushare.daily_basic()` | 权限受限时相关估值列为空 |
| 财务指标 | `tushare.fina_indicator()` / `akshare.stock_financial_abstract_ths()` | 仅使用 `asof_date` 当时可见的口径 |
| 停复牌信息 | `tushare.suspend()` | 支持开区间停牌状态 |

## 采集产物

运行后保存在 `data/raw/<asof_date>/`：

- `news_<asof_date>.csv` — 新闻数据
- `stock_universe.csv` — 股票池
- `trading_calendar_<asof_date>.csv` — 交易日历
- `prices_<asof_date>.csv` — 个股行情
- `benchmark_<asof_date>.csv` — 基准指数
- `financial_<asof_date>.csv` — 财务指标
- `suspend_resume_<asof_date>.csv` — 停复牌

## 本轮数据链路修复

- 导入事件时，单条坏记录不再直接中断整批流程，而是记录 warning 后跳过
- 交易日历现在会记录实际来源（Tushare / Akshare / 本地缓存 / 工作日降级）
- 若最终降级为工作日列表，报告会明确标注“不含中国节假日修正”

这部分修复的目标，是让比赛周数据准备更稳，也让报告能交代清楚“本次运行的数据有没有降级”。

## 财务指标字段

| 字段 | 说明 |
|---|---|
| `pe` | 市盈率 |
| `pb` | 市净率 |
| `turnover_rate` | 换手率 |
| `roe` | 净资产收益率 |
| `net_profit_growth` | 净利润增长率 |
| `revenue_growth` | 营业收入增长率 |
| `debt_to_asset` | 资产负债率 |

## 事件数据标准字段

- `title` — 标题
- `content` — 正文
- `published_at` — 发布时间
- `source_type` — 事件来源类别
- `source_name` — 采集来源名称
- `source_url` — 来源链接
- `entity_candidates` — 文本中提取的公司候选
- `raw_id` — 来源侧原始标识
- `content_hash` — 去重与追踪用哈希
- `collected_at` — 采集时间
