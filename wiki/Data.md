# 数据说明

## 数据来源

| 数据类型 | 主要来源 | Fallback |
|---|---|---|
| 新闻/事件 | `qstock.news_data()` | `data/manual/sample_news.json` |
| 个股行情（日频 OHLCV） | `tushare.daily()` | 伪随机生成 |
| 基准指数（沪深300） | `tushare.index_daily()` | 伪随机生成 |
| 财务指标 | `tushare.fina_indicator()` | `data/manual/stock_financial_sample.json` |
| 停复牌信息 | `tushare.suspend()` | `data/manual/suspend_resume_sample.json` |

## 采集产物

运行后保存在 `data/raw/<asof_date>/`：

- `news_<asof_date>.csv` — 新闻数据
- `stock_universe.csv` — 股票池
- `prices_<asof_date>.csv` — 个股行情
- `benchmark_<asof_date>.csv` — 基准指数
- `financial_<asof_date>.csv` — 财务指标
- `suspend_resume_<asof_date>.csv` — 停复牌

## 财务指标字段

| 字段 | 说明 |
|---|---|
| `pe` | 市盈率 |
| `pb` | 市净率 |
| `roe` | 净资产收益率 |
| `net_profit_growth` | 净利润增长率 |
| `revenue_growth` | 营业收入增长率 |
| `debt_to_asset` | 资产负债率 |

## 配置开关

`config/config.yaml` 中的数据控制：

```yaml
data:
  strict_real_data: false      # true=API失败时抛异常
  allow_synthetic_future_extension: true  # 允许真实数据后接样例延伸
```
