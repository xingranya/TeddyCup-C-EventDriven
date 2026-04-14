# TeddyCup-C-EventDriven

面向**泰迪杯 C 题"事件驱动型股市投资策略构建"**的完整解决方案，覆盖从事件感知到策略落地的全链路分析。

## 快速开始

```bash
cd /Users/xingranya/Downloads/TeddyCup-C-EventDriven
source .venv/bin/activate
export TUSHARE_TOKEN='你的_TOKEN'
```

如果你已经激活了 `.venv`，可以直接使用 `python`；如果不想依赖当前 shell 环境，下面 README 里的所有命令都可以直接替换成 `.venv/bin/python ...` 执行。

如果不想手动 `export`，也可以直接在 `config/config.yaml` 中写入：

```yaml
tushare:
  token_env: TUSHARE_TOKEN
  token: "你的_TOKEN"
```

当前配置加载规则为：优先使用 `config.yaml` 中显式填写的 `tushare.token`，若为空则回退读取环境变量 `TUSHARE_TOKEN`。

## 竞赛说明

| 任务   | 内容                                                              |
| ------ | ----------------------------------------------------------------- |
| Task 1 | 从海量数据中识别事件，完成四维分类与五大量化特征提取              |
| Task 2 | 挖掘事件关联公司，构建"事件-上市公司"关联图谱                     |
| Task 3 | 用事件研究法量化事件影响，给出传导逻辑链条                        |
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
├── stock_universe.csv         # 历史小样本参考，不参与正式运行
├── industry_relation_map.json  # 产业链映射
├── stock_financial_sample.json # 历史测试夹具
└── suspend_resume_sample.json  # 历史测试夹具

data/events/                   # 正式事件导入目录
├── policy/                    # 政策类事件导入文件
├── announcement/              # 公司公告类事件导入文件
├── industry/                  # 行业/技术类事件导入文件
└── macro/                     # 宏观/地缘类事件导入文件

data/inbox/events_raw/         # 原始采集留痕（jsonl）
data/staging/events/           # 标准化候选事件与审阅队列

scripts/
└── event_ingest.py            # 事件采集/标准化/发布入口
```

## 数据来源

| 数据类型  | 正式来源                                                                  | 说明                                        |
| --------- | ------------------------------------------------------------------------- | ------------------------------------------- |
| 新闻/事件 | `data/events/*` 规范化导入                                              | 当前默认关闭 qstock，正式运行以导入事件为主 |
| 股票池    | `tushare.stock_basic()` + `tushare.stock_company()`                   | 权限或频控受限时回退到竞赛候选池缓存        |
| 交易日历  | `tushare.trade_cal()` / `akshare.tool_trade_date_hist_sina()`         | 买卖日、事件窗口统一基于真实交易日历        |
| 个股行情  | `tushare.daily()`                                                       | 正式运行不再生成伪随机行情                  |
| 基准指数  | `tushare.index_daily()` / 候选股票池市场代理序列                        | 指数权限缺失时用横截面收益构造代理基准      |
| 估值快照  | `tushare.daily_basic()`                                                 | 权限受限时返回空估值列，不伪造数据          |
| 财务指标  | `tushare.fina_indicator()` / `akshare.stock_financial_abstract_ths()` | 仅使用 `asof_date` 当时可见的口径         |
| 停复牌    | `tushare.suspend()`                                                     | 支持“仅有停牌日、暂无复牌日”的开区间停牌  |

所有原始数据缓存在 `data/raw/<asof_date>/`，中间结果保存在 `data/processed/<asof_date>/`。

## 事件导入要求

推荐优先使用项目内采集脚本生成正式事件文件，而不是直接手工改写 `data/events/*`。当前事件链路分为三层：

- `data/inbox/events_raw/<source>/<batch>/records.jsonl`
  原始抓取留痕，保留标题、正文、发布时间、来源 URL、抓取时间和采集器版本
- `data/staging/events/<source_type>/`
  标准化后的候选事件，配套 `data/staging/events/review_queue.csv` 做人工审阅
- `data/events/<source_type>/`
  仅保存 `review_status=accepted` 的正式事件文件，由主流程直接读取

### 采集脚本用法

```bash
# 1) 抓取原始事件（推荐始终使用 .venv/bin/python，避免本机没有 python 别名）
.venv/bin/python scripts/event_ingest.py collect --source gov_cn --since 2026-04-01 --until 2026-04-07

# 2) 标准化并生成审阅队列
.venv/bin/python scripts/event_ingest.py normalize --source gov_cn --batch 2026-04-07

# 3) 在 data/staging/events/review_queue.csv 中人工确认 review_status

# 4) 发布 accepted 事件到 data/events/policy/
.venv/bin/python scripts/event_ingest.py publish --source-type policy --batch 2026-04-07
```

首批来源支持策略：

- `gov_cn` / `ndrc` / `csrc`
  政策类事件，优先自动抓取官方站点页面
- `cninfo`
  公告类事件，优先接收你导出的 CSV / JSON / URL 清单，再由脚本补抓详情页并标准化
- `eastmoney_industry` / `36kr_manual`
  行业新闻类事件，优先做半自动整理
- `yicai_manual` / `macro_manual`
  宏观/地缘类事件，优先做半自动整理

手动输入型来源支持：

- `--input xxx.csv`
- `--input xxx.json`
- `--input xxx.jsonl`
- `--input xxx.txt`（一行一个 URL）

正式运行前，请将事件文件放入下列目录之一：

- `data/events/policy/`：政策类事件
- `data/events/announcement/`：公司公告类事件
- `data/events/industry/`：行业/技术类事件
- `data/events/macro/`：宏观/地缘类事件

每条事件至少包含以下字段：

- `title`
- `content`
- `published_at`
- `source_name`
- `source_url`

推荐 JSON 结构示例：

```json
[
  {
    "raw_id": "announcement-20260418-demo",
    "title": "事件标题",
    "content": "事件正文，尽量写清楚影响链条、行业和公司。",
    "published_at": "2026-04-18 20:30:00",
    "source_name": "来源名称",
    "source_url": "https://example.com/news"
  }
]
```

### 审阅规则建议

- `accepted`
  标题明确、来源可信、时间在目标窗口内、正文不是噪声、具备明确市场影响意义
- `rejected`
  重复转载、标题过短、无明确事件主体、明显不在比赛窗口内
- `pending`
  正文过短、来源可信度一般、需要你补详情页或二次确认

### 比赛周运行前必须检查

默认配置 `lookback_days: 14`，因此系统只会读取 `asof_date` 往前 14 天内的事件：

- 当运行 `python main_weekly.py --asof 2026-04-20` 时，只会读取 `2026-04-06` 到 `2026-04-20` 的事件
- 当运行 `python main_weekly.py --asof 2026-04-27` 时，只会读取 `2026-04-13` 到 `2026-04-27` 的事件

如果导入事件都早于这些时间窗口，即使字段格式正确，也无法用于正式比赛周运行。

建议优先补充以下类型的近期事件：

- 政策类：政府网、发改委、证监会
- 公司公告类：巨潮资讯、上交所、深交所
- 行业/技术类：行业协会、36 氪、东方财富行业频道
- 宏观/地缘类：财新、第一财经等

### 比赛提交最短命令序列

```bash
cd /Users/xingranya/Downloads/TeddyCup-C-EventDriven
source .venv/bin/activate
export TUSHARE_TOKEN='你的_TOKEN'
python main_weekly.py --asof 2026-04-20
```

第二周提交时运行：

```bash
python main_weekly.py --asof 2026-04-27
```

运行完成后，重点查看：

- `outputs/weekly/<asof_date>/result.xlsx`
- `outputs/weekly/<asof_date>/report.md`
- `outputs/weekly/<asof_date>/final_picks.csv`

## 常用命令速查

### 1. 环境与依赖

```bash
cd /Users/xingranya/Downloads/TeddyCup-C-EventDriven
source .venv/bin/activate
python -V
python -m pip install -r requirements.txt
```

如果你不想激活虚拟环境，也可以直接：

```bash
.venv/bin/python -V
.venv/bin/python -m pip install -r requirements.txt
```

### 2. 政府网事件抓取（当前最先可用）

```bash
# 抓取 2026-04-01 到 2026-04-07 的政策事件原始记录
.venv/bin/python scripts/event_ingest.py collect --source gov_cn --since 2026-04-01 --until 2026-04-07

# 标准化为候选事件，并生成审阅队列
.venv/bin/python scripts/event_ingest.py normalize --source gov_cn --batch 2026-04-07

# 手动审阅
open data/staging/events/review_queue.csv

# 审阅完成后，发布 accepted 事件到 data/events/policy/
.venv/bin/python scripts/event_ingest.py publish --source-type policy --batch 2026-04-07
```

补充说明：

- `collect` 产物：`data/inbox/events_raw/gov_cn/<batch>/records.jsonl`
- `normalize` 产物：
  - `data/staging/events/policy/<batch>_gov_cn.jsonl`
  - `data/staging/events/review_queue.csv`
- `publish` 产物：`data/events/policy/policy_YYYYMM.json`

### 3. 其他来源半自动导入

如果你手头已经有 URL 列表、CSV、JSON、JSONL，可以直接导入后走同一套标准化/发布流程：

```bash
# 例：用 CSV 导入行业新闻
.venv/bin/python scripts/event_ingest.py collect --source eastmoney_industry --since 2026-04-01 --until 2026-04-07 --input /path/to/industry_urls.csv
.venv/bin/python scripts/event_ingest.py normalize --source eastmoney_industry --batch 2026-04-07
.venv/bin/python scripts/event_ingest.py publish --source-type industry --batch 2026-04-07
```

支持的 `--input` 格式：

- `.csv`
- `.json`
- `.jsonl`
- `.txt`（一行一个 URL）

### 4. 比赛周运行命令

第一周：

```bash
cd /Users/xingranya/Downloads/TeddyCup-C-EventDriven
source .venv/bin/activate
export TUSHARE_TOKEN='你的_TOKEN'
python main_weekly.py --asof 2026-04-20
```

第二周：

```bash
python main_weekly.py --asof 2026-04-27
```

如果不激活虚拟环境：

```bash
.venv/bin/python main_weekly.py --asof 2026-04-20
.venv/bin/python main_weekly.py --asof 2026-04-27
```

### 5. 历史回测命令

```bash
source .venv/bin/activate
python main_backtest.py --start 2025-12-08 --end 2025-12-26
```

或：

```bash
.venv/bin/python main_backtest.py --start 2025-12-08 --end 2025-12-26
```

### 6. 测试命令

最小专项测试：

```bash
.venv/bin/python -m unittest tests.test_event_ingest
.venv/bin/python -m unittest tests.test_fetch_data
.venv/bin/python -m unittest tests.test_report_builder
```

完整低成本回归：

```bash
.venv/bin/python -m unittest tests.test_settings tests.test_event_study tests.test_fetch_data tests.test_impact_estimate tests.test_strategy tests.test_result_xlsx tests.test_relation_mining tests.test_report_builder tests.test_workflow tests.test_event_ingest
```

静态编译检查：

```bash
.venv/bin/python -m compileall pipeline scripts tests main_weekly.py main_backtest.py generate_result_xlsx.py
```

### 7. 比赛周前一天的最短操作链

如果你只想知道“比赛前一天最少要敲哪些命令”，按下面走：

```bash
cd /Users/xingranya/Downloads/TeddyCup-C-EventDriven
source .venv/bin/activate
export TUSHARE_TOKEN='你的_TOKEN'

# 先抓取政府网政策事件
python scripts/event_ingest.py collect --source gov_cn --since 2026-04-06 --until 2026-04-20
python scripts/event_ingest.py normalize --source gov_cn --batch 2026-04-20

# 人工审阅 review_queue.csv 后，把 review_status 改成 accepted
python scripts/event_ingest.py publish --source-type policy --batch 2026-04-20

# 然后执行比赛周主流程
python main_weekly.py --asof 2026-04-20
```

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

| 参数                         | 默认值              | 说明                                          |
| ---------------------------- | ------------------- | --------------------------------------------- |
| `market_close_time`        | `15:00:00`        | 用于判断收盘后事件锚点是否顺延                |
| `max_positions`            | 3                   | 最大持仓股票数                                |
| `single_position_max`      | 0.5                 | 单股最大仓位比例                              |
| `single_position_min`      | 0.2                 | 单股最小仓位比例                              |
| `min_listing_days`         | 60                  | 最短上市天数                                  |
| `min_avg_turnover_million` | 80                  | 最低日均成交额（万元）                        |
| `positive_score_threshold` | 0.02                | 入选预测得分门槛                              |
| `estimation_window`        | -60 ~ -6            | 事件研究估计窗口                              |
| `event_window`             | 观察窗 `-1 ~ +10` | 统计口径严格使用 `CAR(0,2)` 与 `CAR(0,4)` |

## 输出内容

| 文件                      | 说明                                                    |
| ------------------------- | ------------------------------------------------------- |
| `result.xlsx`           | 投资决策（事件名、股票代码、资金比例）                  |
| `report.md`             | 周度分析报告（含事件特征、关联图谱、CAR分析、决策理由） |
| `predictions.csv`       | 所有事件-股票组合的预测得分与 CAR                       |
| `company_relations.csv` | 事件-公司关联关系                                       |
| `event_study/`          | 事件研究明细/统计/联合均值CAR                           |
| `kg_visual/`            | 产业链图谱（PNG + HTML）                                |
| `report.md` 新增章节    | 典型事件完整展示、模型性能实验、数据来源与限制          |
| `weekly_summary.csv`    | 回测每周收益与净值                                      |

## 当前验证情况

- 已实际跑通 `python main_weekly.py --asof 2025-12-08`
- 已实际跑通 `python main_weekly.py --asof 2026-03-30`
- 已实际跑通 `python main_backtest.py --start 2025-12-08 --end 2025-12-26`
- 对应输出已生成在 `outputs/weekly/` 与 `outputs/backtest/`

## 基本面过滤规则

Task 4 排除了以下标的：

- ST 股票
- 上市不足 60 天
- 日均成交额低于 80 万元
- PE > 100 或 PE < 0（极值）
- ROE < 5%
- 净利润同比增长 < -20%
- 停牌持续中或当周无有效交易日

## 本轮优化重点

- Task1 的事件分类体系现在会显式读取 `config/config.yaml` 中的 `event_taxonomy`
- Task2 的关联基础权重由 `scoring.association` 控制，主体类型差异通过 `scoring.association_profiles` 调整
- Task3 的主体偏置与空仓阈值已配置化，避免关键参数散落在代码中
- Task4 的仓位分配改为“约束分配 + 最大余数法舍入”，确保资金比例求和稳定为 1
- 报告新增“典型事件完整展示”“模型性能实验”“数据来源与限制”三个章节

## 本轮竞赛对齐修复

这一轮主要不是“加花活”，而是把项目里原本和赛题要求对不齐、或者容易让提交结果失真的地方收紧：

| 对齐点 | 之前的问题 | 现在的修复 |
| --- | --- | --- |
| Task 1 分类体系 | `event_taxonomy` 在配置、模型、代码里多处各管各的，周度流程也没有真正把配置传进 Task1 | `workflow.py` 现在显式把 `config.event_taxonomy` 注入 `run_event_identification()`，分类体系真正变成配置驱动 |
| Task 2 关联强度 | `config.yaml` 里有 `scoring.association`，但代码实际用的是硬编码权重，改配置不生效 | 关联基础权重改为读取 `scoring.association`，主体差异通过 `scoring.association_profiles` 做配置化调整 |
| Task 3 逻辑链与性能实验 | 报告里没有“典型事件完整展示”和“模型性能实验”，和赛题 Task2/Task3 的展示要求不齐 | `report_builder.py` 新增“典型事件完整展示”“模型性能实验”“数据来源与限制”三节，报告内容更贴题 |
| Task 3 评估可信度 | 模型性能实验原先会读 `outputs/weekly` 下所有目录，重跑旧周时会把未来周结果混进来 | 性能实验现在只统计 `<= asof_date` 的周目录，避免前视偏差 |
| Task 4 仓位约束 | 仓位分配曾存在幽灵配置 `position_floor_new` 和补差式归一化，容易和配置不一致 | 仓位分配改为“约束分配 + 最大余数法舍入”，严格使用 `single_position_min/single_position_max`，并保证资金比例求和为 1 |
| 提交报告可信度 | 增强阶段失败时，报告仍会列出并不存在的图表/统计文件名 | 报告现在只列真实存在的增强产物，不再误报未生成文件 |
| 数据链路稳定性 | 导入事件时单条坏记录可能直接中断流程，影响比赛周交付 | 现在改为记录 warning 并跳过异常记录，只有完全没有有效事件时才整体失败 |
| 交易链路可解释性 | 交易日历降级、空仓阈值、主体偏置等关键决策散落在代码里，难以说明 | 这些行为被配置化并写进报告/日志，便于说明“本周为什么这样选、数据有没有降级” |

如果只用一句话总结，这轮修复把项目从“主流程能跑”推进到了“更接近赛题要求、报告更能交代清楚、提交结果更不容易失真”。
