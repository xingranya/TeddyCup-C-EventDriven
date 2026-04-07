# TeddyCup-C-EventDriven

面向**泰迪杯 C 题"事件驱动型股市投资策略构建"**的完整解决方案实现。

## 核心能力

本项目实现了从事件感知到策略落地的全链条分析，覆盖竞赛要求的四个阶段：

| 阶段 | 任务 | 核心模块 |
|---|---|---|
| Task 1 | 事件识别与分类 | `task1_event_identify.py` |
| Task 2 | 事件关联公司挖掘 | `task2_relation_mining.py` |
| Task 3 | 事件影响预测与逻辑链条 | `task3_impact_estimate.py` + `event_study_enhanced.py` |
| Task 4 | 投资策略构建 | `task4_strategy.py` |

## 快速开始

```bash
# 激活虚拟环境
source .venv/bin/activate

# 配置 Tushare 凭证
export TUSHARE_TOKEN=你的_TOKEN

# 周度实测（竞赛提交用）
python main_weekly.py --asof 2026-04-20

# 历史回测
python main_backtest.py --start 2025-12-08 --end 2025-12-26
```

## 竞赛时间线

- **第一周提交**：2026-04-20 ~ 2026-04-21
- **第二周提交**：2026-04-27 ~ 2026-04-28

## 最近更新

- [2026-04-07 — 竞赛对齐修复](2026-04-07-Competition-Alignment)

这次更新重点把“能跑”进一步收紧到“更贴题、更可信”：

- Task 1 的事件分类体系现在真正从 `config/config.yaml` 注入主流程
- Task 2 的关联权重改为配置驱动，主体类型差异通过 profile 调整
- 周报新增“典型事件完整展示”“模型性能实验”“数据来源与限制”
- Task 4 仓位分配改为严格受上下限约束并保证资金比例求和为 1
- 模型性能实验排除了未来周输出，避免前视偏差
- 增强阶段失败时，报告不会再误报不存在的图表/统计文件

## 项目结构

```
pipeline/
├── fetch_data.py           # 数据采集（新闻/行情/财务/停复牌）
├── task1_event_identify.py # 事件识别与分类
├── task2_relation_mining.py # 事件-公司关联关系
├── task3_impact_estimate.py # CAR 影响预测
├── event_study_enhanced.py   # 事件研究增强（标准 CAR 图）
├── industry_chain_enhanced.py # 产业链图谱
├── task4_strategy.py        # 策略构建与仓位分配
├── report_builder.py         # 周报生成
├── workflow.py              # 完整流水线编排
├── backtest.py              # 回测引擎
└── models.py               # 数据模型
```

## 当前运行约束

- 默认使用 `data/events/*` 中的正式事件导入文件
- Tushare 权限不足时，交易日历和财务部分会切换到公开源或代理口径
- `result.xlsx` 固定输出三列：事件名称、标的（股票）代码、资金比例
