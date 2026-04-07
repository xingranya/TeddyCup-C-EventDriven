# 更新日志

## 2026-04-07 — [竞赛对齐修复](2026-04-07-Competition-Alignment)

### 与赛题要求对齐的关键修复

- **Task 1 配置真正生效**：`workflow.py` 现在显式把 `config.event_taxonomy` 传入 `run_event_identification()`，事件分类体系不再是“改了配置但主流程没用”
- **Task 2 关联权重配置化**：`task2_relation_mining.py` 改为读取 `scoring.association`，并支持通过 `scoring.association_profiles` 调整不同主体类型的关联结构
- **Task 3 报告补齐题目要求**：`report_builder.py` 新增“典型事件完整展示”“模型性能实验”“数据来源与限制”，自动报告更贴近竞赛要求
- **性能实验去前视偏差**：模型性能实验只统计 `<= asof_date` 的周目录，重跑旧周时不会混入未来周结果
- **Task 4 仓位分配重构**：仓位分配改成“约束分配 + 最大余数法舍入”，严格使用 `single_position_min/single_position_max` 并保证资金比例求和为 1
- **报告产物名纠偏**：增强阶段失败时，报告只显示真实存在的文件，不再误报未生成的图表或统计文件
- **导入事件容错增强**：单条坏记录改为 warning + 跳过，只有完全无有效事件时才整批失败，更适合比赛周临近提交时使用

## 2026-04-02 — 全面优化与竞赛适配升级

### 关键缺陷修复

- **修复 `historical_co_move` 伪随机问题**：`task2_relation_mining.py` 中的 `compute_historical_co_move()` 从伪随机种子改为基于真实价格数据的皮尔逊相关系数计算，关联强度指标现在具有真实市场意义
- **全局容错机制**：`workflow.py` 每个 pipeline 步骤添加 try-except 容错，单步失败降级继续，不再导致全流程中断
- **回测交易成本**：`backtest.py` 新增佣金(0.1%)和滑点(0.05%)扣除，回测结果更贴近实盘
- **IndexError 防护**：`task4_strategy.py` 的 `build_pick_reason` 添加空值检查
- **Dataclass 兜底构造修正**：修复 `EventStudyArtifacts` 和 `IndustryChainArtifacts` 异常路径的参数不匹配
- **行业标签映射**：`industry_chain_enhanced.py` 和 `task2_relation_mining.py` 添加 `INDUSTRY_LABEL_MAP`，确保 "军工类事件" 等标签正确映射到 `industry_relation_map.json` 的键

### 事件识别增强 (Task 1)

- `EVENT_TAXONOMY` 从硬编码迁移至 `config/config.yaml`，支持动态扩展
- 行业覆盖从 5 类扩展至 10 类（新增消费、医药、金融、地产、农业）
- 聚类算法增加标题 Jaccard 相似度维度（阈值 0.35）
- `confidence_score` 改用 logistic 非线性变换提升区分度
- 事件名称选择增加长度过滤（8-60 字符）

### 关联挖掘改进 (Task 2)

- `compute_business_match` 实现渐进式匹配（完全匹配 0.30 + 部分匹配 0.15）
- `compute_industry_overlap` 引入 `INDUSTRY_GROUP_MAP` 申万行业映射
- 关联权重根据事件驱动主体类型动态调整（`WEIGHT_PROFILES` 5 种配置）
- `industry_relation_map.json` 新增消费、医药、金融、农业四个行业产业链映射

### 影响预测提升 (Task 3)

- 基本面评分支持行业中位数相对评分（`sector_median_pe/pb/roe` 参数），保持向后兼容
- CAR 缩放因子从固定 0.18 改为基于历史 CAR 波动率的自适应计算（范围 0.10-0.25）
- `event_study_enhanced.py` 新增 t-stat 和 p-value 统计检验列（依赖 scipy）
- 逻辑链条输出包含具体数值（热度、关联度、预期 CAR、综合评分）

### 策略优化 (Task 4)

- 选股评分融入 5 日动量因子（权重 15%）
- 兜底池采用显式加权公式（流动性 0.4 + 置信度 0.35 + 安全性 0.25）
- 仓位分配增加置信度加权逻辑，仓位下限放宽至 15%
- 新增空仓保护：所有标的预期收益为负时不交易

### 报告与输出完善

- `report_builder.py` 新增研究方法论详细说明章节
- 新增每只选中股票的完整投资决策推理链
- `generate_result_xlsx.py` 格式确认符合竞赛三列要求

### 依赖更新

- `requirements.txt` 新增 `scipy>=1.12.0`

## 2026-04-02

### 数据链路修复

### 数据链路修复

- **竞赛模式切换**：正式运行不再使用仓库样例或伪随机行情生成结果，改为真实数据链路优先
- **事件输入标准化**：新增 `data/events/policy|announcement|industry|macro` 导入目录，事件表统一输出 `published_at`、`source_name`、`source_url`、`entity_candidates`、`raw_id`、`content_hash`
- **交易日历兜底**：`fetch_trading_calendar()` 在 Tushare 权限不足时改走公开交易日日历
- **股票池收缩**：`fetch_stock_universe()` 在接口频控受限时回退到本地竞赛候选池，并结合事件文本与产业映射收缩候选股票范围
- **财务与基准兜底**：估值、财务和指数权限不足时改为公开源或市场代理口径，不再直接中断整条流程

### 正确性修复

- **事件锚点修复**：`event_study_enhanced.py` 按完整 `published_at` 与收盘时点确定锚点，收盘后事件顺延到下一交易日
- **CAR 统计口径修复**：统计表严格输出 `AR(+1)`、`CAR(0,2)`、`CAR(0,4)`，不再把 `day_offset=-1` 误计入汇总
- **停牌开区间处理**：`is_tradeable()` 将“只有停牌日、没有复牌日”的记录视为持续停牌
- **周内买卖日修复**：周二休市时顺延到当周后续首个交易日买入，卖出日改为当周最后一个交易日

### 验证结果

- 已跑通 `main_weekly.py --asof 2025-12-08`
- 已跑通 `main_weekly.py --asof 2026-03-30`
- 已跑通 `main_backtest.py --start 2025-12-08 --end 2025-12-26`
- 新增 8 个单元测试，覆盖事件锚点、CAR 口径、停牌区间、交易日顺延、财务快照与 `result.xlsx` 输出格式

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
