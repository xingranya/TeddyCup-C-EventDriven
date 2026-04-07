# 系统架构

## 整体流水线

```
run_weekly_pipeline (workflow.py)
│
├── fetch_data → FetchArtifacts
│   ├── news_df           # 新闻与公告
│   ├── stock_df          # 股票池
│   ├── price_df          # 个股日频行情
│   ├── benchmark_df      # 沪深300基准指数
│   └── trading_calendar  # 真实交易日历
│
├── task1_event_identify → event_df
│   └── 四维分类 + 五大量化特征 + 聚类成员追踪
│
├── task2_relation_mining → relation_df + kg_visual/
│   └── 四维关联评分 + 知识图谱
│
├── task3_impact_estimate → prediction_df
│   ├── 市场模型回归（估计窗口 -60 ~ -6 天）
│   ├── 异常收益 CAR 计算（观察窗 -1 ~ +10）
│   └── fundamental_score 基本面得分接入
│
├── event_study_enhanced → EventStudyArtifacts
│   ├── event_study_detail.csv   # 每事件每日 AR/CAR 明细
│   ├── event_study_stats.csv    # 事件汇总统计（AR(+1)、CAR(0,2)、CAR(0,4)）
│   └── joint_mean_car.png       # 联合均值 CAR 图
│
├── industry_chain_enhanced → IndustryChainArtifacts
│   ├── industry_chain_relations.csv
│   ├── industry_chain_graph.png # 三层链式图谱
│   └── industry_chain_graph.html
│
├── task4_strategy → final_picks
│   ├── pass_basic_filter     # ST/上市天数/成交额
│   ├── pass_fundamental_filter # PE/ROE/净利润增长
│   ├── is_tradeable         # 停牌持续状态/周内有效买卖日过滤
│   └── allocate_positions    # 资金分配（≤3股）
│
└── report_builder → report.md + result.xlsx
```

## 本轮架构收口点

- `workflow.py` 现在会把 `config.event_taxonomy` 显式传给 Task 1，保证分类体系与配置一致
- `task2_relation_mining.py` 的关联基础权重改为读取配置，不再和 `config.yaml` 脱节
- `task3_impact_estimate.py` 在预测输出中补齐 `subject_type`、`relation_type`、`association_score` 等报告所需字段
- `report_builder.py` 新增典型事件展示、模型性能实验和数据来源限制章节
- 报告只会列出真实存在的增强产物文件，增强阶段失败时不会误报文件名

## 关键配置

| 参数 | 值 | 说明 |
|---|---|---|
| `max_positions` | 3 | 最大持仓股票数 |
| `single_position_max` | 0.5 | 单股最大仓位 |
| `single_position_min` | 0.2 | 单股最小仓位 |
| `min_listing_days` | 60 | 最短上市天数 |
| `min_avg_turnover_million` | 80 | 最低日均成交额（万） |
| `positive_score_threshold` | 0.02 | 预测得分入选门槛 |
| `estimation_window` | -60 ~ -6 | 事件研究估计窗口 |
| `event_window` | -1 ~ +10 | 事件观察窗口 |
| `stat_window` | 0 ~ +4 | 统计严格使用 `CAR(0,2)` 与 `CAR(0,4)` |
