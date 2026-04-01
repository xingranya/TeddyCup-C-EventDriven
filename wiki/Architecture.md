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
│   ├── financial_df      # 公司财务指标（PE/PB/ROE/净利润增长率）
│   └── suspend_resume_df # 停复牌信息
│
├── task1_event_identify → event_df
│   └── 四维分类 + 五大量化特征
│
├── task2_relation_mining → relation_df + kg_visual/
│   └── 四维关联评分 + 知识图谱
│
├── task3_impact_estimate → prediction_df
│   ├── 市场模型回归（估计窗口 -60 ~ -6 天）
│   ├── 异常收益 CAR 计算（事件窗口 -1 ~ +4 天）
│   └── fundamental_score 基本面得分接入
│
├── event_study_enhanced → EventStudyArtifacts
│   ├── event_study_detail.csv   # 每事件每日 AR/CAR 明细
│   ├── event_study_stats.csv    # 事件汇总统计
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
│   ├── is_tradeable         # 停复牌/涨跌停过滤
│   └── allocate_positions    # 资金分配（≤3股）
│
└── report_builder → report.md + result.xlsx
```

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
| `event_window` | -1 ~ +4 | 事件窗口 |
