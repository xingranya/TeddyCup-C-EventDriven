# 事件影响预测（Task 3）

## 事件研究法

### 窗口定义

- **估计窗口**：事件日前第 60 ~ 第 6 个交易日（用于估计市场模型 β）
- **事件窗口**：事件日前第 1 天 ~ 后第 4 天（用于计算异常收益）

### 市场模型

使用单因子市场模型：
```
R_stock = α + β × R_benchmark + ε
Expected Return = α + β × R_benchmark
AR = Actual Return - Expected Return
CAR = Σ AR（累计异常收益）
```

## 预测模型

`expected_car_4d` 的计算融合了多个因子：

```
expected_car_4d = sentiment_direction
                × event_score(0.30/0.35/0.20/0.15 加权)
                × association_score
                × subject_multiplier(地缘1.15/政策1.08/公司1.12/宏观0.92)
                × (0.55 + market_state)
                × max(0.15, 1 - residual_risk)
                × (1 + fundamental_score × 0.15)   # 基本面增强
                × 0.18
```

其中 `fundamental_score` 由 PE/PB/ROE/净利润增长率标准化后加权计算。

## 事件研究增强模块

`event_study_enhanced.py` 输出：

- `event_study_detail.csv` — 每个事件-股票组合的每日 AR/CAR 明细
- `event_study_stats.csv` — 按事件聚合的统计量（样本数、均值CAR、标准差、正收益占比）
- `joint_mean_car.png` — 按正向/负向事件分组的均值 CAR 曲线

## 可解释逻辑链

每个预测结果附带逻辑链文本，格式为：

> "{事件}发生后，首先强化了{行业}相关需求或情绪预期；随后通过{关联类型}传导至{公司}所处业务环节；市场预期其短期股价会出现{正向/负向}响应。"
