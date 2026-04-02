from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class RunContext:
    """一次运行所需的上下文信息。"""

    asof_date: date
    project_root: Path
    output_dir: Path
    raw_dir: Path
    processed_dir: Path


@dataclass(slots=True)
class AppConfig:
    """项目配置。"""

    raw: dict[str, Any]

    @property
    def timezone(self) -> str:
        return str(self.raw["project"]["timezone"])

    @property
    def market_close_time(self) -> time:
        return time.fromisoformat(str(self.raw["project"]["market_close_time"]))

    @property
    def lookback_days(self) -> int:
        return int(self.raw["data"]["lookback_days"])

    @property
    def benchmark_code(self) -> str:
        return str(self.raw["data"]["benchmark_code"])

    @property
    def trading_calendar_source(self) -> str:
        return str(self.raw["data"]["trading_calendar_source"])

    @property
    def stock_whitelist_path(self) -> str:
        return str(self.raw["data"].get("stock_whitelist_path", "") or "")

    @property
    def stock_blacklist_path(self) -> str:
        return str(self.raw["data"].get("stock_blacklist_path", "") or "")

    @property
    def tushare_token(self) -> str:
        return str(self.raw["tushare"]["token"])

    @property
    def qstock_enabled(self) -> bool:
        return bool(self.raw.get("events", {}).get("qstock_enabled", False))

    @property
    def event_import_paths(self) -> dict[str, str]:
        return {
            str(key): str(value)
            for key, value in self.raw.get("events", {}).get("import_paths", {}).items()
        }

    @property
    def initial_capital(self) -> float:
        return float(self.raw["project"]["initial_capital"])

    @property
    def max_positions(self) -> int:
        return int(self.raw["strategy"]["max_positions"])

    @property
    def position_cap(self) -> float:
        return float(self.raw["strategy"]["single_position_max"])

    @property
    def position_floor(self) -> float:
        return float(self.raw["strategy"]["single_position_min"])

    @property
    def min_listing_days(self) -> int:
        return int(self.raw["strategy"]["min_listing_days"])

    @property
    def min_avg_turnover_million(self) -> float:
        return float(self.raw["strategy"]["min_avg_turnover_million"])

    @property
    def positive_score_threshold(self) -> float:
        return float(self.raw["strategy"]["positive_score_threshold"])

    @property
    def event_taxonomy(self) -> dict[str, dict[str, list[str]]]:
        """事件分类体系配置，包含 duration_type, subject_type, predictability, industry_type 等维度。"""
        default_taxonomy = {
            "duration_type": {
                "脉冲型事件": ["突发", "爆炸", "冲突", "坠毁", "事故", "紧急", "速报", "快讯", "突然", "骤然", "猝然"],
                "长尾型事件": ["规划", "战略", "改革", "转型", "长期", "五年", "十四五", "十五五", "远景", "纲要", "路线图"],
                "中期型事件": ["季度", "半年", "年度", "阶段", "周期", "中期", "短期目标"],
            },
            "subject_type": {
                "政策类事件": ["政策", "方案", "规划", "意见", "通知", "办法", "条例", "法规", "决定", "指导", "纲要", "白皮书", "实施细则", "管理办法", "监管", "审批"],
                "公司类事件": ["公告", "业绩", "重组", "并购", "增持", "减持", "回购", "分红", "股权", "定增", "配股", "解禁", "质押", "担保", "诉讼", "违规", "IPO", "上市", "退市"],
                "行业类事件": ["行业", "产业", "技术突破", "新产品", "创新", "首发", "量产", "商用", "落地", "试点", "示范", "应用", "渗透率", "市占率", "产能"],
                "宏观类事件": ["GDP", "CPI", "PMI", "利率", "降息", "加息", "降准", "汇率", "贸易", "关税", "通胀", "通缩", "就业", "失业率", "财政", "货币政策", "央行", "美联储"],
                "地缘类事件": ["战争", "冲突", "制裁", "封锁", "军演", "导弹", "空袭", "领土", "外交", "紧张局势", "对抗", "联盟", "北约", "台海"],
            },
            "predictability": {
                "突发型事件": ["突发", "意外", "紧急", "黑天鹅", "不可预测", "震惊", "出乎意料", "罕见", "首次"],
                "预披露型事件": ["预告", "预计", "预期", "计划", "拟", "将", "即将", "有望", "筹划", "草案", "征求意见"],
            },
            "industry_type": {
                "军工类事件": ["军工", "国防", "武器", "导弹", "战斗机", "航母", "军舰", "雷达", "卫星", "北斗", "航天", "火箭", "无人机", "军民融合"],
                "科技类事件": ["AI", "人工智能", "芯片", "半导体", "算力", "大模型", "机器人", "量子", "5G", "6G", "物联网", "云计算", "区块链", "数据中心", "光刻", "GPU", "自动驾驶", "智能驾驶"],
                "新能源类事件": ["新能源", "光伏", "风电", "储能", "氢能", "锂电", "电池", "充电桩", "碳中和", "碳达峰", "绿电", "核电", "太阳能"],
                "低空类事件": ["低空", "eVTOL", "飞行汽车", "通航", "无人机", "空中交通", "适航", "空域"],
                "消费类事件": ["消费", "零售", "电商", "品牌", "白酒", "食品", "餐饮", "旅游", "免税", "奢侈品", "家电", "汽车消费"],
                "医药类事件": ["医药", "创新药", "生物医药", "疫苗", "医疗器械", "集采", "带量采购", "临床试验", "FDA", "CRO", "CDMO", "基因", "细胞治疗"],
                "金融类事件": ["银行", "保险", "券商", "基金", "信托", "金融科技", "数字货币", "数字人民币", "注册制", "全面注册制"],
                "地产类事件": ["房地产", "楼市", "限购", "限贷", "房贷", "利率下调", "保交楼", "城中村", "旧改"],
                "农业类事件": ["农业", "种业", "粮食安全", "转基因", "化肥", "农药", "养殖", "猪价", "粮价"],
                "业绩类事件": ["业绩预告", "业绩快报", "年报", "季报", "盈利", "亏损", "扭亏", "高增长", "超预期", "不及预期"],
            },
        }
        return self.raw.get("event_taxonomy", default_taxonomy)
