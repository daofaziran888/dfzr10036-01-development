"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class AlertRuleService(BaseModuleService):
    MODULE = "alert_rule"
    ENTITY = "告警策略"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["策略编号", "监控指标", "告警条件", "告警级别", "通知渠道", "聚合窗口", "静默时段", "触发次数"]
    REQUIRED_FIELDS = ["策略编号", "监控指标", "告警条件"]
    STATUS_ORDER = ["草稿", "监控中", "已触发", "已静默"]
    ACTION_RULES = {"启用监控": "监控中", "确认告警": "已触发", "静默告警": "已静默"}
    NEGATIVE_ACTIONS = []
