"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class StreamHealthService(BaseModuleService):
    MODULE = "stream_health"
    ENTITY = "健康评估"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["评估编号", "数据流", "健康分数", "风险项", "修复建议", "评估时间", "下次评估", "评估状态"]
    REQUIRED_FIELDS = ["评估编号", "数据流", "健康分数"]
    STATUS_ORDER = ["待评估", "评估中", "已出分", "需修复"]
    ACTION_RULES = {"发起评估": "评估中", "确认评分": "已出分", "指派修复": "需修复"}
    NEGATIVE_ACTIONS = []
