"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class StreamAuditService(BaseModuleService):
    MODULE = "stream_audit"
    ENTITY = "审计记录"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["记录编号", "数据流", "操作类型", "影响表", "操作人", "操作时间", "审计结论", "合规标记"]
    REQUIRED_FIELDS = ["记录编号", "数据流", "操作类型"]
    STATUS_ORDER = ["已记录", "待复核", "已复核", "已归档"]
    ACTION_RULES = {"提交复核": "待复核", "确认复核": "已复核", "归档记录": "已归档"}
    NEGATIVE_ACTIONS = []
