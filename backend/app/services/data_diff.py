"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class DataDiffService(BaseModuleService):
    MODULE = "data_diff"
    ENTITY = "比对任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务编号", "源端", "目标端", "比对字段", "差异类型", "差异行数", "修复建议", "比对时间"]
    REQUIRED_FIELDS = ["任务编号", "源端", "目标端"]
    STATUS_ORDER = ["待执行", "比对中", "一致", "存在差异"]
    ACTION_RULES = {"启动比对": "比对中", "确认一致": "一致", "标记差异": "存在差异"}
    NEGATIVE_ACTIONS = []
