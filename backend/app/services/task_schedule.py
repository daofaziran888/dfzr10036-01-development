"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class TaskScheduleService(BaseModuleService):
    MODULE = "task_schedule"
    ENTITY = "调度计划"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["计划编号", "关联链路", "Cron表达式", "时区设置", "前置检查", "失败通知", "最大并发", "下次执行"]
    REQUIRED_FIELDS = ["计划编号", "关联链路", "Cron表达式"]
    STATUS_ORDER = ["待激活", "执行中", "已完成", "已停用"]
    ACTION_RULES = {"激活计划": "执行中", "立即执行": "已完成", "停用计划": "已停用"}
    NEGATIVE_ACTIONS = ["停用计划"]
