"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class BackfillRunService(BaseModuleService):
    MODULE = "backfill_run"
    ENTITY = "回填任务"
    OPERATION_TERM = "回填"
    LIST_FIELDS = ["任务编号", "目标表", "起始位点", "截止位点", "回填模式", "并行粒度", "当前进度", "回填速率"]
    REQUIRED_FIELDS = ["任务编号", "目标表", "起始位点"]
    STATUS_ORDER = ["待执行", "回填中", "已完成", "速率过缓"]
    ACTION_RULES = {"启动回填": "回填中", "暂停回填": "已完成", "标记过缓": "速率过缓"}
    NEGATIVE_ACTIONS = []
