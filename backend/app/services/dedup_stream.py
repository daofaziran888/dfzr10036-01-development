"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class DedupStreamService(BaseModuleService):
    MODULE = "dedup_stream"
    ENTITY = "去重窗口"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["窗口编号", "数据流", "去重键", "窗口宽度", "去重算法", "误杀率", "内存占用", "最近命中"]
    REQUIRED_FIELDS = ["窗口编号", "数据流", "去重键"]
    STATUS_ORDER = ["草稿", "去重中", "窗口溢出", "已停用"]
    ACTION_RULES = {"启效去重": "去重中", "扩窗处理": "窗口溢出", "停用去重": "已停用"}
    NEGATIVE_ACTIONS = ["停用去重"]
