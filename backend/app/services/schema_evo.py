"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class SchemaEvoService(BaseModuleService):
    MODULE = "schema_evo"
    ENTITY = "演进记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录编号", "数据流", "变更列", "列变更前", "列变更后", "兼容模式", "下游影响", "演进时间"]
    REQUIRED_FIELDS = ["记录编号", "数据流", "变更列"]
    STATUS_ORDER = ["已捕获", "兼容通过", "存在风险", "已阻断"]
    ACTION_RULES = {"提交校验": "兼容通过", "标记风险": "存在风险", "阻断演进": "已阻断"}
    NEGATIVE_ACTIONS = []
