"""CDC源端服务：配置源端连接后会级联触发下游binlog解析规则生成。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CdcSourceService(BaseModuleService):
    MODULE = "cdc_source"
    ENTITY = "链路源端"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["源端编号", "数据库类型", "连接地址", "日志协议", "监听表", "包含列", "排除列", "连接状态"]
    REQUIRED_FIELDS = ["源端编号", "数据库类型", "连接地址"]
    STATUS_ORDER = ["待连接", "监听中", "连接断开", "已停用"]
    ACTION_RULES = {"建立连接": "监听中", "断开重连": "连接断开", "停用源端": "已停用"}
    NEGATIVE_ACTIONS = ["停用源端"]

    def trigger_downstream_binlog(self, entry_id: int) -> dict[str, Any] | None:
        """CDC源端连接建立后，触发下游 binlog_parse 自动生成解析规则。

        数据流链路: cdc_source -> binlog_parse
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "监听中":
            return None

        # 检查是否已有对应下游规则，避免重复创建
        binlog_rows = store.rows("binlog_parse")
        existing = [r for r in binlog_rows if str(r.get("源端", "")) == str(entry.get("源端编号", ""))]
        if existing:
            # 已有下游规则，恢复为解析中
            existing[0]["status"] = "解析中"
            existing[0]["pending"] = True
            existing[0]["abnormal"] = False
            return existing[0]

        new_id = max((int(r.get("id", 0)) for r in binlog_rows), default=0) + 1
        downstream = {
            "id": new_id,
            "规则编号": f"BINL-{new_id:04d}",
            "源端": str(entry.get("源端编号", "")),
            "日志格式": f"ROW格式-{entry.get('日志协议', 'mixed')}",
            "事件过滤": "INSERT,UPDATE,DELETE",
            "字段映射": f"全列映射@{entry.get('监听表', 'default')}",
            "DDL处理": "自动同步",
            "大事务拆分": "启用",
            "解析速率": "5000 eps",
            "status": "解析中",
            "pending": True,
            "abnormal": False,
        }
        binlog_rows.append(downstream)
        return downstream