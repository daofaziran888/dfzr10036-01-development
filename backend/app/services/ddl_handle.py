"""DDL适配服务：DDL变更成功后级联影响解析规则和Schema演进。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DdlHandleService(BaseModuleService):
    MODULE = "ddl_handle"
    ENTITY = "DDL适配"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["适配编号", "源端", "DDL类型", "影响表", "兼容策略", "自动应用", "人工确认", "处理状态"]
    REQUIRED_FIELDS = ["适配编号", "源端", "DDL类型"]
    STATUS_ORDER = ["待处理", "适配中", "已应用", "需人工"]
    ACTION_RULES = {"自动应用": "适配中", "人工确认": "已应用", "忽略变更": "需人工"}
    NEGATIVE_ACTIONS = ["忽略变更"]

    def trigger_binlog_and_schema_evo(self, entry_id: int) -> dict[str, Any] | None:
        """DDL适配成功后，触发binlog解析规则调整和Schema演进记录。

        数据流链路: ddl_handle -> binlog_parse + schema_evo
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "已应用":
            return None

        tag = str(entry.get("源端", ""))
        table = str(entry.get("影响表", ""))
        ddl_type = str(entry.get("DDL类型", ""))
        results: dict[str, Any] = {"binlog": None, "schema": None}

        # 调整binlog解析规则
        for bp in store.rows("binlog_parse"):
            if str(bp.get("源端", "")) == tag:
                bp["DDL处理"] = ddl_type
                bp["字段映射"] = f"自动适配{ddl_type}@{table}"
                bp["status"] = "解析中"
                bp["pending"] = True
                bp["abnormal"] = False
                results["binlog"] = bp
                break

        # 创建Schema演进记录
        se_rows = store.rows("schema_evo")
        new_se_id = max((int(r.get("id", 0)) for r in se_rows), default=0) + 1
        schema = {
            "id": new_se_id,
            "记录编号": f"SCHE-{new_se_id:04d}",
            "数据流": tag,
            "变更列": f"DDL变更-{ddl_type}",
            "列变更前": "预变更前schema",
            "列变更后": f"应用{ddl_type}后schema",
            "兼容模式": str(entry.get("兼容策略", "向前兼容")),
            "下游影响": f"影响表{table}及下游消费",
            "演进时间": "2026-11-18 10:44:00",
            "status": "已捕获",
            "pending": True,
            "abnormal": False,
        }
        se_rows.append(schema)
        results["schema"] = schema

        return results