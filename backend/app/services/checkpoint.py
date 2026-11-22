"""检查点服务：断点恢复时触发水位重置和回填任务。"""
from __future__ import annotations

import re
from typing import Any

from app.services.base import BaseModuleService
from app.services.backfill_coverage import backfill_coverage_service
from app.store import store


class CheckpointService(BaseModuleService):
    MODULE = "checkpoint"
    ENTITY = "检查点"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["检查点编号", "数据流", "保存位点", "保存时间", "恢复状态", "丢失行数", "回放代价", "恢复耗时"]
    REQUIRED_FIELDS = ["检查点编号", "数据流", "保存位点"]
    STATUS_ORDER = ["已保存", "恢复中", "已恢复", "恢复失败"]
    ACTION_RULES = {"触发恢复": "恢复中", "确认恢复": "已恢复", "标记失败": "恢复失败"}
    NEGATIVE_ACTIONS = []

    # 断点恢复入口注入在途回填批次时需要透传的扩展字段
    INFLIGHT_FIELDS = ["在途区间", "执行端", "连接状态", "在途批次"]

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        entry, missing = super().create_entry(values)
        if entry is not None:
            for field in self.INFLIGHT_FIELDS:
                if field in values:
                    entry[field] = values.get(field)
        return entry, missing

    def trigger_watermark_reset_and_backfill(self, entry_id: int) -> dict[str, Any]:
        """断点恢复后触发水位重置和回填任务。

        数据流链路: checkpoint -> watermark_mgr + backfill_run
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None

        tag = str(entry.get("数据流", ""))
        results: dict[str, Any] = {"watermark": None, "backfill": None, "inflight_injection": None}

        # 断点恢复入口注入在途回填批次（字段「在途区间」如 111-120，
        # 「执行端」给出持有方，「连接状态」区分失联占用）。
        interval = str(entry.get("在途区间", "")).strip()
        match = re.fullmatch(r"\s*(\d+)\s*-\s*(\d+)\s*", interval)
        if match:
            lo, hi = int(match.group(1)), int(match.group(2))
            executor = str(entry.get("执行端", "执行端")).strip() or "执行端"
            connected = str(entry.get("连接状态", "")).strip() not in ("失联", "断开", "false", "False")
            batch_no = str(entry.get("在途批次", "")).strip() or None
            results["inflight_injection"] = backfill_coverage_service.register_inflight_batch(
                tag, lo, hi, executor=executor, batch_no=batch_no,
                connected=connected, source_ref=str(entry.get("检查点编号", "checkpoint")),
            )

        if entry.get("status") == "已恢复":
            # 重置水位
            for wm in store.rows("watermark_mgr"):
                if str(wm.get("数据流", "")) == tag:
                    wm["当前位点"] = str(entry.get("保存位点", ""))
                    wm["status"] = "推进中"
                    wm["pending"] = True
                    wm["abnormal"] = False
                    results["watermark"] = wm
                    break

            # 如果有丢失行数，创建回填任务
            loss = int(entry.get("丢失行数", 0))
            if loss > 0:
                bf_rows = store.rows("backfill_run")
                new_bf_id = max((int(r.get("id", 0)) for r in bf_rows), default=0) + 1
                backfill = {
                    "id": new_bf_id,
                    "任务编号": f"BACK-{new_bf_id:04d}",
                    "目标表": tag,
                    "起始位点": str(entry.get("保存位点", "")),
                    "截止位点": "latest_binlog",
                    "回填模式": "批量并行回填",
                    "并行粒度": "按分区",
                    "当前进度": "0%",
                    "回填速率": "0 eps",
                    "status": "回填中",
                    "pending": True,
                    "abnormal": False,
                }
                bf_rows.append(backfill)
                results["backfill"] = backfill

        return results