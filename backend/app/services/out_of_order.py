"""乱序处理服务：乱序策略生效后级联延迟监控和断点恢复。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class OutOfOrderService(BaseModuleService):
    MODULE = "out_of_order"
    ENTITY = "乱序策略"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["策略编号", "数据流", "时间字段", "允许迟到", "乱序窗口", "纠错模式", "丢弃行数", "修正行数"]
    REQUIRED_FIELDS = ["策略编号", "数据流", "时间字段"]
    STATUS_ORDER = ["草稿", "生效中", "超窗口丢弃", "已停用"]
    ACTION_RULES = {"启效策略": "生效中", "扩窗容忍": "超窗口丢弃", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    def trigger_lag_monitor_update(self, entry_id: int) -> dict[str, Any] | None:
        """乱序策略生效后，更新lag_monitor中的乱序相关延迟指标。

        数据流链路: out_of_order -> lag_monitor
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "生效中":
            return None

        tag = str(entry.get("数据流", ""))
        for lm in store.rows("lag_monitor"):
            if str(lm.get("数据流", "")) == tag:
                lm["状态"] = "正常"
                lm["延迟秒数"] = f"乱序纠错中: {entry.get('修正行数', 0)} 行"
                return lm

        # 无现有lag_monitor，创建新记录
        lm_rows = store.rows("lag_monitor")
        new_id = max((int(r.get("id", 0)) for r in lm_rows), default=0) + 1
        new_lm = {
            "id": new_id,
            "指标编号": f"LAG_-{new_id:04d}",
            "数据流": tag,
            "源端位点": f"binlog@{tag}",
            "处理位点": f"乱序窗口{entry.get('乱序窗口', '未知')}",
            "延迟秒数": f"乱序纠错中: {entry.get('修正行数', 0)} 行",
            "告警阈值": "30s",
            "最近采样": "2026-11-18 10:33:00",
            "采样间隔": "10s",
            "status": "正常",
            "pending": True,
            "abnormal": False,
        }
        lm_rows.append(new_lm)
        return new_lm

    def handle_window_overflow(self, entry_id: int) -> dict[str, Any] | None:
        """乱序超窗口丢弃时，触发checkpoint保底和告警。

        数据流链路: out_of_order(超窗口丢弃) -> checkpoint + alert_rule
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "超窗口丢弃":
            return None

        tag = str(entry.get("数据流", ""))
        results = {"checkpoint": None, "alert": None}

        # 创建告警
        alert_rows = store.rows("alert_rule")
        new_alert_id = max((int(r.get("id", 0)) for r in alert_rows), default=0) + 1
        alert = {
            "id": new_alert_id,
            "策略编号": f"ALER-{new_alert_id:04d}",
            "监控指标": "乱序丢弃率",
            "告警条件": f"丢弃 > {entry.get('丢弃行数', '阈值')}行",
            "告警级别": "P3-注意",
            "通知渠道": "飞书群",
            "聚合窗口": "1min",
            "静默时段": "无",
            "触发次数": 1,
            "status": "已触发",
            "pending": True,
            "abnormal": True,
        }
        alert_rows.append(alert)
        results["alert"] = alert

        # 创建保底checkpoint
        cp_rows = store.rows("checkpoint")
        new_cp_id = max((int(r.get("id", 0)) for r in cp_rows), default=0) + 1
        cp = {
            "id": new_cp_id,
            "检查点编号": f"CHEC-{new_cp_id:04d}",
            "数据流": tag,
            "保存位点": f"checkpoint_after_overflow_{new_cp_id}",
            "保存时间": "2026-11-18 10:34:00",
            "恢复状态": "紧急保底",
            "丢失行数": int(entry.get("丢弃行数", 0)),
            "回放代价": "需全量回填",
            "恢复耗时": "预估15min",
            "status": "已保存",
            "pending": True,
            "abnormal": True,
        }
        cp_rows.append(cp)
        results["checkpoint"] = cp

        return results