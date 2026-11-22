"""回填区间数据视图领域服务。

断点恢复按数据流核对覆盖：把乱序超窗丢弃区间、在途回填批次归并到同一条数据流，
扣除已经收到落库回执的位点，得到按区间排列的覆盖视图（已覆盖 / 占用 / 待派发）。

核心规则：
1. 只按数据流归并，支付流的同号位点不计入订单流覆盖；
2. 只有实际收到落库回执的位点才计为已补齐（已覆盖），已落库位点不能重写；
3. 同一数据流同一未提交位点同一时刻只能由一笔有效回填占有（写入租约排他）；
4. 已失联执行端持有的在途批次仍保留占用，其重连补写时按租约校验写入资格。

仅依赖标准库，状态保存在进程内存中（与 store 一致的演示形态）。
"""
from __future__ import annotations

from threading import RLock
from typing import Any


# ---- 纯函数：区间运算 -------------------------------------------------------

def _merge_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """把若干 [lo, hi]（含两端）区间归并成有序、不重叠的区间列表。"""
    merged: list[tuple[int, int]] = []
    for lo, hi in sorted(ranges):
        if lo > hi:
            continue
        if merged and lo <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
        else:
            merged.append((lo, hi))
    return merged


def _subtract_ranges(
    base: list[tuple[int, int]],
    taken: list[tuple[int, int]],
) -> list[tuple[int, int]]:
    """从 base 区间集合中扣除 taken 区间集合，返回剩余区间。"""
    remaining = list(base)
    for t_lo, t_hi in sorted(taken):
        nxt: list[tuple[int, int]] = []
        for lo, hi in remaining:
            if t_hi < lo or t_lo > hi:  # 不相交
                nxt.append((lo, hi))
            else:
                if lo < t_lo:
                    nxt.append((lo, t_lo - 1))
                if hi > t_hi:
                    nxt.append((t_hi + 1, hi))
        remaining = nxt
    return _merge_ranges(remaining)


def _intersect_ranges(
    left: list[tuple[int, int]],
    right: list[tuple[int, int]],
) -> list[tuple[int, int]]:
    """两个区间集合的交集。"""
    result: list[tuple[int, int]] = []
    for a_lo, a_hi in left:
        for b_lo, b_hi in right:
            lo, hi = max(a_lo, b_lo), min(a_hi, b_hi)
            if lo <= hi:
                result.append((lo, hi))
    return _merge_ranges(result)


def _format_range(lo: int, hi: int) -> str:
    return f"{lo}-{hi}" if lo != hi else str(lo)


def _format_ranges(ranges: list[tuple[int, int]]) -> str:
    return "、".join(_format_range(lo, hi) for lo, hi in ranges) if ranges else "无"


def _to_ranges(points: list[int]) -> list[tuple[int, int]]:
    return _merge_ranges([(p, p) for p in points])


class BackfillCoverageService:
    """回填区间数据视图 + 回填派发/回执/中断的写入资格仲裁器。"""

    def __init__(self) -> None:
        self._lock = RLock()
        # 每条数据流的乱序丢弃区间（回填全集来源），按流归并
        self.gap_inputs: dict[str, list[dict[str, Any]]] = {}
        # 在途回填批次（含执行端与连接状态）
        self.batches: dict[str, dict[str, Any]] = {}
        # 写入租约：batch_no -> 占有的区间集合
        self.leases: dict[str, list[tuple[int, int]]] = {}
        # 已确认落库回执
        self.receipts: list[dict[str, Any]] = []
        # 每次派发的记录（含当次受理范围与被阻挡范围）
        self.dispatches: list[dict[str, Any]] = []
        # 目标表实际行（stream, pos）唯一，验证只写一行
        self.target_rows: dict[tuple[str, int], dict[str, Any]] = {}
        self._dispatch_seq = 0
        self._batch_seq = 0
        self._seeded = False

    def reset(self) -> None:
        """清空全部状态（测试/重新演练用）。"""
        with self._lock:
            self.gap_inputs.clear()
            self.batches.clear()
            self.leases.clear()
            self.receipts.clear()
            self.dispatches.clear()
            self.target_rows.clear()
            self._dispatch_seq = 0
            self._batch_seq = 0
            self._seeded = False

    # ---- 入口注入：乱序超窗丢弃区间 / 断点恢复在途批次 ----------------------

    def inject_gap(
        self,
        stream: str,
        lo: int,
        hi: int,
        *,
        source: str = "out_of_order",
        source_ref: str = "",
    ) -> dict[str, Any]:
        """乱序超窗入口注入丢弃区间；同一流的多次注入按区间归并。"""
        with self._lock:
            inputs = self.gap_inputs.setdefault(stream, [])
            existing = next(
                (item for item in inputs if item["lo"] == lo and item["hi"] == hi),
                None,
            )
            if existing is None:
                inputs.append({
                    "lo": lo, "hi": hi, "source": source, "source_ref": source_ref,
                })
            return {"stream": stream, "range": _format_range(lo, hi), "merged": existing is None}

    def register_inflight_batch(
        self,
        stream: str,
        lo: int,
        hi: int,
        *,
        executor: str,
        batch_no: str | None = None,
        connected: bool = False,
        source_ref: str = "checkpoint",
    ) -> dict[str, Any]:
        """断点恢复入口注入在途回填批次，并登记其写入租约。

        租约只在丢弃区间（回填全集）内生效；与同一数据流已有有效租约冲突的
        部分不会重复占有，冲突明细随结果返回。
        """
        with self._lock:
            if batch_no is None:
                self._batch_seq += 1
                batch_no = f"BF-INFLIGHT-{self._batch_seq:04d}"
            want = [(lo, hi)]
            leased = self._leased_ranges(stream)
            conflicts = _intersect_ranges(want, leased)
            granted = _subtract_ranges(want, leased)
            self.batches[batch_no] = {
                "batch_no": batch_no,
                "stream": stream,
                "lo": lo,
                "hi": hi,
                "executor": executor,
                "connected": connected,
                "source_ref": source_ref,
                "status": "在途" if connected else "失联占用",
            }
            self.leases[batch_no] = _merge_ranges(
                self.leases.get(batch_no, []) + granted,
            )
            return {
                "batch_no": batch_no,
                "stream": stream,
                "granted_ranges": [_format_range(a, b) for a, b in granted],
                "conflict_ranges": [_format_range(a, b) for a, b in conflicts],
            }

    # ---- 内部状态读取 -------------------------------------------------------

    def _gap_ranges(self, stream: str) -> list[tuple[int, int]]:
        return _merge_ranges([(item["lo"], item["hi"]) for item in self.gap_inputs.get(stream, [])])

    def _receipt_points(self, stream: str) -> list[int]:
        return sorted(
            item["pos"] for item in self.receipts if item["stream"] == stream
        )

    def _covered_ranges(self, stream: str) -> list[tuple[int, int]]:
        """已覆盖 = 丢弃区间内、实际收到落库回执的位点。"""
        covered = _to_ranges(self._receipt_points(stream))
        return _intersect_ranges(self._gap_ranges(stream), covered)

    def _leased_ranges(self, stream: str) -> list[tuple[int, int]]:
        ranges: list[tuple[int, int]] = []
        for batch_no, leased in self.leases.items():
            batch = self.batches.get(batch_no)
            if batch and batch["stream"] == stream and leased:
                ranges.extend(leased)
        return _merge_ranges(ranges)

    def _lease_owner(self, stream: str, pos: int) -> str | None:
        for batch_no, leased in self.leases.items():
            batch = self.batches.get(batch_no)
            if batch and batch["stream"] == stream and any(
                lo <= pos <= hi for lo, hi in leased
            ):
                return batch_no
        return None

    def streams(self) -> list[str]:
        with self._lock:
            return sorted(self.gap_inputs.keys())

    # ---- 回填区间数据视图 ---------------------------------------------------

    def build_view(self, stream: str) -> dict[str, Any]:
        """按数据流生成回填区间数据视图：已覆盖 / 占用 / 待派发分段。"""
        with self._lock:
            gaps = self._gap_ranges(stream)
            covered = self._covered_ranges(stream)
            # 占用只统计尚未落库的租约位点（已落库位点已转为已覆盖）
            occupied_all = _subtract_ranges(self._leased_ranges(stream), covered)
            dispatchable = _subtract_ranges(gaps, _merge_ranges(covered + occupied_all))

            # 按持有方拆出租约片段，保证同一区间段只呈现一个有效写入方
            owner_pieces: list[tuple[int, int, str, str]] = []
            for batch_no, leased in self.leases.items():
                batch = self.batches.get(batch_no)
                if not batch or batch["stream"] != stream:
                    continue
                for lo, hi in _subtract_ranges(leased, covered):
                    owner_pieces.append((lo, hi, "占用", batch_no))

            segments: list[dict[str, Any]] = []
            for lo, hi in gaps:
                pieces: list[tuple[int, int, str, str]] = []
                for c_lo, c_hi in _intersect_ranges([(lo, hi)], covered):
                    pieces.append((c_lo, c_hi, "已覆盖", "已落库回执"))
                for o_lo, o_hi, state, owner in owner_pieces:
                    for p_lo, p_hi in _intersect_ranges([(lo, hi)], [(o_lo, o_hi)]):
                        pieces.append((p_lo, p_hi, state, owner))
                for d_lo, d_hi in _intersect_ranges([(lo, hi)], dispatchable):
                    pieces.append((d_lo, d_hi, "待派发", "可发起增量回填"))
                # 相邻且状态/写入方一致的片段合并，否则保持拆分
                for p_lo, p_hi, state, owner in sorted(pieces):
                    if segments and segments[-1]["state"] == state and segments[-1]["owner"] == owner \
                            and segments[-1]["hi"] + 1 == p_lo:
                        segments[-1]["hi"] = p_hi
                        segments[-1]["range"] = _format_range(segments[-1]["lo"], p_hi)
                        segments[-1]["length"] = p_hi - segments[-1]["lo"] + 1
                    else:
                        segments.append({
                            "lo": p_lo, "hi": p_hi,
                            "range": _format_range(p_lo, p_hi),
                            "state": state, "owner": owner,
                            "length": p_hi - p_lo + 1,
                        })

            # 占用区间里每个位点的有效写入方（值班员核对 111-120 用）
            occupancy: list[dict[str, Any]] = []
            for lo, hi in occupied_all:
                for pos in range(lo, hi + 1):
                    owner = self._lease_owner(stream, pos)
                    batch = self.batches.get(owner) if owner else None
                    occupancy.append({
                        "pos": pos,
                        "valid_writer": owner,
                        "executor": batch["executor"] if batch else "",
                        "connected": batch["connected"] if batch else False,
                        "batch_status": batch["status"] if batch else "",
                    })

            return {
                "stream": stream,
                "gap_ranges": [_format_range(lo, hi) for lo, hi in gaps],
                "covered_ranges": [_format_range(lo, hi) for lo, hi in covered],
                "occupied_ranges": [_format_range(lo, hi) for lo, hi in occupied_all],
                "dispatchable_ranges": [_format_range(lo, hi) for lo, hi in dispatchable],
                "segments": segments,
                "occupancy": occupancy,
                "counts": {
                    "gap": sum(hi - lo + 1 for lo, hi in gaps),
                    "covered": sum(hi - lo + 1 for lo, hi in covered),
                    "occupied": sum(hi - lo + 1 for lo, hi in occupied_all),
                    "dispatchable": sum(hi - lo + 1 for lo, hi in dispatchable),
                },
            }

    # ---- 派发增量回填 -------------------------------------------------------

    def dispatch_backfill(
        self,
        stream: str,
        lo: int,
        hi: int,
        *,
        executor: str,
        batch_no: str | None = None,
    ) -> dict[str, Any]:
        """值班员发起新回填：只受理仍可派发的位点，占用/已覆盖部分被阻挡。

        当次受理范围即新批次拿到的写入租约；同一未提交位点不会重复发放。
        """
        with self._lock:
            gaps = self._gap_ranges(stream)
            request = _intersect_ranges([(lo, hi)], gaps)
            if not request:
                return {
                    "ok": False,
                    "message": f"请求范围 {_format_range(lo, hi)} 不在数据流 {stream} 的丢弃区间内",
                    "stream": stream,
                }
            covered = self._covered_ranges(stream)
            leased = self._leased_ranges(stream)
            blocked_covered = _intersect_ranges(request, covered)
            blocked_leased = _subtract_ranges(
                _intersect_ranges(request, leased), blocked_covered,
            )
            accepted = _subtract_ranges(request, _merge_ranges(covered + leased))

            blocked: list[dict[str, Any]] = []
            for b_lo, b_hi in blocked_covered:
                blocked.append({
                    "lo": b_lo, "hi": b_hi,
                    "range": _format_range(b_lo, b_hi),
                    "reason": "已落库-禁止重写",
                    "owner": "",
                })
            # 占用阻挡按有效写入方归并成连续区间
            leased_blocks: dict[str, list[tuple[int, int]]] = {}
            for b_lo, b_hi in blocked_leased:
                for pos in range(b_lo, b_hi + 1):
                    owner = self._lease_owner(stream, pos) or ""
                    leased_blocks.setdefault(owner, []).append((pos, pos))
            for owner, point_ranges in sorted(leased_blocks.items()):
                batch = self.batches.get(owner)
                for m_lo, m_hi in _merge_ranges(point_ranges):
                    blocked.append({
                        "lo": m_lo, "hi": m_hi,
                        "range": _format_range(m_lo, m_hi),
                        "reason": "在途回填占用-暂不可派发",
                        "owner": owner,
                        "executor": batch["executor"] if batch else "",
                    })

            if batch_no is None or batch_no in self.batches:
                self._batch_seq += 1
                batch_no = batch_no or f"BF-{self._batch_seq:04d}"
                if batch_no in self.batches:
                    batch_no = f"BF-{self._batch_seq:04d}"
            self.batches[batch_no] = {
                "batch_no": batch_no,
                "stream": stream,
                "lo": lo,
                "hi": hi,
                "executor": executor,
                "connected": True,
                "source_ref": "operator_dispatch",
                "status": "回填中",
            }
            self.leases[batch_no] = _merge_ranges(
                self.leases.get(batch_no, []) + accepted,
            )
            self._dispatch_seq += 1
            record = {
                "dispatch_id": self._dispatch_seq,
                "batch_no": batch_no,
                "stream": stream,
                "executor": executor,
                "request_range": _format_range(lo, hi),
                "accepted_ranges": [_format_range(a, b) for a, b in accepted],
                "blocked": blocked,
                "accepted_count": sum(b - a + 1 for a, b in accepted),
            }
            self.dispatches.append(record)
            view = self.build_view(stream)
            return {
                "ok": bool(accepted),
                "message": (
                    f"批次 {batch_no} 实际受理 {_format_ranges(accepted)}"
                    if accepted else f"批次 {batch_no} 未受理任何位点，请求范围均被占用或已落库"
                ),
                "dispatch": record,
                "view": view,
            }

    # ---- 写入回执（执行端补写/连续确认） ------------------------------------

    def receive_writes(
        self,
        stream: str,
        batch_no: str,
        positions: list[int],
        *,
        executor: str = "",
    ) -> dict[str, Any]:
        """执行端提交写入回执；按写入租约仲裁资格，合法写入落目标表。

        - 无租约（含失联批次被新回填顶替的位点）：拒绝；
        - 已落库位点：拒绝重写（同批次重复回执按幂等处理，不新增行）；
        - 合法回执：目标表只插入一行，并把位点转为已覆盖、释放对应租约。
        """
        with self._lock:
            batch = self.batches.get(batch_no)
            if batch is None or batch["stream"] != stream:
                return {"ok": False, "message": f"批次 {batch_no} 不属于数据流 {stream}", "accepted": [], "rejected": []}
            leased = self.leases.get(batch_no, [])
            covered_points = set(self._receipt_points(stream))
            gap_points = {
                pos for lo, hi in self._gap_ranges(stream) for pos in range(lo, hi + 1)
            }
            accepted: list[int] = []
            idempotent: list[int] = []
            rejected: list[dict[str, Any]] = []

            for pos in sorted(set(positions)):
                if pos not in gap_points:
                    rejected.append({"pos": pos, "reason": "位点不在丢弃区间内"})
                    continue
                row = self.target_rows.get((stream, pos))
                if row is not None:
                    if row["writer_batch"] == batch_no:
                        idempotent.append(pos)  # 同批次重复回执：幂等，不产生第二行
                    else:
                        rejected.append({
                            "pos": pos,
                            "reason": f"已落库-禁止重写（现有写入方 {row['writer_batch']}）",
                        })
                    continue
                if pos in covered_points:
                    rejected.append({"pos": pos, "reason": "已落库-禁止重写"})
                    continue
                if not any(lo <= pos <= hi for lo, hi in leased):
                    owner = self._lease_owner(stream, pos)
                    rejected.append({
                        "pos": pos,
                        "reason": (
                            f"无写入资格，位点由有效回填 {owner} 占有"
                            if owner else "无写入资格，该位点未派发给本批次"
                        ),
                    })
                    continue
                # 合法写入：目标表只保留一行
                self.target_rows[(stream, pos)] = {
                    "stream": stream,
                    "pos": pos,
                    "writer_batch": batch_no,
                    "executor": executor or batch["executor"],
                }
                self.receipts.append({
                    "stream": stream,
                    "pos": pos,
                    "batch_no": batch_no,
                    "executor": executor or batch["executor"],
                })
                covered_points.add(pos)
                accepted.append(pos)

            # 释放已落库位点的租约
            if accepted:
                self.leases[batch_no] = _subtract_ranges(leased, _to_ranges(accepted))
            return {
                "ok": not rejected,
                "message": (
                    f"接受 {len(accepted)} 个位点，幂等重复 {len(idempotent)} 个，拒绝 {len(rejected)} 个"
                ),
                "batch_no": batch_no,
                "accepted": accepted,
                "idempotent": idempotent,
                "rejected": rejected,
                "view": self.build_view(stream),
            }

    # ---- 中断执行端：释放未确认租约，已确认保留为已覆盖 ----------------------

    def interrupt_batch(self, stream: str, batch_no: str) -> dict[str, Any]:
        """执行端中断：已落库回执保留，未提交的占用释放回待派发池。"""
        with self._lock:
            batch = self.batches.get(batch_no)
            if batch is None or batch["stream"] != stream:
                return {"ok": False, "message": f"批次 {batch_no} 不存在或不属于 {stream}"}
            released = list(self.leases.get(batch_no, []))
            self.leases[batch_no] = []
            batch["status"] = "已中断"
            batch["connected"] = False
            return {
                "ok": True,
                "message": f"批次 {batch_no} 已中断，释放未确认占用 {_format_ranges(released)}",
                "released_ranges": [_format_range(lo, hi) for lo, hi in released],
                "view": self.build_view(stream),
            }

    def mark_connection(self, batch_no: str, connected: bool) -> dict[str, Any]:
        """旧执行端重连/失联标记；不改变租约本身的有效性。"""
        with self._lock:
            batch = self.batches.get(batch_no)
            if batch is None:
                return {"ok": False, "message": f"批次 {batch_no} 不存在"}
            batch["connected"] = connected
            if connected and batch["status"] == "失联占用":
                batch["status"] = "在途"
            elif not connected and batch["status"] == "在途":
                batch["status"] = "失联占用"
            return {"ok": True, "batch_no": batch_no, "connected": connected, "status": batch["status"]}

    # ---- 查询：派发记录 / 目标表 --------------------------------------------

    def list_dispatches(self, stream: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if stream is None:
                return [dict(item) for item in self.dispatches]
            return [dict(item) for item in self.dispatches if item["stream"] == stream]

    def list_target_rows(self, stream: str) -> list[dict[str, Any]]:
        with self._lock:
            return [
                dict(row)
                for key, row in sorted(self.target_rows.items())
                if key[0] == stream
            ]

    def list_batches(self, stream: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = []
            for batch_no, batch in sorted(self.batches.items()):
                if stream is not None and batch["stream"] != stream:
                    continue
                rows.append({
                    **dict(batch),
                    "leased_ranges": [_format_range(lo, hi) for lo, hi in self.leases.get(batch_no, [])],
                })
            return rows

    # ---- 演练场景种子 -------------------------------------------------------

    def seed_demo_scenario(self) -> None:
        """注入题设场景：

        订单流 order_events_cdc 缺口 101-130；
        101-110 已确认落库；旧执行端失联但持有 111-120 在途租约；
        支付流 payment_log_cdc 同号位点用于验证按流隔离。
        """
        with self._lock:
            if self._seeded:
                return
            # 乱序超窗入口注入的丢弃区间
            self.inject_gap(
                "order_events_cdc", 101, 130,
                source="out_of_order", source_ref="OOO-ORDER-GAP",
            )
            self.inject_gap(
                "payment_log_cdc", 101, 120,
                source="out_of_order", source_ref="OOO-PAY-GAP",
            )
            # 订单流 101-110 已落库（带落库回执）
            for pos in range(101, 111):
                self.target_rows[("order_events_cdc", pos)] = {
                    "stream": "order_events_cdc", "pos": pos,
                    "writer_batch": "BF-SEED-COVERED", "executor": "常规链路写入端",
                }
                self.receipts.append({
                    "stream": "order_events_cdc", "pos": pos,
                    "batch_no": "BF-SEED-COVERED", "executor": "常规链路写入端",
                })
            # 断点恢复入口注入：旧执行端失联但仍持有 111-120 的在途回填
            self.register_inflight_batch(
                "order_events_cdc", 111, 120,
                executor="旧执行端", batch_no="BF-OLD-0001",
                connected=False, source_ref="checkpoint",
            )
            # 支付流同号位点：验证跨流隔离，不能计入订单流覆盖
            for pos in range(101, 111):
                self.receipts.append({
                    "stream": "payment_log_cdc", "pos": pos,
                    "batch_no": "BF-PAY-0001", "executor": "支付流写入端",
                })
            self._seeded = True


# 进程级单例（与 app.store.store 同样的内存形态）
backfill_coverage_service = BackfillCoverageService()
