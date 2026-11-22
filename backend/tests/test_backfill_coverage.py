"""回填区间数据视图端到端演练（断点恢复缺口 101-130 场景）。

直接运行：.venv/bin/python tests/test_backfill_coverage.py
也兼容 pytest 收集（函数名 test_ 开头，仅用标准库断言）。
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.backfill_coverage import backfill_coverage_service as svc

ORDER = "order_events_cdc"
PAY = "payment_log_cdc"


def setup_scenario():
    svc.reset()
    svc.seed_demo_scenario()
    return svc


def test_initial_view_partitions_gap_by_stream() -> None:
    setup_scenario()
    view = svc.build_view(ORDER)
    # 视图按区间排列：已覆盖 / 占用 / 待派发
    assert view["gap_ranges"] == ["101-130"]
    assert view["covered_ranges"] == ["101-110"]          # 只有落库回执才计已补齐
    assert view["occupied_ranges"] == ["111-120"]         # 失联旧端仍持有占用
    assert view["dispatchable_ranges"] == ["121-130"]     # 剩余缺口可派发
    assert view["counts"] == {"gap": 30, "covered": 10, "occupied": 10, "dispatchable": 10}
    assert [(s["range"], s["state"], s["owner"]) for s in view["segments"]] == [
        ("101-110", "已覆盖", "已落库回执"),
        ("111-120", "占用", "BF-OLD-0001"),
        ("121-130", "待派发", "可发起增量回填"),
    ]
    # 111-120 的有效写入方只有失联旧批次一笔
    assert {o["pos"]: o["valid_writer"] for o in view["occupancy"]} == {
        pos: "BF-OLD-0001" for pos in range(111, 121)
    }
    assert all(o["connected"] is False for o in view["occupancy"])

    # 支付流同号位点不能计入订单流覆盖；按流隔离
    pay_view = svc.build_view(PAY)
    assert pay_view["covered_ranges"] == ["101-110"]
    assert "101-110" not in view["occupied_ranges"]
    assert svc.build_view(ORDER)["counts"]["covered"] == 10


def test_dispatch_only_accepts_dispatchable_and_records_blockers() -> None:
    setup_scenario()
    result = svc.dispatch_backfill(ORDER, 101, 130, executor="新执行端", batch_no="BF-NEW-0001")
    dispatch = result["dispatch"]
    # 当次实际受理范围 = 待派发的 121-130
    assert dispatch["accepted_ranges"] == ["121-130"]
    assert dispatch["accepted_count"] == 10
    # 已落库禁止重写、在途占用暂不可派发，均被阻挡
    assert {(b["range"], b["reason"]) for b in dispatch["blocked"]} == {
        ("101-110", "已落库-禁止重写"),
        ("111-120", "在途回填占用-暂不可派发"),
    }
    owner_block = next(b for b in dispatch["blocked"] if b["range"] == "111-120")
    assert owner_block["owner"] == "BF-OLD-0001"
    # 新批次拿到的是 121-130 的独占租约
    assert svc.leases["BF-NEW-0001"] == [(121, 130)]
    # 视图分段按有效写入方拆分，不把两个批次并成一段
    assert [(s["range"], s["state"], s["owner"]) for s in result["view"]["segments"]] == [
        ("101-110", "已覆盖", "已落库回执"),
        ("111-120", "占用", "BF-OLD-0001"),
        ("121-130", "占用", "BF-NEW-0001"),
    ]


def test_old_executor_reconnect_writes_115_and_new_executor_cannot_overwrite() -> None:
    setup_scenario()
    svc.dispatch_backfill(ORDER, 101, 130, executor="新执行端", batch_no="BF-NEW-0001")

    # 旧执行端重新连接，补送 115（租约仍有效，写入成功并落库一行）
    assert svc.mark_connection("BF-OLD-0001", True)["connected"] is True
    old_write = svc.receive_writes(ORDER, "BF-OLD-0001", [115], executor="旧执行端")
    assert old_write["accepted"] == [115]
    rows = svc.list_target_rows(ORDER)
    row115 = [r for r in rows if r["pos"] == 115]
    assert len(row115) == 1
    assert row115[0]["writer_batch"] == "BF-OLD-0001"

    # 同一未提交/已落库位点，新批次无写入资格
    conflict = svc.receive_writes(ORDER, "BF-NEW-0001", [115], executor="新执行端")
    assert conflict["accepted"] == []
    assert conflict["rejected"][0]["pos"] == 115
    # 已落库的 105 任何回填都不能重写
    rewrite = svc.receive_writes(ORDER, "BF-NEW-0001", [105])
    assert rewrite["accepted"] == [] and rewrite["rejected"][0]["pos"] == 105
    # 旧批次重复回执幂等，目标表 115 仍只有一行
    again = svc.receive_writes(ORDER, "BF-OLD-0001", [115])
    assert again["idempotent"] == [115]
    assert len([r for r in svc.list_target_rows(ORDER) if r["pos"] == 115]) == 1


def test_new_executor_confirms_to_124_then_interrupt_releases_remainder() -> None:
    setup_scenario()
    svc.dispatch_backfill(ORDER, 101, 130, executor="新执行端", batch_no="BF-NEW-0001")
    svc.mark_connection("BF-OLD-0001", True)
    svc.receive_writes(ORDER, "BF-OLD-0001", [115], executor="旧执行端")

    # 新执行端从实际领到的起点 121 连续确认到 124
    writes = svc.receive_writes(ORDER, "BF-NEW-0001", [121, 122, 123, 124], executor="新执行端")
    assert writes["accepted"] == [121, 122, 123, 124]
    assert writes["rejected"] == []
    # 中断：125-130 未确认占用释放回待派发，已确认保留为已覆盖
    interrupted = svc.interrupt_batch(ORDER, "BF-NEW-0001")
    assert interrupted["released_ranges"] == ["125-130"]
    view = interrupted["view"]
    assert view["covered_ranges"] == ["101-110", "115", "121-124"]
    assert view["dispatchable_ranges"] == ["125-130"]
    assert view["occupied_ranges"] == ["111-114", "116-120"]

    # 目标表核对：101-110、115、121-124 共 15 行，每个位点唯一一行
    rows = svc.list_target_rows(ORDER)
    assert len(rows) == 15
    assert sorted(r["pos"] for r in rows) == list(range(101, 111)) + [115] + list(range(121, 125))
    assert len({(r["stream"], r["pos"]) for r in rows}) == len(rows)


def test_second_dispatch_shows_still_occupied_vs_claimable() -> None:
    setup_scenario()
    first = svc.dispatch_backfill(ORDER, 101, 130, executor="新执行端", batch_no="BF-NEW-0001")
    assert first["dispatch"]["dispatch_id"] == 1
    svc.mark_connection("BF-OLD-0001", True)
    svc.receive_writes(ORDER, "BF-OLD-0001", [115], executor="旧执行端")
    svc.receive_writes(ORDER, "BF-NEW-0001", [121, 122, 123, 124], executor="新执行端")
    svc.interrupt_batch(ORDER, "BF-NEW-0001")

    # 再次派发整个缺口：只剩 125-130 可领取
    pre_view = svc.build_view(ORDER)
    # 派发前数据缺口里仍被旧批次占用的位点及有效写入方
    assert {o["pos"]: o["valid_writer"] for o in pre_view["occupancy"]} == {
        pos: "BF-OLD-0001" for pos in [111, 112, 113, 114, 116, 117, 118, 119, 120]
    }
    second = svc.dispatch_backfill(ORDER, 101, 130, executor="第二轮执行端", batch_no="BF-NEW-0002")
    assert second["dispatch"]["dispatch_id"] == 2
    assert second["dispatch"]["accepted_ranges"] == ["125-130"]
    blocked = {(b["range"], b["reason"], b.get("owner", "")) for b in second["dispatch"]["blocked"]}
    # 111-114、116-120 仍被失联重连的旧批次占用；115 已落库
    assert ("111-114", "在途回填占用-暂不可派发", "BF-OLD-0001") in blocked
    assert ("116-120", "在途回填占用-暂不可派发", "BF-OLD-0001") in blocked
    assert ("115", "已落库-禁止重写", "") in blocked
    assert ("101-110", "已落库-禁止重写", "") in blocked
    assert ("121-124", "已落库-禁止重写", "") in blocked

    # 派发后 125-130 的有效写入方变为第二批次；旧批次仍占有 111-114、116-120
    view = second["view"]
    owners = {o["pos"]: o["valid_writer"] for o in view["occupancy"]}
    assert owners.get(125) == "BF-NEW-0002" and owners.get(130) == "BF-NEW-0002"
    assert all(owners[pos] == "BF-OLD-0001" for pos in [111, 112, 113, 114, 116, 117, 118, 119, 120])
    assert view["dispatchable_ranges"] == []  # 缺口已全部有主或落库
    # 两次派发记录可对照
    dispatches = svc.list_dispatches(ORDER)
    assert [d["batch_no"] for d in dispatches] == ["BF-NEW-0001", "BF-NEW-0002"]
    assert dispatches[0]["accepted_ranges"] == ["121-130"]
    assert dispatches[1]["accepted_ranges"] == ["125-130"]


def test_payment_stream_receipts_never_count_for_order_stream() -> None:
    setup_scenario()
    # 支付流再多落库一些同号位点，订单流视图不受影响
    svc.receive_writes  # 方法存在性
    before = svc.build_view(ORDER)["counts"]
    for pos in range(111, 121):
        svc.receipts.append({"stream": PAY, "pos": pos, "batch_no": "BF-PAY-0001", "executor": "支付流写入端"})
    after = svc.build_view(ORDER)["counts"]
    assert before == after
    assert svc.build_view(PAY)["counts"]["covered"] == 20


def _run_all() -> None:
    tests = [obj for name, obj in sorted(globals().items()) if name.startswith("test_") and callable(obj)]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"\n全部 {len(tests)} 个演练断言通过")


if __name__ == "__main__":
    _run_all()
