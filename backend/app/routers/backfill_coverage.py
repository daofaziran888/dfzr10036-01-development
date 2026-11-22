"""回填区间数据视图路由：按数据流核对覆盖，并受理增量回填派发/回执/中断。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.backfill_coverage import backfill_coverage_service as service

router = APIRouter(prefix="/api/backfill-coverage", tags=["回填区间数据视图"])


class GapPayload(BaseModel):
    stream: str
    lo: int = Field(ge=0)
    hi: int = Field(ge=0)
    source: str = "out_of_order"
    source_ref: str = ""


class InflightPayload(BaseModel):
    stream: str
    lo: int = Field(ge=0)
    hi: int = Field(ge=0)
    executor: str
    batch_no: str | None = None
    connected: bool = False
    source_ref: str = "checkpoint"


class DispatchPayload(BaseModel):
    stream: str
    lo: int = Field(ge=0)
    hi: int = Field(ge=0)
    executor: str
    batch_no: str | None = None


class WritePayload(BaseModel):
    stream: str
    batch_no: str
    positions: list[int]
    executor: str = ""


class BatchRefPayload(BaseModel):
    stream: str
    batch_no: str


class ConnectionPayload(BaseModel):
    batch_no: str
    connected: bool


@router.get("/streams")
def get_streams() -> dict[str, Any]:
    """列出存在丢弃区间的数据流。"""
    return {"streams": service.streams()}


@router.get("/view/{stream}")
def get_view(stream: str) -> dict[str, Any]:
    """回填区间数据视图：按区间列出已覆盖、占用、待派发及有效写入方。"""
    return service.build_view(stream)


@router.post("/gaps")
def inject_gap(payload: GapPayload) -> dict[str, Any]:
    """乱序超窗入口注入丢弃区间（同流多次注入自动归并）。"""
    return service.inject_gap(
        payload.stream, payload.lo, payload.hi,
        source=payload.source, source_ref=payload.source_ref,
    )


@router.post("/inflight")
def register_inflight(payload: InflightPayload) -> dict[str, Any]:
    """断点恢复入口注入在途回填批次及其写入租约。"""
    return service.register_inflight_batch(
        payload.stream, payload.lo, payload.hi,
        executor=payload.executor, batch_no=payload.batch_no,
        connected=payload.connected, source_ref=payload.source_ref,
    )


@router.post("/dispatch")
def dispatch_backfill(payload: DispatchPayload) -> dict[str, Any]:
    """值班员发起增量回填，返回当次实际受理范围、被阻挡区间和最新视图。"""
    return service.dispatch_backfill(
        payload.stream, payload.lo, payload.hi,
        executor=payload.executor, batch_no=payload.batch_no,
    )


@router.post("/writes")
def receive_writes(payload: WritePayload) -> dict[str, Any]:
    """执行端补写/连续确认：按写入租约仲裁，回执合法才落目标表。"""
    return service.receive_writes(
        payload.stream, payload.batch_no, payload.positions, executor=payload.executor,
    )


@router.post("/interrupt")
def interrupt_batch(payload: BatchRefPayload) -> dict[str, Any]:
    """执行端中断：释放未确认占用，已落库回执保留。"""
    return service.interrupt_batch(payload.stream, payload.batch_no)


@router.post("/connection")
def update_connection(payload: ConnectionPayload) -> dict[str, Any]:
    """旧执行端重连/失联标记。"""
    return service.mark_connection(payload.batch_no, payload.connected)


@router.get("/dispatches")
def list_dispatches(stream: str | None = None) -> dict[str, Any]:
    """两次派发记录对照。"""
    return {"items": service.list_dispatches(stream)}


@router.get("/target-rows/{stream}")
def list_target_rows(stream: str) -> dict[str, Any]:
    """目标表实际落库行，核对位点由谁写入、是否只有一行。"""
    rows = service.list_target_rows(stream)
    return {"items": rows, "total": len(rows)}


@router.get("/batches")
def list_batches(stream: str | None = None) -> dict[str, Any]:
    """在途回填批次及其当前占有区间。"""
    return {"items": service.list_batches(stream)}


@router.post("/reset")
def reset_state() -> dict[str, Any]:
    """清空视图状态并重新注入题设演练场景。"""
    service.reset()
    service.seed_demo_scenario()
    return {"ok": True, "message": "已重置并重新注入演练场景"}
