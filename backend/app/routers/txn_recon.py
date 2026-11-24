"""事务采集对账专用路由：跨来源事务日志查询入口、质量报告、缺口发现与补采回执。

与通用资源路由并存：/api/txn_recon 的列表/登记/动作仍由通用资源路由处理，
本路由承载事务维度的领域操作，业务判断全部落在 TxnReconService。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.core.txn_recon import ReconError
from app.schemas import ActionResult
from app.services import service_for
from app.services.txn_recon import TxnReconService

router = APIRouter(prefix="/api/txn_recon", tags=["事务采集对账"])


class IngestPayload(BaseModel):
    source_id: str
    raw_line: str
    txn_key: str | None = None
    event_at: str | None = None
    collect_ref: str | None = None


class ReportPayload(BaseModel):
    txn_key: str
    sources: list[str]
    start: str
    end: str


class GapDetectPayload(BaseModel):
    txn_key: str
    start: str
    end: str


class DeliverPayload(BaseModel):
    lines: list[dict[str, Any]] = Field(default_factory=list)


def _service() -> TxnReconService:
    service = service_for("txn_recon")
    assert isinstance(service, TxnReconService)
    return service


def _guard(call, *args, **kwargs) -> ActionResult:
    try:
        entry = call(*args, **kwargs)
    except ReconError as exc:
        return ActionResult(ok=False, message=str(exc))
    return ActionResult(ok=True, message="ok", entry=entry)


@router.get("/query")
def query_txn_logs(
    txn_key: str = Query(...),
    sources: str | None = Query(default=None),
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
) -> dict[str, Any]:
    """事务日志查询入口：按事务键、来源和时段定位真实原行与各自解析结果。"""
    source_list = [s.strip() for s in (sources or "").split(",") if s.strip()]
    items = _service().query_txn_logs(txn_key, source_list, start, end)
    return {"ok": True, "total": len(items), "items": items}


@router.post("/ingest", response_model=ActionResult)
def ingest_line(payload: IngestPayload) -> ActionResult:
    """采集侧写入原行；重传同一物理行自动去重，不增加报告分母。"""
    return _guard(
        _service().ingest_line,
        payload.source_id, payload.raw_line,
        txn_key=payload.txn_key, event_at=payload.event_at, collect_ref=payload.collect_ref,
    )


@router.post("/reports", response_model=ActionResult)
def generate_report(payload: ReportPayload) -> ActionResult:
    """用对账明细当前确认的原行实际计算解析成功率。"""
    return _guard(
        _service().generate_report,
        payload.txn_key, payload.sources, payload.start, payload.end,
    )


@router.get("/reports")
def list_reports(txn_key: str | None = Query(default=None)) -> dict[str, Any]:
    """报告列表：旧报告与已派单的依据仍可回查。"""
    items = _service().list_reports(txn_key)
    return {"ok": True, "total": len(items), "items": items}


@router.get("/reports/{report_id}")
def get_report(report_id: str) -> dict[str, Any]:
    try:
        return {"ok": True, "item": _service().get_report(report_id)}
    except ReconError as exc:
        return {"ok": False, "message": str(exc)}


@router.post("/reports/{report_id}/confirm", response_model=ActionResult)
def confirm_report(report_id: str) -> ActionResult:
    """确认报告：幂等，并发确认只出一份质量报告。"""
    return _guard(_service().confirm_report, report_id)


@router.post("/reports/{report_id}/refresh", response_model=ActionResult)
def refresh_report(report_id: str) -> ActionResult:
    """查询范围变化后读取被选日志重新计算，生成新报告。"""
    return _guard(_service().refresh_report, report_id)


@router.post("/gaps/detect", response_model=ActionResult)
def detect_gaps(payload: GapDetectPayload) -> ActionResult:
    """按已登记的必需来源找事务缺口；失败原行交纠错而非再当缺失。"""
    return _guard(_service().detect_gaps, payload.txn_key, payload.start, payload.end)


@router.get("/gaps")
def list_gaps(txn_key: str | None = Query(default=None)) -> dict[str, Any]:
    items = _service().list_gaps(txn_key)
    return {"ok": True, "total": len(items), "items": items}


@router.post("/gaps/{gap_id}/dispatch", response_model=ActionResult)
def dispatch_backfill(gap_id: str) -> ActionResult:
    """派补采单：只领取缺失来源对应的时间范围。"""
    return _guard(_service().dispatch_backfill, gap_id)


@router.get("/backfills")
def list_backfills(txn_key: str | None = Query(default=None)) -> dict[str, Any]:
    items = _service().list_backfills(txn_key)
    return {"ok": True, "total": len(items), "items": items}


@router.post("/backfills/{backfill_id}/deliver", response_model=ActionResult)
def deliver_backfill(backfill_id: str, payload: DeliverPayload) -> ActionResult:
    """补采交付：写入实际原行，物理指纹去重。"""
    return _guard(_service().deliver_backfill, backfill_id, payload.lines)


@router.post("/backfills/{backfill_id}/receipt", response_model=ActionResult)
def confirm_receipt(backfill_id: str) -> ActionResult:
    """对账确认回执：核清已完成的交付后关闭缺口。"""
    return _guard(_service().confirm_receipt, backfill_id)
