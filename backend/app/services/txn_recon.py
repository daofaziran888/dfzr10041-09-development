"""事务采集对账领域服务：对账明细经通用资源接口可见，真实流转由对账引擎完成。"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from app.core.txn_recon import (
    BACKFILL_ACKED,
    DISPOSITION_CORRECTION,
    REPORT_CONFIRMED,
    TxnQualityReport,
    TxnReconEngine,
)
from app.services.base import BaseModuleService
from app.store import store

# 运行期状态文件：进程重启后引擎从这里恢复缺口与补采现场
DEFAULT_STATE_PATH = Path(__file__).resolve().parents[2] / ".data" / "txn_recon_state.json"

_PARSE_STATUS_LABELS = {
    "ok": "解析成功",
    "field_missing": "字段缺失",
    "format_mismatch": "格式不匹配",
    "encoding_error": "编码错误",
    "line_empty": "空行",
}

# 演示事务：网关成功、应用字段缺失、审计全缺，另有一条同时段外同码日志
_DEMO_TXN = "TXN-1001"
_DEMO_PERIOD = ("2026-11-15 14:30:00", "2026-11-15 14:31:00")
_DEMO_LINES = [
    ("gateway", "2026-11-15 14:30:05.123 GATEWAY INFO txn=TXN-1001 path=/api/pay status=200"),
    ("application", "2026-11-15 14:30:05.789 APP ERROR txn=TXN-1001"),
    ("application", "2026-11-15 15:00:00.000 APP INFO txn=TXN-1001 service=order msg=补单对账"),
    ("gateway", "2026-11-15 14:35:01.000 GATEWAY INFO txn=TXN-1002 path=/api/query status=200"),
    ("application", "2026-11-15 14:35:01.200 APP INFO txn=TXN-1002 service=order msg=查询完成"),
    ("audit", "2026-11-15 14:35:01.400 AUDIT INFO txn=TXN-1002 action=read result=ok"),
]


class TxnReconService(BaseModuleService):
    MODULE = "txn_recon"
    ENTITY = "对账明细"
    OPERATION_TERM = "对账"
    LIST_FIELDS = ["明细编号", "事务键", "日志来源", "发生时间", "解析状态", "采集引用", "处置方式", "物理指纹"]
    REQUIRED_FIELDS = ["事务键", "日志来源"]
    STATUS_ORDER = ["已确认", "待纠错", "已纠正", "已归档"]
    ACTION_RULES = {"提交纠错": "待纠错", "标记纠正": "已纠正", "归档明细": "已归档"}
    NEGATIVE_ACTIONS = ["提交纠错"]

    def __init__(self, engine: TxnReconEngine | None = None, *, seed_demo: bool = True) -> None:
        self._engine = engine or TxnReconEngine(state_path=DEFAULT_STATE_PATH)
        if seed_demo and not self._engine.list_lines():
            self._seed_demo()
        self._sync_store()

    # ---------- 通用资源接口 ----------

    def list_entries(self, **kwargs: Any):
        self._sync_store()
        return super().list_entries(**kwargs)

    def run_action(self, entry_id: int, action: str):
        entry, message = super().run_action(entry_id, action)
        if entry is not None and action == "提交纠错":
            line = self._find_line_by_code(str(entry.get("明细编号", "")))
            if line is not None:
                self._engine.set_disposition(line.line_id, DISPOSITION_CORRECTION)
            self._sync_store()
            entry = store.find(self.MODULE, entry_id)
        return entry, message

    # ---------- 查询入口与质量报告 ----------

    def query_txn_logs(self, txn_key: str, sources: list[str] | None,
                       start: str | None, end: str | None) -> list[dict[str, Any]]:
        lines = self._engine.query_txn_logs(txn_key, sources or None, start, end)
        return [self._line_payload(line) for line in lines]

    def ingest_line(self, source_id: str, raw_line: str, *, txn_key: str | None = None,
                    event_at: str | None = None, collect_ref: str | None = None) -> dict[str, Any]:
        line = self._engine.ingest_raw_line(
            source_id, raw_line, txn_key=txn_key, event_at=event_at, collect_ref=collect_ref)
        self._sync_store()
        return self._line_payload(line)

    def generate_report(self, txn_key: str, sources: list[str], start: str, end: str) -> dict[str, Any]:
        report = self._engine.generate_quality_report(txn_key, sources, start, end)
        self._mirror_quality_report(report)
        return self._report_payload(report)

    def confirm_report(self, report_id: str) -> dict[str, Any]:
        report = self._engine.confirm_report(report_id)
        self._mirror_quality_report(report)
        return self._report_payload(report)

    def refresh_report(self, report_id: str) -> dict[str, Any]:
        report = self._engine.refresh_report(report_id)
        self._mirror_quality_report(report)
        return self._report_payload(report)

    def list_reports(self, txn_key: str | None = None) -> list[dict[str, Any]]:
        return [self._report_payload(r) for r in self._engine.list_reports(txn_key)]

    def get_report(self, report_id: str) -> dict[str, Any]:
        return self._report_payload(self._engine.get_report(report_id))

    # ---------- 缺口与补采 ----------

    def detect_gaps(self, txn_key: str, start: str, end: str) -> dict[str, Any]:
        result = self._engine.detect_gaps(txn_key, start, end)
        self._sync_store()
        return {
            "gaps": [self._gap_payload(g) for g in result["gaps"]],
            "correction_lines": [self._line_payload(l) for l in result["correction_lines"]],
        }

    def dispatch_backfill(self, gap_id: str) -> dict[str, Any]:
        return self._backfill_payload(self._engine.dispatch_backfill(gap_id))

    def deliver_backfill(self, backfill_id: str, lines: list[dict[str, Any]]) -> dict[str, Any]:
        job = self._engine.deliver_backfill(backfill_id, lines)
        self._sync_store()
        return self._backfill_payload(job)

    def confirm_receipt(self, backfill_id: str) -> dict[str, Any]:
        return self._gap_payload(self._engine.confirm_receipt(backfill_id))

    def list_gaps(self, txn_key: str | None = None) -> list[dict[str, Any]]:
        return [self._gap_payload(g) for g in self._engine.list_gaps(txn_key)]

    def list_backfills(self, txn_key: str | None = None) -> list[dict[str, Any]]:
        return [self._backfill_payload(b) for b in self._engine.list_backfills(txn_key)]

    # ---------- 内部实现 ----------

    def _seed_demo(self) -> None:
        for source_id, raw_line in _DEMO_LINES:
            self._engine.ingest_raw_line(source_id, raw_line)
        self._engine.detect_gaps(_DEMO_TXN, *_DEMO_PERIOD)

    def _sync_store(self) -> None:
        """把引擎当前确认的对账明细同步进通用列表，人工流转状态不被覆盖。"""
        rows = store.rows(self.MODULE)
        by_code = {str(row.get("明细编号", "")): row for row in rows}
        for line in self._engine.list_lines():
            row = by_code.get(line.line_id)
            if row is None:
                row = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
                rows.append(row)
                by_code[line.line_id] = row
            row.update({
                "明细编号": line.line_id,
                "事务键": line.txn_key,
                "日志来源": line.source_id,
                "发生时间": line.event_at,
                "解析状态": _PARSE_STATUS_LABELS.get(line.parse_status, line.parse_status),
                "采集引用": line.collect_ref,
                "处置方式": line.disposition,
                "物理指纹": line.physical_id,
            })
            if row.get("status") not in ("已纠正", "已归档"):
                row["status"] = "已确认" if line.parse_status == "ok" else "待纠错"
            row["pending"] = row.get("status") != "已归档"
            row["abnormal"] = line.parse_status != "ok"

    def _mirror_quality_report(self, report: TxnQualityReport) -> None:
        """把按对账明细算出的质量报告镜像到 quality_report 模块。"""
        rows = store.rows("quality_report")
        row = next((r for r in rows if r.get("报告编号") == report.report_id), None)
        if row is None:
            row = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
            rows.append(row)
        covered, required = self._engine.source_coverage(
            report.txn_key, report.period_start, report.period_end)
        confirmed = report.state == REPORT_CONFIRMED
        row.update({
            "报告编号": report.report_id,
            "日志来源": f"{report.txn_key}({'+'.join(report.sources)})",
            "完整率": f"{(covered / required) if required else 0.0:.4f}",
            "解析成功率": f"{report.parse_success_rate:.4f}",
            "字段覆盖率": f"{self._engine.field_coverage(report.line_ids):.4f}",
            "时效延迟": "0",
            "评估周期": f"{report.period_start}~{report.period_end}",
            "报告状态": "已发布" if confirmed else "已生成",
            "status": "已发布" if confirmed else "已生成",
            "pending": not confirmed,
            "abnormal": report.parse_success_rate < 1.0,
        })

    def _find_line_by_code(self, line_id: str):
        for line in self._engine.list_lines():
            if line.line_id == line_id:
                return line
        return None

    @staticmethod
    def _line_payload(line) -> dict[str, Any]:
        return {
            "line_id": line.line_id,
            "txn_key": line.txn_key,
            "source_id": line.source_id,
            "event_at": line.event_at,
            "raw_line": line.raw_line,
            "collect_ref": line.collect_ref,
            "parse_status": line.parse_status,
            "parsed_fields": line.parsed_fields,
            "physical_id": line.physical_id,
            "disposition": line.disposition,
        }

    @staticmethod
    def _report_payload(report: TxnQualityReport) -> dict[str, Any]:
        return {
            "report_id": report.report_id,
            "txn_key": report.txn_key,
            "sources": report.sources,
            "period_start": report.period_start,
            "period_end": report.period_end,
            "line_ids": report.line_ids,
            "total_lines": report.total_lines,
            "ok_lines": report.ok_lines,
            "parse_success_rate": report.parse_success_rate,
            "state": report.state,
            "supersedes": report.supersedes,
            "created_at": report.created_at,
        }

    @staticmethod
    def _gap_payload(gap) -> dict[str, Any]:
        return {
            "gap_id": gap.gap_id,
            "txn_key": gap.txn_key,
            "source_id": gap.source_id,
            "period_start": gap.period_start,
            "period_end": gap.period_end,
            "state": gap.state,
            "backfill_id": gap.backfill_id,
        }

    @staticmethod
    def _backfill_payload(job) -> dict[str, Any]:
        return {
            "backfill_id": job.backfill_id,
            "gap_id": job.gap_id,
            "txn_key": job.txn_key,
            "source_id": job.source_id,
            "period_start": job.period_start,
            "period_end": job.period_end,
            "state": job.state,
            "delivered_line_ids": job.delivered_line_ids,
            "acked": job.state == BACKFILL_ACKED,
        }
