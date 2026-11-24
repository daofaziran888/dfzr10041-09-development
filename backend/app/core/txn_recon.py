"""事务采集对账：按事务键跨来源定位原行、核算质量报告、发现缺口并驱动补采。

数据流：采集原行 → 事务采集对账明细 → 质量报告 / 事务缺口 → 补采作业 → 回执确认 → 缺口关闭

- 查询入口：按事务键 + 来源 + 时段定位真实原行与各自解析结果；
- 质量报告：用对账明细当前确认的原行实际计算解析成功率，
  有采集引用但因字段缺失失败的行仍计入分母；
- 缺口发现：按已登记的必需来源区分「已收到但解析失败」（交纠错）
  和「这一侧根本没有采到」（开缺口派补采），失败原行不会再当缺失采一遍；
- 补采作业：只领取缺失来源对应的时间范围；交付写入实际原行、对账确认
  回执后缺口才关闭；进程重启后重开未回执缺口，物理原行指纹保证同一行
  不会被重采出第二条，也不允许只改质报比例就把真实缺口结清。
"""
from __future__ import annotations

import hashlib
import json
import re
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from app.core.models import LogEntry, ParseResultStatus, ParseRule
from app.core.pipeline import IngestPipeline

# ---------- 预置事务来源与解析规则 ----------

TXN_SOURCE_GATEWAY = "gateway"
TXN_SOURCE_APPLICATION = "application"
TXN_SOURCE_AUDIT = "audit"

TXN_PARSE_RULES: dict[str, ParseRule] = {
    TXN_SOURCE_GATEWAY: ParseRule(
        rule_id="PARS-TXN-GATEWAY",
        source_id=TXN_SOURCE_GATEWAY,
        regex_pattern=(
            r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) GATEWAY "
            r"(INFO|WARN|ERROR) txn=(\S+) path=(\S+) status=(\d{3})$"
        ),
        field_names=["timestamp", "level", "txn_key", "path", "status"],
        state="生效中",
    ),
    TXN_SOURCE_APPLICATION: ParseRule(
        rule_id="PARS-TXN-APP",
        source_id=TXN_SOURCE_APPLICATION,
        regex_pattern=(
            r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) APP "
            r"(INFO|WARN|ERROR) txn=(\S+?)(?:\s+service=(\S+))?(?:\s+msg=(.+))?$"
        ),
        field_names=["timestamp", "level", "txn_key", "service", "msg"],
        state="生效中",
    ),
    TXN_SOURCE_AUDIT: ParseRule(
        rule_id="PARS-TXN-AUDIT",
        source_id=TXN_SOURCE_AUDIT,
        regex_pattern=(
            r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) AUDIT "
            r"(INFO|WARN|ERROR) txn=(\S+) action=(\S+) result=(\S+)$"
        ),
        field_names=["timestamp", "level", "txn_key", "action", "result"],
        state="生效中",
    ),
}

# 已登记的必需来源：对账按此清单找事务缺口
DEFAULT_REQUIRED_SOURCES = [TXN_SOURCE_GATEWAY, TXN_SOURCE_APPLICATION, TXN_SOURCE_AUDIT]

# ---------- 状态与处置常量 ----------

# 对账明细处置方式
DISPOSITION_NONE = "无"
DISPOSITION_CORRECTION = "纠错"  # 已收到但解析失败 → 交给纠错

# 事务缺口状态
GAP_OPEN = "开放"
GAP_DISPATCHED = "补采中"
GAP_DELIVERED = "待回执"
GAP_CLOSED = "已关闭"

# 补采作业状态
BACKFILL_DISPATCHED = "已派单"
BACKFILL_DELIVERED = "已交付"
BACKFILL_ACKED = "已回执"

# 质量报告状态
REPORT_GENERATED = "已生成"
REPORT_CONFIRMED = "已确认"

_TXN_KEY_PATTERN = re.compile(r"txn=(\S+)")
_TIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
)


class ReconError(Exception):
    """事务采集对账的领域错误：未登记来源、无法归属事务键、缺口未交付等。"""


def _norm_time(value: Any) -> str:
    """把输入时间归一到毫秒精度的可比较字符串。"""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    text = str(value or "").strip()
    for fmt in _TIME_FORMATS:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        except ValueError:
            continue
    raise ReconError(f"无法识别的时间格式：{value!r}")


def _physical_id(source_id: str, raw_line: str) -> str:
    """物理原行指纹：同一来源重传同一物理行得到同一指纹，用于去重。"""
    return hashlib.sha256(f"{source_id}|{raw_line}".encode()).hexdigest()[:16]


@dataclass
class ReconLine:
    """事务采集对账明细：一条按事务键归属到来源的真实原行及其解析结果。"""
    line_id: str
    txn_key: str
    source_id: str
    event_at: str
    raw_line: str
    collect_ref: str            # 采集引用：有引用即计入质量报告分母
    parse_status: str           # apply_parse_rules 返回的解析状态
    parsed_fields: dict[str, str] = field(default_factory=dict)
    physical_id: str = ""
    disposition: str = DISPOSITION_NONE


@dataclass
class TxnQualityReport:
    """事务质量报告：分母对回实际曾选择的来源、时段与确认原行。"""
    report_id: str
    txn_key: str
    sources: list[str]
    period_start: str
    period_end: str
    line_ids: list[str]
    total_lines: int
    ok_lines: int
    parse_success_rate: float
    state: str = REPORT_GENERATED
    supersedes: str = ""
    created_at: str = ""

    def selection_key(self) -> tuple[str, tuple[str, ...], str, str]:
        return (self.txn_key, tuple(self.sources), self.period_start, self.period_end)


@dataclass
class TxnGap:
    """事务缺口：必需来源在事务时段内根本没有采到。"""
    gap_id: str
    txn_key: str
    source_id: str
    period_start: str
    period_end: str
    state: str = GAP_OPEN
    backfill_id: str = ""


@dataclass
class BackfillJob:
    """补采作业：只领取缺失来源对应的时间范围。"""
    backfill_id: str
    gap_id: str
    txn_key: str
    source_id: str
    period_start: str
    period_end: str
    state: str = BACKFILL_DISPATCHED
    delivered_line_ids: list[str] = field(default_factory=list)


class TxnReconEngine:
    """事务采集对账引擎。

    线程安全；state_path 非空时每次变更落盘，重启后载入并执行
    recover()：已交付未回执的缺口重开，已写入的原行保留并参与去重。
    """

    def __init__(
        self,
        *,
        required_sources: list[str] | None = None,
        rules: dict[str, ParseRule] | None = None,
        pipeline: IngestPipeline | None = None,
        state_path: str | Path | None = None,
    ) -> None:
        self._lock = threading.RLock()
        self._pipeline = pipeline or IngestPipeline()
        self._rules: dict[str, ParseRule] = dict(rules or TXN_PARSE_RULES)
        self._required_sources = list(required_sources or DEFAULT_REQUIRED_SOURCES)
        self._lines: list[ReconLine] = []
        self._reports: list[TxnQualityReport] = []
        self._gaps: list[TxnGap] = []
        self._backfills: list[BackfillJob] = []
        self._seq = {"line": 0, "report": 0, "gap": 0, "backfill": 0}
        self._state_path = Path(state_path) if state_path else None
        if self._state_path and self._state_path.exists():
            self._load()
            self.recover()

    # ---------- 原行登记（重传同一物理行去重） ----------

    def ingest_raw_line(
        self,
        source_id: str,
        raw_line: str,
        *,
        txn_key: str | None = None,
        event_at: Any = None,
        collect_ref: str | None = None,
    ) -> ReconLine:
        """登记一条采集原行为对账明细。

        解析状态经 IngestPipeline.apply_parse_rules 取得并留在明细上，
        供质量报告核对分母；同一物理行重传直接返回已有明细，不增加分母。
        """
        with self._lock:
            if source_id not in self._rules:
                raise ReconError(f"未登记的日志来源：{source_id}")
            physical = _physical_id(source_id, raw_line)
            for line in self._lines:
                if line.physical_id == physical:
                    return line
            parse_status, fields = self._parse(source_id, raw_line)
            txn_key = txn_key or fields.get("txn_key") or self._extract_txn_key(raw_line)
            if not txn_key:
                raise ReconError("原行无法归属事务键，已拒绝入账")
            if event_at is None:
                event_at = fields.get("timestamp")
            if event_at is None:
                raise ReconError("原行缺少发生时间，无法按时段对账")
            line = ReconLine(
                line_id=self._next_id("line", "RECL"),
                txn_key=txn_key,
                source_id=source_id,
                event_at=_norm_time(event_at),
                raw_line=raw_line,
                collect_ref=collect_ref or f"COLL-{physical[:12]}",
                parse_status=parse_status,
                parsed_fields=fields,
                physical_id=physical,
            )
            self._lines.append(line)
            self._save()
            return line

    # ---------- 查询入口：按事务键 + 来源 + 时段 ----------

    def query_txn_logs(
        self,
        txn_key: str,
        sources: list[str] | None = None,
        start: Any = None,
        end: Any = None,
    ) -> list[ReconLine]:
        """跨服务查询同一事务的来源日志：返回真实原行与各自解析结果。"""
        with self._lock:
            start_s = _norm_time(start) if start else None
            end_s = _norm_time(end) if end else None
            rows = [line for line in self._lines if line.txn_key == txn_key]
            if sources:
                allowed = set(sources)
                rows = [line for line in rows if line.source_id in allowed]
            if start_s:
                rows = [line for line in rows if line.event_at >= start_s]
            if end_s:
                rows = [line for line in rows if line.event_at <= end_s]
            return sorted(rows, key=lambda line: (line.event_at, line.line_id))

    # ---------- 质量报告 ----------

    def generate_quality_report(
        self,
        txn_key: str,
        sources: list[str],
        start: Any,
        end: Any,
        *,
        supersedes: str = "",
    ) -> TxnQualityReport:
        """用对账明细当前确认的原行实际计算解析成功率。

        分母为所选来源与时段内全部持有采集引用的确认原行——有采集引用
        但因字段缺失等原因解析失败的行仍计入分母。
        """
        with self._lock:
            start_s, end_s = _norm_time(start), _norm_time(end)
            rows = self.query_txn_logs(txn_key, list(sources), start_s, end_s)
            denominator = [line for line in rows if line.collect_ref]
            ok = [line for line in denominator
                  if line.parse_status == ParseResultStatus.OK.value]
            rate = round(len(ok) / len(denominator), 4) if denominator else 0.0
            report = TxnQualityReport(
                report_id=self._next_id("report", "QRTN"),
                txn_key=txn_key,
                sources=list(sources),
                period_start=start_s,
                period_end=end_s,
                line_ids=[line.line_id for line in denominator],
                total_lines=len(denominator),
                ok_lines=len(ok),
                parse_success_rate=rate,
                supersedes=supersedes,
                created_at=datetime.now().isoformat(timespec="seconds"),
            )
            self._reports.append(report)
            self._save()
            return report

    def confirm_report(self, report_id: str) -> TxnQualityReport:
        """确认质量报告：幂等，并发确认只出一份。"""
        with self._lock:
            report = self._find_report(report_id)
            if report.state == REPORT_CONFIRMED:
                return report
            for other in self._reports:
                if (
                    other.state == REPORT_CONFIRMED
                    and other.report_id != report.report_id
                    and other.selection_key() == report.selection_key()
                    and other.line_ids == report.line_ids
                ):
                    return other
            report.state = REPORT_CONFIRMED
            self._save()
            return report

    def refresh_report(self, report_id: str) -> TxnQualityReport:
        """查询范围变化后重算：读取当前被选日志生成新报告，旧报告仍可回查。"""
        with self._lock:
            old = self._find_report(report_id)
            return self.generate_quality_report(
                old.txn_key, old.sources, old.period_start, old.period_end,
                supersedes=old.report_id,
            )

    def get_report(self, report_id: str) -> TxnQualityReport:
        with self._lock:
            return self._find_report(report_id)

    def list_reports(self, txn_key: str | None = None) -> list[TxnQualityReport]:
        with self._lock:
            rows = self._reports
            if txn_key is not None:
                rows = [r for r in rows if r.txn_key == txn_key]
            return sorted(rows, key=lambda r: r.report_id)

    # ---------- 缺口发现与补采 ----------

    def detect_gaps(self, txn_key: str, start: Any, end: Any) -> dict[str, Any]:
        """按已登记的必需来源找事务缺口。

        区分两类问题：时段内一行都没有的来源开缺口（根本没有采到）；
        已在对账中的解析失败原行标记交纠错，不再当缺失采一遍。
        """
        with self._lock:
            start_s, end_s = _norm_time(start), _norm_time(end)
            gaps: list[TxnGap] = []
            correction: list[ReconLine] = []
            for source in self._required_sources:
                rows = [
                    line for line in self._lines
                    if line.txn_key == txn_key and line.source_id == source
                    and start_s <= line.event_at <= end_s
                ]
                if not rows:
                    gaps.append(self._open_gap(txn_key, source, start_s, end_s))
                    continue
                for line in rows:
                    if line.parse_status != ParseResultStatus.OK.value:
                        line.disposition = DISPOSITION_CORRECTION
                        correction.append(line)
            self._save()
            return {"gaps": gaps, "correction_lines": correction}

    def dispatch_backfill(self, gap_id: str) -> BackfillJob:
        """为缺口派补采单：只领取缺失来源对应的时间范围。

        同一缺口已有未回执的补采单时复用原单，避免重复派单。
        """
        with self._lock:
            gap = self._find_gap(gap_id)
            if gap.state == GAP_CLOSED:
                raise ReconError(f"缺口 {gap_id} 已关闭，无需补采")
            for job in self._backfills:
                if job.gap_id == gap.gap_id and job.state != BACKFILL_ACKED:
                    return job
            job = BackfillJob(
                backfill_id=self._next_id("backfill", "BKFL"),
                gap_id=gap.gap_id,
                txn_key=gap.txn_key,
                source_id=gap.source_id,
                period_start=gap.period_start,
                period_end=gap.period_end,
            )
            self._backfills.append(job)
            gap.state = GAP_DISPATCHED
            gap.backfill_id = job.backfill_id
            self._save()
            return job

    def deliver_backfill(self, backfill_id: str, lines: list[dict[str, Any]]) -> BackfillJob:
        """补采交付：把实际原行写入对账明细（物理指纹去重）。"""
        with self._lock:
            job = self._find_backfill(backfill_id)
            if job.state == BACKFILL_ACKED:
                raise ReconError(f"补采单 {backfill_id} 已回执，不能再交付")
            gap = self._find_gap(job.gap_id)
            for item in lines:
                raw_line = str(item.get("raw_line") or "").strip()
                if not raw_line:
                    raise ReconError("补采交付包含空原行")
                line_txn = self._extract_txn_key(raw_line)
                if line_txn and line_txn != job.txn_key:
                    raise ReconError(
                        f"补采原行属于事务 {line_txn}，与缺口事务 {job.txn_key} 不符"
                    )
                line = self.ingest_raw_line(
                    job.source_id,
                    raw_line,
                    txn_key=job.txn_key,
                    event_at=item.get("event_at"),
                    collect_ref=item.get("collect_ref") or f"COLL-{job.backfill_id}",
                )
                if line.line_id not in job.delivered_line_ids:
                    job.delivered_line_ids.append(line.line_id)
            job.state = BACKFILL_DELIVERED
            gap.state = GAP_DELIVERED
            self._save()
            return job

    def confirm_receipt(self, backfill_id: str) -> TxnGap:
        """对账确认回执：核清已完成的交付后关闭缺口。

        必须存在落在缺口来源与时段内的已交付原行；不允许只改质报
        比例就把真实缺口结清。重复确认幂等返回。
        """
        with self._lock:
            job = self._find_backfill(backfill_id)
            gap = self._find_gap(job.gap_id)
            if job.state == BACKFILL_ACKED and gap.state == GAP_CLOSED:
                return gap
            delivered = [
                line for line in self._lines
                if line.line_id in job.delivered_line_ids
                and line.source_id == gap.source_id
                and gap.period_start <= line.event_at <= gap.period_end
            ]
            if not delivered:
                raise ReconError(
                    f"补采单 {backfill_id} 尚未在缺口时段内交付实际原行，不能结清缺口"
                )
            job.state = BACKFILL_ACKED
            gap.state = GAP_CLOSED
            self._save()
            return gap

    def recover(self) -> list[TxnGap]:
        """进程重启恢复：已交付但未收到回执的缺口重开。

        已写入的实际原行保留在明细中并继续按物理指纹去重，
        恢复后核清交付、确认回执即可关闭原缺口。
        """
        with self._lock:
            reopened = [
                gap for gap in self._gaps if gap.state == GAP_DELIVERED
            ]
            for gap in reopened:
                gap.state = GAP_OPEN
            if reopened:
                self._save()
            return reopened

    # ---------- 统计辅助（供质量报告镜像使用） ----------

    def source_coverage(self, txn_key: str, start: Any, end: Any) -> tuple[int, int]:
        """必需来源中已在时段内采到的数量，返回 (已覆盖, 必需总数)。"""
        with self._lock:
            start_s, end_s = _norm_time(start), _norm_time(end)
            present = {
                line.source_id for line in self._lines
                if line.txn_key == txn_key and start_s <= line.event_at <= end_s
            }
            covered = sum(1 for s in self._required_sources if s in present)
            return covered, len(self._required_sources)

    def field_coverage(self, line_ids: list[str]) -> float:
        """已填充字段数 / 期望字段总数（按来源解析规则的字段定义）。"""
        with self._lock:
            expected = 0
            filled = 0
            for line in self._lines:
                if line.line_id not in line_ids:
                    continue
                rule = self._rules.get(line.source_id)
                names = rule.field_names if rule else []
                expected += len(names)
                filled += sum(1 for name in names if line.parsed_fields.get(name))
            return round(filled / expected, 4) if expected else 0.0

    # ---------- 查询辅助 ----------

    def list_lines(self) -> list[ReconLine]:
        with self._lock:
            return sorted(self._lines, key=lambda line: (line.event_at, line.line_id))

    def get_line(self, line_id: str) -> ReconLine:
        with self._lock:
            for line in self._lines:
                if line.line_id == line_id:
                    return line
            raise ReconError(f"对账明细 {line_id} 不存在")

    def set_disposition(self, line_id: str, disposition: str) -> ReconLine:
        """更新明细处置方式（如人工提交纠错）。"""
        with self._lock:
            line = self.get_line(line_id)
            line.disposition = disposition
            self._save()
            return line

    def list_gaps(self, txn_key: str | None = None) -> list[TxnGap]:
        with self._lock:
            rows = self._gaps
            if txn_key is not None:
                rows = [g for g in rows if g.txn_key == txn_key]
            return sorted(rows, key=lambda g: g.gap_id)

    def get_gap(self, gap_id: str) -> TxnGap:
        with self._lock:
            return self._find_gap(gap_id)

    def list_backfills(self, txn_key: str | None = None) -> list[BackfillJob]:
        with self._lock:
            rows = self._backfills
            if txn_key is not None:
                rows = [b for b in rows if b.txn_key == txn_key]
            return sorted(rows, key=lambda b: b.backfill_id)

    def get_backfill(self, backfill_id: str) -> BackfillJob:
        with self._lock:
            return self._find_backfill(backfill_id)

    @property
    def required_sources(self) -> list[str]:
        return list(self._required_sources)

    # ---------- 内部实现 ----------

    def _parse(self, source_id: str, raw_line: str) -> tuple[str, dict[str, str]]:
        """经采集管道的 apply_parse_rules 取得解析状态与字段。"""
        rule = self._rules[source_id]
        entry = LogEntry(raw_line=raw_line, line_number=1, source_id=source_id)
        self._pipeline.apply_parse_rules([entry], rule)
        return entry.parse_status.value, dict(entry.parsed_fields or {})

    @staticmethod
    def _extract_txn_key(raw_line: str) -> str:
        match = _TXN_KEY_PATTERN.search(raw_line)
        return match.group(1) if match else ""

    def _open_gap(self, txn_key: str, source_id: str, start: str, end: str) -> TxnGap:
        for gap in self._gaps:
            if (
                gap.txn_key == txn_key and gap.source_id == source_id
                and gap.period_start == start and gap.period_end == end
                and gap.state != GAP_CLOSED
            ):
                return gap
        gap = TxnGap(
            gap_id=self._next_id("gap", "GAPD"),
            txn_key=txn_key,
            source_id=source_id,
            period_start=start,
            period_end=end,
        )
        self._gaps.append(gap)
        return gap

    def _next_id(self, kind: str, prefix: str) -> str:
        self._seq[kind] += 1
        return f"{prefix}-{self._seq[kind]:06d}"

    def _find_report(self, report_id: str) -> TxnQualityReport:
        for report in self._reports:
            if report.report_id == report_id:
                return report
        raise ReconError(f"质量报告 {report_id} 不存在")

    def _find_gap(self, gap_id: str) -> TxnGap:
        for gap in self._gaps:
            if gap.gap_id == gap_id:
                return gap
        raise ReconError(f"事务缺口 {gap_id} 不存在")

    def _find_backfill(self, backfill_id: str) -> BackfillJob:
        for job in self._backfills:
            if job.backfill_id == backfill_id:
                return job
        raise ReconError(f"补采作业 {backfill_id} 不存在")

    # ---------- 持久化 ----------

    def _save(self) -> None:
        if not self._state_path:
            return
        payload = {
            "seq": self._seq,
            "required_sources": self._required_sources,
            "lines": [asdict(line) for line in self._lines],
            "reports": [asdict(report) for report in self._reports],
            "gaps": [asdict(gap) for gap in self._gaps],
            "backfills": [asdict(job) for job in self._backfills],
        }
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._state_path.with_suffix(self._state_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
        tmp_path.replace(self._state_path)

    def _load(self) -> None:
        payload = json.loads(self._state_path.read_text(encoding="utf-8"))
        self._seq = {**self._seq, **payload.get("seq", {})}
        self._required_sources = list(payload.get("required_sources", self._required_sources))
        self._lines = [ReconLine(**item) for item in payload.get("lines", [])]
        self._reports = [TxnQualityReport(**item) for item in payload.get("reports", [])]
        self._gaps = [TxnGap(**item) for item in payload.get("gaps", [])]
        self._backfills = [BackfillJob(**item) for item in payload.get("backfills", [])]


def make_txn_recon_engine(state_path: str | Path | None = None) -> TxnReconEngine:
    """工厂函数：创建预配置的事务采集对账引擎。"""
    return TxnReconEngine(state_path=state_path)
