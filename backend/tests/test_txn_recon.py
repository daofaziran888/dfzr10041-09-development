"""测试：事务采集对账明细——跨服务事务日志查询、质量报告、缺口补采与恢复。

覆盖验收场景：
- 按事务键、来源和时段定位真实原行与各自解析结果（同码范围外记录不混入）
- 质量报告用当前确认原行实际计算解析成功率，字段缺失失败行计入分母
- apply_parse_rules 的解析状态交给质量报告核对分母；范围变化后重算
- 区分「已收到但解析失败」（交纠错）与「根本没有采到」（开缺口派补采）
- 补采只领取缺失来源的时间范围；交付+回执后缺口关闭，旧报告可回查
- 进程重启恢复：重开未回执缺口、核清交付、物理行不重采、不改质报结清
- 已领报告快照不被补录改写，刷新后按当前原行重算
- 重传同一物理行不增加分母；并发确认只出一份质量报告
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.models import ParseResultStatus
from app.core.txn_recon import (
    BACKFILL_ACKED,
    BACKFILL_DELIVERED,
    DISPOSITION_CORRECTION,
    GAP_CLOSED,
    GAP_DISPATCHED,
    GAP_OPEN,
    REPORT_CONFIRMED,
    REPORT_GENERATED,
    ReconError,
    TxnReconEngine,
    make_txn_recon_engine,
)

# ---------- 测试数据：同一事务的跨来源原行 ----------

TXN = "TXN-1001"
PERIOD = ("2026-11-15 14:30:00", "2026-11-15 14:31:00")
SOURCES_GW_APP = ["gateway", "application"]

# 网关解析成功
GW_OK = "2026-11-15 14:30:05.123 GATEWAY INFO txn=TXN-1001 path=/api/pay status=200"
# 应用已收到但字段缺失（service/msg 未捕获）
APP_FIELD_MISSING = "2026-11-15 14:30:05.789 APP ERROR txn=TXN-1001"
# 别的时段恰巧同码的日志（范围外记录）
APP_OUT_OF_PERIOD = "2026-11-15 15:00:00.000 APP INFO txn=TXN-1001 service=order msg=补单对账"
# 交错处补来的可归属成功原行
APP_OK_INTERLEAVED = "2026-11-15 14:30:05.500 APP INFO txn=TXN-1001 service=order msg=订单提交成功"
# 审计来源补采交付的原行
AUDIT_OK = "2026-11-15 14:30:06.000 AUDIT INFO txn=TXN-1001 action=commit result=ok"


def _engine_with_two_lines() -> TxnReconEngine:
    """网关成功 + 应用字段缺失 + 范围外同码记录。"""
    engine = make_txn_recon_engine()
    engine.ingest_raw_line("gateway", GW_OK)
    engine.ingest_raw_line("application", APP_FIELD_MISSING)
    engine.ingest_raw_line("application", APP_OUT_OF_PERIOD)
    return engine


class TestTxnLogQueryEntry:
    """事务日志查询入口：按事务键、来源和时段定位真实原行。"""

    def test_first_query_finds_exactly_two_rows(self):
        """同码范围外记录不混入：首查只命中时段内的两行。"""
        engine = _engine_with_two_lines()
        rows = engine.query_txn_logs(TXN, SOURCES_GW_APP, *PERIOD)
        assert len(rows) == 2
        assert rows[0].raw_line == GW_OK
        assert rows[1].raw_line == APP_FIELD_MISSING

    def test_query_returns_real_raw_lines_and_parse_results(self):
        """每个来源的真实原行与各自解析结果都能定位。"""
        engine = _engine_with_two_lines()
        rows = engine.query_txn_logs(TXN, SOURCES_GW_APP, *PERIOD)
        gateway, app = rows
        assert gateway.source_id == "gateway"
        assert gateway.parse_status == ParseResultStatus.OK.value
        assert gateway.parsed_fields["path"] == "/api/pay"
        assert gateway.parsed_fields["status"] == "200"
        assert app.source_id == "application"
        assert app.parse_status == ParseResultStatus.FIELD_MISSING.value

    def test_parse_status_handed_over_from_apply_parse_rules(self):
        """明细上的解析状态与 apply_parse_rules 的返回一致。"""
        engine = _engine_with_two_lines()
        status, fields = engine._parse("application", APP_FIELD_MISSING)
        assert status == ParseResultStatus.FIELD_MISSING.value
        assert fields.get("service") is None
        status_ok, fields_ok = engine._parse("gateway", GW_OK)
        assert status_ok == ParseResultStatus.OK.value
        assert fields_ok["txn_key"] == TXN

    def test_out_of_period_same_code_record_excluded(self):
        """别的时段恰巧同码的日志不进首查结果，放宽时段才可见。"""
        engine = _engine_with_two_lines()
        rows = engine.query_txn_logs(TXN, SOURCES_GW_APP, *PERIOD)
        assert all(r.raw_line != APP_OUT_OF_PERIOD for r in rows)
        wide = engine.query_txn_logs(TXN, SOURCES_GW_APP, "2026-11-15 14:30:00", "2026-11-15 16:00:00")
        assert len(wide) == 3
        assert any(r.raw_line == APP_OUT_OF_PERIOD for r in wide)


class TestQualityReportFromRecon:
    """质量报告：用对账明细当前确认的原行实际计算解析成功率。"""

    def test_field_missing_line_counts_in_denominator(self):
        """有采集引用但字段缺失失败的行仍计入分母：报告为 1/2。"""
        engine = _engine_with_two_lines()
        report = engine.generate_quality_report(TXN, SOURCES_GW_APP, *PERIOD)
        assert report.total_lines == 2
        assert report.ok_lines == 1
        assert report.parse_success_rate == 0.5
        assert report.state == REPORT_GENERATED

    def test_denominator_maps_back_to_selected_sources_and_period(self):
        """每次报告的分母对回实际曾选择的来源、时段与原行。"""
        engine = _engine_with_two_lines()
        report = engine.generate_quality_report(TXN, SOURCES_GW_APP, *PERIOD)
        assert report.sources == SOURCES_GW_APP
        assert report.period_start <= report.period_end
        lines = [engine.get_line(line_id) for line_id in report.line_ids]
        assert len(lines) == 2
        assert all(line.collect_ref for line in lines)
        assert all(report.period_start <= line.event_at <= report.period_end for line in lines)
        assert all(line.source_id in SOURCES_GW_APP for line in lines)

    def test_issued_report_kept_after_late_arrival_then_refresh(self):
        """已领报告保持 1/2；刷新后读三条则为 2/3。"""
        engine = _engine_with_two_lines()
        issued = engine.generate_quality_report(TXN, SOURCES_GW_APP, *PERIOD)
        assert issued.parse_success_rate == 0.5

        # 报告未确认期间，同期限内应用补来一条可归属的成功原行
        engine.ingest_raw_line("application", APP_OK_INTERLEAVED)

        # 首查范围与已确认的补录冲突：沿用已领报告，仍是 1/2
        kept = engine.get_report(issued.report_id)
        assert kept.total_lines == 2
        assert kept.ok_lines == 1
        assert kept.parse_success_rate == 0.5

        # 刷新后读取被选日志重新计算：三条，2/3
        refreshed = engine.refresh_report(issued.report_id)
        assert refreshed.report_id != issued.report_id
        assert refreshed.supersedes == issued.report_id
        assert refreshed.total_lines == 3
        assert refreshed.ok_lines == 2
        assert abs(refreshed.parse_success_rate - round(2 / 3, 4)) < 1e-10

        # 旧报告仍可回查，数值不变
        old = engine.get_report(issued.report_id)
        assert old.parse_success_rate == 0.5
        assert len(engine.list_reports(TXN)) == 2

    def test_retransmission_does_not_inflate_denominator(self):
        """重传同一物理行不增加分母。"""
        engine = make_txn_recon_engine()
        first = engine.ingest_raw_line("gateway", GW_OK)
        again = engine.ingest_raw_line("gateway", GW_OK, collect_ref="COLL-RETRANSMIT")
        assert first.line_id == again.line_id
        assert len(engine.list_lines()) == 1
        report = engine.generate_quality_report(TXN, ["gateway"], *PERIOD)
        assert report.total_lines == 1
        assert report.ok_lines == 1

    def test_concurrent_confirm_produces_single_report(self):
        """并发确认只出一份质量报告；重复确认幂等。"""
        engine = _engine_with_two_lines()
        report = engine.generate_quality_report(TXN, SOURCES_GW_APP, *PERIOD)

        results: list[str] = []
        barrier = threading.Barrier(8)

        def confirm():
            barrier.wait()
            results.append(engine.confirm_report(report.report_id).report_id)

        threads = [threading.Thread(target=confirm) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert set(results) == {report.report_id}
        confirmed = [r for r in engine.list_reports(TXN) if r.state == REPORT_CONFIRMED]
        assert len(confirmed) == 1
        # 顺序重复确认也幂等
        again = engine.confirm_report(report.report_id)
        assert again.report_id == report.report_id
        assert len(engine.list_reports(TXN)) == 1


class TestGapDetectionAndBackfill:
    """缺口发现：区分「已收到但解析失败」与「根本没有采到」。"""

    def _engine_with_audit_missing(self) -> TxnReconEngine:
        engine = make_txn_recon_engine()
        engine.ingest_raw_line("gateway", GW_OK)
        engine.ingest_raw_line("application", APP_FIELD_MISSING)
        return engine

    def test_gap_only_for_never_collected_source(self):
        """网关成功、应用失败、审计全缺：只对审计来源开缺口。"""
        engine = self._engine_with_audit_missing()
        result = engine.detect_gaps(TXN, *PERIOD)
        gaps = result["gaps"]
        assert len(gaps) == 1
        assert gaps[0].source_id == "audit"
        assert gaps[0].state == GAP_OPEN

    def test_failed_lines_go_to_correction_not_backfill(self):
        """已收到但解析失败的原行交给纠错，不再当缺失采一遍。"""
        engine = self._engine_with_audit_missing()
        result = engine.detect_gaps(TXN, *PERIOD)
        correction = result["correction_lines"]
        assert len(correction) == 1
        assert correction[0].raw_line == APP_FIELD_MISSING
        assert correction[0].disposition == DISPOSITION_CORRECTION
        # 应用来源有失败原行，不应被开缺口
        assert all(g.source_id != "application" for g in result["gaps"])

    def test_backfill_claims_only_missing_source_period(self):
        """补采作业只领取缺失来源对应的时间范围。"""
        engine = self._engine_with_audit_missing()
        gap = engine.detect_gaps(TXN, *PERIOD)["gaps"][0]
        job = engine.dispatch_backfill(gap.gap_id)
        assert job.source_id == "audit"
        assert job.period_start == gap.period_start
        assert job.period_end == gap.period_end
        assert job.state == "已派单"
        assert engine.get_gap(gap.gap_id).state == GAP_DISPATCHED
        # 重复派单复用同一单
        again = engine.dispatch_backfill(gap.gap_id)
        assert again.backfill_id == job.backfill_id

    def test_audit_backfill_closes_gap_and_keeps_history(self):
        """审计补齐后缺口关闭；旧报告与已派单的依据仍可回查。"""
        engine = self._engine_with_audit_missing()
        issued = engine.generate_quality_report(TXN, ["gateway", "application", "audit"], *PERIOD)
        gap = engine.detect_gaps(TXN, *PERIOD)["gaps"][0]
        job = engine.dispatch_backfill(gap.gap_id)

        engine.deliver_backfill(job.backfill_id, [{"raw_line": AUDIT_OK}])
        assert engine.get_backfill(job.backfill_id).state == BACKFILL_DELIVERED
        assert engine.get_gap(gap.gap_id).state == "待回执"

        closed = engine.confirm_receipt(job.backfill_id)
        assert closed.state == GAP_CLOSED
        assert engine.get_backfill(job.backfill_id).state == BACKFILL_ACKED

        # 审计原行真实入账，缺口不再出现
        rows = engine.query_txn_logs(TXN, ["audit"], *PERIOD)
        assert len(rows) == 1 and rows[0].raw_line == AUDIT_OK
        assert engine.detect_gaps(TXN, *PERIOD)["gaps"] == []

        # 旧报告与已派单依据仍可回查
        old = engine.get_report(issued.report_id)
        assert old.total_lines == 2 and old.parse_success_rate == 0.5
        basis = engine.get_backfill(job.backfill_id)
        assert basis.source_id == "audit" and basis.gap_id == gap.gap_id
        assert engine.get_gap(gap.gap_id).state == GAP_CLOSED

    def test_cannot_close_gap_without_real_delivery(self):
        """不能只改质报比例就把真实缺口结清：未交付不允许回执。"""
        engine = self._engine_with_audit_missing()
        gap = engine.detect_gaps(TXN, *PERIOD)["gaps"][0]
        job = engine.dispatch_backfill(gap.gap_id)
        try:
            engine.confirm_receipt(job.backfill_id)
            assert False, "未交付的补采单不应允许回执结清"
        except ReconError:
            pass
        assert engine.get_gap(gap.gap_id).state != GAP_CLOSED

    def test_detect_gaps_idempotent(self):
        """重复检测不重复开缺口。"""
        engine = self._engine_with_audit_missing()
        first = engine.detect_gaps(TXN, *PERIOD)["gaps"][0]
        second = engine.detect_gaps(TXN, *PERIOD)["gaps"][0]
        assert first.gap_id == second.gap_id
        assert len(engine.list_gaps(TXN)) == 1


class TestRestartRecovery:
    """进程重启恢复：重开未回执缺口，核清交付，物理行不重采。"""

    def test_recovery_reopens_unacked_gap_and_dedups_physical_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = os.path.join(tmp, "recon.json")
            engine = make_txn_recon_engine(state_path=state)
            engine.ingest_raw_line("gateway", GW_OK)
            engine.ingest_raw_line("application", APP_FIELD_MISSING)
            gap = engine.detect_gaps(TXN, *PERIOD)["gaps"][0]
            job = engine.dispatch_backfill(gap.gap_id)
            # 审计补采已写入实际原行，但对账尚未收到回执
            engine.deliver_backfill(job.backfill_id, [{"raw_line": AUDIT_OK}])
            assert engine.get_gap(gap.gap_id).state == "待回执"
            lines_before = len(engine.list_lines())

            # 进程重启：从同一状态文件恢复
            restored = make_txn_recon_engine(state_path=state)
            reopened = restored.recover()
            assert len(reopened) == 0  # 载入时已恢复，再次调用无副作用
            gap_after = restored.get_gap(gap.gap_id)
            assert gap_after.state == GAP_OPEN  # 重开原缺口
            assert len(restored.list_lines()) == lines_before  # 交付的原行仍在

            # 恢复后重交付同一物理原行：不得重采出第二条
            job_after = restored.dispatch_backfill(gap.gap_id)
            assert job_after.backfill_id == job.backfill_id  # 复用原补采单
            restored.deliver_backfill(job.backfill_id, [{"raw_line": AUDIT_OK}])
            audit_rows = restored.query_txn_logs(TXN, ["audit"], *PERIOD)
            assert len(audit_rows) == 1
            assert audit_rows[0].raw_line == AUDIT_OK

            # 核清审计来源已经完成的交付后，缺口才关闭
            closed = restored.confirm_receipt(job.backfill_id)
            assert closed.state == GAP_CLOSED
            assert restored.get_backfill(job.backfill_id).state == BACKFILL_ACKED

            # 质报按真实原行计算：审计补齐后三行两成功一失败
            report = restored.generate_quality_report(
                TXN, ["gateway", "application", "audit"], *PERIOD)
            assert report.total_lines == 3
            assert report.ok_lines == 2

    def test_recovery_keeps_reports_and_dispatch_basis(self):
        """重启后旧报告与已派单依据仍可回查。"""
        with tempfile.TemporaryDirectory() as tmp:
            state = os.path.join(tmp, "recon.json")
            engine = make_txn_recon_engine(state_path=state)
            engine.ingest_raw_line("gateway", GW_OK)
            engine.ingest_raw_line("application", APP_FIELD_MISSING)
            issued = engine.generate_quality_report(TXN, SOURCES_GW_APP, *PERIOD)
            gap = engine.detect_gaps(TXN, *PERIOD)["gaps"][0]
            job = engine.dispatch_backfill(gap.gap_id)

            restored = make_txn_recon_engine(state_path=state)
            old = restored.get_report(issued.report_id)
            assert old.parse_success_rate == 0.5
            basis = restored.get_backfill(job.backfill_id)
            assert basis.source_id == "audit"
            assert restored.get_gap(gap.gap_id).state == GAP_DISPATCHED


class TestAcceptanceInterleavedSupplement:
    """最终验收：按来源和时段准确查询，交错补录后执行实际质量报告。"""

    def test_interleaved_success_line_full_flow(self):
        engine = _engine_with_two_lines()

        # 1. 按来源和时段准确查询两条原行
        rows = engine.query_txn_logs(TXN, SOURCES_GW_APP, *PERIOD)
        assert [r.raw_line for r in rows] == [GW_OK, APP_FIELD_MISSING]

        # 2. 首次质量报告为 1/2
        first = engine.generate_quality_report(TXN, SOURCES_GW_APP, *PERIOD)
        assert (first.ok_lines, first.total_lines) == (1, 2)

        # 3. 在交错处补一条可归属的成功原行
        engine.ingest_raw_line("application", APP_OK_INTERLEAVED)

        # 4. 已领报告不变，刷新后执行实际质量报告
        assert engine.get_report(first.report_id).parse_success_rate == 0.5
        current = engine.refresh_report(first.report_id)

        # 5. 检查本次比例：2/3
        assert current.total_lines == 3
        assert current.ok_lines == 2
        assert abs(current.parse_success_rate - round(2 / 3, 4)) < 1e-10

        # 6. 检查原行详情：三行按时间交错排列，各自解析结果正确
        rows = engine.query_txn_logs(TXN, SOURCES_GW_APP, *PERIOD)
        assert [r.raw_line for r in rows] == [GW_OK, APP_OK_INTERLEAVED, APP_FIELD_MISSING]
        assert rows[0].parse_status == ParseResultStatus.OK.value
        assert rows[1].parse_status == ParseResultStatus.OK.value
        assert rows[1].parsed_fields["service"] == "order"
        assert rows[2].parse_status == ParseResultStatus.FIELD_MISSING.value

        # 7. 同码的范围外记录：不进查询结果，也不进报告分母
        assert all(r.raw_line != APP_OUT_OF_PERIOD for r in rows)
        out_line = next(l for l in engine.list_lines() if l.raw_line == APP_OUT_OF_PERIOD)
        assert out_line.line_id not in current.line_ids
        outside = engine.query_txn_logs(TXN, None, "2026-11-15 14:45:00", "2026-11-15 15:30:00")
        assert [r.raw_line for r in outside] == [APP_OUT_OF_PERIOD]


# ---------- 运行入口（确定性测试，不依赖 pytest） ----------

if __name__ == "__main__":
    import traceback

    tests = {
        "query_first_finds_two_rows": TestTxnLogQueryEntry().test_first_query_finds_exactly_two_rows,
        "query_returns_raw_lines_and_parse_results": TestTxnLogQueryEntry().test_query_returns_real_raw_lines_and_parse_results,
        "parse_status_from_apply_parse_rules": TestTxnLogQueryEntry().test_parse_status_handed_over_from_apply_parse_rules,
        "out_of_period_same_code_excluded": TestTxnLogQueryEntry().test_out_of_period_same_code_record_excluded,
        "report_field_missing_in_denominator": TestQualityReportFromRecon().test_field_missing_line_counts_in_denominator,
        "report_denominator_maps_to_selection": TestQualityReportFromRecon().test_denominator_maps_back_to_selected_sources_and_period,
        "report_kept_then_refresh": TestQualityReportFromRecon().test_issued_report_kept_after_late_arrival_then_refresh,
        "retransmission_not_inflating_denominator": TestQualityReportFromRecon().test_retransmission_does_not_inflate_denominator,
        "concurrent_confirm_single_report": TestQualityReportFromRecon().test_concurrent_confirm_produces_single_report,
        "gap_only_for_never_collected": TestGapDetectionAndBackfill().test_gap_only_for_never_collected_source,
        "failed_lines_to_correction": TestGapDetectionAndBackfill().test_failed_lines_go_to_correction_not_backfill,
        "backfill_claims_only_missing_source": TestGapDetectionAndBackfill().test_backfill_claims_only_missing_source_period,
        "audit_backfill_closes_gap": TestGapDetectionAndBackfill().test_audit_backfill_closes_gap_and_keeps_history,
        "no_close_without_delivery": TestGapDetectionAndBackfill().test_cannot_close_gap_without_real_delivery,
        "detect_gaps_idempotent": TestGapDetectionAndBackfill().test_detect_gaps_idempotent,
        "recovery_reopens_and_dedups": TestRestartRecovery().test_recovery_reopens_unacked_gap_and_dedups_physical_line,
        "recovery_keeps_reports_and_basis": TestRestartRecovery().test_recovery_keeps_reports_and_dispatch_basis,
        "acceptance_interleaved_success": TestAcceptanceInterleavedSupplement().test_interleaved_success_line_full_flow,
    }

    passed = 0
    failed = 0
    for name, test_func in tests.items():
        try:
            test_func()
            passed += 1
            print(f"  PASS  test_txn_recon: {name}")
        except Exception:
            failed += 1
            print(f"  FAIL  test_txn_recon: {name}")
            traceback.print_exc()

    print(f"\ntest_txn_recon: {passed} passed, {failed} failed, {len(tests)} total")
    if failed > 0:
        sys.exit(1)
    else:
        sys.exit(0)
