"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.log_source import LogSourceService
from app.services.parser_rule import ParserRuleService
from app.services.mask_rule import MaskRuleService
from app.services.event_type import EventTypeService
from app.services.ingest_job import IngestJobService
from app.services.batch_checkpoint import BatchCheckpointService
from app.services.event_store import EventStoreService
from app.services.anomaly_rule import AnomalyRuleService
from app.services.sample_job import SampleJobService
from app.services.field_enrich import FieldEnrichService
from app.services.archive_policy import ArchivePolicyService
from app.services.forward_rule import ForwardRuleService
from app.services.schema_version import SchemaVersionService
from app.services.dedup_filter import DedupFilterService
from app.services.ingest_metric import IngestMetricService
from app.services.replay_record import ReplayRecordService
from app.services.quality_report import QualityReportService
from app.services.tag_taxonomy import TagTaxonomyService
from app.services.export_bundle import ExportBundleService
from app.services.audit_event import AuditEventService
from app.services.tenant_isolation import TenantIsolationService
from app.services.format_adapt import FormatAdaptService
from app.services.txn_recon import TxnReconService

SERVICES: dict[str, BaseModuleService] = {
    "log_source": LogSourceService(),
    "parser_rule": ParserRuleService(),
    "mask_rule": MaskRuleService(),
    "event_type": EventTypeService(),
    "ingest_job": IngestJobService(),
    "batch_checkpoint": BatchCheckpointService(),
    "event_store": EventStoreService(),
    "anomaly_rule": AnomalyRuleService(),
    "sample_job": SampleJobService(),
    "field_enrich": FieldEnrichService(),
    "archive_policy": ArchivePolicyService(),
    "forward_rule": ForwardRuleService(),
    "schema_version": SchemaVersionService(),
    "dedup_filter": DedupFilterService(),
    "ingest_metric": IngestMetricService(),
    "replay_record": ReplayRecordService(),
    "quality_report": QualityReportService(),
    "tag_taxonomy": TagTaxonomyService(),
    "export_bundle": ExportBundleService(),
    "audit_event": AuditEventService(),
    "tenant_isolation": TenantIsolationService(),
    "format_adapt": FormatAdaptService(),
    "txn_recon": TxnReconService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
