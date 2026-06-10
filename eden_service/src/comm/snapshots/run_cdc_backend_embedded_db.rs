use crate::EdenDb;
use crate::pipeline::manager::CdcManager;
use actix_web::web;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::OrganizationUuid;
use eden_core::telemetry::TelemetryWrapper;
use eden_logger_internal::LogContext;
use endpoint_core::ep_core::database::schema::snapshot::{CdcConfig, SnapshotSchema};
use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
use ep_runtime::comp::MyEngineService;
use uuid::Uuid;

pub async fn start_cdc_run(
    _database: web::Data<EdenDb>,
    _service: web::Data<MyEngineService>,
    _cdc_manager: web::Data<CdcManager>,
    _templates: web::Data<TemplateRegistry>,
    _schema: &SnapshotSchema,
    _cdc_config: &CdcConfig,
    _org_uuid: &OrganizationUuid,
    _snapshot_uuid: Uuid,
    _ctx: LogContext,
    _telemetry_wrapper: TelemetryWrapper,
) -> ResultEP<()> {
    Err(EpError::database("Snapshots are not supported in embedded-db mode"))
}
