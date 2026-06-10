use crate::EdenDb;
use crate::pipeline::manager::CdcManager;
use actix_web::web;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::OrganizationUuid;
use eden_core::telemetry::TelemetryWrapper;
use eden_logger_internal::{LogAudience, LogContext, log_info};
use endpoint_core::ep_core::database::schema::snapshot::{CdcConfig, SnapshotSchema};
use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
use ep_runtime::comp::MyEngineService;
use uuid::Uuid;

pub async fn start_cdc_run(
    database: web::Data<EdenDb>,
    service: web::Data<MyEngineService>,
    cdc_manager: web::Data<CdcManager>,
    templates: web::Data<TemplateRegistry>,
    schema: &SnapshotSchema,
    cdc_config: &CdcConfig,
    org_uuid: &OrganizationUuid,
    snapshot_uuid: Uuid,
    ctx: LogContext,
    telemetry_wrapper: TelemetryWrapper,
) -> ResultEP<()> {
    use crate::pipeline::cdc::postgres::ReplicationCommands;
    use crate::pipeline::cdc::traits::{CdcDestination, CdcSource};

    let pub_name = cdc_config.effective_publication_name().unwrap_or("eden_cdc_pub").to_string();
    let slot_name = cdc_config.effective_slot_name().unwrap_or("eden_cdc_slot").to_string();

    {
        let conn = database.pg_connection().await?;
        let create_pub_sql = ReplicationCommands::create_publication(&pub_name, &cdc_config.tables);
        if let Err(error) = conn.execute(&create_pub_sql, &[]).await {
            let message = error.to_string();
            if !message.contains("already exists") {
                return Err(EpError::database(error));
            }
        }
    }

    let worker_config = crate::pipeline::cdc::worker::CdcWorkerConfig::from_snapshot(schema, cdc_config)?;
    let db = database.into_inner();
    let worker_ctx = ctx.clone().with_feature("cdc_worker");
    let engine = service.into_inner();
    let template_registry = templates.into_inner();

    let pg_source = crate::pipeline::cdc::pg_source::PgCdcSource::new(db.clone(), slot_name.clone(), pub_name.clone(), worker_ctx.clone());

    let mut source: Box<dyn CdcSource> = Box::new(crate::pipeline::cdc::template_source::TemplateCdcSource::new(
        pg_source,
        worker_config.read_template_uuid,
        template_registry.clone(),
        engine.clone(),
        db.clone(),
        org_uuid.clone(),
        telemetry_wrapper.clone(),
        worker_ctx.clone(),
    ));
    source.setup().await?;

    let destination: Box<dyn CdcDestination> = Box::new(crate::pipeline::cdc::template_destination::TemplateCdcDestination::new(
        worker_config.write_template_uuid,
        template_registry,
        engine,
        db.clone(),
        org_uuid.clone(),
        telemetry_wrapper,
        worker_ctx.clone(),
    ));

    let (signal_tx, signal_rx) = CdcManager::new_signal_channel();
    let worker_handle = tokio::spawn(async move {
        crate::pipeline::cdc::worker::run_cdc_worker(worker_config, source, destination, db, signal_rx, worker_ctx).await;
    });

    cdc_manager.register(snapshot_uuid, worker_handle, signal_tx).await;

    log_info!(
        ctx,
        "CDC worker activated",
        audience = LogAudience::Both,
        snapshot_uuid = snapshot_uuid.to_string(),
        slot_name = slot_name.as_str(),
        publication_name = pub_name.as_str()
    );

    Ok(())
}
