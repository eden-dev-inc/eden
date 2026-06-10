use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
#[cfg(not(embedded_db))]
use crate::pipeline::cdc::traits::CdcSource;
use crate::pipeline::manager::CdcManager;
use actix_web::{HttpRequest, Responder, web};
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_info};
use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
use ep_runtime::comp::MyEngineService;
use function_name::named;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;
use uuid::Uuid;

/// Activate a pipeline (start CDC WAL consumer)
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Pipelines"],
    path="/pipelines/{pipeline}/run",
    operation_id = "run_pipeline",
    responses((status = OK, body = RunPipelineResponse))
)]
#[allow(clippy::too_many_arguments)]
#[named]
pub async fn run(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    pipeline: web::Path<String>,
    database: web::Data<EdenDb>,
    cdc_manager: web::Data<CdcManager>,
    engine_service: web::Data<MyEngineService>,
    templates: web::Data<TemplateRegistry>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_uuid = auth.org_uuid().clone();
    let pipeline_ref = pipeline.into_inner();
    let ctx = ctx_with_trace!().with_feature("pipeline");

    // Look up the pipeline (org-scoped via SQL join)
    let schema = match Uuid::from_str(&pipeline_ref) {
        Ok(uuid) => database.select_pipeline_uuid(&uuid, &org_uuid, telemetry_wrapper).await,
        Err(_) => database.select_pipeline_id(&pipeline_ref, &org_uuid, telemetry_wrapper).await,
    }
    .map_err(|e| error_handling(e, &mut span))?;

    // Atomically claim by setting status = 'Running' only if in a runnable state.
    // Allow resuming from Paused state.
    let claim_sql = "UPDATE pipelines SET status = 'Running', updated_at = NOW()
         WHERE uuid = $1
           AND status IN ('Pending', 'Completed', 'Failed', 'Paused')
         RETURNING uuid";
    let claimed = {
        let conn = database.pg_connection().await.map_err(|e| error_handling(e, &mut span))?;
        conn.query_opt(claim_sql, &[schema.uuid()]).await.map_err(|e| error_handling(EpError::database(e), &mut span))?
    };

    if claimed.is_none() {
        let status = schema.status();
        return Err(error_handling(EpError::api(format!("Cannot run pipeline in {} state", status)), &mut span));
    }

    let pipeline_uuid = *schema.uuid();

    // Check if a CDC worker is already running
    if cdc_manager.is_active(&pipeline_uuid).await {
        return Err(error_handling(EpError::api("CDC worker is already active for this pipeline"), &mut span));
    }

    let cdc_config = schema.cdc_config();
    let pub_name = cdc_config.effective_publication_name().unwrap_or("eden_cdc_pub").to_string();
    let slot_name = cdc_config.effective_slot_name().unwrap_or("eden_cdc_slot").to_string();

    // Create publication on the source (Postgres-specific infrastructure)
    {
        use crate::pipeline::cdc::postgres::ReplicationCommands;

        let conn = database.pg_connection().await.map_err(|e| error_handling(e, &mut span))?;
        let create_pub_sql = ReplicationCommands::create_publication(&pub_name, &cdc_config.tables);
        if let Err(e) = conn.execute(&create_pub_sql, &[]).await {
            let msg = e.to_string();
            if !msg.contains("already exists") {
                return Err(error_handling(EpError::database(e), &mut span));
            }
        }
    }

    // Build worker config (validates that read/write template UUIDs are set)
    let worker_config = crate::pipeline::cdc::worker::CdcWorkerConfig::from_pipeline(&schema).map_err(|e| error_handling(e, &mut span))?;

    // Construct source and destination using templates
    let db = database.clone().into_inner();
    let worker_ctx = ctx.clone().with_feature("cdc_worker");
    let engine = engine_service.into_inner();
    let template_registry = templates.into_inner();

    #[cfg(embedded_db)]
    {
        return EdenResponse::response(RunPipelineResponse {
            uuid: String::new(),
            status: "Not supported in embedded-db mode".to_string(),
        })
        .into();
    }

    #[cfg(not(embedded_db))]
    {
        let pg_source =
            crate::pipeline::cdc::pg_source::PgCdcSource::new(db.clone(), slot_name.clone(), pub_name.clone(), worker_ctx.clone());

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
        source.setup().await.map_err(|e| error_handling(e, &mut span))?;

        let destination: Box<dyn crate::pipeline::cdc::traits::CdcDestination> =
            Box::new(crate::pipeline::cdc::template_destination::TemplateCdcDestination::new(
                worker_config.write_template_uuid,
                template_registry,
                engine,
                db.clone(),
                org_uuid,
                telemetry_wrapper.clone(),
                worker_ctx.clone(),
            ));

        // Create signal channel and spawn the CDC worker task
        let (signal_tx, signal_rx) = CdcManager::new_signal_channel();

        let worker_handle = tokio::spawn(async move {
            crate::pipeline::cdc::worker::run_cdc_worker(worker_config, source, destination, db, signal_rx, worker_ctx).await;
        });

        cdc_manager.register(pipeline_uuid, worker_handle, signal_tx).await;

        log_info!(
            ctx,
            "Pipeline CDC worker activated",
            audience = LogAudience::Both,
            pipeline_uuid = pipeline_uuid.to_string(),
            slot_name = slot_name.as_str(),
            publication_name = pub_name.as_str()
        );

        EdenResponse::response(RunPipelineResponse {
            uuid: pipeline_uuid.to_string(),
            status: "Running".to_string(),
        })
        .into()
    } // #[cfg(not(embedded_db))]
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RunPipelineResponse {
    pub uuid: String,
    pub status: String,
}
