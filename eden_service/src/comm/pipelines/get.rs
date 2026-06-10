use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use eden_core::auth::ParsedJwt;
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use std::str::FromStr;
use telemetry_extensions_macro::with_telemetry;
use uuid::Uuid;

/// Get a single pipeline by id or uuid
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Pipelines"],
    path="/pipelines/{pipeline}",
    operation_id = "get_pipeline",
    responses((status = OK, body = endpoint_core::ep_core::database::schema::pipeline::PipelineSchema))
)]
pub async fn get(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    pipeline: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_uuid = auth.org_uuid();
    let pipeline_ref = pipeline.into_inner();

    // Org-scoped lookup (SQL joins organization_pipelines)
    let schema = match Uuid::from_str(&pipeline_ref) {
        Ok(uuid) => database.select_pipeline_uuid(&uuid, org_uuid, telemetry_wrapper).await,
        Err(_) => database.select_pipeline_id(&pipeline_ref, org_uuid, telemetry_wrapper).await,
    }
    .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(schema).into()
}

/// List all pipelines for the organization
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Pipelines"],
    path="/pipelines",
    operation_id = "list_pipelines",
    responses((status = OK, body = Vec<endpoint_core::ep_core::database::schema::pipeline::PipelineSchema>))
)]
pub async fn get_all(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();

    verify_control_perms(&database, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let schemas = database.select_all_pipelines(org_uuid, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(schemas).into()
}
