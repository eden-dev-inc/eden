use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::methods::update::{SqlQueries, UpdateActor, UpdateMethod};
use eden_core::auth::ParsedJwt;
use eden_core::error::ResultEP;
use eden_core::format::cache_id::WorkflowCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, WorkflowCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EdenId, WorkflowId, WorkflowUuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::workflow::{UpdateWorkflowSchema, WorkflowSchema};
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Update a Workflow
/// **Permissions**: `ControlPerms::CONFIGURE` on the Workflow or Organization
#[with_telemetry]
#[utoipa::path(
    patch,
    tags = ["Workflows"],
    path="/workflows/{workflow}",
    operation_id = "update_workflow",
    request_body = UpdateWorkflowSchema,
    responses((status = OK, body = String))
)]
#[allow(clippy::too_many_arguments)]
pub async fn patch(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    workflow: web::Path<String>,
    database: web::Data<EdenDb>,
    input: web::Json<UpdateWorkflowSchema>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    let workflow_input = input.into_inner();

    let workflow_schema =
        <EdenDb as CacheFunctions<WorkflowSchema, WorkflowCacheUuid, WorkflowUuid, WorkflowCacheId, WorkflowId>>::get_from_cache(
            &database,
            &CacheObjectType::from((Some(org_key), workflow.to_string())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let workflow_object = &CacheObjectType::new(
        Some(WorkflowCacheUuid::new(
            Some(OrganizationCacheUuid::new(None, auth.org_uuid().clone())),
            workflow_schema.uuid(),
        )),
        None,
    );

    update_workflow(&database, workflow_object, UpdateActor::User(auth.user_uuid()), telemetry_wrapper, workflow_input)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::<String>::ok("success").into()
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Response {}

pub(crate) async fn update_workflow(
    db_manager: &EdenDb,
    cache_object: &CacheObjectType<WorkflowCacheUuid, WorkflowCacheId>,
    updated_by: UpdateActor<'_>,
    telemetry_wrapper: &mut TelemetryWrapper,
    update_workflow: UpdateWorkflowSchema,
) -> ResultEP<()> {
    if let Some(id) = update_workflow.id() {
        <EdenDb as UpdateMethod<WorkflowSchema, WorkflowCacheUuid, WorkflowUuid, WorkflowCacheId, WorkflowId>>::update_id(
            db_manager,
            cache_object,
            SqlQueries::UpdateWorkflowId,
            id.id().to_owned(),
            updated_by,
            telemetry_wrapper,
        )
        .await?;
    }
    if let Some(description) = update_workflow.description() {
        <EdenDb as UpdateMethod<WorkflowSchema, WorkflowCacheUuid, WorkflowUuid, WorkflowCacheId, WorkflowId>>::update_description(
            db_manager,
            cache_object,
            SqlQueries::UpdateWorkflowDescription,
            description.to_owned(),
            updated_by,
            telemetry_wrapper,
        )
        .await?;
    }
    if let Some(dag) = update_workflow.dag() {
        <EdenDb as UpdateMethod<WorkflowSchema, WorkflowCacheUuid, WorkflowUuid, WorkflowCacheId, WorkflowId>>::update_workflow_dag(
            db_manager,
            cache_object,
            dag.to_owned(),
            updated_by,
            telemetry_wrapper,
        )
        .await?;
    }

    Ok(())
}
