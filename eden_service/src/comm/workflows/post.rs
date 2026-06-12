use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::methods::insert::InsertMethod;
use database::db::methods::insert::workflow::InsertWorkflow;
use eden_core::auth::ParsedJwt;
use eden_core::error::ResultEP;
use eden_core::format::cache_id::WorkflowCacheId;
use eden_core::format::cache_uuid::WorkflowCacheUuid;
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::workflow::WorkflowSchema;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Create a Workflow
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Workflows"],
    path="/workflows",
    operation_id = "create_workflow",
    request_body = InsertWorkflow,
    responses((status = OK, body = String))
)]
#[allow(clippy::too_many_arguments)]
pub async fn post(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    input: web::Json<InsertWorkflow>,
) -> Result<impl Responder, actix_web::Error> {
    // let org_uuid = auth.org_uuid();
    // let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());
    //
    // let workflow_uuid = <EdenDb as CacheFunctions<
    //     WorkflowSchema,
    //     WorkflowCacheUuid,
    //     WorkflowCacheId,
    // >>::get_uuid::<WorkflowUuid>(
    //     &database,
    //     &CacheObjectType::from((Some(org_key), workflow.clone())),
    //     telemetry_wrapper,
    // )
    // .await
    // .map_err(actix_web::error::ErrorInternalServerError)?;

    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let mut insert_workflow = input.into_inner();
    if insert_workflow.org_uuid() != auth.org_uuid() {
        return Err(error_handling(
            eden_core::error::EpError::rbac(format!(
                "Unauthorized: requested org_uuid '{}' does not match your organization",
                insert_workflow.org_uuid()
            )),
            &mut span,
        ));
    }

    insert_workflow.set_created_by(auth.user_uuid().clone());
    insert_workflow.set_updated_by(auth.user_uuid().clone());
    post_workflow(&database, insert_workflow, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::<String>::ok("success").into()
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Response {}

pub(crate) async fn post_workflow(db_manager: &EdenDb, workflow: InsertWorkflow, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
    <EdenDb as InsertMethod<WorkflowSchema, WorkflowCacheUuid, WorkflowCacheId, InsertWorkflow>>::insert(
        db_manager,
        workflow,
        telemetry_wrapper,
    )
    .await
}
