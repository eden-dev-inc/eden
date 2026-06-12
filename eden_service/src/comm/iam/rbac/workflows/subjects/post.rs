use crate::EdenDb;
use crate::comm::iam::SubjectInput;
use crate::comm::iam::rbac::resolve_user_cache_uuid_for_org;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::WorkflowCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, WorkflowCacheUuid};
use eden_core::format::rbac::ControlPlaneRbacData;
use eden_core::format::{CacheObjectType, IdKind, WorkflowId, WorkflowUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::workflow::WorkflowSchema;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// **Permissions**: `ControlPerms::GRANT | granted_bits` on Organization
#[with_telemetry]
#[allow(clippy::too_many_arguments)]
pub async fn post(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<String>,
    database: web::Data<EdenDb>,
    body: web::Json<SubjectInput>,
) -> Result<impl Responder, actix_web::Error> {
    let entity = input.into_inner();

    let subject_input = body.into_inner();

    verify_control_perms(&database, &auth, None, subject_input.required_grant_perms(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_key = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    let workflow_schema =
        <EdenDb as CacheFunctions<WorkflowSchema, WorkflowCacheUuid, WorkflowUuid, WorkflowCacheId, WorkflowId>>::get_from_cache(
            &database,
            &CacheObjectType::from((Some(org_key.clone()), entity.clone())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    let workflow_cache = WorkflowCacheUuid::new(Some(org_key.clone()), workflow_schema.uuid());

    let version_ms = chrono::Utc::now().timestamp_millis();

    for (subject, relation) in subject_input.to_vec() {
        let user_cache = resolve_user_cache_uuid_for_org(&database, &org_key, auth.org_uuid(), &subject, telemetry_wrapper)
            .await
            .map_err(|e| error_handling(e, &mut span))?;

        let data = ControlPlaneRbacData {
            org_uuid: org_key.uuid(),
            entity_kind: IdKind::Workflow.as_str().to_owned(),
            entity_uuid: workflow_cache.uuid(),
            subject_kind: IdKind::User.as_str().to_owned(),
            subject_uuid: user_cache.uuid(),
            perms: relation,
        };

        database.control_plane_grant(&data, version_ms, 0i64).await.map_err(|e| error_handling(e, &mut span))?;
    }

    EdenResponse::<String>::ok("success").into()
}

#[allow(dead_code)]
#[derive(Debug, Serialize, ToSchema)]
struct Response {}
