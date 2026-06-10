use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use chrono::{DateTime, Utc};
use database::db::cache::CacheFunctions;
use eden_core::auth::ParsedJwt;
use eden_core::error::ResultEP;
use eden_core::format::cache_id::OrganizationCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, OrganizationId, OrganizationUuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::organization::{OrganizationSchema, RateLimitSettings};
use endpoint_core::ep_core::settings::EdenSettings;
use serde::Deserialize;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Get all data associated with an existing Org
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Organization"],
    path="/organizations",
    operation_id = "get_organization",
    responses((status = OK, body = EdenResponse<Response>))
)]
pub async fn get(req: HttpRequest, auth: web::ReqData<ParsedJwt>, database: web::Data<EdenDb>) -> Result<impl Responder, actix_web::Error> {
    let settings = EdenSettings::from(req.headers());

    verify_control_perms(&database, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let output = get_organization(
        &database,
        &CacheObjectType::new(Some(OrganizationCacheUuid::new(None, auth.org_uuid().clone())), None),
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    //TODO handle response returning Org info and verbose
    match settings.verbose() {
        false => EdenResponse::response(Response {
            id: output.id(),
            uuid: output.uuid(),
            description: output.description(),
            created_at: output.created_at(),
            updated_at: output.updated_at(),
            eden_nodes: output.eden_node_pairs().len(),
            super_admins: output.super_admin_pairs().len(),
            users: output.user_pairs().len(),
            endpoints: output.endpoint_pairs().len(),
            robots: output.robot_pairs().len(),
            templates: output.template_pairs().len(),
            workflows: output.workflow_pairs().len(),
            rate_limit_settings: output.rate_limit_settings().cloned(),
        })
        .into(),
        true => EdenResponse::response(output).into(),
    }
}

pub(crate) async fn get_organization(
    db_manager: &EdenDb,
    cache_object: &CacheObjectType<OrganizationCacheUuid, OrganizationCacheId>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<OrganizationSchema> {
    <EdenDb as CacheFunctions<
        OrganizationSchema,
        OrganizationCacheUuid,
        OrganizationUuid,
        OrganizationCacheId,
        OrganizationId,
    >>::get_from_cache(db_manager, cache_object, telemetry_wrapper)
    .await
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Response {
    id: OrganizationId,
    uuid: OrganizationUuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[schema(value_type = String)]
    created_at: DateTime<Utc>,
    #[schema(value_type = String)]
    updated_at: DateTime<Utc>,
    eden_nodes: usize,
    super_admins: usize,
    users: usize,
    endpoints: usize,
    robots: usize,
    templates: usize,
    workflows: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    rate_limit_settings: Option<RateLimitSettings>,
}
