use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache_ops::CacheOps;
use database::db::methods::update::{SqlQueries, UpdateActor, UpdateMethod};
use eden_core::auth::ParsedJwt;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::OrganizationCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EdenId, OrganizationId, OrganizationUuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::{AllMetrics, TelemetryWrapper};
use endpoint_core::ep_core::database::schema::organization::{OrganizationSchema, UpdateOrganizationSchema};
use serde::Deserialize;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Update Organization
///
/// Update organization data.
/// **Permissions**: `ControlPerms::CONFIGURE` on Organization
#[with_telemetry]
#[utoipa::path(
    patch,
    tags = ["Organization"],
    path="/organizations",
    operation_id = "update_organization",
    request_body = UpdateOrganizationSchema,
    responses((status = OK, body = String))
)]
#[allow(clippy::too_many_arguments)]
pub async fn patch(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    input: web::Json<UpdateOrganizationSchema>,
    metrics: web::Data<AllMetrics>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_object = &CacheObjectType::new(Some(OrganizationCacheUuid::new(None, auth.org_uuid().to_owned())), None);

    let input = input.into_inner();
    let has_rate_limit_change = input.rate_limit_settings().is_some();

    update_organization(&database, org_object, UpdateActor::User(auth.user_uuid()), telemetry_wrapper, input)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // When rate limit settings change, flush the token bucket keys so the new limits
    // take effect immediately from a clean slate rather than inheriting old accumulated usage.
    if has_rate_limit_change {
        let org_uuid_str = auth.org_uuid().to_string();
        let ingress_key = crate::rate_limiter::token_bucket_key(&org_uuid_str, "token_ingress");
        let egress_key = crate::rate_limiter::token_bucket_key(&org_uuid_str, "token_egress");
        let _ = database.kv_del(&ingress_key).await;
        let _ = database.kv_del(&egress_key).await;
    }

    EdenResponse::response(Response::new(auth.org_id().clone(), auth.org_uuid().clone())).into()
}

pub(crate) async fn update_organization(
    db_manager: &EdenDb,
    cache_object: &CacheObjectType<OrganizationCacheUuid, OrganizationCacheId>,
    updated_by: UpdateActor<'_>,
    telemetry_wrapper: &mut TelemetryWrapper,
    update_schema: UpdateOrganizationSchema,
) -> ResultEP<()> {
    if let Some(id) = update_schema.id() {
        <EdenDb as UpdateMethod<
            OrganizationSchema,
            OrganizationCacheUuid,
            OrganizationUuid,
            OrganizationCacheId,
            OrganizationId,
        >>::update_id(
            db_manager,
            cache_object,
            SqlQueries::UpdateOrganizationId,
            id.id().to_owned(),
            updated_by,
            telemetry_wrapper,
        )
        .await?
    }
    if let Some(description) = update_schema.description() {
        <EdenDb as UpdateMethod<
            OrganizationSchema,
            OrganizationCacheUuid,
            OrganizationUuid,
            OrganizationCacheId,
            OrganizationId,
        >>::update_description(
            db_manager,
            cache_object,
            SqlQueries::UpdateOrganizationDescription,
            description.to_owned(),
            updated_by,
            telemetry_wrapper,
        )
        .await?
    }
    if let Some(rate_limit_settings) = update_schema.rate_limit_settings() {
        let json_value = serde_json::to_value(rate_limit_settings).map_err(|e| EpError::database(e.to_string()))?;
        <EdenDb as UpdateMethod<
            OrganizationSchema,
            OrganizationCacheUuid,
            OrganizationUuid,
            OrganizationCacheId,
            OrganizationId,
        >>::update_organization_rate_limit_settings(db_manager, cache_object, Some(json_value), telemetry_wrapper)
        .await?
    }

    Ok(())
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Response {
    id: OrganizationId,
    uuid: OrganizationUuid,
}

impl Response {
    fn new(id: OrganizationId, uuid: OrganizationUuid) -> ResultEP<Self> {
        Ok(Self { id, uuid })
    }
}
