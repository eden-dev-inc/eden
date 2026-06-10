use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use eden_core::auth::ParsedJwt;
use eden_core::error::ResultEP;
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EndpointId, EndpointUuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::TelemetryWrapper;
pub use endpoint_schema::endpoint::EndpointSchema;
use telemetry_extensions_macro::with_telemetry;

/// Get an Endpoint
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Endpoints"],
    path="/endpoints/{endpoint}",
    operation_id = "get_endpoint",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn get(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    // Record endpoint request start time for duration tracking
    telemetry_wrapper.mut_durations(|durations| durations.set_endpoint_request(chrono::Utc::now()));

    let org_uuid = auth.org_uuid();
    let organization_cache_uuid = OrganizationCacheUuid::new(None, org_uuid.to_owned());

    let endpoint_name = endpoint.into_inner();
    let endpoint_schema = get_endpoint(
        &database,
        &CacheObjectType::<EndpointCacheUuid, EndpointCacheId>::from((Some(organization_cache_uuid.clone()), endpoint_name.clone())),
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, Some(endpoint_schema.endpoint_uuid()), ControlPerms::READ, telemetry_wrapper)
        .await
        .inspect(|_| span.add_event("Verified RBAC (read)", vec![]))?;

    let response: Result<actix_web::HttpResponse, actix_web::error::Error> = EdenResponse::response(endpoint_schema).into();

    // Record endpoint response end time for duration tracking
    telemetry_wrapper.mut_durations(|durations| durations.set_endpoint_response(chrono::Utc::now()));

    response
}

pub(crate) async fn get_endpoint(
    db_manager: &EdenDb,
    cache_object: &CacheObjectType<EndpointCacheUuid, EndpointCacheId>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<EndpointSchema> {
    <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
        db_manager,
        cache_object,
        telemetry_wrapper,
    )
    .await
}

pub type Response = EndpointSchema;
