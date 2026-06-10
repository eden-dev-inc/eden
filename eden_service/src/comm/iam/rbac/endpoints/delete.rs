use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EndpointId, EndpointUuid, IdKind};
use eden_core::response::EdenResponse;
use eden_core::telemetry::labels::TelemetryLabels;
use eden_core::telemetry::{AllMetrics, MetadataMapWrapper, TelemetryDurations, TelemetryWrapper, TraceContext};
use endpoint_schema::endpoint::EndpointSchema;
use function_name::named;
use serde::Deserialize;
use serde::Serialize;
use utoipa::ToSchema;

/// **Permissions**: `ControlPerms::GRANT | ControlPerms::DESTROY` on Organization
#[named]
#[utoipa::path(
    delete,
    tags = ["RBAC"],
    path="/iam/control/endpoints/{endpoint}",
    operation_id = "delete_rbac_endpoint",
    responses((status = OK, body = Response))
)]
// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub async fn delete(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<String>,
    database: web::Data<EdenDb>,
    // telemetry data
    metrics: web::Data<AllMetrics>,
    metadata: MetadataMapWrapper,
    labels: TelemetryLabels,
    durations: TelemetryDurations,
) -> Result<impl Responder, actix_web::Error> {
    // Initialize telemetry wrapper and span using fast-telemetry
    let mut telemetry_wrapper_value =
        TelemetryWrapper::new_with_telemetry(TraceContext::from(metadata.metadata().clone()), metrics.into_inner(), labels, durations);
    let telemetry_wrapper = &mut telemetry_wrapper_value;
    let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());

    let entity = input.into_inner();

    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    let endpoint_cache =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_cache_uuid(
            &database,
            &CacheObjectType::from((Some(org_cache.clone()), entity)),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT | ControlPerms::DESTROY, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    database
        .control_plane_remove_entity(
            org_cache.uuid(),
            IdKind::Endpoint,
            endpoint_cache.uuid(),
            chrono::Utc::now().timestamp_millis(),
            0i64,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let response: Result<actix_web::HttpResponse, actix_web::error::Error> =
        EdenResponse::response(Response::new("removed all rbac rules for endpoint".to_string())).into();

    response.map(|mut response| {
        let mut extensions = response.extensions_mut();
        extensions.insert(telemetry_wrapper.labels().clone());
        extensions.insert(telemetry_wrapper.durations().clone())
    })
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Response(String);

impl Response {
    fn new(message: String) -> Self {
        Self(message)
    }
}
