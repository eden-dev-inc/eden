use crate::EdenDb;
use crate::comm::els::{apply_els_for_request, resolve_els_endpoint_switch_schema, resolve_els_required};
use crate::comm::endpoints::hydrate_llm_endpoint_config;
use crate::comm::rbac::{AuthMode, verify_endpoint_access};
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::DataPerms;
use eden_core::format::{CacheObjectType, EndpointId, EndpointUuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::{FastSpan, TelemetryWrapper};
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::ep::ConnectionTier;
use endpoint_core::ep_core::settings::EdenSettings;
use endpoint_schema::endpoint::EndpointSchema;
use endpoints::endpoint::EpRequest;
#[cfg(feature = "cassandra")]
use endpoints::endpoint::cassandra::output::CassandraOutput;
#[cfg(feature = "redis")]
use endpoints::endpoint::ep_redis::output::RedisEndpointOutput;
#[cfg(feature = "mongo")]
use endpoints::endpoint::mongo::output::MongoOutput;
#[cfg(feature = "postgres")]
use endpoints::endpoint::postgres::api::wrapper::output::PostgresOutput;
use endpoints::endpoint::request::EndpointRequestInput;
use ep_runtime::comp::MyEngineService;
use serde::{Deserialize, Serialize};
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

#[derive(ToSchema)]
#[schema(title = "Endpoint response")]
// TODO: Consider boxing to reduce size differences between variants.
#[allow(clippy::large_enum_variant)]
pub enum ReadResponse {
    #[schema(title = "MongoDB")]
    #[cfg(feature = "mongo")]
    MongoResponse(MongoOutput),
    #[schema(title = "Cassandra")]
    #[cfg(feature = "cassandra")]
    CassandraResponse(CassandraOutput),
    #[schema(title = "PostgreSQL")]
    #[cfg(feature = "postgres")]
    Postgres(PostgresOutput),
    #[schema(title = "Redis")]
    #[cfg(feature = "redis")]
    RedisResponse(RedisEndpointOutput),
}

/// Read data from an Endpoint with gRPC
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Endpoints"],
    path="/endpoints/{endpoint}/read",
    operation_id = "read_endpoint",
    request_body = EndpointRequestInput,
    responses(
        (status = OK, description = "Endpoint read response", body = serde_json::Value),
    )
)]
pub async fn read(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    engine_service: web::Data<MyEngineService>,
    database: web::Data<EdenDb>,
    input: web::Json<EndpointRequestInput>,
    // telemetry data
    // metrics: web::Data<AllMetrics>,
    // metadata: MetadataMapWrapper,
    // labels: TelemetryLabels,
    // durations: TelemetryDurations,
) -> Result<impl Responder, actix_web::Error> {
    // Telemetry wrapper and span are created by #[with_telemetry] macro

    let settings = EdenSettings::from(req.headers());

    let organization_cache_uuid = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned());

    let cache_object =
        CacheObjectType::<EndpointCacheUuid, EndpointCacheId>::from((Some(organization_cache_uuid.clone()), endpoint.into_inner()));

    endpoint_read(
        &database,
        &engine_service,
        organization_cache_uuid,
        &cache_object,
        input.into_inner(),
        &auth.into_inner(),
        settings,
        &mut span,
        telemetry_wrapper,
    )
    .await
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Response(serde_json::Value);

// TODO: Refactor parameters into a request/context struct to reduce argument count.
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn endpoint_read(
    db_manager: &EdenDb,
    engine_service: &web::Data<MyEngineService>,
    org_cache: OrganizationCacheUuid,
    cache_object: &CacheObjectType<EndpointCacheUuid, EndpointCacheId>,
    request: EndpointRequestInput,
    auth: &ParsedJwt,
    settings: EdenSettings,
    span: &mut FastSpan,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> Result<actix_web::HttpResponse, actix_web::error::Error> {
    // Record endpoint request start time for duration tracking
    telemetry_wrapper.mut_durations(|durations| durations.set_endpoint_request(chrono::Utc::now()));

    let mut endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            db_manager,
            cache_object,
            telemetry_wrapper,
        )
        .await?;

    telemetry_wrapper.mut_labels(|labels| {
        labels.set_endpoint_uuid(endpoint_schema.uuid());
        labels.set_endpoint_id(endpoint_schema.id());
        labels.set_endpoint_kind(endpoint_schema.kind());
    });

    let endpoint_cache_uuid = EndpointCacheUuid::new(Some(org_cache.clone()), endpoint_schema.endpoint_uuid());
    let auth_mode = verify_endpoint_access(
        db_manager,
        auth,
        &endpoint_cache_uuid,
        endpoint_schema.endpoint_uuid(),
        DataPerms::READ,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, span))?;

    hydrate_llm_endpoint_config(db_manager, &mut endpoint_schema, auth.org_uuid(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, span))?;

    let mut effective_request = request;

    // Resolve ELS (Endpoint-Level Security) credentials after RBAC.
    // Applies per-user auth: PG session variables, HTTP header injection,
    // or connection override depending on endpoint type.
    let els_conn = match auth_mode {
        AuthMode::Rbac => None,
        AuthMode::Els => {
            let els_auth = resolve_els_required(db_manager, auth, &endpoint_cache_uuid).await.map_err(|e| error_handling(e, span))?;
            if let Some(switched_schema) =
                resolve_els_endpoint_switch_schema(db_manager, &org_cache, endpoint_schema.kind(), els_auth.as_ref(), telemetry_wrapper)
                    .await
                    .map_err(|e| error_handling(e, span))?
            {
                endpoint_schema = switched_schema;
                telemetry_wrapper.mut_labels(|labels| {
                    labels.set_endpoint_uuid(endpoint_schema.uuid());
                    labels.set_endpoint_id(endpoint_schema.id());
                    labels.set_endpoint_kind(endpoint_schema.kind());
                });
                None
            } else {
                apply_els_for_request(
                    endpoint_schema.kind(),
                    els_auth.as_ref(),
                    endpoint_schema.config().as_ref(),
                    ConnectionTier::Read,
                    &mut effective_request.request.0,
                )
                .map_err(|e| error_handling(e, span))?
            }
        }
    };

    let mut request: Box<dyn EpRequest> =
        TryInto::try_into((effective_request, endpoint_schema.kind())).map_err(|e| error_handling(e, span))?;

    let response = engine_service
        .read_els(&mut *request, &endpoint_schema, els_conn, org_cache, settings, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, span))?;

    // Record endpoint response end time for duration tracking
    telemetry_wrapper.mut_durations(|durations| durations.set_endpoint_response(chrono::Utc::now()));

    EdenResponse::response(Response(response)).into()
}
