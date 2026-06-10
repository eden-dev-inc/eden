use crate::EdenDb;
use crate::comm::els::{apply_els_for_request, resolve_els_endpoint_switch_schema, resolve_els_required};
use crate::comm::endpoints::hydrate_llm_endpoint_config;
use crate::comm::rbac::{AuthMode, verify_endpoint_access};
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use eden_core::auth::ParsedJwt;
use eden_core::format::EdenUuid;
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid};
use eden_core::format::rbac::DataPerms;
use eden_core::format::{CacheObjectType, EndpointId, EndpointUuid, OrganizationCacheUuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::{FastSpan, FastSpanAttribute, TelemetryWrapper};
use endpoint_core::ep_core::ep::ConnectionTier;
#[cfg(not(feature = "openapi"))]
use endpoint_core::ep_core::ep::EndpointAPIRequest as EndpointWriteAPIRequest;
use endpoint_core::ep_core::settings::EdenSettings;
#[cfg(feature = "openapi")]
use endpoint_openapi::EndpointAPIRequest as EndpointWriteAPIRequest;
use endpoint_schema::endpoint::EndpointSchema;
use endpoints::endpoint::EpRequest;
use endpoints::endpoint::request::EndpointRequestInput;
use ep_runtime::comp::MyEngineService;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Write data from an Endpoint with gRPC
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Endpoints"],
    path="/endpoints/{endpoint}/write",
    operation_id = "write_endpoint",
    request_body = EndpointWriteAPIRequest,
    responses((status = OK, body = serde_json::Value))
)]
// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub async fn write(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    database: web::Data<EdenDb>,
    engine_service: web::Data<MyEngineService>,
    input: web::Json<EndpointRequestInput>,
) -> Result<impl Responder, actix_web::Error> {
    // Record endpoint request start time for duration tracking
    telemetry_wrapper.mut_durations(|durations| durations.set_endpoint_request(chrono::Utc::now()));

    let settings = EdenSettings::from(req.headers());

    let organization_cache_uuid = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned());

    let cache_object =
        CacheObjectType::<EndpointCacheUuid, EndpointCacheId>::from((Some(organization_cache_uuid.clone()), endpoint.into_inner()));

    endpoint_write(
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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn endpoint_write(
    db_manager: &EdenDb,
    engine_service: &web::Data<MyEngineService>,
    organization_cache_uuid: OrganizationCacheUuid,
    cache_object: &CacheObjectType<EndpointCacheUuid, EndpointCacheId>,
    input: EndpointRequestInput,
    auth: &ParsedJwt,
    settings: EdenSettings,
    span: &mut FastSpan,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> Result<actix_web::HttpResponse, actix_web::error::Error> {
    let mut endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            db_manager,
            cache_object,
            telemetry_wrapper,
        )
        .await?;

    span.add_event(
        "collected `endpoint_uuid` from cache".to_string(),
        vec![FastSpanAttribute::new("uuid", endpoint_schema.endpoint_uuid().uuid().to_string())],
    );

    let endpoint_cache_uuid = EndpointCacheUuid::new(Some(organization_cache_uuid.clone()), endpoint_schema.endpoint_uuid());
    let auth_mode = verify_endpoint_access(
        db_manager,
        auth,
        &endpoint_cache_uuid,
        endpoint_schema.endpoint_uuid(),
        DataPerms::WRITE,
        telemetry_wrapper,
    )
    .await
    .inspect(|_| span.add_event("Verified RBAC", vec![]))?;

    hydrate_llm_endpoint_config(db_manager, &mut endpoint_schema, auth.org_uuid(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, span))?;

    let mut request_input = input;

    let els_conn = match auth_mode {
        AuthMode::Rbac => None,
        AuthMode::Els => {
            let els_auth = resolve_els_required(db_manager, auth, &endpoint_cache_uuid).await.map_err(|e| error_handling(e, span))?;
            if let Some(switched_schema) = resolve_els_endpoint_switch_schema(
                db_manager,
                &organization_cache_uuid,
                endpoint_schema.kind(),
                els_auth.as_ref(),
                telemetry_wrapper,
            )
            .await
            .map_err(|e| error_handling(e, span))?
            {
                endpoint_schema = switched_schema;
                None
            } else {
                apply_els_for_request(
                    endpoint_schema.kind(),
                    els_auth.as_ref(),
                    endpoint_schema.config().as_ref(),
                    ConnectionTier::Write,
                    &mut request_input.request.0,
                )
                .map_err(|e| error_handling(e, span))?
            }
        }
    };

    let mut request: Box<dyn EpRequest> =
        TryInto::try_into((request_input, endpoint_schema.kind())).map_err(|e| error_handling(e, span))?;

    let response = engine_service
        .write_els(&mut *request, &endpoint_schema, els_conn, organization_cache_uuid, settings, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, span))?;

    let response: Result<actix_web::HttpResponse, actix_web::error::Error> = EdenResponse::response(Response::new(response)).into();

    telemetry_wrapper.mut_durations(|durations| durations.set_endpoint_response(chrono::Utc::now()));

    response
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct Response(serde_json::Value);

impl Response {
    fn new(value: serde_json::Value) -> Self {
        Self(value)
    }
}
