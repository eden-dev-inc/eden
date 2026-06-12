use crate::EdenDb;
use crate::comm::endpoints::get::get_endpoint;
use crate::comm::rbac::verify_endpoint_access;
use actix_web::{HttpRequest, Responder, web};
use eden_core::auth::ParsedJwt;
use eden_core::format::CacheObjectType;
use eden_core::format::EndpointId;
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::endpoint::EpKind;
use eden_core::format::rbac::DataPerms;
use eden_core::telemetry::{TelemetryWrapper, TraceContext};
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::endpoint::EndpointRequestInput;
use endpoint_schema::EndpointSchemaInput;
use ep_runtime::comp::MyEngineService;
use serde::Deserialize;
use serde_json::json;
use utoipa::ToSchema;

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateFunctionEndpointRequest {
    pub endpoint: EndpointId,
    pub config: serde_json::Value,
    #[serde(default)]
    pub description: Option<String>,
}

/// Create a Function endpoint (Lambda provider via endpoint config)
#[utoipa::path(
    post,
    tags = ["Functions"],
    path="/functions",
    operation_id = "create_function_endpoint",
    request_body = CreateFunctionEndpointRequest,
    responses((status = OK, body = eden_core::response::EdenResponse<crate::comm::endpoints::post::Response>))
)]
// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub async fn post(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    node_data: web::Data<eden_core::comm::NodeData>,
    engine_service: web::Data<MyEngineService>,
    payload: web::Json<CreateFunctionEndpointRequest>,
    metrics: web::Data<eden_core::telemetry::AllMetrics>,
    metadata: eden_core::telemetry::MetadataMapWrapper,
    labels: eden_core::telemetry::TelemetryLabels,
    durations: eden_core::telemetry::TelemetryDurations,
) -> Result<impl Responder, actix_web::Error> {
    let payload = payload.into_inner();

    let endpoint_input: EndpointSchemaInput = serde_json::from_value(json!({
        "endpoint": payload.endpoint,
        "kind": EpKind::Function,
        "config": payload.config,
        "description": payload.description
    }))
    .map_err(|e| actix_web::error::ErrorBadRequest(format!("invalid function endpoint payload: {e}")))?;

    crate::comm::endpoints::post::post(
        req,
        auth,
        database,
        node_data,
        engine_service,
        web::Json(endpoint_input),
        metrics,
        metadata,
        labels,
        durations,
    )
    .await
}

/// Invoke a function endpoint using the standard endpoint request envelope
/// **Permissions**: See exact permission-bit checks in the handler body.
#[utoipa::path(
    post,
    tags = ["Functions"],
    path="/functions/{endpoint}/invoke",
    operation_id = "invoke_function_endpoint",
    params(("endpoint" = String, Path, description = "Function endpoint ID")),
    request_body = EndpointRequestInput,
    responses((status = OK, body = serde_json::Value))
)]
// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub async fn invoke(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    database: web::Data<EdenDb>,
    engine_service: web::Data<MyEngineService>,
    input: web::Json<EndpointRequestInput>,
    metrics: web::Data<eden_core::telemetry::AllMetrics>,
    metadata: eden_core::telemetry::MetadataMapWrapper,
    labels: eden_core::telemetry::TelemetryLabels,
    durations: eden_core::telemetry::TelemetryDurations,
) -> Result<impl Responder, actix_web::Error> {
    let endpoint = endpoint.into_inner();
    let mut telemetry_wrapper = TelemetryWrapper::new_with_telemetry(
        TraceContext::from(metadata.metadata().clone()),
        metrics.clone().into_inner(),
        labels.clone(),
        durations.clone(),
    );

    let organization_cache_uuid = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned());
    let endpoint_schema = get_endpoint(
        &database,
        &CacheObjectType::<EndpointCacheUuid, EndpointCacheId>::from((Some(organization_cache_uuid), endpoint.clone())),
        &mut telemetry_wrapper,
    )
    .await
    .map_err(actix_web::Error::from)?;

    let organization_cache_uuid2 = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned());
    let endpoint_cache_uuid = EndpointCacheUuid::new(Some(organization_cache_uuid2), endpoint_schema.endpoint_uuid());
    verify_endpoint_access(
        &database,
        &auth,
        &endpoint_cache_uuid,
        endpoint_schema.endpoint_uuid(),
        DataPerms::EXECUTE,
        &mut telemetry_wrapper,
    )
    .await
    .map_err(actix_web::Error::from)?;

    if endpoint_schema.kind() != EpKind::Function {
        return Err(actix_web::error::ErrorBadRequest(format!(
            "endpoint `{}` has kind `{}`; expected `Function`",
            endpoint_schema.id(),
            endpoint_schema.kind()
        )));
    }

    crate::comm::endpoints::write::write(
        req,
        auth,
        web::Path::from(endpoint),
        database,
        engine_service,
        input,
        metrics,
        metadata,
        labels,
        durations,
    )
    .await
}
