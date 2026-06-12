use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use chrono::{DateTime, Utc};
use eden_core::auth::ParsedJwt;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::timestamp::DateTimeWrapper;
use eden_core::format::{EdenNodeUuid, EndpointId, EndpointUuid};
use eden_core::request::ServerData;
use eden_core::response::EdenResponse;
use eden_core::telemetry::guards::EndpointGuard;
use eden_core::telemetry::labels::TelemetryLabels;
use eden_core::telemetry::{AllMetrics, FastSpanAttribute, TelemetryDurations, TelemetryWrapper};
use eden_logger_internal::LogContextEdenExt;
use eden_logger_internal::ctx_with_trace;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_schema::endpoint::EndpointSchema;
use function_name::named;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::str::FromStr;
use utoipa::ToSchema;

/// Response for listing endpoints
#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ListEndpointsResponse {
    pub endpoints: Vec<EndpointSummary>,
}

/// Summary information for an endpoint
#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct EndpointSummary {
    pub id: EndpointId,
    pub uuid: EndpointUuid,
    pub kind: String,
    pub config: serde_json::Value,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

fn endpoint_summary_from_schema(endpoint_schema: endpoint_core::endpoint::EndpointSchema) -> ResultEP<EndpointSummary> {
    Ok(EndpointSummary {
        id: endpoint_schema.id(),
        uuid: endpoint_schema.uuid(),
        kind: endpoint_schema.kind().to_string(),
        config: endpoint_schema.config().serialize()?,
        description: endpoint_schema.description(),
        created_at: endpoint_schema.created_at(),
        updated_at: endpoint_schema.updated_at(),
    })
}

async fn authorized_endpoint_summary(
    database: &EdenDb,
    auth: &ParsedJwt,
    endpoint_schema: EndpointSchema,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<Option<EndpointSummary>> {
    if verify_control_perms(database, auth, Some(endpoint_schema.endpoint_uuid()), ControlPerms::READ, telemetry_wrapper)
        .await
        .is_err()
    {
        return Ok(None);
    }

    endpoint_summary_from_schema(endpoint_schema).map(Some)
}

async fn authorized_endpoint_summaries(
    database: &EdenDb,
    auth: &ParsedJwt,
    endpoint_schemas: Vec<EndpointSchema>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<Vec<EndpointSummary>> {
    let mut endpoints = Vec::new();
    let mut seen_endpoint_uuids = BTreeSet::new();

    for endpoint_schema in endpoint_schemas {
        let endpoint_uuid = endpoint_schema.uuid().to_string();
        if !seen_endpoint_uuids.insert(endpoint_uuid) {
            continue;
        }

        if let Some(summary) = authorized_endpoint_summary(database, auth, endpoint_schema, telemetry_wrapper).await? {
            endpoints.push(summary);
        }
    }

    Ok(endpoints)
}

/// List all endpoints for the organization
/// **Permissions**: See exact permission-bit checks in the handler body.
#[named]
#[utoipa::path(
    get,
    tags = ["Endpoints"],
    path="/endpoints",
    operation_id = "list_endpoints",
    responses((status = OK, body = ListEndpointsResponse))
)]
pub async fn list(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    metrics: web::Data<AllMetrics>,
    durations: TelemetryDurations,
) -> Result<impl Responder, actix_web::Error> {
    let eden_node_uuid = if let Some(service_data) = req.app_data::<web::Data<ServerData>>() {
        service_data.public_key.clone()
    } else {
        EdenNodeUuid::new_uuid()
    };

    let mut labels = TelemetryLabels::new(&eden_node_uuid);
    labels.set_jwt(auth.clone().into_inner());
    labels.set_http_request(&req);

    let mut telemetry_wrapper = TelemetryWrapper::new(metrics.into_inner(), labels.clone(), durations);

    let mut span = telemetry_wrapper.client_tracer(format!("relay.{}", function_name!()));

    let metrics_arc = telemetry_wrapper.metrics().clone();
    let labels_owned = labels.labels_low_cardinality();
    let labels_refs: Vec<(&str, &str)> = labels_owned.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let _endpoint_guard = EndpointGuard::new(metrics_arc.endpoint(), &labels_refs);

    let org_uuid = auth.org_uuid();

    let _ctx = ctx_with_trace!()
        .with_feature("endpoints")
        .with_organization_uuid(org_uuid.to_string())
        .with_eden_node_uuid(eden_node_uuid.to_string());

    let endpoint_schemas =
        database.select_all_endpoints(org_uuid, &mut telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;
    let endpoints = authorized_endpoint_summaries(&database, &auth, endpoint_schemas, &mut telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    span.add_event("Listed endpoints", vec![FastSpanAttribute::new("endpoint_count", endpoints.len() as i64)]);

    EdenResponse::response(endpoints).into()
}

/// List all endpoints for the organization that have been updated recently
/// **Permissions**: See exact permission-bit checks in the handler body.
#[named]
#[utoipa::path(
    get,
    tags = ["Endpoints"],
    path="/endpoints/updated",
    operation_id = "list_endpoints_updated",
    responses((status = OK, body = ListEndpointsResponse))
)]
pub async fn list_updated(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    metrics: web::Data<AllMetrics>,
    durations: TelemetryDurations,
    timestamp: web::Json<String>,
) -> Result<impl Responder, actix_web::Error> {
    let eden_node_uuid = if let Some(service_data) = req.app_data::<web::Data<ServerData>>() {
        service_data.public_key.clone()
    } else {
        EdenNodeUuid::new_uuid()
    };

    let mut labels = TelemetryLabels::new(&eden_node_uuid);
    labels.set_jwt(auth.clone().into_inner());
    labels.set_http_request(&req);

    let time: DateTime<Utc> = DateTime::from_str(timestamp.as_str()).map_err(EpError::parse)?;

    let mut telemetry_wrapper = TelemetryWrapper::new(metrics.into_inner(), labels.clone(), durations);

    let mut span = telemetry_wrapper.client_tracer(format!("relay.{}", function_name!()));

    let metrics_arc = telemetry_wrapper.metrics().clone();
    let labels_owned = labels.labels_low_cardinality();
    let labels_refs: Vec<(&str, &str)> = labels_owned.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let _endpoint_guard = EndpointGuard::new(metrics_arc.endpoint(), &labels_refs);

    let org_uuid = auth.org_uuid();

    let _ctx = ctx_with_trace!()
        .with_feature("endpoints")
        .with_organization_uuid(org_uuid.to_string())
        .with_eden_node_uuid(eden_node_uuid.to_string());

    let endpoint_schemas = database
        .select_all_endpoints_updated(org_uuid, &DateTimeWrapper::from(time), &mut telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    let endpoints = authorized_endpoint_summaries(&database, &auth, endpoint_schemas, &mut telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    span.add_event("Listed endpoints", vec![FastSpanAttribute::new("endpoint_count", endpoints.len() as i64)]);

    let response = ListEndpointsResponse { endpoints };

    EdenResponse::response(response).into()
}
