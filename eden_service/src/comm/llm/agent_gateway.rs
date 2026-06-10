//! Control-plane API for the in-memory agent gateway connection registry.

use actix_web::{HttpResponse, Responder, web};
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::EdenUuid;
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use eden_gateway::agent::{
    AgentConnectionMetrics, AgentConnectionRegistration, AgentConnectionSession, AgentGatewayIdentity, AgentGatewayNetworkEndpoint,
    AgentGatewayRateLimit, AgentGatewayRoute, AgentGatewayState, AgentGatewayTransport, AgentGatewayUsageEvent,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use telemetry_extensions_macro::with_telemetry;
use uuid::Uuid;

use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;

#[derive(Debug, Deserialize)]
pub struct RegisterAgentGatewayConnectionRequest {
    pub agent_id: Uuid,
    pub transport: AgentGatewayTransport,
    #[serde(default)]
    pub advertise_url: Option<String>,
    #[serde(default)]
    pub callback_url: Option<String>,
    #[serde(default)]
    pub node_id: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub identity: AgentGatewayIdentity,
    #[serde(default)]
    pub metrics: AgentConnectionMetrics,
    #[serde(default)]
    pub rate_limit: AgentGatewayRateLimit,
}

#[derive(Debug, Deserialize)]
pub struct HeartbeatAgentGatewayConnectionRequest {
    #[serde(default)]
    pub metrics: AgentConnectionMetrics,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct AgentGatewayConnectionResponse {
    pub session_id: String,
    pub org_id: String,
    pub agent_id: String,
    pub fingerprint: String,
    pub instance_id: Option<String>,
    pub principal: Option<String>,
    pub tags: BTreeMap<String, String>,
    pub transport: String,
    pub advertise_url: Option<String>,
    pub callback_url: Option<String>,
    pub node_id: Option<String>,
    pub region: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub active_streams: u32,
    pub queued_messages: u32,
    pub avg_latency_ms: Option<u64>,
    pub last_error: Option<String>,
    pub requests_per_minute: Option<u64>,
    pub prompt_tokens_per_minute: Option<u64>,
    pub completion_tokens_per_minute: Option<u64>,
    pub total_tokens_per_minute: Option<u64>,
    pub max_active_streams: Option<u32>,
    pub max_queued_messages: Option<u32>,
    pub status: String,
    pub connected_at_ms: u64,
    pub last_heartbeat_ms: u64,
    pub expires_at_ms: u64,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct AgentGatewayRouteResponse {
    pub org_id: String,
    pub agent_id: String,
    pub fingerprint: String,
    pub instance_id: Option<String>,
    pub principal: Option<String>,
    pub tags: BTreeMap<String, String>,
    pub session_id: String,
    pub transport: String,
    pub advertise_url: Option<String>,
    pub callback_url: Option<String>,
    pub node_id: Option<String>,
    pub region: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub requests_per_minute: Option<u64>,
    pub prompt_tokens_per_minute: Option<u64>,
    pub completion_tokens_per_minute: Option<u64>,
    pub total_tokens_per_minute: Option<u64>,
    pub max_active_streams: Option<u32>,
    pub max_queued_messages: Option<u32>,
    pub active_streams: u32,
    pub queued_messages: u32,
    pub last_heartbeat_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct RecordAgentGatewayUsageRequest {
    #[serde(default)]
    pub usage: AgentGatewayUsageEvent,
}

#[with_telemetry]
pub async fn register_connection(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    gateway: web::Data<AgentGatewayState>,
    payload: web::Json<RegisterAgentGatewayConnectionRequest>,
) -> Result<impl Responder, actix_web::Error> {
    verify_agent_gateway_access(&database, &auth, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    let payload = payload.into_inner();
    verify_agent_belongs_to_org(&database, auth.org_uuid().uuid(), payload.agent_id, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let session = gateway
        .register_connection(AgentConnectionRegistration {
            org_id: auth.org_uuid().to_string(),
            agent_id: payload.agent_id.to_string(),
            transport: payload.transport,
            identity: payload.identity,
            endpoint: AgentGatewayNetworkEndpoint {
                advertise_url: payload.advertise_url,
                callback_url: payload.callback_url,
                node_id: payload.node_id,
                region: payload.region,
                labels: payload.labels,
            },
            metrics: payload.metrics,
            rate_limit: payload.rate_limit,
        })
        .map_err(|e| error_handling(EpError::request(e.to_string()), &mut span))?;

    Ok::<HttpResponse, actix_web::Error>(
        HttpResponse::Created().json(EdenResponse::response(AgentGatewayConnectionResponse::from(session))),
    )
}

#[with_telemetry]
pub async fn list_connections(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    gateway: web::Data<AgentGatewayState>,
) -> Result<impl Responder, actix_web::Error> {
    verify_agent_gateway_access(&database, &auth, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    let response = gateway
        .list_connections(&auth.org_uuid().to_string())
        .into_iter()
        .map(AgentGatewayConnectionResponse::from)
        .collect::<Vec<_>>();

    EdenResponse::response(response).into()
}

#[with_telemetry]
pub async fn heartbeat_connection(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    gateway: web::Data<AgentGatewayState>,
    path: web::Path<String>,
    payload: web::Json<HeartbeatAgentGatewayConnectionRequest>,
) -> Result<impl Responder, actix_web::Error> {
    verify_agent_gateway_access(&database, &auth, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    let session = gateway
        .heartbeat(&auth.org_uuid().to_string(), path.as_str(), payload.into_inner().metrics)
        .map_err(|e| error_handling(EpError::request(e.to_string()), &mut span))?;

    EdenResponse::response(AgentGatewayConnectionResponse::from(session)).into()
}

#[with_telemetry]
pub async fn mark_connection_draining(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    gateway: web::Data<AgentGatewayState>,
    path: web::Path<String>,
) -> Result<impl Responder, actix_web::Error> {
    verify_agent_gateway_access(&database, &auth, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    let session = gateway
        .mark_draining(&auth.org_uuid().to_string(), path.as_str())
        .map_err(|e| error_handling(EpError::request(e.to_string()), &mut span))?;

    EdenResponse::response(AgentGatewayConnectionResponse::from(session)).into()
}

#[with_telemetry]
pub async fn disconnect_connection(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    gateway: web::Data<AgentGatewayState>,
    path: web::Path<String>,
) -> Result<impl Responder, actix_web::Error> {
    verify_agent_gateway_access(&database, &auth, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    gateway
        .disconnect(&auth.org_uuid().to_string(), path.as_str())
        .map_err(|e| error_handling(EpError::request(e.to_string()), &mut span))?;

    Ok::<HttpResponse, actix_web::Error>(HttpResponse::NoContent().finish())
}

#[with_telemetry]
pub async fn route_to_agent(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    gateway: web::Data<AgentGatewayState>,
    path: web::Path<Uuid>,
) -> Result<impl Responder, actix_web::Error> {
    verify_agent_gateway_access(&database, &auth, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    let agent_id = path.into_inner();
    verify_agent_belongs_to_org(&database, auth.org_uuid().uuid(), agent_id, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let Some(route) = gateway.route_to_agent(&auth.org_uuid().to_string(), &agent_id.to_string()) else {
        return Err(error_handling(EpError::request("agent has no active gateway route"), &mut span));
    };

    EdenResponse::response(AgentGatewayRouteResponse::from(route)).into()
}

#[with_telemetry]
pub async fn record_usage(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    gateway: web::Data<AgentGatewayState>,
    path: web::Path<String>,
    payload: web::Json<RecordAgentGatewayUsageRequest>,
) -> Result<impl Responder, actix_web::Error> {
    verify_agent_gateway_access(&database, &auth, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    let decision = gateway
        .record_usage(&auth.org_uuid().to_string(), path.as_str(), payload.into_inner().usage)
        .map_err(|e| error_handling(EpError::request(e.to_string()), &mut span))?;

    EdenResponse::response(decision).into()
}

#[with_telemetry]
pub async fn list_usage(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    gateway: web::Data<AgentGatewayState>,
) -> Result<impl Responder, actix_web::Error> {
    verify_agent_gateway_access(&database, &auth, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    let response = gateway.list_usage(&auth.org_uuid().to_string());
    EdenResponse::response(response).into()
}

async fn verify_agent_gateway_access(
    database: &EdenDb,
    auth: &ParsedJwt,
    telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) -> Result<(), EpError> {
    verify_control_perms(database, auth, None, ControlPerms::CONFIGURE, telemetry_wrapper).await
}

async fn verify_agent_belongs_to_org(
    database: &EdenDb,
    org_uuid: Uuid,
    agent_id: Uuid,
    telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) -> Result<(), EpError> {
    let agent = database.load_agent(agent_id, telemetry_wrapper).await?;
    if agent.organization_uuid != org_uuid {
        return Err(EpError::rbac("Agent does not belong to this organization"));
    }
    Ok(())
}

impl From<AgentConnectionSession> for AgentGatewayConnectionResponse {
    fn from(value: AgentConnectionSession) -> Self {
        Self {
            session_id: value.session_id,
            org_id: value.org_id,
            agent_id: value.agent_id,
            fingerprint: value.identity.fingerprint,
            instance_id: value.identity.instance_id,
            principal: value.identity.principal,
            tags: value.identity.tags,
            transport: value.transport.to_string(),
            advertise_url: value.endpoint.advertise_url,
            callback_url: value.endpoint.callback_url,
            node_id: value.endpoint.node_id,
            region: value.endpoint.region,
            labels: value.endpoint.labels,
            active_streams: value.metrics.active_streams,
            queued_messages: value.metrics.queued_messages,
            avg_latency_ms: value.metrics.avg_latency_ms,
            last_error: value.metrics.last_error,
            requests_per_minute: value.rate_limit.requests_per_minute,
            prompt_tokens_per_minute: value.rate_limit.prompt_tokens_per_minute,
            completion_tokens_per_minute: value.rate_limit.completion_tokens_per_minute,
            total_tokens_per_minute: value.rate_limit.total_tokens_per_minute,
            max_active_streams: value.rate_limit.max_active_streams,
            max_queued_messages: value.rate_limit.max_queued_messages,
            status: value.status.to_string(),
            connected_at_ms: value.connected_at_ms,
            last_heartbeat_ms: value.last_heartbeat_ms,
            expires_at_ms: value.expires_at_ms,
        }
    }
}

impl From<AgentGatewayRoute> for AgentGatewayRouteResponse {
    fn from(value: AgentGatewayRoute) -> Self {
        Self {
            org_id: value.org_id,
            agent_id: value.agent_id,
            fingerprint: value.identity.fingerprint,
            instance_id: value.identity.instance_id,
            principal: value.identity.principal,
            tags: value.identity.tags,
            session_id: value.session_id,
            transport: value.transport.to_string(),
            advertise_url: value.endpoint.advertise_url,
            callback_url: value.endpoint.callback_url,
            node_id: value.endpoint.node_id,
            region: value.endpoint.region,
            labels: value.endpoint.labels,
            requests_per_minute: value.rate_limit.requests_per_minute,
            prompt_tokens_per_minute: value.rate_limit.prompt_tokens_per_minute,
            completion_tokens_per_minute: value.rate_limit.completion_tokens_per_minute,
            total_tokens_per_minute: value.rate_limit.total_tokens_per_minute,
            max_active_streams: value.rate_limit.max_active_streams,
            max_queued_messages: value.rate_limit.max_queued_messages,
            active_streams: value.active_streams,
            queued_messages: value.queued_messages,
            last_heartbeat_ms: value.last_heartbeat_ms,
        }
    }
}
