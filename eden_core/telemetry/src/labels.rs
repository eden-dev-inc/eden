use actix_web::body::BoxBody;
use actix_web::dev::Payload;
use actix_web::http::{Method, StatusCode};
use actix_web::web::Data;
use actix_web::{FromRequest, HttpMessage, HttpRequest, HttpResponse, Responder};
use auth::ParsedJwt;
use borsh::{BorshDeserialize, BorshSerialize};
use format::endpoint::EpKind;
use format::{EdenNodeUuid, EndpointId, EndpointUuid, OrganizationUuid, UserId, UserUuid};
use opentelemetry::KeyValue;
use request::ServerData;
use serde::{Deserialize, Serialize};
use std::future::{Ready, ready};

pub const LABEL_ORG_UUID: &str = "org_uuid";
pub const LABEL_TRAFFIC_CLASS: &str = "traffic_class";
pub const SYSTEM_ORG_UUID: &str = "_system";
pub const TRAFFIC_CLASS_EXTERNAL: &str = "external";
pub const TRAFFIC_CLASS_INTERNAL: &str = "internal";

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct TelemetryLabels {
    // Eden node UUID - mandatory, can't be None
    eden_node_uuid: String,
    /// HTTP Method (Get, Post, etc...)
    http_method: Option<String>,
    /// HTTP Path
    http_path: Option<String>,
    /// HTTP Status Code
    http_status: Option<String>,
    /// gRPC Method
    grpc_method: Option<String>,
    /// Organization Uuid
    org_uuid: Option<String>,
    /// User Id
    user_id: Option<String>,
    /// User Uuid
    user_uuid: Option<String>,
    /// Endpoint Id
    endpoint_id: Option<String>,
    /// Endpoint Uuid
    endpoint_uuid: Option<String>,
    /// Endpoint type (Mongo, Redis, etc...)
    endpoint_kind: Option<EpKind>,
    /// Endpoint type (Mongo, Redis, etc...)
    endpoint_request: Option<String>,
    /// Read or Write
    access: Option<String>,
    /// Trace ID for correlating with spans
    trace_id: Option<String>,
    /// Span ID for correlating with spans
    span_id: Option<String>,
    /// HTTP User-Agent header
    user_agent: Option<String>,
    /// HTTP Content-Type header
    content_type: Option<String>,
    /// Request ID (x-request-id header)
    request_id: Option<String>,
    /// Correlation ID (x-correlation-id header)
    correlation_id: Option<String>,
    /// Client IP address (from TCP connection)
    client_ip: Option<String>,
    /// Whether the telemetry came from Eden's own control-plane/internal work
    /// or from user/API/proxy traffic.
    traffic_class: String,
}

impl Responder for TelemetryLabels {
    type Body = BoxBody;

    fn respond_to(self, _request: &HttpRequest) -> HttpResponse {
        HttpResponse::Ok().json(self)
    }
}

impl FromRequest for TelemetryLabels {
    type Error = actix_web::Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        // Try to get the pre-populated labels from request extensions first
        if let Some(labels) = req.extensions().get::<TelemetryLabels>() {
            ready(Ok(labels.clone()))
        } else {
            // Fallback: create default labels if none exist in extensions
            // This shouldn't happen if the middleware is properly set up

            let mut labels = if let Some(server_data) = req.app_data::<Data<ServerData>>() {
                TelemetryLabels::new(&server_data.public_key)
            } else {
                TelemetryLabels::new(&EdenNodeUuid::new_uuid())
            };
            labels.set_http_request(req);

            // Try to extract JWT information if available
            if let Some(jwt) = req.extensions().get::<ParsedJwt>() {
                labels.set_jwt(jwt.clone());
            }

            ready(Ok(labels))
        }
    }
}

impl TelemetryLabels {
    pub fn new(eden_node_uuid: &EdenNodeUuid) -> Self {
        Self {
            eden_node_uuid: eden_node_uuid.to_string(),
            http_method: None,
            http_path: None,
            http_status: None,
            grpc_method: None,
            org_uuid: None,
            user_id: None,
            user_uuid: None,
            endpoint_id: None,
            endpoint_uuid: None,
            endpoint_kind: None,
            endpoint_request: None,
            access: None,
            trace_id: None,
            span_id: None,
            user_agent: None,
            content_type: None,
            request_id: None,
            correlation_id: None,
            client_ip: None,
            traffic_class: TRAFFIC_CLASS_INTERNAL.to_string(),
        }
    }

    pub fn set_node_uuid(&mut self, uuid: EdenNodeUuid) {
        self.eden_node_uuid = uuid.to_string();
    }
    pub fn set_org_uuid(&mut self, uuid: OrganizationUuid) {
        self.org_uuid.replace(uuid.to_string());
    }
    pub fn org_uuid(&self) -> Option<&str> {
        self.org_uuid.as_deref()
    }
    pub fn set_user_id(&mut self, id: UserId) {
        self.user_id.replace(id.to_string());
    }
    pub fn set_user_uuid(&mut self, uuid: UserUuid) {
        self.user_uuid.replace(uuid.to_string());
    }
    pub fn user_uuid(&self) -> Option<&str> {
        self.user_uuid.as_deref()
    }

    pub fn set_endpoint_id(&mut self, id: EndpointId) {
        self.endpoint_id.replace(id.to_string());
    }
    pub fn endpoint_id(&self) -> Option<&str> {
        self.endpoint_id.as_deref()
    }
    pub fn set_endpoint_uuid(&mut self, uuid: EndpointUuid) {
        self.endpoint_uuid.replace(uuid.to_string());
    }
    pub fn endpoint_uuid(&self) -> Option<&str> {
        self.endpoint_uuid.as_deref()
    }
    pub fn set_endpoint_kind(&mut self, kind: EpKind) {
        self.endpoint_kind.replace(kind);
    }
    pub fn set_read(&mut self) {
        self.access.replace("read".to_string());
    }
    pub fn set_write(&mut self) {
        self.access.replace("write".to_string());
    }
    // endpoint request type
    pub fn set_delete_request(&mut self) {
        self.endpoint_request.replace("delete".to_string());
        self.set_write();
    }
    pub fn set_get_request(&mut self) {
        self.endpoint_request.replace("get".to_string());
        self.set_read();
    }
    pub fn set_ati_request(&mut self) {
        self.endpoint_request.replace("ati".to_string());
        self.set_write();
    }
    pub fn set_patch_request(&mut self) {
        self.endpoint_request.replace("patch".to_string());
        self.set_write();
    }
    pub fn set_post_request(&mut self) {
        self.endpoint_request.replace("post".to_string());
        self.set_write();
    }
    pub fn set_read_request(&mut self) {
        self.endpoint_request.replace("read".to_string());
        self.set_read();
    }
    pub fn set_transaction_request(&mut self) {
        self.endpoint_request.replace("transaction".to_string());
        self.set_write();
    }
    pub fn set_write_request(&mut self) {
        self.endpoint_request.replace("write".to_string());
        self.set_write();
    }
    // jwt
    pub fn set_jwt(&mut self, jwt: ParsedJwt) {
        self.set_org_uuid(jwt.org_uuid().clone());
        self.set_user_uuid(jwt.user_uuid().clone());
        self.set_user_id(jwt.user_id().clone());
    }
    pub fn set_http_request(&mut self, request: &HttpRequest) {
        self.http_method.replace(request.method().to_string());
        self.set_http_path(request.path());
    }
    pub fn set_http_method(&mut self, method: &Method) {
        self.http_method.replace(method.to_string());
    }
    pub fn set_http_path(&mut self, path: &str) {
        self.http_path.replace(path.to_string());
        self.set_traffic_class_for_path(path);
    }
    pub fn set_http_status(&mut self, status: StatusCode) {
        self.http_status.replace(status.to_string());
    }
    pub fn set_grpc_method(&mut self, grpc_method: String) {
        self.grpc_method.replace(grpc_method);
    }
    pub fn set_trace_context(&mut self) {
        use opentelemetry::Context;
        use opentelemetry::trace::{SpanContext, TraceContextExt};

        let context = Context::current();
        let span_context: SpanContext = context.span().span_context().clone();

        if span_context.is_valid() {
            self.trace_id.replace(span_context.trace_id().to_string());
            self.span_id.replace(span_context.span_id().to_string());
        }
    }

    pub fn set_trace_ids(&mut self, trace_id: String, span_id: String) {
        self.trace_id.replace(trace_id);
        self.span_id.replace(span_id);
    }

    pub fn set_client_ip(&mut self, ip: String) {
        self.client_ip.replace(ip);
    }

    pub fn set_internal_traffic(&mut self) {
        self.traffic_class = TRAFFIC_CLASS_INTERNAL.to_string();
    }

    pub fn set_external_traffic(&mut self) {
        self.traffic_class = TRAFFIC_CLASS_EXTERNAL.to_string();
    }

    pub fn traffic_class(&self) -> &str {
        self.traffic_class.as_str()
    }

    fn set_traffic_class_for_path(&mut self, path: &str) {
        if is_internal_eden_path(path) {
            self.set_internal_traffic();
        } else {
            self.set_external_traffic();
        }
    }

    pub fn client_ip(&self) -> Option<&str> {
        self.client_ip.as_deref()
    }

    /// Set HTTP header fields from metadata
    pub fn set_http_headers(
        &mut self,
        user_agent: Option<String>,
        content_type: Option<String>,
        request_id: Option<String>,
        correlation_id: Option<String>,
    ) {
        self.user_agent = user_agent;
        self.content_type = content_type;
        self.request_id = request_id;
        self.correlation_id = correlation_id;
    }

    /// Extract and set HTTP headers from MetadataMap
    pub fn populate_from_metadata(&mut self, metadata: &crate::MetadataMap) {
        use tonic::metadata::MetadataKey;

        // Extract user-agent
        if let Ok(key) = "user-agent".parse::<MetadataKey<_>>()
            && let Some(value) = metadata.get(&key)
            && let Ok(s) = value.to_str()
        {
            self.user_agent = Some(s.to_string());
        }

        // Extract content-type
        if let Ok(key) = "content-type".parse::<MetadataKey<_>>()
            && let Some(value) = metadata.get(&key)
            && let Ok(s) = value.to_str()
        {
            self.content_type = Some(s.to_string());
        }

        // Extract x-request-id
        if let Ok(key) = "x-request-id".parse::<MetadataKey<_>>()
            && let Some(value) = metadata.get(&key)
            && let Ok(s) = value.to_str()
        {
            self.request_id = Some(s.to_string());
        }

        // Extract x-correlation-id
        if let Ok(key) = "x-correlation-id".parse::<MetadataKey<_>>()
            && let Some(value) = metadata.get(&key)
            && let Ok(s) = value.to_str()
        {
            self.correlation_id = Some(s.to_string());
        }
    }
}

impl TelemetryLabels {
    pub fn key_value(&self) -> Vec<KeyValue> {
        vec![
            Some(KeyValue::new("eden_node_uuid", self.eden_node_uuid.to_string())),
            self.http_method.as_ref().map(|v| KeyValue::new("http_method", v.to_string())),
            self.http_path.as_ref().map(|v| KeyValue::new("http_path", v.to_string())),
            self.http_status.as_ref().map(|v| KeyValue::new("http_status", v.to_string())),
            self.grpc_method.as_ref().map(|v| KeyValue::new("grpc_method", v.to_string())),
            self.org_uuid.as_ref().map(|v| KeyValue::new("org_uuid", v.to_string())),
            self.user_id.as_ref().map(|v| KeyValue::new("user_id", v.to_string())),
            self.user_uuid.as_ref().map(|v| KeyValue::new("user_uuid", v.to_string())),
            self.endpoint_uuid.as_ref().map(|v| KeyValue::new("endpoint_uuid", v.to_string())),
            self.endpoint_kind.as_ref().map(|v| KeyValue::new("endpoint_type", v.to_string())),
            self.endpoint_request.as_ref().map(|v| KeyValue::new("endpoint_request", v.to_string())),
            self.access.as_ref().map(|v| KeyValue::new("access", v.to_string())),
            self.trace_id.as_ref().map(|v| KeyValue::new("trace_id", v.to_string())),
            self.span_id.as_ref().map(|v| KeyValue::new("span_id", v.to_string())),
            self.user_agent.as_ref().map(|v| KeyValue::new("user_agent", v.to_string())),
            self.content_type.as_ref().map(|v| KeyValue::new("content_type", v.to_string())),
            self.request_id.as_ref().map(|v| KeyValue::new("request_id", v.to_string())),
            self.correlation_id.as_ref().map(|v| KeyValue::new("correlation_id", v.to_string())),
            self.client_ip.as_ref().map(|v| KeyValue::new("client_ip", v.to_string())),
            Some(KeyValue::new(LABEL_TRAFFIC_CLASS, self.traffic_class.clone())),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    /// Returns only low-cardinality labels suitable for metrics aggregation.
    /// Excludes: http_path, trace_id, span_id, request_id, correlation_id,
    /// user_uuid, user_id, user_agent, endpoint_uuid.
    pub fn key_value_low_cardinality(&self) -> Vec<KeyValue> {
        vec![
            Some(KeyValue::new("eden_node_uuid", self.eden_node_uuid.to_string())),
            self.org_uuid.as_ref().map(|v| KeyValue::new("org_uuid", v.to_string())),
            self.endpoint_kind.as_ref().map(|v| KeyValue::new("endpoint_type", v.to_string())),
            Some(KeyValue::new(LABEL_TRAFFIC_CLASS, self.traffic_class.clone())),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    /// Returns low-cardinality labels as owned string tuples for fast-telemetry.
    pub fn labels_low_cardinality(&self) -> Vec<(String, String)> {
        let mut labels = vec![("eden_node_uuid".to_string(), self.eden_node_uuid.clone())];
        if let Some(ref v) = self.org_uuid {
            labels.push(("org_uuid".to_string(), v.clone()));
        }
        if let Some(ref v) = self.endpoint_kind {
            labels.push(("endpoint_type".to_string(), v.to_string()));
        }
        labels.push((LABEL_TRAFFIC_CLASS.to_string(), self.traffic_class.clone()));
        labels
    }
}

fn is_internal_eden_path(path: &str) -> bool {
    path.starts_with("/dashboard")
        || path.starts_with("/api/v1/analytics/series")
        || path.starts_with("/api/v1/analytics/telemetry")
        || path.starts_with("/api/v1/llm/gateway/dashboard")
        || path.starts_with("/api/v1/llm/cost/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_labels_default_to_internal_traffic() {
        let node = EdenNodeUuid::new_uuid();
        let labels = TelemetryLabels::new(&node);
        assert_eq!(labels.traffic_class(), TRAFFIC_CLASS_INTERNAL);
        let pairs = labels.labels_low_cardinality();
        assert!(pairs.iter().any(|(key, value)| key == LABEL_TRAFFIC_CLASS && value == TRAFFIC_CLASS_INTERNAL));
    }

    #[test]
    fn dashboard_paths_are_internal_traffic() {
        let node = EdenNodeUuid::new_uuid();
        let mut labels = TelemetryLabels::new(&node);
        labels.set_http_path("/dashboard/metrics");
        assert_eq!(labels.traffic_class(), TRAFFIC_CLASS_INTERNAL);
        let pairs = labels.labels_low_cardinality();
        assert!(pairs.iter().any(|(key, value)| key == LABEL_TRAFFIC_CLASS && value == TRAFFIC_CLASS_INTERNAL));
    }

    #[test]
    fn user_api_paths_are_external_traffic() {
        let node = EdenNodeUuid::new_uuid();
        let mut labels = TelemetryLabels::new(&node);
        labels.set_http_path("/api/v1/endpoints");
        assert_eq!(labels.traffic_class(), TRAFFIC_CLASS_EXTERNAL);
    }
}
