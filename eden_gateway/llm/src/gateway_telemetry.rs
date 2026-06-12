use eden_core::format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use eden_core::format::endpoint::EpKind;
use eden_core::format::{CacheUuid, EdenUuid, EndpointUuid, OrganizationUuid};
use eden_core::telemetry::{FastSpan, TelemetryWrapper};
use endpoint_core::llm_core::LlmGatewayAuthScheme;
use std::net::SocketAddr;
use std::time::Instant;

pub(crate) trait GatewayHttpTelemetrySource {
    fn method(&self) -> &str;
    fn path_without_query(&self) -> &str;
    fn header(&self, name: &str) -> Option<&str>;
}

#[derive(Debug, Clone)]
pub(crate) struct GatewayTelemetryContext {
    method: String,
    route: &'static str,
    path: String,
    client_addr: SocketAddr,
    interlay_uuid: String,
    org_uuid: Option<OrganizationUuid>,
    endpoint_uuid: Option<EndpointUuid>,
    auth_scheme: &'static str,
}

impl GatewayTelemetryContext {
    pub(crate) fn from_http_request<T>(
        telemetry_wrapper: &mut TelemetryWrapper,
        request: &T,
        interlay_cache_uuid: &InterlayCacheUuid,
        endpoint_kind: EpKind,
        client_addr: SocketAddr,
        route_for_path: impl FnOnce(&str) -> &'static str,
    ) -> Self
    where
        T: GatewayHttpTelemetrySource,
    {
        GatewayTelemetry::capture_trace_context(telemetry_wrapper, request);

        let path = request.path_without_query();
        let route = route_for_path(path);
        let auth_scheme = GatewayTelemetry::auth_scheme(request);
        let organization_uuid = interlay_cache_uuid.org().map(|org| org.eden_uuid::<OrganizationUuid>());
        if let Some(organization_uuid) = organization_uuid.clone() {
            telemetry_wrapper.set_org_uuid(organization_uuid);
        }
        let interlay_uuid = interlay_cache_uuid.uuid().to_string();

        telemetry_wrapper.mut_labels(|labels| {
            labels.set_http_path(path);
            labels.set_client_ip(client_addr.ip().to_string());
            labels.set_endpoint_kind(endpoint_kind);
        });

        Self {
            method: request.method().to_string(),
            route,
            path: path.to_string(),
            client_addr,
            interlay_uuid,
            org_uuid: organization_uuid,
            endpoint_uuid: None,
            auth_scheme,
        }
    }

    pub(crate) fn set_endpoint_uuid(&mut self, endpoint_cache_uuid: &EndpointCacheUuid) {
        self.endpoint_uuid = Some(endpoint_cache_uuid.eden_uuid::<EndpointUuid>());
    }

    pub(crate) fn set_request_span_attributes(&self, span: &mut FastSpan, route_attribute: &'static str, auth_attribute: &'static str) {
        span.set_attribute(route_attribute, self.route);
        span.set_attribute("http.request.method", self.method.clone());
        span.set_attribute("url.path", self.path.clone());
        span.set_attribute("network.peer.address", self.client_addr.ip().to_string());
        span.set_attribute("network.peer.port", self.client_addr.port().to_string());
        span.set_attribute(auth_attribute, self.auth_scheme);
        span.set_attribute("eden.interlay_uuid", self.interlay_uuid.clone());
    }

    pub(crate) fn set_response_span_attributes(&self, span: &mut FastSpan, status_code: &str) {
        span.set_attribute("http.response.status_code", status_code.to_string());
        span.set_attribute("http.route", self.route);
        span.set_attribute("url.path", self.path.clone());
        span.set_attribute("network.peer.address", self.client_addr.ip().to_string());
        span.set_attribute("eden.interlay_uuid", self.interlay_uuid.clone());
        span.set_attribute("eden.endpoint_uuid", self.endpoint_uuid_label());
    }

    pub(crate) fn method(&self) -> &str {
        self.method.as_str()
    }

    pub(crate) fn route(&self) -> &'static str {
        self.route
    }

    pub(crate) fn endpoint_uuid_label(&self) -> String {
        self.endpoint_uuid
            .as_ref()
            .map(|endpoint_uuid| endpoint_uuid.uuid().to_string())
            .unwrap_or_else(|| "unknown".to_string())
    }

    pub(crate) fn endpoint_uuid(&self) -> Option<&EndpointUuid> {
        self.endpoint_uuid.as_ref()
    }

    pub(crate) fn org_uuid_label(&self) -> String {
        self.org_uuid
            .as_ref()
            .map(|org_uuid| org_uuid.uuid().to_string())
            .unwrap_or_else(|| eden_core::telemetry::labels::SYSTEM_ORG_UUID.to_string())
    }

    pub(crate) fn org_uuid(&self) -> Option<&OrganizationUuid> {
        self.org_uuid.as_ref()
    }

    pub(crate) fn auth_scheme(&self) -> &'static str {
        self.auth_scheme
    }
}

pub(crate) struct GatewayTelemetry;

impl GatewayTelemetry {
    pub(crate) fn capture_trace_context<T>(telemetry_wrapper: &mut TelemetryWrapper, request: &T)
    where
        T: GatewayHttpTelemetrySource,
    {
        let metadata = telemetry_wrapper.context_mut().metadata_mut();
        for header in ["traceparent", "tracestate"] {
            if let Some(value) = request.header(header)
                && let Ok(parsed) = value.parse()
            {
                metadata.remove(header);
                metadata.insert(header, parsed);
            }
        }
    }

    pub(crate) fn auth_scheme<T>(request: &T) -> &'static str
    where
        T: GatewayHttpTelemetrySource,
    {
        LlmGatewayAuthScheme::classify(request.header("authorization"), request.header("x-api-key"), request.header("api-key")).as_str()
    }

    pub(crate) fn bool_label(value: bool) -> &'static str {
        if value { "true" } else { "false" }
    }

    pub(crate) fn status_class(status: u16) -> &'static str {
        match status {
            100..=199 => "1xx",
            200..=299 => "2xx",
            300..=399 => "3xx",
            400..=499 => "4xx",
            500..=599 => "5xx",
            _ => "unknown",
        }
    }

    pub(crate) fn elapsed_since_us(start: Instant) -> u64 {
        start.elapsed().as_micros().min(u64::MAX as u128) as u64
    }

    pub(crate) fn elapsed_between_us(start: Instant, end: Instant) -> u64 {
        end.saturating_duration_since(start).as_micros().min(u64::MAX as u128) as u64
    }
}
