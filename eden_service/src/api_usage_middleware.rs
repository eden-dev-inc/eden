//! API usage tracking middleware.
//!
//! This middleware records API usage statistics for each authenticated request.

use crate::user_sessions::{API_USAGE_STORE, ApiUsageEntry, SESSION_STORE};
use actix_http::HttpMessage;
use actix_web::Error;
use actix_web::body::{BodySize, MessageBody};
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use chrono::Utc;
use eden_core::auth::ParsedJwt;
use std::future::{Future, Ready, ready};
use std::pin::Pin;
use std::task::{Context, Poll};
use uuid::Uuid;

/// API usage tracking middleware.
pub struct ApiUsageTracking;

impl<S, B> Transform<S, ServiceRequest> for ApiUsageTracking
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = ApiUsageTrackingMiddleware<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(ApiUsageTrackingMiddleware { service }))
    }
}

/// The middleware service that wraps requests.
pub struct ApiUsageTrackingMiddleware<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for ApiUsageTrackingMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&self, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(ctx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let start_time = Utc::now();
        let request_id = Uuid::new_v4().to_string();

        // Capture request info before moving req
        let http_method = req.method().to_string();
        let http_path = req.path().to_string();
        let client_ip = req.connection_info().realip_remote_addr().unwrap_or("unknown").to_string();
        let user_agent = req.headers().get("user-agent").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();

        // Get request size from content-length header
        let request_bytes =
            req.headers().get("content-length").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);

        // Try to extract endpoint_uuid from path (e.g., /endpoints/{uuid}/...)
        let endpoint_uuid = extract_endpoint_uuid_from_path(&http_path);

        let fut = self.service.call(req);

        Box::pin(async move {
            let response = fut.await?;

            // Try to get auth info from response request extensions
            // The auth middleware runs as part of the inner service and populates extensions
            let auth_info = response.request().extensions().get::<ParsedJwt>().map(|jwt| {
                (
                    jwt.org_uuid().to_string(),
                    jwt.user_uuid().to_string(),
                    jwt.user_id().as_str().to_string(),
                    jwt.jti().map(|s| s.to_string()),
                )
            });

            // Debug: log whether we found auth info
            log::debug!("API usage middleware: path={}, auth_info={}", http_path, auth_info.is_some());

            // Record usage only if we have auth info
            if let Some((organization_uuid, user_uuid, user_id, jti)) = auth_info {
                log::debug!(
                    "Recording API usage: organization_uuid={}, user_uuid={}, path={}",
                    organization_uuid,
                    user_uuid,
                    http_path
                );

                // Update session with jti for token revocation tracking
                let session_uuid = SESSION_STORE.record_session_with_jti(
                    &organization_uuid,
                    &user_uuid,
                    &user_id,
                    &client_ip,
                    &user_agent,
                    analytics_schema::events::AuthMethod::Bearer,
                    jti.as_deref(),
                );
                let status = response.status();
                let end_time = Utc::now();
                let latency_us = (end_time - start_time).num_microseconds().unwrap_or(0) as u64;

                // Get response size
                let response_bytes = match response.response().body().size() {
                    BodySize::Sized(size) => size,
                    _ => 0,
                };

                // Extract error info from response if it's an error status
                let (error_code, error_message) = if status.is_client_error() || status.is_server_error() {
                    (Some(status.as_str().to_string()), status.canonical_reason().map(|s| s.to_string()))
                } else {
                    (None, None)
                };

                let entry = ApiUsageEntry {
                    request_time: start_time,
                    organization_uuid,
                    user_uuid,
                    user_id,
                    session_uuid: Some(session_uuid),
                    request_id,
                    http_method,
                    http_path,
                    http_status: status.as_u16(),
                    endpoint_uuid,
                    endpoint_id: None,
                    latency_us,
                    request_bytes,
                    response_bytes,
                    client_ip,
                    user_agent,
                    error_code,
                    error_message,
                };

                API_USAGE_STORE.record(entry);
            }

            Ok(response)
        })
    }
}

/// Extract endpoint UUID from request path if present.
fn extract_endpoint_uuid_from_path(path: &str) -> Option<String> {
    // Common patterns: /endpoints/{uuid}, /endpoints/{uuid}/read, etc.
    let parts: Vec<&str> = path.split('/').collect();

    for (i, part) in parts.iter().enumerate() {
        if *part == "endpoints" || *part == "endpoint" {
            if let Some(uuid_str) = parts.get(i + 1) {
                // Validate it looks like a UUID
                if uuid_str.len() == 36 && uuid_str.contains('-') {
                    return Some(uuid_str.to_string());
                }
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_endpoint_uuid() {
        assert_eq!(
            extract_endpoint_uuid_from_path("/api/v1/endpoints/550e8400-e29b-41d4-a716-446655440000/read"),
            Some("550e8400-e29b-41d4-a716-446655440000".to_string())
        );
        assert_eq!(
            extract_endpoint_uuid_from_path("/api/v1/endpoints/550e8400-e29b-41d4-a716-446655440000"),
            Some("550e8400-e29b-41d4-a716-446655440000".to_string())
        );
        assert_eq!(extract_endpoint_uuid_from_path("/api/v1/organizations"), None);
        assert_eq!(extract_endpoint_uuid_from_path("/api/v1/endpoints/invalid"), None);
    }
}
