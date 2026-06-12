use std::borrow::Cow;

use database::endpoint_schema::endpoint::EndpointSchema;
use eden_core::error::{ConnectError, EpError};
use eden_core::format::OrganizationCacheUuid;
#[cfg(any(feature = "mongo", feature = "redis", feature = "postgres"))]
use eden_core::format::endpoint::EpKind;
use eden_core::macros::execute_with_timeout;
use eden_core::telemetry::{FastSpanAttribute, FastSpanStatus, TelemetryWrapper};
use endpoint::EpRequest;
use ep_core::ep::EpConnection;
use ep_core::settings::EdenSettings;
use function_name::named;
use serde_json::Value;
use tokio::time::Duration;

#[cfg(any(feature = "mongo", feature = "redis", feature = "postgres"))]
use super::analytics;
use crate::comp::MyEngineService;

enum ReadDispatch {
    Pooled { organization_cache_uuid: OrganizationCacheUuid },
    Els { els_conn: Box<dyn EpConnection> },
}

impl MyEngineService {
    #[named]
    async fn read_with_reconnect(
        &self,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());
        let endpoint_cache_key = endpoint_schema.cache_key(organization_cache_uuid);
        let kind = endpoint_schema.kind();

        let lock = self.router.read().await;
        let ep = match lock.get(&kind) {
            Some(route) => route,
            None => {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned("could not get endpoint".to_string()) });
                return Err(EpError::Connect(ConnectError::CouldNotGetEndpoint));
            }
        };

        span.add_simple_event("processing async read");

        let result = execute_with_timeout!(
            span,
            telemetry_wrapper,
            settings,
            ep,
            read_boxed(&endpoint_cache_key, request, settings, telemetry_wrapper)
        );

        drop(lock);
        span.add_simple_event("dropped lock");
        span.add_simple_event("received response from database");

        if let Err(EpError::Connect(e)) = result {
            span.add_event("connection error, attempting to reconnect", vec![FastSpanAttribute::new("error", e.to_string())]);

            let mut lock = self.router.write().await;
            let ep = match lock.get_mut(&kind) {
                Some(route) => route,
                None => {
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned("could not get endpoint".to_string()) });
                    return Err(EpError::Connect(ConnectError::CouldNotGetEndpoint));
                }
            };

            span.add_simple_event("got mutable reference to endpoints");

            ep.reconnect_boxed(&endpoint_cache_key, endpoint_schema.config(), telemetry_wrapper).await?;

            span.add_simple_event("reconnected! sending read again");

            execute_with_timeout!(
                span,
                telemetry_wrapper,
                settings,
                ep,
                read_boxed(&endpoint_cache_key, request, settings, telemetry_wrapper)
            )
        } else {
            result
        }
    }

    #[named]
    async fn read_els_with_conn(
        &self,
        els_conn: Box<dyn EpConnection>,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());
        let kind = endpoint_schema.kind();

        let lock = self.router.read().await;
        let ep = match lock.get(&kind) {
            Some(route) => route,
            None => {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned("could not get endpoint".to_string()) });
                return Err(EpError::Connect(ConnectError::CouldNotGetEndpoint));
            }
        };

        span.add_simple_event("processing ELS read with override connection");
        let result = ep.read_with_conn_boxed(els_conn, endpoint_schema.config(), request, settings, telemetry_wrapper).await;

        drop(lock);
        result
    }

    async fn dispatch_read_endpoint_result(
        &self,
        dispatch: ReadDispatch,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        match dispatch {
            ReadDispatch::Pooled { organization_cache_uuid } => {
                self.read_with_reconnect(request, endpoint_schema, organization_cache_uuid, settings, telemetry_wrapper).await
            }
            ReadDispatch::Els { els_conn } => {
                self.read_els_with_conn(els_conn, request, endpoint_schema, settings, telemetry_wrapper).await
            }
        }
    }
}

impl MyEngineService {
    #[named]
    pub async fn read(
        &self,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());

        span.add_simple_event("processing read...");
        span.add_event(format!("{} read", endpoint_schema.kind()), vec![]);

        match endpoint_schema.kind() {
            #[cfg(feature = "mongo")]
            EpKind::Mongo => {
                self.read_with_mongo_analytics(request, endpoint_schema, organization_cache_uuid, settings, telemetry_wrapper).await
            }
            #[cfg(feature = "redis")]
            EpKind::Redis => {
                self.read_with_redis_analytics(request, endpoint_schema, organization_cache_uuid, settings, telemetry_wrapper).await
            }
            #[cfg(feature = "postgres")]
            EpKind::Postgres => {
                self.read_with_postgres_analytics(request, endpoint_schema, organization_cache_uuid, settings, telemetry_wrapper).await
            }
            _ => {
                self.dispatch_read_endpoint_result(
                    ReadDispatch::Pooled { organization_cache_uuid },
                    request,
                    endpoint_schema,
                    settings,
                    telemetry_wrapper,
                )
                .await
            }
        }
        .inspect_err(|e: &EpError| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))
    }

    #[named]
    pub async fn read_els(
        &self,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        els_conn: Option<Box<dyn EpConnection>>,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let els_conn = match els_conn {
            Some(conn) => conn,
            None => return self.read(request, endpoint_schema, organization_cache_uuid, settings, telemetry_wrapper).await,
        };

        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());
        span.add_event(format!("{} ELS read", endpoint_schema.kind()), vec![]);

        let result = self
            .dispatch_read_endpoint_result(ReadDispatch::Els { els_conn }, request, endpoint_schema, settings, telemetry_wrapper)
            .await;

        result.inspect_err(|e: &EpError| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))
    }

    #[cfg(feature = "mongo")]
    async fn read_with_mongo_analytics(
        &self,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let start = std::time::Instant::now();
        let facts = analytics::extract_mongo_request_facts(request);
        let endpoint_uuid = endpoint_schema.endpoint_uuid();
        let organization_uuid = organization_cache_uuid.to_string();

        let result = self
            .dispatch_read_endpoint_result(
                ReadDispatch::Pooled { organization_cache_uuid },
                request,
                endpoint_schema,
                settings,
                telemetry_wrapper,
            )
            .await;

        if let Some(facts) = facts {
            let latency_us = start.elapsed().as_micros() as u64;
            let response_facts = result.as_ref().ok().map(analytics::extract_response_facts);
            let response_bytes = result
                .as_ref()
                .ok()
                .and_then(|value| serde_json::to_vec(value).ok())
                .map(|bytes| analytics::usize_to_u32(bytes.len()))
                .unwrap_or(0);

            analytics::record_mongo_operation(
                &endpoint_uuid,
                &organization_uuid,
                &facts,
                response_facts.as_ref(),
                latency_us,
                result.is_err(),
                response_bytes,
                telemetry_wrapper.labels().user_uuid(),
            );
        }

        result
    }

    #[cfg(feature = "redis")]
    async fn read_with_redis_analytics(
        &self,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let start = std::time::Instant::now();
        let facts = analytics::extract_redis_request_facts(request);
        let endpoint_uuid = endpoint_schema.endpoint_uuid();
        let organization_uuid = organization_cache_uuid.to_string();

        let result = self
            .dispatch_read_endpoint_result(
                ReadDispatch::Pooled { organization_cache_uuid },
                request,
                endpoint_schema,
                settings,
                telemetry_wrapper,
            )
            .await;

        if let Some(facts) = facts {
            let latency_us = start.elapsed().as_micros() as u64;
            let response_bytes = result
                .as_ref()
                .ok()
                .and_then(|value| serde_json::to_vec(value).ok())
                .map(|bytes| analytics::usize_to_u32(bytes.len()))
                .unwrap_or(0);

            analytics::record_redis_operation(
                &endpoint_uuid,
                &organization_uuid,
                &facts,
                latency_us,
                result.is_err(),
                response_bytes,
                telemetry_wrapper.labels().user_uuid(),
            );
        }

        result
    }

    #[cfg(feature = "postgres")]
    async fn read_with_postgres_analytics(
        &self,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let start = std::time::Instant::now();
        let facts = analytics::extract_postgres_request_facts(request);
        let endpoint_uuid = endpoint_schema.endpoint_uuid();
        let organization_uuid = organization_cache_uuid.to_string();

        let result = self
            .dispatch_read_endpoint_result(
                ReadDispatch::Pooled { organization_cache_uuid },
                request,
                endpoint_schema,
                settings,
                telemetry_wrapper,
            )
            .await;

        if let Some(facts) = facts {
            let latency_us = start.elapsed().as_micros() as u64;
            let response_bytes = result
                .as_ref()
                .ok()
                .and_then(|value| serde_json::to_vec(value).ok())
                .map(|bytes| analytics::usize_to_u32(bytes.len()))
                .unwrap_or(0);

            analytics::record_postgres_operation(
                &endpoint_uuid,
                &organization_uuid,
                &facts,
                latency_us,
                result.is_err(),
                response_bytes,
                telemetry_wrapper.labels().user_uuid(),
            );
        }

        result
    }
}
