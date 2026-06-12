use std::borrow::Cow;

use bytes::Bytes;
use database::endpoint_schema::endpoint::EndpointSchema;
use eden_core::error::{ConnectError, EpError};
use eden_core::format::OrganizationCacheUuid;
use eden_core::macros::execute_with_timeout;
use eden_core::telemetry::{FastSpanAttribute, FastSpanStatus, TelemetryWrapper};
use endpoint::EpRequest;
use ep_core::settings::EdenSettings;
use function_name::named;
use tokio::time::Duration;

use crate::comp::MyEngineService;

impl MyEngineService {
    #[named]
    async fn read_bytes_with_reconnect(
        &self,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Bytes, EpError> {
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

        span.add_simple_event("processing async read bytes");

        let result = execute_with_timeout!(
            span,
            telemetry_wrapper,
            settings,
            ep,
            read_bytes_boxed(&endpoint_cache_key, request, settings, telemetry_wrapper)
        );

        drop(lock);
        span.add_simple_event("dropped lock");

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

            ep.reconnect_boxed(&endpoint_cache_key, endpoint_schema.config(), telemetry_wrapper).await?;

            span.add_simple_event("reconnected! sending read bytes again");

            execute_with_timeout!(
                span,
                telemetry_wrapper,
                settings,
                ep,
                read_bytes_boxed(&endpoint_cache_key, request, settings, telemetry_wrapper)
            )
        } else {
            result
        }
    }
}

impl MyEngineService {
    #[named]
    pub async fn read_bytes(
        &self,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Bytes, EpError> {
        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());

        span.add_simple_event("processing read bytes...");
        span.add_event(format!("{} read bytes", endpoint_schema.kind()), vec![]);

        self.read_bytes_with_reconnect(request, endpoint_schema, organization_cache_uuid, settings, telemetry_wrapper)
            .await
            .inspect_err(|e: &EpError| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))
    }
}
