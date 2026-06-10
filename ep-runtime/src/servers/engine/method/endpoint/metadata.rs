use std::borrow::Cow;

use database::endpoint_schema::endpoint::EndpointSchema;
use database::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use eden_core::error::{ConnectError, EpError};
use eden_core::format::OrganizationCacheUuid;
use eden_core::macros::execute_with_timeout;
use eden_core::telemetry::{FastSpanAttribute, FastSpanStatus, TelemetryWrapper};
use endpoint::metadata::{MetadataCollectorInfo, SyncFrequency};
use ep_core::settings::EdenSettings;
use function_name::named;
use serde_json::Value;
use tokio::time::Duration;

use crate::comp::MyEngineService;

impl MyEngineService {
    #[allow(clippy::too_many_arguments)]
    #[named]
    async fn metadata_with_reconnect(
        &self,
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        endpoint_schema: &EndpointSchema,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        packages: Option<Vec<String>>,
        frequency: Option<SyncFrequency>,
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

        span.add_simple_event("processing async metadata");

        let result = execute_with_timeout!(
            span,
            telemetry_wrapper,
            settings,
            ep,
            metadata_boxed(db_manager, &endpoint_cache_key, settings, packages.as_deref(), frequency, telemetry_wrapper)
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

            span.add_simple_event("reconnected! sending metadata again");

            execute_with_timeout!(
                span,
                telemetry_wrapper,
                settings,
                ep,
                metadata_boxed(db_manager, &endpoint_cache_key, settings, packages.as_deref(), frequency, telemetry_wrapper)
            )
        } else {
            result
        }
    }
}

impl MyEngineService {
    #[allow(clippy::too_many_arguments)]
    #[named]
    pub async fn metadata(
        &self,
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        endpoint_schema: &EndpointSchema,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        packages: Option<Vec<String>>,
        frequency: Option<SyncFrequency>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());

        span.add_simple_event("processing metadata...");
        span.add_event(format!("{} metadata", endpoint_schema.kind()), vec![]);

        self.metadata_with_reconnect(
            db_manager,
            endpoint_schema,
            organization_cache_uuid,
            settings,
            packages,
            frequency,
            telemetry_wrapper,
        )
        .await
        .inspect_err(|e: &EpError| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))
    }

    pub async fn metadata_collectors(&self, endpoint_schema: &EndpointSchema) -> Result<Vec<MetadataCollectorInfo>, EpError> {
        let lock = self.router.read().await;
        match lock.get(&endpoint_schema.kind()) {
            Some(route) => Ok(route.collector_info_boxed()),
            None => Err(EpError::Connect(ConnectError::CouldNotGetEndpoint)),
        }
    }
}
