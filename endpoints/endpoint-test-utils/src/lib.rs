#![allow(clippy::unwrap_used)]

pub const DEFAULT_REDIS_VERSION: &str = "7.2.4";
pub const DEFAULT_REDIS_STACK_VERSION: &str = "7.4.0-v8";

#[cfg_attr(embedded_db, path = "database_manager_test_utils_embedded_db.rs")]
pub mod database_manager_test_utils;

pub mod database_test_utils {
    use endpoint_core::ep_core::ep::{EpConfig, RWPool};
    use endpoint_types::{EP, EpRequest, RequestConstructor, RunRequest};

    use endpoint_core::ep_core::settings::EdenSettings;
    use endpoint_types::metadata::{EpMetadata, SyncMetadata};
    use endpoint_types::{ApiExample, EndpointType, Operation};
    use format::cache_uuid::EndpointCacheUuid;
    use serde::Serialize;
    use telemetry::TelemetryWrapper;
    use utoipa::ToSchema;

    pub async fn generic_write<
        A: Clone + Sync + Send + 'static,
        K: 'static,
        X: 'static,
        C: RWPool<A> + EpConfig + Clone + ToSchema + 'static,
        M: EpMetadata + SyncMetadata<A> + Clone + Serialize + 'static,
        Req: EpRequest + RequestConstructor<AsyncType = A, ApiKindType = K, TxType = X> + EndpointType + RunRequest<A, K, X> + 'static,
        E: EP<A, C, Req, M, K, X>,
        T: Operation<A, K, X> + Clone + 'static,
    >(
        example: ApiExample<T>,
        endpoint_cache_uuid: &EndpointCacheUuid,
        ep: E,
        test_telemetry: &mut TelemetryWrapper,
        _sync: bool,
    ) -> T
    where
        Box<<Req as RequestConstructor>::OperationType>: From<T>,
    {
        println!("Test: {}", example.name);

        let request = Box::new(Req::new(From::from(example.request.clone()))) as Box<dyn EpRequest>;

        let output = ep.write(endpoint_cache_uuid, &*request, EdenSettings::default(), test_telemetry).await.expect("Failed to write");

        match example.response {
            Ok(response) => {
                if let Some(response) = response {
                    assert_eq!(
                        serde_json::from_str::<serde_json::Value>(&response.to_string()).expect("failed to deserialize"),
                        output
                    );
                }
            }
            Err(e) => {
                if let Some(e) = e {
                    assert_eq!(serde_json::from_str::<serde_json::Value>(&e.to_string()).expect("failed to deserialize"), output)
                }
            }
        }

        example.request.clone()
    }

    pub async fn generic_read<
        A: Clone + Sync + Send + 'static,
        K: 'static,
        X: 'static,
        C: RWPool<A> + EpConfig + Clone + ToSchema + 'static,
        M: EpMetadata + SyncMetadata<A> + Clone + Serialize + 'static,
        Req: EpRequest + EndpointType + RunRequest<A, K, X> + 'static,
        E: EP<A, C, Req, M, K, X>,
    >(
        request: &mut dyn EpRequest,
        endpoint_cache_uuid: &EndpointCacheUuid,
        ep: E,
        test_telemetry: &mut TelemetryWrapper,
        sync: bool,
    ) -> serde_json::Value {
        match sync {
            true => ep.write(endpoint_cache_uuid, request, EdenSettings::default(), test_telemetry).await.expect("Failed to write"),
            false => ep.read(endpoint_cache_uuid, request, EdenSettings::default(), test_telemetry).await.expect("Failed to read"),
        }
    }
}

pub mod telemetry_test_utils {
    use format::EdenNodeUuid;
    use std::sync::Arc;
    use telemetry::labels::TelemetryLabels;
    use telemetry::{TelemetryDurations, TelemetryWrapper, setup_metrics};

    pub fn test_telemetry() -> TelemetryWrapper {
        TelemetryWrapper::new(
            Arc::new(setup_metrics("http://localhost:4317", "").expect("Failed to setup metrics")),
            TelemetryLabels::new(&EdenNodeUuid::new_uuid()),
            TelemetryDurations::default(),
        )
    }
}
