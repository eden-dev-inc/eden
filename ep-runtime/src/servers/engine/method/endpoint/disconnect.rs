#[allow(unused_imports)] // CacheUuid used in feature-gated branches
use eden_core::format::{CacheUuid, EndpointUuid, OrganizationCacheUuid};
use eden_core::proto::proto::EndpointDisconnect;
use eden_core::telemetry::FastSpanAttribute;

use crate::comp::MyEngineService;
use borsh::{BorshDeserialize, BorshSerialize};
use eden_core::error::{ConnectError, EpError};
use eden_core::format::cache_uuid::EndpointCacheUuid;
use eden_core::format::endpoint::EpKind;
use eden_core::telemetry::TelemetryWrapper;
use function_name::named;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize)]
pub struct DisconnectInfo {
    endpoint_uuid: EndpointUuid,
    kind: EpKind,
}

impl TryFrom<EndpointDisconnect> for DisconnectInfo {
    type Error = EpError;

    fn try_from(conn: EndpointDisconnect) -> Result<Self, Self::Error> {
        borsh::from_slice(&conn.disconnect_info).map_err(EpError::serde)
    }
}

impl DisconnectInfo {
    pub fn new(endpoint_uuid: EndpointUuid, kind: EpKind) -> Self {
        Self { endpoint_uuid, kind }
    }

    pub fn endpoint_uuid(&self) -> &EndpointUuid {
        &self.endpoint_uuid
    }

    pub fn kind(&self) -> &EpKind {
        &self.kind
    }
}

impl MyEngineService {
    #[named]
    pub async fn disconnect(
        &self,
        disconnect_info: &DisconnectInfo,
        organization_cache_uuid: OrganizationCacheUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<String, EpError> {
        let mut span = telemetry_wrapper.client_tracer(format!("endpoint.{}", function_name!()));

        span.add_simple_event("disconnecting...");

        let endpoint_cache_uuid = EndpointCacheUuid::new(Some(organization_cache_uuid), disconnect_info.endpoint_uuid().clone());
        let mut lock = self.router.write().await;
        let ep = lock.get_mut(disconnect_info.kind()).ok_or(EpError::Connect(ConnectError::CouldNotGetEndpoint))?;

        match ep.disconnect_boxed(&endpoint_cache_uuid, telemetry_wrapper).await {
            Ok(()) => {
                span.add_simple_event("disconnected");
                Ok("disconnected".to_string())
            }
            Err(e) => {
                span.add_event("failed to disconnect", vec![FastSpanAttribute::new("error", e.to_string())]);
                Err(e)
            }
        }
    }
}
