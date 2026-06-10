//! Simplified engine execution helpers for CDC template rendering.
//!
//! These are stripped-down versions of `read_endpoint`/`write_endpoint` from
//! the template run handler, designed for use in spawned CDC worker tasks
//! where there is no HTTP request context.
//!
//! Differences from the template API path:
//! - No RBAC check (already verified when the pipeline/snapshot was started)
//! - No LLM hydration or semantic bridge (CDC doesn't target LLM endpoints)
//! - Uses `EdenSettings::default()` instead of parsing from request headers
use crate::EdenDb;

use database::db::cache::CacheFunctions;
use database::template::EndpointRequestTemplate;
use eden_core::error::EpError;
use eden_core::format::OrganizationUuid;
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::{CacheObjectType, EndpointId, EndpointUuid};
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::settings::EdenSettings;
use endpoint_schema::endpoint::EndpointSchema;
use endpoints::endpoint::EpRequest;
use ep_runtime::comp::MyEngineService;
use serde_json::Value;

/// Execute a rendered read template against an endpoint via the engine service.
pub async fn execute_template_read(
    engine_service: &MyEngineService,
    db: &EdenDb,
    request: EndpointRequestTemplate,
    org_uuid: &OrganizationUuid,
    telemetry: &mut TelemetryWrapper,
) -> Result<Value, EpError> {
    let org_cache_uuid = OrganizationCacheUuid::new(None, org_uuid.to_owned());

    let endpoint_cache_uuid = EndpointCacheUuid::new(Some(org_cache_uuid.clone()), request.get_endpoint_uuid().to_owned());
    let endpoint_cache_object = CacheObjectType::new(Some(endpoint_cache_uuid), None);

    let endpoint_schema: EndpointSchema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            db,
            &endpoint_cache_object,
            telemetry,
        )
        .await?;

    let request_input = request.get_request().clone();
    let mut ep_request: Box<dyn EpRequest> = TryInto::<Box<dyn EpRequest>>::try_into((request_input, endpoint_schema.kind()))?;

    engine_service.read(&mut *ep_request, &endpoint_schema, org_cache_uuid, EdenSettings::default(), telemetry).await
}

/// Execute a rendered write template against an endpoint via the engine service.
pub async fn execute_template_write(
    engine_service: &MyEngineService,
    db: &EdenDb,
    request: EndpointRequestTemplate,
    org_uuid: &OrganizationUuid,
    telemetry: &mut TelemetryWrapper,
) -> Result<Value, EpError> {
    let org_cache_uuid = OrganizationCacheUuid::new(None, org_uuid.to_owned());

    let endpoint_cache_uuid = EndpointCacheUuid::new(Some(org_cache_uuid.clone()), request.get_endpoint_uuid().to_owned());
    let endpoint_cache_object = CacheObjectType::new(Some(endpoint_cache_uuid), None);

    let endpoint_schema: EndpointSchema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            db,
            &endpoint_cache_object,
            telemetry,
        )
        .await?;

    let request_input = request.get_request().clone();
    let mut ep_request: Box<dyn EpRequest> = TryInto::<Box<dyn EpRequest>>::try_into((request_input, endpoint_schema.kind()))?;

    engine_service.write(&mut *ep_request, &endpoint_schema, org_cache_uuid, EdenSettings::default(), telemetry).await
}
