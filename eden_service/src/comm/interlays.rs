use crate::EdenDb;
pub mod analysis_timeseries;
pub mod delete;
pub mod get;
pub mod patch;
pub mod post;
pub(crate) mod runtime_cleanup;
pub mod shard;
pub mod start;
pub mod stop;

use chrono::{DateTime, Utc};
use database::cache::CacheFunctions;
use eden_core::error::ResultEP;
use eden_core::format::cache_id::{EndpointCacheId, InterlayCacheId};
use eden_core::format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid, OrganizationCacheUuid};
use eden_core::format::endpoint::EpKind;
use eden_core::format::{CacheObjectType, CacheUuid, EndpointId, EndpointUuid, InterlayId, InterlayUuid};
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::interlay::{InterlayListener, InterlaySchema, InterlaySettings, InterlayState};
use endpoint_schema::endpoint::EndpointSchema;
use ep_runtime::comp::MyEngineService;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::OnceLock, time::Duration};
use utoipa::ToSchema;

const DEFAULT_INTERLAY_SHUTDOWN_TIMEOUT_MS: u64 = 5_000;
const ENV_INTERLAY_SHUTDOWN_TIMEOUT_MS: &str = "EDEN_INTERLAY_SHUTDOWN_TIMEOUT_MS";

static INTERLAY_SHUTDOWN_TIMEOUT: OnceLock<Duration> = OnceLock::new();

pub(crate) fn interlay_shutdown_timeout() -> Duration {
    *INTERLAY_SHUTDOWN_TIMEOUT.get_or_init(|| {
        Duration::from_millis(
            std::env::var(ENV_INTERLAY_SHUTDOWN_TIMEOUT_MS)
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .unwrap_or(DEFAULT_INTERLAY_SHUTDOWN_TIMEOUT_MS)
                .max(1),
        )
    })
}

/// Validates that a port is acceptable for interlay use.
/// Rejects privileged ports.
pub(crate) fn validate_port(port: u16) -> Result<(), String> {
    if port < 1024 {
        return Err(format!("Port {port} is privileged and cannot be used for an interlay"));
    }
    Ok(())
}

pub(crate) fn normalize_interlay_listeners(port: Option<u16>, listeners: Vec<InterlayListener>) -> Result<Vec<InterlayListener>, String> {
    if listeners.is_empty() {
        let port = port.ok_or_else(|| "either `port` or `listeners` must be provided".to_string())?;
        validate_port(port)?;
        return Ok(vec![InterlayListener::new("default", port, port)]);
    }

    if let Some(port) = port {
        let matches_legacy_single_listener =
            listeners.len() == 1 && listeners[0].bind_port() == port && listeners[0].advertise_port() == port;
        if !matches_legacy_single_listener {
            return Err("`port` cannot be combined with a distinct `listeners` topology".to_string());
        }
    }

    let mut listener_ids = std::collections::HashSet::new();
    let mut bind_ports = std::collections::HashSet::new();
    let mut advertise_ports = std::collections::HashSet::new();

    for listener in &listeners {
        validate_port(listener.bind_port())?;
        validate_port(listener.advertise_port())?;
        if !listener_ids.insert(listener.id().to_string()) {
            return Err(format!("duplicate interlay listener id '{}'", listener.id()));
        }
        if !bind_ports.insert(listener.bind_port()) {
            return Err(format!("duplicate interlay bind port {}", listener.bind_port()));
        }
        if !advertise_ports.insert(listener.advertise_port()) {
            return Err(format!("duplicate interlay advertise port {}", listener.advertise_port()));
        }
    }

    Ok(listeners)
}

pub(crate) fn interlay_conflicting_bind_port(existing: &InterlaySchema, candidate_ports: &[u16]) -> Option<u16> {
    candidate_ports
        .iter()
        .copied()
        .find(|port| existing.port() == *port || existing.listeners().iter().any(|listener| listener.bind_port() == *port))
}

pub(crate) fn validate_multi_listener_interlay_shape(
    endpoint_kind: EpKind,
    listeners: &[InterlayListener],
    advertise_host: Option<&str>,
    settings: &InterlaySettings,
) -> Result<(), String> {
    if listeners.len() <= 1 {
        return Ok(());
    }

    if endpoint_kind != EpKind::Redis {
        return Err("multi-listener interlays are currently supported only for Redis endpoints".to_string());
    }

    if advertise_host.unwrap_or_default().trim().is_empty() {
        return Err("`advertise_host` is required for multi-listener Redis interlays".to_string());
    }

    if settings.command_policy_value().is_some() {
        return Err("multi-listener Redis interlays do not currently support `command_policy`".to_string());
    }

    if settings.audit_config_value().is_some() {
        return Err("multi-listener Redis interlays do not currently support `audit_config`".to_string());
    }

    Ok(())
}

pub(crate) async fn validate_interlay_mirror_settings(
    database_manager: &EdenDb,
    org_key: &OrganizationCacheUuid,
    primary_endpoint_schema: &EndpointSchema,
    interlay_schema: &InterlaySchema,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> Result<(), String> {
    let mirror = interlay_schema.settings().mirror();
    if !mirror.enabled() {
        return Ok(());
    }

    if mirror.mode() != endpoint_core::ep_core::database::schema::interlay::InterlayMirrorMode::Mirror {
        return Err("only mirror mode is supported for interlay mirror settings".to_string());
    }

    if mirror.mirror_endpoint_uuids().is_empty() {
        return Err("mirror mode requires at least one mirror endpoint".to_string());
    }

    if !primary_endpoint_schema.routing().is_direct() {
        return Err("mirror mode currently requires a direct primary endpoint routing configuration".to_string());
    }

    if !(0.0..=1.0).contains(&mirror.sample_ratio()) || !mirror.sample_ratio().is_finite() {
        return Err("mirror sample_ratio must be finite and between 0.0 and 1.0".to_string());
    }

    if mirror.max_in_flight_per_mirror() == 0 {
        return Err("mirror max_in_flight_per_mirror must be greater than zero".to_string());
    }

    let primary_endpoint_uuid = primary_endpoint_schema.endpoint_uuid();
    let mut seen = std::collections::HashSet::new();
    for mirror_endpoint_uuid in mirror.mirror_endpoint_uuids() {
        if mirror_endpoint_uuid == &primary_endpoint_uuid {
            return Err("mirror endpoints cannot include the interlay primary endpoint".to_string());
        }
        if !seen.insert(mirror_endpoint_uuid.clone()) {
            return Err(format!("duplicate mirror endpoint '{}'", mirror_endpoint_uuid));
        }

        let mirror_endpoint_schema =
            <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
                database_manager,
                &CacheObjectType::new(Some(EndpointCacheUuid::new(Some(org_key.clone()), mirror_endpoint_uuid.clone())), None),
                telemetry_wrapper,
            )
            .await
            .map_err(|err| format!("failed to resolve mirror endpoint '{}': {err}", mirror_endpoint_uuid))?;

        if mirror_endpoint_schema.kind() != primary_endpoint_schema.kind() {
            return Err(format!(
                "mirror endpoint '{}' has kind {:?}; expected {:?}",
                mirror_endpoint_uuid,
                mirror_endpoint_schema.kind(),
                primary_endpoint_schema.kind()
            ));
        }

        if !mirror_endpoint_schema.routing().is_direct() {
            return Err(format!(
                "mirror endpoint '{}' must use a direct endpoint routing configuration",
                mirror_endpoint_uuid
            ));
        }
    }

    Ok(())
}

pub(crate) async fn reconnect_interlay_runtime_endpoints(
    engine_service: &MyEngineService,
    database_manager: &EdenDb,
    org_key: &OrganizationCacheUuid,
    primary_endpoint_schema: &EndpointSchema,
    interlay_schema: &InterlaySchema,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<()> {
    let mut endpoint_uuids = Vec::new();
    let mut seen = HashSet::new();

    for endpoint_uuid in primary_endpoint_schema.routing().all_endpoints() {
        if seen.insert(endpoint_uuid.clone()) {
            endpoint_uuids.push(endpoint_uuid.clone());
        }
    }

    if interlay_schema.settings().mirror().enabled() {
        for endpoint_uuid in interlay_schema.settings().mirror().mirror_endpoint_uuids() {
            if seen.insert(endpoint_uuid.clone()) {
                endpoint_uuids.push(endpoint_uuid.clone());
            }
        }
    }

    let organization_uuid = org_key.eden_uuid();
    for endpoint_uuid in endpoint_uuids {
        let endpoint_schema =
            <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
                database_manager,
                &CacheObjectType::new(Some(EndpointCacheUuid::new(Some(org_key.clone()), endpoint_uuid)), None),
                telemetry_wrapper,
            )
            .await?;

        engine_service.reconnect(database_manager, &endpoint_schema, &organization_uuid, telemetry_wrapper).await?;
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InterlayResponse {
    id: InterlayId,
    uuid: InterlayUuid,
    endpoint: EndpointUuid,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    listeners: Vec<InterlayListener>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    advertise_host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    settings: InterlaySettings,
    running: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl InterlayResponse {
    pub fn new(
        id: InterlayId,
        uuid: InterlayUuid,
        endpoint: EndpointUuid,
        listeners: Vec<InterlayListener>,
        advertise_host: Option<String>,
        port: Option<u16>,
        settings: InterlaySettings,
        running: bool,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            uuid,
            endpoint,
            listeners,
            advertise_host,
            port,
            settings,
            running,
            created_at,
            updated_at,
        }
    }

    pub fn with_running(mut self, running: bool) -> Self {
        self.running = running;
        self
    }
}

impl From<InterlaySchema> for InterlayResponse {
    fn from(schema: InterlaySchema) -> Self {
        Self::new(
            schema.id(),
            schema.uuid(),
            schema.endpoint().clone(),
            schema.listeners().to_vec(),
            schema.advertise_host().cloned(),
            (schema.listeners().len() == 1).then_some(schema.port()),
            schema.settings().clone(),
            false,
            schema.created_at(),
            schema.updated_at(),
        )
    }
}

pub(crate) async fn get_interlay_schema(
    database_manager: &EdenDb,
    interlay_cache_object: &CacheObjectType<InterlayCacheUuid, InterlayCacheId>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<InterlaySchema> {
    <EdenDb as CacheFunctions<InterlaySchema, InterlayCacheUuid, InterlayUuid, InterlayCacheId, InterlayId>>::get_from_cache(
        database_manager,
        interlay_cache_object,
        telemetry_wrapper,
    )
    .await
}

pub(crate) async fn shutdown_running_interlay(state: &InterlayState) -> bool {
    if !state.is_running() {
        return false;
    }

    state.shutdown();
    state.wait_for_shutdown(interlay_shutdown_timeout()).await;
    true
}

#[cfg(test)]
mod tests {
    use super::{normalize_interlay_listeners, validate_multi_listener_interlay_shape, validate_port};
    use eden_core::format::endpoint::EpKind;
    use endpoint_core::ep_core::database::schema::interlay::{InterlayListener, InterlaySettings};

    #[test]
    fn test_validate_port_rejects_privileged_ports() {
        assert_eq!(validate_port(80), Err("Port 80 is privileged and cannot be used for an interlay".to_string()));
    }

    #[test]
    fn test_validate_port_allows_non_privileged_ports() {
        assert_eq!(validate_port(6200), Ok(()));
    }

    #[test]
    fn normalize_interlay_listeners_uses_legacy_port_shorthand() {
        let listeners = normalize_interlay_listeners(Some(6200), Vec::new()).expect("legacy single-port interlay");
        assert_eq!(listeners, vec![InterlayListener::new("default", 6200, 6200)]);
    }

    #[test]
    fn normalize_interlay_listeners_rejects_duplicate_ports() {
        let listeners = vec![InterlayListener::new("a", 6200, 16200), InterlayListener::new("b", 6200, 16201)];
        assert_eq!(normalize_interlay_listeners(None, listeners), Err("duplicate interlay bind port 6200".to_string()));
    }

    #[test]
    fn normalize_interlay_listeners_rejects_privileged_advertise_ports() {
        let listeners = vec![InterlayListener::new("a", 6200, 80)];
        assert_eq!(
            normalize_interlay_listeners(None, listeners),
            Err("Port 80 is privileged and cannot be used for an interlay".to_string())
        );
    }

    #[test]
    fn validate_multi_listener_interlay_shape_requires_redis() {
        let listeners = vec![InterlayListener::new("a", 6200, 16200), InterlayListener::new("b", 6201, 16201)];
        assert_eq!(
            validate_multi_listener_interlay_shape(EpKind::Postgres, &listeners, Some("proxy.example.com"), &InterlaySettings::default()),
            Err("multi-listener interlays are currently supported only for Redis endpoints".to_string())
        );
    }

    #[test]
    fn validate_multi_listener_interlay_shape_requires_advertise_host() {
        let listeners = vec![InterlayListener::new("a", 6200, 16200), InterlayListener::new("b", 6201, 16201)];
        assert_eq!(
            validate_multi_listener_interlay_shape(EpKind::Redis, &listeners, None, &InterlaySettings::default()),
            Err("`advertise_host` is required for multi-listener Redis interlays".to_string())
        );
    }

    #[test]
    fn validate_multi_listener_interlay_shape_rejects_policy_and_audit() {
        let listeners = vec![InterlayListener::new("a", 6200, 16200), InterlayListener::new("b", 6201, 16201)];
        let mut settings = InterlaySettings::default();
        settings.update_command_policy(Some(serde_json::json!({ "mode": "block" })));

        assert_eq!(
            validate_multi_listener_interlay_shape(EpKind::Redis, &listeners, Some("proxy.example.com"), &settings),
            Err("multi-listener Redis interlays do not currently support `command_policy`".to_string())
        );

        let mut settings = InterlaySettings::default();
        settings.update_audit_config(Some(serde_json::json!({ "enabled": true })));

        assert_eq!(
            validate_multi_listener_interlay_shape(EpKind::Redis, &listeners, Some("proxy.example.com"), &settings),
            Err("multi-listener Redis interlays do not currently support `audit_config`".to_string())
        );
    }
}
