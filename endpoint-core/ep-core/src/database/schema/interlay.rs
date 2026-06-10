use super::Row;
use crate::database::schema::interlay_tls::{InterlayTls, deserialize_interlay_tls};
use crate::database::schema::routing::{EndpointRouting, EndpointRoutingInput};
use crate::database::schema::{FromRow, Table};
use crate::settings::EdenSettings;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use error::EpError;
use format::cache_uuid::{CacheUuid, EndpointCacheUuid};
use format::endpoint::EpKind;
use format::timestamp::DateTimeWrapper;
use format::{EdenId, EdenUuid, EndpointUuid, InterlayId, InterlayUuid, UserUuid};
use redis::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError, broadcast};
use tokio::task::AbortHandle;
use utoipa::ToSchema;

/// Precomputed mirror endpoint metadata used by protocol processors.
///
/// Mirroring is on the request hot path, so this keeps stable endpoint routing
/// data and the per-target limiter together instead of rebuilding labels and
/// looking up semaphores for every mirrored command.
#[derive(Debug, Clone)]
pub struct InterlayMirrorTarget {
    endpoint_cache_uuid: EndpointCacheUuid,
    endpoint_uuid_label: Arc<str>,
    limiter: Arc<Semaphore>,
}

impl InterlayMirrorTarget {
    pub fn endpoint_cache_uuid(&self) -> &EndpointCacheUuid {
        &self.endpoint_cache_uuid
    }

    pub fn endpoint_uuid_label(&self) -> &str {
        &self.endpoint_uuid_label
    }

    pub fn endpoint_uuid_label_arc(&self) -> Arc<str> {
        self.endpoint_uuid_label.clone()
    }

    pub fn try_acquire_owned(&self) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        self.limiter.clone().try_acquire_owned()
    }
}

/// Signals that can be sent to interlay processors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterlaySignal {
    /// Graceful shutdown - stop accepting new requests and close
    Shutdown,
    /// Mirror state changed - processors should refresh mirror targets
    MirrorUpdate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InterlayListener {
    id: String,
    bind_port: u16,
    advertise_port: u16,
}

impl InterlayListener {
    pub fn new(id: impl Into<String>, bind_port: u16, advertise_port: u16) -> Self {
        Self { id: id.into(), bind_port, advertise_port }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn bind_port(&self) -> u16 {
        self.bind_port
    }

    pub fn advertise_port(&self) -> u16 {
        self.advertise_port
    }
}

fn default_interlay_listener_id() -> String {
    "default".to_string()
}

fn legacy_listener_for_port(port: u16) -> Vec<InterlayListener> {
    vec![InterlayListener::new(default_interlay_listener_id(), port, port)]
}

/// The state of the interlay that is stored as part of the proxy channel shared across workers.
#[derive(Debug, Clone)]
pub struct InterlayState {
    endpoint_uuid: EndpointCacheUuid,
    endpoint_uuid_label: Arc<str>,
    endpoint_kind: EpKind,
    routing: EndpointRouting,
    listeners: Vec<InterlayListener>,
    advertise_host: Option<String>,
    command_policy: Option<Value>,
    audit_config: Option<Value>,
    mirror: InterlayMirrorSettings,
    mirror_targets: Arc<[InterlayMirrorTarget]>,
    /// Signal channel sender for shutdown and update notifications
    signal_tx: Option<broadcast::Sender<InterlaySignal>>,
    /// Monotonic revision for routing-affecting runtime state changes.
    state_version: Arc<AtomicU64>,
    /// Sticky shutdown flag so lagging receivers can still observe shutdown.
    shutdown_requested: Arc<AtomicBool>,
    /// Abort handle to check task status and force abort if needed
    abort_handles: Vec<AbortHandle>,
    /// Shared store of PARSE messages (keyed by statement name) across all proxy
    /// connections for this interlay. Used for replaying prepared statements on
    /// the target endpoint during and after endpoint swaps.
    prepared_stmt_store: Arc<DashMap<String, Bytes>>,
    /// Notified when the spawned interlay task exits (normal or aborted)
    shutdown_notify: Option<Arc<Notify>>,
}

impl InterlayState {
    pub fn new(
        endpoint_uuid: EndpointCacheUuid,
        endpoint_kind: EpKind,
        routing: EndpointRouting,
        command_policy: Option<Value>,
        audit_config: Option<Value>,
        mirror: InterlayMirrorSettings,
    ) -> Self {
        let endpoint_uuid_label = Self::build_endpoint_uuid_label(&endpoint_uuid);
        let mirror_targets = Self::build_mirror_targets(&endpoint_uuid, &mirror);
        Self {
            endpoint_uuid_label,
            endpoint_uuid,
            endpoint_kind,
            routing,
            listeners: Vec::new(),
            advertise_host: None,
            command_policy,
            audit_config,
            mirror_targets,
            mirror,
            signal_tx: None,
            state_version: Arc::new(AtomicU64::new(0)),
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            abort_handles: Vec::new(),
            prepared_stmt_store: Arc::new(DashMap::new()),
            shutdown_notify: None,
        }
    }

    pub fn endpoint_uuid(&self) -> &EndpointCacheUuid {
        &self.endpoint_uuid
    }
    pub fn endpoint_uuid_label(&self) -> &str {
        &self.endpoint_uuid_label
    }
    pub fn endpoint_uuid_label_arc(&self) -> Arc<str> {
        self.endpoint_uuid_label.clone()
    }
    pub fn endpoint_kind(&self) -> EpKind {
        self.endpoint_kind
    }
    pub fn routing(&self) -> &EndpointRouting {
        &self.routing
    }
    pub fn listeners(&self) -> &[InterlayListener] {
        &self.listeners
    }
    pub fn advertise_host(&self) -> Option<&str> {
        self.advertise_host.as_deref()
    }
    pub fn command_policy_value(&self) -> Option<&Value> {
        self.command_policy.as_ref()
    }
    pub fn audit_config_value(&self) -> Option<&Value> {
        self.audit_config.as_ref()
    }
    pub fn mirror(&self) -> &InterlayMirrorSettings {
        &self.mirror
    }
    pub fn mirror_targets(&self) -> &[InterlayMirrorTarget] {
        &self.mirror_targets
    }
    pub fn prepared_stmt_store(&self) -> &Arc<DashMap<String, Bytes>> {
        &self.prepared_stmt_store
    }

    pub fn version(&self) -> u64 {
        self.state_version.load(Ordering::Relaxed)
    }

    pub fn shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::Relaxed)
    }

    fn bump_version(&self) {
        self.state_version.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_endpoint(&mut self, endpoint_uuid: EndpointCacheUuid, endpoint_kind: EpKind) {
        self.endpoint_uuid_label = Self::build_endpoint_uuid_label(&endpoint_uuid);
        self.endpoint_uuid = endpoint_uuid;
        self.endpoint_kind = endpoint_kind;
        self.mirror_targets = Self::build_mirror_targets(&self.endpoint_uuid, &self.mirror);
        self.bump_version();
    }

    pub fn update_routing(&mut self, routing: EndpointRouting) {
        self.routing = routing;
        self.bump_version();
    }

    pub fn update_listener_config(&mut self, listeners: Vec<InterlayListener>, advertise_host: Option<String>) {
        self.listeners = listeners;
        self.advertise_host = advertise_host;
        self.bump_version();
    }

    pub fn apply_runtime_state(&mut self, endpoint_uuid: EndpointCacheUuid, endpoint_kind: EpKind, routing: EndpointRouting) {
        self.endpoint_uuid_label = Self::build_endpoint_uuid_label(&endpoint_uuid);
        self.endpoint_uuid = endpoint_uuid;
        self.endpoint_kind = endpoint_kind;
        self.routing = routing;
        self.mirror_targets = Self::build_mirror_targets(&self.endpoint_uuid, &self.mirror);
        self.bump_version();
    }

    pub fn update_command_policy(&mut self, command_policy: Option<Value>) {
        self.command_policy = command_policy;
        self.bump_version();
    }

    pub fn update_audit_config(&mut self, audit_config: Option<Value>) {
        self.audit_config = audit_config;
        self.bump_version();
    }

    pub fn update_mirror(&mut self, mirror: InterlayMirrorSettings) {
        self.mirror_targets = Self::build_mirror_targets(&self.endpoint_uuid, &mirror);
        self.mirror = mirror;
        self.bump_version();
        self.send_signal(InterlaySignal::MirrorUpdate);
    }

    fn build_endpoint_uuid_label(endpoint_uuid: &EndpointCacheUuid) -> Arc<str> {
        endpoint_uuid.uuid().to_string().into()
    }

    fn build_mirror_targets(endpoint_uuid: &EndpointCacheUuid, mirror: &InterlayMirrorSettings) -> Arc<[InterlayMirrorTarget]> {
        let org = endpoint_uuid.org();
        mirror
            .mirror_endpoint_uuids()
            .iter()
            .map(|endpoint_uuid| InterlayMirrorTarget {
                endpoint_cache_uuid: EndpointCacheUuid::new(org.clone(), endpoint_uuid.clone()),
                endpoint_uuid_label: endpoint_uuid.uuid().to_string().into(),
                limiter: Arc::new(Semaphore::new(mirror.max_in_flight_per_mirror())),
            })
            .collect::<Vec<_>>()
            .into()
    }

    /// Set the signal channel sender (used for shutdown updates)
    pub fn set_signal_tx(&mut self, tx: broadcast::Sender<InterlaySignal>) {
        self.signal_tx = Some(tx);
    }

    /// Get a reference to the signal channel sender
    pub fn signal_tx(&self) -> Option<&broadcast::Sender<InterlaySignal>> {
        self.signal_tx.as_ref()
    }

    /// Subscribe to interlay signals (shutdown updates)
    pub fn subscribe_signals(&self) -> Option<broadcast::Receiver<InterlaySignal>> {
        self.signal_tx.as_ref().map(|tx| tx.subscribe())
    }

    /// Send a signal to all subscribers
    fn send_signal(&self, signal: InterlaySignal) {
        if let Some(tx) = &self.signal_tx {
            // Ignore send errors (no subscribers or channel full)
            let _ = tx.send(signal);
        }
    }

    /// Set the abort handle for the interlay task
    pub fn set_abort_handle(&mut self, handle: AbortHandle) {
        self.abort_handles = vec![handle];
    }

    pub fn set_abort_handles(&mut self, handles: Vec<AbortHandle>) {
        self.abort_handles = handles;
    }

    /// Get a reference to the abort handle
    pub fn abort_handle(&self) -> Option<&AbortHandle> {
        self.abort_handles.first()
    }

    pub fn abort_handles(&self) -> &[AbortHandle] {
        &self.abort_handles
    }

    /// Check if the interlay task is currently running
    /// Returns true if the abort handle exists and the task is not finished
    pub fn is_running(&self) -> bool {
        self.abort_handles.iter().any(|handle| !handle.is_finished())
    }

    /// Gracefully shutdown the interlay by sending a shutdown signal
    /// Returns true if the signal was sent successfully
    pub fn shutdown(&self) -> bool {
        self.shutdown_requested.store(true, Ordering::Relaxed);
        if let Some(tx) = &self.signal_tx {
            tx.send(InterlaySignal::Shutdown).is_ok()
        } else {
            false
        }
    }

    /// Force abort the interlay task
    pub fn abort(&self) {
        for handle in &self.abort_handles {
            handle.abort();
        }
    }

    /// Set the shutdown notification handle
    pub fn set_shutdown_notify(&mut self, notify: Arc<Notify>) {
        self.shutdown_notify = Some(notify);
    }

    /// Get a reference to the shutdown notification handle
    pub fn shutdown_notify(&self) -> Option<&Arc<Notify>> {
        self.shutdown_notify.as_ref()
    }

    /// Wait for the interlay task to exit after a shutdown signal.
    /// Falls back to force-abort if the timeout expires.
    pub async fn wait_for_shutdown(&self, timeout: std::time::Duration) {
        if let Some(notify) = &self.shutdown_notify {
            if tokio::time::timeout(timeout, notify.notified()).await.is_ok() {
                return;
            }

            self.abort();
            let _ = tokio::time::timeout(timeout, notify.notified()).await;
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InterlaySettings {
    #[serde(flatten)]
    request: EdenSettings,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    command_policy: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    audit_config: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    policy_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sampling_config: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    alerts_config: Option<Value>,
    #[serde(default, skip_serializing_if = "InterlayMirrorSettings::is_disabled")]
    mirror: InterlayMirrorSettings,
}

impl InterlaySettings {
    pub fn request(&self) -> &EdenSettings {
        &self.request
    }

    pub fn command_policy_value(&self) -> Option<&Value> {
        self.command_policy.as_ref()
    }

    pub fn update_command_policy(&mut self, command_policy: Option<Value>) {
        self.command_policy = command_policy;
    }

    pub fn audit_config_value(&self) -> Option<&Value> {
        self.audit_config.as_ref()
    }

    pub fn update_audit_config(&mut self, audit_config: Option<Value>) {
        self.audit_config = audit_config;
    }

    pub fn policy_mode(&self) -> Option<&String> {
        self.policy_mode.as_ref()
    }

    pub fn update_policy_mode(&mut self, policy_mode: Option<String>) {
        self.policy_mode = policy_mode;
    }

    pub fn sampling_config_value(&self) -> Option<&Value> {
        self.sampling_config.as_ref()
    }

    pub fn update_sampling_config(&mut self, sampling_config: Option<Value>) {
        self.sampling_config = sampling_config;
    }

    pub fn alerts_config_value(&self) -> Option<&Value> {
        self.alerts_config.as_ref()
    }

    pub fn update_alerts_config(&mut self, alerts_config: Option<Value>) {
        self.alerts_config = alerts_config;
    }

    pub fn mirror(&self) -> &InterlayMirrorSettings {
        &self.mirror
    }

    pub fn update_mirror(&mut self, mirror: InterlayMirrorSettings) {
        self.mirror = mirror;
    }
}

impl From<EdenSettings> for InterlaySettings {
    fn from(request: EdenSettings) -> Self {
        Self {
            request,
            command_policy: None,
            audit_config: None,
            policy_mode: None,
            sampling_config: None,
            alerts_config: None,
            mirror: InterlayMirrorSettings::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum InterlayMirrorMode {
    #[default]
    Mirror,
}

fn default_mirror_reads() -> bool {
    true
}

fn default_mirror_writes() -> bool {
    true
}

fn default_mirror_sample_ratio() -> f64 {
    1.0
}

fn default_max_in_flight_per_mirror() -> usize {
    128
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InterlayMirrorSettings {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    mode: InterlayMirrorMode,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    mirror_endpoint_uuids: Vec<EndpointUuid>,
    #[serde(default = "default_mirror_reads")]
    mirror_reads: bool,
    #[serde(default = "default_mirror_writes")]
    mirror_writes: bool,
    #[serde(default = "default_mirror_sample_ratio")]
    sample_ratio: f64,
    #[serde(default = "default_max_in_flight_per_mirror")]
    max_in_flight_per_mirror: usize,
}

impl Default for InterlayMirrorSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: InterlayMirrorMode::Mirror,
            mirror_endpoint_uuids: Vec::new(),
            mirror_reads: true,
            mirror_writes: true,
            sample_ratio: 1.0,
            max_in_flight_per_mirror: default_max_in_flight_per_mirror(),
        }
    }
}

impl InterlayMirrorSettings {
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn mode(&self) -> InterlayMirrorMode {
        self.mode
    }

    pub fn mirror_endpoint_uuids(&self) -> &[EndpointUuid] {
        &self.mirror_endpoint_uuids
    }

    pub fn mirror_reads(&self) -> bool {
        self.mirror_reads
    }

    pub fn mirror_writes(&self) -> bool {
        self.mirror_writes
    }

    pub fn sample_ratio(&self) -> f64 {
        self.sample_ratio
    }

    pub fn max_in_flight_per_mirror(&self) -> usize {
        self.max_in_flight_per_mirror
    }

    pub fn is_disabled(&self) -> bool {
        !self.enabled
            && self.mode == InterlayMirrorMode::Mirror
            && self.mirror_endpoint_uuids.is_empty()
            && self.mirror_reads
            && self.mirror_writes
            && (self.sample_ratio - 1.0).abs() < f64::EPSILON
            && self.max_in_flight_per_mirror == default_max_in_flight_per_mirror()
    }
}

impl Deref for InterlaySettings {
    type Target = EdenSettings;

    fn deref(&self) -> &Self::Target {
        &self.request
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InterlaySchema {
    id: InterlayId,
    uuid: InterlayUuid,
    description: Option<String>,
    endpoint: EndpointUuid, // which endpoint this interlay routes to
    port: u16,              // port traffic is intercepted on
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    listeners: Vec<InterlayListener>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    advertise_host: Option<String>,
    #[serde(deserialize_with = "deserialize_interlay_tls")]
    tls: Option<InterlayTls>,
    settings: InterlaySettings, // request and policy settings
    //TODO handle API construction/reconstruction
    created_by: UserUuid,
    updated_by: UserUuid,
    created_at: DateTimeWrapper,
    updated_at: DateTimeWrapper,
}

impl InterlaySchema {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        description: Option<String>,
        endpoint: EndpointUuid,
        port: u16,
        tls: Option<InterlayTls>,
        settings: Option<InterlaySettings>,
        created_by: UserUuid,
    ) -> Self {
        let now = DateTimeWrapper::now();
        Self {
            id: InterlayId::new(id),
            uuid: InterlayUuid::new_uuid(),
            description,
            endpoint,
            port,
            listeners: legacy_listener_for_port(port),
            advertise_host: None,
            tls,
            settings: settings.unwrap_or_default(),
            updated_by: created_by.clone(),
            created_by,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_listeners(
        id: String,
        description: Option<String>,
        endpoint: EndpointUuid,
        listeners: Vec<InterlayListener>,
        advertise_host: Option<String>,
        tls: Option<InterlayTls>,
        settings: Option<InterlaySettings>,
        created_by: UserUuid,
    ) -> Self {
        let port = listeners.first().map(|listener| listener.bind_port()).unwrap_or_default();
        let mut schema = Self::new(id, description, endpoint, port, tls, settings, created_by);
        schema.listeners = listeners;
        schema.advertise_host = advertise_host;
        schema.port = schema.listeners.first().map(|listener| listener.bind_port()).unwrap_or(port);
        schema
    }

    pub fn created_by(&self) -> &UserUuid {
        &self.created_by
    }
    pub fn updated_by(&self) -> &UserUuid {
        &self.updated_by
    }
    pub fn set_updated_by(&mut self, updated_by: UserUuid) {
        self.updated_by = updated_by;
    }
    /// Returns the endpoint UUID this interlay routes to.
    pub fn endpoint(&self) -> &EndpointUuid {
        &self.endpoint
    }
    /// Sets the endpoint UUID this interlay routes to.
    pub fn set_endpoint(&mut self, endpoint: EndpointUuid) {
        self.endpoint = endpoint;
        self.update_timestamp();
    }
    pub fn set_port(&mut self, port: u16) {
        self.port = port;
        if let Some(first_listener) = self.listeners.first_mut() {
            first_listener.bind_port = port;
            first_listener.advertise_port = port;
        } else {
            self.listeners = legacy_listener_for_port(port);
        }
        self.update_timestamp();
    }
    pub fn set_listeners(&mut self, listeners: Vec<InterlayListener>) {
        self.port = listeners.first().map(|listener| listener.bind_port()).unwrap_or_default();
        self.listeners = listeners;
        self.update_timestamp();
    }
    pub fn set_advertise_host(&mut self, advertise_host: Option<String>) {
        self.advertise_host = advertise_host;
        self.update_timestamp();
    }
    pub fn set_description(&mut self, description: Option<String>) {
        self.description = description;
        self.update_timestamp();
    }
    pub fn set_tls(&mut self, tls: Option<InterlayTls>) {
        self.tls = tls;
        self.update_timestamp();
    }
    pub fn set_settings(&mut self, settings: InterlaySettings) {
        self.settings = settings;
        self.update_timestamp();
    }
    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn listeners(&self) -> &[InterlayListener] {
        &self.listeners
    }
    pub fn advertise_host(&self) -> Option<&String> {
        self.advertise_host.as_ref()
    }
    pub fn is_multi_listener(&self) -> bool {
        self.listeners.len() > 1
    }
    pub fn tls(&self) -> Option<&InterlayTls> {
        self.tls.as_ref()
    }
    pub fn settings(&self) -> &InterlaySettings {
        &self.settings
    }
    pub fn update_command_policy(&mut self, command_policy: Option<Value>) {
        self.settings.update_command_policy(command_policy);
        self.update_timestamp();
    }
    pub fn update_audit_config(&mut self, audit_config: Option<Value>) {
        self.settings.update_audit_config(audit_config);
        self.update_timestamp();
    }
    pub fn update_policy_mode(&mut self, policy_mode: Option<String>) {
        self.settings.update_policy_mode(policy_mode);
        self.update_timestamp();
    }
    pub fn update_sampling_config(&mut self, sampling_config: Option<Value>) {
        self.settings.update_sampling_config(sampling_config);
        self.update_timestamp();
    }
    pub fn update_alerts_config(&mut self, alerts_config: Option<Value>) {
        self.settings.update_alerts_config(alerts_config);
        self.update_timestamp();
    }
}

impl Table for InterlaySchema {
    type I = InterlayId;
    type U = InterlayUuid;

    fn id(&self) -> InterlayId {
        self.id.to_owned()
    }
    fn update_id(&mut self, id: String) -> Option<String> {
        let out = self.id.update(id);
        self.update_timestamp();
        Some(out)
    }
    fn uuid(&self) -> InterlayUuid {
        self.uuid.to_owned()
    }
    fn created_at(&self) -> DateTime<Utc> {
        self.created_at.as_datetime()
    }
    fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at.as_datetime()
    }
    fn update_timestamp(&mut self) {
        self.updated_at = DateTimeWrapper::now();
    }
    fn update_updated_by(&mut self, updated_by: UserUuid) {
        self.updated_by = updated_by;
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::schema::routing::EndpointRouting;
    use format::cache_uuid::{EndpointCacheUuid, OrganizationCacheUuid};
    use format::{CacheUuid, OrganizationUuid};

    fn mirror_settings(mirror_endpoint_uuids: Vec<EndpointUuid>, max_in_flight_per_mirror: usize) -> InterlayMirrorSettings {
        InterlayMirrorSettings {
            enabled: true,
            mode: InterlayMirrorMode::Mirror,
            mirror_endpoint_uuids,
            mirror_reads: true,
            mirror_writes: true,
            sample_ratio: 1.0,
            max_in_flight_per_mirror,
        }
    }

    #[test]
    fn mirror_targets_cache_labels_org_scope_and_limiters() {
        let org_one = OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid());
        let org_two = OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid());
        let primary_one_uuid = EndpointUuid::new_uuid();
        let primary_two_uuid = EndpointUuid::new_uuid();
        let primary_one = EndpointCacheUuid::new(Some(org_one.clone()), primary_one_uuid.clone());
        let primary_two = EndpointCacheUuid::new(Some(org_two.clone()), primary_two_uuid.clone());
        let mirror_one = EndpointUuid::new_uuid();
        let mirror_two = EndpointUuid::new_uuid();

        let mut state = InterlayState::new(
            primary_one,
            EpKind::Redis,
            EndpointRouting::default(),
            None,
            None,
            mirror_settings(vec![mirror_one.clone()], 1),
        );

        assert_eq!(state.endpoint_uuid_label(), primary_one_uuid.uuid().to_string());
        assert_eq!(state.mirror_targets().len(), 1);
        let target = &state.mirror_targets()[0];
        assert_eq!(target.endpoint_uuid_label(), mirror_one.uuid().to_string());
        assert_eq!(target.endpoint_cache_uuid(), &EndpointCacheUuid::new(Some(org_one.clone()), mirror_one));
        let held_permit = target.try_acquire_owned().expect("first mirror permit should be available");
        assert!(target.try_acquire_owned().is_err(), "cached mirror limiter should enforce max_in_flight_per_mirror");
        drop(held_permit);

        state.update_endpoint(primary_two, EpKind::Redis);
        state.update_mirror(mirror_settings(vec![mirror_two.clone()], 2));

        assert_eq!(state.endpoint_uuid_label(), primary_two_uuid.uuid().to_string());
        assert_eq!(state.mirror_targets().len(), 1);
        let target = &state.mirror_targets()[0];
        assert_eq!(target.endpoint_uuid_label(), mirror_two.uuid().to_string());
        assert_eq!(target.endpoint_cache_uuid(), &EndpointCacheUuid::new(Some(org_two), mirror_two));
        let first = target.try_acquire_owned().expect("first refreshed mirror permit should be available");
        let second = target.try_acquire_owned().expect("second refreshed mirror permit should be available");
        assert!(
            target.try_acquire_owned().is_err(),
            "refreshed mirror limiter should use updated max_in_flight_per_mirror"
        );
        drop((first, second));
    }
}

impl FromRow for InterlaySchema {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        Ok(Self {
            id: row.try_get("id").map_err(EpError::database)?,
            uuid: row.try_get("uuid").map_err(EpError::database)?,
            description: row.try_get("description").map_err(EpError::database)?,
            endpoint: row.try_get("endpoint").map_err(EpError::database)?,
            port: row.try_get::<_, i32>("port").map_err(EpError::database)? as u16,
            listeners: {
                let port = row.try_get::<_, i32>("port").map_err(EpError::database)? as u16;
                let listeners_value: Option<serde_json::Value> = row.try_get("listeners").map_err(EpError::database)?;
                match listeners_value {
                    Some(value) if !value.is_null() => {
                        let listeners: Vec<InterlayListener> = serde_json::from_value(value).map_err(EpError::serde)?;
                        if listeners.is_empty() {
                            legacy_listener_for_port(port)
                        } else {
                            listeners
                        }
                    }
                    _ => legacy_listener_for_port(port),
                }
            },
            advertise_host: row.try_get("advertise_host").map_err(EpError::database)?,
            tls: serde_json::from_value(row.try_get("tls").map_err(EpError::database)?).map_err(EpError::serde)?,
            settings: serde_json::from_value(row.try_get("settings").map_err(EpError::database)?).map_err(EpError::serde)?,
            created_by: row.try_get("created_by").map_err(EpError::database)?,
            updated_by: row.try_get("updated_by").map_err(EpError::database)?,
            created_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("created_at").map_err(EpError::database)?),
            updated_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("updated_at").map_err(EpError::database)?),
        })
    }
}

impl ToRedisArgs for InterlaySchema {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        // Serialize the AuthSchema to JSON
        let serialized = serde_json::to_vec(self).unwrap_or_default();

        // Write the serialized bytes to the Redis output
        out.write_arg(&serialized);
    }
}

impl FromRedisValue for InterlaySchema {
    fn from_redis_value(v: &redis::Value) -> Result<Self, redis::RedisError> {
        match v {
            // redis::Value::Data
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| redis::RedisError::from((redis::ErrorKind::ParseError, "Failed to deserialize JSON", e.to_string()))),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Invalid response type when expecting AuthSchema",
            ))),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InterlaySchemaIds {
    id: InterlayId,
    uuid: InterlayUuid,
    description: String,
    endpoint: EndpointUuid,
    created_at: DateTimeWrapper,
    updated_at: DateTimeWrapper,
}

impl From<InterlaySchema> for InterlaySchemaIds {
    fn from(schema: InterlaySchema) -> Self {
        Self {
            id: schema.id,
            uuid: schema.uuid,
            endpoint: schema.endpoint,
            description: schema.description.unwrap_or_default(),
            created_at: schema.created_at,
            updated_at: schema.updated_at,
        }
    }
}

impl FromRow for InterlaySchemaIds {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        Ok(Self {
            id: row.try_get("id").map_err(EpError::database)?,
            uuid: row.try_get("uuid").map_err(EpError::database)?,
            description: row.try_get::<&str, Option<String>>("description").map_err(EpError::database)?.unwrap_or_default(),
            endpoint: row.try_get("endpoint").map_err(EpError::database)?,
            created_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("created_at").map_err(EpError::database)?),
            updated_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("updated_at").map_err(EpError::database)?),
        })
    }
}

impl ToRedisArgs for InterlaySchemaIds {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        // Serialize the AuthSchema to JSON
        let serialized = serde_json::to_vec(self).unwrap_or_default();

        // Write the serialized bytes to the Redis output
        out.write_arg(&serialized);
    }
}

impl FromRedisValue for InterlaySchemaIds {
    fn from_redis_value(v: &redis::Value) -> Result<Self, redis::RedisError> {
        match v {
            // redis::Value::Data
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| redis::RedisError::from((redis::ErrorKind::ParseError, "Failed to deserialize JSON", e.to_string()))),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Invalid response type when expecting AuthSchema",
            ))),
        }
    }
}

/// Input for partially updating an existing interlay via PATCH.
/// All fields are optional — only provided fields are updated.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateInterlaySchema {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<EndpointUuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settings: Option<InterlaySettings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<Option<InterlayTls>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InterlayBuilder {
    id: String,
    /// Legacy single-endpoint field (used for Direct routing).
    /// If `routing` is also provided, `routing` takes precedence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,
    /// Full routing configuration. Overrides `endpoint` when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    routing: Option<EndpointRoutingInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    port: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    settings: Option<InterlaySettings>,
}

impl InterlayBuilder {
    pub fn new(id: String, endpoint: String, description: Option<String>, port: u16, settings: Option<InterlaySettings>) -> Self {
        Self {
            id,
            endpoint: Some(endpoint),
            routing: None,
            description,
            port,
            settings,
        }
    }

    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }
    pub fn id_ref(&self) -> &String {
        &self.id
    }
    /// Sets the legacy endpoint field (creates Direct routing).
    pub fn endpoint(mut self, id: impl Into<String>) -> Self {
        self.endpoint = Some(id.into());
        self
    }
    /// Returns the legacy endpoint string, if set.
    pub fn endpoint_ref(&self) -> Option<&String> {
        self.endpoint.as_ref()
    }
    pub fn routing_input(mut self, routing: EndpointRoutingInput) -> Self {
        self.routing = Some(routing);
        self
    }
    pub fn routing_input_ref(&self) -> Option<&EndpointRoutingInput> {
        self.routing.as_ref()
    }
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
    pub fn description_ref(&self) -> Option<&String> {
        self.description.as_ref()
    }
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
    pub fn port_ref(&self) -> u16 {
        self.port
    }
    pub fn settings(mut self, settings: impl Into<InterlaySettings>) -> Self {
        self.settings = Some(settings.into());
        self
    }
    pub fn settings_ref(&self) -> Option<&InterlaySettings> {
        self.settings.as_ref()
    }
}
