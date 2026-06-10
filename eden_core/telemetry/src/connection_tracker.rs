//! Connection tracking via RAII guards.
//!
//! Endpoint pools embed a [`ConnectionGuard`] in each client instance. The guard
//! increments the `eden.connections` gauge on creation and decrements on drop,
//! so "active connections" reflects real pool state regardless of how a
//! connection is released (normal return, recycle error, pool eviction, etc.).
//!
//! It also updates the process-wide [`ConnectionState`] registry — a stable
//! in-memory counter that is NOT subject to the fast-telemetry eviction sweep.
//! Long-lived connections stay counted correctly, and the ingestion loop
//! snapshots this registry into `analytics.connection_metrics` every tick.
//!
//! Pool code calls [`global_metrics`] to get the current `AllMetrics` handle.
//! The handle is installed once at service startup via [`set_global_metrics`].
//! Before startup (or in test contexts without telemetry), guards are inert.

use crate::AllMetrics;
use crate::labels::SYSTEM_ORG_UUID;
use dashmap::DashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use tokio_util::sync::CancellationToken;

static GLOBAL_METRICS: OnceLock<Weak<AllMetrics>> = OnceLock::new();

/// Install the process-wide metrics handle. Call this once at service startup,
/// immediately after constructing the `Arc<AllMetrics>`.
pub fn set_global_metrics(metrics: &Arc<AllMetrics>) {
    let _ = GLOBAL_METRICS.set(Arc::downgrade(metrics));
}

/// Retrieve the process-wide metrics handle, if set and still alive.
pub fn global_metrics() -> Option<Arc<AllMetrics>> {
    GLOBAL_METRICS.get().and_then(Weak::upgrade)
}

// ── Stable connection state registry ─────────────────────────────────────────
//
// The fast-telemetry gauges above are subject to an eviction sweep that removes
// dynamic series when they haven't been modified in N cycles. For long-lived
// connections that sit idle between "open" and "close", this causes the +1 to
// be evicted and the gauge to read 0 even when the connection is open.
//
// `CONNECTION_STATE` is a parallel, process-wide registry of the same counters
// that is never evicted. The analytics ingestion loop snapshots these numbers
// into `analytics.connection_metrics` so the dashboard (and any other ClickHouse
// consumer) sees a correct, persistent record.

/// Per-endpoint connection counter: key = `(db_type, endpoint_uuid)`.
/// `endpoint_uuid` may be empty when the caller doesn't know it.
type EndpointKey = (&'static str, String);

fn add_counter_delta<K>(map: &DashMap<K, AtomicI64>, key: K, delta: i64)
where
    K: Clone + Eq + Hash,
{
    let count = {
        let counter = map.entry(key.clone()).or_insert_with(|| AtomicI64::new(0));
        counter.fetch_add(delta, Ordering::Relaxed) + delta
    };

    if count <= 0 {
        remove_non_positive_counter(map, &key);
    }
}

fn decrement_counter<K>(map: &DashMap<K, AtomicI64>, key: K)
where
    K: Eq + Hash,
{
    let count = match map.get(&key) {
        Some(counter) => counter.fetch_sub(1, Ordering::Relaxed) - 1,
        None => return,
    };

    if count <= 0 {
        remove_non_positive_counter(map, &key);
    }
}

fn remove_non_positive_counter<K>(map: &DashMap<K, AtomicI64>, key: &K)
where
    K: Eq + Hash,
{
    map.remove_if(key, |_, count| count.load(Ordering::Relaxed) <= 0);
}

/// Process-wide connection registry. Stable (no eviction).
pub struct ConnectionState {
    /// Open endpoint connections (idle + in-use). Keyed by (db_type, endpoint_uuid).
    endpoint_open: DashMap<EndpointKey, AtomicI64>,
    /// Endpoint connections currently checked out of a pool (active).
    endpoint_in_use: DashMap<EndpointKey, AtomicI64>,
    /// Active proxy client sessions. Keyed by interlay_id.
    proxy_active: DashMap<String, AtomicI64>,
    /// Active proxy sessions keyed by (client_ip, interlay_id) so we can show a
    /// real client-side breakdown (e.g. "10.0.1.5 has 12 sessions on interlay X").
    proxy_clients: DashMap<(String, String), AtomicI64>,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            endpoint_open: DashMap::new(),
            endpoint_in_use: DashMap::new(),
            proxy_active: DashMap::new(),
            proxy_clients: DashMap::new(),
        }
    }

    pub fn add_endpoint_open(&self, db_type: &'static str, endpoint_uuid: Option<&str>) {
        let key = (db_type, normalize_endpoint_uuid(endpoint_uuid));
        add_counter_delta(&self.endpoint_open, key, 1);
    }

    pub fn remove_endpoint_open(&self, db_type: &'static str, endpoint_uuid: Option<&str>) {
        let key = (db_type, normalize_endpoint_uuid(endpoint_uuid));
        decrement_counter(&self.endpoint_open, key);
    }

    pub fn add_endpoint_in_use(&self, db_type: &'static str, endpoint_uuid: Option<&str>, delta: i64) {
        let key = (db_type, normalize_endpoint_uuid(endpoint_uuid));
        add_counter_delta(&self.endpoint_in_use, key, delta);
    }

    pub fn add_proxy(&self, interlay_id: &str) {
        let key = normalize_interlay_id(interlay_id);
        add_counter_delta(&self.proxy_active, key, 1);
    }

    pub fn remove_proxy(&self, interlay_id: &str) {
        let key = normalize_interlay_id(interlay_id);
        decrement_counter(&self.proxy_active, key);
    }

    /// Record a proxy session from a specific client IP. The pair
    /// `(client_ip, interlay_id)` lets us break activity down per-client while
    /// still knowing which endpoint (via interlay) they connect to.
    pub fn add_proxy_client(&self, client_ip: &str, interlay_id: &str) {
        let key = (client_ip.to_string(), normalize_interlay_id(interlay_id));
        add_counter_delta(&self.proxy_clients, key, 1);
    }

    pub fn remove_proxy_client(&self, client_ip: &str, interlay_id: &str) {
        let key = (client_ip.to_string(), normalize_interlay_id(interlay_id));
        decrement_counter(&self.proxy_clients, key);
    }

    /// Snapshot endpoint open counts as `Vec<(db_type, endpoint_uuid, count)>`.
    pub fn snapshot_endpoint_open(&self) -> Vec<(&'static str, String, i64)> {
        self.endpoint_open
            .iter()
            .map(|entry| {
                let (db, uuid) = entry.key();
                (*db, uuid.clone(), entry.value().load(Ordering::Relaxed))
            })
            .filter(|(_, _, count)| *count > 0)
            .collect()
    }

    pub fn snapshot_endpoint_in_use(&self) -> Vec<(&'static str, String, i64)> {
        self.endpoint_in_use
            .iter()
            .map(|entry| {
                let (db, uuid) = entry.key();
                (*db, uuid.clone(), entry.value().load(Ordering::Relaxed))
            })
            .filter(|(_, _, count)| *count > 0)
            .collect()
    }

    pub fn snapshot_proxy(&self) -> Vec<(String, i64)> {
        self.proxy_active
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
            .filter(|(_, count)| *count > 0)
            .collect()
    }

    /// Snapshot proxy sessions grouped by client as `(client_ip, interlay_id, count)`.
    pub fn snapshot_proxy_clients(&self) -> Vec<(String, String, i64)> {
        self.proxy_clients
            .iter()
            .map(|entry| {
                let (ip, interlay) = entry.key();
                (ip.clone(), interlay.clone(), entry.value().load(Ordering::Relaxed))
            })
            .filter(|(_, _, count)| *count > 0)
            .collect()
    }
}

/// Strip a leading `"<kind>:"` prefix if present. `InterlayUuid::to_string()`
/// and `EndpointUuid::to_string()` return `"interlay:<uuid>"` /
/// `"endpoint:<uuid>"`, while other call-sites pass the raw inner UUID. Normalize
/// to the raw form so the same identity doesn't end up in two buckets.
fn strip_kind_prefix(raw: &str) -> String {
    if let Some(idx) = raw.find(':') {
        let (_prefix, rest) = raw.split_at(idx + 1);
        if !rest.is_empty() {
            return rest.to_string();
        }
    }
    raw.to_string()
}

fn normalize_interlay_id(raw: &str) -> String {
    strip_kind_prefix(raw)
}

fn normalize_endpoint_uuid(raw: Option<&str>) -> String {
    match raw {
        None | Some("") => String::new(),
        Some(s) => strip_kind_prefix(s),
    }
}

static CONNECTION_STATE: OnceLock<ConnectionState> = OnceLock::new();

fn state() -> &'static ConnectionState {
    CONNECTION_STATE.get_or_init(ConnectionState::new)
}

/// Get the global connection state registry. Safe to call before or after
/// `set_global_metrics`; initialized on first access.
pub fn connection_state() -> &'static ConnectionState {
    state()
}

/// Tracks one active connection to a backend.
///
/// Increments `eden.connections` with the configured labels on creation, and
/// decrements with the identical labels on drop. Embed in the client struct so
/// the count tracks real lifetimes (drop fires on detach, recycle error, pool
/// eviction, or process exit).
pub struct ConnectionGuard {
    db_type: &'static str,
    org_uuid: String,
    endpoint_uuid: Option<String>,
    metrics: Option<Arc<AllMetrics>>,
}

impl ConnectionGuard {
    /// Create a guard without an endpoint UUID (legacy callers).
    pub fn new(db_type: &'static str) -> Self {
        Self::new_with_endpoint(db_type, SYSTEM_ORG_UUID, None)
    }

    /// Create a guard tagged with the owning endpoint's UUID so the gauge is
    /// broken down per endpoint in addition to per db_type.
    pub fn new_with_endpoint(db_type: &'static str, org_uuid: impl Into<String>, endpoint_uuid: Option<String>) -> Self {
        let org_uuid = org_uuid.into();
        let metrics = global_metrics();
        if let Some(ref m) = metrics {
            let labels = build_labels(db_type, &org_uuid, endpoint_uuid.as_deref());
            m.eden().add_connection(&labels);
        }
        // Also update the stable state registry (not subject to fast-telemetry eviction).
        state().add_endpoint_open(db_type, endpoint_uuid.as_deref());
        Self { db_type, org_uuid, endpoint_uuid, metrics }
    }
}

fn build_labels<'a>(db_type: &'a str, org_uuid: &'a str, endpoint_uuid: Option<&'a str>) -> Vec<(&'a str, &'a str)> {
    let mut labels: Vec<(&str, &str)> = vec![("org_uuid", org_uuid), ("db_type", db_type)];
    if let Some(uuid) = endpoint_uuid {
        labels.push(("endpoint_uuid", uuid));
    }
    labels
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if let Some(ref m) = self.metrics {
            let labels = build_labels(self.db_type, &self.org_uuid, self.endpoint_uuid.as_deref());
            m.eden().remove_connection(&labels);
        }
        state().remove_endpoint_open(self.db_type, self.endpoint_uuid.as_deref());
    }
}

impl std::fmt::Debug for ConnectionGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionGuard").field("db_type", &self.db_type).finish()
    }
}

/// Owned cancellation handle for a background pool status poller.
///
/// The poller keeps sampling while at least one handle clone is alive. When the
/// final handle is dropped, the task is cancelled and any positive in-use count
/// previously reported by the poller is subtracted.
#[derive(Clone)]
pub struct PoolStatusPollerHandle {
    inner: Arc<PoolStatusPollerInner>,
}

struct PoolStatusPollerInner {
    db_type: &'static str,
    org_uuid: String,
    endpoint_uuid: Option<String>,
    state: Arc<Mutex<PoolStatusPollerState>>,
    cancellation: CancellationToken,
}

#[derive(Default)]
struct PoolStatusPollerState {
    last_in_use: i64,
    closed: bool,
}

impl PoolStatusPollerInner {
    fn close(&self) {
        close_poller(self.db_type, &self.org_uuid, self.endpoint_uuid.as_deref(), &self.state);
    }
}

impl Drop for PoolStatusPollerInner {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.close();
    }
}

impl std::fmt::Debug for PoolStatusPollerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolStatusPollerHandle").field("db_type", &self.inner.db_type).finish()
    }
}

fn adjust_connections_in_use(db_type: &'static str, org_uuid: &str, endpoint_uuid: Option<&str>, delta: i64) {
    if delta == 0 {
        return;
    }

    if let Some(m) = global_metrics() {
        let labels = build_labels(db_type, org_uuid, endpoint_uuid);
        if delta > 0 {
            for _ in 0..delta {
                m.eden().add_connection_in_use(&labels);
            }
        } else {
            for _ in 0..(-delta) {
                m.eden().remove_connection_in_use(&labels);
            }
        }
    }

    state().add_endpoint_in_use(db_type, endpoint_uuid, delta);
}

fn lock_poller_state(state: &Mutex<PoolStatusPollerState>) -> MutexGuard<'_, PoolStatusPollerState> {
    match state.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn close_poller(db_type: &'static str, org_uuid: &str, endpoint_uuid: Option<&str>, state: &Mutex<PoolStatusPollerState>) {
    let mut state = lock_poller_state(state);
    if state.closed {
        return;
    }

    state.closed = true;
    let previous = state.last_in_use;
    state.last_in_use = 0;
    if previous > 0 {
        adjust_connections_in_use(db_type, org_uuid, endpoint_uuid, -previous);
    }
}

fn record_current_in_use(
    db_type: &'static str,
    org_uuid: &str,
    endpoint_uuid: Option<&str>,
    state: &Mutex<PoolStatusPollerState>,
    current: i64,
) -> bool {
    let mut state = lock_poller_state(state);
    if state.closed {
        return false;
    }

    let delta = current - state.last_in_use;
    if delta != 0 {
        adjust_connections_in_use(db_type, org_uuid, endpoint_uuid, delta);
        state.last_in_use = current;
    }
    true
}

/// Spawn a background task that periodically reads pool status and updates
/// the `eden.connections_in_use` gauge. This avoids any hot-path overhead
/// on `pool.get()` by sampling instead of instrumenting every checkout.
///
/// `poll_status` should return `(size, available)` where `size - available`
/// is the number of connections currently checked out (in use).
pub fn spawn_pool_status_poller<F>(
    db_type: &'static str,
    org_uuid: impl Into<String>,
    endpoint_uuid: Option<String>,
    interval: std::time::Duration,
    poll_status: F,
) -> PoolStatusPollerHandle
where
    F: Fn() -> Option<(usize, usize)> + Send + Sync + 'static,
{
    let org_uuid = org_uuid.into();
    let cancellation = CancellationToken::new();
    let state = Arc::new(Mutex::new(PoolStatusPollerState::default()));
    let handle = PoolStatusPollerHandle {
        inner: Arc::new(PoolStatusPollerInner {
            db_type,
            org_uuid: org_uuid.clone(),
            endpoint_uuid: endpoint_uuid.clone(),
            state: state.clone(),
            cancellation: cancellation.clone(),
        }),
    };

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // Skip the immediate tick so the first measurement reflects real state.
        tokio::select! {
            _ = ticker.tick() => {}
            _ = cancellation.cancelled() => {
                close_poller(db_type, org_uuid.as_str(), endpoint_uuid.as_deref(), &state);
                return;
            }
        }
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = cancellation.cancelled() => {
                    close_poller(db_type, org_uuid.as_str(), endpoint_uuid.as_deref(), &state);
                    return;
                }
            }
            let Some((size, available)) = poll_status() else {
                // Pool gone; stop polling.
                close_poller(db_type, org_uuid.as_str(), endpoint_uuid.as_deref(), &state);
                return;
            };
            let current = (size.saturating_sub(available)) as i64;
            if !record_current_in_use(db_type, org_uuid.as_str(), endpoint_uuid.as_deref(), &state, current) {
                return;
            }
        }
    });

    handle
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use tokio::time::{Duration, Instant};

    static TEST_ENDPOINT_ID: AtomicUsize = AtomicUsize::new(0);

    fn test_endpoint() -> String {
        let id = TEST_ENDPOINT_ID.fetch_add(1, Ordering::Relaxed);
        format!("poller-test-{id}")
    }

    fn endpoint_in_use_count(endpoint_uuid: &str) -> Option<i64> {
        state()
            .snapshot_endpoint_in_use()
            .into_iter()
            .find(|(db_type, uuid, _)| *db_type == "test" && uuid == endpoint_uuid)
            .map(|(_, _, count)| count)
    }

    async fn wait_for_endpoint_in_use(endpoint_uuid: &str, expected: Option<i64>) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if endpoint_in_use_count(endpoint_uuid) == expected {
                return;
            }

            assert!(Instant::now() < deadline, "timed out waiting for endpoint in-use count {expected:?}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn counter_value<K>(map: &DashMap<K, AtomicI64>, key: &K) -> Option<i64>
    where
        K: Eq + Hash,
    {
        map.get(key).map(|counter| counter.load(Ordering::Relaxed))
    }

    #[test]
    fn endpoint_open_entries_are_removed_at_zero_and_extra_removes_are_ignored() {
        let state = ConnectionState::new();
        let endpoint_uuid = test_endpoint();
        let key = ("test", endpoint_uuid.clone());

        state.add_endpoint_open("test", Some(&endpoint_uuid));
        assert_eq!(counter_value(&state.endpoint_open, &key), Some(1));

        state.remove_endpoint_open("test", Some(&endpoint_uuid));
        assert_eq!(counter_value(&state.endpoint_open, &key), None);
        assert!(state.snapshot_endpoint_open().is_empty());

        state.remove_endpoint_open("test", Some(&endpoint_uuid));
        assert_eq!(counter_value(&state.endpoint_open, &key), None);
    }

    #[test]
    fn endpoint_in_use_entries_are_removed_at_zero_and_negative_deltas_are_cleaned_up() {
        let state = ConnectionState::new();
        let endpoint_uuid = test_endpoint();
        let key = ("test", endpoint_uuid.clone());

        state.add_endpoint_in_use("test", Some(&endpoint_uuid), 2);
        assert_eq!(counter_value(&state.endpoint_in_use, &key), Some(2));

        state.add_endpoint_in_use("test", Some(&endpoint_uuid), -2);
        assert_eq!(counter_value(&state.endpoint_in_use, &key), None);
        assert!(state.snapshot_endpoint_in_use().is_empty());

        state.add_endpoint_in_use("test", Some(&endpoint_uuid), -1);
        assert_eq!(counter_value(&state.endpoint_in_use, &key), None);
    }

    #[test]
    fn proxy_entries_are_removed_at_zero_and_extra_removes_are_ignored() {
        let state = ConnectionState::new();
        let interlay_id = format!("interlay:{}", test_endpoint());
        let key = normalize_interlay_id(&interlay_id);

        state.add_proxy(&interlay_id);
        assert_eq!(counter_value(&state.proxy_active, &key), Some(1));

        state.remove_proxy(&interlay_id);
        assert_eq!(counter_value(&state.proxy_active, &key), None);
        assert!(state.snapshot_proxy().is_empty());

        state.remove_proxy(&interlay_id);
        assert_eq!(counter_value(&state.proxy_active, &key), None);
    }

    #[test]
    fn proxy_client_entries_are_removed_at_zero_and_extra_removes_are_ignored() {
        let state = ConnectionState::new();
        let id = test_endpoint();
        let client_ip = format!("client-{id}");
        let interlay_id = format!("interlay:{id}");
        let key = (client_ip.clone(), normalize_interlay_id(&interlay_id));

        state.add_proxy_client(&client_ip, &interlay_id);
        assert_eq!(counter_value(&state.proxy_clients, &key), Some(1));

        state.remove_proxy_client(&client_ip, &interlay_id);
        assert_eq!(counter_value(&state.proxy_clients, &key), None);
        assert!(state.snapshot_proxy_clients().is_empty());

        state.remove_proxy_client(&client_ip, &interlay_id);
        assert_eq!(counter_value(&state.proxy_clients, &key), None);
    }

    #[test]
    fn endpoint_in_use_entries_are_removed_at_zero() {
        let endpoint_uuid = test_endpoint();

        state().add_endpoint_in_use("test", Some(&endpoint_uuid), 2);
        assert_eq!(endpoint_in_use_count(&endpoint_uuid), Some(2));

        state().add_endpoint_in_use("test", Some(&endpoint_uuid), -2);
        assert_eq!(endpoint_in_use_count(&endpoint_uuid), None);
    }

    #[test]
    fn endpoint_in_use_underflow_does_not_leave_snapshot_count() {
        let endpoint_uuid = test_endpoint();

        state().add_endpoint_in_use("test", Some(&endpoint_uuid), -1);

        assert_eq!(endpoint_in_use_count(&endpoint_uuid), None);
    }

    #[tokio::test]
    async fn dropping_pool_status_poller_subtracts_last_positive_count() {
        let endpoint_uuid = test_endpoint();
        let in_use = Arc::new(AtomicUsize::new(3));
        let poll_count = Arc::new(AtomicUsize::new(0));
        let poll_in_use = in_use.clone();
        let poll_count_for_closure = poll_count.clone();

        let handle = spawn_pool_status_poller("test", SYSTEM_ORG_UUID, Some(endpoint_uuid.clone()), Duration::from_millis(10), move || {
            poll_count_for_closure.fetch_add(1, Ordering::Relaxed);
            Some((poll_in_use.load(Ordering::Relaxed), 0))
        });

        wait_for_endpoint_in_use(&endpoint_uuid, Some(3)).await;
        drop(handle);
        wait_for_endpoint_in_use(&endpoint_uuid, None).await;

        tokio::time::sleep(Duration::from_millis(30)).await;
        let count_after_drop = poll_count.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(poll_count.load(Ordering::Relaxed), count_after_drop);
    }

    #[tokio::test]
    async fn pool_status_poller_none_status_subtracts_last_positive_count() {
        let endpoint_uuid = test_endpoint();
        let active = Arc::new(AtomicBool::new(true));
        let poll_active = active.clone();

        let _handle =
            spawn_pool_status_poller("test", SYSTEM_ORG_UUID, Some(endpoint_uuid.clone()), Duration::from_millis(10), move || {
                poll_active.load(Ordering::Relaxed).then_some((2, 0))
            });

        wait_for_endpoint_in_use(&endpoint_uuid, Some(2)).await;
        active.store(false, Ordering::Relaxed);
        wait_for_endpoint_in_use(&endpoint_uuid, None).await;
    }
}
