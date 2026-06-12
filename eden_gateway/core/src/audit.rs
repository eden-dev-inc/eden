//! Shared audit recording traits for proxy processors.
//!
//! `BlockedCommandRecorder` is database-agnostic (all string parameters) and
//! shared between Redis and PostgreSQL proxies.

use eden_core::format::EndpointUuid;
use std::sync::OnceLock;

/// Trait for recording blocked command events from the proxy.
pub trait BlockedCommandRecorder: Send + Sync + 'static {
    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    fn record(
        &self,
        tenant: &str,
        endpoint_uuid: &EndpointUuid,
        command: &str,
        reason: &str,
        severity: u8,
        service: &str,
        client_ip: Option<&str>,
    );
}

static BLOCKED_RECORDER: OnceLock<Box<dyn BlockedCommandRecorder>> = OnceLock::new();

/// Initialize the global blocked command recorder. Should be called once at startup.
pub fn init_blocked_recorder(recorder: Box<dyn BlockedCommandRecorder>) {
    let _ = BLOCKED_RECORDER.set(recorder);
}

pub fn blocked_record(
    tenant: &str,
    endpoint_uuid: &EndpointUuid,
    command: &str,
    reason: &str,
    severity: u8,
    service: &str,
    client_ip: Option<&str>,
) {
    if let Some(r) = BLOCKED_RECORDER.get() {
        r.record(tenant, endpoint_uuid, command, reason, severity, service, client_ip);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// PostgreSQL Per-Query Audit Recording
// ──────────────────────────────────────────────────────────────────────────────

/// Trait for recording per-query audit trail entries from the PostgreSQL proxy.
///
/// All parameters are database-agnostic strings so the trait can live in `core`.
/// The eden-service provides an adapter that persists entries to ClickHouse.
pub trait PgQueryRecorder: Send + Sync + 'static {
    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    fn record(
        &self,
        tenant: &str,
        endpoint_uuid: &EndpointUuid,
        sql_type: &str,
        latency_us: u64,
        success: bool,
        service: &str,
        client_ip: Option<&str>,
        connection_id: u64,
    );
}

static PG_QUERY_RECORDER: OnceLock<Box<dyn PgQueryRecorder>> = OnceLock::new();

/// Initialize the global PostgreSQL query recorder. Should be called once at startup.
pub fn init_pg_query_recorder(recorder: Box<dyn PgQueryRecorder>) {
    let _ = PG_QUERY_RECORDER.set(recorder);
}

pub fn pg_query_recorder_enabled() -> bool {
    PG_QUERY_RECORDER.get().is_some()
}

/// Record a PostgreSQL query execution for audit trail.
// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub fn pg_query_record(
    tenant: &str,
    endpoint_uuid: &EndpointUuid,
    sql_type: &str,
    latency_us: u64,
    success: bool,
    service: &str,
    client_ip: Option<&str>,
    connection_id: u64,
) {
    if let Some(r) = PG_QUERY_RECORDER.get() {
        r.record(tenant, endpoint_uuid, sql_type, latency_us, success, service, client_ip, connection_id);
    }
}
