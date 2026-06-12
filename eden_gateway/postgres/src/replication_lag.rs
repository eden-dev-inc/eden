//! Replication lag-aware read routing for WAL mode (DW-14).
//!
//! When `CanaryWriteMode::WalReplication` is active, all writes go to the old
//! (source) database and PostgreSQL logical replication streams them to the new
//! (target). Reads directed to the new database may see stale data if
//! replication hasn't caught up.
//!
//! This module provides a cached replication lag check that read routing can
//! consult. When the lag exceeds a configurable threshold, reads are redirected
//! to the old (source) database.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use eden_core::format::cache_uuid::EndpointCacheUuid;
use eden_logger_internal::{LogAudience, LogContext, log_trace, log_warn};
use endpoints::endpoint::postgres::ep::PostgresEp;
use ep_core::ReqType;

/// Default threshold in bytes below which the subscriber is considered "caught up".
const DEFAULT_LAG_THRESHOLD_BYTES: i64 = 1024;

/// Default interval for polling replication lag.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Cached replication lag state.
///
/// A background poller updates the cached lag value. Read routing checks
/// [`is_caught_up`] to decide whether to route to the new database.
pub struct ReplicationLagCache {
    /// Cached lag in bytes (negative means unknown/error).
    cached_lag_bytes: Arc<AtomicI64>,
    /// Threshold below which the subscriber is considered caught up.
    threshold_bytes: i64,
    /// Poll interval.
    poll_interval: Duration,
    /// Number of times the lag exceeded the threshold.
    lag_exceeded_count: Arc<AtomicU64>,
}

impl ReplicationLagCache {
    /// Create a new lag cache with default settings.
    pub fn new() -> Self {
        Self {
            cached_lag_bytes: Arc::new(AtomicI64::new(-1)), // unknown initially
            threshold_bytes: DEFAULT_LAG_THRESHOLD_BYTES,
            poll_interval: DEFAULT_POLL_INTERVAL,
            lag_exceeded_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Whether the subscriber is caught up (lag below threshold).
    ///
    /// Returns `false` if the lag is unknown (initial state or query errors).
    pub fn is_caught_up(&self) -> bool {
        let lag = self.cached_lag_bytes.load(Ordering::Relaxed);
        lag >= 0 && lag <= self.threshold_bytes
    }

    /// Current cached lag in bytes, or -1 if unknown.
    pub fn lag_bytes(&self) -> i64 {
        self.cached_lag_bytes.load(Ordering::Relaxed)
    }

    /// Number of times the lag exceeded the threshold during polling.
    pub fn lag_exceeded_count(&self) -> u64 {
        self.lag_exceeded_count.load(Ordering::Relaxed)
    }

    /// Spawn a background poller using `PostgresEp` for raw wire connections.
    ///
    /// Unlike [`start_poller`], this variant does not require `MyEngineService`
    /// and is suitable for use from the proxy processor. Queries the source
    /// database's `pg_replication_slots` view via `simple_query_raw`.
    pub fn start_poller_with_ep(
        self: &Arc<Self>,
        ep: PostgresEp,
        source_endpoint: EndpointCacheUuid,
        slot_name: String,
        ctx: LogContext,
    ) -> tokio::task::JoinHandle<()> {
        let cache = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(cache.poll_interval).await;

                let conn_result = ep.raw_connection(&source_endpoint, ReqType::Read).await;
                let mut conn = match conn_result {
                    Ok(c) => c,
                    Err(_) => {
                        cache.cached_lag_bytes.store(-1, Ordering::Relaxed);
                        continue;
                    }
                };

                let query = format!(
                    "SELECT pg_current_wal_lsn() - confirmed_flush_lsn AS lag_bytes \
                     FROM pg_replication_slots WHERE slot_name = '{}'",
                    slot_name.replace('\'', "''")
                );

                match conn.simple_query_raw(&query).await {
                    Ok(raw) => {
                        match parse_first_data_row_value(&raw) {
                            Some(lag) => {
                                let prev = cache.cached_lag_bytes.swap(lag, Ordering::Relaxed);
                                if lag > cache.threshold_bytes {
                                    cache.lag_exceeded_count.fetch_add(1, Ordering::Relaxed);
                                    if prev <= cache.threshold_bytes {
                                        log_warn!(
                                            ctx.clone(),
                                            "DW-14: replication lag exceeded threshold",
                                            audience = LogAudience::Internal,
                                            lag_bytes = lag,
                                            threshold = cache.threshold_bytes
                                        );
                                    }
                                }
                            }
                            None => {
                                // No DataRow — slot not found or empty result.
                                cache.cached_lag_bytes.store(-1, Ordering::Relaxed);
                                log_trace!(
                                    ctx.clone(),
                                    "DW-14: replication slot not found",
                                    audience = LogAudience::Internal,
                                    slot = slot_name.as_str()
                                );
                            }
                        }
                    }
                    Err(_) => {
                        cache.cached_lag_bytes.store(-1, Ordering::Relaxed);
                    }
                }
            }
        })
    }
}

/// Extract the first column's text value from a simple query raw wire response
/// and parse it as i64.
///
/// Scans for message type `'D'` (DataRow), reads `num_columns(i16)`, then
/// `value_len(i32)` + `value_bytes` for the first column, and parses as i64.
/// Returns `None` if no DataRow is found, the column is NULL, or parsing fails.
fn parse_first_data_row_value(response: &[u8]) -> Option<i64> {
    let mut i = 0;
    while i + 5 <= response.len() {
        let msg_type = response[i];
        let msg_len = i32::from_be_bytes([response[i + 1], response[i + 2], response[i + 3], response[i + 4]]) as usize;
        let msg_end = i + 1 + msg_len;

        if msg_type == b'D' && msg_end <= response.len() {
            // DataRow: after type(1) + len(4), num_columns(i16)
            let payload_start = i + 5;
            if payload_start + 2 > msg_end {
                return None;
            }
            let num_cols = i16::from_be_bytes([response[payload_start], response[payload_start + 1]]);
            if num_cols < 1 {
                return None;
            }
            // First column: value_len(i32)
            let val_len_start = payload_start + 2;
            if val_len_start + 4 > msg_end {
                return None;
            }
            let val_len = i32::from_be_bytes([
                response[val_len_start],
                response[val_len_start + 1],
                response[val_len_start + 2],
                response[val_len_start + 3],
            ]);
            if val_len < 0 {
                return None; // NULL value
            }
            let val_start = val_len_start + 4;
            let val_end = val_start + val_len as usize;
            if val_end > msg_end {
                return None;
            }
            let text = std::str::from_utf8(&response[val_start..val_end]).ok()?;
            return text.trim().parse::<i64>().ok();
        }

        if msg_end <= i {
            break; // malformed — avoid infinite loop
        }
        i = msg_end;
    }
    None
}

impl Default for ReplicationLagCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state_not_caught_up() {
        let cache = ReplicationLagCache::new();
        // Initial lag is -1 (unknown), so not caught up.
        assert!(!cache.is_caught_up());
        assert_eq!(cache.lag_bytes(), -1);
    }

    #[test]
    fn test_caught_up_when_below_threshold() {
        let cache = ReplicationLagCache::new();
        cache.cached_lag_bytes.store(0, Ordering::Relaxed);
        assert!(cache.is_caught_up());

        cache.cached_lag_bytes.store(1024, Ordering::Relaxed);
        assert!(cache.is_caught_up()); // equal to threshold

        cache.cached_lag_bytes.store(500, Ordering::Relaxed);
        assert!(cache.is_caught_up());
    }

    #[test]
    fn test_not_caught_up_when_above_threshold() {
        let cache = ReplicationLagCache::new();
        cache.cached_lag_bytes.store(1025, Ordering::Relaxed);
        assert!(!cache.is_caught_up());

        cache.cached_lag_bytes.store(100000, Ordering::Relaxed);
        assert!(!cache.is_caught_up());
    }

    /// Build a minimal PG DataRow message with a single text column.
    fn build_data_row(value: &str) -> Vec<u8> {
        let val_bytes = value.as_bytes();
        // DataRow: type(1) + len(4) + num_cols(2) + col_len(4) + value
        let msg_len = 4 + 2 + 4 + val_bytes.len();
        let mut buf = Vec::new();
        buf.push(b'D');
        buf.extend_from_slice(&(msg_len as i32).to_be_bytes());
        buf.extend_from_slice(&1_i16.to_be_bytes()); // 1 column
        buf.extend_from_slice(&(val_bytes.len() as i32).to_be_bytes());
        buf.extend_from_slice(val_bytes);
        buf
    }

    #[test]
    fn test_parse_data_row_value() {
        let row = build_data_row("512");
        assert_eq!(parse_first_data_row_value(&row), Some(512));
    }

    #[test]
    fn test_parse_data_row_zero() {
        let row = build_data_row("0");
        assert_eq!(parse_first_data_row_value(&row), Some(0));
    }

    #[test]
    fn test_parse_data_row_negative() {
        let row = build_data_row("-100");
        assert_eq!(parse_first_data_row_value(&row), Some(-100));
    }

    #[test]
    fn test_parse_no_data_row() {
        // CommandComplete message: 'C' + len + "SELECT 0\0"
        let msg = b"C\x00\x00\x00\x0eSELECT 0\x00";
        assert_eq!(parse_first_data_row_value(msg), None);
    }

    #[test]
    fn test_parse_empty_response() {
        assert_eq!(parse_first_data_row_value(&[]), None);
    }

    #[test]
    fn test_parse_data_row_with_preceding_row_description() {
        // RowDescription followed by DataRow — parser should skip non-D messages.
        let mut buf = Vec::new();
        // Minimal RowDescription: 'T' + len(4) + num_fields(2)=0
        buf.push(b'T');
        buf.extend_from_slice(&6_i32.to_be_bytes());
        buf.extend_from_slice(&0_i16.to_be_bytes());
        // Then DataRow
        buf.extend_from_slice(&build_data_row("1024"));
        assert_eq!(parse_first_data_row_value(&buf), Some(1024));
    }
}
