//! Background divergence detection for dual-write mode (DW-3, DW-10).
//!
//! Periodically compares row counts between old and new databases for migrated
//! tables. Detects drift caused by missed writes, replay failures, or
//! trigger side-effects (DW-10).
//!
//! # Lifecycle
//!
//! Started when a migration enters Replicated write mode. Polls on a
//! configurable interval (default 30s) and logs warnings on mismatches.

use eden_core::format::cache_uuid::EndpointCacheUuid;
use eden_logger_internal::{LogAudience, LogContext, log_info, log_warn};
use endpoints::endpoint::postgres::ep::PostgresEp;
use ep_core::ReqType;
use std::sync::Arc;
use std::time::Duration;

/// Default interval between divergence checks.
const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Background divergence detector.
///
/// Compares `SELECT count(*)` between old and new endpoints for a set of
/// tables. Mismatches are logged as warnings. Tables with known triggers
/// (DW-10) are checked first and flagged in log output.
pub struct DivergenceDetector {
    /// Tables to check.
    tables: Vec<TableInfo>,
    /// How often to check.
    check_interval: Duration,
}

/// Metadata about a table being monitored for divergence.
struct TableInfo {
    name: String,
    /// True if the table has triggers that may cause side-effects not captured
    /// by proxy-level dual-write (DW-10).
    has_triggers: bool,
}

impl DivergenceDetector {
    /// Create a new detector for the given table names.
    pub fn new(table_names: Vec<String>) -> Self {
        Self {
            tables: table_names.into_iter().map(|name| TableInfo { name, has_triggers: false }).collect(),
            check_interval: DEFAULT_CHECK_INTERVAL,
        }
    }

    /// Mark tables that have triggers (discovered via information_schema query).
    pub fn mark_trigger_tables(&mut self, trigger_tables: &[String]) {
        for table in &mut self.tables {
            if trigger_tables.contains(&table.name) {
                table.has_triggers = true;
            }
        }
    }

    /// Spawn a background task that periodically checks for divergence.
    ///
    /// Returns a `JoinHandle` that can be aborted to stop the detector.
    pub fn start(
        self: Arc<Self>,
        ep: PostgresEp,
        old_endpoint: EndpointCacheUuid,
        new_endpoint: EndpointCacheUuid,
        ctx: LogContext,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            log_info!(
                ctx.clone(),
                "DW-3: divergence detector started",
                audience = LogAudience::Internal,
                tables = self.tables.len(),
                interval_secs = self.check_interval.as_secs()
            );

            // DW-10: Warn about trigger tables at startup.
            let trigger_tables: Vec<&str> = self.tables.iter().filter(|t| t.has_triggers).map(|t| t.name.as_str()).collect();
            if !trigger_tables.is_empty() {
                log_warn!(
                    ctx.clone(),
                    "DW-10: tables with triggers detected — side-effects may cause divergence",
                    audience = LogAudience::Internal,
                    trigger_tables = trigger_tables.join(", ")
                );
            }

            loop {
                tokio::time::sleep(self.check_interval).await;
                self.check_all(&ep, &old_endpoint, &new_endpoint, &ctx).await;
            }
        })
    }

    /// Check all tables for divergence.
    async fn check_all(&self, ep: &PostgresEp, old_endpoint: &EndpointCacheUuid, new_endpoint: &EndpointCacheUuid, ctx: &LogContext) {
        // Prioritize trigger tables first (DW-10).
        let mut sorted_tables: Vec<&TableInfo> = self.tables.iter().collect();
        sorted_tables.sort_by_key(|t| !t.has_triggers); // triggers first

        for table in sorted_tables {
            let old_count = Self::count_rows(ep, old_endpoint, &table.name).await;
            let new_count = Self::count_rows(ep, new_endpoint, &table.name).await;

            match (old_count, new_count) {
                (Some(old), Some(new)) if old != new => {
                    let label = if table.has_triggers { "TRIGGER-TABLE " } else { "" };
                    log_warn!(
                        ctx.clone(),
                        "DW-3: row count divergence detected",
                        audience = LogAudience::Internal,
                        table = table.name.as_str(),
                        old_count = old,
                        new_count = new,
                        diff = (old as i64 - new as i64).unsigned_abs(),
                        label = label
                    );
                }
                (None, Some(_)) => {
                    log_warn!(
                        ctx.clone(),
                        "DD-5: divergence check failed for old endpoint",
                        audience = LogAudience::Internal,
                        table = table.name.as_str(),
                        side = "old"
                    );
                }
                (Some(_), None) => {
                    log_warn!(
                        ctx.clone(),
                        "DD-5: divergence check failed for new endpoint",
                        audience = LogAudience::Internal,
                        table = table.name.as_str(),
                        side = "new"
                    );
                }
                (None, None) => {
                    log_warn!(
                        ctx.clone(),
                        "DD-5: divergence check failed for both endpoints",
                        audience = LogAudience::Internal,
                        table = table.name.as_str()
                    );
                }
                _ => {
                    // Counts match — no divergence.
                }
            }
        }
    }

    /// Count rows in a table via a raw `SELECT count(*)` query.
    ///
    /// Returns `None` if the query fails (e.g., table doesn't exist on target).
    async fn count_rows(ep: &PostgresEp, endpoint: &EndpointCacheUuid, table_name: &str) -> Option<u64> {
        // Sanitize table name (basic double-quote escaping).
        let safe_name = table_name.replace('"', "\"\"");
        let query = format!("SELECT count(*) FROM \"{}\"", safe_name);
        let query_bytes = postgres_core::client::build_query_message(&query);

        let mut conn = ep.raw_connection(endpoint, ReqType::Read).await.ok()?;
        let (response, _) = conn.send_query_raw(&query_bytes).await.ok()?;

        // Parse count from the DataRow in the response.
        // Simple Q response: RowDescription + DataRow + CommandComplete + ReadyForQuery
        // DataRow format: 'D' + len + field_count(i16) + [field_len(i32) + data]...
        parse_count_from_response(&response)
    }
}

/// Extract count value from a simple query response containing a single integer column.
fn parse_count_from_response(response: &[u8]) -> Option<u64> {
    let mut pos = 0;
    while pos < response.len() {
        if response.len() < pos + 5 {
            break;
        }
        let msg_type = response[pos];
        let length = i32::from_be_bytes([response[pos + 1], response[pos + 2], response[pos + 3], response[pos + 4]]);
        // PG wire protocol length includes itself (4 bytes minimum).
        if length < 4 {
            break;
        }
        let total = 1 + length as usize;

        if msg_type == b'D' {
            // DataRow: skip type(1) + length(4) + field_count(2)
            let data_start = pos + 7;
            if data_start + 4 > pos + total {
                break;
            }
            // First field: length(i32) + data
            let field_len = i32::from_be_bytes([
                response[data_start],
                response[data_start + 1],
                response[data_start + 2],
                response[data_start + 3],
            ]);
            if field_len > 0 {
                let field_start = data_start + 4;
                let field_end = field_start + field_len as usize;
                if field_end <= pos + total {
                    let text = std::str::from_utf8(&response[field_start..field_end]).ok()?;
                    return text.parse::<u64>().ok();
                }
            }
        }
        pos += total;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_count_from_datarow() {
        // Build a minimal DataRow message with "42"
        let mut data_row = Vec::new();
        data_row.push(b'D'); // DataRow type
        let field_data = b"42";
        // length = 4 (self) + 2 (field_count) + 4 (field_len) + 2 (data)
        let len: i32 = 4 + 2 + 4 + field_data.len() as i32;
        data_row.extend_from_slice(&len.to_be_bytes());
        data_row.extend_from_slice(&1_i16.to_be_bytes()); // 1 field
        data_row.extend_from_slice(&(field_data.len() as i32).to_be_bytes());
        data_row.extend_from_slice(field_data);

        assert_eq!(parse_count_from_response(&data_row), Some(42));
    }

    #[test]
    fn test_parse_count_empty() {
        assert_eq!(parse_count_from_response(&[]), None);
    }

    #[test]
    fn test_new_and_mark_triggers() {
        let mut det = DivergenceDetector::new(vec!["users".into(), "orders".into(), "logs".into()]);
        det.mark_trigger_tables(&["orders".into()]);
        assert_eq!(det.tables.len(), 3);
        assert!(!det.tables[0].has_triggers); // users
        assert!(det.tables[1].has_triggers); // orders
        assert!(!det.tables[2].has_triggers); // logs
    }
}
