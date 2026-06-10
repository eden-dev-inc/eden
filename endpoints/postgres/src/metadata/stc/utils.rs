use std::collections::HashMap;
use std::fmt::Display;
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, Utc};
use error::{EpError, ResultEP};
use format::timestamp::DateTimeWrapper;
use log::warn;
use postgres_core::{PgSimpleRow, PostgresAsync};
use serde_json::Value;
use tokio::time::timeout;

use crate::api::lib::query::QueryInput;

/// Convenience helpers shared by Postgres metadata collectors.
#[allow(dead_code)]
pub trait RowExt {
    fn get_u64(&self, column: &str) -> ResultEP<u64>;
    fn get_opt_u64(&self, column: &str) -> ResultEP<Option<u64>>;
    fn get_u32(&self, column: &str) -> ResultEP<u32>;
    fn get_i64(&self, column: &str) -> ResultEP<i64>;
    fn get_i32(&self, column: &str) -> ResultEP<i32>;
    fn get_f64(&self, column: &str) -> ResultEP<f64>;
    fn get_opt_f64(&self, column: &str) -> ResultEP<Option<f64>>;
    fn get_bool(&self, column: &str) -> ResultEP<bool>;
    fn get_string(&self, column: &str) -> ResultEP<String>;
    fn get_opt_string(&self, column: &str) -> ResultEP<Option<String>>;
    fn get_opt_i32(&self, column: &str) -> ResultEP<Option<i32>>;
    fn get_datetime(&self, column: &str) -> ResultEP<DateTimeWrapper>;
    fn get_opt_datetime(&self, column: &str) -> ResultEP<Option<DateTimeWrapper>>;
    fn get_json(&self, column: &str) -> ResultEP<Value>;
}

impl RowExt for PgSimpleRow {
    fn get_u64(&self, column: &str) -> ResultEP<u64> {
        let text = self.get(column).ok_or_else(|| column_error(column, "column not found or NULL"))?;
        // Try parsing as i64 first (PG returns signed integers), then as f64 for numeric
        if let Ok(v) = text.parse::<i64>() {
            ensure_non_negative(v, column)
        } else if let Ok(v) = text.parse::<f64>() {
            ensure_non_negative(v as i64, column)
        } else {
            Err(column_error(column, format!("cannot parse '{text}' as u64")))
        }
    }

    fn get_opt_u64(&self, column: &str) -> ResultEP<Option<u64>> {
        match self.get(column) {
            Some(text) => {
                if let Ok(v) = text.parse::<i64>() {
                    Ok(Some(ensure_non_negative(v, column)?))
                } else if let Ok(v) = text.parse::<f64>() {
                    Ok(Some(ensure_non_negative(v as i64, column)?))
                } else {
                    Err(column_error(column, format!("cannot parse '{text}' as u64")))
                }
            }
            None => Ok(None),
        }
    }

    fn get_u32(&self, column: &str) -> ResultEP<u32> {
        let value = self.get_u64(column)?;
        if value > u32::MAX as u64 {
            return Err(EpError::metadata(format!("Value for {column} exceeds u32 range: {value}")));
        }
        Ok(value as u32)
    }

    fn get_i64(&self, column: &str) -> ResultEP<i64> {
        let text = self.get(column).ok_or_else(|| column_error(column, "column not found or NULL"))?;
        text.parse::<i64>()
            .or_else(|_| {
                // PG numeric type may come as decimal string
                text.parse::<f64>().map(|v| v as i64)
            })
            .map_err(|_| column_error(column, format!("cannot parse '{text}' as i64")))
    }

    fn get_i32(&self, column: &str) -> ResultEP<i32> {
        let text = self.get(column).ok_or_else(|| column_error(column, "column not found or NULL"))?;
        text.parse::<i32>().map_err(|_| column_error(column, format!("cannot parse '{text}' as i32")))
    }

    fn get_f64(&self, column: &str) -> ResultEP<f64> {
        let text = self.get(column).ok_or_else(|| column_error(column, "column not found or NULL"))?;
        text.parse::<f64>().map_err(|_| column_error(column, format!("cannot parse '{text}' as f64")))
    }

    fn get_opt_f64(&self, column: &str) -> ResultEP<Option<f64>> {
        match self.get(column) {
            Some(text) => Ok(Some(
                text.parse::<f64>().map_err(|_| column_error(column, format!("cannot parse '{text}' as f64")))?,
            )),
            None => Ok(None),
        }
    }

    fn get_bool(&self, column: &str) -> ResultEP<bool> {
        let text = self.get(column).ok_or_else(|| column_error(column, "column not found or NULL"))?;
        match text {
            "t" | "true" | "1" | "yes" | "on" => Ok(true),
            "f" | "false" | "0" | "no" | "off" => Ok(false),
            _ => Err(column_error(column, format!("cannot parse '{text}' as bool"))),
        }
    }

    fn get_string(&self, column: &str) -> ResultEP<String> {
        let text = self.get(column).ok_or_else(|| column_error(column, "column not found or NULL"))?;
        Ok(text.to_string())
    }

    fn get_opt_string(&self, column: &str) -> ResultEP<Option<String>> {
        Ok(self.get(column).map(|s| s.to_string()))
    }

    fn get_opt_i32(&self, column: &str) -> ResultEP<Option<i32>> {
        match self.get(column) {
            Some(text) => Ok(Some(
                text.parse::<i32>().map_err(|_| column_error(column, format!("cannot parse '{text}' as i32")))?,
            )),
            None => Ok(None),
        }
    }

    fn get_datetime(&self, column: &str) -> ResultEP<DateTimeWrapper> {
        let text = self.get(column).ok_or_else(|| column_error(column, "column not found or NULL"))?;
        parse_pg_timestamp(text, column).map(DateTimeWrapper::from)
    }

    fn get_opt_datetime(&self, column: &str) -> ResultEP<Option<DateTimeWrapper>> {
        match self.get(column) {
            Some(text) => Ok(Some(parse_pg_timestamp(text, column).map(DateTimeWrapper::from)?)),
            None => Ok(None),
        }
    }

    fn get_json(&self, column: &str) -> ResultEP<Value> {
        let text = self.get(column).ok_or_else(|| column_error(column, "column not found or NULL"))?;
        serde_json::from_str(text).map_err(|e| column_error(column, e))
    }
}

fn column_error(column: &str, err: impl Display) -> EpError {
    EpError::metadata(format!("Failed to get column {column}: {err}"))
}

fn ensure_non_negative(value: i64, column: &str) -> ResultEP<u64> {
    if value < 0 {
        warn!("Negative value for {column}: {value}, clamping to 0");
        Ok(0)
    } else {
        Ok(value as u64)
    }
}

pub fn get_first_string<'a, F>(mut lookup: F, columns: &[&str]) -> ResultEP<String>
where
    F: FnMut(&str) -> Option<&'a str>,
{
    for column in columns {
        if let Some(value) = lookup(column) {
            return Ok(value.to_string());
        }
    }

    Err(EpError::metadata(format!(
        "Failed to get any of columns [{}]: column not found or NULL",
        columns.join(", ")
    )))
}

/// Parse a PostgreSQL timestamp string into `DateTime<Utc>`.
///
/// Handles formats:
/// - "2021-01-01 12:00:00.123456+00" (timestamptz)
/// - "2021-01-01 12:00:00+00" (timestamptz without fractional)
/// - "2021-01-01 12:00:00.123456" (timestamp, treated as UTC)
/// - "2021-01-01 12:00:00" (timestamp, treated as UTC)
fn parse_pg_timestamp(text: &str, column: &str) -> ResultEP<DateTime<Utc>> {
    // Try with timezone first (timestamptz)
    if let Ok(dt) = DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z") {
        return Ok(dt.with_timezone(&Utc));
    }
    // Try without fractional seconds but with timezone
    if let Ok(dt) = DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%#z") {
        return Ok(dt.with_timezone(&Utc));
    }
    // Try without timezone (treat as UTC)
    if let Ok(ndt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(ndt.and_utc());
    }
    if let Ok(ndt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S") {
        return Ok(ndt.and_utc());
    }
    Err(column_error(column, format!("cannot parse '{text}' as timestamp")))
}

pub async fn run_named_query(
    requests: &HashMap<String, QueryInput>,
    name: &str,
    context: PostgresAsync,
    timeout_duration: Duration,
) -> ResultEP<Vec<PgSimpleRow>> {
    let query = requests.get(name).ok_or_else(|| EpError::metadata(format!("Missing query: {name}")))?;

    run_query_with_timeout(query, context, timeout_duration, name).await
}

pub async fn run_query_with_timeout(
    query: &QueryInput,
    context: PostgresAsync,
    timeout_duration: Duration,
    label: &str,
) -> ResultEP<Vec<PgSimpleRow>> {
    timeout(timeout_duration, query.run_query_parsed(context))
        .await
        .map_err(|_| EpError::metadata(format!("Query timeout for {label}")))?
}

/// Convenience helper to run a named query and return the first row (if any).
pub async fn run_single_row(
    requests: &HashMap<String, QueryInput>,
    name: &str,
    context: PostgresAsync,
    timeout_duration: Duration,
) -> ResultEP<Option<PgSimpleRow>> {
    let rows = run_named_query(requests, name, context, timeout_duration).await?;
    Ok(rows.into_iter().next())
}

pub fn seconds_since(stats_reset: Option<DateTimeWrapper>) -> Option<f64> {
    stats_reset.map(|reset| {
        let reset_dt = reset.as_datetime();
        (Utc::now() - reset_dt).num_milliseconds().max(1) as f64 / 1000.0
    })
}

/// Handles the result of a privileged query, returning `None` when access is denied.
///
/// Queries against system views may fail if the connected role lacks sufficient
/// privileges. This helper converts permission errors into a silent `Ok(None)`,
/// logs a warning and propagates all other errors unchanged.
#[allow(dead_code)]
pub fn handle_privileged_query(result: ResultEP<Vec<PgSimpleRow>>, query_name: &str) -> ResultEP<Option<Vec<PgSimpleRow>>> {
    match result {
        Ok(rows) => Ok(Some(rows)),
        Err(err) if is_permission_error(&err) => {
            warn!("query `{}` skipped due to insufficient privileges: {}", query_name, err);
            Ok(None)
        }
        Err(err) => Err(err),
    }
}

/// Returns `true` when the error message indicates a permission / privilege failure.
pub fn is_permission_error(err: &EpError) -> bool {
    let message = err.to_string().to_lowercase();
    message.contains("permission denied") || message.contains("insufficient privilege") || message.contains("must be superuser")
}

/// Returns the human-readable tier label for a health score.
///
/// The tiers are:
/// * 90-100 -- `"Excellent"`
/// * 75-89  -- `"Good"`
/// * 60-74  -- `"Fair"`
/// * 40-59  -- `"Poor"`
/// * 0-39   -- `"Critical"`
#[allow(dead_code)]
pub fn health_tier_label(score: f64) -> &'static str {
    match score as u8 {
        90..=100 => "Excellent",
        75..=89 => "Good",
        60..=74 => "Fair",
        40..=59 => "Poor",
        _ => "Critical",
    }
}

#[allow(dead_code)]
pub fn is_version_error(err: &EpError) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("relation") && msg.contains("does not exist")
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PgErrorClass {
    VersionMismatch,
    PermissionDenied,
    Transient,
    Permanent,
}

#[allow(dead_code)]
pub fn classify_pg_error(err: &EpError) -> PgErrorClass {
    if is_version_error(err) {
        PgErrorClass::VersionMismatch
    } else if is_permission_error(err) {
        PgErrorClass::PermissionDenied
    } else {
        let msg = err.to_string().to_lowercase();
        if msg.contains("timeout") || msg.contains("connection") {
            PgErrorClass::Transient
        } else {
            PgErrorClass::Permanent
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn get_first_string_prefers_first_matching_column() {
        let values = HashMap::from([("table_name", "orders"), ("relname", "orders_fallback")]);
        let result =
            get_first_string(|column| values.get(column).copied(), &["table_name", "tablename", "relname"]).expect("column should resolve");

        assert_eq!(result, "orders");
    }

    #[test]
    fn get_first_string_falls_back_to_relname() {
        let values = HashMap::from([("relname", "events")]);
        let result = get_first_string(|column| values.get(column).copied(), &["table_name", "tablename", "relname"])
            .expect("fallback column should resolve");

        assert_eq!(result, "events");
    }
}
