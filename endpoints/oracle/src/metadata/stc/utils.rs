use std::collections::HashMap;
use std::time::Duration;

use crate::api::lib::query::QueryInput;
use chrono::{DateTime, Utc};
use error::{EpError, MetadataError, ResultEP};
use format::timestamp::DateTimeWrapper;
use oracle_client::Row;
use oracle_core::OracleAsync;
use tokio::time::timeout;

/// Shared helpers for working with Oracle metadata rows.
#[allow(dead_code)]
pub trait RowExt {
    fn get_u64(&self, column: &str) -> ResultEP<u64>;
    fn get_u32(&self, column: &str) -> ResultEP<u32>;
    fn get_opt_i32(&self, column: &str) -> ResultEP<Option<i32>>;
    fn get_i64(&self, column: &str) -> ResultEP<i64>;
    fn get_i32(&self, column: &str) -> ResultEP<i32>;
    fn get_f64(&self, column: &str) -> ResultEP<f64>;
    fn get_opt_u64(&self, column: &str) -> ResultEP<Option<u64>>;
    fn get_string(&self, column: &str) -> ResultEP<String>;
    fn get_opt_string(&self, column: &str) -> ResultEP<Option<String>>;
    fn get_datetime(&self, column: &str) -> ResultEP<DateTimeWrapper>;
    fn get_opt_datetime(&self, column: &str) -> ResultEP<Option<DateTimeWrapper>>;
}

impl RowExt for Row {
    fn get_u64(&self, column: &str) -> ResultEP<u64> {
        let value = self.get::<_, Option<i64>>(column).unwrap_or(Some(0)).unwrap_or(0);
        if value < 0 {
            Err(EpError::metadata(format!("Negative value for {column}: {value}")))
        } else {
            Ok(value as u64)
        }
    }

    fn get_u32(&self, column: &str) -> ResultEP<u32> {
        let value = self.get::<_, Option<i32>>(column).unwrap_or(Some(0)).unwrap_or(0);
        if value < 0 {
            return Err(EpError::metadata(format!("Negative value for {column}: {value}")));
        }
        u32::try_from(value).map_err(|_| EpError::metadata(format!("Value out of u32 range for {column}: {value}")))
    }

    fn get_opt_i32(&self, column: &str) -> ResultEP<Option<i32>> {
        self.get::<_, Option<i32>>(column).map_err(EpError::metadata)
    }

    fn get_i64(&self, column: &str) -> ResultEP<i64> {
        self.get::<_, i64>(column).map_err(EpError::metadata)
    }

    fn get_i32(&self, column: &str) -> ResultEP<i32> {
        self.get::<_, i32>(column).map_err(EpError::metadata)
    }

    fn get_f64(&self, column: &str) -> ResultEP<f64> {
        self.get::<_, f64>(column).map_err(EpError::metadata)
    }

    fn get_opt_u64(&self, column: &str) -> ResultEP<Option<u64>> {
        match self.get::<_, Option<i64>>(column).map_err(EpError::metadata)? {
            Some(value) => {
                if value < 0 {
                    Ok(Some(0))
                } else {
                    Ok(Some(value as u64))
                }
            }
            None => Ok(None),
        }
    }

    fn get_string(&self, column: &str) -> ResultEP<String> {
        self.get::<_, String>(column).map_err(EpError::metadata)
    }

    fn get_opt_string(&self, column: &str) -> ResultEP<Option<String>> {
        self.get::<_, Option<String>>(column).map_err(EpError::metadata)
    }

    fn get_datetime(&self, column: &str) -> ResultEP<DateTimeWrapper> {
        let dt = self.get::<_, DateTime<Utc>>(column).map_err(EpError::metadata)?;
        Ok(DateTimeWrapper::from(dt))
    }

    fn get_opt_datetime(&self, column: &str) -> ResultEP<Option<DateTimeWrapper>> {
        match self.get::<_, Option<DateTime<Utc>>>(column).map_err(EpError::metadata)? {
            Some(dt) => Ok(Some(DateTimeWrapper::from(dt))),
            None => Ok(None),
        }
    }
}

/// Execute a metadata query with a timeout and consistent timeout error.
pub async fn run_query_with_timeout(
    query: &QueryInput,
    context: OracleAsync,
    timeout_duration: Duration,
    label: &str,
) -> ResultEP<Vec<Row>> {
    timeout(timeout_duration, query.run_query(context))
        .await
        .map_err(|_| EpError::Metadata(MetadataError::QueryTimeout(label.to_string())))?
}

/// Execute a named query with a shared timeout helper.
pub async fn run_named_query(
    requests: &HashMap<String, QueryInput>,
    name: &str,
    context: OracleAsync,
    timeout_duration: Duration,
) -> ResultEP<Vec<Row>> {
    let query = requests.get(name).ok_or_else(|| EpError::metadata(format!("Missing query: {name}")))?;
    run_query_with_timeout(query, context, timeout_duration, name).await
}

/// Execute a query and parse optional results.
///
/// Query failures are intentionally treated as non-fatal and return `Ok(None)`.
/// Parsing errors are still propagated as `Err`.
pub async fn run_optional_query<T, F>(
    query: &QueryInput,
    context: OracleAsync,
    timeout_duration: Duration,
    label: &str,
    parse: F,
) -> ResultEP<Option<T>>
where
    F: FnOnce(Vec<Row>) -> ResultEP<T>,
{
    let rows = match run_query_with_timeout(query, context, timeout_duration, label).await {
        Ok(rows) => rows,
        Err(_) => return Ok(None),
    };

    parse(rows).map(Some)
}

/// Build a query with no bind parameters.
pub fn query(sql: impl Into<String>) -> QueryInput {
    QueryInput::new(sql.into(), Vec::new())
}

/// Build a query and append a `FETCH FIRST ... ROWS ONLY` clause.
pub fn query_with_limit(sql: impl Into<String>, limit: usize) -> QueryInput {
    let sql = sql.into();
    query(format!("{}\nFETCH FIRST {} ROWS ONLY", sql.trim_end(), limit))
}

/// Execute an optional query and assign the parsed value to an optional field.
pub async fn assign_optional<T, F>(
    target: &mut Option<T>,
    query: &QueryInput,
    context: OracleAsync,
    timeout_duration: Duration,
    label: &str,
    parse: F,
) -> ResultEP<()>
where
    F: FnOnce(Vec<Row>) -> ResultEP<T>,
{
    if let Some(value) = run_optional_query(query, context, timeout_duration, label, parse).await? {
        *target = Some(value);
    }
    Ok(())
}

/// Execute an optional query only when enabled and assign the parsed value.
pub async fn assign_optional_if<T, Q, F>(
    enabled: bool,
    target: &mut Option<T>,
    query_builder: Q,
    context: OracleAsync,
    timeout_duration: Duration,
    label: &str,
    parse: F,
) -> ResultEP<()>
where
    Q: FnOnce() -> QueryInput,
    F: FnOnce(Vec<Row>) -> ResultEP<T>,
{
    if !enabled {
        return Ok(());
    }

    let query = query_builder();
    assign_optional(target, &query, context, timeout_duration, label, parse).await
}

/// Execute an optional query and assign the parsed rows to a vector field.
pub async fn assign_optional_vec<T, F>(
    target: &mut Vec<T>,
    query: &QueryInput,
    context: OracleAsync,
    timeout_duration: Duration,
    label: &str,
    parse: F,
) -> ResultEP<()>
where
    F: FnOnce(Vec<Row>) -> ResultEP<Vec<T>>,
{
    if let Some(value) = run_optional_query(query, context, timeout_duration, label, parse).await? {
        *target = value;
    }
    Ok(())
}

/// Execute an optional query only when enabled and assign the parsed rows.
pub async fn assign_optional_vec_if<T, Q, F>(
    enabled: bool,
    target: &mut Vec<T>,
    query_builder: Q,
    context: OracleAsync,
    timeout_duration: Duration,
    label: &str,
    parse: F,
) -> ResultEP<()>
where
    Q: FnOnce() -> QueryInput,
    F: FnOnce(Vec<Row>) -> ResultEP<Vec<T>>,
{
    if !enabled {
        return Ok(());
    }

    let query = query_builder();
    assign_optional_vec(target, &query, context, timeout_duration, label, parse).await
}

/// Returns true when at least one collection condition is enabled.
pub fn should_collect(conditions: &[bool]) -> bool {
    conditions.iter().copied().any(std::convert::identity)
}

/// Execute a named query and return only the first row.
pub async fn run_single_row(
    requests: &HashMap<String, QueryInput>,
    name: &str,
    context: OracleAsync,
    timeout_duration: Duration,
) -> ResultEP<Option<Row>> {
    let rows = run_named_query(requests, name, context, timeout_duration).await?;
    Ok(rows.into_iter().next())
}

/// Maps Oracle rows into typed structs with a shared allocation pattern.
pub fn map_rows<T, F>(rows: Vec<Row>, mut mapper: F) -> ResultEP<Vec<T>>
where
    F: FnMut(Row) -> ResultEP<T>,
{
    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(mapper(row)?);
    }
    Ok(items)
}
