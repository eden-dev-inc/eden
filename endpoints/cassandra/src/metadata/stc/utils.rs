#![allow(dead_code)]

use crate::api::lib::QueryUnpagedInput;
use crate::output::CassandraQueryOutput;
use cassandra_core::CassandraAsync;
use ep_core::ToOutput;
use error::{EpError, MetadataError, ResultEP};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;

/// Default timeout for metadata queries.
pub(crate) const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(10);

// Query construction

/// Create a `QueryUnpagedInput` from a CQL string.
pub(crate) fn query(cql: impl Into<String>) -> QueryUnpagedInput {
    QueryUnpagedInput::new(cql.into())
}

/// Build a named query map from an array of `(name, QueryUnpagedInput)` pairs.
pub(crate) fn query_map<const N: usize>(queries: [(&str, QueryUnpagedInput); N]) -> HashMap<String, QueryUnpagedInput> {
    queries.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

// Query execution

/// Execute a CQL query with a timeout, returning parsed JSON rows.
pub(crate) async fn run_query_with_timeout(
    query_input: &QueryUnpagedInput,
    context: CassandraAsync,
    query_timeout: Duration,
    label: &str,
) -> ResultEP<Value> {
    let result = timeout(query_timeout, query_input.run_query(context))
        .await
        .map_err(|_| EpError::Metadata(MetadataError::QueryTimeout(label.to_string())))?;

    CassandraQueryOutput(result?).try_serde_serialize()
}

/// Look up a named query from a request map and execute it with a timeout.
pub(crate) async fn run_named_query(
    requests: &HashMap<String, QueryUnpagedInput>,
    name: &str,
    context: CassandraAsync,
    query_timeout: Duration,
) -> ResultEP<Value> {
    let query_input = requests.get(name).ok_or_else(|| EpError::metadata(format!("Missing query: {name}")))?;

    run_query_with_timeout(query_input, context, query_timeout, name).await
}

/// Execute a named query, returning `None` on any error instead of propagating.
///
/// Use this for optional/non-critical subqueries where a failure should not
/// prevent the rest of the collector from completing.
pub(crate) async fn run_optional_named_query(
    requests: &HashMap<String, QueryUnpagedInput>,
    name: &str,
    context: CassandraAsync,
    query_timeout: Duration,
) -> Option<Value> {
    run_named_query(requests, name, context, query_timeout).await.ok()
}

/// Execute an ad-hoc CQL string, returning `None` on any error.
pub(crate) async fn run_optional_query(cql: &str, context: CassandraAsync, query_timeout: Duration, label: &str) -> Option<Value> {
    let q = query(cql);
    run_query_with_timeout(&q, context, query_timeout, label).await.ok()
}

// Value extraction helpers

/// Extract a string field from a JSON row object.
pub(crate) fn get_string(value: &Value, field: &str) -> Option<String> {
    value.get(field)?.as_str().map(|s| s.to_string())
}

/// Extract a string field, returning a default if missing.
pub(crate) fn get_string_or(value: &Value, field: &str, default: &str) -> String {
    get_string(value, field).unwrap_or_else(|| default.to_string())
}

/// Extract a u64 field from a JSON row object.
pub(crate) fn get_u64(value: &Value, field: &str) -> Option<u64> {
    value.get(field)?.as_u64()
}

/// Extract a u64 field, returning 0 if missing.
pub(crate) fn get_u64_or_zero(value: &Value, field: &str) -> u64 {
    get_u64(value, field).unwrap_or(0)
}

/// Extract an i64 field from a JSON row object.
pub(crate) fn get_i64(value: &Value, field: &str) -> Option<i64> {
    value.get(field)?.as_i64()
}

/// Extract an f64 field from a JSON row object.
pub(crate) fn get_f64(value: &Value, field: &str) -> Option<f64> {
    value.get(field)?.as_f64()
}

/// Extract an f64 field, returning 0.0 if missing.
pub(crate) fn get_f64_or_zero(value: &Value, field: &str) -> f64 {
    get_f64(value, field).unwrap_or(0.0)
}

/// Extract a bool field from a JSON row object.
pub(crate) fn get_bool(value: &Value, field: &str) -> Option<bool> {
    value.get(field)?.as_bool()
}

/// Extract a bool field, returning false if missing.
pub(crate) fn get_bool_or_false(value: &Value, field: &str) -> bool {
    get_bool(value, field).unwrap_or(false)
}

// Row iteration helpers

/// Iterate over rows in a `Value::Array`, applying a mapper to each row.
/// Skips rows for which the mapper returns `None`.
pub(crate) fn map_rows<T, F>(data: &Value, mapper: F) -> Vec<T>
where
    F: Fn(&Value) -> Option<T>,
{
    let Value::Array(rows) = data else {
        return Vec::new();
    };
    rows.iter().filter_map(mapper).collect()
}

/// Count the rows in a `Value::Array`.
pub(crate) fn row_count(data: &Value) -> usize {
    match data {
        Value::Array(rows) => rows.len(),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn value_extraction() {
        let row = json!({"name": "test", "count": 42, "ratio": 3.15, "active": true});

        assert_eq!(get_string(&row, "name"), Some("test".to_string()));
        assert_eq!(get_string(&row, "missing"), None);
        assert_eq!(get_string_or(&row, "missing", "default"), "default");

        assert_eq!(get_u64(&row, "count"), Some(42));
        assert_eq!(get_u64_or_zero(&row, "missing"), 0);

        assert_eq!(get_f64(&row, "ratio"), Some(3.15));
        assert_eq!(get_f64_or_zero(&row, "missing"), 0.0);

        assert_eq!(get_bool(&row, "active"), Some(true));
        assert!(!get_bool_or_false(&row, "missing"));
    }

    #[test]
    fn map_rows_extracts_values() {
        let data = json!([
            {"name": "alpha"},
            {"name": "beta"},
            {"other": "no name"}
        ]);

        let names: Vec<String> = map_rows(&data, |row| get_string(row, "name"));
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    #[test]
    fn row_count_on_array() {
        assert_eq!(row_count(&json!([1, 2, 3])), 3);
        assert_eq!(row_count(&json!("not an array")), 0);
    }
}
