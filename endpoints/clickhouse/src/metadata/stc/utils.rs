use crate::api::lib::QueryInput;
use crate::output::ClickhouseRow;
use clickhouse_core::ClickhouseAsync;
use error::{EpError, MetadataError, ResultEP};
use format::timestamp::DateTimeWrapper;
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;
use tokio::time::timeout;

/// Reusable query batch helper for metadata sync methods.
pub(crate) struct MetadataQueryBatch<'a> {
    context: ClickhouseAsync,
    requests: &'a HashMap<String, QueryInput>,
    timeout_duration: Duration,
}

impl<'a> MetadataQueryBatch<'a> {
    pub(crate) fn new(context: ClickhouseAsync, requests: &'a HashMap<String, QueryInput>, timeout_duration: Duration) -> Self {
        Self { context, requests, timeout_duration }
    }

    pub(crate) async fn rows(&self, name: &str) -> ResultEP<Vec<ClickhouseRow>> {
        run_named_query(self.context.clone(), self.requests, name, self.timeout_duration).await
    }

    pub(crate) async fn row(&self, name: &str) -> ResultEP<Option<ClickhouseRow>> {
        run_named_query_row(self.context.clone(), self.requests, name, self.timeout_duration).await
    }

    /// Execute a named query and return results when successful, swallowing timeout/query errors.
    /// Returns `Some(rows)` for successful execution even if `rows` is empty; `None` on failure.
    pub(crate) async fn optional_rows(&self, name: &str) -> Option<Vec<ClickhouseRow>> {
        run_optional_named_query(self.context.clone(), self.requests, name, self.timeout_duration).await
    }
}

/// Convenience constructor for query definitions that don't require params.
pub(crate) fn query(sql: impl Into<String>) -> QueryInput {
    QueryInput::new(sql.into(), Vec::new(), Vec::new())
}

/// Convenience constructor for metadata request maps keyed by query name.
pub(crate) fn query_map<const N: usize>(queries: [(&str, QueryInput); N]) -> HashMap<String, QueryInput> {
    queries.into_iter().map(|(name, input)| (name.to_string(), input)).collect()
}

/// Reusable one-off query helper for detailed/conditional query blocks.
pub(crate) struct OptionalQueryBatch {
    context: ClickhouseAsync,
    timeout_duration: Duration,
}

impl OptionalQueryBatch {
    pub(crate) fn new(context: ClickhouseAsync, timeout_duration: Duration) -> Self {
        Self { context, timeout_duration }
    }

    pub(crate) async fn assign<T, F>(&self, target: &mut T, query: &QueryInput, name: &str, parser: F) -> ResultEP<()>
    where
        F: FnOnce(Vec<ClickhouseRow>) -> ResultEP<T>,
    {
        assign_optional_query(target, self.context.clone(), query, self.timeout_duration, name, parser).await
    }

    pub(crate) async fn assign_sql<T, F>(&self, target: &mut T, name: &str, sql: impl Into<String>, parser: F) -> ResultEP<()>
    where
        F: FnOnce(Vec<ClickhouseRow>) -> ResultEP<T>,
    {
        let query = query(sql);
        self.assign(target, &query, name, parser).await
    }

    pub(crate) async fn assign_sql_if<T, F, S>(&self, condition: bool, target: &mut T, name: &str, sql: S, parser: F) -> ResultEP<()>
    where
        F: FnOnce(Vec<ClickhouseRow>) -> ResultEP<T>,
        S: FnOnce() -> String,
    {
        if condition {
            self.assign_sql(target, name, sql(), parser).await?;
        }
        Ok(())
    }
}

#[allow(dead_code)]
pub(crate) trait RowExt {
    fn required_u64(&self, column: &str) -> ResultEP<u64>;
    fn required_f64(&self, column: &str) -> ResultEP<f64>;
    fn required_string(&self, column: &str) -> ResultEP<String>;
    fn optional_string(&self, column: &str) -> ResultEP<Option<String>>;
    fn required_bool(&self, column: &str) -> ResultEP<bool>;
    fn required_datetime(&self, column: &str) -> ResultEP<DateTimeWrapper>;
    fn optional_datetime(&self, column: &str) -> ResultEP<Option<DateTimeWrapper>>;
    fn u64_or_zero(&self, column: &str) -> ResultEP<u64>;
    fn f64_or_zero(&self, column: &str) -> ResultEP<f64>;
    fn string_or_empty(&self, column: &str) -> ResultEP<String>;
    fn bool_or_false(&self, column: &str) -> ResultEP<bool>;
}

impl RowExt for ClickhouseRow {
    fn required_u64(&self, column: &str) -> ResultEP<u64> {
        match self.get(column) {
            Some(val) => val
                .as_u64()
                .or_else(|| val.as_f64().map(|f| f as u64))
                .or_else(|| val.as_str().and_then(|s| s.parse::<u64>().ok().or_else(|| s.parse::<f64>().ok().map(|f| f as u64))))
                .ok_or_else(|| EpError::metadata(format!("Failed to get column {column} as u64"))),
            None => Err(EpError::metadata(format!("Missing value: {column}"))),
        }
    }

    fn required_f64(&self, column: &str) -> ResultEP<f64> {
        match self.get(column) {
            Some(val) => val
                .as_f64()
                .or_else(|| val.as_str().and_then(|s| s.parse::<f64>().ok()))
                .ok_or_else(|| EpError::metadata(format!("Failed to get column {column} as f64"))),
            None => Err(EpError::metadata(format!("Missing value: {column}"))),
        }
    }

    fn required_string(&self, column: &str) -> ResultEP<String> {
        match self.get(column) {
            Some(val) => val
                .as_str()
                .map(ToString::to_string)
                .ok_or_else(|| EpError::metadata(format!("Failed to get column {column} as string"))),
            None => Err(EpError::metadata(format!("Missing value: {column}"))),
        }
    }

    fn optional_string(&self, column: &str) -> ResultEP<Option<String>> {
        Ok(self.get(column).and_then(|val| val.as_str().map(ToString::to_string)))
    }

    fn required_bool(&self, column: &str) -> ResultEP<bool> {
        match self.get(column) {
            Some(val) => {
                if let Some(value) = val.as_bool() {
                    Ok(value)
                } else if let Some(value) = val.as_u64() {
                    Ok(value != 0)
                } else if let Some(s) = val.as_str() {
                    match s {
                        "1" | "true" => Ok(true),
                        "0" | "false" | "" => Ok(false),
                        _ => s
                            .parse::<u64>()
                            .map(|v| v != 0)
                            .map_err(|_| EpError::metadata(format!("Failed to get column {column} as bool"))),
                    }
                } else {
                    Err(EpError::metadata(format!("Failed to get column {column} as bool")))
                }
            }
            None => Err(EpError::metadata(format!("Missing value: {column}"))),
        }
    }

    fn required_datetime(&self, column: &str) -> ResultEP<DateTimeWrapper> {
        self.get_datetime(column)
            .map(DateTimeWrapper::from)
            .ok_or_else(|| EpError::metadata(format!("Failed to parse datetime for column {column}")))
    }

    fn optional_datetime(&self, column: &str) -> ResultEP<Option<DateTimeWrapper>> {
        if self
            .get(column)
            .is_some_and(|val| val.as_str().is_some_and(|date_str| date_str.is_empty() || date_str == "0000-00-00 00:00:00"))
        {
            Ok(None)
        } else {
            Ok(self.get_datetime(column).map(DateTimeWrapper::from))
        }
    }

    fn u64_or_zero(&self, column: &str) -> ResultEP<u64> {
        match self.get(column) {
            Some(val) if val.is_null() => Ok(0),
            Some(val) => val
                .as_u64()
                .or_else(|| val.as_f64().map(|f| f as u64))
                .or_else(|| val.as_str().and_then(|s| s.parse::<u64>().ok().or_else(|| s.parse::<f64>().ok().map(|f| f as u64))))
                .ok_or_else(|| EpError::metadata(format!("Failed to get column {column} as u64"))),
            None => Ok(0),
        }
    }

    fn f64_or_zero(&self, column: &str) -> ResultEP<f64> {
        match self.get(column) {
            Some(val) if val.is_null() => Ok(0.0),
            Some(val) => val
                .as_f64()
                .or_else(|| val.as_str().and_then(|s| s.parse::<f64>().ok()))
                .ok_or_else(|| EpError::metadata(format!("Failed to get column {column} as f64"))),
            None => Ok(0.0),
        }
    }

    fn string_or_empty(&self, column: &str) -> ResultEP<String> {
        match self.get(column) {
            Some(val) => val
                .as_str()
                .map(ToString::to_string)
                .ok_or_else(|| EpError::metadata(format!("Failed to get column {column} as string"))),
            None => Ok(String::new()),
        }
    }

    fn bool_or_false(&self, column: &str) -> ResultEP<bool> {
        match self.get(column) {
            Some(val) => {
                if let Some(value) = val.as_bool() {
                    Ok(value)
                } else if let Some(value) = val.as_u64() {
                    Ok(value != 0)
                } else if let Some(s) = val.as_str() {
                    match s {
                        "1" | "true" => Ok(true),
                        "0" | "false" | "" => Ok(false),
                        _ => s
                            .parse::<u64>()
                            .map(|v| v != 0)
                            .map_err(|_| EpError::metadata(format!("Failed to get column {column} as bool"))),
                    }
                } else {
                    Err(EpError::metadata(format!("Failed to get column {column} as bool")))
                }
            }
            None => Ok(false),
        }
    }
}

/// Execute a metadata query with a timeout and a consistent timeout error.
pub(crate) async fn run_query_with_timeout(
    context: ClickhouseAsync,
    query: &QueryInput,
    timeout_duration: Duration,
    label: &str,
) -> ResultEP<Vec<ClickhouseRow>> {
    timeout(timeout_duration, query.run_query(context))
        .await
        .map_err(|_| EpError::Metadata(MetadataError::QueryTimeout(label.to_string())))?
}

/// Execute a named query from the metadata request map.
pub(crate) async fn run_named_query(
    context: ClickhouseAsync,
    requests: &HashMap<String, QueryInput>,
    name: &str,
    timeout_duration: Duration,
) -> ResultEP<Vec<ClickhouseRow>> {
    let query = requests.get(name).ok_or_else(|| EpError::metadata(format!("Missing query: {name}")))?;
    run_query_with_timeout(context, query, timeout_duration, name).await
}

/// Execute a named query and return only the first row (if present).
pub(crate) async fn run_named_query_row(
    context: ClickhouseAsync,
    requests: &HashMap<String, QueryInput>,
    name: &str,
    timeout_duration: Duration,
) -> ResultEP<Option<ClickhouseRow>> {
    Ok(run_named_query(context, requests, name, timeout_duration).await?.into_iter().next())
}

/// Execute a named query and return results when successful, swallowing timeout/query errors.
/// Returns `Some(rows)` for successful execution even if `rows` is empty; `None` only
/// when the named query cannot be executed (missing key, timeout or execution error).
pub(crate) async fn run_optional_named_query(
    context: ClickhouseAsync,
    requests: &HashMap<String, QueryInput>,
    name: &str,
    timeout_duration: Duration,
) -> Option<Vec<ClickhouseRow>> {
    run_named_query(context, requests, name, timeout_duration).await.ok()
}

/// Execute a one-off query and return results when successful, swallowing timeout/query errors.
/// Returns `Some(rows)` for successful execution even if `rows` is empty; `None` on failure.
pub(crate) async fn run_optional_query(
    context: ClickhouseAsync,
    query: &QueryInput,
    timeout_duration: Duration,
    name: &str,
) -> Option<Vec<ClickhouseRow>> {
    run_query_with_timeout(context, query, timeout_duration, name).await.ok()
}

/// Execute an optional query and, when successful, parse and assign its result to `target`.
pub(crate) async fn assign_optional_query<T, F>(
    target: &mut T,
    context: ClickhouseAsync,
    query: &QueryInput,
    timeout_duration: Duration,
    name: &str,
    parser: F,
) -> ResultEP<()>
where
    F: FnOnce(Vec<ClickhouseRow>) -> ResultEP<T>,
{
    if let Some(rows) = run_optional_query(context, query, timeout_duration, name).await {
        *target = parser(rows)?;
    }
    Ok(())
}

/// Parse a full row set into strongly typed models with a shared map/collect flow.
pub(crate) fn parse_rows<T, F>(rows: Vec<ClickhouseRow>, parser: F) -> ResultEP<Vec<T>>
where
    F: FnMut(ClickhouseRow) -> ResultEP<T>,
{
    rows.into_iter().map(parser).collect()
}

/// Build optional detailed metadata with a shared collection flow.
pub(crate) async fn collect_if_needed<T, F, Fut>(
    should_collect: bool,
    context: ClickhouseAsync,
    timeout_duration: Duration,
    collector: F,
) -> ResultEP<Option<T>>
where
    T: Default,
    F: FnOnce(OptionalQueryBatch, T) -> Fut,
    Fut: Future<Output = ResultEP<T>>,
{
    if !should_collect {
        return Ok(None);
    }

    let detail_queries = OptionalQueryBatch::new(context, timeout_duration);
    let detailed = collector(detail_queries, T::default()).await?;
    Ok(Some(detailed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    #[derive(Debug, Default, PartialEq, Eq)]
    struct TestDetail {
        value: u64,
    }

    #[test]
    fn query_builder_sets_empty_binds_and_params() {
        let input = query("SELECT 1");
        let value = serde_json::to_value(input).expect("query input should serialize");

        assert_eq!(value["query"], "SELECT 1");
        assert_eq!(value["binds"], json!([]));
        assert_eq!(value["params"], json!([]));
    }

    #[test]
    fn row_ext_extracts_required_values() {
        let row = ClickhouseRow::from(vec![
            ("u".to_string(), json!(42)),
            ("f".to_string(), json!(3.5)),
            ("s".to_string(), json!("ok")),
            ("b".to_string(), json!(1)),
        ]);

        assert_eq!(row.required_u64("u").expect("u64"), 42);
        assert_eq!(row.required_f64("f").expect("f64"), 3.5);
        assert_eq!(row.required_string("s").expect("string"), "ok");
        assert!(row.required_bool("b").expect("bool"));
    }

    #[test]
    fn row_ext_parses_string_encoded_numbers() {
        let row = ClickhouseRow::from(vec![
            ("u".to_string(), json!("42")),
            ("f".to_string(), json!("3.5")),
            ("b_true".to_string(), json!("1")),
            ("b_false".to_string(), json!("0")),
        ]);

        assert_eq!(row.required_u64("u").expect("u64 from string"), 42);
        assert_eq!(row.required_f64("f").expect("f64 from string"), 3.5);
        assert_eq!(row.u64_or_zero("u").expect("u64_or_zero from string"), 42);
        assert_eq!(row.f64_or_zero("f").expect("f64_or_zero from string"), 3.5);
        assert!(row.required_bool("b_true").expect("bool true from string"));
        assert!(!row.required_bool("b_false").expect("bool false from string"));
        assert!(row.bool_or_false("b_true").expect("bool_or_false true from string"));
        assert!(!row.bool_or_false("b_false").expect("bool_or_false false from string"));
    }

    #[test]
    fn row_ext_or_zero_handles_nulls_and_missing() {
        let row = ClickhouseRow::from(vec![("null_col".to_string(), serde_json::Value::Null)]);

        assert_eq!(row.u64_or_zero("null_col").expect("null u64"), 0);
        assert_eq!(row.f64_or_zero("null_col").expect("null f64"), 0.0);
        assert_eq!(row.u64_or_zero("missing").expect("missing u64"), 0);
        assert_eq!(row.f64_or_zero("missing").expect("missing f64"), 0.0);
    }

    #[test]
    fn parse_rows_maps_rows_into_models() {
        let rows = vec![
            ClickhouseRow::from(vec![("value".to_string(), json!(1))]),
            ClickhouseRow::from(vec![("value".to_string(), json!(2))]),
        ];

        let values = parse_rows(rows, |row| row.required_u64("value")).expect("rows should parse");
        assert_eq!(values, vec![1, 2]);
    }

    #[test]
    fn parse_rows_propagates_parser_error() {
        let rows = vec![
            ClickhouseRow::from(vec![("value".to_string(), json!(1))]),
            ClickhouseRow::from(vec![("other".to_string(), json!(2))]),
        ];

        let err = parse_rows(rows, |row| row.required_u64("value")).expect_err("second row should fail");
        assert!(err.to_string().contains("Missing value: value"));
    }

    #[tokio::test]
    async fn collect_if_needed_skips_collector_when_disabled() {
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();
        let context = ClickhouseAsync::from(Vec::<clickhouse_client::Client>::new());

        let detailed = collect_if_needed::<TestDetail, _, _>(false, context, Duration::from_secs(1), move |_batch, mut state| {
            let called = called_clone.clone();
            async move {
                called.store(true, Ordering::SeqCst);
                state.value = 1;
                Ok(state)
            }
        })
        .await
        .expect("collection should succeed");

        assert_eq!(detailed, None);
        assert!(!called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn collect_if_needed_runs_collector_when_enabled() {
        let context = ClickhouseAsync::from(Vec::<clickhouse_client::Client>::new());

        let detailed = collect_if_needed::<TestDetail, _, _>(true, context, Duration::from_secs(1), |_batch, mut state| async move {
            state.value = 7;
            Ok(state)
        })
        .await
        .expect("collection should succeed");

        assert_eq!(detailed, Some(TestDetail { value: 7 }));
    }
}
