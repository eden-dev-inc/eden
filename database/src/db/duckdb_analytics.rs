//! Embedded DuckDB analytics storage for `embedded-db` builds.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use duckdb::types::{TimeUnit, Value as DuckValue, ValueRef};
use duckdb::{Connection, params_from_iter};
use eden_config::DuckDbTelemetryConfig;
use eden_core::error::{EpError, ResultEP};
use regex::{Captures, Regex};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use tokio::sync::Mutex;

const ANALYTICS_SCHEMA: &str = "analytics";
const INSERTED_AT_COLUMN: &str = "_eden_inserted_at";

#[derive(Debug, Clone)]
pub struct DuckDbAnalyticsConfig {
    pub path: PathBuf,
    pub memory_limit: String,
    pub temp_directory: PathBuf,
    pub max_temp_directory_size: String,
    pub checkpoint_threshold: String,
    pub checkpoint_interval_secs: u64,
    pub analytics_retention_days: u32,
    pub logs_retention_days: u32,
    pub traces_retention_days: u32,
}

impl DuckDbAnalyticsConfig {
    pub fn from_telemetry(config: &DuckDbTelemetryConfig) -> ResultEP<Self> {
        Ok(Self {
            path: resolve_local_path(&config.path)?,
            memory_limit: required_value("telemetry.duckdb.memory_limit", &config.memory_limit)?,
            temp_directory: resolve_local_path(&config.temp_directory)?,
            max_temp_directory_size: required_value("telemetry.duckdb.max_temp_directory_size", &config.max_temp_directory_size)?,
            checkpoint_threshold: required_value("telemetry.duckdb.checkpoint_threshold", &config.checkpoint_threshold)?,
            checkpoint_interval_secs: config.checkpoint_interval_secs,
            analytics_retention_days: config.analytics_retention_days,
            logs_retention_days: config.logs_retention_days,
            traces_retention_days: config.traces_retention_days,
        })
    }
}

#[derive(Debug)]
struct DuckDbAnalyticsInner {
    config: DuckDbAnalyticsConfig,
    writer: Mutex<()>,
}

#[derive(Debug, Clone)]
pub struct DuckDbAnalyticsStore {
    inner: Arc<DuckDbAnalyticsInner>,
}

#[derive(Debug, Clone)]
pub struct DuckDbAnalyticsConnection {
    store: DuckDbAnalyticsStore,
}

#[derive(Debug, Clone)]
pub struct DuckDbQuery {
    store: DuckDbAnalyticsStore,
    sql: String,
    bindings: Vec<Result<DuckValue, String>>,
}

impl DuckDbAnalyticsStore {
    pub async fn new(config: DuckDbAnalyticsConfig) -> ResultEP<Self> {
        prepare_paths(&config).await?;
        let store = Self {
            inner: Arc::new(DuckDbAnalyticsInner { config, writer: Mutex::new(()) }),
        };
        store.initialize().await?;
        Ok(store)
    }

    pub async fn get(&self) -> ResultEP<DuckDbAnalyticsConnection> {
        self.health_check().await?;
        Ok(DuckDbAnalyticsConnection { store: self.clone() })
    }

    pub async fn health_check(&self) -> ResultEP<()> {
        let _: u8 = self.read_blocking(|conn| conn.query_row("SELECT 1", [], |row| row.get(0)).map_err(duck_error)).await?;
        Ok(())
    }

    pub async fn ensure_schema(&self) -> ResultEP<()> {
        self.write_blocking(|conn| {
            conn.execute_batch(&format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident(ANALYTICS_SCHEMA))).map_err(duck_error)?;
            Ok(())
        })
        .await
    }

    pub async fn checkpoint(&self) -> ResultEP<()> {
        self.write_blocking(|conn| conn.execute_batch("CHECKPOINT").map_err(duck_error)).await
    }

    pub async fn prune_retention(&self) -> ResultEP<()> {
        let analytics_days = self.inner.config.analytics_retention_days;
        let logs_days = self.inner.config.logs_retention_days;
        let traces_days = self.inner.config.traces_retention_days;
        self.write_blocking(move |conn| {
            prune_table(conn, "analytics.logs", "timestamp", logs_days)?;
            prune_table(conn, "analytics.traces", "timestamp", traces_days)?;

            let tables = list_tables(conn)?;
            for table in tables {
                if table == "logs" || table == "traces" {
                    continue;
                }
                let qualified = format!("analytics.{table}");
                if let Some(column) = retention_columns(conn, &qualified)?.into_iter().next() {
                    prune_table(conn, &qualified, &column, analytics_days)?;
                }
            }
            Ok(())
        })
        .await
    }

    pub async fn insert_rows<T>(&self, table: &str, rows: &[T]) -> ResultEP<()>
    where
        T: Serialize + Sync,
    {
        if rows.is_empty() {
            return Ok(());
        }

        let qualified_table = parse_table_name(table)?;
        let serialized_rows = serialize_rows(rows)?;
        self.write_blocking(move |conn| insert_serialized_rows(conn, &qualified_table, &serialized_rows)).await
    }

    pub fn query(&self, sql: impl Into<String>) -> DuckDbQuery {
        DuckDbQuery { store: self.clone(), sql: sql.into(), bindings: Vec::new() }
    }

    async fn initialize(&self) -> ResultEP<()> {
        self.ensure_schema().await?;
        self.checkpoint().await
    }

    async fn read_blocking<F, T>(&self, op: F) -> ResultEP<T>
    where
        F: FnOnce(&Connection) -> ResultEP<T> + Send + 'static,
        T: Send + 'static,
    {
        let config = self.inner.config.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_configured_connection(&config)?;
            op(&conn)
        })
        .await
        .map_err(|err| EpError::database(format!("DuckDB blocking task failed: {err}")))?
    }

    async fn write_blocking<F, T>(&self, op: F) -> ResultEP<T>
    where
        F: FnOnce(&Connection) -> ResultEP<T> + Send + 'static,
        T: Send + 'static,
    {
        let _guard = self.inner.writer.lock().await;
        self.read_blocking(op).await
    }
}

impl DuckDbAnalyticsConnection {
    pub async fn ensure_schema(&self) -> ResultEP<()> {
        self.store.ensure_schema().await
    }

    pub async fn insert_rows<T>(&self, table: &str, rows: &[T]) -> ResultEP<()>
    where
        T: Serialize + Sync,
    {
        self.store.insert_rows(table, rows).await
    }

    pub fn query(&self, sql: impl Into<String>) -> DuckDbQuery {
        self.store.query(sql)
    }

    pub async fn checkpoint(&self) -> ResultEP<()> {
        self.store.checkpoint().await
    }
}

impl DuckDbQuery {
    pub fn bind<T>(mut self, value: T) -> Self
    where
        T: Serialize,
    {
        self.bindings.push(binding_value(value));
        self
    }

    pub async fn fetch_all<T>(self) -> ResultEP<Vec<T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let bindings = self.bindings.into_iter().collect::<Result<Vec<_>, _>>().map_err(EpError::database)?;
        let sql = translate_clickhouse_sql(&self.sql);
        self.store
            .read_blocking(move |conn| {
                let mut stmt = conn.prepare(&sql).map_err(duck_error)?;
                let mut rows = stmt.query(params_from_iter(bindings.iter())).map_err(duck_error)?;
                let column_names = rows.as_ref().map(|stmt| stmt.column_names()).unwrap_or_default();
                let mut out = Vec::new();
                while let Some(row) = rows.next().map_err(duck_error)? {
                    let value = row_to_json_value(row, &column_names)?;
                    out.push(deserialize_row(value)?);
                }
                Ok(out)
            })
            .await
    }

    pub async fn fetch_one<T>(self) -> ResultEP<T>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let mut rows = self.fetch_all::<T>().await?;
        if rows.is_empty() {
            return Err(EpError::database("DuckDB query returned no rows"));
        }
        Ok(rows.remove(0))
    }

    pub async fn fetch_optional<T>(self) -> ResultEP<Option<T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let mut rows = self.fetch_all::<T>().await?;
        Ok(if rows.is_empty() { None } else { Some(rows.remove(0)) })
    }
}

fn required_value(name: &str, value: &str) -> ResultEP<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(EpError::database(format!("{name} must not be empty")));
    }
    Ok(trimmed.to_string())
}

fn resolve_local_path(value: &str) -> ResultEP<PathBuf> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(EpError::database("telemetry.duckdb path values must not be empty"));
    }
    if trimmed == ":memory:" {
        return Ok(PathBuf::from(trimmed));
    }
    if let Some(rest) = trimmed.strip_prefix("~/") {
        let home = std::env::var("HOME").map_err(|err| EpError::database(format!("HOME must be set for DuckDB path expansion: {err}")))?;
        return Ok(PathBuf::from(home).join(rest));
    }
    Ok(PathBuf::from(trimmed))
}

async fn prepare_paths(config: &DuckDbAnalyticsConfig) -> ResultEP<()> {
    if config.path != Path::new(":memory:")
        && let Some(parent) = config.path.parent().filter(|path| !path.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|err| EpError::init(format!("Failed to create DuckDB analytics directory {}: {err}", parent.display())))?;
    }
    tokio::fs::create_dir_all(&config.temp_directory)
        .await
        .map_err(|err| EpError::init(format!("Failed to create DuckDB temp directory {}: {err}", config.temp_directory.display())))
}

fn open_configured_connection(config: &DuckDbAnalyticsConfig) -> ResultEP<Connection> {
    let conn = if config.path == Path::new(":memory:") {
        Connection::open_in_memory().map_err(duck_error)?
    } else {
        Connection::open(&config.path).map_err(duck_error)?
    };
    configure_connection(&conn, config)?;
    Ok(conn)
}

fn configure_connection(conn: &Connection, config: &DuckDbAnalyticsConfig) -> ResultEP<()> {
    set_duckdb_option(conn, "memory_limit", &config.memory_limit)?;
    set_duckdb_option(conn, "temp_directory", &config.temp_directory.display().to_string())?;
    set_duckdb_option(conn, "max_temp_directory_size", &config.max_temp_directory_size)?;
    set_duckdb_option(conn, "checkpoint_threshold", &config.checkpoint_threshold)?;
    Ok(())
}

fn set_duckdb_option(conn: &Connection, name: &str, value: &str) -> ResultEP<()> {
    if !is_valid_identifier(name) {
        return Err(EpError::database(format!("invalid DuckDB setting name: {name}")));
    }
    conn.execute_batch(&format!("SET {name} = '{}'", escape_sql_string(value))).map_err(duck_error)?;
    Ok(())
}

fn insert_serialized_rows(conn: &Connection, table: &QualifiedTable, rows: &[JsonMap<String, JsonValue>]) -> ResultEP<()> {
    if rows.is_empty() {
        return Ok(());
    }

    conn.execute_batch(&format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident(&table.schema))).map_err(duck_error)?;

    let column_types = infer_columns(rows);
    let create_columns = column_types
        .iter()
        .map(|(name, kind)| format!("{name} {}", kind.sql_type(), name = quote_ident(name)))
        .collect::<Vec<_>>()
        .join(", ");

    let create_sql = if create_columns.is_empty() {
        format!(
            "CREATE TABLE IF NOT EXISTS {} ({} TIMESTAMP DEFAULT current_timestamp)",
            table.sql(),
            quote_ident(INSERTED_AT_COLUMN)
        )
    } else {
        format!(
            "CREATE TABLE IF NOT EXISTS {} ({} TIMESTAMP DEFAULT current_timestamp, {})",
            table.sql(),
            quote_ident(INSERTED_AT_COLUMN),
            create_columns
        )
    };
    conn.execute_batch(&create_sql).map_err(duck_error)?;

    for (name, kind) in &column_types {
        let sql = format!("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}", table.sql(), quote_ident(name), kind.sql_type());
        conn.execute_batch(&sql).map_err(duck_error)?;
    }

    let columns = column_types.keys().cloned().collect::<Vec<_>>();
    if columns.is_empty() {
        return Ok(());
    }

    let column_sql = columns.iter().map(|column| quote_ident(column)).collect::<Vec<_>>().join(", ");
    let placeholders = std::iter::repeat_n("?", columns.len()).collect::<Vec<_>>().join(", ");
    let insert_sql = format!("INSERT INTO {} ({column_sql}) VALUES ({placeholders})", table.sql());
    let mut stmt = conn.prepare(&insert_sql).map_err(duck_error)?;

    for row in rows {
        let values = columns
            .iter()
            .map(|column| {
                let kind = column_types.get(column).copied().unwrap_or(ColumnKind::Text);
                duck_value_for_column(row.get(column).unwrap_or(&JsonValue::Null), kind)
            })
            .collect::<Vec<_>>();
        stmt.execute(params_from_iter(values.iter())).map_err(duck_error)?;
    }

    Ok(())
}

fn serialize_rows<T>(rows: &[T]) -> ResultEP<Vec<JsonMap<String, JsonValue>>>
where
    T: Serialize + Sync,
{
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        match serde_json::to_value(row).map_err(EpError::serde)? {
            JsonValue::Object(map) => out.push(map),
            other => return Err(EpError::serde(format!("analytics row must serialize to an object, got {other:?}"))),
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnKind {
    Bool,
    Int,
    UInt,
    Float,
    Timestamp,
    Text,
}

impl ColumnKind {
    fn sql_type(self) -> &'static str {
        match self {
            Self::Bool => "BOOLEAN",
            Self::Int => "BIGINT",
            Self::UInt => "UBIGINT",
            Self::Float => "DOUBLE",
            Self::Timestamp => "TIMESTAMP",
            Self::Text => "TEXT",
        }
    }
}

fn infer_columns(rows: &[JsonMap<String, JsonValue>]) -> BTreeMap<String, ColumnKind> {
    let mut columns = BTreeMap::new();
    for row in rows {
        for (name, value) in row {
            if !is_valid_identifier(name) {
                continue;
            }
            let next = infer_kind_for_column(name, value);
            columns.entry(name.clone()).and_modify(|kind| *kind = merge_kinds(*kind, next)).or_insert(next);
        }
    }
    columns
}

fn infer_kind_for_column(name: &str, value: &JsonValue) -> ColumnKind {
    if is_retention_timestamp_column(name) && (value.is_null() || timestamp_millis_from_json(value).is_some()) {
        return ColumnKind::Timestamp;
    }
    infer_kind(value)
}

fn infer_kind(value: &JsonValue) -> ColumnKind {
    match value {
        JsonValue::Null => ColumnKind::Text,
        JsonValue::Bool(_) => ColumnKind::Bool,
        JsonValue::Number(number) if number.is_f64() => ColumnKind::Float,
        JsonValue::Number(number) if number.as_u64().is_some() => ColumnKind::UInt,
        JsonValue::Number(_) => ColumnKind::Int,
        JsonValue::String(_) | JsonValue::Array(_) | JsonValue::Object(_) => ColumnKind::Text,
    }
}

fn merge_kinds(left: ColumnKind, right: ColumnKind) -> ColumnKind {
    match (left, right) {
        (ColumnKind::Text, ColumnKind::Timestamp) | (ColumnKind::Timestamp, ColumnKind::Text) => ColumnKind::Timestamp,
        (ColumnKind::Text, _) | (_, ColumnKind::Text) => ColumnKind::Text,
        (ColumnKind::Timestamp, ColumnKind::Timestamp) => ColumnKind::Timestamp,
        (ColumnKind::Timestamp, _) | (_, ColumnKind::Timestamp) => ColumnKind::Text,
        (ColumnKind::Float, _) | (_, ColumnKind::Float) => ColumnKind::Float,
        (ColumnKind::Int, ColumnKind::UInt) | (ColumnKind::UInt, ColumnKind::Int) => ColumnKind::Int,
        (ColumnKind::Bool, ColumnKind::Bool) => ColumnKind::Bool,
        (ColumnKind::Bool, _) | (_, ColumnKind::Bool) => ColumnKind::Text,
        (kind, _) => kind,
    }
}

fn duck_value_for_column(value: &JsonValue, kind: ColumnKind) -> DuckValue {
    if value.is_null() {
        return DuckValue::Null;
    }

    match kind {
        ColumnKind::Bool => value.as_bool().map(DuckValue::Boolean).unwrap_or_else(|| DuckValue::Text(json_to_text(value))),
        ColumnKind::Int => value
            .as_i64()
            .map(DuckValue::BigInt)
            .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()).map(DuckValue::BigInt))
            .unwrap_or_else(|| DuckValue::Text(json_to_text(value))),
        ColumnKind::UInt => value
            .as_u64()
            .map(DuckValue::UBigInt)
            .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()).map(DuckValue::UBigInt))
            .unwrap_or_else(|| DuckValue::Text(json_to_text(value))),
        ColumnKind::Float => value.as_f64().map(DuckValue::Double).unwrap_or_else(|| DuckValue::Text(json_to_text(value))),
        ColumnKind::Timestamp => timestamp_millis_from_json(value)
            .map(|millis| DuckValue::Timestamp(TimeUnit::Millisecond, millis))
            .unwrap_or_else(|| DuckValue::Text(json_to_text(value))),
        ColumnKind::Text => DuckValue::Text(json_to_text(value)),
    }
}

fn binding_value<T>(value: T) -> Result<DuckValue, String>
where
    T: Serialize,
{
    serde_json::to_value(value)
        .map(|value| duck_value_for_column(&value, infer_kind(&value)))
        .map_err(|err| format!("failed to serialize DuckDB query binding: {err}"))
}

fn json_to_text(value: &JsonValue) -> String {
    match value {
        JsonValue::String(value) => value.clone(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}

fn row_to_json_value(row: &duckdb::Row<'_>, column_names: &[String]) -> ResultEP<JsonValue> {
    let mut object = JsonMap::with_capacity(column_names.len());
    let mut positional = Vec::with_capacity(column_names.len());
    for (index, name) in column_names.iter().enumerate() {
        let value = value_ref_to_json(row.get_ref(index).map_err(duck_error)?);
        positional.push(value.clone());
        object.insert(name.clone(), value);
    }
    if column_names.len() == 1 && column_names.first().map(|name| name.is_empty()).unwrap_or(false) {
        return Ok(positional.into_iter().next().unwrap_or(JsonValue::Null));
    }
    Ok(JsonValue::Object(object))
}

fn deserialize_row<T>(value: JsonValue) -> ResultEP<T>
where
    T: DeserializeOwned,
{
    match serde_json::from_value(value.clone()) {
        Ok(row) => Ok(row),
        Err(object_err) => {
            if let JsonValue::Object(map) = &value
                && map.len() == 1
                && let Some(single_value) = map.values().next()
                && let Ok(row) = serde_json::from_value(single_value.clone())
            {
                return Ok(row);
            }
            let array = match value {
                JsonValue::Object(map) => JsonValue::Array(map.into_values().collect()),
                other => other,
            };
            serde_json::from_value(array).map_err(|array_err| {
                EpError::serde(format!("failed to deserialize DuckDB row by name ({object_err}) or position ({array_err})"))
            })
        }
    }
}

fn value_ref_to_json(value: ValueRef<'_>) -> JsonValue {
    match value {
        ValueRef::Null => JsonValue::Null,
        ValueRef::Boolean(value) => JsonValue::Bool(value),
        ValueRef::TinyInt(value) => JsonValue::Number(JsonNumber::from(value)),
        ValueRef::SmallInt(value) => JsonValue::Number(JsonNumber::from(value)),
        ValueRef::Int(value) => JsonValue::Number(JsonNumber::from(value)),
        ValueRef::BigInt(value) => JsonValue::Number(JsonNumber::from(value)),
        ValueRef::HugeInt(value) => i64::try_from(value)
            .map(JsonNumber::from)
            .map(JsonValue::Number)
            .unwrap_or_else(|_| JsonValue::String(value.to_string())),
        ValueRef::UTinyInt(value) => JsonValue::Number(JsonNumber::from(value)),
        ValueRef::USmallInt(value) => JsonValue::Number(JsonNumber::from(value)),
        ValueRef::UInt(value) => JsonValue::Number(JsonNumber::from(value)),
        ValueRef::UBigInt(value) => JsonValue::Number(JsonNumber::from(value)),
        ValueRef::Float(value) => JsonNumber::from_f64(f64::from(value)).map(JsonValue::Number).unwrap_or(JsonValue::Null),
        ValueRef::Double(value) => JsonNumber::from_f64(value).map(JsonValue::Number).unwrap_or(JsonValue::Null),
        ValueRef::Decimal(value) => JsonValue::String(value.to_string()),
        ValueRef::Timestamp(unit, value) => JsonValue::Number(JsonNumber::from(timestamp_to_millis(unit, value))),
        ValueRef::Text(value) => text_to_json(value),
        ValueRef::Blob(value) => JsonValue::String(hex::encode(value)),
        ValueRef::Date32(value) => JsonValue::Number(JsonNumber::from(value)),
        ValueRef::Time64(_, value) => JsonValue::Number(JsonNumber::from(value)),
        ValueRef::Interval { months, days, nanos } => {
            let mut object = JsonMap::new();
            object.insert("months".to_string(), JsonValue::Number(JsonNumber::from(months)));
            object.insert("days".to_string(), JsonValue::Number(JsonNumber::from(days)));
            object.insert("nanos".to_string(), JsonValue::Number(JsonNumber::from(nanos)));
            JsonValue::Object(object)
        }
        ValueRef::List(_, _)
        | ValueRef::Enum(_, _)
        | ValueRef::Struct(_, _)
        | ValueRef::Array(_, _)
        | ValueRef::Map(_, _)
        | ValueRef::Union(_, _) => JsonValue::String(format!("{value:?}")),
    }
}

fn timestamp_to_millis(unit: TimeUnit, value: i64) -> i64 {
    match unit {
        TimeUnit::Second => value.saturating_mul(1_000),
        TimeUnit::Millisecond => value,
        TimeUnit::Microsecond => value / 1_000,
        TimeUnit::Nanosecond => value / 1_000_000,
    }
}

fn text_to_json(value: &[u8]) -> JsonValue {
    match std::str::from_utf8(value) {
        Ok(text) => {
            let trimmed = text.trim_start();
            if trimmed.starts_with('[') || trimmed.starts_with('{') {
                serde_json::from_str(text).unwrap_or_else(|_| JsonValue::String(text.to_string()))
            } else {
                JsonValue::String(text.to_string())
            }
        }
        Err(_) => JsonValue::String(hex::encode(value)),
    }
}

#[derive(Debug, Clone)]
struct QualifiedTable {
    schema: String,
    table: String,
}

impl QualifiedTable {
    fn sql(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.table))
    }
}

fn parse_table_name(table: &str) -> ResultEP<QualifiedTable> {
    let parts = table.split('.').collect::<Vec<_>>();
    let (schema, table) = match parts.as_slice() {
        [table] => (ANALYTICS_SCHEMA, *table),
        [schema, table] => (*schema, *table),
        _ => return Err(EpError::database(format!("invalid DuckDB analytics table name: {table}"))),
    };

    if !is_valid_identifier(schema) || !is_valid_identifier(table) {
        return Err(EpError::database(format!("invalid DuckDB analytics table identifier: {table}")));
    }

    Ok(QualifiedTable { schema: schema.to_string(), table: table.to_string() })
}

fn is_valid_identifier(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn list_tables(conn: &Connection) -> ResultEP<BTreeSet<String>> {
    let mut stmt = conn.prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'analytics'").map_err(duck_error)?;
    let mut rows = stmt.query([]).map_err(duck_error)?;
    let mut tables = BTreeSet::new();
    while let Some(row) = rows.next().map_err(duck_error)? {
        let table: String = row.get(0).map_err(duck_error)?;
        tables.insert(table);
    }
    Ok(tables)
}

fn retention_columns(conn: &Connection, table: &str) -> ResultEP<Vec<String>> {
    let qualified = parse_table_name(table)?;
    let query = "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?";
    let mut stmt = conn.prepare(query).map_err(duck_error)?;
    let mut rows = stmt.query([qualified.schema.as_str(), qualified.table.as_str()]).map_err(duck_error)?;
    let mut columns = Vec::new();
    while let Some(row) = rows.next().map_err(duck_error)? {
        let column: String = row.get(0).map_err(duck_error)?;
        if matches!(
            column.as_str(),
            "timestamp" | "event_time" | "detected_at" | "window_start" | "snapshot_time" | "sample_timestamp" | "started_at"
        ) {
            columns.push(column);
        }
    }
    Ok(columns)
}

fn prune_table(conn: &Connection, table: &str, column: &str, retention_days: u32) -> ResultEP<()> {
    if retention_days == 0 || !is_valid_identifier(column) {
        return Ok(());
    }
    let qualified = parse_table_name(table)?;
    let cutoff = Utc::now() - chrono::Duration::days(i64::from(retention_days));
    let cutoff_millis = cutoff.timestamp_millis();
    let sql = format!("DELETE FROM {} WHERE {} < ?", qualified.sql(), quote_ident(column));
    conn.execute(&sql, [DuckValue::Timestamp(TimeUnit::Millisecond, cutoff_millis)]).map_err(duck_error)?;
    Ok(())
}

fn translate_clickhouse_sql(sql: &str) -> String {
    let mut translated = sql.to_string();
    translated = replace_regex(&translated, r"(?i)\s+SETTINGS\s+use_query_cache\s*=\s*[01]", |_| String::new());
    translated = replace_regex(&translated, r"(?i)\s+LIMIT\s+1\s+BY\s+.*?(?=\s+LIMIT\s+\d+\s+OFFSET)", |_| String::new());
    translated = replace_regex(&translated, r"(?i)toDateTime64\('([^']*)',\s*3,\s*'UTC'\)", |captures| {
        format!("TIMESTAMP '{}'", escape_sql_string(&captures[1]))
    });
    translated = replace_regex(
        &translated,
        r"(?i)toUInt32\(toUnixTimestamp\(toStartOfInterval\(([^,]+),\s*INTERVAL\s+(\d+)\s+SECOND\)\)\)",
        |captures| format!("CAST(epoch(time_bucket(INTERVAL '{} seconds', {})) AS UINTEGER)", &captures[2], &captures[1]),
    );
    translated = replace_regex(
        &translated,
        r"(?i)toString\(toStartOfInterval\(([^,]+),\s*INTERVAL\s+(\d+)\s+second\)\)",
        |captures| format!("CAST(time_bucket(INTERVAL '{} seconds', {}) AS VARCHAR)", &captures[2], &captures[1]),
    );
    translated = replace_regex(&translated, r"(?i)toStartOfInterval\(([^,]+),\s*INTERVAL\s+(\d+)\s+second\)", |captures| {
        format!("time_bucket(INTERVAL '{} seconds', {})", &captures[2], &captures[1])
    });
    translated = replace_regex(&translated, r"(?i)toUnixTimestamp64Milli\(([^)]+)\)", |captures| {
        format!("CAST(epoch({}) * 1000 AS BIGINT)", &captures[1])
    });
    translated = replace_regex(&translated, r"(?i)toUnixTimestamp\(([^)]+)\)", |captures| {
        format!("CAST(epoch({}) AS BIGINT)", &captures[1])
    });
    translated = replace_regex(&translated, r"(?i)toUInt64\(sum\(arraySum\(([^)]+)\)\)\)", |_| "CAST(0 AS UBIGINT)".to_string());
    translated = replace_regex(&translated, r"(?i)toUInt64\(sum\(([^)]+)\)\)", |captures| {
        format!("CAST(sum({}) AS UBIGINT)", &captures[1])
    });
    translated = replace_regex(&translated, r"(?i)toUInt64\(0\)", |_| "CAST(0 AS UBIGINT)".to_string());
    translated = replace_regex(&translated, r"(?i)toUInt64\(ifNull\(argMax\(([^,]+),\s*([^)]+)\),\s*0\)\)", |captures| {
        format!("CAST(coalesce(arg_max({}, {}), 0) AS UBIGINT)", &captures[1], &captures[2])
    });
    translated = replace_regex(&translated, r"(?i)toValidUTF8\(argMax\(([^,]+),\s*([^)]+)\)\)", |captures| {
        format!("arg_max({}, {})", &captures[1], &captures[2])
    });
    translated = replace_regex(&translated, r"(?i)argMax\(([^,]+),\s*([^)]+)\)", |captures| {
        format!("arg_max({}, {})", &captures[1], &captures[2])
    });
    translated = replace_regex(&translated, r"(?i)lagInFrame\(", |_| "lag(".to_string());
    translated = replace_regex(&translated, r"(?i)sumForEach\(([^)]+)\)", |captures| format!("first({})", &captures[1]));
    translated = replace_regex(&translated, r"(?i)any\(([^)]+)\)", |captures| format!("first({})", &captures[1]));
    translated = replace_regex(&translated, r"(?i)uniqExact\(([^)]+)\)", |captures| format!("count(DISTINCT {})", &captures[1]));
    translated = replace_regex(&translated, r"(?i)ifNull\(", |_| "coalesce(".to_string());
    translated = replace_regex(&translated, r"(?i)startsWith\(", |_| "starts_with(".to_string());
    translated = replace_regex(&translated, r"(?i)substring\(([^,]+),\s*([0-9]+)\)", |captures| {
        format!("substr({}, {})", &captures[1], &captures[2])
    });
    translated = replace_regex(&translated, r"(?i)positionCaseInsensitive\(([^,]+),\s*'([^']*)'\)\s*>\s*0", |captures| {
        format!("contains(lower({}), lower('{}'))", &captures[1], escape_sql_string(&captures[2]))
    });
    translated = replace_regex(&translated, r"([A-Za-z_][A-Za-z0-9_]*)\['([^']+)'\]\s*=\s*'([^']*)'", |captures| {
        let pair = format!("[\"{}\",\"{}\"]", escape_json_string(&captures[2]), escape_json_string(&captures[3]));
        format!("contains({}, '{}')", &captures[1], escape_sql_string(&pair))
    });
    translated = translated.replace("count()", "count(*)");
    translated
}

fn duck_error(error: duckdb::Error) -> EpError {
    EpError::database(format!("DuckDB analytics error: {error}"))
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn escape_json_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn replace_regex(sql: &str, pattern: &str, replacement: impl Fn(&Captures<'_>) -> String) -> String {
    match Regex::new(pattern) {
        Ok(regex) => regex.replace_all(sql, replacement).into_owned(),
        Err(_) => sql.to_string(),
    }
}

fn timestamp_millis_from_json(value: &JsonValue) -> Option<i64> {
    if let Some(millis) = value.as_i64() {
        return Some(millis);
    }

    let text = value.as_str()?;
    DateTime::parse_from_rfc3339(text)
        .map(|timestamp| timestamp.timestamp_millis())
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.3f").map(|timestamp| timestamp.and_utc().timestamp_millis())
        })
        .ok()
}

fn is_retention_timestamp_column(column: &str) -> bool {
    matches!(
        column,
        "timestamp"
            | "event_time"
            | "detected_at"
            | "window_start"
            | "snapshot_time"
            | "sample_timestamp"
            | "started_at"
            | "updated_at"
            | "start_time"
            | "end_time"
            | "created_at"
    )
}

#[allow(dead_code)]
fn millis_to_rfc3339(millis: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(millis).map(|value| value.to_rfc3339()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    #[derive(Debug, Serialize)]
    struct TestAnalyticsRow {
        organization_uuid: String,
        request_count: u64,
        labels: JsonValue,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct TestAnalyticsReadRow {
        organization_uuid: String,
        request_count: u64,
        labels: JsonValue,
    }

    #[derive(Debug, Serialize)]
    struct TestMetricRow {
        timestamp: DateTime<Utc>,
        organization_uuid: String,
        service_name: String,
        node_uuid: String,
        metric_name: String,
        metric_kind: String,
        value: Option<f64>,
        count: Option<u64>,
        sum: Option<f64>,
        bucket_bounds: Vec<f64>,
        bucket_counts: Vec<u64>,
        labels: Vec<(String, String)>,
        scope: String,
    }

    #[derive(Debug, Deserialize)]
    struct TestMetricExportRow {
        metric_group: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
        timestamp: DateTime<Utc>,
        organization_uuid: String,
        labels: Vec<(String, String)>,
    }

    #[derive(Debug, Serialize)]
    struct TestTraceRow {
        timestamp: DateTime<Utc>,
        organization_uuid: String,
        service_name: String,
        node_uuid: String,
        trace_id: String,
        span_id: String,
        parent_span_id: String,
        span_name: String,
        span_kind: String,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        duration_ns: u64,
        status: String,
        status_message: String,
        attributes: Vec<(String, String)>,
        events_json: String,
    }

    #[derive(Debug, Deserialize)]
    struct TestTraceExportRow {
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
        start_time: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
        end_time: DateTime<Utc>,
        attributes: Vec<(String, String)>,
    }

    #[derive(Debug, Serialize)]
    struct TestLogRow {
        timestamp: DateTime<Utc>,
        service_name: String,
        node_uuid: String,
        level: String,
        audience: String,
        message: String,
        trace_id: String,
        span_id: String,
        feature: String,
        function: String,
        file: String,
        line: Option<u32>,
        eden_node_uuid: String,
        organization_uuid: String,
        organization_id: String,
        user_uuid: String,
        user_id: String,
        endpoint_uuid: String,
        endpoint_id: String,
        endpoint_kind: String,
        error_code: String,
        error_category: String,
        labels: Vec<(String, String)>,
    }

    #[derive(Debug, Deserialize)]
    struct TestLogExportRow {
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
        timestamp: DateTime<Utc>,
        message: String,
        labels: Vec<(String, String)>,
    }

    #[derive(Debug, Deserialize)]
    struct TestCountRow {
        total: u64,
    }

    fn test_config(name: &str) -> DuckDbAnalyticsConfig {
        let base = std::env::temp_dir().join(format!("eden-duckdb-analytics-{name}-{}.duckdb", Uuid::new_v4()));
        DuckDbAnalyticsConfig {
            path: base.clone(),
            memory_limit: "512MB".to_string(),
            temp_directory: base.with_extension("tmp"),
            max_temp_directory_size: "2GB".to_string(),
            checkpoint_threshold: "64MB".to_string(),
            checkpoint_interval_secs: 60,
            analytics_retention_days: 30,
            logs_retention_days: 14,
            traces_retention_days: 14,
        }
    }

    async fn cleanup(config: &DuckDbAnalyticsConfig) {
        let _ = tokio::fs::remove_file(&config.path).await;
        let _ = tokio::fs::remove_dir_all(&config.temp_directory).await;
    }

    #[tokio::test]
    async fn insert_query_round_trips_rows_and_scalars() -> ResultEP<()> {
        let config = test_config("round-trip");
        cleanup(&config).await;

        let store = DuckDbAnalyticsStore::new(config.clone()).await?;
        let rows = vec![
            TestAnalyticsRow {
                organization_uuid: "org-a".to_string(),
                request_count: 2,
                labels: serde_json::json!({ "route": "/v1/a" }),
            },
            TestAnalyticsRow {
                organization_uuid: "org-b".to_string(),
                request_count: 7,
                labels: serde_json::json!({ "route": "/v1/b" }),
            },
        ];

        store.insert_rows("analytics.test_rows", &rows).await?;

        let count = store.query("SELECT count() FROM analytics.test_rows").fetch_one::<u64>().await?;
        assert_eq!(count, 2);

        let fetched = store
            .query("SELECT organization_uuid, request_count, labels FROM analytics.test_rows ORDER BY request_count")
            .fetch_all::<TestAnalyticsReadRow>()
            .await?;
        assert_eq!(
            fetched,
            vec![
                TestAnalyticsReadRow {
                    organization_uuid: "org-a".to_string(),
                    request_count: 2,
                    labels: serde_json::json!({ "route": "/v1/a" }),
                },
                TestAnalyticsReadRow {
                    organization_uuid: "org-b".to_string(),
                    request_count: 7,
                    labels: serde_json::json!({ "route": "/v1/b" }),
                },
            ]
        );

        cleanup(&config).await;
        Ok(())
    }

    #[tokio::test]
    async fn file_backed_store_persists_rows_after_restart() -> ResultEP<()> {
        let config = test_config("persist");
        cleanup(&config).await;

        {
            let store = DuckDbAnalyticsStore::new(config.clone()).await?;
            store
                .insert_rows(
                    "analytics.persist_rows",
                    &[TestAnalyticsRow {
                        organization_uuid: "org-a".to_string(),
                        request_count: 11,
                        labels: serde_json::json!({ "source": "before-restart" }),
                    }],
                )
                .await?;
            store.checkpoint().await?;
        }

        let reopened = DuckDbAnalyticsStore::new(config.clone()).await?;
        let count = reopened.query("SELECT count() FROM analytics.persist_rows").fetch_one::<u64>().await?;
        assert_eq!(count, 1);

        cleanup(&config).await;
        Ok(())
    }

    #[tokio::test]
    async fn telemetry_export_sql_runs_on_duckdb() -> ResultEP<()> {
        let config = test_config("telemetry-export");
        cleanup(&config).await;

        let store = DuckDbAnalyticsStore::new(config.clone()).await?;
        let timestamp = DateTime::parse_from_rfc3339("2026-06-04T12:00:00.000Z").map_err(EpError::parse)?.with_timezone(&Utc);
        store
            .insert_rows(
                "analytics.eden",
                &[TestMetricRow {
                    timestamp,
                    organization_uuid: "org-a".to_string(),
                    service_name: "eden-service".to_string(),
                    node_uuid: "node-a".to_string(),
                    metric_name: "eden.requests".to_string(),
                    metric_kind: "Counter".to_string(),
                    value: Some(42.0),
                    count: None,
                    sum: None,
                    bucket_bounds: Vec::new(),
                    bucket_counts: Vec::new(),
                    labels: vec![("endpoint_uuid".to_string(), "endpoint-a".to_string())],
                    scope: "endpoint".to_string(),
                }],
            )
            .await?;

        let rows = store
            .query(
                r#"
                SELECT
                    'eden' AS metric_group,
                    timestamp,
                    organization_uuid,
                    labels
                FROM analytics.eden
                WHERE timestamp >= toDateTime64('2026-06-04 11:00:00.000', 3, 'UTC')
                  AND timestamp <= toDateTime64('2026-06-04 13:00:00.000', 3, 'UTC')
                  AND organization_uuid = 'org-a'
                  AND labels['endpoint_uuid'] = 'endpoint-a'
                ORDER BY timestamp ASC
                "#,
            )
            .fetch_all::<TestMetricExportRow>()
            .await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].metric_group, "eden");
        assert_eq!(rows[0].timestamp, timestamp);
        assert_eq!(rows[0].organization_uuid, "org-a");
        assert_eq!(rows[0].labels, vec![("endpoint_uuid".to_string(), "endpoint-a".to_string())]);

        let count = store
            .query("SELECT toUInt64(sum(row_count)) AS total FROM (SELECT count() AS row_count FROM analytics.eden)")
            .fetch_one::<TestCountRow>()
            .await?;
        assert_eq!(count.total, 1);

        cleanup(&config).await;
        Ok(())
    }

    #[tokio::test]
    async fn telemetry_trace_and_log_export_sql_runs_on_duckdb() -> ResultEP<()> {
        let config = test_config("telemetry-traces-logs");
        cleanup(&config).await;

        let store = DuckDbAnalyticsStore::new(config.clone()).await?;
        let timestamp = DateTime::parse_from_rfc3339("2026-06-04T12:30:00.000Z").map_err(EpError::parse)?.with_timezone(&Utc);
        store
            .insert_rows(
                "analytics.traces",
                &[TestTraceRow {
                    timestamp,
                    organization_uuid: "org-a".to_string(),
                    service_name: "eden-service".to_string(),
                    node_uuid: "node-a".to_string(),
                    trace_id: "trace-a".to_string(),
                    span_id: "span-a".to_string(),
                    parent_span_id: String::new(),
                    span_name: "request".to_string(),
                    span_kind: "server".to_string(),
                    start_time: timestamp,
                    end_time: timestamp + chrono::Duration::milliseconds(5),
                    duration_ns: 5_000_000,
                    status: "ok".to_string(),
                    status_message: String::new(),
                    attributes: vec![("route".to_string(), "/api/v1/analytics/telemetry".to_string())],
                    events_json: "[]".to_string(),
                }],
            )
            .await?;
        store
            .insert_rows(
                "analytics.logs",
                &[TestLogRow {
                    timestamp,
                    service_name: "eden-service".to_string(),
                    node_uuid: "node-a".to_string(),
                    level: "ERROR".to_string(),
                    audience: "internal".to_string(),
                    message: "Export Error captured".to_string(),
                    trace_id: "trace-a".to_string(),
                    span_id: "span-a".to_string(),
                    feature: "telemetry".to_string(),
                    function: "export".to_string(),
                    file: "telemetry_analytics.rs".to_string(),
                    line: Some(42),
                    eden_node_uuid: "node-a".to_string(),
                    organization_uuid: "org-a".to_string(),
                    organization_id: "org-a".to_string(),
                    user_uuid: "user-a".to_string(),
                    user_id: "user-a".to_string(),
                    endpoint_uuid: "endpoint-a".to_string(),
                    endpoint_id: "endpoint-a".to_string(),
                    endpoint_kind: "redis".to_string(),
                    error_code: "E_TEST".to_string(),
                    error_category: "test".to_string(),
                    labels: vec![("shard_id".to_string(), "0".to_string())],
                }],
            )
            .await?;

        let traces = store
            .query(
                r#"
                SELECT timestamp, start_time, end_time, attributes
                FROM analytics.traces
                WHERE timestamp >= toDateTime64('2026-06-04 12:00:00.000', 3, 'UTC')
                  AND timestamp <= toDateTime64('2026-06-04 13:00:00.000', 3, 'UTC')
                  AND organization_uuid = 'org-a'
                  AND attributes['route'] = '/api/v1/analytics/telemetry'
                ORDER BY timestamp DESC
                "#,
            )
            .fetch_all::<TestTraceExportRow>()
            .await?;
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].timestamp, timestamp);
        assert_eq!(traces[0].start_time, timestamp);
        assert_eq!(traces[0].end_time, timestamp + chrono::Duration::milliseconds(5));
        assert_eq!(traces[0].attributes, vec![("route".to_string(), "/api/v1/analytics/telemetry".to_string())]);

        let logs = store
            .query(
                r#"
                SELECT timestamp, message, labels
                FROM analytics.logs
                WHERE timestamp >= toDateTime64('2026-06-04 12:00:00.000', 3, 'UTC')
                  AND timestamp <= toDateTime64('2026-06-04 13:00:00.000', 3, 'UTC')
                  AND organization_uuid = 'org-a'
                  AND positionCaseInsensitive(message, 'error') > 0
                  AND labels['shard_id'] = '0'
                ORDER BY timestamp DESC
                "#,
            )
            .fetch_all::<TestLogExportRow>()
            .await?;
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].timestamp, timestamp);
        assert_eq!(logs[0].message, "Export Error captured");
        assert_eq!(logs[0].labels, vec![("shard_id".to_string(), "0".to_string())]);

        cleanup(&config).await;
        Ok(())
    }
}
