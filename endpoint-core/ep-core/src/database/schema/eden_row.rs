//! Row wrapper for Turso that provides a `try_get` interface compatible with
//! `tokio_postgres::Row`.
//!
//! This module is only compiled when the `embedded-db` feature is active.
//! It allows all existing `FromRow` implementations to work unchanged:
//! they call `row.try_get("column_name")` or `row.try_get::<_, T>("col")`
//! and this wrapper routes to the eagerly extracted column data.
//!
//! **Important**: Column values are eagerly extracted from `turso::Row` into
//! owned `turso::Value`s at construction time. This is necessary because
//! `turso::Row` does not own column names, and result-set metadata lives on
//! `turso::Rows` / `turso::Statement`. By extracting values eagerly, our `Row`
//! is fully self-contained and can be stored in a `Vec`, returned from
//! functions, etc.

use chrono::{DateTime, NaiveDateTime, Utc};
use postgres_types::{FromSqlOwned, Json};

/// Self-contained row that owns all column names and values.
///
/// Constructed from Turso result metadata plus a `turso::Row`.
pub struct Row {
    inner: RowInner,
}

enum RowInner {
    Postgres(tokio_postgres::Row),
    Turso(TursoRow),
}

struct TursoRow {
    columns: Vec<(String, turso::Value)>,
}

impl Row {
    /// Eagerly extract all column names and values from a `turso::Row`.
    pub fn new(column_names: &[String], row: turso::Row) -> Self {
        let count = row.column_count();
        let mut columns = Vec::with_capacity(count);
        for i in 0..count {
            let name = column_names.get(i).cloned().unwrap_or_default();
            let value = row.get_value(i).unwrap_or(turso::Value::Null);
            columns.push((name, value));
        }
        Self { inner: RowInner::Turso(TursoRow { columns }) }
    }

    /// Get a column value by name or index, matching `tokio_postgres::Row::try_get`.
    pub fn try_get<I, T>(&self, idx: I) -> Result<T, Box<dyn std::error::Error + Sync + Send>>
    where
        I: RowIndex,
        T: FromTursoColumn + FromPostgresColumn,
    {
        match &self.inner {
            RowInner::Postgres(row) => idx.try_get_postgres::<T>(row),
            RowInner::Turso(row) => {
                let col_idx = idx.resolve_turso(&row.columns)?;
                let value = &row.columns[col_idx].1;
                T::from_value(value.clone()).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)
            }
        }
    }

    /// Get a column value by name or index, panicking on failure.
    /// Mirrors the `tokio_postgres::Row::get()` API contract.
    #[allow(clippy::unwrap_used)]
    pub fn get<I, T>(&self, idx: I) -> T
    where
        I: RowIndex,
        T: FromTursoColumn + FromPostgresColumn,
    {
        self.try_get(idx).expect("failed to get column value from row")
    }

    /// Get a JSON-backed column and deserialize it into `T`.
    pub fn try_get_json<I, T>(&self, idx: I) -> Result<T, Box<dyn std::error::Error + Sync + Send>>
    where
        I: RowIndex,
        T: serde::de::DeserializeOwned,
    {
        match &self.inner {
            RowInner::Postgres(row) => idx.try_get_postgres_json(row),
            RowInner::Turso(row) => {
                let col_idx = idx.resolve_turso(&row.columns)?;
                let value = &row.columns[col_idx].1;
                from_turso_value(value.clone()).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)
            }
        }
    }

    /// Returns the number of columns in the row.
    pub fn len(&self) -> usize {
        match &self.inner {
            RowInner::Postgres(row) => row.len(),
            RowInner::Turso(row) => row.columns.len(),
        }
    }

    /// Returns `true` if the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the column names in this row.
    pub fn columns(&self) -> Vec<String> {
        match &self.inner {
            RowInner::Postgres(row) => row.columns().iter().map(|column| column.name().to_string()).collect(),
            RowInner::Turso(row) => row.columns.iter().map(|(name, _)| name.clone()).collect(),
        }
    }
}

impl From<tokio_postgres::Row> for Row {
    fn from(row: tokio_postgres::Row) -> Self {
        Self { inner: RowInner::Postgres(row) }
    }
}

/// Error type for row access failures.
#[derive(Debug)]
pub struct RowError {
    message: String,
}

impl RowError {
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}

impl std::fmt::Display for RowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for RowError {}

/// Trait for types that can be extracted from a Turso column value.
///
/// A blanket implementation is provided for any `T: serde::de::DeserializeOwned`.
pub trait FromTursoColumn: Sized + FromSqlOwned + serde::de::DeserializeOwned {
    fn from_value(value: turso::Value) -> Result<Self, RowError>;
}

impl<T> FromTursoColumn for T
where
    T: FromSqlOwned + serde::de::DeserializeOwned,
{
    fn from_value(value: turso::Value) -> Result<Self, RowError> {
        from_turso_value(value)
    }
}

#[doc(hidden)]
pub trait FromPostgresColumn: Sized {
    fn from_named(row: &tokio_postgres::Row, idx: &str) -> Result<Self, Box<dyn std::error::Error + Sync + Send>>;
    fn from_index(row: &tokio_postgres::Row, idx: usize) -> Result<Self, Box<dyn std::error::Error + Sync + Send>>;
}

impl<T> FromPostgresColumn for T
where
    T: FromSqlOwned + serde::de::DeserializeOwned,
{
    fn from_named(row: &tokio_postgres::Row, idx: &str) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        row.try_get(idx).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)
    }

    fn from_index(row: &tokio_postgres::Row, idx: usize) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        row.try_get(idx).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)
    }
}

/// Convert a `turso::Value` to any DeserializeOwned type via serde.
fn from_turso_value<T: serde::de::DeserializeOwned>(value: turso::Value) -> Result<T, RowError> {
    match value {
        turso::Value::Null => {
            serde_json::from_value(serde_json::Value::Null).map_err(|e| RowError { message: format!("cannot deserialize null: {e}") })
        }
        turso::Value::Text(s) => {
            if let Some(result) = try_datetime_parse::<T>(&s) {
                return result;
            }
            let json_str = if s.starts_with('[') {
                if let Ok(serde_json::Value::Array(arr)) = serde_json::from_str::<serde_json::Value>(&s) {
                    let filtered: Vec<_> = arr.into_iter().filter(|v| !v.is_null()).collect();
                    std::borrow::Cow::Owned(serde_json::to_string(&filtered).unwrap_or_else(|_| s.clone()))
                } else {
                    std::borrow::Cow::Borrowed(s.as_str())
                }
            } else {
                std::borrow::Cow::Borrowed(s.as_str())
            };
            if let Ok(val) = serde_json::from_str::<T>(&json_str) {
                return Ok(val);
            }
            let json_string_val = serde_json::Value::String(s.clone());
            let quoted = serde_json::to_string(&json_string_val).unwrap_or_else(|_| format!("\"{}\"", s));
            serde_json::from_str::<T>(&quoted).map_err(|e| RowError { message: format!("cannot deserialize text '{s}': {e}") })
        }
        turso::Value::Integer(i) => {
            if let Ok(val) = serde_json::from_value::<T>(serde_json::Value::Number(i.into())) {
                return Ok(val);
            }
            let bool_val = if i == 0 {
                serde_json::Value::Bool(false)
            } else {
                serde_json::Value::Bool(true)
            };
            serde_json::from_value::<T>(bool_val).map_err(|e| RowError { message: format!("cannot deserialize integer {i}: {e}") })
        }
        turso::Value::Real(f) => {
            serde_json::from_value(serde_json::json!(f)).map_err(|e| RowError { message: format!("cannot deserialize real {f}: {e}") })
        }
        turso::Value::Blob(b) => {
            if let Ok(val) = serde_json::from_value::<T>(serde_json::json!(b)) {
                return Ok(val);
            }
            serde_json::from_slice(&b).map_err(|e| RowError { message: format!("cannot deserialize blob: {e}") })
        }
    }
}

/// Try to parse a text value as a chrono DateTime.
fn try_datetime_parse<T: serde::de::DeserializeOwned>(s: &str) -> Option<Result<T, RowError>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        let utc: DateTime<Utc> = dt.with_timezone(&Utc);
        if let Ok(val) = serde_json::from_value::<T>(serde_json::json!(utc.to_rfc3339())) {
            return Some(Ok(val));
        }
    }
    for fmt in &[
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
    ] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            let utc = dt.and_utc();
            if let Ok(val) = serde_json::from_value::<T>(serde_json::json!(utc.to_rfc3339())) {
                return Some(Ok(val));
            }
        }
    }
    None
}

/// Trait for resolving a column index from either a name or numeric index.
#[doc(hidden)]
pub trait RowIndex {
    fn resolve_turso(self, columns: &[(String, turso::Value)]) -> Result<usize, Box<dyn std::error::Error + Sync + Send>>;
    fn try_get_postgres<T>(self, row: &tokio_postgres::Row) -> Result<T, Box<dyn std::error::Error + Sync + Send>>
    where
        T: FromPostgresColumn;
    fn try_get_postgres_json<T>(self, row: &tokio_postgres::Row) -> Result<T, Box<dyn std::error::Error + Sync + Send>>
    where
        T: serde::de::DeserializeOwned;
}

impl RowIndex for &str {
    fn resolve_turso(self, columns: &[(String, turso::Value)]) -> Result<usize, Box<dyn std::error::Error + Sync + Send>> {
        for (i, (name, _)) in columns.iter().enumerate() {
            if name == self {
                return Ok(i);
            }
        }
        Err(Box::new(RowError { message: format!("column not found: {self}") }) as Box<dyn std::error::Error + Sync + Send>)
    }

    fn try_get_postgres<T>(self, row: &tokio_postgres::Row) -> Result<T, Box<dyn std::error::Error + Sync + Send>>
    where
        T: FromPostgresColumn,
    {
        T::from_named(row, self)
    }

    fn try_get_postgres_json<T>(self, row: &tokio_postgres::Row) -> Result<T, Box<dyn std::error::Error + Sync + Send>>
    where
        T: serde::de::DeserializeOwned,
    {
        row.try_get::<_, Json<T>>(self)
            .map(|value| value.0)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)
    }
}

impl RowIndex for usize {
    fn resolve_turso(self, columns: &[(String, turso::Value)]) -> Result<usize, Box<dyn std::error::Error + Sync + Send>> {
        if self < columns.len() {
            Ok(self)
        } else {
            Err(Box::new(RowError {
                message: format!("column index {self} out of range (len={})", columns.len()),
            }) as Box<dyn std::error::Error + Sync + Send>)
        }
    }

    fn try_get_postgres<T>(self, row: &tokio_postgres::Row) -> Result<T, Box<dyn std::error::Error + Sync + Send>>
    where
        T: FromPostgresColumn,
    {
        T::from_index(row, self)
    }

    fn try_get_postgres_json<T>(self, row: &tokio_postgres::Row) -> Result<T, Box<dyn std::error::Error + Sync + Send>>
    where
        T: serde::de::DeserializeOwned,
    {
        row.try_get::<_, Json<T>>(self)
            .map(|value| value.0)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)
    }
}
