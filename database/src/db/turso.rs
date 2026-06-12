//! Turso connection wrapper for the `embedded-db` feature.
//!
//! Provides `TursoConnection` and `TursoPool` types that implement the same
//! method surface as `tokio_postgres::Client` and `bb8::Pool<PostgresConnectionManager>`,
//! so that existing call sites in the database crate work unchanged.

use eden_core::error::{EpError, ResultEP};
use ep_core::database::schema::Row;
use regex::{Captures, Regex};
use std::sync::Arc;
use std::sync::OnceLock;

type SqlParam<'a> = dyn tokio_postgres::types::ToSql + Sync + 'a;

/// Wraps a `turso::Database` and serves as the pool-equivalent for Turso.
///
/// Since Turso is in-process, there's no need for a real connection pool.
/// `connect()` creates lightweight connection handles.
#[derive(Clone)]
pub struct TursoPool {
    db: Arc<turso::Database>,
}

impl TursoPool {
    pub fn new(db: turso::Database) -> Self {
        Self { db: Arc::new(db) }
    }

    pub fn connect(&self) -> ResultEP<TursoConnection> {
        let conn = self.db.connect().map_err(|e| EpError::database(format!("Failed to connect to Turso: {e}")))?;
        Ok(TursoConnection { conn })
    }
}

/// Wraps a `turso::Connection` and exposes methods matching `tokio_postgres::Client`.
pub struct TursoConnection {
    conn: turso::Connection,
}

async fn query_prepared_rows(conn: &turso::Connection, sql: &str, params: &[&SqlParam<'_>]) -> Result<(Vec<String>, turso::Rows), EpError> {
    let rewritten = rewrite_pg_sql(sql);
    let turso_params = convert_params(params)?;
    let mut stmt = conn.prepare(&rewritten).await.map_err(|e| EpError::database(format!("prepare failed: {e}")))?;
    let column_names = stmt.column_names();
    let rows = stmt.query(turso_params).await.map_err(|e| EpError::database(format!("query failed: {e}")))?;
    Ok((column_names, rows))
}

fn is_effectively_empty_sql(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            b' ' | b'\t' | b'\r' | b'\n' => {
                i += 1;
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                i += 2;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
            }
            _ => return false,
        }
    }

    true
}

fn split_sql_statements(sql: &str) -> impl Iterator<Item = &str> {
    sql.split(';').map(str::trim).filter(|statement| !is_effectively_empty_sql(statement))
}

struct AddColumnIfNotExists {
    table: String,
    column: String,
    statement: String,
}

fn parse_add_column_if_not_exists(statement: &str) -> Option<AddColumnIfNotExists> {
    let captures = add_column_if_not_exists_regex().captures(statement)?;
    let table = captures.name("table")?.as_str();
    let column = captures.name("column")?.as_str();
    let definition = captures.name("definition")?.as_str().trim();
    let normalized_table = normalize_sql_identifier(table)?;
    let normalized_column = normalize_sql_identifier(column)?;

    Some(AddColumnIfNotExists {
        table: normalized_table,
        column: normalized_column,
        statement: format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
    })
}

fn add_column_if_not_exists_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(
            r#"(?is)^\s*ALTER\s+TABLE\s+(?P<table>"?[A-Za-z_][A-Za-z0-9_]*"?)\s+ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+(?P<column>"?[A-Za-z_][A-Za-z0-9_]*"?)\s+(?P<definition>.+?)\s*$"#,
        )
        .expect("ADD COLUMN IF NOT EXISTS regex should compile")
    })
}

fn normalize_sql_identifier(identifier: &str) -> Option<String> {
    let trimmed = identifier.trim().trim_matches('"');
    if trimmed.is_empty() || !trimmed.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return None;
    }

    Some(trimmed.to_string())
}

async fn turso_column_exists(conn: &turso::Connection, table: &str, column: &str) -> Result<bool, EpError> {
    let table_identifier = quote_sqlite_identifier(table)?;
    let sql = format!("PRAGMA table_info({table_identifier})");
    let mut stmt = conn.prepare(&sql).await.map_err(|e| EpError::database(format!("prepare table_info failed: {e}")))?;
    let column_names = stmt.column_names();
    let mut rows = stmt.query(Vec::<turso::Value>::new()).await.map_err(|e| EpError::database(format!("table_info query failed: {e}")))?;

    while let Some(row) = rows.next().await.map_err(|e| EpError::database(format!("table_info row fetch: {e}")))? {
        let row = Row::new(&column_names, row);
        let name: String = row.get("name");
        if name.eq_ignore_ascii_case(column) {
            return Ok(true);
        }
    }

    Ok(false)
}

fn quote_sqlite_identifier(identifier: &str) -> Result<String, EpError> {
    let normalized = normalize_sql_identifier(identifier)
        .ok_or_else(|| EpError::database(format!("unsupported SQLite identifier in schema statement: {identifier}")))?;
    Ok(format!("\"{normalized}\""))
}

async fn execute_rewritten_batch(conn: &turso::Connection, sql: &str, error_prefix: &str) -> Result<(), EpError> {
    for statement in split_sql_statements(sql) {
        if let Some(add_column) = parse_add_column_if_not_exists(statement) {
            if turso_column_exists(conn, &add_column.table, &add_column.column).await? {
                continue;
            }

            conn.execute_batch(&add_column.statement).await.map_err(|e| EpError::database(format!("{error_prefix}: {e}")))?;
            continue;
        }

        conn.execute_batch(statement).await.map_err(|e| EpError::database(format!("{error_prefix}: {e}")))?;
    }

    Ok(())
}

impl TursoConnection {
    /// Execute a query expecting exactly one row.
    ///
    /// Matches `tokio_postgres::Client::query_one(sql, params)`.
    pub async fn query_one(&self, sql: &str, params: &[&SqlParam<'_>]) -> Result<Row, EpError> {
        let (column_names, mut rows) =
            query_prepared_rows(&self.conn, sql, params).await.map_err(|e| EpError::database(format!("query_one failed: {e}")))?;
        match rows.next().await.map_err(|e| EpError::database(format!("query_one row fetch: {e}")))? {
            Some(row) => Ok(Row::new(&column_names, row)),
            None => Err(EpError::database("query returned zero rows")),
        }
    }

    /// Execute a query expecting zero or one row.
    ///
    /// Returns `Ok(None)` when the query matches no rows instead of erroring.
    /// Matches `tokio_postgres::Client::query_opt(sql, params)`.
    pub async fn query_opt(&self, sql: &str, params: &[&SqlParam<'_>]) -> Result<Option<Row>, EpError> {
        let (column_names, mut rows) =
            query_prepared_rows(&self.conn, sql, params).await.map_err(|e| EpError::database(format!("query_opt failed: {e}")))?;
        match rows.next().await.map_err(|e| EpError::database(format!("query_opt row fetch: {e}")))? {
            Some(row) => Ok(Some(Row::new(&column_names, row))),
            None => Ok(None),
        }
    }

    /// Execute a query returning multiple rows.
    ///
    /// Matches `tokio_postgres::Client::query(sql, params)`.
    pub async fn query(&self, sql: &str, params: &[&SqlParam<'_>]) -> Result<Vec<Row>, EpError> {
        let (column_names, mut rows) = query_prepared_rows(&self.conn, sql, params).await?;
        let mut result = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| EpError::database(format!("row fetch: {e}")))? {
            result.push(Row::new(&column_names, row));
        }
        Ok(result)
    }

    /// Execute a statement returning the number of affected rows.
    ///
    /// Matches `tokio_postgres::Client::execute(sql, params)`.
    ///
    /// Turso's native `execute` errors when a statement returns rows (e.g.
    /// `SELECT` or `INSERT ... RETURNING`).  We use `query` internally and
    /// drain the result set so that callers behave identically to
    /// `tokio_postgres::Client::execute`, which accepts any statement type.
    pub async fn execute(&self, sql: &str, params: &[&SqlParam<'_>]) -> Result<u64, EpError> {
        let (_, mut rows) =
            query_prepared_rows(&self.conn, sql, params).await.map_err(|e| EpError::database(format!("execute failed: {e}")))?;
        let mut count: u64 = 0;
        while rows.next().await.map_err(|e| EpError::database(format!("execute row drain: {e}")))?.is_some() {
            count += 1;
        }
        Ok(count)
    }

    /// Execute multiple SQL statements separated by semicolons.
    ///
    /// Matches `tokio_postgres::Client::batch_execute(sql)`.
    pub async fn batch_execute(&self, sql: &str) -> Result<(), EpError> {
        let rewritten = rewrite_pg_sql(sql);
        if is_effectively_empty_sql(&rewritten) {
            return Ok(());
        }
        execute_rewritten_batch(&self.conn, &rewritten, "batch_execute failed").await
    }

    /// Begin a transaction.
    pub async fn transaction(&self) -> Result<TursoTransaction<'_>, EpError> {
        let tx = self.conn.unchecked_transaction().await.map_err(|e| EpError::database(format!("transaction failed: {e}")))?;
        Ok(TursoTransaction { tx })
    }

    /// Alias for `transaction()`, matching the tokio_postgres `build_transaction().start()` pattern.
    pub async fn build_transaction(&self) -> Result<TursoTransaction<'_>, EpError> {
        self.transaction().await
    }
}

/// Wraps a `turso::Transaction` with the same method surface as `TursoConnection`,
/// handling SQL rewriting and parameter conversion.
pub struct TursoTransaction<'conn> {
    tx: turso::transaction::Transaction<'conn>,
}

impl TursoTransaction<'_> {
    pub async fn query_one(&self, sql: &str, params: &[&SqlParam<'_>]) -> Result<Row, EpError> {
        let (column_names, mut rows) =
            query_prepared_rows(&self.tx, sql, params).await.map_err(|e| EpError::database(format!("tx query_one failed: {e}")))?;
        match rows.next().await.map_err(|e| EpError::database(format!("tx query_one fetch: {e}")))? {
            Some(row) => Ok(Row::new(&column_names, row)),
            None => Err(EpError::database("tx query returned zero rows")),
        }
    }

    pub async fn query_opt(&self, sql: &str, params: &[&SqlParam<'_>]) -> Result<Option<Row>, EpError> {
        let (column_names, mut rows) =
            query_prepared_rows(&self.tx, sql, params).await.map_err(|e| EpError::database(format!("tx query_opt failed: {e}")))?;
        match rows.next().await.map_err(|e| EpError::database(format!("tx query_opt fetch: {e}")))? {
            Some(row) => Ok(Some(Row::new(&column_names, row))),
            None => Ok(None),
        }
    }

    pub async fn query(&self, sql: &str, params: &[&SqlParam<'_>]) -> Result<Vec<Row>, EpError> {
        let (column_names, mut rows) =
            query_prepared_rows(&self.tx, sql, params).await.map_err(|e| EpError::database(format!("tx query failed: {e}")))?;
        let mut result = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| EpError::database(format!("tx row fetch: {e}")))? {
            result.push(Row::new(&column_names, row));
        }
        Ok(result)
    }

    pub async fn execute(&self, sql: &str, params: &[&SqlParam<'_>]) -> Result<u64, EpError> {
        let (_, mut rows) =
            query_prepared_rows(&self.tx, sql, params).await.map_err(|e| EpError::database(format!("tx execute failed: {e}")))?;
        let mut count: u64 = 0;
        while rows.next().await.map_err(|e| EpError::database(format!("tx execute row drain: {e}")))?.is_some() {
            count += 1;
        }
        Ok(count)
    }

    pub async fn batch_execute(&self, sql: &str) -> Result<(), EpError> {
        let rewritten = rewrite_pg_sql(sql);
        if is_effectively_empty_sql(&rewritten) {
            return Ok(());
        }
        self.tx.execute_batch(&rewritten).await.map_err(|e| EpError::database(format!("tx batch_execute failed: {e}")))?;
        Ok(())
    }

    pub async fn commit(self) -> Result<(), EpError> {
        self.tx.commit().await.map_err(|e| EpError::database(format!("tx commit failed: {e}")))
    }

    pub async fn rollback(self) -> Result<(), EpError> {
        self.tx.rollback().await.map_err(|e| EpError::database(format!("tx rollback failed: {e}")))
    }
}

// ---------------------------------------------------------------------------
// SQL rewriting: PostgreSQL dialect -> SQLite/Turso dialect
// ---------------------------------------------------------------------------

/// Rewrite PostgreSQL-specific SQL syntax to SQLite-compatible syntax.
fn rewrite_pg_sql(sql: &str) -> String {
    let mut out = sql.to_string();

    // $1, $2, ... -> ?1, ?2, ...
    // Use a simple byte scan to avoid pulling in regex
    let mut result = String::with_capacity(out.len());
    let bytes = out.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            result.push('?');
            i += 1; // skip '$', the digits will be copied in the next iterations
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }
    out = result;

    // NOW() -> datetime('now')
    // Note: PG's NOW() returns the transaction-start timestamp (frozen for the
    // transaction duration), whereas SQLite's datetime('now') returns wall-clock
    // time on each call.  The difference is negligible for embedded-db's
    // single-process use case.
    out = out.replace("NOW()", "datetime('now')");
    out = out.replace("now()", "datetime('now')");

    // Strip FOR SHARE / FOR UPDATE row-level locking (no-op in SQLite)
    out = out.replace("FOR SHARE", "");
    out = out.replace("FOR UPDATE", "");
    out = out.replace("for share", "");
    out = out.replace("for update", "");

    // PG INTERVAL arithmetic -> SQLite datetime modifier
    // e.g. datetime('now') - INTERVAL '30 minutes' -> datetime('now', '-30 minutes')
    // This runs after NOW() has been rewritten to datetime('now')
    while let Some(start) = out.find("INTERVAL '") {
        // Find the preceding datetime('now') and the operator
        if let Some(dt_end) = out[..start].rfind("datetime('now')") {
            let dt_expr_end = dt_end + "datetime('now')".len();
            // Get the operator between datetime('now') and INTERVAL
            let between = out[dt_expr_end..start].trim();
            let sign = if between.ends_with('-') { "-" } else { "+" };

            // Extract the interval value: INTERVAL '30 minutes'
            let interval_start = start + "INTERVAL '".len();
            if let Some(interval_end) = out[interval_start..].find('\'') {
                let interval_value = &out[interval_start..interval_start + interval_end];
                let modifier = format!("'{sign}{interval_value}'");
                let replacement = format!("datetime('now', {modifier})");

                // Find the full range to replace: from datetime('now') through INTERVAL '...'
                let full_end = interval_start + interval_end + 1;
                out = format!("{}{replacement}{}", &out[..dt_end], &out[full_end..]);
            } else {
                break;
            }
        } else {
            break;
        }
    }

    // PG array aggregation -> SQLite JSON array aggregation
    // array_remove(array_agg(DISTINCT x), NULL) -> json_group_array(x)
    while let Some(start) = out.find("array_remove(array_agg(DISTINCT ") {
        let expr_start = start + "array_remove(array_agg(DISTINCT ".len();
        if let Some(close) = out[expr_start..].find("), NULL)") {
            let expr = out[expr_start..expr_start + close].to_string();
            let end = expr_start + close + "), NULL)".len();
            let replacement = format!("json_group_array(DISTINCT {expr})");
            out = format!("{}{replacement}{}", &out[..start], &out[end..]);
        } else {
            break;
        }
    }

    // Handle remaining array_agg / array_remove patterns
    while let Some(start) = out.find("array_remove(array_agg(") {
        let expr_start = start + "array_remove(array_agg(".len();
        if let Some(close) = out[expr_start..].find("), NULL)") {
            let expr = out[expr_start..expr_start + close].to_string();
            let end = expr_start + close + "), NULL)".len();
            let replacement = format!("json_group_array({expr})");
            out = format!("{}{replacement}{}", &out[..start], &out[end..]);
        } else {
            break;
        }
    }

    // Plain array_agg / json_agg -> json_group_array
    out = out.replace("array_agg(", "json_group_array(");
    out = out.replace("json_agg(", "json_group_array(");

    // Strip PG type casts: ::type_name (e.g. ::text, ::uuid, ::jsonb, ::boolean, ::bigint)
    // Also handle sized casts like ::VARCHAR(32) and array casts like ::uuid[].
    let mut stripped = String::with_capacity(out.len());
    let cast_bytes = out.as_bytes();
    let mut j = 0;
    while j < cast_bytes.len() {
        if j + 1 < cast_bytes.len() && cast_bytes[j] == b':' && cast_bytes[j + 1] == b':' {
            // Skip :: and any whitespace before the type name.
            j += 2;
            while j < cast_bytes.len() && cast_bytes[j].is_ascii_whitespace() {
                j += 1;
            }

            // Skip the type name itself.
            while j < cast_bytes.len() && (cast_bytes[j].is_ascii_alphanumeric() || cast_bytes[j] == b'_' || cast_bytes[j] == b'.') {
                j += 1;
            }

            // Skip an optional size/precision suffix like (32) or (10, 2).
            if j < cast_bytes.len() && cast_bytes[j] == b'(' {
                let mut depth = 0usize;
                while j < cast_bytes.len() {
                    if cast_bytes[j] == b'(' {
                        depth += 1;
                    } else if cast_bytes[j] == b')' {
                        depth -= 1;
                        if depth == 0 {
                            j += 1;
                            break;
                        }
                    }
                    j += 1;
                }
            }

            // Skip optional array suffixes.
            while j + 1 < cast_bytes.len() && cast_bytes[j] == b'[' && cast_bytes[j + 1] == b']' {
                j += 2;
            }
        } else {
            stripped.push(cast_bytes[j] as char);
            j += 1;
        }
    }
    out = stripped;

    out = rewrite_tuple_comparisons(&out);

    out = strip_on_conflict_predicate(&out);

    out
}

/// Drop the partial-index predicate from an upsert arbiter:
/// `ON CONFLICT (cols) WHERE <pred> DO ...` -> `ON CONFLICT (cols) DO ...`.
///
/// libsql/SQLite cannot resolve an `ON CONFLICT` arbiter against a *partial*
/// unique index (it reports "ON CONFLICT clause does not match any PRIMARY KEY
/// or UNIQUE constraint"). The embedded backend is single-tenant — every row
/// has `organization_uuid IS NULL` — so the shared upsert SQL (which carries a
/// `WHERE organization_uuid IS (NOT) NULL` predicate for the Postgres partial
/// indexes) is matched here against the plain, non-partial unique indexes the
/// Turso DDL ships. Removing the predicate lets the column list alone pick the
/// arbiter.
fn strip_on_conflict_predicate(sql: &str) -> String {
    let mut out = sql.to_string();
    let mut search_from = 0usize;
    loop {
        let lower = out.to_ascii_lowercase();
        let Some(rel) = lower[search_from..].find("on conflict") else {
            break;
        };
        let conflict = search_from + rel;
        // Locate the conflict-target column list `( ... )`.
        let Some(open) = lower[conflict..].find('(').map(|i| conflict + i) else {
            break;
        };
        let Some(close) = lower[open..].find(')').map(|i| open + i) else {
            break;
        };
        let after = close + 1;
        let rest = &lower[after..];
        let trimmed = rest.trim_start();
        if let Some(do_rel) = trimmed.strip_prefix("where").and_then(|w| w.find(" do ")) {
            // Absolute range: from the start of `WHERE` through (but not
            // including) the leading space of ` DO `.
            let ws_len = rest.len() - trimmed.len();
            let where_abs = after + ws_len;
            let do_space_abs = where_abs + "where".len() + do_rel;
            out.replace_range(where_abs..do_space_abs + 1, "");
            search_from = where_abs;
        } else {
            search_from = after;
        }
    }
    out
}

fn rewrite_tuple_comparisons(sql: &str) -> String {
    tuple_comparison_regex()
        .replace_all(sql, |captures: &Captures<'_>| {
            let left_first = captures.get(1).map(|m| m.as_str()).unwrap_or_default();
            let left_second = captures.get(2).map(|m| m.as_str()).unwrap_or_default();
            let operator = captures.get(3).map(|m| m.as_str()).unwrap_or_default();
            let right_first = captures.get(4).map(|m| m.as_str()).unwrap_or_default();
            let right_second = captures.get(5).map(|m| m.as_str()).unwrap_or_default();
            let primary_operator = if operator.starts_with('<') { "<" } else { ">" };

            format!(
                "(({left_first} {primary_operator} {right_first}) OR ({left_first} = {right_first} AND {left_second} {operator} {right_second}))"
            )
        })
        .into_owned()
}

fn tuple_comparison_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(
            r"\(\s*([A-Za-z0-9_.?]+)\s*,\s*([A-Za-z0-9_.?]+)\s*\)\s*(<=|>=|<|>)\s*\(\s*([A-Za-z0-9_.?]+)\s*,\s*([A-Za-z0-9_.?]+)\s*\)",
        )
        .expect("tuple comparison regex should compile")
    })
}

// ---------------------------------------------------------------------------
// Parameter conversion: tokio_postgres ToSql -> turso::Value
// ---------------------------------------------------------------------------

/// Convert a slice of `&dyn ToSql` params to `Vec<turso::Value>`.
///
/// Uses `ToSql::to_sql_checked` to serialize each param via the PG binary
/// protocol, then converts the resulting bytes to the appropriate
/// `turso::Value` variant. This approach works for all concrete types
/// (including Eden newtype wrappers and reference types) without needing
/// `Any` downcasting.
fn convert_params(params: &[&SqlParam<'_>]) -> ResultEP<Vec<turso::Value>> {
    params.iter().map(|p| to_turso_value(*p)).collect()
}

/// Try to serialize `param` with the given PG `ty`.
///
/// Returns `Some(IsNull)` on success (param accepted the type),
/// `None` if the type was rejected.  On success the serialized bytes
/// are in `buf`.
fn try_serialize(
    param: &(dyn tokio_postgres::types::ToSql + Sync),
    ty: &tokio_postgres::types::Type,
    buf: &mut bytes::BytesMut,
) -> Option<tokio_postgres::types::IsNull> {
    buf.clear();
    param.to_sql_checked(ty, buf).ok()
}

fn to_turso_value(param: &SqlParam<'_>) -> ResultEP<turso::Value> {
    use tokio_postgres::types::{IsNull, Type};

    let mut buf = bytes::BytesMut::new();

    // ---- BOOL ----
    // PG bool is a single byte (0x00 or 0x01).
    if let Some(is_null) = try_serialize(param, &Type::BOOL, &mut buf) {
        return match is_null {
            IsNull::Yes => Ok(turso::Value::Null),
            IsNull::No => {
                let val = if buf.as_ref() == [1] { 1i64 } else { 0i64 };
                Ok(turso::Value::Integer(val))
            }
        };
    }

    // ---- INT8 (i64) ----
    // PG int8 is 8 bytes big-endian.
    if let Some(is_null) = try_serialize(param, &Type::INT8, &mut buf) {
        return match is_null {
            IsNull::Yes => Ok(turso::Value::Null),
            IsNull::No if buf.len() == 8 => {
                let val = i64::from_be_bytes(buf.as_ref().try_into().expect("length verified by match guard"));
                Ok(turso::Value::Integer(val))
            }
            _ => Ok(turso::Value::Integer(0)),
        };
    }

    // ---- INT4 (i32) ----
    // PG int4 is 4 bytes big-endian.
    if let Some(is_null) = try_serialize(param, &Type::INT4, &mut buf) {
        return match is_null {
            IsNull::Yes => Ok(turso::Value::Null),
            IsNull::No if buf.len() == 4 => {
                let val = i32::from_be_bytes(buf.as_ref().try_into().expect("length verified by match guard"));
                Ok(turso::Value::Integer(val as i64))
            }
            _ => Ok(turso::Value::Integer(0)),
        };
    }

    // ---- INT2 (i16) ----
    // PG int2 is 2 bytes big-endian.
    if let Some(is_null) = try_serialize(param, &Type::INT2, &mut buf) {
        return match is_null {
            IsNull::Yes => Ok(turso::Value::Null),
            IsNull::No if buf.len() == 2 => {
                let val = i16::from_be_bytes(buf.as_ref().try_into().expect("length verified by match guard"));
                Ok(turso::Value::Integer(val as i64))
            }
            _ => Ok(turso::Value::Integer(0)),
        };
    }

    // ---- FLOAT8 (f64) ----
    // PG float8 is 8 bytes big-endian IEEE 754.
    if let Some(is_null) = try_serialize(param, &Type::FLOAT8, &mut buf) {
        return match is_null {
            IsNull::Yes => Ok(turso::Value::Null),
            IsNull::No if buf.len() == 8 => {
                let val = f64::from_be_bytes(buf.as_ref().try_into().expect("length verified by match guard"));
                Ok(turso::Value::Real(val))
            }
            _ => Ok(turso::Value::Real(0.0)),
        };
    }

    // ---- FLOAT4 (f32) ----
    // PG float4 is 4 bytes big-endian IEEE 754.
    if let Some(is_null) = try_serialize(param, &Type::FLOAT4, &mut buf) {
        return match is_null {
            IsNull::Yes => Ok(turso::Value::Null),
            IsNull::No if buf.len() == 4 => {
                let val = f32::from_be_bytes(buf.as_ref().try_into().expect("length verified by match guard"));
                Ok(turso::Value::Real(val as f64))
            }
            _ => Ok(turso::Value::Real(0.0)),
        };
    }

    // ---- UUID ----
    // PG UUID is 16 bytes.  Covers `uuid::Uuid`, Eden newtype wrappers
    // (`OrganizationUuid`, `EndpointUuid`, etc.) and references to them.
    if let Some(is_null) = try_serialize(param, &Type::UUID, &mut buf) {
        return match is_null {
            IsNull::Yes => Ok(turso::Value::Null),
            IsNull::No => {
                if buf.len() == 16
                    && let Ok(u) = uuid::Uuid::from_slice(&buf)
                {
                    return Ok(turso::Value::Text(u.to_string()));
                }
                // Unexpected length -- treat as hex text.
                Ok(turso::Value::Text(hex::encode(&buf)))
            }
        };
    }

    // ---- TIMESTAMPTZ ----
    // PG timestamptz is 8 bytes (microseconds since 2000-01-01).
    if let Some(is_null) = try_serialize(param, &Type::TIMESTAMPTZ, &mut buf) {
        return match is_null {
            IsNull::Yes => Ok(turso::Value::Null),
            IsNull::No if buf.len() == 8 => {
                let micros = i64::from_be_bytes(buf.as_ref().try_into().expect("length verified by match guard"));
                // PG epoch is 2000-01-01 00:00:00 UTC
                let pg_epoch =
                    chrono::NaiveDate::from_ymd_opt(2000, 1, 1).expect("valid date").and_hms_opt(0, 0, 0).expect("valid time").and_utc();
                let dt = pg_epoch + chrono::Duration::microseconds(micros);
                Ok(turso::Value::Text(dt.to_rfc3339()))
            }
            _ => Ok(turso::Value::Text(String::new())),
        };
    }

    // ---- TIMESTAMP (without tz) ----
    // Same binary format as TIMESTAMPTZ.
    if let Some(is_null) = try_serialize(param, &Type::TIMESTAMP, &mut buf) {
        return match is_null {
            IsNull::Yes => Ok(turso::Value::Null),
            IsNull::No if buf.len() == 8 => {
                let micros = i64::from_be_bytes(buf.as_ref().try_into().expect("length verified by match guard"));
                let pg_epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).expect("valid date").and_hms_opt(0, 0, 0).expect("valid time");
                let dt = pg_epoch + chrono::Duration::microseconds(micros);
                Ok(turso::Value::Text(dt.format("%Y-%m-%dT%H:%M:%S").to_string()))
            }
            _ => Ok(turso::Value::Text(String::new())),
        };
    }

    // ---- JSONB ----
    // Stored as text in SQLite.
    if let Some(is_null) = try_serialize(param, &Type::JSONB, &mut buf) {
        return match is_null {
            IsNull::Yes => Ok(turso::Value::Null),
            IsNull::No => {
                // JSONB binary format has a version byte prefix (0x01)
                let slice = if !buf.is_empty() && buf[0] == 1 { &buf[1..] } else { &buf[..] };
                let text =
                    String::from_utf8(slice.to_vec()).map_err(|e| EpError::database(format!("JSONB param was not valid UTF-8: {e}")))?;
                Ok(turso::Value::Text(text))
            }
        };
    }

    // ---- BYTEA ----
    // Raw bytes.
    if let Some(is_null) = try_serialize(param, &Type::BYTEA, &mut buf) {
        return match is_null {
            IsNull::Yes => Ok(turso::Value::Null),
            IsNull::No => Ok(turso::Value::Blob(buf.to_vec())),
        };
    }

    // ---- TEXT ----
    // Covers `String`, `&str`, Eden `XxxId` newtypes (which wrap String),
    // and any other type that serializes as text in PG.
    if let Some(is_null) = try_serialize(param, &Type::TEXT, &mut buf) {
        return match is_null {
            IsNull::Yes => Ok(turso::Value::Null),
            IsNull::No => {
                let text = String::from_utf8(buf.to_vec())
                    .map_err(|e| EpError::database(format!("ToSql TEXT conversion produced invalid UTF-8: {e}")))?;
                Ok(turso::Value::Text(text))
            }
        };
    }

    // ---- VARCHAR (last resort for scalars) ----
    // Some types accept VARCHAR but not TEXT.
    if let Some(is_null) = try_serialize(param, &Type::VARCHAR, &mut buf) {
        return match is_null {
            IsNull::Yes => Ok(turso::Value::Null),
            IsNull::No => {
                let text = String::from_utf8(buf.to_vec())
                    .map_err(|e| EpError::database(format!("ToSql VARCHAR conversion produced invalid UTF-8: {e}")))?;
                Ok(turso::Value::Text(text))
            }
        };
    }

    // ---- ARRAY types ----
    // PG arrays (TEXT[], UUID[], INT4[], etc.) are stored as JSON text in SQLite.
    // Binary format: 4B ndim, 4B flags, 4B elem_oid, per-dim (4B size, 4B lower_bound), then elements (4B len, data).
    let array_types = [
        Type::TEXT_ARRAY,
        Type::VARCHAR_ARRAY,
        Type::UUID_ARRAY,
        Type::INT4_ARRAY,
        Type::INT8_ARRAY,
    ];
    for arr_ty in &array_types {
        if let Some(is_null) = try_serialize(param, arr_ty, &mut buf) {
            return match is_null {
                IsNull::Yes => Ok(turso::Value::Null),
                IsNull::No => {
                    let json_arr = pg_array_binary_to_json(&buf, arr_ty)?;
                    Ok(turso::Value::Text(json_arr))
                }
            };
        }
    }

    Err(EpError::database(format!(
        "unsupported parameter type for Turso conversion (no PG type accepted): {:?}",
        std::any::type_name_of_val(param)
    )))
}

/// Parse a PG binary array into a JSON array string for SQLite storage.
///
/// PG binary array format:
///   4B ndim | 4B has_null flags | 4B elem_oid
///   per dimension: 4B size | 4B lower_bound
///   per element: 4B len (-1 = NULL) | len bytes data
fn pg_array_binary_to_json(buf: &[u8], arr_ty: &tokio_postgres::types::Type) -> ResultEP<String> {
    use tokio_postgres::types::Type;

    if buf.len() < 12 {
        return Ok("[]".to_string());
    }

    let ndim = i32::from_be_bytes(buf[0..4].try_into().map_err(|_| EpError::database("array ndim"))?);
    if ndim == 0 {
        return Ok("[]".to_string());
    }

    // Skip header: 12 bytes + ndim * 8 bytes (size + lower_bound per dim)
    let header_len = 12 + (ndim as usize) * 8;
    if buf.len() < header_len {
        return Ok("[]".to_string());
    }

    // Read element count from first dimension
    let n_elems = i32::from_be_bytes(buf[12..16].try_into().map_err(|_| EpError::database("array dim size"))?) as usize;

    let mut elements = Vec::with_capacity(n_elems);
    let mut pos = header_len;

    for _ in 0..n_elems {
        if pos + 4 > buf.len() {
            break;
        }
        let elem_len = i32::from_be_bytes(buf[pos..pos + 4].try_into().map_err(|_| EpError::database("array elem len"))?);
        pos += 4;

        if elem_len == -1 {
            elements.push(serde_json::Value::Null);
        } else {
            let elem_len = elem_len as usize;
            if pos + elem_len > buf.len() {
                break;
            }
            let elem_bytes = &buf[pos..pos + elem_len];
            pos += elem_len;

            let val = match *arr_ty {
                Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => {
                    let s = String::from_utf8_lossy(elem_bytes);
                    serde_json::Value::String(s.into_owned())
                }
                Type::UUID_ARRAY => {
                    if elem_len == 16 {
                        if let Ok(u) = uuid::Uuid::from_slice(elem_bytes) {
                            serde_json::Value::String(u.to_string())
                        } else {
                            serde_json::Value::String(hex::encode(elem_bytes))
                        }
                    } else {
                        serde_json::Value::String(hex::encode(elem_bytes))
                    }
                }
                Type::INT4_ARRAY if elem_len == 4 => {
                    let v = i32::from_be_bytes(elem_bytes.try_into().map_err(|_| EpError::database("int4 array elem"))?);
                    serde_json::Value::Number(v.into())
                }
                Type::INT8_ARRAY if elem_len == 8 => {
                    let v = i64::from_be_bytes(elem_bytes.try_into().map_err(|_| EpError::database("int8 array elem"))?);
                    serde_json::Value::Number(v.into())
                }
                _ => serde_json::Value::String(String::from_utf8_lossy(elem_bytes).into_owned()),
            };
            elements.push(val);
        }
    }

    serde_json::to_string(&elements).map_err(|e| EpError::database(format!("array to json: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_pg_params() {
        assert_eq!(rewrite_pg_sql("SELECT * FROM t WHERE id = $1"), "SELECT * FROM t WHERE id = ?1");
        assert_eq!(rewrite_pg_sql("INSERT INTO t (a, b) VALUES ($1, $2)"), "INSERT INTO t (a, b) VALUES (?1, ?2)");
        assert_eq!(rewrite_pg_sql("SELECT * FROM t WHERE id = $10"), "SELECT * FROM t WHERE id = ?10");
    }

    #[test]
    fn test_rewrite_now() {
        assert_eq!(
            rewrite_pg_sql("INSERT INTO t (created_at) VALUES (NOW())"),
            "INSERT INTO t (created_at) VALUES (datetime('now'))"
        );
    }

    // --- Parameter conversion tests ---

    #[test]
    fn test_to_turso_value_string() {
        let val = "hello".to_string();
        let param: &(dyn tokio_postgres::types::ToSql + Sync) = &val;
        let result = to_turso_value(param).unwrap();
        assert!(matches!(result, turso::Value::Text(ref s) if s == "hello"));
    }

    #[test]
    fn test_to_turso_value_str() {
        let val = "hello";
        let param: &(dyn tokio_postgres::types::ToSql + Sync) = &val;
        let result = to_turso_value(param).unwrap();
        assert!(matches!(result, turso::Value::Text(ref s) if s == "hello"));
    }

    #[test]
    fn test_to_turso_value_i32() {
        let val = 42i32;
        let param: &(dyn tokio_postgres::types::ToSql + Sync) = &val;
        let result = to_turso_value(param).unwrap();
        assert!(matches!(result, turso::Value::Integer(42)));
    }

    #[test]
    fn test_to_turso_value_i64() {
        let val = 42i64;
        let param: &(dyn tokio_postgres::types::ToSql + Sync) = &val;
        let result = to_turso_value(param).unwrap();
        assert!(matches!(result, turso::Value::Integer(42)));
    }

    #[test]
    fn test_to_turso_value_bool_true() {
        let val = true;
        let param: &(dyn tokio_postgres::types::ToSql + Sync) = &val;
        let result = to_turso_value(param).unwrap();
        assert!(matches!(result, turso::Value::Integer(1)));
    }

    #[test]
    fn test_to_turso_value_bool_false() {
        let val = false;
        let param: &(dyn tokio_postgres::types::ToSql + Sync) = &val;
        let result = to_turso_value(param).unwrap();
        assert!(matches!(result, turso::Value::Integer(0)));
    }

    #[test]
    fn test_to_turso_value_uuid() {
        let val = uuid::Uuid::nil();
        let param: &(dyn tokio_postgres::types::ToSql + Sync) = &val;
        let result = to_turso_value(param).unwrap();
        assert!(matches!(result, turso::Value::Text(ref s) if s == "00000000-0000-0000-0000-000000000000"));
    }

    #[test]
    fn test_to_turso_value_f64() {
        let val = std::f64::consts::PI;
        let param: &(dyn tokio_postgres::types::ToSql + Sync) = &val;
        let result = to_turso_value(param).unwrap();
        match result {
            turso::Value::Real(v) => assert!((v - std::f64::consts::PI).abs() < f64::EPSILON),
            other => panic!("expected Real, got {other:?}"),
        }
    }

    #[test]
    fn test_to_turso_value_json() {
        let val = serde_json::json!({"key": "val"});
        let param: &(dyn tokio_postgres::types::ToSql + Sync) = &val;
        let result = to_turso_value(param).unwrap();
        match result {
            turso::Value::Text(s) => {
                let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
                assert_eq!(parsed, serde_json::json!({"key": "val"}));
            }
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn test_to_turso_value_bytes() {
        let val = vec![1u8, 2, 3];
        let param: &(dyn tokio_postgres::types::ToSql + Sync) = &val;
        let result = to_turso_value(param).unwrap();
        assert!(matches!(result, turso::Value::Blob(ref b) if b == &[1u8, 2, 3]));
    }

    #[test]
    fn test_convert_params_mixed() {
        let str_val = "hello".to_string();
        let uuid_val = uuid::Uuid::nil();
        let i32_val = 99i32;
        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&str_val, &uuid_val, &i32_val];
        let result = convert_params(&params).unwrap();
        assert_eq!(result.len(), 3);
        assert!(matches!(&result[0], turso::Value::Text(s) if s == "hello"));
        assert!(matches!(&result[1], turso::Value::Text(s) if s == "00000000-0000-0000-0000-000000000000"));
        assert!(matches!(&result[2], turso::Value::Integer(99)));
    }

    // --- SQL rewriting edge cases ---

    #[test]
    fn test_rewrite_no_params() {
        assert_eq!(rewrite_pg_sql("SELECT 1"), "SELECT 1");
    }

    #[test]
    fn test_rewrite_dollar_not_followed_by_digit() {
        assert_eq!(rewrite_pg_sql("SELECT $abc"), "SELECT $abc");
    }

    #[test]
    fn test_rewrite_now_lowercase() {
        assert_eq!(rewrite_pg_sql("now()"), "datetime('now')");
    }

    #[test]
    fn test_rewrite_strips_type_casts() {
        assert_eq!(rewrite_pg_sql("o.uuid::text"), "o.uuid");
        assert_eq!(rewrite_pg_sql("'[]'::json"), "'[]'");
        assert_eq!(rewrite_pg_sql("val::boolean"), "val");
        assert_eq!(
            rewrite_pg_sql("SELECT o.uuid::text, name FROM t WHERE id = ?1"),
            "SELECT o.uuid, name FROM t WHERE id = ?1"
        );
    }

    #[test]
    fn test_rewrite_strips_sized_type_casts() {
        assert_eq!(rewrite_pg_sql("?2::VARCHAR(32)"), "?2");
        assert_eq!(rewrite_pg_sql("?7::NUMERIC(10, 2)"), "?7");
        assert_eq!(rewrite_pg_sql("?3::uuid[]"), "?3");
    }

    #[test]
    fn test_rewrite_tuple_comparison_less_than() {
        assert_eq!(
            rewrite_pg_sql("WHERE (version_ms, version_seq) < ($4, $5)"),
            "WHERE ((version_ms < ?4) OR (version_ms = ?4 AND version_seq < ?5))"
        );
    }

    #[test]
    fn test_rewrite_tuple_comparison_greater_than_or_equal() {
        assert_eq!(
            rewrite_pg_sql("AND (rt.version_ms, rt.version_seq) >= ($7::BIGINT, $8::BIGINT)"),
            "AND ((rt.version_ms > ?7) OR (rt.version_ms = ?7 AND rt.version_seq >= ?8))"
        );
    }

    #[test]
    fn test_rewrite_rbac_control_insert_for_sqlite() {
        let sql = include_str!("../../sql/insert/rbac_control.sql");
        let rewritten = rewrite_pg_sql(sql);

        assert!(!rewritten.contains("?2(32)"));
        assert!(!rewritten.contains("?4(32)"));
        assert!(!rewritten.contains("?6(8)"));
        assert!(!rewritten.contains("(rt.version_ms, rt.version_seq)"));
        assert!(!rewritten.contains("(rbac_control.version_ms, rbac_control.version_seq)"));
        assert!(rewritten.contains("ON CONFLICT"));
    }

    #[test]
    fn test_strip_on_conflict_partial_predicate() {
        // Single-column partial arbiter -> bare column-list arbiter.
        assert_eq!(
            strip_on_conflict_predicate("ON CONFLICT (name) WHERE organization_uuid IS NULL DO UPDATE SET x = 1"),
            "ON CONFLICT (name) DO UPDATE SET x = 1"
        );
        // Multi-column partial arbiter.
        assert_eq!(
            strip_on_conflict_predicate("ON CONFLICT (organization_uuid, name) WHERE organization_uuid IS NOT NULL DO UPDATE SET x = 1"),
            "ON CONFLICT (organization_uuid, name) DO UPDATE SET x = 1"
        );
        // Non-partial arbiter is left untouched (no WHERE between target and DO).
        assert_eq!(strip_on_conflict_predicate("ON CONFLICT (a) DO NOTHING"), "ON CONFLICT (a) DO NOTHING");
        // SQL without ON CONFLICT is unchanged.
        assert_eq!(strip_on_conflict_predicate("SELECT * FROM t WHERE x = 1"), "SELECT * FROM t WHERE x = 1");
    }

    #[test]
    fn test_rewrite_pg_sql_strips_partial_conflict_predicate_end_to_end() {
        let got = rewrite_pg_sql(
            "INSERT INTO llm_skills (name) VALUES ($1) ON CONFLICT (name) WHERE organization_uuid IS NULL DO UPDATE SET name = EXCLUDED.name",
        );
        assert_eq!(
            got,
            "INSERT INTO llm_skills (name) VALUES (?1) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name"
        );
    }
}
