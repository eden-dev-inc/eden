//! Typed query layer on top of `PostgresClient`.
//!
//! Provides structured query results by parsing PG wire protocol responses
//! from raw bytes into rows with named columns. Used by the migration system,
//! health checks, and any code that needs structured data rather than raw bytes.
//!
//! All values are received in text format (simple query protocol) and converted
//! to appropriate JSON types using the column's type OID from `RowDescription`.

use crate::client::{PostgresClient, build_query_message};
use error::{EpError, ResultEP};
use postgres_wire::error::{backend, type_oid};
use postgres_wire::parse::PgParseSync;
use postgres_wire::types::data_row::DataRow;
use postgres_wire::types::error_response::ErrorResponse;
use postgres_wire::types::row_description::RowDescription;
use wire_stream::SliceStream;

// ─────────────────────────────────────────────────────────────────────────────
// PgSimpleRow — a row with named, typed columns
// ─────────────────────────────────────────────────────────────────────────────

/// Column metadata from a RowDescription.
#[derive(Clone, Debug)]
pub struct ColumnMeta {
    pub name: String,
    pub type_oid: i32,
}

/// A single row from a simple query result.
///
/// Column values are stored as `Option<String>` (None = SQL NULL).
/// Use `get()` for raw text access or `to_json()` for type-aware JSON conversion.
#[derive(Clone, Debug)]
pub struct PgSimpleRow {
    columns: Vec<ColumnMeta>,
    values: Vec<Option<String>>,
}

impl PgSimpleRow {
    /// Get a column value by name as an optional string reference.
    /// Returns `None` if the column doesn't exist or the value is SQL NULL.
    pub fn get(&self, column: &str) -> Option<&str> {
        self.columns.iter().position(|c| c.name == column).and_then(|i| self.values[i].as_deref())
    }

    /// Get a column value by index as an optional string reference.
    pub fn get_idx(&self, index: usize) -> Option<&str> {
        self.values.get(index).and_then(|v| v.as_deref())
    }

    /// Get the column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Number of columns in this row.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Whether this row has no columns.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Convert this row to a `serde_json::Value` (JSON Object).
    ///
    /// Text values are converted to appropriate JSON types based on the column's
    /// PostgreSQL type OID:
    /// - `bool` → `Value::Bool`
    /// - `int2/int4/int8/oid` → `Value::Number` (i64)
    /// - `float4/float8/numeric` → `Value::Number` (f64)
    /// - `text[]/varchar[]/etc.` arrays → `Value::Array` of strings
    /// - NULL → `Value::Null`
    /// - Everything else → `Value::String`
    pub fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        for (col, val) in self.columns.iter().zip(self.values.iter()) {
            let json_val = match val {
                None => serde_json::Value::Null,
                Some(text) => text_to_json(text, col.type_oid),
            };
            map.insert(col.name.clone(), json_val);
        }
        serde_json::Value::Object(map)
    }
}

/// Convert a PostgreSQL text-format value to a JSON value using the type OID.
fn text_to_json(text: &str, type_oid: i32) -> serde_json::Value {
    match type_oid {
        // Boolean
        type_oid::BOOL => match text {
            "t" | "true" | "TRUE" | "1" => serde_json::Value::Bool(true),
            "f" | "false" | "FALSE" | "0" => serde_json::Value::Bool(false),
            _ => serde_json::Value::String(text.to_string()),
        },

        // Integer types → i64
        type_oid::INT2 | type_oid::INT4 | type_oid::INT8 | type_oid::OID | type_oid::XID | type_oid::XID8 | type_oid::CID => text
            .parse::<i64>()
            .map(|n| serde_json::Value::Number(n.into()))
            .unwrap_or_else(|_| serde_json::Value::String(text.to_string())),

        // Float types → f64 (NaN/Infinity become null since JSON can't represent them)
        type_oid::FLOAT4 | type_oid::FLOAT8 | type_oid::NUMERIC => text
            .parse::<f64>()
            .ok()
            .and_then(|f| {
                if f.is_nan() || f.is_infinite() {
                    None
                } else {
                    serde_json::Number::from_f64(f)
                }
            })
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),

        // JSON/JSONB → parse as JSON
        type_oid::JSON | type_oid::JSONB => serde_json::from_str(text).unwrap_or_else(|_| serde_json::Value::String(text.to_string())),

        // Array types → parse PG array text format "{a,b,c}" to JSON array
        oid if type_oid::is_array_type(oid) => {
            let elem_oid = type_oid::array_element_type(oid);
            parse_pg_text_array(text, elem_oid)
        }

        // Everything else → string
        _ => serde_json::Value::String(text.to_string()),
    }
}

/// Parse a PostgreSQL text-format array like `{foo,bar,"baz qux",NULL}` into a JSON array.
///
/// Handles:
/// - Unquoted elements (separated by commas)
/// - Quoted elements (double-quoted, with `\"` and `\\` escapes)
/// - NULL elements (literal `NULL` without quotes)
///
/// `elem_type_oid` is the element type OID (e.g., INT4 for int4[]).
/// When non-zero, elements are converted using `text_to_json` for proper typing.
fn parse_pg_text_array(text: &str, elem_type_oid: i32) -> serde_json::Value {
    let trimmed = text.trim();
    if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return serde_json::Value::String(text.to_string());
    }

    let inner = &trimmed[1..trimmed.len() - 1];
    if inner.is_empty() {
        return serde_json::Value::Array(Vec::new());
    }

    let mut elements = Vec::new();
    let mut chars = inner.chars().peekable();

    loop {
        // Skip leading whitespace
        while chars.peek() == Some(&' ') {
            chars.next();
        }

        if chars.peek().is_none() {
            break;
        }

        if chars.peek() == Some(&'"') {
            // Quoted element
            chars.next(); // consume opening quote
            let mut elem = String::new();
            loop {
                match chars.next() {
                    Some('\\') => {
                        if let Some(c) = chars.next() {
                            elem.push(c);
                        }
                    }
                    Some('"') => break,
                    Some(c) => elem.push(c),
                    None => break,
                }
            }
            if elem_type_oid != 0 {
                elements.push(text_to_json(&elem, elem_type_oid));
            } else {
                elements.push(serde_json::Value::String(elem));
            }
        } else {
            // Unquoted element or NULL
            let mut elem = String::new();
            while let Some(&c) = chars.peek() {
                if c == ',' {
                    break;
                }
                elem.push(c);
                chars.next();
            }
            if elem == "NULL" {
                elements.push(serde_json::Value::Null);
            } else if elem_type_oid != 0 {
                elements.push(text_to_json(&elem, elem_type_oid));
            } else {
                elements.push(serde_json::Value::String(elem));
            }
        }

        // Consume comma separator
        if chars.peek() == Some(&',') {
            chars.next();
        }
    }

    serde_json::Value::Array(elements)
}

// ─────────────────────────────────────────────────────────────────────────────
// StatementResult — per-statement result for simple query protocol
// ─────────────────────────────────────────────────────────────────────────────

/// Result of a single statement within a simple query batch.
///
/// Simple query protocol can contain multiple statements separated by `;`.
/// Each statement produces either data rows (SELECT) or an affected row count
/// (INSERT/UPDATE/DELETE/DDL).
pub enum StatementResult {
    /// Statement returned rows (SELECT or RETURNING).
    Rows(Vec<PgSimpleRow>),
    /// Statement completed without rows (INSERT/UPDATE/DELETE/DDL).
    Command { affected_rows: u64 },
}

// ─────────────────────────────────────────────────────────────────────────────
// TypedPgClient — structured queries on PostgresClient
// ─────────────────────────────────────────────────────────────────────────────

/// Thin typed layer on top of `PostgresClient`.
///
/// Wraps raw wire protocol operations with response parsing to provide
/// structured query results. Does not own the client — borrows it mutably.
pub struct TypedPgClient<'a> {
    client: &'a mut PostgresClient,
}

impl<'a> TypedPgClient<'a> {
    /// Wrap a `PostgresClient` for typed queries.
    pub fn new(client: &'a mut PostgresClient) -> Self {
        Self { client }
    }

    /// Execute a simple query and return parsed rows.
    ///
    /// Sends a Q message, reads the full response, and parses it into rows.
    /// Multiple statements in a single query are supported — all result rows
    /// are flattened into one `Vec`.
    ///
    /// Returns an error if the server sends an ErrorResponse.
    pub async fn simple_query(&mut self, sql: &str) -> ResultEP<Vec<PgSimpleRow>> {
        let q_msg = build_query_message(sql);
        let (raw, _) = self.client.send_query_raw(&q_msg).await?;
        parse_simple_query_response(&raw)
    }

    /// Execute a SQL statement that returns no rows (DDL, DML).
    ///
    /// Returns an error if the server sends an ErrorResponse.
    pub async fn batch_execute(&mut self, sql: &str) -> ResultEP<()> {
        let q_msg = build_query_message(sql);
        let (raw, _) = self.client.send_query_raw(&q_msg).await?;

        // Scan for ErrorResponse
        check_for_error(&raw)
    }

    // ─────────────────────────────────────────────────────────────────────
    // Transaction support
    // ─────────────────────────────────────────────────────────────────────

    /// Begin a transaction (`BEGIN`).
    pub async fn begin(&mut self) -> ResultEP<()> {
        self.batch_execute("BEGIN").await
    }

    /// Commit the current transaction (`COMMIT`).
    pub async fn commit(&mut self) -> ResultEP<()> {
        self.batch_execute("COMMIT").await
    }

    /// Roll back the current transaction (`ROLLBACK`).
    pub async fn rollback(&mut self) -> ResultEP<()> {
        self.batch_execute("ROLLBACK").await
    }

    /// Get the current transaction status byte.
    /// `b'I'` = idle, `b'T'` = in transaction, `b'E'` = failed transaction.
    pub fn transaction_status(&self) -> u8 {
        self.client.transaction_status()
    }

    // ─────────────────────────────────────────────────────────────────────
    // COPY operations
    // ─────────────────────────────────────────────────────────────────────

    /// Execute COPY TO STDOUT and return the raw data.
    pub async fn copy_out(&mut self, sql: &str) -> ResultEP<Vec<u8>> {
        self.client.copy_out(sql).await
    }

    /// Execute COPY FROM STDIN with the given data.
    /// Returns the number of rows copied.
    pub async fn copy_in(&mut self, sql: &str, data: &[u8]) -> ResultEP<u64> {
        self.client.copy_in(sql, data).await
    }

    // ─────────────────────────────────────────────────────────────────────
    // Accessors
    // ─────────────────────────────────────────────────────────────────────

    /// Get a reference to the underlying client.
    pub fn client(&self) -> &PostgresClient {
        self.client
    }

    /// Get a mutable reference to the underlying client.
    pub fn client_mut(&mut self) -> &mut PostgresClient {
        self.client
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Response parsing
// ─────────────────────────────────────────────────────────────────────────────

/// Parse a simple query response into rows.
///
/// Expected message flow: `[RowDescription DataRow* CommandComplete]* ReadyForQuery`
/// ErrorResponse may appear instead of RowDescription.
///
/// Also handles extended query protocol responses (skips ParseComplete, BindComplete, NoData).
pub fn parse_simple_query_response(raw: &[u8]) -> ResultEP<Vec<PgSimpleRow>> {
    let mut rows = Vec::new();
    let mut current_columns: Option<Vec<ColumnMeta>> = None;
    let mut offset = 0;

    while offset < raw.len() {
        if raw.len() - offset < 5 {
            break;
        }

        let msg_type = raw[offset];
        let length = i32::from_be_bytes([raw[offset + 1], raw[offset + 2], raw[offset + 3], raw[offset + 4]]) as usize;
        let total = 1 + length;

        if raw.len() - offset < total {
            break;
        }

        match msg_type {
            backend::ROW_DESCRIPTION => {
                let stream = SliceStream::new(&raw[offset..offset + total]);
                let rd = RowDescription::parse_sync(&stream).map_err(|e| EpError::parse(format!("Failed to parse RowDescription: {e}")))?;
                current_columns = Some(rd.fields.iter().map(|f| ColumnMeta { name: f.name.clone(), type_oid: f.type_oid }).collect());
            }

            backend::DATA_ROW => {
                let stream = SliceStream::new(&raw[offset..offset + total]);
                let dr = DataRow::parse_sync(&stream).map_err(|e| EpError::parse(format!("Failed to parse DataRow: {e}")))?;

                let columns = current_columns.as_ref().ok_or_else(|| EpError::parse("DataRow without preceding RowDescription"))?;

                let values: Vec<Option<String>> = dr.columns.iter().map(|cv| cv.as_str().map(|s| s.to_string())).collect();

                rows.push(PgSimpleRow { columns: columns.clone(), values });
            }

            backend::COMMAND_COMPLETE => {
                // Statement complete — reset columns for potential next statement
                current_columns = None;
            }

            backend::ERROR_RESPONSE => {
                let stream = SliceStream::new(&raw[offset..offset + total]);
                let err = ErrorResponse::parse_sync(&stream).map_err(|e| EpError::parse(format!("Failed to parse ErrorResponse: {e}")))?;
                let msg = err.message().unwrap_or("Unknown error");
                let code = err.code().unwrap_or("?????");
                return Err(EpError::request(format!("PostgreSQL error [{code}]: {msg}")));
            }

            backend::READY_FOR_QUERY => {
                // Done
                break;
            }

            backend::EMPTY_QUERY_RESPONSE | backend::NOTICE_RESPONSE => {
                // Skip
            }

            _ => {
                // Skip unknown message types
            }
        }

        offset += total;
    }

    Ok(rows)
}

/// Parse a simple query response into per-statement results.
///
/// Unlike `parse_simple_query_response()` which flattens all rows, this function
/// groups results by statement. Each statement produces either:
/// - `StatementResult::Rows` — for SELECT-like statements (has RowDescription)
/// - `StatementResult::Command` — for DML/DDL statements (no RowDescription)
pub fn parse_simple_query_statements(raw: &[u8]) -> ResultEP<Vec<StatementResult>> {
    let mut results = Vec::new();
    let mut current_columns: Option<Vec<ColumnMeta>> = None;
    let mut current_rows = Vec::new();
    let mut offset = 0;

    while offset < raw.len() {
        if raw.len() - offset < 5 {
            break;
        }

        let msg_type = raw[offset];
        let length = i32::from_be_bytes([raw[offset + 1], raw[offset + 2], raw[offset + 3], raw[offset + 4]]) as usize;
        let total = 1 + length;

        if raw.len() - offset < total {
            break;
        }

        match msg_type {
            backend::ROW_DESCRIPTION => {
                let stream = SliceStream::new(&raw[offset..offset + total]);
                let rd = RowDescription::parse_sync(&stream).map_err(|e| EpError::parse(format!("Failed to parse RowDescription: {e}")))?;
                current_columns = Some(rd.fields.iter().map(|f| ColumnMeta { name: f.name.clone(), type_oid: f.type_oid }).collect());
            }

            backend::DATA_ROW => {
                let stream = SliceStream::new(&raw[offset..offset + total]);
                let dr = DataRow::parse_sync(&stream).map_err(|e| EpError::parse(format!("Failed to parse DataRow: {e}")))?;

                let columns = current_columns.as_ref().ok_or_else(|| EpError::parse("DataRow without preceding RowDescription"))?;

                let values: Vec<Option<String>> = dr.columns.iter().map(|cv| cv.as_str().map(|s| s.to_string())).collect();

                current_rows.push(PgSimpleRow { columns: columns.clone(), values });
            }

            backend::COMMAND_COMPLETE => {
                if current_columns.is_some() {
                    // Had RowDescription — this is a SELECT-like statement
                    results.push(StatementResult::Rows(std::mem::take(&mut current_rows)));
                } else {
                    // No RowDescription — DML/DDL, extract affected row count
                    let tag_bytes = &raw[offset + 5..offset + total];
                    let mut affected_rows = 0u64;
                    if let Some(null_pos) = tag_bytes.iter().position(|&b| b == 0) {
                        let tag = String::from_utf8_lossy(&tag_bytes[..null_pos]);
                        if let Some(count_str) = tag.rsplit(' ').next()
                            && let Ok(count) = count_str.parse::<u64>()
                        {
                            affected_rows = count;
                        }
                    }
                    results.push(StatementResult::Command { affected_rows });
                }
                current_columns = None;
            }

            backend::ERROR_RESPONSE => {
                let stream = SliceStream::new(&raw[offset..offset + total]);
                let err = ErrorResponse::parse_sync(&stream).map_err(|e| EpError::parse(format!("Failed to parse ErrorResponse: {e}")))?;
                let msg = err.message().unwrap_or("Unknown error");
                let code = err.code().unwrap_or("?????");
                return Err(EpError::request(format!("PostgreSQL error [{code}]: {msg}")));
            }

            backend::READY_FOR_QUERY => {
                break;
            }

            backend::EMPTY_QUERY_RESPONSE | backend::NOTICE_RESPONSE => {
                // Skip
            }

            _ => {
                // Skip unknown message types (ParseComplete, BindComplete, etc.)
            }
        }

        offset += total;
    }

    Ok(results)
}

/// Check a raw response for ErrorResponse messages.
/// Returns `Ok(())` if no error found, or `Err` with the error message.
pub fn check_for_error(raw: &[u8]) -> ResultEP<()> {
    let mut offset = 0;

    while offset < raw.len() {
        if raw.len() - offset < 5 {
            break;
        }

        let msg_type = raw[offset];
        let length = i32::from_be_bytes([raw[offset + 1], raw[offset + 2], raw[offset + 3], raw[offset + 4]]) as usize;
        let total = 1 + length;

        if raw.len() - offset < total {
            break;
        }

        if msg_type == backend::ERROR_RESPONSE {
            let stream = SliceStream::new(&raw[offset..offset + total]);
            let err = ErrorResponse::parse_sync(&stream).map_err(|e| EpError::parse(format!("Failed to parse ErrorResponse: {e}")))?;
            let msg = err.message().unwrap_or("Unknown error");
            let code = err.code().unwrap_or("?????");
            return Err(EpError::request(format!("PostgreSQL error [{code}]: {msg}")));
        }

        if msg_type == backend::READY_FOR_QUERY {
            break;
        }

        offset += total;
    }

    Ok(())
}

/// Extract the affected row count from a raw wire response containing CommandComplete.
///
/// CommandComplete tag format: "INSERT 0 N", "UPDATE N", "DELETE N", "SELECT N", etc.
/// The count is the last number in the tag string.
pub fn extract_command_complete_count(raw: &[u8]) -> u64 {
    let mut offset = 0;
    while offset < raw.len() {
        if raw.len() - offset < 5 {
            break;
        }
        let msg_type = raw[offset];
        let length = i32::from_be_bytes([raw[offset + 1], raw[offset + 2], raw[offset + 3], raw[offset + 4]]) as usize;
        let total = 1 + length;
        if raw.len() - offset < total {
            break;
        }
        if msg_type == backend::COMMAND_COMPLETE {
            // Tag is null-terminated string in payload
            let tag_bytes = &raw[offset + 5..offset + total];
            if let Some(null_pos) = tag_bytes.iter().position(|&b| b == 0) {
                let tag = String::from_utf8_lossy(&tag_bytes[..null_pos]);
                // Last word in tag is the count
                if let Some(count_str) = tag.rsplit(' ').next()
                    && let Ok(count) = count_str.parse::<u64>()
                {
                    return count;
                }
            }
        }
        if msg_type == backend::READY_FOR_QUERY {
            break;
        }
        offset += total;
    }
    0
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use postgres_wire::types::row_description::FieldDescription;

    #[test]
    fn text_to_json_bool() {
        assert_eq!(text_to_json("t", type_oid::BOOL), serde_json::Value::Bool(true));
        assert_eq!(text_to_json("f", type_oid::BOOL), serde_json::Value::Bool(false));
    }

    #[test]
    fn text_to_json_integer() {
        assert_eq!(text_to_json("42", type_oid::INT4), serde_json::json!(42));
        assert_eq!(text_to_json("-7", type_oid::INT8), serde_json::json!(-7));
        assert_eq!(text_to_json("0", type_oid::INT2), serde_json::json!(0));
    }

    #[test]
    fn text_to_json_float() {
        assert_eq!(text_to_json("3.15", type_oid::FLOAT8), serde_json::json!(3.15));
        assert_eq!(text_to_json("1.5", type_oid::NUMERIC), serde_json::json!(1.5));
    }

    #[test]
    fn text_to_json_text() {
        assert_eq!(text_to_json("hello", type_oid::TEXT), serde_json::json!("hello"));
        assert_eq!(text_to_json("world", type_oid::VARCHAR), serde_json::json!("world"));
    }

    #[test]
    fn text_to_json_json_passthrough() {
        assert_eq!(text_to_json(r#"{"key": "value"}"#, type_oid::JSON), serde_json::json!({"key": "value"}));
        assert_eq!(text_to_json("[1,2,3]", type_oid::JSONB), serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn parse_pg_text_array_basic() {
        let result = parse_pg_text_array("{foo,bar,baz}", 0);
        assert_eq!(result, serde_json::json!(["foo", "bar", "baz"]));
    }

    #[test]
    fn parse_pg_text_array_empty() {
        let result = parse_pg_text_array("{}", 0);
        assert_eq!(result, serde_json::json!([]));
    }

    #[test]
    fn parse_pg_text_array_with_nulls() {
        let result = parse_pg_text_array("{a,NULL,b}", 0);
        assert_eq!(result, serde_json::json!(["a", null, "b"]));
    }

    #[test]
    fn parse_pg_text_array_quoted() {
        let result = parse_pg_text_array(r#"{"hello world","foo\"bar"}"#, 0);
        let arr = result.as_array().expect("should be array");
        assert_eq!(arr[0], serde_json::json!("hello world"));
        assert_eq!(arr[1], serde_json::json!("foo\"bar"));
    }

    #[test]
    fn parse_pg_text_array_typed_int() {
        let result = parse_pg_text_array("{1,2,3}", type_oid::INT4);
        assert_eq!(result, serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn pg_simple_row_get_by_name() {
        let row = PgSimpleRow {
            columns: vec![
                ColumnMeta { name: "id".into(), type_oid: type_oid::INT4 },
                ColumnMeta { name: "name".into(), type_oid: type_oid::TEXT },
            ],
            values: vec![Some("42".into()), Some("alice".into())],
        };
        assert_eq!(row.get("id"), Some("42"));
        assert_eq!(row.get("name"), Some("alice"));
        assert_eq!(row.get("missing"), None);
    }

    #[test]
    fn pg_simple_row_to_json() {
        let row = PgSimpleRow {
            columns: vec![
                ColumnMeta { name: "id".into(), type_oid: type_oid::INT8 },
                ColumnMeta { name: "name".into(), type_oid: type_oid::TEXT },
                ColumnMeta { name: "active".into(), type_oid: type_oid::BOOL },
                ColumnMeta { name: "score".into(), type_oid: type_oid::FLOAT8 },
                ColumnMeta { name: "nullable".into(), type_oid: type_oid::TEXT },
            ],
            values: vec![Some("100".into()), Some("bob".into()), Some("t".into()), Some("9.5".into()), None],
        };

        let json = row.to_json();
        assert_eq!(json["id"], serde_json::json!(100));
        assert_eq!(json["name"], serde_json::json!("bob"));
        assert_eq!(json["active"], serde_json::json!(true));
        assert_eq!(json["score"], serde_json::json!(9.5));
        assert!(json["nullable"].is_null());
    }

    #[test]
    fn parse_simple_query_response_empty() {
        // CommandComplete('C') + ReadyForQuery('Z')
        // CommandComplete: 'C' + len(4) + "SELECT 0\0"
        let mut raw = Vec::new();
        let tag = b"SELECT 0\0";
        let cc_len = (4 + tag.len()) as i32;
        raw.push(b'C');
        raw.extend_from_slice(&cc_len.to_be_bytes());
        raw.extend_from_slice(tag);
        // ReadyForQuery: 'Z' + len(4)=5 + status='I'
        raw.push(b'Z');
        raw.extend_from_slice(&5i32.to_be_bytes());
        raw.push(b'I');

        let rows = parse_simple_query_response(&raw).expect("parse should succeed");
        assert!(rows.is_empty());
    }

    #[test]
    fn parse_simple_query_response_with_rows() {
        let mut raw = Vec::new();

        // RowDescription: one column "val" of type TEXT (25)
        {
            let rd = RowDescription::new(vec![FieldDescription {
                name: "val".into(),
                table_oid: 0,
                column_id: 0,
                type_oid: type_oid::TEXT,
                type_size: -1,
                type_modifier: -1,
                format_code: 0,
            }]);
            raw.extend_from_slice(&rd.encode());
        }

        // DataRow: one column with value "hello"
        {
            let dr = DataRow::new(vec![postgres_wire::types::ColumnValue::Value(b"hello".to_vec())]);
            raw.extend_from_slice(&dr.encode());
        }

        // CommandComplete
        let tag = b"SELECT 1\0";
        let cc_len = (4 + tag.len()) as i32;
        raw.push(b'C');
        raw.extend_from_slice(&cc_len.to_be_bytes());
        raw.extend_from_slice(tag);

        // ReadyForQuery
        raw.push(b'Z');
        raw.extend_from_slice(&5i32.to_be_bytes());
        raw.push(b'I');

        let rows = parse_simple_query_response(&raw).expect("parse should succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("val"), Some("hello"));
    }

    #[test]
    fn parse_simple_query_response_error() {
        let mut raw = Vec::new();

        // ErrorResponse
        let err = ErrorResponse::simple("ERROR", "42P01", "relation \"foo\" does not exist");
        raw.extend_from_slice(&err.encode());

        // ReadyForQuery
        raw.push(b'Z');
        raw.extend_from_slice(&5i32.to_be_bytes());
        raw.push(b'I');

        let result = parse_simple_query_response(&raw);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("42P01"), "should contain SQLSTATE: {err_msg}");
        assert!(err_msg.contains("does not exist"), "should contain message: {err_msg}");
    }

    #[test]
    fn check_for_error_ok() {
        let mut raw = Vec::new();
        // CommandComplete
        let tag = b"CREATE TABLE\0";
        let cc_len = (4 + tag.len()) as i32;
        raw.push(b'C');
        raw.extend_from_slice(&cc_len.to_be_bytes());
        raw.extend_from_slice(tag);
        // ReadyForQuery
        raw.push(b'Z');
        raw.extend_from_slice(&5i32.to_be_bytes());
        raw.push(b'I');

        assert!(check_for_error(&raw).is_ok());
    }

    #[test]
    fn check_for_error_with_error() {
        let mut raw = Vec::new();
        let err = ErrorResponse::simple("ERROR", "42601", "syntax error");
        raw.extend_from_slice(&err.encode());
        raw.push(b'Z');
        raw.extend_from_slice(&5i32.to_be_bytes());
        raw.push(b'I');

        let result = check_for_error(&raw);
        assert!(result.is_err());
    }

    // ── text_to_json edge cases ──────────────────────────────────────────

    #[test]
    fn text_to_json_bool_variants() {
        // All accepted truthy values
        assert_eq!(text_to_json("t", type_oid::BOOL), serde_json::Value::Bool(true));
        assert_eq!(text_to_json("true", type_oid::BOOL), serde_json::Value::Bool(true));
        assert_eq!(text_to_json("TRUE", type_oid::BOOL), serde_json::Value::Bool(true));
        assert_eq!(text_to_json("1", type_oid::BOOL), serde_json::Value::Bool(true));
        // All accepted falsy values
        assert_eq!(text_to_json("f", type_oid::BOOL), serde_json::Value::Bool(false));
        assert_eq!(text_to_json("false", type_oid::BOOL), serde_json::Value::Bool(false));
        assert_eq!(text_to_json("FALSE", type_oid::BOOL), serde_json::Value::Bool(false));
        assert_eq!(text_to_json("0", type_oid::BOOL), serde_json::Value::Bool(false));
        // Unexpected value falls back to string
        assert_eq!(text_to_json("yes", type_oid::BOOL), serde_json::json!("yes"));
    }

    #[test]
    fn text_to_json_integer_boundaries() {
        assert_eq!(text_to_json("32767", type_oid::INT2), serde_json::json!(32767));
        assert_eq!(text_to_json("-32768", type_oid::INT2), serde_json::json!(-32768));
        assert_eq!(text_to_json("2147483647", type_oid::INT4), serde_json::json!(2147483647_i64));
        assert_eq!(text_to_json("-2147483648", type_oid::INT4), serde_json::json!(-2147483648_i64));
        assert_eq!(text_to_json("9223372036854775807", type_oid::INT8), serde_json::json!(i64::MAX));
        assert_eq!(text_to_json("-9223372036854775808", type_oid::INT8), serde_json::json!(i64::MIN));
        // Invalid integer falls back to string
        assert_eq!(text_to_json("not_a_number", type_oid::INT4), serde_json::json!("not_a_number"));
    }

    #[test]
    fn text_to_json_oid_and_xid() {
        assert_eq!(text_to_json("12345", type_oid::OID), serde_json::json!(12345));
        assert_eq!(text_to_json("67890", type_oid::XID), serde_json::json!(67890));
    }

    #[test]
    fn text_to_json_xid8() {
        assert_eq!(text_to_json("12345678901", type_oid::XID8), serde_json::json!(12345678901_i64));
        assert_eq!(text_to_json("0", type_oid::XID8), serde_json::json!(0));
    }

    #[test]
    fn text_to_json_cid() {
        assert_eq!(text_to_json("0", type_oid::CID), serde_json::json!(0));
        assert_eq!(text_to_json("42", type_oid::CID), serde_json::json!(42));
    }

    #[test]
    fn text_to_json_float_special() {
        // NaN and Infinity become null
        assert_eq!(text_to_json("NaN", type_oid::FLOAT4), serde_json::Value::Null);
        assert_eq!(text_to_json("NaN", type_oid::FLOAT8), serde_json::Value::Null);
        assert_eq!(text_to_json("Infinity", type_oid::FLOAT8), serde_json::Value::Null);
        assert_eq!(text_to_json("-Infinity", type_oid::FLOAT8), serde_json::Value::Null);
        assert_eq!(text_to_json("NaN", type_oid::NUMERIC), serde_json::Value::Null);
        // Normal float values
        assert_eq!(text_to_json("0", type_oid::FLOAT8), serde_json::json!(0.0));
        assert_eq!(text_to_json("-0", type_oid::FLOAT8), serde_json::json!(0.0));
        assert_eq!(text_to_json("1e10", type_oid::FLOAT8), serde_json::json!(1e10));
        assert_eq!(text_to_json("1.23e-4", type_oid::FLOAT8), serde_json::json!(1.23e-4));
    }

    #[test]
    fn text_to_json_numeric_precision() {
        // NUMERIC with high precision — note that f64 has limited precision
        assert_eq!(text_to_json("123.45", type_oid::NUMERIC), serde_json::json!(123.45));
        assert_eq!(text_to_json("0.01", type_oid::NUMERIC), serde_json::json!(0.01));
        assert_eq!(text_to_json("99999.99", type_oid::NUMERIC), serde_json::json!(99999.99));
        // Very large NUMERIC values that fit in f64
        assert_eq!(text_to_json("1000000000", type_oid::NUMERIC), serde_json::json!(1e9));
    }

    #[test]
    fn text_to_json_json_invalid() {
        // Invalid JSON falls back to string
        assert_eq!(text_to_json("{invalid json", type_oid::JSON), serde_json::json!("{invalid json"));
        assert_eq!(text_to_json("", type_oid::JSONB), serde_json::json!(""));
    }

    #[test]
    fn text_to_json_passthrough_types() {
        // All types not in the match arms return as strings
        assert_eq!(text_to_json("192.168.1.1", type_oid::INET), serde_json::json!("192.168.1.1"));
        assert_eq!(text_to_json("10.0.0.0/8", type_oid::CIDR), serde_json::json!("10.0.0.0/8"));
        assert_eq!(text_to_json("08:00:2b:01:02:03", type_oid::MACADDR), serde_json::json!("08:00:2b:01:02:03"));
        assert_eq!(text_to_json("2024-06-15", type_oid::DATE), serde_json::json!("2024-06-15"));
        assert_eq!(text_to_json("14:30:00", type_oid::TIME), serde_json::json!("14:30:00"));
        assert_eq!(text_to_json("14:30:00+05:30", type_oid::TIMETZ), serde_json::json!("14:30:00+05:30"));
        assert_eq!(text_to_json("2024-06-15 14:30:00", type_oid::TIMESTAMP), serde_json::json!("2024-06-15 14:30:00"));
        assert_eq!(
            text_to_json("2024-06-15 14:30:00+00", type_oid::TIMESTAMPTZ),
            serde_json::json!("2024-06-15 14:30:00+00")
        );
        assert_eq!(text_to_json("1 year 2 mons 3 days", type_oid::INTERVAL), serde_json::json!("1 year 2 mons 3 days"));
        assert_eq!(text_to_json("\\xDEADBEEF", type_oid::BYTEA), serde_json::json!("\\xDEADBEEF"));
        assert_eq!(text_to_json("$1,234.56", type_oid::MONEY), serde_json::json!("$1,234.56"));
        assert_eq!(text_to_json("101010", type_oid::BIT), serde_json::json!("101010"));
        assert_eq!(text_to_json("101", type_oid::VARBIT), serde_json::json!("101"));
        assert_eq!(text_to_json("(1.5,2.5)", type_oid::POINT), serde_json::json!("(1.5,2.5)"));
        assert_eq!(text_to_json("[(0,0),(1,1)]", type_oid::LSEG), serde_json::json!("[(0,0),(1,1)]"));
        assert_eq!(text_to_json("(0,0),(1,1)", type_oid::BOX), serde_json::json!("(0,0),(1,1)"));
        assert_eq!(text_to_json("<(0,0),5>", type_oid::CIRCLE), serde_json::json!("<(0,0),5>"));
        assert_eq!(text_to_json("a]b", 99999), serde_json::json!("a]b")); // unknown OID
    }

    // ── Array type mapping tests ─────────────────────────────────────────

    #[test]
    fn parse_pg_text_array_typed_bool() {
        let result = parse_pg_text_array("{t,f,t}", type_oid::BOOL);
        assert_eq!(result, serde_json::json!([true, false, true]));
    }

    #[test]
    fn parse_pg_text_array_typed_int2() {
        let result = parse_pg_text_array("{1,-2,32767}", type_oid::INT2);
        assert_eq!(result, serde_json::json!([1, -2, 32767]));
    }

    #[test]
    fn parse_pg_text_array_typed_int8() {
        let result = parse_pg_text_array("{100,-200,9999999999}", type_oid::INT8);
        assert_eq!(result, serde_json::json!([100, -200, 9999999999_i64]));
    }

    #[test]
    fn parse_pg_text_array_typed_float4() {
        let result = parse_pg_text_array("{1.5,2.5,-3.15}", type_oid::FLOAT4);
        assert_eq!(result, serde_json::json!([1.5, 2.5, -3.15]));
    }

    #[test]
    fn parse_pg_text_array_typed_float8() {
        let result = parse_pg_text_array("{3.24159,2.81828}", type_oid::FLOAT8);
        assert_eq!(result, serde_json::json!([3.24159, 2.81828]));
    }

    #[test]
    fn parse_pg_text_array_typed_numeric() {
        let result = parse_pg_text_array("{123.45,0.01,99999.99}", type_oid::NUMERIC);
        assert_eq!(result, serde_json::json!([123.45, 0.01, 99999.99]));
    }

    #[test]
    fn parse_pg_text_array_typed_text() {
        let result = parse_pg_text_array("{hello,world}", type_oid::TEXT);
        assert_eq!(result, serde_json::json!(["hello", "world"]));
    }

    #[test]
    fn parse_pg_text_array_typed_varchar() {
        let result = parse_pg_text_array("{foo,bar}", type_oid::VARCHAR);
        assert_eq!(result, serde_json::json!(["foo", "bar"]));
    }

    #[test]
    fn parse_pg_text_array_typed_uuid() {
        let result = parse_pg_text_array("{550e8400-e29b-41d4-a716-446655440000}", type_oid::UUID);
        assert_eq!(result, serde_json::json!(["550e8400-e29b-41d4-a716-446655440000"]));
    }

    #[test]
    fn parse_pg_text_array_typed_date() {
        let result = parse_pg_text_array("{2024-01-01,2024-12-31}", type_oid::DATE);
        assert_eq!(result, serde_json::json!(["2024-01-01", "2024-12-31"]));
    }

    #[test]
    fn parse_pg_text_array_typed_timestamp() {
        let result = parse_pg_text_array(r#"{"2024-01-01 12:00:00","2024-12-31 23:59:59"}"#, type_oid::TIMESTAMP);
        assert_eq!(result, serde_json::json!(["2024-01-01 12:00:00", "2024-12-31 23:59:59"]));
    }

    #[test]
    fn parse_pg_text_array_typed_json() {
        let result = parse_pg_text_array(r#"{"{\"a\":1}","{\"b\":2}"}"#, type_oid::JSON);
        assert_eq!(result, serde_json::json!([{"a": 1}, {"b": 2}]));
    }

    #[test]
    fn parse_pg_text_array_typed_jsonb() {
        let result = parse_pg_text_array(r#"{"[1,2]","[3,4]"}"#, type_oid::JSONB);
        assert_eq!(result, serde_json::json!([[1, 2], [3, 4]]));
    }

    #[test]
    fn parse_pg_text_array_with_nulls_typed() {
        let result = parse_pg_text_array("{1,NULL,3}", type_oid::INT4);
        assert_eq!(result, serde_json::json!([1, null, 3]));
    }

    #[test]
    fn parse_pg_text_array_float_with_nan() {
        let result = parse_pg_text_array("{1.5,NaN,-Infinity}", type_oid::FLOAT8);
        let arr = result.as_array().expect("should be array");
        assert_eq!(arr[0], serde_json::json!(1.5));
        assert!(arr[1].is_null()); // NaN → null
        assert!(arr[2].is_null()); // -Infinity → null
    }

    // ── Array type OID routing via text_to_json ──────────────────────────

    #[test]
    fn text_to_json_array_int4() {
        let result = text_to_json("{1,2,3}", type_oid::INT4_ARRAY);
        assert_eq!(result, serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn text_to_json_array_int2() {
        let result = text_to_json("{10,20}", type_oid::INT2_ARRAY);
        assert_eq!(result, serde_json::json!([10, 20]));
    }

    #[test]
    fn text_to_json_array_int8() {
        let result = text_to_json("{9999999999}", type_oid::INT8_ARRAY);
        assert_eq!(result, serde_json::json!([9999999999_i64]));
    }

    #[test]
    fn text_to_json_array_float4() {
        let result = text_to_json("{1.5,2.5}", type_oid::FLOAT4_ARRAY);
        assert_eq!(result, serde_json::json!([1.5, 2.5]));
    }

    #[test]
    fn text_to_json_array_float8() {
        let result = text_to_json("{3.15}", type_oid::FLOAT8_ARRAY);
        assert_eq!(result, serde_json::json!([3.15]));
    }

    #[test]
    fn text_to_json_array_bool() {
        let result = text_to_json("{t,f,t}", type_oid::BOOL_ARRAY);
        assert_eq!(result, serde_json::json!([true, false, true]));
    }

    #[test]
    fn text_to_json_array_text() {
        let result = text_to_json("{hello,world}", type_oid::TEXT_ARRAY);
        assert_eq!(result, serde_json::json!(["hello", "world"]));
    }

    #[test]
    fn text_to_json_array_varchar() {
        let result = text_to_json("{a,b}", type_oid::VARCHAR_ARRAY);
        assert_eq!(result, serde_json::json!(["a", "b"]));
    }

    #[test]
    fn text_to_json_array_numeric() {
        let result = text_to_json("{1.23,4.56}", type_oid::NUMERIC_ARRAY);
        assert_eq!(result, serde_json::json!([1.23, 4.56]));
    }

    #[test]
    fn text_to_json_array_uuid() {
        let result = text_to_json("{550e8400-e29b-41d4-a716-446655440000}", type_oid::UUID_ARRAY);
        assert_eq!(result, serde_json::json!(["550e8400-e29b-41d4-a716-446655440000"]));
    }

    #[test]
    fn text_to_json_array_date() {
        let result = text_to_json("{2024-01-01}", type_oid::DATE_ARRAY);
        assert_eq!(result, serde_json::json!(["2024-01-01"]));
    }

    #[test]
    fn text_to_json_array_time() {
        let result = text_to_json("{14:30:00}", type_oid::TIME_ARRAY);
        assert_eq!(result, serde_json::json!(["14:30:00"]));
    }

    #[test]
    fn text_to_json_array_timestamp() {
        let result = text_to_json(r#"{"2024-01-01 12:00:00"}"#, type_oid::TIMESTAMP_ARRAY);
        assert_eq!(result, serde_json::json!(["2024-01-01 12:00:00"]));
    }

    #[test]
    fn text_to_json_array_timestamptz() {
        let result = text_to_json(r#"{"2024-01-01 12:00:00+00"}"#, type_oid::TIMESTAMPTZ_ARRAY);
        assert_eq!(result, serde_json::json!(["2024-01-01 12:00:00+00"]));
    }

    #[test]
    fn text_to_json_array_interval() {
        let result = text_to_json(r#"{"1 day","2 hours"}"#, type_oid::INTERVAL_ARRAY);
        assert_eq!(result, serde_json::json!(["1 day", "2 hours"]));
    }

    #[test]
    fn text_to_json_array_json() {
        let result = text_to_json(r#"{"{\"a\":1}"}"#, type_oid::JSON_ARRAY);
        assert_eq!(result, serde_json::json!([{"a": 1}]));
    }

    #[test]
    fn text_to_json_array_jsonb() {
        let result = text_to_json(r#"{"[1,2]"}"#, type_oid::JSONB_ARRAY);
        assert_eq!(result, serde_json::json!([[1, 2]]));
    }

    #[test]
    fn text_to_json_array_inet() {
        let result = text_to_json("{192.168.1.1,::1}", type_oid::INET_ARRAY);
        assert_eq!(result, serde_json::json!(["192.168.1.1", "::1"]));
    }

    #[test]
    fn text_to_json_array_cidr() {
        let result = text_to_json("{10.0.0.0/8}", type_oid::CIDR_ARRAY);
        assert_eq!(result, serde_json::json!(["10.0.0.0/8"]));
    }

    #[test]
    fn text_to_json_array_macaddr() {
        let result = text_to_json("{08:00:2b:01:02:03}", type_oid::MACADDR_ARRAY);
        assert_eq!(result, serde_json::json!(["08:00:2b:01:02:03"]));
    }

    #[test]
    fn text_to_json_array_bytea() {
        // bytea array elements are hex-escaped
        let result = text_to_json(r#"{"\\xDEADBEEF"}"#, type_oid::BYTEA_ARRAY);
        assert_eq!(result, serde_json::json!(["\\xDEADBEEF"]));
    }

    #[test]
    fn text_to_json_array_money() {
        // money array elements are strings (e.g., "$1.50") — mapped to MONEY which falls to string
        let result = text_to_json(r#"{"$1.50","$2.75"}"#, type_oid::MONEY_ARRAY);
        assert_eq!(result, serde_json::json!(["$1.50", "$2.75"]));
    }

    // ── parse_pg_text_array edge cases ───────────────────────────────────

    #[test]
    fn parse_pg_text_array_single_element() {
        let result = parse_pg_text_array("{42}", type_oid::INT4);
        assert_eq!(result, serde_json::json!([42]));
    }

    #[test]
    fn parse_pg_text_array_quoted_with_commas() {
        let result = parse_pg_text_array(r#"{"a,b","c,d"}"#, 0);
        assert_eq!(result, serde_json::json!(["a,b", "c,d"]));
    }

    #[test]
    fn parse_pg_text_array_quoted_with_braces() {
        let result = parse_pg_text_array(r#"{"a{b","c}d"}"#, 0);
        assert_eq!(result, serde_json::json!(["a{b", "c}d"]));
    }

    #[test]
    fn parse_pg_text_array_quoted_with_backslash() {
        let result = parse_pg_text_array(r#"{"a\\b","c\\d"}"#, 0);
        assert_eq!(result, serde_json::json!(["a\\b", "c\\d"]));
    }

    #[test]
    fn parse_pg_text_array_all_nulls() {
        let result = parse_pg_text_array("{NULL,NULL,NULL}", type_oid::INT4);
        assert_eq!(result, serde_json::json!([null, null, null]));
    }

    #[test]
    fn parse_pg_text_array_not_array_format() {
        // Non-array input returns as string
        let result = parse_pg_text_array("not an array", 0);
        assert_eq!(result, serde_json::json!("not an array"));
    }

    // ── parse_simple_query_statements tests ──────────────────────────────

    #[test]
    fn parse_statements_single_command() {
        let mut raw = Vec::new();
        let tag = b"INSERT 0 5\0";
        let cc_len = (4 + tag.len()) as i32;
        raw.push(b'C');
        raw.extend_from_slice(&cc_len.to_be_bytes());
        raw.extend_from_slice(tag);
        raw.push(b'Z');
        raw.extend_from_slice(&5i32.to_be_bytes());
        raw.push(b'I');

        let stmts = parse_simple_query_statements(&raw).expect("parse should succeed");
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            StatementResult::Command { affected_rows } => assert_eq!(*affected_rows, 5),
            _ => panic!("Expected Command"),
        }
    }

    #[test]
    fn parse_statements_mixed() {
        let mut raw = Vec::new();

        // First statement: INSERT 0 3
        let tag1 = b"INSERT 0 3\0";
        let cc_len1 = (4 + tag1.len()) as i32;
        raw.push(b'C');
        raw.extend_from_slice(&cc_len1.to_be_bytes());
        raw.extend_from_slice(tag1);

        // Second statement: SELECT with 1 row
        {
            let rd = RowDescription::new(vec![FieldDescription {
                name: "x".into(),
                table_oid: 0,
                column_id: 0,
                type_oid: type_oid::INT4,
                type_size: 4,
                type_modifier: -1,
                format_code: 0,
            }]);
            raw.extend_from_slice(&rd.encode());
            let dr = DataRow::new(vec![postgres_wire::types::ColumnValue::Value(b"42".to_vec())]);
            raw.extend_from_slice(&dr.encode());
            let tag2 = b"SELECT 1\0";
            let cc_len2 = (4 + tag2.len()) as i32;
            raw.push(b'C');
            raw.extend_from_slice(&cc_len2.to_be_bytes());
            raw.extend_from_slice(tag2);
        }

        // ReadyForQuery
        raw.push(b'Z');
        raw.extend_from_slice(&5i32.to_be_bytes());
        raw.push(b'I');

        let stmts = parse_simple_query_statements(&raw).expect("parse should succeed");
        assert_eq!(stmts.len(), 2);
        match &stmts[0] {
            StatementResult::Command { affected_rows } => assert_eq!(*affected_rows, 3),
            _ => panic!("Expected Command for first statement"),
        }
        match &stmts[1] {
            StatementResult::Rows(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("x"), Some("42"));
            }
            _ => panic!("Expected Rows for second statement"),
        }
    }

    // ── extract_command_complete_count tests ──────────────────────────────

    #[test]
    fn extract_count_insert() {
        let mut raw = Vec::new();
        let tag = b"INSERT 0 10\0";
        let cc_len = (4 + tag.len()) as i32;
        raw.push(b'C');
        raw.extend_from_slice(&cc_len.to_be_bytes());
        raw.extend_from_slice(tag);
        raw.push(b'Z');
        raw.extend_from_slice(&5i32.to_be_bytes());
        raw.push(b'I');

        assert_eq!(extract_command_complete_count(&raw), 10);
    }

    #[test]
    fn extract_count_create_table() {
        let mut raw = Vec::new();
        let tag = b"CREATE TABLE\0";
        let cc_len = (4 + tag.len()) as i32;
        raw.push(b'C');
        raw.extend_from_slice(&cc_len.to_be_bytes());
        raw.extend_from_slice(tag);
        raw.push(b'Z');
        raw.extend_from_slice(&5i32.to_be_bytes());
        raw.push(b'I');

        assert_eq!(extract_command_complete_count(&raw), 0);
    }

    #[test]
    fn extract_count_update() {
        let mut raw = Vec::new();
        let tag = b"UPDATE 7\0";
        let cc_len = (4 + tag.len()) as i32;
        raw.push(b'C');
        raw.extend_from_slice(&cc_len.to_be_bytes());
        raw.extend_from_slice(tag);
        raw.push(b'Z');
        raw.extend_from_slice(&5i32.to_be_bytes());
        raw.push(b'I');

        assert_eq!(extract_command_complete_count(&raw), 7);
    }

    #[test]
    fn extract_count_delete() {
        let mut raw = Vec::new();
        let tag = b"DELETE 3\0";
        let cc_len = (4 + tag.len()) as i32;
        raw.push(b'C');
        raw.extend_from_slice(&cc_len.to_be_bytes());
        raw.extend_from_slice(tag);
        raw.push(b'Z');
        raw.extend_from_slice(&5i32.to_be_bytes());
        raw.push(b'I');

        assert_eq!(extract_command_complete_count(&raw), 3);
    }

    #[test]
    fn extract_count_copy() {
        let mut raw = Vec::new();
        let tag = b"COPY 100\0";
        let cc_len = (4 + tag.len()) as i32;
        raw.push(b'C');
        raw.extend_from_slice(&cc_len.to_be_bytes());
        raw.extend_from_slice(tag);
        raw.push(b'Z');
        raw.extend_from_slice(&5i32.to_be_bytes());
        raw.push(b'I');

        assert_eq!(extract_command_complete_count(&raw), 100);
    }
}
