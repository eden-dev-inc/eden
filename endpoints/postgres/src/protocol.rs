//! PostgreSQL raw bytes passthrough for the proxy.
//!
//! Implements `EpWireRequest<PgRawPool>` for `PostgresBytes`, following the same
//! pattern as `RedisBytes` in `endpoints/redis/src/protocol.rs`.
//!
//! The proxy sends raw PG wire Q messages through `EP::raw_bytes_with_req_type()`.
//! Raw wire bytes are forwarded directly to the backend PostgreSQL server
//! and the raw response is returned — no deserialization/re-serialization.

use bytes::Bytes;
use endpoint_types::request::EpWireRequest;
use ep_core::ReqType;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use postgres_core::PgRawPool;
use postgres_core::client::PostgresClient;
use postgres_wire::sql::{PgReqType, classify_sql_req_type};

pub use crate::ep::PgPinnedConnection;
pub use postgres_wire::sql::skip_sql_comments;

/// Raw PG wire protocol bytes for passthrough.
///
/// Wraps a Q message: `type(1) + length(4) + sql_string + '\0'`.
/// Uses `Bytes` internally for zero-copy cloning in dual-write scenarios.
#[derive(Debug, Clone)]
pub struct PostgresBytes(Bytes);

impl PostgresBytes {
    pub fn new(bytes: Bytes) -> Self {
        Self(bytes)
    }

    pub fn bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    /// Extract SQL string from the raw Q message bytes.
    ///
    /// Q message format: `b'Q' + i32(length) + sql_string + '\0'`
    pub fn extract_sql(&self) -> ResultEP<&str> {
        if self.0.len() < 6 {
            return Err(EpError::parse("Q message too short"));
        }
        // Skip type byte (1) + length (4), strip null terminator
        let sql_bytes = &self.0[5..self.0.len() - 1];
        std::str::from_utf8(sql_bytes).map_err(|e| EpError::parse(e.to_string()))
    }
}

impl From<Vec<u8>> for PostgresBytes {
    fn from(v: Vec<u8>) -> Self {
        PostgresBytes(Bytes::from(v))
    }
}

impl From<Bytes> for PostgresBytes {
    /// Zero-copy conversion from Bytes — just increments the Arc reference count.
    fn from(v: Bytes) -> Self {
        PostgresBytes(v)
    }
}

impl PostgresBytes {
    /// Execute this query on a pinned connection (raw wire protocol passthrough).
    ///
    /// Forwards wire bytes directly to the backend server and returns the raw
    /// response bytes including ReadyForQuery. The caller should NOT append
    /// a synthetic ReadyForQuery.
    pub async fn send_raw_on_pinned(&self, client: &mut PgPinnedConnection) -> ResultEP<(Bytes, u64)> {
        client.send_query_raw(&self.0).await
    }

    /// Execute this query on a raw wire protocol PostgresClient.
    ///
    /// True raw passthrough: forwards wire bytes directly to the backend server
    /// and returns the raw response bytes including ReadyForQuery.
    pub async fn send_raw_on_client(&self, client: &mut PostgresClient) -> ResultEP<(Bytes, u64)> {
        client.send_query_raw(&self.0).await
    }
}

/// Raw wire protocol passthrough using deadpool + PostgresClient.
///
/// True raw passthrough: forwards wire bytes directly to the backend PostgreSQL
/// server and returns the raw response bytes. No deserialization/re-serialization.
/// Response includes ReadyForQuery from the real server.
impl EpWireRequest<PgRawPool> for PostgresBytes {
    fn kind(&self) -> EpKind {
        EpKind::Postgres
    }

    fn request_type(&self) -> ResultEP<ReqType> {
        let sql = self.extract_sql()?;
        Ok(req_type_from_pg(classify_sql_req_type(sql)))
    }

    async fn send_raw_bytes(&self, context: &PgRawPool) -> ResultEP<(Bytes, u64)> {
        use ep_core::pool::PoisonGuard;

        let client = context.get().await.map_err(EpError::request)?;
        let mut guard = PoisonGuard::new(client);
        let result = guard.send_query_raw(&self.0).await;
        if result.is_ok() {
            guard.disarm();
        }
        result
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// SQL Classification
// ──────────────────────────────────────────────────────────────────────────────

fn req_type_from_pg(req_type: PgReqType) -> ReqType {
    match req_type {
        PgReqType::Read => ReqType::Read,
        PgReqType::Write => ReqType::Write,
    }
}

/// Extract the primary table name from SQL for conflict detection and analytics key hashing.
///
/// Handles the most common DML and DDL patterns:
/// - `INSERT INTO <table>`: word at position 2
/// - `UPDATE <table>`: word at position 1
/// - `DELETE FROM <table>`: word at position 2
/// - `SELECT ... FROM <table>`, `TRUNCATE TABLE <table>`, any other `FROM <table>`: first
///   non-subquery identifier after the `FROM` or `TABLE` keyword
///
/// Returns `None` when no table can be identified (e.g. `SHOW`, `SET`, bare `BEGIN`).
pub fn extract_table_name(sql: &str) -> Option<String> {
    let trimmed = skip_sql_comments(sql.trim());
    let upper = trimmed.to_ascii_uppercase();
    let words: Vec<&str> = trimmed.split_whitespace().collect();

    if upper.starts_with("INSERT") {
        return words.get(2).map(|s| s.trim_matches('"').to_string());
    }
    if upper.starts_with("UPDATE") {
        return words.get(1).map(|s| s.trim_matches('"').to_string());
    }
    if upper.starts_with("DELETE") {
        return words.get(2).map(|s| s.trim_matches('"').to_string());
    }

    // SELECT, TRUNCATE TABLE and other FROM-based statements:
    // scan for the first FROM or TABLE keyword and take the identifier that follows.
    let upper_words: Vec<&str> = upper.split_whitespace().collect();
    for (i, uw) in upper_words.iter().enumerate() {
        if (*uw == "FROM" || *uw == "TABLE")
            && let Some(name) = words.get(i + 1)
        {
            let clean = name.trim_matches('"').trim_end_matches(';');
            if !clean.is_empty() && !clean.starts_with('(') {
                return Some(clean.to_string());
            }
        }
    }

    None
}

/// Extract PK values from a SQL write statement for PK-level conflict detection.
///
/// Handles three statement types:
/// - `INSERT INTO table (col1, col2) VALUES (v1, v2)` — matches PK columns to positions in column list
/// - `UPDATE table SET ... WHERE pk = value` — extracts PK equality conditions from WHERE clause
/// - `DELETE FROM table WHERE pk = value` — same WHERE parsing as UPDATE
///
/// Returns empty Vec on:
/// - Non-write statements (SELECT, DDL, etc.)
/// - Complex SQL (subqueries, CTEs, INSERT...SELECT, bind parameters `$1`)
/// - Parse failures (better to fall back to table-level conflict detection)
pub fn extract_pk_values_from_sql(sql: &str, pk_columns: &[String], columns: &[String]) -> Vec<String> {
    if pk_columns.is_empty() {
        return vec![];
    }

    let trimmed = skip_sql_comments(sql.trim());
    let upper = trimmed.to_ascii_uppercase();

    if upper.starts_with("INSERT") {
        extract_pk_from_insert(trimmed, pk_columns, columns)
    } else if upper.starts_with("UPDATE") || upper.starts_with("DELETE") {
        extract_pk_from_where(trimmed, pk_columns)
    } else {
        vec![]
    }
}

/// Extract PK values from an INSERT statement's column list and VALUES clause.
fn extract_pk_from_insert(sql: &str, pk_columns: &[String], columns: &[String]) -> Vec<String> {
    // Find opening paren of column list (after INTO <table>)
    let upper = sql.to_ascii_uppercase();

    // Check for INSERT ... SELECT or INSERT ... DEFAULT VALUES (unsupported)
    if upper.contains("SELECT ") || upper.contains("DEFAULT VALUES") {
        return vec![];
    }

    // Find the VALUES keyword
    let values_pos = match upper.find("VALUES") {
        Some(pos) => pos,
        None => return vec![],
    };

    // Extract column list between first `(` and the `)` before VALUES
    let before_values = &sql[..values_pos];
    let col_open = match before_values.find('(') {
        Some(pos) => pos,
        None => {
            // No column list — use table's column order
            return extract_values_by_column_order(sql, values_pos, pk_columns, columns);
        }
    };
    let col_close = match before_values[col_open..].find(')') {
        Some(pos) => col_open + pos,
        None => return vec![],
    };

    let col_list_str = &sql[col_open + 1..col_close];
    let insert_columns: Vec<String> = col_list_str.split(',').map(|s| normalize_identifier(s.trim())).collect();

    // Find PK column positions in the INSERT column list.
    // pk_columns contains raw catalog names (already canonical), so compare
    // case-insensitively since SQL identifiers fold to lowercase.
    let pk_positions: Vec<Option<usize>> =
        pk_columns.iter().map(|pk| insert_columns.iter().position(|c| c.eq_ignore_ascii_case(pk))).collect();

    // All PK columns must be present in the INSERT column list
    if pk_positions.iter().any(|p| p.is_none()) {
        return vec![];
    }
    let pk_positions: Vec<usize> = pk_positions.into_iter().map(|p| p.expect("checked above")).collect();

    // Extract values from the VALUES clause
    let values_str = &sql[values_pos + 6..]; // skip "VALUES"
    let values = match extract_values_list(values_str) {
        Some(v) => v,
        None => return vec![],
    };

    // Pick PK values from the parsed value list
    pk_positions.iter().filter_map(|&pos| values.get(pos).cloned()).collect()
}

/// Extract values when INSERT has no explicit column list — use table column order.
fn extract_values_by_column_order(sql: &str, values_pos: usize, pk_columns: &[String], columns: &[String]) -> Vec<String> {
    let pk_positions: Vec<Option<usize>> = pk_columns.iter().map(|pk| columns.iter().position(|c| c.eq_ignore_ascii_case(pk))).collect();

    if pk_positions.iter().any(|p| p.is_none()) {
        return vec![];
    }
    let pk_positions: Vec<usize> = pk_positions.into_iter().map(|p| p.expect("checked above")).collect();

    let values_str = &sql[values_pos + 6..];
    let values = match extract_values_list(values_str) {
        Some(v) => v,
        None => return vec![],
    };

    pk_positions.iter().filter_map(|&pos| values.get(pos).cloned()).collect()
}

/// Extract PK values from WHERE clause equality conditions.
/// Used for UPDATE and DELETE statements.
fn extract_pk_from_where(sql: &str, pk_columns: &[String]) -> Vec<String> {
    let upper = sql.to_ascii_uppercase();
    let where_pos = match upper.find("WHERE") {
        Some(pos) => pos,
        None => return vec![],
    };

    let where_clause = &sql[where_pos + 5..]; // skip "WHERE"

    let mut result: Vec<Option<String>> = vec![None; pk_columns.len()];

    // Split on AND (case-insensitive), extract `col = value` pairs.
    // pk_columns are raw catalog names; col is already normalized by
    // parse_equality_condition, so compare case-insensitively.
    for condition in split_where_conditions(where_clause) {
        let condition = condition.trim();
        if let Some((col, val)) = parse_equality_condition(condition) {
            for (i, pk) in pk_columns.iter().enumerate() {
                if col.eq_ignore_ascii_case(pk) {
                    result[i] = Some(val.clone());
                }
            }
        }
    }

    // All PK columns must have been found
    if result.iter().any(|r| r.is_none()) {
        return vec![];
    }

    result.into_iter().map(|r| r.expect("checked above")).collect()
}

/// Split WHERE clause on top-level AND keywords (not inside parentheses or strings).
fn split_where_conditions(clause: &str) -> Vec<String> {
    let mut conditions = Vec::new();
    let mut current = String::new();
    let mut depth = 0u32;
    let mut in_single_quote = false;
    let bytes = clause.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];
        if in_single_quote {
            current.push(b as char);
            if b == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    current.push('\'');
                    i += 2;
                    continue;
                }
                in_single_quote = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' => {
                in_single_quote = true;
                current.push(b as char);
            }
            b'(' => {
                depth += 1;
                current.push('(');
            }
            b')' => {
                depth = depth.saturating_sub(1);
                current.push(')');
            }
            _ if depth == 0 => {
                // Check for " AND " boundary (case insensitive)
                if (b == b'A' || b == b'a')
                    && i + 4 <= bytes.len()
                    && bytes[i..i + 3].eq_ignore_ascii_case(b"AND")
                    && (i == 0 || bytes[i - 1].is_ascii_whitespace())
                    && (i + 3 >= bytes.len() || bytes[i + 3].is_ascii_whitespace())
                {
                    conditions.push(current.clone());
                    current.clear();
                    i += 3; // skip "AND"
                    continue;
                }
                current.push(b as char);
            }
            _ => {
                current.push(b as char);
            }
        }
        i += 1;
    }

    if !current.trim().is_empty() {
        conditions.push(current);
    }

    conditions
}

/// Parse a single `column = value` equality condition.
/// Returns None for non-equality conditions (LIKE, IN, IS, >, <, etc.)
/// or if bind parameters ($1) are used.
fn parse_equality_condition(condition: &str) -> Option<(String, String)> {
    // Find the `=` sign (but not `!=`, `<>`, `<=`, `>=`)
    let bytes = condition.as_bytes();
    let mut eq_pos = None;
    let mut in_single_quote = false;

    for (i, &b) in bytes.iter().enumerate() {
        if in_single_quote {
            if b == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    continue; // escaped quote
                }
                in_single_quote = false;
            }
            continue;
        }
        if b == b'\'' {
            in_single_quote = true;
            continue;
        }
        if b == b'=' {
            // Check it's not != or <= or >=
            if i > 0 && (bytes[i - 1] == b'!' || bytes[i - 1] == b'<' || bytes[i - 1] == b'>') {
                continue;
            }
            // Check it's not ==
            if i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                continue;
            }
            eq_pos = Some(i);
            break;
        }
    }

    let eq_pos = eq_pos?;
    let col_part = condition[..eq_pos].trim();
    let val_part = condition[eq_pos + 1..].trim();

    // Reject bind parameters
    if val_part.starts_with('$') {
        return None;
    }

    let col = normalize_identifier(col_part);
    let val = parse_sql_literal(val_part)?;

    Some((col, val))
}

/// Parse a SQL literal value, stripping quotes from strings.
/// Returns None for complex expressions (function calls, subqueries, etc.)
fn parse_sql_literal(s: &str) -> Option<String> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // String literal: 'value' (with '' escape)
    if s.starts_with('\'') {
        if !s.ends_with('\'') || s.len() < 2 {
            return None;
        }
        let inner = &s[1..s.len() - 1];
        return Some(inner.replace("''", "'"));
    }

    // Numeric literal (integer or decimal, possibly negative)
    if s.starts_with('-') || s.starts_with('.') || s.as_bytes()[0].is_ascii_digit() {
        // Verify it looks like a number
        let num_str = s.trim_end_matches(|c: char| c.is_ascii_whitespace());
        if num_str.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '-' || c == '+' || c == 'e' || c == 'E') {
            return Some(num_str.to_string());
        }
    }

    // NULL
    if s.eq_ignore_ascii_case("NULL") {
        return Some("NULL".to_string());
    }

    // TRUE/FALSE
    if s.eq_ignore_ascii_case("TRUE") || s.eq_ignore_ascii_case("FALSE") {
        return Some(s.to_lowercase());
    }

    // Reject anything else (function calls, subqueries, casts, etc.)
    None
}

/// Normalize a SQL identifier by removing surrounding double-quotes and lowercasing.
fn normalize_identifier(s: &str) -> String {
    let s = s.trim();
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        // Quoted identifier — preserve case but remove quotes and unescape ""
        s[1..s.len() - 1].replace("\"\"", "\"")
    } else {
        // Unquoted — lowercase per SQL standard
        s.to_lowercase()
    }
}

/// Extract values from a VALUES (...) clause, handling SQL literal parsing.
/// Returns None on parse failure (nested parens, subqueries, etc.)
fn extract_values_list(values_str: &str) -> Option<Vec<String>> {
    let trimmed = values_str.trim();
    // Find the opening and closing parens
    let open = trimmed.find('(')?;
    let close = find_matching_paren(trimmed, open)?;

    let inner = &trimmed[open + 1..close];

    // Check for bind parameters anywhere
    if inner.contains('$') {
        return None;
    }

    // Split on commas at depth 0
    let parts = split_top_level_commas(inner);

    let mut values = Vec::new();
    for part in parts {
        let val = parse_sql_literal(part.trim())?;
        values.push(val);
    }

    Some(values)
}

/// Find the matching closing parenthesis for the one at `start`.
fn find_matching_paren(s: &str, start: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth = 0u32;
    let mut in_single_quote = false;
    for i in start..bytes.len() {
        if in_single_quote {
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    continue; // skip escaped quote — will be handled in next iteration
                }
                in_single_quote = false;
            }
            continue;
        }
        match bytes[i] {
            b'\'' => in_single_quote = true,
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Split a string on top-level commas (not inside parens or string literals).
fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth = 0u32;
    let mut in_single_quote = false;
    let bytes = s.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];
        if in_single_quote {
            current.push(b as char);
            if b == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    current.push('\'');
                    i += 2;
                    continue;
                }
                in_single_quote = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' => {
                in_single_quote = true;
                current.push(b as char);
            }
            b'(' => {
                depth += 1;
                current.push('(');
            }
            b')' => {
                depth = depth.saturating_sub(1);
                current.push(')');
            }
            b',' if depth == 0 => {
                parts.push(current.clone());
                current.clear();
            }
            _ => current.push(b as char),
        }
        i += 1;
    }

    if !current.is_empty() {
        parts.push(current);
    }

    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skip_sql_comments_preserves_comment_markers_inside_strings() {
        assert_eq!(skip_sql_comments("SELECT '--x'"), "SELECT '--x'");
        assert_eq!(skip_sql_comments("SELECT '/* hello */'"), "SELECT '/* hello */'");
        assert_eq!(skip_sql_comments("-- comment\nSELECT '--x'"), "SELECT '--x'");
        assert_eq!(skip_sql_comments("/* block */ SELECT '/* hello */'"), "SELECT '/* hello */'");
        assert_eq!(skip_sql_comments("SELECT 'it''s a test' -- comment"), "SELECT 'it''s a test' -- comment");
    }

    #[test]
    fn test_extract_table_name() {
        assert_eq!(extract_table_name("INSERT INTO users VALUES (1)"), Some("users".to_string()));
        assert_eq!(extract_table_name("UPDATE users SET name = 'a'"), Some("users".to_string()));
        assert_eq!(extract_table_name("DELETE FROM users WHERE id = 1"), Some("users".to_string()));
        assert_eq!(extract_table_name("SELECT * FROM users"), Some("users".to_string()));
        assert_eq!(extract_table_name("TRUNCATE TABLE users"), Some("users".to_string()));
        assert_eq!(extract_table_name("SELECT 1"), None);
    }

    #[test]
    fn test_extract_sql_from_q_message() {
        // Build a Q message: 'Q' + length(4) + "SELECT 1" + '\0'
        let sql = "SELECT 1";
        let len = (4 + sql.len() + 1) as i32;
        let mut msg = vec![b'Q'];
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(sql.as_bytes());
        msg.push(0);

        let pg_bytes = PostgresBytes::from(msg);
        assert_eq!(pg_bytes.extract_sql().expect("extract_sql failed"), "SELECT 1");
    }

    #[test]
    fn test_request_type() {
        let pg_bytes = PostgresBytes::from(make_q_message("SELECT 1"));
        assert_eq!(
            <PostgresBytes as EpWireRequest<PgRawPool>>::request_type(&pg_bytes).expect("request_type"),
            ReqType::Read
        );

        let pg_bytes = PostgresBytes::from(make_q_message("UPDATE users SET name = 'a'"));
        assert_eq!(
            <PostgresBytes as EpWireRequest<PgRawPool>>::request_type(&pg_bytes).expect("request_type"),
            ReqType::Write
        );
    }

    // ---- extract_pk_values_from_sql tests ----

    fn make_q_message(sql: &str) -> Vec<u8> {
        let len = (4 + sql.len() + 1) as i32;
        let mut msg = vec![b'Q'];
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(sql.as_bytes());
        msg.push(0);
        msg
    }

    fn pk_cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn pk_extract_insert_with_column_list() {
        let sql = "INSERT INTO users (id, name, email) VALUES (42, 'alice', 'a@b.com')";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["id"]), &pk_cols(&["id", "name", "email"]));
        assert_eq!(result, vec!["42"]);
    }

    #[test]
    fn pk_extract_insert_without_column_list() {
        let sql = "INSERT INTO users VALUES (42, 'alice', 'a@b.com')";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["id"]), &pk_cols(&["id", "name", "email"]));
        assert_eq!(result, vec!["42"]);
    }

    #[test]
    fn pk_extract_insert_composite_pk() {
        let sql = "INSERT INTO orders (tenant_id, order_id, amount) VALUES (1, 100, 99.99)";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["tenant_id", "order_id"]), &pk_cols(&["tenant_id", "order_id", "amount"]));
        assert_eq!(result, vec!["1", "100"]);
    }

    #[test]
    fn pk_extract_insert_quoted_identifiers() {
        let sql = r#"INSERT INTO "Users" ("Id", "Name") VALUES (7, 'bob')"#;
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["Id"]), &pk_cols(&["Id", "Name"]));
        assert_eq!(result, vec!["7"]);
    }

    #[test]
    fn pk_extract_insert_string_pk() {
        let sql = "INSERT INTO codes (code, description) VALUES ('ABC', 'Alpha Bravo Charlie')";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["code"]), &pk_cols(&["code", "description"]));
        assert_eq!(result, vec!["ABC"]);
    }

    #[test]
    fn pk_extract_insert_select_returns_empty() {
        let sql = "INSERT INTO users (id, name) SELECT id, name FROM temp_users";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["id"]), &pk_cols(&["id", "name"]));
        assert!(result.is_empty());
    }

    #[test]
    fn pk_extract_insert_bind_params_returns_empty() {
        let sql = "INSERT INTO users (id, name) VALUES ($1, $2)";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["id"]), &pk_cols(&["id", "name"]));
        assert!(result.is_empty());
    }

    #[test]
    fn pk_extract_update_simple() {
        let sql = "UPDATE users SET name = 'bob' WHERE id = 42";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["id"]), &pk_cols(&["id", "name"]));
        assert_eq!(result, vec!["42"]);
    }

    #[test]
    fn pk_extract_update_composite_where() {
        let sql = "UPDATE orders SET amount = 50 WHERE tenant_id = 1 AND order_id = 100";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["tenant_id", "order_id"]), &pk_cols(&["tenant_id", "order_id", "amount"]));
        assert_eq!(result, vec!["1", "100"]);
    }

    #[test]
    fn pk_extract_update_quoted_value() {
        let sql = "UPDATE users SET email = 'x@y.com' WHERE name = 'alice'";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["name"]), &pk_cols(&["name", "email"]));
        assert_eq!(result, vec!["alice"]);
    }

    #[test]
    fn pk_extract_update_no_where_returns_empty() {
        let sql = "UPDATE users SET name = 'bob'";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["id"]), &pk_cols(&["id", "name"]));
        assert!(result.is_empty());
    }

    #[test]
    fn pk_extract_delete_simple() {
        let sql = "DELETE FROM users WHERE id = 42";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["id"]), &pk_cols(&["id", "name"]));
        assert_eq!(result, vec!["42"]);
    }

    #[test]
    fn pk_extract_delete_string_pk() {
        let sql = "DELETE FROM codes WHERE code = 'XYZ'";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["code"]), &pk_cols(&["code", "description"]));
        assert_eq!(result, vec!["XYZ"]);
    }

    #[test]
    fn pk_extract_select_returns_empty() {
        let sql = "SELECT * FROM users WHERE id = 42";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["id"]), &pk_cols(&["id", "name"]));
        assert!(result.is_empty());
    }

    #[test]
    fn pk_extract_ddl_returns_empty() {
        let sql = "CREATE TABLE foo (id INT PRIMARY KEY)";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["id"]), &pk_cols(&["id"]));
        assert!(result.is_empty());
    }

    #[test]
    fn pk_extract_no_pk_columns_returns_empty() {
        let sql = "INSERT INTO users (id, name) VALUES (1, 'a')";
        let result = extract_pk_values_from_sql(sql, &[], &pk_cols(&["id", "name"]));
        assert!(result.is_empty());
    }

    #[test]
    fn pk_extract_update_bind_param_returns_empty() {
        let sql = "UPDATE users SET name = 'x' WHERE id = $1";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["id"]), &pk_cols(&["id", "name"]));
        assert!(result.is_empty());
    }

    #[test]
    fn pk_extract_insert_negative_number() {
        let sql = "INSERT INTO temps (id, value) VALUES (1, -5)";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["id"]), &pk_cols(&["id", "value"]));
        assert_eq!(result, vec!["1"]);
    }

    #[test]
    fn pk_extract_update_with_escaped_quote() {
        let sql = "UPDATE users SET bio = 'it''s fine' WHERE name = 'O''Brien'";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["name"]), &pk_cols(&["name", "bio"]));
        assert_eq!(result, vec!["O'Brien"]);
    }

    #[test]
    fn pk_extract_insert_reordered_columns() {
        // PK (id) is not the first column in the INSERT column list
        let sql = "INSERT INTO users (name, id, email) VALUES ('alice', 42, 'a@b.com')";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["id"]), &pk_cols(&["id", "name", "email"]));
        assert_eq!(result, vec!["42"]);
    }

    #[test]
    fn pk_extract_delete_composite_pk() {
        let sql = "DELETE FROM orders WHERE tenant_id = 5 AND order_id = 200";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["tenant_id", "order_id"]), &pk_cols(&["tenant_id", "order_id", "amount"]));
        assert_eq!(result, vec!["5", "200"]);
    }

    #[test]
    fn pk_extract_delete_missing_pk_column_returns_empty() {
        // Only one of two composite PK columns in WHERE
        let sql = "DELETE FROM orders WHERE tenant_id = 5";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["tenant_id", "order_id"]), &pk_cols(&["tenant_id", "order_id", "amount"]));
        assert!(result.is_empty());
    }

    #[test]
    fn pk_extract_insert_case_insensitive() {
        let sql = "insert into users (ID, Name) values (99, 'test')";
        let result = extract_pk_values_from_sql(sql, &pk_cols(&["id"]), &pk_cols(&["id", "name"]));
        assert_eq!(result, vec!["99"]);
    }
}
