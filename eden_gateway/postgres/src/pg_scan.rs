//! PostgreSQL wire protocol message boundary scanner.
//!
//! Scans raw backend (server→client) response bytes and returns message
//! boundaries with metadata. Used by the processor to determine when a
//! complete response has been received and to detect errors and COPY modes.
//!
//! PG message format: type(1 byte) + length(4 bytes, big-endian, includes self).
//! Total message size = 1 + length.

use postgres_wire::error::backend;

/// Scan a single PG backend message, returning total byte length.
///
/// Returns `None` if the buffer doesn't contain a complete message.
#[inline]
pub fn scan_pg_message(buf: &[u8]) -> Option<usize> {
    if buf.len() < 5 {
        return None;
    }
    let length = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    let total = 1 + length;
    if buf.len() < total { None } else { Some(total) }
}

/// Metadata about a scanned PG response (sequence of backend messages).
#[derive(Debug, Clone)]
pub struct PgResponseScan {
    /// Total byte length of all scanned messages.
    pub total_len: usize,
    /// Number of individual messages in the response.
    pub message_count: usize,
    /// Whether an ErrorResponse ('E') was found.
    pub has_error: bool,
    /// Transaction status from ReadyForQuery ('Z'): 'I', 'T', or 'E'.
    pub transaction_status: u8,
    /// Whether a CopyInResponse ('G') was found.
    pub has_copy_in: bool,
    /// Whether a CopyOutResponse ('H') was found.
    pub has_copy_out: bool,
    /// Whether a CopyBothResponse ('W') was found.
    pub has_copy_both: bool,
    /// Whether the response is complete (ends with ReadyForQuery).
    pub is_complete: bool,
}

/// Scan a buffer of PG backend messages until ReadyForQuery or end of data.
///
/// Returns `None` if the buffer is empty or doesn't contain even one complete message.
/// Returns a `PgResponseScan` with `is_complete = true` if ReadyForQuery was found.
pub fn scan_pg_response(buf: &[u8]) -> Option<PgResponseScan> {
    if buf.len() < 5 {
        return None;
    }

    let mut result = PgResponseScan {
        total_len: 0,
        message_count: 0,
        has_error: false,
        transaction_status: b'I',
        has_copy_in: false,
        has_copy_out: false,
        has_copy_both: false,
        is_complete: false,
    };

    let mut pos = 0;

    while pos + 5 <= buf.len() {
        let msg_type = buf[pos];
        let length = i32::from_be_bytes([buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4]]) as usize;
        let total = 1 + length;

        if pos + total > buf.len() {
            // Incomplete message — stop scanning
            break;
        }

        result.message_count += 1;

        match msg_type {
            backend::ERROR_RESPONSE => {
                result.has_error = true;
            }
            backend::READY_FOR_QUERY => {
                // ReadyForQuery is always 6 bytes: 'Z' + 5 (length) + 1 (status)
                if total >= 6 {
                    result.transaction_status = buf[pos + 5];
                }
                result.is_complete = true;
                result.total_len = pos + total;
                return Some(result);
            }
            backend::COPY_IN_RESPONSE => {
                result.has_copy_in = true;
            }
            backend::COPY_OUT_RESPONSE => {
                result.has_copy_out = true;
            }
            backend::COPY_BOTH_RESPONSE => {
                result.has_copy_both = true;
            }
            _ => {}
        }

        pos += total;
    }

    if result.message_count > 0 {
        result.total_len = pos;
        Some(result)
    } else {
        None
    }
}

/// Extract the SQL string from a Simple Query ('Q') frontend message.
///
/// Format: 'Q' + length(4) + sql\0
/// Returns the SQL without the null terminator.
pub fn extract_simple_query(buf: &[u8]) -> Option<&str> {
    if buf.len() < 6 || buf[0] != b'Q' {
        return None;
    }
    let length = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    let total = 1 + length;
    if buf.len() < total {
        return None;
    }
    // SQL is from offset 5 to total-1 (excluding trailing NUL)
    let sql_bytes = &buf[5..total - 1];
    std::str::from_utf8(sql_bytes).ok()
}

/// Extract the statement name from a Parse ('P') frontend message.
///
/// Format: 'P' + length(4) + statement_name\0 + query\0 + num_params(i16) + param_oids*
/// Returns the statement name (empty string for the unnamed statement).
pub fn extract_parse_name(buf: &[u8]) -> Option<&str> {
    if buf.len() < 6 || buf[0] != b'P' {
        return None;
    }
    let length = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    let total = 1 + length;
    if buf.len() < total {
        return None;
    }

    let payload = &buf[5..total];
    let name_end = payload.iter().position(|&b| b == 0)?;
    std::str::from_utf8(&payload[..name_end]).ok()
}

/// Extract the SQL string from a Parse ('P') frontend message.
///
/// Format: 'P' + length(4) + statement_name\0 + query\0 + num_params(i16) + param_oids*
/// Returns the query string.
pub fn extract_parse_query(buf: &[u8]) -> Option<&str> {
    if buf.len() < 6 || buf[0] != b'P' {
        return None;
    }
    let length = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    let total = 1 + length;
    if buf.len() < total {
        return None;
    }

    // Skip statement name (null-terminated string)
    let payload = &buf[5..total];
    let name_end = payload.iter().position(|&b| b == 0)?;
    let query_start = name_end + 1;

    // Find end of query string
    let query_bytes = &payload[query_start..];
    let query_end = query_bytes.iter().position(|&b| b == 0)?;

    std::str::from_utf8(&query_bytes[..query_end]).ok()
}

/// Extract the full contents of a Parse ('P') frontend message.
///
/// Format: 'P' + length(4) + statement_name\0 + query\0 + num_params(i16) + param_oids*
/// Returns (statement_name, query, param_type_oids).
pub fn extract_parse_full(buf: &[u8]) -> Option<(&str, &str, Vec<i32>)> {
    if buf.len() < 6 || buf[0] != b'P' {
        return None;
    }
    let length = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    let total = 1 + length;
    if buf.len() < total {
        return None;
    }

    let payload = &buf[5..total];
    // Statement name (null-terminated)
    let name_end = payload.iter().position(|&b| b == 0)?;
    let name = std::str::from_utf8(&payload[..name_end]).ok()?;
    // Query string (null-terminated)
    let query_start = name_end + 1;
    let query_bytes = &payload[query_start..];
    let query_end = query_bytes.iter().position(|&b| b == 0)?;
    let query = std::str::from_utf8(&query_bytes[..query_end]).ok()?;
    // Parameter type OIDs
    let params_start = query_start + query_end + 1;
    let rest = &payload[params_start..];
    if rest.len() < 2 {
        return Some((name, query, vec![]));
    }
    let num_params = i16::from_be_bytes([rest[0], rest[1]]) as usize;
    let mut param_types = Vec::with_capacity(num_params);
    let oid_data = &rest[2..];
    for i in 0..num_params {
        let offset = i * 4;
        if offset + 4 > oid_data.len() {
            break;
        }
        param_types.push(i32::from_be_bytes([
            oid_data[offset],
            oid_data[offset + 1],
            oid_data[offset + 2],
            oid_data[offset + 3],
        ]));
    }
    Some((name, query, param_types))
}

/// Extract the statement name from a Bind ('B') frontend message.
///
/// Format: 'B' + length(4) + portal_name\0 + statement_name\0 + ...
/// Returns (statement_name, byte offset of stmt name in buf, byte length of stmt name).
pub fn extract_bind_stmt_name(buf: &[u8]) -> Option<(&str, usize, usize)> {
    if buf.len() < 6 || buf[0] != b'B' {
        return None;
    }
    let length = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    let total = 1 + length;
    if buf.len() < total {
        return None;
    }

    let payload = &buf[5..total];
    // Skip portal name (null-terminated)
    let portal_end = payload.iter().position(|&b| b == 0)?;
    // Statement name starts after portal's null terminator
    let stmt_offset_in_payload = portal_end + 1;
    let stmt_bytes = &payload[stmt_offset_in_payload..];
    let stmt_len = stmt_bytes.iter().position(|&b| b == 0)?;
    let stmt_name = std::str::from_utf8(&stmt_bytes[..stmt_len]).ok()?;
    // Absolute offset in buf: 5 (header) + portal_end + 1 (null)
    let abs_offset = 5 + stmt_offset_in_payload;
    Some((stmt_name, abs_offset, stmt_len))
}

/// Extract the target kind and name from a Describe ('D') or Close ('C') frontend message.
///
/// Format: type_byte + length(4) + kind(1: 'S'=statement, 'P'=portal) + name\0
/// Returns (kind_byte, name, byte offset of name in buf, byte length of name).
pub fn extract_describe_or_close_target(buf: &[u8]) -> Option<(u8, &str, usize, usize)> {
    if buf.len() < 7 {
        return None;
    }
    let msg_type = buf[0];
    if msg_type != b'D' && msg_type != b'C' {
        return None;
    }
    let length = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    let total = 1 + length;
    if buf.len() < total {
        return None;
    }

    let kind = buf[5]; // 'S' or 'P'
    let name_start = 6;
    let name_bytes = &buf[name_start..total];
    let name_len = name_bytes.iter().position(|&b| b == 0)?;
    let name = std::str::from_utf8(&name_bytes[..name_len]).ok()?;
    Some((kind, name, name_start, name_len))
}

/// Determine the frontend message type from the first byte.
/// Returns None if the buffer is empty.
#[inline]
pub fn frontend_message_type(buf: &[u8]) -> Option<u8> {
    buf.first().copied()
}

/// Get the total length of a frontend message (type + length-field-value).
/// Returns None if incomplete.
#[inline]
pub fn frontend_message_len(buf: &[u8]) -> Option<usize> {
    if buf.len() < 5 {
        return None;
    }
    let length = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    let total = 1 + length;
    if buf.len() < total { None } else { Some(total) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ready_for_query(status: u8) -> Vec<u8> {
        vec![b'Z', 0, 0, 0, 5, status]
    }

    fn make_command_complete(tag: &str) -> Vec<u8> {
        let tag_bytes = tag.as_bytes();
        let length = 4 + tag_bytes.len() + 1; // length field + tag + NUL
        let mut msg = vec![b'C'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.extend_from_slice(tag_bytes);
        msg.push(0);
        msg
    }

    fn make_error_response() -> Vec<u8> {
        // Minimal ErrorResponse: 'E' + length + 'M' + "error\0" + '\0'
        let fields = b"Merror\0\0"; // M field + terminator
        let length = 4 + fields.len();
        let mut msg = vec![b'E'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.extend_from_slice(fields);
        msg
    }

    fn make_simple_query(sql: &str) -> Vec<u8> {
        let sql_bytes = sql.as_bytes();
        let length = 4 + sql_bytes.len() + 1; // length + sql + NUL
        let mut msg = vec![b'Q'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.extend_from_slice(sql_bytes);
        msg.push(0);
        msg
    }

    fn make_parse(stmt_name: &str, query: &str) -> Vec<u8> {
        let stmt_bytes = stmt_name.as_bytes();
        let query_bytes = query.as_bytes();
        // length: 4 + stmt\0 + query\0 + 2 (num_params=0)
        let length = 4 + stmt_bytes.len() + 1 + query_bytes.len() + 1 + 2;
        let mut msg = vec![b'P'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.extend_from_slice(stmt_bytes);
        msg.push(0);
        msg.extend_from_slice(query_bytes);
        msg.push(0);
        msg.extend_from_slice(&0i16.to_be_bytes());
        msg
    }

    #[test]
    fn test_scan_single_message() {
        let rfq = make_ready_for_query(b'I');
        let total = scan_pg_message(&rfq);
        assert_eq!(total, Some(6));
    }

    #[test]
    fn test_scan_incomplete() {
        assert_eq!(scan_pg_message(&[b'Z', 0, 0]), None);
    }

    #[test]
    fn test_scan_response_complete() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&make_command_complete("SELECT 1"));
        buf.extend_from_slice(&make_ready_for_query(b'I'));

        let scan = scan_pg_response(&buf).expect("scan_pg_response failed");
        assert!(scan.is_complete);
        assert_eq!(scan.message_count, 2);
        assert_eq!(scan.transaction_status, b'I');
        assert!(!scan.has_error);
        assert_eq!(scan.total_len, buf.len());
    }

    #[test]
    fn test_scan_response_with_error() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&make_error_response());
        buf.extend_from_slice(&make_ready_for_query(b'I'));

        let scan = scan_pg_response(&buf).expect("scan_pg_response failed");
        assert!(scan.is_complete);
        assert!(scan.has_error);
    }

    #[test]
    fn test_scan_response_incomplete() {
        let cc = make_command_complete("SELECT 1");
        // Just CommandComplete, no ReadyForQuery
        let scan = scan_pg_response(&cc).expect("scan_pg_response failed");
        assert!(!scan.is_complete);
        assert_eq!(scan.message_count, 1);
    }

    #[test]
    fn test_scan_response_in_transaction() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&make_command_complete("BEGIN"));
        buf.extend_from_slice(&make_ready_for_query(b'T'));

        let scan = scan_pg_response(&buf).expect("scan_pg_response failed");
        assert_eq!(scan.transaction_status, b'T');
    }

    #[test]
    fn test_extract_simple_query() {
        let msg = make_simple_query("SELECT 1");
        let sql = extract_simple_query(&msg);
        assert_eq!(sql, Some("SELECT 1"));
    }

    #[test]
    fn test_extract_parse_name_unnamed() {
        let msg = make_parse("", "SELECT $1::int");
        assert_eq!(extract_parse_name(&msg), Some(""));
    }

    #[test]
    fn test_extract_parse_name_named() {
        let msg = make_parse("sqlx_s_1", "INSERT INTO t VALUES ($1)");
        assert_eq!(extract_parse_name(&msg), Some("sqlx_s_1"));
    }

    #[test]
    fn test_extract_parse_query() {
        let msg = make_parse("", "SELECT $1::int");
        let sql = extract_parse_query(&msg);
        assert_eq!(sql, Some("SELECT $1::int"));
    }

    #[test]
    fn test_extract_parse_query_named() {
        let msg = make_parse("my_stmt", "INSERT INTO t VALUES ($1)");
        let sql = extract_parse_query(&msg);
        assert_eq!(sql, Some("INSERT INTO t VALUES ($1)"));
    }

    #[test]
    fn test_frontend_message_len() {
        let msg = make_simple_query("SELECT 1");
        let total = frontend_message_len(&msg);
        assert_eq!(total, Some(msg.len()));
    }

    // ── extract_parse_full tests ──

    fn make_parse_with_params(stmt_name: &str, query: &str, param_oids: &[i32]) -> Vec<u8> {
        let stmt_bytes = stmt_name.as_bytes();
        let query_bytes = query.as_bytes();
        let length = 4 + stmt_bytes.len() + 1 + query_bytes.len() + 1 + 2 + (4 * param_oids.len());
        let mut msg = vec![b'P'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.extend_from_slice(stmt_bytes);
        msg.push(0);
        msg.extend_from_slice(query_bytes);
        msg.push(0);
        msg.extend_from_slice(&(param_oids.len() as i16).to_be_bytes());
        for &oid in param_oids {
            msg.extend_from_slice(&oid.to_be_bytes());
        }
        msg
    }

    #[test]
    fn test_extract_parse_full_unnamed() {
        let msg = make_parse("", "SELECT $1::int");
        let (name, query, params) = extract_parse_full(&msg).expect("extract_parse_full failed");
        assert_eq!(name, "");
        assert_eq!(query, "SELECT $1::int");
        assert!(params.is_empty());
    }

    #[test]
    fn test_extract_parse_full_named() {
        let msg = make_parse("sqlx_s_1", "INSERT INTO t VALUES ($1)");
        let (name, query, params) = extract_parse_full(&msg).expect("extract_parse_full failed");
        assert_eq!(name, "sqlx_s_1");
        assert_eq!(query, "INSERT INTO t VALUES ($1)");
        assert!(params.is_empty());
    }

    #[test]
    fn test_extract_parse_full_with_params() {
        let msg = make_parse_with_params("s1", "SELECT $1, $2", &[23, 25]); // int4=23, text=25
        let (name, query, params) = extract_parse_full(&msg).expect("extract_parse_full failed");
        assert_eq!(name, "s1");
        assert_eq!(query, "SELECT $1, $2");
        assert_eq!(params, vec![23, 25]);
    }

    // ── extract_bind_stmt_name tests ──

    fn make_bind(portal: &str, stmt: &str) -> Vec<u8> {
        let portal_bytes = portal.as_bytes();
        let stmt_bytes = stmt.as_bytes();
        // Minimal BIND: portal\0 + stmt\0 + i16(0) format codes + i16(0) params + i16(0) result formats
        let length = 4 + portal_bytes.len() + 1 + stmt_bytes.len() + 1 + 2 + 2 + 2;
        let mut msg = vec![b'B'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.extend_from_slice(portal_bytes);
        msg.push(0);
        msg.extend_from_slice(stmt_bytes);
        msg.push(0);
        msg.extend_from_slice(&0i16.to_be_bytes()); // format code count
        msg.extend_from_slice(&0i16.to_be_bytes()); // param count
        msg.extend_from_slice(&0i16.to_be_bytes()); // result format count
        msg
    }

    #[test]
    fn test_extract_bind_stmt_name_unnamed() {
        let msg = make_bind("", "");
        let (name, offset, len) = extract_bind_stmt_name(&msg).expect("extract_bind_stmt_name failed");
        assert_eq!(name, "");
        assert_eq!(len, 0);
        // Offset should be 5 (header) + 0 (empty portal) + 1 (null) = 6
        assert_eq!(offset, 6);
    }

    #[test]
    fn test_extract_bind_stmt_name_named() {
        let msg = make_bind("", "sqlx_s_1");
        let (name, offset, len) = extract_bind_stmt_name(&msg).expect("extract_bind_stmt_name failed");
        assert_eq!(name, "sqlx_s_1");
        assert_eq!(len, 8);
        assert_eq!(offset, 6);
    }

    #[test]
    fn test_extract_bind_with_named_portal() {
        let msg = make_bind("my_portal", "sqlx_s_3");
        let (name, offset, len) = extract_bind_stmt_name(&msg).expect("extract_bind_stmt_name failed");
        assert_eq!(name, "sqlx_s_3");
        assert_eq!(len, 8);
        // Offset: 5 (header) + 9 (portal) + 1 (null) = 15
        assert_eq!(offset, 15);
    }

    // ── extract_describe_or_close_target tests ──

    fn make_describe(kind: u8, name: &str) -> Vec<u8> {
        let name_bytes = name.as_bytes();
        let length = 4 + 1 + name_bytes.len() + 1; // length + kind + name + null
        let mut msg = vec![b'D'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.push(kind);
        msg.extend_from_slice(name_bytes);
        msg.push(0);
        msg
    }

    fn make_close(kind: u8, name: &str) -> Vec<u8> {
        let name_bytes = name.as_bytes();
        let length = 4 + 1 + name_bytes.len() + 1;
        let mut msg = vec![b'C'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.push(kind);
        msg.extend_from_slice(name_bytes);
        msg.push(0);
        msg
    }

    #[test]
    fn test_describe_statement() {
        let msg = make_describe(b'S', "sqlx_s_1");
        let (kind, name, offset, len) = extract_describe_or_close_target(&msg).expect("extract_describe_or_close_target failed");
        assert_eq!(kind, b'S');
        assert_eq!(name, "sqlx_s_1");
        assert_eq!(offset, 6);
        assert_eq!(len, 8);
    }

    #[test]
    fn test_describe_portal() {
        let msg = make_describe(b'P', "");
        let (kind, name, _offset, len) = extract_describe_or_close_target(&msg).expect("extract_describe_or_close_target failed");
        assert_eq!(kind, b'P');
        assert_eq!(name, "");
        assert_eq!(len, 0);
    }

    #[test]
    fn test_close_statement() {
        let msg = make_close(b'S', "s5");
        let (kind, name, offset, len) = extract_describe_or_close_target(&msg).expect("extract_describe_or_close_target failed");
        assert_eq!(kind, b'S');
        assert_eq!(name, "s5");
        assert_eq!(offset, 6);
        assert_eq!(len, 2);
    }
}
