//! PostgreSQL frontend/backend message boundary scanners.

use crate::error::backend;

/// Scan a single PostgreSQL message with a type byte, returning total length.
#[inline]
pub fn scan_pg_message(buf: &[u8]) -> Option<usize> {
    if buf.len() < 5 {
        return None;
    }
    let length = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    let total = 1 + length;
    if buf.len() < total { None } else { Some(total) }
}

/// Metadata about a scanned backend response group.
#[derive(Debug, Clone)]
pub struct PgResponseScan {
    pub total_len: usize,
    pub message_count: usize,
    pub has_error: bool,
    pub transaction_status: u8,
    pub has_copy_in: bool,
    pub has_copy_out: bool,
    pub has_copy_both: bool,
    pub is_complete: bool,
}

/// Scan backend messages until ReadyForQuery or end of data.
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
            break;
        }

        result.message_count += 1;
        match msg_type {
            backend::ERROR_RESPONSE => result.has_error = true,
            backend::READY_FOR_QUERY => {
                if total >= 6 {
                    result.transaction_status = buf[pos + 5];
                }
                result.is_complete = true;
                result.total_len = pos + total;
                return Some(result);
            }
            backend::COPY_IN_RESPONSE => result.has_copy_in = true,
            backend::COPY_OUT_RESPONSE => result.has_copy_out = true,
            backend::COPY_BOTH_RESPONSE => result.has_copy_both = true,
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

/// Extract SQL from a simple Query (`Q`) frontend message.
pub fn extract_simple_query(buf: &[u8]) -> Option<&str> {
    if buf.len() < 6 || buf[0] != b'Q' {
        return None;
    }
    let total = frontend_message_len(buf)?;
    let sql_bytes = &buf[5..total - 1];
    std::str::from_utf8(sql_bytes).ok()
}

/// Extract the statement name from a Parse (`P`) frontend message.
pub fn extract_parse_name(buf: &[u8]) -> Option<&str> {
    if buf.len() < 6 || buf[0] != b'P' {
        return None;
    }
    let total = frontend_message_len(buf)?;
    let payload = &buf[5..total];
    let name_end = payload.iter().position(|&b| b == 0)?;
    std::str::from_utf8(&payload[..name_end]).ok()
}

/// Extract the SQL text from a Parse (`P`) frontend message.
pub fn extract_parse_query(buf: &[u8]) -> Option<&str> {
    let (_, query, _) = extract_parse_full(buf)?;
    Some(query)
}

/// Extract (statement name, query, param type OIDs) from a Parse message.
pub fn extract_parse_full(buf: &[u8]) -> Option<(&str, &str, Vec<i32>)> {
    if buf.len() < 6 || buf[0] != b'P' {
        return None;
    }
    let total = frontend_message_len(buf)?;
    let payload = &buf[5..total];

    let name_end = payload.iter().position(|&b| b == 0)?;
    let name = std::str::from_utf8(&payload[..name_end]).ok()?;
    let query_start = name_end + 1;
    let query_bytes = &payload[query_start..];
    let query_end = query_bytes.iter().position(|&b| b == 0)?;
    let query = std::str::from_utf8(&query_bytes[..query_end]).ok()?;

    let params_start = query_start + query_end + 1;
    let rest = &payload[params_start..];
    if rest.len() < 2 {
        return Some((name, query, Vec::new()));
    }
    let num_params = i16::from_be_bytes([rest[0], rest[1]]) as usize;
    let oid_data = &rest[2..];
    let mut param_types = Vec::with_capacity(num_params);
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

/// Extract the statement name from a Bind (`B`) message.
///
/// Returns `(statement_name, byte_offset, byte_len)`.
pub fn extract_bind_stmt_name(buf: &[u8]) -> Option<(&str, usize, usize)> {
    if buf.len() < 6 || buf[0] != b'B' {
        return None;
    }
    let total = frontend_message_len(buf)?;
    let payload = &buf[5..total];
    let portal_end = payload.iter().position(|&b| b == 0)?;
    let stmt_offset_in_payload = portal_end + 1;
    let stmt_bytes = &payload[stmt_offset_in_payload..];
    let stmt_len = stmt_bytes.iter().position(|&b| b == 0)?;
    let stmt_name = std::str::from_utf8(&stmt_bytes[..stmt_len]).ok()?;
    Some((stmt_name, 5 + stmt_offset_in_payload, stmt_len))
}

/// Extract `(portal_name, statement_name)` from a Bind (`B`) message.
pub fn extract_bind_names(buf: &[u8]) -> Option<(&str, &str)> {
    if buf.len() < 6 || buf[0] != b'B' {
        return None;
    }
    let total = frontend_message_len(buf)?;
    let payload = &buf[5..total];
    let portal_end = payload.iter().position(|&b| b == 0)?;
    let portal_name = std::str::from_utf8(&payload[..portal_end]).ok()?;
    let stmt_start = portal_end + 1;
    let stmt_bytes = &payload[stmt_start..];
    let stmt_end = stmt_bytes.iter().position(|&b| b == 0)?;
    let statement_name = std::str::from_utf8(&stmt_bytes[..stmt_end]).ok()?;
    Some((portal_name, statement_name))
}

/// Extract the portal name from an Execute (`E`) message.
pub fn extract_execute_portal(buf: &[u8]) -> Option<&str> {
    if buf.len() < 6 || buf[0] != b'E' {
        return None;
    }
    let total = frontend_message_len(buf)?;
    let payload = &buf[5..total];
    let portal_end = payload.iter().position(|&b| b == 0)?;
    std::str::from_utf8(&payload[..portal_end]).ok()
}

/// Extract the target from Describe (`D`) or Close (`C`) messages.
///
/// Returns `(kind, name, byte_offset, byte_len)` where kind is `S` or `P`.
pub fn extract_describe_or_close_target(buf: &[u8]) -> Option<(u8, &str, usize, usize)> {
    if buf.len() < 7 || (buf[0] != b'D' && buf[0] != b'C') {
        return None;
    }
    let total = frontend_message_len(buf)?;
    let kind = buf[5];
    let name_start = 6;
    let name_bytes = &buf[name_start..total];
    let name_len = name_bytes.iter().position(|&b| b == 0)?;
    let name = std::str::from_utf8(&name_bytes[..name_len]).ok()?;
    Some((kind, name, name_start, name_len))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgExtendedBatchMuxSafety {
    SafeUnnamedOneShot,
    Malformed,
    MissingSync,
    MissingParse,
    MissingBind,
    MissingExecute,
    UnsafeNamedStatement,
    UnsafeNamedPortal,
    UnsupportedMessage(u8),
}

impl PgExtendedBatchMuxSafety {
    pub const fn is_safe(self) -> bool {
        matches!(self, Self::SafeUnnamedOneShot)
    }
}

/// Return whether an extended-query batch is safe to send as raw bytes through
/// a shared backend worker without per-backend statement-name rewriting.
///
/// This is intentionally narrower than PostgreSQL's full extended protocol. It
/// only admits one-shot unnamed batches: Parse unnamed statement, Bind unnamed
/// portal to unnamed statement, Execute unnamed portal, optional Describe/Flush,
/// and a final Sync. Named statements/portals are backend-local state and must
/// use the statement-cache rewrite path instead.
pub fn extended_batch_mux_safety(buf: &[u8]) -> PgExtendedBatchMuxSafety {
    let mut pos = 0usize;
    let mut seen_parse = false;
    let mut seen_bind = false;
    let mut seen_execute = false;
    let mut seen_sync = false;

    while pos < buf.len() {
        if pos + 5 > buf.len() {
            return PgExtendedBatchMuxSafety::Malformed;
        }
        let length = i32::from_be_bytes([buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4]]);
        if length < 4 {
            return PgExtendedBatchMuxSafety::Malformed;
        }
        let total = 1 + length as usize;
        if pos + total > buf.len() {
            return PgExtendedBatchMuxSafety::Malformed;
        }

        let msg = &buf[pos..pos + total];
        match msg[0] {
            b'P' => {
                let Some((statement_name, _, _)) = extract_parse_full(msg) else {
                    return PgExtendedBatchMuxSafety::Malformed;
                };
                if !statement_name.is_empty() {
                    return PgExtendedBatchMuxSafety::UnsafeNamedStatement;
                }
                seen_parse = true;
            }
            b'B' => {
                let Some((portal_name, statement_name)) = extract_bind_names(msg) else {
                    return PgExtendedBatchMuxSafety::Malformed;
                };
                if !portal_name.is_empty() {
                    return PgExtendedBatchMuxSafety::UnsafeNamedPortal;
                }
                if !statement_name.is_empty() {
                    return PgExtendedBatchMuxSafety::UnsafeNamedStatement;
                }
                seen_bind = true;
            }
            b'E' => {
                let Some(portal_name) = extract_execute_portal(msg) else {
                    return PgExtendedBatchMuxSafety::Malformed;
                };
                if !portal_name.is_empty() {
                    return PgExtendedBatchMuxSafety::UnsafeNamedPortal;
                }
                seen_execute = true;
            }
            b'D' | b'C' => {
                let Some((kind, name, _, _)) = extract_describe_or_close_target(msg) else {
                    return PgExtendedBatchMuxSafety::Malformed;
                };
                if !name.is_empty() {
                    return if kind == b'S' {
                        PgExtendedBatchMuxSafety::UnsafeNamedStatement
                    } else {
                        PgExtendedBatchMuxSafety::UnsafeNamedPortal
                    };
                }
            }
            b'H' => {}
            b'S' => {
                if pos + total != buf.len() {
                    return PgExtendedBatchMuxSafety::UnsupportedMessage(msg[0]);
                }
                seen_sync = true;
            }
            other => return PgExtendedBatchMuxSafety::UnsupportedMessage(other),
        }

        pos += total;
    }

    if !seen_sync {
        PgExtendedBatchMuxSafety::MissingSync
    } else if !seen_parse {
        PgExtendedBatchMuxSafety::MissingParse
    } else if !seen_bind {
        PgExtendedBatchMuxSafety::MissingBind
    } else if !seen_execute {
        PgExtendedBatchMuxSafety::MissingExecute
    } else {
        PgExtendedBatchMuxSafety::SafeUnnamedOneShot
    }
}

#[inline]
pub fn frontend_message_type(buf: &[u8]) -> Option<u8> {
    buf.first().copied()
}

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

    fn make_simple_query(sql: &str) -> Vec<u8> {
        let length = 4 + sql.len() + 1;
        let mut msg = vec![b'Q'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.extend_from_slice(sql.as_bytes());
        msg.push(0);
        msg
    }

    fn make_parse(stmt_name: &str, query: &str, param_oids: &[i32]) -> Vec<u8> {
        let length = 4 + stmt_name.len() + 1 + query.len() + 1 + 2 + (4 * param_oids.len());
        let mut msg = vec![b'P'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.extend_from_slice(stmt_name.as_bytes());
        msg.push(0);
        msg.extend_from_slice(query.as_bytes());
        msg.push(0);
        msg.extend_from_slice(&(param_oids.len() as i16).to_be_bytes());
        for oid in param_oids {
            msg.extend_from_slice(&oid.to_be_bytes());
        }
        msg
    }

    fn make_bind(portal_name: &str, statement_name: &str) -> Vec<u8> {
        let length = 4 + portal_name.len() + 1 + statement_name.len() + 1 + 2 + 2 + 2;
        let mut msg = vec![b'B'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.extend_from_slice(portal_name.as_bytes());
        msg.push(0);
        msg.extend_from_slice(statement_name.as_bytes());
        msg.push(0);
        msg.extend_from_slice(&0i16.to_be_bytes());
        msg.extend_from_slice(&0i16.to_be_bytes());
        msg.extend_from_slice(&0i16.to_be_bytes());
        msg
    }

    fn make_describe(kind: u8, name: &str) -> Vec<u8> {
        let length = 4 + 1 + name.len() + 1;
        let mut msg = vec![b'D'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.push(kind);
        msg.extend_from_slice(name.as_bytes());
        msg.push(0);
        msg
    }

    fn make_execute(portal_name: &str) -> Vec<u8> {
        let length = 4 + portal_name.len() + 1 + 4;
        let mut msg = vec![b'E'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.extend_from_slice(portal_name.as_bytes());
        msg.push(0);
        msg.extend_from_slice(&0i32.to_be_bytes());
        msg
    }

    fn make_sync() -> Vec<u8> {
        let mut msg = vec![b'S'];
        msg.extend_from_slice(&4i32.to_be_bytes());
        msg
    }

    #[test]
    fn extracts_simple_query() {
        assert_eq!(extract_simple_query(&make_simple_query("SELECT 1")), Some("SELECT 1"));
    }

    #[test]
    fn extracts_parse_full() {
        let msg = make_parse("s1", "SELECT $1", &[23]);
        assert_eq!(extract_parse_full(&msg), Some(("s1", "SELECT $1", vec![23])));
    }

    #[test]
    fn extended_mux_safety_allows_unnamed_one_shot_batches() {
        let mut batch = Vec::new();
        batch.extend_from_slice(&make_parse("", "SELECT 1", &[]));
        batch.extend_from_slice(&make_bind("", ""));
        batch.extend_from_slice(&make_describe(b'P', ""));
        batch.extend_from_slice(&make_execute(""));
        batch.extend_from_slice(&make_sync());

        assert_eq!(extended_batch_mux_safety(&batch), PgExtendedBatchMuxSafety::SafeUnnamedOneShot);
    }

    #[test]
    fn extended_mux_safety_rejects_named_backend_state() {
        let mut named_statement = Vec::new();
        named_statement.extend_from_slice(&make_parse("s1", "SELECT 1", &[]));
        named_statement.extend_from_slice(&make_bind("", "s1"));
        named_statement.extend_from_slice(&make_execute(""));
        named_statement.extend_from_slice(&make_sync());
        assert_eq!(extended_batch_mux_safety(&named_statement), PgExtendedBatchMuxSafety::UnsafeNamedStatement);

        let mut named_portal = Vec::new();
        named_portal.extend_from_slice(&make_parse("", "SELECT 1", &[]));
        named_portal.extend_from_slice(&make_bind("p1", ""));
        named_portal.extend_from_slice(&make_execute("p1"));
        named_portal.extend_from_slice(&make_sync());
        assert_eq!(extended_batch_mux_safety(&named_portal), PgExtendedBatchMuxSafety::UnsafeNamedPortal);
    }

    #[test]
    fn extended_mux_safety_rejects_session_dependent_batches() {
        let mut missing_parse = Vec::new();
        missing_parse.extend_from_slice(&make_bind("", ""));
        missing_parse.extend_from_slice(&make_execute(""));
        missing_parse.extend_from_slice(&make_sync());
        assert_eq!(extended_batch_mux_safety(&missing_parse), PgExtendedBatchMuxSafety::MissingParse);

        let simple_query = make_simple_query("SELECT 1");
        assert_eq!(extended_batch_mux_safety(&simple_query), PgExtendedBatchMuxSafety::UnsupportedMessage(b'Q'));
    }
}
