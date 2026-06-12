use bytes::Bytes;
use postgres_wire::types::ReadyForQuery;
use std::sync::atomic::AtomicU64;

pub(crate) const SSL_REQUEST_CODE: i32 = 80877103;
pub(crate) const PROTOCOL_VERSION_3_0: i32 = 196608;

pub(crate) const MSG_QUERY: u8 = b'Q';
pub(crate) const MSG_TERMINATE: u8 = b'X';
pub(crate) const MSG_PARSE: u8 = b'P';
pub(crate) const MSG_BIND: u8 = b'B';
pub(crate) const MSG_EXECUTE: u8 = b'E';
pub(crate) const MSG_SYNC: u8 = b'S';
pub(crate) const MSG_DESCRIBE: u8 = b'D';
pub(crate) const MSG_CLOSE: u8 = b'C';
pub(crate) const MSG_FLUSH: u8 = b'H';
pub(crate) const MSG_COPY_DATA: u8 = b'd';
pub(crate) const MSG_COPY_DONE: u8 = b'c';
pub(crate) const MSG_COPY_FAIL: u8 = b'f';

pub(crate) static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(0);
pub(crate) const CANCEL_REQUEST_CODE: i32 = 80877102;

pub(crate) fn parse_startup_params(msg_bytes: &[u8]) -> Vec<(String, String)> {
    let mut params = Vec::new();
    let mut i = 8;
    while i < msg_bytes.len() {
        let key_start = i;
        while i < msg_bytes.len() && msg_bytes[i] != 0 {
            i += 1;
        }
        if i >= msg_bytes.len() || key_start == i {
            break;
        }
        let key = String::from_utf8_lossy(&msg_bytes[key_start..i]).to_string();
        i += 1;

        let val_start = i;
        while i < msg_bytes.len() && msg_bytes[i] != 0 {
            i += 1;
        }
        let value = String::from_utf8_lossy(&msg_bytes[val_start..i]).to_string();
        if i < msg_bytes.len() {
            i += 1;
        }

        params.push((key, value));
    }
    params
}

pub(crate) fn extract_parse_sql(msg_bytes: &[u8]) -> Option<(String, String)> {
    if msg_bytes.len() < 6 {
        return None;
    }
    let payload = &msg_bytes[5..];
    let name_end = payload.iter().position(|&b| b == 0)?;
    let statement_name = std::str::from_utf8(&payload[..name_end]).ok()?.to_string();
    let rest = &payload[name_end + 1..];
    let query_end = rest.iter().position(|&b| b == 0)?;
    let sql = std::str::from_utf8(&rest[..query_end]).ok()?.to_string();
    Some((statement_name, sql))
}

pub(crate) fn extract_close_statement(msg_bytes: &[u8]) -> Option<String> {
    if msg_bytes.len() < 7 || msg_bytes[5] != b'S' {
        return None;
    }
    let payload = &msg_bytes[6..];
    let name_end = payload.iter().position(|&b| b == 0)?;
    let name = std::str::from_utf8(&payload[..name_end]).ok()?.to_string();
    if name.is_empty() { None } else { Some(name) }
}

pub(crate) fn extract_bind_statement(msg_bytes: &[u8]) -> Option<String> {
    if msg_bytes.len() < 6 {
        return None;
    }
    let payload = &msg_bytes[5..];
    let portal_end = payload.iter().position(|&b| b == 0)?;
    let rest = &payload[portal_end + 1..];
    let stmt_end = rest.iter().position(|&b| b == 0)?;
    Some(std::str::from_utf8(&rest[..stmt_end]).ok()?.to_string())
}

pub(crate) fn ready_for_query_status(bytes: &[u8]) -> Option<u8> {
    let mut idx = 0usize;
    let mut last_ready_status = None;
    while idx + 5 <= bytes.len() {
        let msg_type = bytes[idx];
        let len = i32::from_be_bytes([bytes[idx + 1], bytes[idx + 2], bytes[idx + 3], bytes[idx + 4]]) as usize;
        let total = 1 + len;
        if total == 0 || idx + total > bytes.len() {
            return None;
        }
        last_ready_status = if msg_type == b'Z' && total >= 6 {
            Some(bytes[idx + 5])
        } else {
            None
        };
        idx += total;
    }
    if idx == bytes.len() { last_ready_status } else { None }
}

pub(crate) fn response_has_ready_for_query(bytes: &[u8]) -> bool {
    ready_for_query_status(bytes).is_some()
}

pub(crate) fn build_q_message(sql: &str) -> Vec<u8> {
    let sql_bytes = sql.as_bytes();
    let length = (4 + sql_bytes.len() + 1) as i32;
    let mut msg = Vec::with_capacity(1 + 4 + sql_bytes.len() + 1);
    msg.push(b'Q');
    msg.extend_from_slice(&length.to_be_bytes());
    msg.extend_from_slice(sql_bytes);
    msg.push(0);
    msg
}

pub(crate) fn strip_leading_command_completes(response: Bytes, count: usize) -> Bytes {
    let mut offset = 0;
    let mut stripped = 0;

    while stripped < count && offset + 5 <= response.len() {
        let msg_type = response[offset];
        let length = i32::from_be_bytes([
            response[offset + 1],
            response[offset + 2],
            response[offset + 3],
            response[offset + 4],
        ]) as usize;
        let total = 1 + length;

        if total == 0 || offset + total > response.len() {
            break;
        }

        if msg_type == b'C' {
            stripped += 1;
            offset += total;
        } else {
            break;
        }
    }

    response.slice(offset..)
}

pub(crate) fn build_command_complete_msg(tag: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(b'C');
    let len = (4 + tag.len() + 1) as i32;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(tag.as_bytes());
    buf.push(0);
    buf
}

#[cfg(test)]
pub(crate) fn pg_catalog_cmd(sql: &str) -> String {
    let mut words = sql.split_whitespace();
    let first = words.next().unwrap_or("").to_ascii_uppercase();
    match first.as_str() {
        "CREATE" | "DROP" | "ALTER" => {
            const MODIFIERS: &[&str] = &[
                "UNIQUE",
                "TEMPORARY",
                "TEMP",
                "UNLOGGED",
                "OR",
                "REPLACE",
                "GLOBAL",
                "LOCAL",
                "RECURSIVE",
                "IF",
                "NOT",
                "EXISTS",
                "CONCURRENTLY",
                "MATERIALIZED",
            ];

            let object = words.find_map(|word| {
                let upper = word.to_ascii_uppercase();
                (!MODIFIERS.contains(&upper.as_str())).then_some(upper)
            });

            match object {
                Some(object) => format!("{}_{}", first, object),
                None => first,
            }
        }
        _ => first,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxState {
    Idle,
    InTransaction,
    Failed,
}

impl TxState {
    pub(crate) fn from_ready_status(status: u8) -> Option<Self> {
        match status {
            b'I' => Some(TxState::Idle),
            b'T' => Some(TxState::InTransaction),
            b'E' => Some(TxState::Failed),
            _ => None,
        }
    }

    pub(crate) fn ready_for_query(self) -> Vec<u8> {
        match self {
            TxState::Idle => ReadyForQuery::idle().encode(),
            TxState::InTransaction => ReadyForQuery::in_transaction().encode(),
            TxState::Failed => ReadyForQuery::failed().encode(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use endpoints::endpoint::postgres::protocol::PostgresBytes;

    #[test]
    fn pg_catalog_cmd_skips_modifier_tokens() {
        assert_eq!(pg_catalog_cmd("CREATE UNIQUE INDEX idx ON users(id)"), "CREATE_INDEX");
        assert_eq!(pg_catalog_cmd("CREATE TEMP TABLE t (id int)"), "CREATE_TABLE");
        assert_eq!(
            pg_catalog_cmd("CREATE OR REPLACE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql"),
            "CREATE_FUNCTION"
        );
        assert_eq!(pg_catalog_cmd("CREATE MATERIALIZED VIEW mv AS SELECT 1"), "CREATE_VIEW");
    }

    #[test]
    fn build_q_message_roundtrip() {
        let sql = "SELECT 1";
        let msg = build_q_message(sql);
        let pg_bytes = PostgresBytes::from(msg);
        assert_eq!(pg_bytes.extract_sql().expect("extract_sql"), sql);
    }

    #[test]
    fn ready_for_query_status_reads_trailing_status() {
        let mut response = fake_command_complete("SELECT 1");
        response.extend_from_slice(&ReadyForQuery::in_transaction().encode());

        assert_eq!(ready_for_query_status(&response), Some(b'T'));
        assert!(response_has_ready_for_query(&response));
    }

    #[test]
    fn ready_for_query_status_requires_rfq_to_be_last_complete_message() {
        let mut response = ReadyForQuery::idle().encode();
        response.extend_from_slice(&fake_command_complete("SELECT 1"));

        assert_eq!(ready_for_query_status(&response), None);
        assert!(!response_has_ready_for_query(&response));
    }

    #[test]
    fn ready_for_query_status_rejects_trailing_partial_bytes() {
        let mut response = ReadyForQuery::idle().encode();
        response.extend_from_slice(b"Z");

        assert_eq!(ready_for_query_status(&response), None);
        assert!(!response_has_ready_for_query(&response));
    }

    fn fake_command_complete(tag: &str) -> Vec<u8> {
        let tag_bytes = tag.as_bytes();
        let length = (4 + tag_bytes.len() + 1) as i32;
        let mut msg = Vec::new();
        msg.push(b'C');
        msg.extend_from_slice(&length.to_be_bytes());
        msg.extend_from_slice(tag_bytes);
        msg.push(0);
        msg
    }

    fn fake_row_description() -> Vec<u8> {
        let length: i32 = 4 + 2;
        let mut msg = Vec::new();
        msg.push(b'T');
        msg.extend_from_slice(&length.to_be_bytes());
        msg.extend_from_slice(&0i16.to_be_bytes());
        msg
    }

    #[test]
    fn strip_leading_command_completes_zero() {
        let data = Bytes::from(fake_command_complete("SET"));
        let result = strip_leading_command_completes(data.clone(), 0);
        assert_eq!(result, data);
    }

    #[test]
    fn strip_leading_command_completes_one() {
        let mut data = Vec::new();
        data.extend(fake_command_complete("SET"));
        data.extend(fake_row_description());
        let original_rd = fake_row_description();

        let result = strip_leading_command_completes(Bytes::from(data), 1);
        assert_eq!(result.as_ref(), original_rd.as_slice());
    }

    #[test]
    fn strip_leading_command_completes_two() {
        let mut data = Vec::new();
        data.extend(fake_command_complete("SET"));
        data.extend(fake_command_complete("SET"));
        data.extend(fake_row_description());
        data.extend(fake_command_complete("SELECT 1"));
        let expected_start = fake_command_complete("SET").len() * 2;

        let result = strip_leading_command_completes(Bytes::from(data.clone()), 2);
        assert_eq!(result.as_ref(), &data[expected_start..]);
    }

    #[test]
    fn strip_preserves_non_cc_messages() {
        let mut data = Vec::new();
        data.extend(fake_command_complete("SET"));
        data.extend(fake_row_description());
        data.extend(fake_command_complete("SELECT 1"));

        let result = strip_leading_command_completes(Bytes::from(data.clone()), 2);
        let expected_start = fake_command_complete("SET").len();
        assert_eq!(result.as_ref(), &data[expected_start..]);
    }

    #[test]
    fn strip_empty_response() {
        let result = strip_leading_command_completes(Bytes::new(), 2);
        assert!(result.is_empty());
    }
}
