//! Postgres WAL consumer for CDC-based real-time replication.
//!
//! Manages logical replication slots, publications, and streaming WAL changes.
//! Decodes pgoutput protocol messages and produces `RowChange` events.

use super::buffer::{ChangeKind, RowChange};
use eden_core::error::EpError;
use serde_json::Value;
use std::collections::HashMap;

/// Decoded relation (table) metadata from pgoutput Relation messages.
#[derive(Debug, Clone)]
pub struct RelationInfo {
    /// Schema-qualified table name.
    pub full_name: String,
    /// Column names in order.
    pub columns: Vec<ColumnInfo>,
}

/// Column metadata from a Relation message.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub type_oid: u32,
    /// Whether this column is part of the replica identity key.
    pub is_key: bool,
}

/// Tracks relation metadata for decoding tuple data.
#[derive(Debug, Default)]
pub struct RelationMap {
    relations: HashMap<u32, RelationInfo>,
}

impl RelationMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register or update a relation from a Relation message.
    pub fn register(&mut self, relation_id: u32, info: RelationInfo) {
        self.relations.insert(relation_id, info);
    }

    /// Look up a relation by ID.
    pub fn get(&self, relation_id: u32) -> Option<&RelationInfo> {
        self.relations.get(&relation_id)
    }
}

/// Validate that a string is a safe SQL identifier (table name, slot name, etc.).
///
/// Allows schema-qualified names like `public.orders`. Rejects anything that
/// could be used for SQL injection.
pub fn validate_sql_identifier(name: &str) -> Result<(), EpError> {
    if name.is_empty() {
        return Err(EpError::parse("SQL identifier must not be empty"));
    }
    for part in name.split('.') {
        if part.is_empty() {
            return Err(EpError::parse(format!("Invalid SQL identifier: '{name}' (empty segment)")));
        }
        let mut chars = part.chars();
        let first = chars.next().unwrap_or(' ');
        if !first.is_ascii_alphabetic() && first != '_' {
            return Err(EpError::parse(format!("Invalid SQL identifier: '{name}' (must start with letter or underscore)")));
        }
        if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(EpError::parse(format!(
                "Invalid SQL identifier: '{name}' (only letters, digits, underscores allowed)"
            )));
        }
    }
    Ok(())
}

/// Quote a SQL identifier with double quotes for safe interpolation.
pub fn quote_identifier(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    if escaped.contains('.') {
        escaped.split('.').map(|p| format!("\"{p}\"")).collect::<Vec<_>>().join(".")
    } else {
        format!("\"{escaped}\"")
    }
}

/// SQL commands for managing logical replication infrastructure.
pub struct ReplicationCommands;

impl ReplicationCommands {
    /// SQL to check if `wal_level` is set to `logical`.
    pub fn check_wal_level() -> &'static str {
        "SHOW wal_level"
    }

    /// SQL to create a publication for specific tables.
    pub fn create_publication(publication_name: &str, tables: &[String]) -> String {
        let table_list = tables.iter().map(|t| quote_identifier(t)).collect::<Vec<_>>().join(", ");
        format!("CREATE PUBLICATION {publication_name} FOR TABLE {table_list}")
    }

    /// SQL to drop a publication.
    pub fn drop_publication(publication_name: &str) -> String {
        format!("DROP PUBLICATION IF EXISTS {publication_name}")
    }

    /// SQL to create a logical replication slot.
    pub fn create_replication_slot(slot_name: &str) -> String {
        let escaped = slot_name.replace('\'', "''");
        format!("SELECT pg_create_logical_replication_slot('{escaped}', 'pgoutput')")
    }

    /// SQL to drop a replication slot.
    pub fn drop_replication_slot(slot_name: &str) -> String {
        let escaped = slot_name.replace('\'', "''");
        format!("SELECT pg_drop_replication_slot('{escaped}')")
    }

    /// SQL to get the current WAL LSN (for backfill start point).
    pub fn current_lsn() -> &'static str {
        "SELECT pg_current_wal_lsn()::text AS lsn"
    }

    /// SQL to check if a replication slot exists.
    pub fn check_slot_exists(slot_name: &str) -> String {
        let escaped = slot_name.replace('\'', "''");
        format!("SELECT 1 FROM pg_replication_slots WHERE slot_name = '{escaped}'")
    }

    /// SQL to get replication slot lag info.
    pub fn slot_lag(slot_name: &str) -> String {
        let escaped = slot_name.replace('\'', "''");
        format!(
            "SELECT slot_name, confirmed_flush_lsn, \
             pg_current_wal_lsn() - confirmed_flush_lsn AS lag_bytes \
             FROM pg_replication_slots WHERE slot_name = '{escaped}'"
        )
    }
}

/// Decode a pgoutput tuple data section into column values.
///
/// The pgoutput protocol encodes tuple data as:
/// - Int16: number of columns
/// - For each column: a tag byte followed by data
///   - 'n' = NULL
///   - 'u' = unchanged (only in UPDATE old tuple)
///   - 't' = text value (Int32 length + bytes)
///   - 'b' = binary value (Int32 length + bytes)
pub fn decode_tuple_data(data: &[u8], offset: &mut usize, relation: &RelationInfo) -> Result<HashMap<String, Value>, EpError> {
    let n_cols = read_i16(data, offset)? as usize;
    let mut columns = HashMap::with_capacity(n_cols);

    for i in 0..n_cols {
        let col_info = relation.columns.get(i);
        let col_name = col_info.map(|c| c.name.clone()).unwrap_or_else(|| format!("col_{i}"));
        let type_oid = col_info.map(|c| c.type_oid).unwrap_or(0);

        if *offset >= data.len() {
            break;
        }

        let tag = data[*offset];
        *offset += 1;

        match tag {
            b'n' => {
                columns.insert(col_name, Value::Null);
            }
            b'u' => {
                // Unchanged TOAST column — skip
            }
            b't' => {
                let len = read_i32(data, offset)? as usize;
                if *offset + len > data.len() {
                    return Err(EpError::parse("Truncated tuple text data"));
                }
                let text = std::str::from_utf8(&data[*offset..*offset + len])
                    .map_err(|e| EpError::parse(format!("Invalid UTF-8 in tuple data: {e}")))?;
                *offset += len;
                columns.insert(col_name, text_to_json_value(text, type_oid));
            }
            b'b' => {
                let len = read_i32(data, offset)? as usize;
                if *offset + len > data.len() {
                    return Err(EpError::parse("Truncated tuple binary data"));
                }
                // Store binary data as hex-encoded text
                let encoded = hex_encode(&data[*offset..*offset + len]);
                *offset += len;
                columns.insert(col_name, Value::String(encoded));
            }
            other => {
                return Err(EpError::parse(format!("Unknown tuple data tag: {other}")));
            }
        }
    }

    Ok(columns)
}

/// Decode a pgoutput message into a RowChange.
///
/// Message format (first byte is the message type):
/// - 'R' = Relation
/// - 'I' = Insert
/// - 'U' = Update
/// - 'D' = Delete
/// - 'B' = Begin
/// - 'C' = Commit
/// - 'O' = Origin
/// - 'T' = Truncate
///
/// Returns `Some(RowChange)` for data messages, `None` for metadata messages.
pub fn decode_pgoutput_message(data: &[u8], relations: &mut RelationMap) -> Result<Option<PgOutputEvent>, EpError> {
    if data.is_empty() {
        return Ok(None);
    }

    let msg_type = data[0];
    let mut offset = 1;

    match msg_type {
        b'R' => {
            // Relation message
            let relation_id = read_u32(data, &mut offset)?;
            let _namespace = read_string(data, &mut offset)?;
            let relation_name = read_string(data, &mut offset)?;
            let _replica_identity = data.get(offset).copied().unwrap_or(0);
            offset += 1;
            let n_cols = read_i16(data, &mut offset)? as usize;

            let mut columns = Vec::with_capacity(n_cols);
            for _ in 0..n_cols {
                let flags = data.get(offset).copied().unwrap_or(0);
                offset += 1;
                let name = read_string(data, &mut offset)?;
                let type_oid = read_u32(data, &mut offset)?;
                let _type_modifier = read_i32(data, &mut offset)?;

                columns.push(ColumnInfo { name, type_oid, is_key: flags & 1 != 0 });
            }

            let full_name = if _namespace.is_empty() || _namespace == "public" {
                relation_name
            } else {
                format!("{_namespace}.{relation_name}")
            };

            relations.register(relation_id, RelationInfo { full_name, columns });
            Ok(Some(PgOutputEvent::Relation(relation_id)))
        }

        b'I' => {
            // Insert message
            let relation_id = read_u32(data, &mut offset)?;
            let relation = relations.get(relation_id).ok_or_else(|| EpError::parse(format!("Unknown relation ID: {relation_id}")))?.clone();

            let tag = data.get(offset).copied().unwrap_or(0);
            offset += 1;
            if tag != b'N' {
                return Err(EpError::parse(format!("Expected 'N' (new tuple) in Insert, got: {tag}")));
            }

            let columns = decode_tuple_data(data, &mut offset, &relation)?;
            Ok(Some(PgOutputEvent::Change(RowChange {
                table: relation.full_name,
                kind: ChangeKind::Insert,
                columns,
                old_columns: None,
            })))
        }

        b'U' => {
            // Update message
            let relation_id = read_u32(data, &mut offset)?;
            let relation = relations.get(relation_id).ok_or_else(|| EpError::parse(format!("Unknown relation ID: {relation_id}")))?.clone();

            let mut old_columns = None;
            let tag = data.get(offset).copied().unwrap_or(0);
            offset += 1;

            // 'K' = key columns of old tuple, 'O' = full old tuple
            if tag == b'K' || tag == b'O' {
                old_columns = Some(decode_tuple_data(data, &mut offset, &relation)?);
                // Read next tag for the new tuple
                let next_tag = data.get(offset).copied().unwrap_or(0);
                offset += 1;
                if next_tag != b'N' {
                    return Err(EpError::parse(format!("Expected 'N' after old tuple in Update, got: {next_tag}")));
                }
            } else if tag != b'N' {
                return Err(EpError::parse(format!("Expected 'K', 'O', or 'N' in Update, got: {tag}")));
            }

            let columns = decode_tuple_data(data, &mut offset, &relation)?;
            Ok(Some(PgOutputEvent::Change(RowChange {
                table: relation.full_name,
                kind: ChangeKind::Update,
                columns,
                old_columns,
            })))
        }

        b'D' => {
            // Delete message
            let relation_id = read_u32(data, &mut offset)?;
            let relation = relations.get(relation_id).ok_or_else(|| EpError::parse(format!("Unknown relation ID: {relation_id}")))?.clone();

            let tag = data.get(offset).copied().unwrap_or(0);
            offset += 1;

            // 'K' = key columns, 'O' = full old tuple
            if tag != b'K' && tag != b'O' {
                return Err(EpError::parse(format!("Expected 'K' or 'O' in Delete, got: {tag}")));
            }

            let columns = decode_tuple_data(data, &mut offset, &relation)?;
            Ok(Some(PgOutputEvent::Change(RowChange {
                table: relation.full_name,
                kind: ChangeKind::Delete,
                columns,
                old_columns: None,
            })))
        }

        b'B' => {
            // Begin message
            let _final_lsn = read_u64(data, &mut offset)?;
            let _commit_timestamp = read_u64(data, &mut offset)?;
            let _xid = read_u32(data, &mut offset)?;
            Ok(Some(PgOutputEvent::Begin))
        }

        b'C' => {
            // Commit message
            let _flags = data.get(offset).copied().unwrap_or(0);
            offset += 1;
            let _commit_lsn = read_u64(data, &mut offset)?;
            let end_lsn = read_u64(data, &mut offset)?;
            let _commit_timestamp = read_u64(data, &mut offset)?;
            Ok(Some(PgOutputEvent::Commit { end_lsn: format_lsn(end_lsn) }))
        }

        // Origin, Truncate, Type, and other messages are ignored
        _ => Ok(None),
    }
}

/// Events produced by decoding pgoutput messages.
#[derive(Debug)]
pub enum PgOutputEvent {
    /// A Relation metadata message (table schema).
    Relation(u32),
    /// A data change (Insert, Update, Delete).
    Change(RowChange),
    /// Begin transaction.
    Begin,
    /// Commit transaction with end LSN.
    Commit { end_lsn: String },
}

// --- Wire protocol helpers ---

fn read_i16(data: &[u8], offset: &mut usize) -> Result<i16, EpError> {
    if *offset + 2 > data.len() {
        return Err(EpError::parse("Truncated i16"));
    }
    let val = i16::from_be_bytes([data[*offset], data[*offset + 1]]);
    *offset += 2;
    Ok(val)
}

fn read_i32(data: &[u8], offset: &mut usize) -> Result<i32, EpError> {
    if *offset + 4 > data.len() {
        return Err(EpError::parse("Truncated i32"));
    }
    let val = i32::from_be_bytes([data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3]]);
    *offset += 4;
    Ok(val)
}

fn read_u32(data: &[u8], offset: &mut usize) -> Result<u32, EpError> {
    if *offset + 4 > data.len() {
        return Err(EpError::parse("Truncated u32"));
    }
    let val = u32::from_be_bytes([data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3]]);
    *offset += 4;
    Ok(val)
}

fn read_u64(data: &[u8], offset: &mut usize) -> Result<u64, EpError> {
    if *offset + 8 > data.len() {
        return Err(EpError::parse("Truncated u64"));
    }
    let val = u64::from_be_bytes([
        data[*offset],
        data[*offset + 1],
        data[*offset + 2],
        data[*offset + 3],
        data[*offset + 4],
        data[*offset + 5],
        data[*offset + 6],
        data[*offset + 7],
    ]);
    *offset += 8;
    Ok(val)
}

fn read_string(data: &[u8], offset: &mut usize) -> Result<String, EpError> {
    let start = *offset;
    while *offset < data.len() && data[*offset] != 0 {
        *offset += 1;
    }
    let s = std::str::from_utf8(&data[start..*offset])
        .map_err(|e| EpError::parse(format!("Invalid UTF-8 in string: {e}")))?
        .to_string();
    if *offset < data.len() {
        *offset += 1; // skip null terminator
    }
    Ok(s)
}

/// Format a u64 LSN as the standard PostgreSQL "X/X" format.
fn format_lsn(lsn: u64) -> String {
    let high = (lsn >> 32) as u32;
    let low = lsn as u32;
    format!("{high:X}/{low:X}")
}

/// Convert a text value from pgoutput to a JSON value based on the Postgres type OID.
fn text_to_json_value(text: &str, type_oid: u32) -> Value {
    match type_oid {
        // bool (OID 16)
        16 => match text {
            "t" | "true" | "TRUE" => Value::Bool(true),
            "f" | "false" | "FALSE" => Value::Bool(false),
            _ => Value::String(text.to_string()),
        },
        // int2 (21), int4 (23), int8 (20)
        20 | 21 | 23 => text.parse::<i64>().map(|i| Value::Number(i.into())).unwrap_or_else(|_| Value::String(text.to_string())),
        // float4 (700), float8 (701)
        700 | 701 => text
            .parse::<f64>()
            .ok()
            .and_then(|f| serde_json::Number::from_f64(f))
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(text.to_string())),
        // numeric (1700) — keep as string to preserve precision
        1700 => Value::String(text.to_string()),
        // json (114), jsonb (3802)
        114 | 3802 => serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_string())),
        // Everything else (text, varchar, timestamp, uuid, etc.) → string
        _ => Value::String(text.to_string()),
    }
}

fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_lsn() {
        assert_eq!(format_lsn(0x0000_0001_0000_0000), "1/0");
        assert_eq!(format_lsn(0x0000_0000_0000_0001), "0/1");
        assert_eq!(format_lsn(0x0000_0002_ABCD_1234), "2/ABCD1234");
    }

    #[test]
    fn test_text_to_json_bool() {
        assert_eq!(text_to_json_value("t", 16), Value::Bool(true));
        assert_eq!(text_to_json_value("f", 16), Value::Bool(false));
    }

    #[test]
    fn test_text_to_json_int() {
        assert_eq!(text_to_json_value("42", 23), Value::Number(42.into()));
        assert_eq!(text_to_json_value("-1", 20), Value::Number((-1).into()));
    }

    #[test]
    fn test_text_to_json_float() {
        let val = text_to_json_value("3.14", 701);
        assert!(val.is_number());
    }

    #[test]
    fn test_text_to_json_string() {
        assert_eq!(
            text_to_json_value("hello", 25), // text OID
            Value::String("hello".to_string())
        );
    }

    #[test]
    fn test_text_to_json_jsonb() {
        let val = text_to_json_value(r#"{"key": "value"}"#, 3802);
        assert!(val.is_object());
    }

    #[test]
    fn test_relation_map() {
        let mut map = RelationMap::new();
        let info = RelationInfo {
            full_name: "public.orders".to_string(),
            columns: vec![ColumnInfo { name: "id".to_string(), type_oid: 23, is_key: true }],
        };
        map.register(12345, info);
        assert!(map.get(12345).is_some());
        assert_eq!(map.get(12345).map(|r| r.full_name.as_str()), Some("public.orders"));
        assert!(map.get(99999).is_none());
    }

    #[test]
    fn test_replication_commands() {
        let sql = ReplicationCommands::create_publication("eden_pub_123", &["public.orders".to_string(), "public.items".to_string()]);
        assert!(sql.contains("eden_pub_123"));
        assert!(sql.contains("\"public\".\"orders\", \"public\".\"items\""));

        let sql = ReplicationCommands::drop_publication("eden_pub_123");
        assert!(sql.contains("DROP PUBLICATION"));

        let sql = ReplicationCommands::create_replication_slot("eden_slot_123");
        assert!(sql.contains("pg_create_logical_replication_slot"));
    }

    #[test]
    fn test_validate_sql_identifier() {
        assert!(validate_sql_identifier("orders").is_ok());
        assert!(validate_sql_identifier("public.orders").is_ok());
        assert!(validate_sql_identifier("my_table_123").is_ok());
        assert!(validate_sql_identifier("").is_err());
        assert!(validate_sql_identifier("orders; DROP TABLE users").is_err());
        assert!(validate_sql_identifier("table-name").is_err());
        assert!(validate_sql_identifier("123abc").is_err());
        assert!(validate_sql_identifier("public.").is_err());
    }

    #[test]
    fn test_quote_identifier() {
        assert_eq!(quote_identifier("orders"), "\"orders\"");
        assert_eq!(quote_identifier("public.orders"), "\"public\".\"orders\"");
    }

    #[test]
    fn test_decode_begin_message() {
        // Begin message: type=B, final_lsn=8bytes, timestamp=8bytes, xid=4bytes
        let mut data = vec![b'B'];
        data.extend_from_slice(&0u64.to_be_bytes()); // final_lsn
        data.extend_from_slice(&0u64.to_be_bytes()); // timestamp
        data.extend_from_slice(&1u32.to_be_bytes()); // xid

        let mut relations = RelationMap::new();
        let event = decode_pgoutput_message(&data, &mut relations).expect("decode");
        assert!(matches!(event, Some(PgOutputEvent::Begin)));
    }

    #[test]
    fn test_decode_commit_message() {
        // Commit message: type=C, flags=1byte, commit_lsn=8bytes, end_lsn=8bytes, timestamp=8bytes
        let mut data = vec![b'C', 0]; // type + flags
        data.extend_from_slice(&0u64.to_be_bytes()); // commit_lsn
        data.extend_from_slice(&0x0000_0001_0000_ABCDu64.to_be_bytes()); // end_lsn
        data.extend_from_slice(&0u64.to_be_bytes()); // timestamp

        let mut relations = RelationMap::new();
        let event = decode_pgoutput_message(&data, &mut relations).expect("decode");
        match event {
            Some(PgOutputEvent::Commit { end_lsn }) => {
                assert_eq!(end_lsn, "1/ABCD");
            }
            _ => panic!("Expected Commit event"),
        }
    }
}
