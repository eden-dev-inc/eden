//! MySQL value builder traits and utilities.
//!
//! Provides traits for constructing MySQL protocol values programmatically.

use crate::error::column_types;
use crate::write::{write_lenenc_int, write_lenenc_string};
use std::io::{self, Write};

/// Trait for types that can be encoded as MySQL values.
pub trait MysqlEncode {
    /// Encode this value into the writer using the text protocol.
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()>;

    /// Encode this value into the writer using the binary protocol.
    fn encode_binary(&self, w: &mut impl Write, col_type: u8) -> io::Result<()>;

    /// Get the MySQL column type for this value.
    fn mysql_type(&self) -> u8;
}

/// Trait for building MySQL result sets.
pub trait ResultSetBuilder {
    /// Add a column definition.
    fn add_column(&mut self, name: &str, col_type: u8) -> &mut Self;

    /// Start a new row.
    fn start_row(&mut self) -> &mut Self;

    /// Add a value to the current row.
    fn add_value<T: MysqlEncode>(&mut self, value: &T) -> &mut Self;

    /// Add a NULL value to the current row.
    fn add_null(&mut self) -> &mut Self;

    /// Finish building and return the bytes.
    fn build(self) -> Vec<u8>;
}

// Implementations for common types

impl MysqlEncode for i8 {
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()> {
        write_lenenc_string(w, self.to_string().as_bytes())
    }

    fn encode_binary(&self, w: &mut impl Write, _col_type: u8) -> io::Result<()> {
        w.write_all(&[*self as u8])
    }

    fn mysql_type(&self) -> u8 {
        column_types::MYSQL_TYPE_TINY
    }
}

impl MysqlEncode for i16 {
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()> {
        write_lenenc_string(w, self.to_string().as_bytes())
    }

    fn encode_binary(&self, w: &mut impl Write, _col_type: u8) -> io::Result<()> {
        w.write_all(&self.to_le_bytes())
    }

    fn mysql_type(&self) -> u8 {
        column_types::MYSQL_TYPE_SHORT
    }
}

impl MysqlEncode for i32 {
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()> {
        write_lenenc_string(w, self.to_string().as_bytes())
    }

    fn encode_binary(&self, w: &mut impl Write, _col_type: u8) -> io::Result<()> {
        w.write_all(&self.to_le_bytes())
    }

    fn mysql_type(&self) -> u8 {
        column_types::MYSQL_TYPE_LONG
    }
}

impl MysqlEncode for i64 {
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()> {
        write_lenenc_string(w, self.to_string().as_bytes())
    }

    fn encode_binary(&self, w: &mut impl Write, _col_type: u8) -> io::Result<()> {
        w.write_all(&self.to_le_bytes())
    }

    fn mysql_type(&self) -> u8 {
        column_types::MYSQL_TYPE_LONGLONG
    }
}

impl MysqlEncode for u8 {
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()> {
        write_lenenc_string(w, self.to_string().as_bytes())
    }

    fn encode_binary(&self, w: &mut impl Write, _col_type: u8) -> io::Result<()> {
        w.write_all(&[*self])
    }

    fn mysql_type(&self) -> u8 {
        column_types::MYSQL_TYPE_TINY
    }
}

impl MysqlEncode for u16 {
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()> {
        write_lenenc_string(w, self.to_string().as_bytes())
    }

    fn encode_binary(&self, w: &mut impl Write, _col_type: u8) -> io::Result<()> {
        w.write_all(&self.to_le_bytes())
    }

    fn mysql_type(&self) -> u8 {
        column_types::MYSQL_TYPE_SHORT
    }
}

impl MysqlEncode for u32 {
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()> {
        write_lenenc_string(w, self.to_string().as_bytes())
    }

    fn encode_binary(&self, w: &mut impl Write, _col_type: u8) -> io::Result<()> {
        w.write_all(&self.to_le_bytes())
    }

    fn mysql_type(&self) -> u8 {
        column_types::MYSQL_TYPE_LONG
    }
}

impl MysqlEncode for u64 {
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()> {
        write_lenenc_string(w, self.to_string().as_bytes())
    }

    fn encode_binary(&self, w: &mut impl Write, _col_type: u8) -> io::Result<()> {
        w.write_all(&self.to_le_bytes())
    }

    fn mysql_type(&self) -> u8 {
        column_types::MYSQL_TYPE_LONGLONG
    }
}

impl MysqlEncode for f32 {
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()> {
        write_lenenc_string(w, self.to_string().as_bytes())
    }

    fn encode_binary(&self, w: &mut impl Write, _col_type: u8) -> io::Result<()> {
        w.write_all(&self.to_le_bytes())
    }

    fn mysql_type(&self) -> u8 {
        column_types::MYSQL_TYPE_FLOAT
    }
}

impl MysqlEncode for f64 {
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()> {
        write_lenenc_string(w, self.to_string().as_bytes())
    }

    fn encode_binary(&self, w: &mut impl Write, _col_type: u8) -> io::Result<()> {
        w.write_all(&self.to_le_bytes())
    }

    fn mysql_type(&self) -> u8 {
        column_types::MYSQL_TYPE_DOUBLE
    }
}

impl MysqlEncode for str {
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()> {
        write_lenenc_string(w, self.as_bytes())
    }

    fn encode_binary(&self, w: &mut impl Write, _col_type: u8) -> io::Result<()> {
        write_lenenc_string(w, self.as_bytes())
    }

    fn mysql_type(&self) -> u8 {
        column_types::MYSQL_TYPE_VAR_STRING
    }
}

impl MysqlEncode for String {
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()> {
        write_lenenc_string(w, self.as_bytes())
    }

    fn encode_binary(&self, w: &mut impl Write, _col_type: u8) -> io::Result<()> {
        write_lenenc_string(w, self.as_bytes())
    }

    fn mysql_type(&self) -> u8 {
        column_types::MYSQL_TYPE_VAR_STRING
    }
}

impl MysqlEncode for [u8] {
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()> {
        write_lenenc_string(w, self)
    }

    fn encode_binary(&self, w: &mut impl Write, _col_type: u8) -> io::Result<()> {
        write_lenenc_string(w, self)
    }

    fn mysql_type(&self) -> u8 {
        column_types::MYSQL_TYPE_BLOB
    }
}

impl MysqlEncode for Vec<u8> {
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()> {
        write_lenenc_string(w, self)
    }

    fn encode_binary(&self, w: &mut impl Write, _col_type: u8) -> io::Result<()> {
        write_lenenc_string(w, self)
    }

    fn mysql_type(&self) -> u8 {
        column_types::MYSQL_TYPE_BLOB
    }
}

impl MysqlEncode for bool {
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()> {
        write_lenenc_string(w, if *self { b"1" } else { b"0" })
    }

    fn encode_binary(&self, w: &mut impl Write, _col_type: u8) -> io::Result<()> {
        w.write_all(&[if *self { 1 } else { 0 }])
    }

    fn mysql_type(&self) -> u8 {
        column_types::MYSQL_TYPE_TINY
    }
}

impl<T: MysqlEncode> MysqlEncode for Option<T> {
    fn encode_text(&self, w: &mut impl Write) -> io::Result<()> {
        match self {
            Some(v) => v.encode_text(w),
            None => w.write_all(&[0xFB]), // NULL marker
        }
    }

    fn encode_binary(&self, w: &mut impl Write, col_type: u8) -> io::Result<()> {
        match self {
            Some(v) => v.encode_binary(w, col_type),
            None => Ok(()), // NULL handled by bitmap
        }
    }

    fn mysql_type(&self) -> u8 {
        match self {
            Some(v) => v.mysql_type(),
            None => column_types::MYSQL_TYPE_NULL,
        }
    }
}

/// A simple result set builder for text protocol results.
#[derive(Debug, Default)]
pub struct TextResultSetBuilder {
    column_count: usize,
    columns: Vec<ColumnDef>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    current_row: Vec<Option<Vec<u8>>>,
}

#[derive(Debug, Clone)]
struct ColumnDef {
    name: String,
    col_type: u8,
}

impl TextResultSetBuilder {
    /// Create a new result set builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a column definition.
    pub fn column(mut self, name: &str, col_type: u8) -> Self {
        self.columns.push(ColumnDef { name: name.to_string(), col_type });
        self.column_count += 1;
        self
    }

    /// Start a new row.
    pub fn row(mut self) -> Self {
        if !self.current_row.is_empty() {
            self.rows.push(std::mem::take(&mut self.current_row));
        }
        self
    }

    /// Add a string value.
    pub fn string(mut self, value: &str) -> Self {
        let mut buf = Vec::new();
        write_lenenc_string(&mut buf, value.as_bytes()).expect("write to Vec is infallible");
        self.current_row.push(Some(buf));
        self
    }

    /// Add an integer value.
    pub fn int(mut self, value: i64) -> Self {
        let mut buf = Vec::new();
        write_lenenc_string(&mut buf, value.to_string().as_bytes()).expect("write to Vec is infallible");
        self.current_row.push(Some(buf));
        self
    }

    /// Add a NULL value.
    pub fn null(mut self) -> Self {
        self.current_row.push(None);
        self
    }

    /// Build the column count packet.
    pub fn build_column_count_packet(&self, sequence_id: u8) -> Vec<u8> {
        let mut payload = Vec::new();
        write_lenenc_int(&mut payload, self.column_count as u64).expect("write to Vec is infallible");

        let mut packet = Vec::with_capacity(4 + payload.len());
        let len = payload.len() as u32;
        packet.push(len as u8);
        packet.push((len >> 8) as u8);
        packet.push((len >> 16) as u8);
        packet.push(sequence_id);
        packet.extend_from_slice(&payload);
        packet
    }

    /// Build column definition packets.
    pub fn build_column_packets(&self, start_sequence: u8) -> Vec<Vec<u8>> {
        let mut packets = Vec::new();
        let mut seq = start_sequence;

        for col in &self.columns {
            let mut payload = Vec::new();

            // catalog (always "def")
            write_lenenc_string(&mut payload, b"def").expect("write to Vec is infallible");
            // schema
            write_lenenc_string(&mut payload, b"").expect("write to Vec is infallible");
            // table
            write_lenenc_string(&mut payload, b"").expect("write to Vec is infallible");
            // org_table
            write_lenenc_string(&mut payload, b"").expect("write to Vec is infallible");
            // name
            write_lenenc_string(&mut payload, col.name.as_bytes()).expect("write to Vec is infallible");
            // org_name
            write_lenenc_string(&mut payload, col.name.as_bytes()).expect("write to Vec is infallible");
            // length of fixed-length fields [0c]
            payload.push(0x0c);
            // character set (utf8mb4 = 45)
            payload.extend_from_slice(&45u16.to_le_bytes());
            // column length
            payload.extend_from_slice(&255u32.to_le_bytes());
            // column type
            payload.push(col.col_type);
            // flags
            payload.extend_from_slice(&0u16.to_le_bytes());
            // decimals
            payload.push(0);
            // filler
            payload.extend_from_slice(&[0, 0]);

            let mut packet = Vec::with_capacity(4 + payload.len());
            let len = payload.len() as u32;
            packet.push(len as u8);
            packet.push((len >> 8) as u8);
            packet.push((len >> 16) as u8);
            packet.push(seq);
            packet.extend_from_slice(&payload);

            packets.push(packet);
            seq = seq.wrapping_add(1);
        }

        packets
    }

    /// Build an EOF packet (for after columns or after rows).
    pub fn build_eof_packet(&self, sequence_id: u8, status_flags: u16) -> Vec<u8> {
        let payload = [
            0xFE, // EOF marker
            0x00,
            0x00, // warnings
            (status_flags & 0xFF) as u8,
            ((status_flags >> 8) & 0xFF) as u8,
        ];

        let mut packet = Vec::with_capacity(4 + payload.len());
        packet.push(payload.len() as u8);
        packet.push(0);
        packet.push(0);
        packet.push(sequence_id);
        packet.extend_from_slice(&payload);
        packet
    }

    /// Build row packets.
    pub fn build_row_packets(&self, start_sequence: u8) -> Vec<Vec<u8>> {
        let mut all_rows = self.rows.clone();
        if !self.current_row.is_empty() {
            all_rows.push(self.current_row.clone());
        }

        let mut packets = Vec::new();
        let mut seq = start_sequence;

        for row in all_rows {
            let mut payload = Vec::new();

            for value in row {
                match value {
                    Some(data) => payload.extend_from_slice(&data),
                    None => payload.push(0xFB), // NULL marker
                }
            }

            let mut packet = Vec::with_capacity(4 + payload.len());
            let len = payload.len() as u32;
            packet.push(len as u8);
            packet.push((len >> 8) as u8);
            packet.push((len >> 16) as u8);
            packet.push(seq);
            packet.extend_from_slice(&payload);

            packets.push(packet);
            seq = seq.wrapping_add(1);
        }

        packets
    }

    /// Finalize and return the last sequence ID used.
    pub fn finish(self) -> usize {
        self.rows.len() + if self.current_row.is_empty() { 0 } else { 1 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_i32() {
        let mut buf = Vec::new();
        42i32.encode_text(&mut buf).unwrap();
        assert_eq!(buf, vec![2, b'4', b'2']); // length-encoded "42"
    }

    #[test]
    fn test_encode_string() {
        let mut buf = Vec::new();
        "hello".encode_text(&mut buf).unwrap();
        assert_eq!(buf, vec![5, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn test_encode_option_some() {
        let mut buf = Vec::new();
        Some(123i32).encode_text(&mut buf).unwrap();
        assert_eq!(buf, vec![3, b'1', b'2', b'3']);
    }

    #[test]
    fn test_encode_option_none() {
        let mut buf = Vec::new();
        let none: Option<i32> = None;
        none.encode_text(&mut buf).unwrap();
        assert_eq!(buf, vec![0xFB]); // NULL marker
    }

    #[test]
    fn test_result_set_builder() {
        let builder = TextResultSetBuilder::new()
            .column("id", column_types::MYSQL_TYPE_LONG)
            .column("name", column_types::MYSQL_TYPE_VAR_STRING)
            .row()
            .int(1)
            .string("alice")
            .row()
            .int(2)
            .string("bob");

        let col_count = builder.build_column_count_packet(0);
        assert_eq!(col_count[4], 2); // 2 columns

        let col_packets = builder.build_column_packets(1);
        assert_eq!(col_packets.len(), 2);

        let row_packets = builder.build_row_packets(4);
        assert_eq!(row_packets.len(), 2);
    }

    #[test]
    fn test_mysql_types() {
        assert_eq!(42i32.mysql_type(), column_types::MYSQL_TYPE_LONG);
        assert_eq!("hello".mysql_type(), column_types::MYSQL_TYPE_VAR_STRING);
        assert_eq!(3.25f64.mysql_type(), column_types::MYSQL_TYPE_DOUBLE);
        assert_eq!(true.mysql_type(), column_types::MYSQL_TYPE_TINY);
    }
}
