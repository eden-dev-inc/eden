//! ClickHouse wire protocol writing helpers.
//!
//! Extension traits and functions for encoding ClickHouse-specific data types.

use crate::native::varint::write_varuint;
use std::io::{self, Write};

/// Extension trait for writing ClickHouse-specific data types.
pub trait ClickhouseWriteExt: Write {
    /// Write a VarUInt (variable-length unsigned integer).
    #[inline]
    fn write_varuint(&mut self, value: u64) -> io::Result<usize> {
        write_varuint(self, value)
    }

    /// Write a ClickHouse string (VarUInt length prefix + raw bytes).
    #[inline]
    fn write_ch_string(&mut self, s: &[u8]) -> io::Result<usize> {
        let len_bytes = self.write_varuint(s.len() as u64)?;
        self.write_all(s)?;
        Ok(len_bytes + s.len())
    }

    /// Write a ClickHouse UTF-8 string.
    #[inline]
    fn write_ch_string_utf8(&mut self, s: &str) -> io::Result<usize> {
        self.write_ch_string(s.as_bytes())
    }

    /// Write a single byte.
    #[inline]
    fn write_u8_ch(&mut self, value: u8) -> io::Result<()> {
        self.write_all(&[value])
    }

    /// Write a little-endian u16.
    #[inline]
    fn write_u16_le_ch(&mut self, value: u16) -> io::Result<()> {
        self.write_all(&value.to_le_bytes())
    }

    /// Write a little-endian u32.
    #[inline]
    fn write_u32_le_ch(&mut self, value: u32) -> io::Result<()> {
        self.write_all(&value.to_le_bytes())
    }

    /// Write a little-endian u64.
    #[inline]
    fn write_u64_le_ch(&mut self, value: u64) -> io::Result<()> {
        self.write_all(&value.to_le_bytes())
    }

    /// Write a little-endian i8.
    #[inline]
    fn write_i8_ch(&mut self, value: i8) -> io::Result<()> {
        self.write_all(&[value as u8])
    }

    /// Write a little-endian i16.
    #[inline]
    fn write_i16_le_ch(&mut self, value: i16) -> io::Result<()> {
        self.write_all(&value.to_le_bytes())
    }

    /// Write a little-endian i32.
    #[inline]
    fn write_i32_le_ch(&mut self, value: i32) -> io::Result<()> {
        self.write_all(&value.to_le_bytes())
    }

    /// Write a little-endian i64.
    #[inline]
    fn write_i64_le_ch(&mut self, value: i64) -> io::Result<()> {
        self.write_all(&value.to_le_bytes())
    }

    /// Write a little-endian f32.
    #[inline]
    fn write_f32_le_ch(&mut self, value: f32) -> io::Result<()> {
        self.write_all(&value.to_le_bytes())
    }

    /// Write a little-endian f64.
    #[inline]
    fn write_f64_le_ch(&mut self, value: f64) -> io::Result<()> {
        self.write_all(&value.to_le_bytes())
    }

    /// Write a little-endian u128.
    #[inline]
    fn write_u128_le_ch(&mut self, value: u128) -> io::Result<()> {
        self.write_all(&value.to_le_bytes())
    }

    /// Write a little-endian i128.
    #[inline]
    fn write_i128_le_ch(&mut self, value: i128) -> io::Result<()> {
        self.write_all(&value.to_le_bytes())
    }

    /// Write a boolean (0x00 for false, 0x01 for true).
    #[inline]
    fn write_bool_ch(&mut self, value: bool) -> io::Result<()> {
        self.write_all(&[if value { 0x01 } else { 0x00 }])
    }

    /// Write raw bytes.
    #[inline]
    fn write_bytes_ch(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.write_all(bytes)
    }

    /// Write a fixed-size array.
    #[inline]
    fn write_fixed_ch<const N: usize>(&mut self, bytes: &[u8; N]) -> io::Result<()> {
        self.write_all(bytes)
    }
}

// Blanket implementation for all types that implement Write
impl<W: Write + ?Sized> ClickhouseWriteExt for W {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_varuint() {
        let mut buf = Vec::new();
        buf.write_varuint(0).unwrap();
        assert_eq!(buf, vec![0x00]);

        let mut buf = Vec::new();
        buf.write_varuint(127).unwrap();
        assert_eq!(buf, vec![0x7F]);

        let mut buf = Vec::new();
        buf.write_varuint(128).unwrap();
        assert_eq!(buf, vec![0x80, 0x01]);

        let mut buf = Vec::new();
        buf.write_varuint(300).unwrap();
        assert_eq!(buf, vec![0xAC, 0x02]);
    }

    #[test]
    fn test_write_ch_string() {
        let mut buf = Vec::new();
        buf.write_ch_string(b"hello").unwrap();
        assert_eq!(buf, vec![0x05, b'h', b'e', b'l', b'l', b'o']);

        let mut buf = Vec::new();
        buf.write_ch_string(b"").unwrap();
        assert_eq!(buf, vec![0x00]);
    }

    #[test]
    fn test_write_integers() {
        let mut buf = Vec::new();
        buf.write_u8_ch(0x42).unwrap();
        assert_eq!(buf, vec![0x42]);

        let mut buf = Vec::new();
        buf.write_u16_le_ch(0x0201).unwrap();
        assert_eq!(buf, vec![0x01, 0x02]);

        let mut buf = Vec::new();
        buf.write_u32_le_ch(0x04030201).unwrap();
        assert_eq!(buf, vec![0x01, 0x02, 0x03, 0x04]);

        let mut buf = Vec::new();
        buf.write_i32_le_ch(-1).unwrap();
        assert_eq!(buf, vec![0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn test_write_bool() {
        let mut buf = Vec::new();
        buf.write_bool_ch(false).unwrap();
        assert_eq!(buf, vec![0x00]);

        let mut buf = Vec::new();
        buf.write_bool_ch(true).unwrap();
        assert_eq!(buf, vec![0x01]);
    }

    #[test]
    fn test_write_float() {
        let value: f64 = 3.25;
        let mut buf = Vec::new();
        buf.write_f64_le_ch(value).unwrap();
        assert_eq!(buf, value.to_le_bytes().to_vec());
    }

    #[test]
    fn test_roundtrip_string() {
        use crate::native::read::ClickhouseReadSyncExt;
        use wire_stream::SliceStream;

        let original = "Hello, ClickHouse!";
        let mut buf = Vec::new();
        buf.write_ch_string_utf8(original).unwrap();

        let stream = SliceStream::new(&buf);
        let decoded = stream.read_ch_string_utf8_sync().unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_roundtrip_integers() {
        use crate::native::read::ClickhouseReadSyncExt;
        use wire_stream::SliceStream;

        let test_values: &[i64] = &[0, 1, -1, 127, -128, 32767, -32768, i64::MAX, i64::MIN];

        for &value in test_values {
            let mut buf = Vec::new();
            buf.write_i64_le_ch(value).unwrap();

            let stream = SliceStream::new(&buf);
            let decoded = stream.read_i64_le_ch_sync().unwrap();
            assert_eq!(decoded, value, "roundtrip failed for {}", value);
        }
    }
}
