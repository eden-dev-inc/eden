//! ClickHouse wire protocol reading helpers.
//!
//! Extension traits for reading ClickHouse-specific data types from streams.

use crate::error::ClickhouseWireError;
use crate::{MAX_STRING_SIZE, VARINT_MAX_BYTES};
use wire_stream::{WireRead, WireReadSync, WireReadSyncExt};

/// Extension trait for synchronous ClickHouse wire protocol reading.
///
/// Provides methods for reading ClickHouse-specific data types like VarUInt
/// and length-prefixed strings.
pub trait ClickhouseReadSyncExt: WireReadSync
where
    Self::ReadError: Into<ClickhouseWireError>,
{
    /// Read a VarUInt (variable-length unsigned integer).
    ///
    /// ClickHouse uses 7-bits-per-byte encoding with high bit as continuation flag.
    /// Maximum 9 bytes for u64.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The encoding exceeds 9 bytes (`VarUIntTooLong`)
    /// - The value would overflow u64 (`VarUIntOverflow`)
    /// - The stream ends unexpectedly
    #[inline]
    fn read_varuint_sync(&self) -> Result<u64, ClickhouseWireError> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;

        for _ in 0..VARINT_MAX_BYTES {
            let borrow = self.peek_exactly::<1>().map_err(Into::into)?;
            let byte = borrow[0];
            self.accept_exactly(&borrow).map_err(Into::into)?;

            // Add lower 7 bits to result
            let value = (byte & 0x7F) as u64;

            // Check for overflow before shifting
            if shift >= 64 || (shift == 63 && value > 1) {
                return Err(ClickhouseWireError::VarUIntOverflow);
            }

            result |= value << shift;
            shift += 7;

            // High bit clear means this is the last byte
            if byte & 0x80 == 0 {
                return Ok(result);
            }
        }

        Err(ClickhouseWireError::VarUIntTooLong)
    }

    /// Read a ClickHouse string (VarUInt length prefix + raw bytes).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The length exceeds `MAX_STRING_SIZE`
    /// - The stream ends unexpectedly
    #[inline]
    fn read_ch_string_sync(&self) -> Result<Vec<u8>, ClickhouseWireError> {
        let len = self.read_varuint_sync()? as usize;

        if len > MAX_STRING_SIZE {
            return Err(ClickhouseWireError::StringTooLarge { length: len, max: MAX_STRING_SIZE });
        }

        if len == 0 {
            return Ok(Vec::new());
        }

        let data = self.read_bytes_sync(len).map_err(Into::into)?;
        Ok(data.to_vec())
    }

    /// Read a ClickHouse string and validate it as UTF-8.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The string exceeds maximum size
    /// - The string contains invalid UTF-8
    /// - The stream ends unexpectedly
    #[inline]
    fn read_ch_string_utf8_sync(&self) -> Result<String, ClickhouseWireError> {
        let bytes = self.read_ch_string_sync()?;
        String::from_utf8(bytes).map_err(|e| ClickhouseWireError::InvalidUtf8(e.utf8_error()))
    }

    /// Read a single byte.
    #[inline]
    fn read_u8_ch_sync(&self) -> Result<u8, ClickhouseWireError> {
        let borrow = self.peek_exactly::<1>().map_err(Into::into)?;
        let value = borrow[0];
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian u16.
    #[inline]
    fn read_u16_le_ch_sync(&self) -> Result<u16, ClickhouseWireError> {
        let borrow = self.peek_exactly::<2>().map_err(Into::into)?;
        let value = u16::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian u32.
    #[inline]
    fn read_u32_le_ch_sync(&self) -> Result<u32, ClickhouseWireError> {
        let borrow = self.peek_exactly::<4>().map_err(Into::into)?;
        let value = u32::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian u64.
    #[inline]
    fn read_u64_le_ch_sync(&self) -> Result<u64, ClickhouseWireError> {
        let borrow = self.peek_exactly::<8>().map_err(Into::into)?;
        let value = u64::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian i8.
    #[inline]
    fn read_i8_ch_sync(&self) -> Result<i8, ClickhouseWireError> {
        let borrow = self.peek_exactly::<1>().map_err(Into::into)?;
        let value = borrow[0] as i8;
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian i16.
    #[inline]
    fn read_i16_le_ch_sync(&self) -> Result<i16, ClickhouseWireError> {
        let borrow = self.peek_exactly::<2>().map_err(Into::into)?;
        let value = i16::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian i32.
    #[inline]
    fn read_i32_le_ch_sync(&self) -> Result<i32, ClickhouseWireError> {
        let borrow = self.peek_exactly::<4>().map_err(Into::into)?;
        let value = i32::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian i64.
    #[inline]
    fn read_i64_le_ch_sync(&self) -> Result<i64, ClickhouseWireError> {
        let borrow = self.peek_exactly::<8>().map_err(Into::into)?;
        let value = i64::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian f32.
    #[inline]
    fn read_f32_le_ch_sync(&self) -> Result<f32, ClickhouseWireError> {
        let borrow = self.peek_exactly::<4>().map_err(Into::into)?;
        let value = f32::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian f64.
    #[inline]
    fn read_f64_le_ch_sync(&self) -> Result<f64, ClickhouseWireError> {
        let borrow = self.peek_exactly::<8>().map_err(Into::into)?;
        let value = f64::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian u128 (for UUID, Int128, checksums).
    #[inline]
    fn read_u128_le_ch_sync(&self) -> Result<u128, ClickhouseWireError> {
        let borrow = self.peek_exactly::<16>().map_err(Into::into)?;
        let value = u128::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian i128.
    #[inline]
    fn read_i128_le_ch_sync(&self) -> Result<i128, ClickhouseWireError> {
        let borrow = self.peek_exactly::<16>().map_err(Into::into)?;
        let value = i128::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a fixed-size byte array.
    #[inline]
    fn read_fixed_ch_sync<const N: usize>(&self) -> Result<[u8; N], ClickhouseWireError> {
        let borrow = self.peek_exactly::<N>().map_err(Into::into)?;
        let value = *borrow;
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read N bytes into a Vec.
    #[inline]
    fn read_bytes_ch_sync(&self, len: usize) -> Result<Vec<u8>, ClickhouseWireError> {
        let data = self.read_bytes_sync(len).map_err(Into::into)?;
        Ok(data.to_vec())
    }

    /// Read a boolean (single byte, 0 = false, non-zero = true).
    #[inline]
    fn read_bool_ch_sync(&self) -> Result<bool, ClickhouseWireError> {
        Ok(self.read_u8_ch_sync()? != 0)
    }
}

// Blanket implementation for all types that implement WireReadSync
impl<T> ClickhouseReadSyncExt for T
where
    T: WireReadSync + ?Sized,
    T::ReadError: Into<ClickhouseWireError>,
{
}

/// Extension trait for asynchronous ClickHouse wire protocol reading.
///
/// Provides async versions of all the synchronous reading methods.
pub trait ClickhouseReadExt: WireRead
where
    Self::ReadError: Into<ClickhouseWireError>,
{
    /// Read a VarUInt asynchronously.
    async fn read_varuint(&self) -> Result<u64, ClickhouseWireError> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;

        for _ in 0..VARINT_MAX_BYTES {
            let borrow = self.peek_read_exactly::<1>().await.map_err(Into::into)?;
            let byte = borrow[0];
            self.accept_exactly(&borrow).map_err(Into::into)?;

            let value = (byte & 0x7F) as u64;

            if shift >= 64 || (shift == 63 && value > 1) {
                return Err(ClickhouseWireError::VarUIntOverflow);
            }

            result |= value << shift;
            shift += 7;

            if byte & 0x80 == 0 {
                return Ok(result);
            }
        }

        Err(ClickhouseWireError::VarUIntTooLong)
    }

    /// Read a ClickHouse string asynchronously.
    async fn read_ch_string(&self) -> Result<Vec<u8>, ClickhouseWireError> {
        let len = self.read_varuint().await? as usize;

        if len > MAX_STRING_SIZE {
            return Err(ClickhouseWireError::StringTooLarge { length: len, max: MAX_STRING_SIZE });
        }

        if len == 0 {
            return Ok(Vec::new());
        }

        let data = self.peek_read(Some(len)).await.map_err(Into::into)?;
        let result = data.to_vec();
        self.accept(&data, None).map_err(Into::into)?;
        Ok(result)
    }

    /// Read a ClickHouse string as UTF-8 asynchronously.
    async fn read_ch_string_utf8(&self) -> Result<String, ClickhouseWireError> {
        let bytes = self.read_ch_string().await?;
        String::from_utf8(bytes).map_err(|e| ClickhouseWireError::InvalidUtf8(e.utf8_error()))
    }

    /// Read a single byte asynchronously.
    async fn read_u8_ch(&self) -> Result<u8, ClickhouseWireError> {
        let borrow = self.peek_read_exactly::<1>().await.map_err(Into::into)?;
        let value = borrow[0];
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian u16 asynchronously.
    async fn read_u16_le_ch(&self) -> Result<u16, ClickhouseWireError> {
        let borrow = self.peek_read_exactly::<2>().await.map_err(Into::into)?;
        let value = u16::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian u32 asynchronously.
    async fn read_u32_le_ch(&self) -> Result<u32, ClickhouseWireError> {
        let borrow = self.peek_read_exactly::<4>().await.map_err(Into::into)?;
        let value = u32::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian u64 asynchronously.
    async fn read_u64_le_ch(&self) -> Result<u64, ClickhouseWireError> {
        let borrow = self.peek_read_exactly::<8>().await.map_err(Into::into)?;
        let value = u64::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian i32 asynchronously.
    async fn read_i32_le_ch(&self) -> Result<i32, ClickhouseWireError> {
        let borrow = self.peek_read_exactly::<4>().await.map_err(Into::into)?;
        let value = i32::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian i64 asynchronously.
    async fn read_i64_le_ch(&self) -> Result<i64, ClickhouseWireError> {
        let borrow = self.peek_read_exactly::<8>().await.map_err(Into::into)?;
        let value = i64::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian f64 asynchronously.
    async fn read_f64_le_ch(&self) -> Result<f64, ClickhouseWireError> {
        let borrow = self.peek_read_exactly::<8>().await.map_err(Into::into)?;
        let value = f64::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a little-endian u128 asynchronously.
    async fn read_u128_le_ch(&self) -> Result<u128, ClickhouseWireError> {
        let borrow = self.peek_read_exactly::<16>().await.map_err(Into::into)?;
        let value = u128::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a boolean asynchronously.
    async fn read_bool_ch(&self) -> Result<bool, ClickhouseWireError> {
        Ok(self.read_u8_ch().await? != 0)
    }
}

// Blanket implementation for all types that implement WireRead
impl<T> ClickhouseReadExt for T
where
    T: WireRead + ?Sized,
    T::ReadError: Into<ClickhouseWireError>,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_read_varuint_zero() {
        let data = [0x00];
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_varuint_sync().unwrap(), 0);
    }

    #[test]
    fn test_read_varuint_single_byte() {
        let data = [0x7F];
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_varuint_sync().unwrap(), 127);
    }

    #[test]
    fn test_read_varuint_two_bytes() {
        let data = [0x80, 0x01];
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_varuint_sync().unwrap(), 128);

        let data = [0xAC, 0x02];
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_varuint_sync().unwrap(), 300);
    }

    #[test]
    fn test_read_ch_string_empty() {
        let data = [0x00]; // length = 0
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_ch_string_sync().unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn test_read_ch_string() {
        let data = [0x05, b'h', b'e', b'l', b'l', b'o']; // length = 5, "hello"
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_ch_string_sync().unwrap(), b"hello".to_vec());
    }

    #[test]
    fn test_read_ch_string_utf8() {
        let data = [0x05, b'h', b'e', b'l', b'l', b'o'];
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_ch_string_utf8_sync().unwrap(), "hello");
    }

    #[test]
    fn test_read_integers() {
        // u8
        let stream = SliceStream::new(&[0x42]);
        assert_eq!(stream.read_u8_ch_sync().unwrap(), 0x42);

        // u16 LE
        let stream = SliceStream::new(&[0x01, 0x02]);
        assert_eq!(stream.read_u16_le_ch_sync().unwrap(), 0x0201);

        // u32 LE
        let stream = SliceStream::new(&[0x01, 0x02, 0x03, 0x04]);
        assert_eq!(stream.read_u32_le_ch_sync().unwrap(), 0x04030201);

        // i32 LE (negative)
        let stream = SliceStream::new(&[0xFF, 0xFF, 0xFF, 0xFF]);
        assert_eq!(stream.read_i32_le_ch_sync().unwrap(), -1);
    }

    #[test]
    fn test_read_float() {
        let value: f64 = 3.25;
        let bytes = value.to_le_bytes();
        let stream = SliceStream::new(&bytes);
        let result = stream.read_f64_le_ch_sync().unwrap();
        assert!((result - value).abs() < f64::EPSILON);
    }

    #[test]
    fn test_read_bool() {
        let stream = SliceStream::new(&[0x00]);
        assert!(!stream.read_bool_ch_sync().unwrap());

        let stream = SliceStream::new(&[0x01]);
        assert!(stream.read_bool_ch_sync().unwrap());

        let stream = SliceStream::new(&[0xFF]);
        assert!(stream.read_bool_ch_sync().unwrap());
    }

    #[test]
    fn test_read_u128() {
        let value: u128 = 0x0102030405060708090A0B0C0D0E0F10;
        let bytes = value.to_le_bytes();
        let stream = SliceStream::new(&bytes);
        assert_eq!(stream.read_u128_le_ch_sync().unwrap(), value);
    }
}
