//! VarUInt (variable-length unsigned integer) encoding and decoding.
//!
//! ClickHouse uses a variable-length encoding for unsigned integers where:
//! - Each byte uses 7 bits for data and 1 bit (high bit) as continuation flag
//! - If the high bit is set (0x80), more bytes follow
//! - Maximum of 9 bytes for a 64-bit integer
//!
//! # Encoding Examples
//!
//! - `0` encodes to `[0x00]`
//! - `127` encodes to `[0x7F]`
//! - `128` encodes to `[0x80, 0x01]`
//! - `16383` encodes to `[0xFF, 0x7F]`
//! - `16384` encodes to `[0x80, 0x80, 0x01]`

use crate::VARINT_MAX_BYTES;
use crate::error::ClickhouseWireError;
use std::io::{self, Write};

/// Decode a VarUInt from a byte slice.
///
/// Returns the decoded value and the number of bytes consumed.
///
/// # Errors
///
/// Returns an error if:
/// - The encoding exceeds 9 bytes
/// - The value would overflow u64
/// - The slice is too short
#[inline]
pub fn decode_varuint(bytes: &[u8]) -> Result<(u64, usize), ClickhouseWireError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;

    for (i, &byte) in bytes.iter().enumerate().take(VARINT_MAX_BYTES) {
        let value = (byte & 0x7F) as u64;

        // Check for overflow before shifting
        if shift >= 64 || (shift == 63 && value > 1) {
            return Err(ClickhouseWireError::VarUIntOverflow);
        }

        result |= value << shift;
        shift += 7;

        // High bit clear means this is the last byte
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
    }

    // If we've read VARINT_MAX_BYTES and still have continuation, it's too long
    if bytes.len() >= VARINT_MAX_BYTES {
        Err(ClickhouseWireError::VarUIntTooLong)
    } else {
        // Not enough bytes in the slice
        Err(ClickhouseWireError::incomplete(VARINT_MAX_BYTES, bytes.len()))
    }
}

/// Encode a VarUInt to a byte buffer.
///
/// Returns the number of bytes written.
#[inline]
pub fn encode_varuint(mut value: u64, buf: &mut [u8; VARINT_MAX_BYTES]) -> usize {
    let mut i = 0;

    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;

        if value != 0 {
            byte |= 0x80; // Set continuation bit
        }

        buf[i] = byte;
        i += 1;

        if value == 0 {
            break;
        }
    }

    i
}

/// Write a VarUInt to a writer.
#[inline]
pub fn write_varuint<W: Write + ?Sized>(writer: &mut W, value: u64) -> io::Result<usize> {
    let mut buf = [0u8; VARINT_MAX_BYTES];
    let len = encode_varuint(value, &mut buf);
    writer.write_all(&buf[..len])?;
    Ok(len)
}

/// Calculate the encoded length of a VarUInt without encoding it.
#[inline]
pub const fn varuint_len(value: u64) -> usize {
    if value == 0 {
        return 1;
    }

    // Calculate number of bits needed, then divide by 7 (rounding up)
    let bits = 64 - value.leading_zeros() as usize;
    bits.div_ceil(7)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_zero() {
        let mut buf = [0u8; VARINT_MAX_BYTES];
        let len = encode_varuint(0, &mut buf);
        assert_eq!(len, 1);
        assert_eq!(buf[0], 0x00);

        let (decoded, consumed) = decode_varuint(&buf[..len]).unwrap();
        assert_eq!(decoded, 0);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_encode_decode_single_byte() {
        for value in [1u64, 63, 127] {
            let mut buf = [0u8; VARINT_MAX_BYTES];
            let len = encode_varuint(value, &mut buf);
            assert_eq!(len, 1);
            assert_eq!(buf[0], value as u8);

            let (decoded, consumed) = decode_varuint(&buf[..len]).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(consumed, 1);
        }
    }

    #[test]
    fn test_encode_decode_two_bytes() {
        // 128 = 0x80 + (0x01 << 7)
        let mut buf = [0u8; VARINT_MAX_BYTES];
        let len = encode_varuint(128, &mut buf);
        assert_eq!(len, 2);
        assert_eq!(&buf[..2], &[0x80, 0x01]);

        let (decoded, consumed) = decode_varuint(&buf[..len]).unwrap();
        assert_eq!(decoded, 128);
        assert_eq!(consumed, 2);

        // 16383 = max 2-byte value
        let len = encode_varuint(16383, &mut buf);
        assert_eq!(len, 2);
        assert_eq!(&buf[..2], &[0xFF, 0x7F]);

        let (decoded, consumed) = decode_varuint(&buf[..len]).unwrap();
        assert_eq!(decoded, 16383);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn test_encode_decode_three_bytes() {
        // 16384 = first 3-byte value
        let mut buf = [0u8; VARINT_MAX_BYTES];
        let len = encode_varuint(16384, &mut buf);
        assert_eq!(len, 3);
        assert_eq!(&buf[..3], &[0x80, 0x80, 0x01]);

        let (decoded, consumed) = decode_varuint(&buf[..len]).unwrap();
        assert_eq!(decoded, 16384);
        assert_eq!(consumed, 3);
    }

    #[test]
    fn test_encode_decode_max_u64() {
        // u64::MAX requires 10 bytes (ceil(64/7) = 10), but ClickHouse's VarUInt
        // only uses 9 bytes max (63 bits). Test the max value that fits in 9 bytes.
        let max_9_byte_value = (1u64 << 63) - 1;

        let mut buf = [0u8; VARINT_MAX_BYTES];
        let len = encode_varuint(max_9_byte_value, &mut buf);
        assert_eq!(len, 9); // Max 9-byte value requires exactly 9 bytes

        let (decoded, consumed) = decode_varuint(&buf[..len]).unwrap();
        assert_eq!(decoded, max_9_byte_value);
        assert_eq!(consumed, 9);
    }

    #[test]
    fn test_encode_decode_large_values() {
        let test_values = [
            255u64,
            256,
            1000,
            10000,
            100000,
            1000000,
            1_000_000_000,
            1_000_000_000_000,
            (1u64 << 63) - 1, // Max value that fits in 9 bytes
        ];

        for value in test_values {
            let mut buf = [0u8; VARINT_MAX_BYTES];
            let len = encode_varuint(value, &mut buf);

            let (decoded, consumed) = decode_varuint(&buf[..len]).unwrap();
            assert_eq!(decoded, value, "roundtrip failed for {}", value);
            assert_eq!(consumed, len);
        }
    }

    #[test]
    fn test_varuint_len() {
        assert_eq!(varuint_len(0), 1);
        assert_eq!(varuint_len(127), 1);
        assert_eq!(varuint_len(128), 2);
        assert_eq!(varuint_len(16383), 2);
        assert_eq!(varuint_len(16384), 3);
        assert_eq!(varuint_len(1_000_000), 3);
        assert_eq!(varuint_len(1_000_000_000), 5);
    }

    #[test]
    fn test_decode_incomplete() {
        // Continuation bit set but no more bytes
        let bytes = [0x80];
        let result = decode_varuint(&bytes);
        assert!(matches!(result, Err(ClickhouseWireError::IncompletePacket { .. })));
    }

    #[test]
    fn test_write_varuint() {
        let mut buf = Vec::new();
        let len = write_varuint(&mut buf, 300).unwrap();
        assert_eq!(len, 2);
        assert_eq!(buf, vec![0xAC, 0x02]);

        let (decoded, _) = decode_varuint(&buf).unwrap();
        assert_eq!(decoded, 300);
    }
}
