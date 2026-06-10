/// Zero-copy RESP frame scanner with optional lazy parsing.
///
/// This module provides efficient frame boundary detection without allocating
/// or parsing frame contents. When full parsing is needed, it delegates to
/// the existing decoder.
use bytes::Bytes;
use error::ResultEP;

/// RESP2/RESP3 type prefix bytes
mod prefix {
    pub const SIMPLE_STRING: u8 = b'+';
    pub const ERROR: u8 = b'-';
    pub const INTEGER: u8 = b':';
    pub const BULK_STRING: u8 = b'$';
    pub const ARRAY: u8 = b'*';
    // RESP3 additions
    pub const NULL: u8 = b'_';
    pub const BOOLEAN: u8 = b'#';
    pub const DOUBLE: u8 = b',';
    pub const BIG_NUMBER: u8 = b'(';
    pub const BLOB_ERROR: u8 = b'!';
    pub const VERBATIM_STRING: u8 = b'=';
    pub const MAP: u8 = b'%';
    pub const SET: u8 = b'~';
    pub const PUSH: u8 = b'>';
}

/// Frame type detected during boundary scanning (no content parsing).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
    Null,
    // RESP3
    Boolean,
    Double,
    BigNumber,
    BlobError,
    VerbatimString,
    Map,
    Set,
    Push,
}

impl FrameType {
    /// Parse frame type from prefix byte.
    #[inline]
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            prefix::SIMPLE_STRING => Some(Self::SimpleString),
            prefix::ERROR => Some(Self::Error),
            prefix::INTEGER => Some(Self::Integer),
            prefix::BULK_STRING => Some(Self::BulkString),
            prefix::ARRAY => Some(Self::Array),
            prefix::NULL => Some(Self::Null),
            prefix::BOOLEAN => Some(Self::Boolean),
            prefix::DOUBLE => Some(Self::Double),
            prefix::BIG_NUMBER => Some(Self::BigNumber),
            prefix::BLOB_ERROR => Some(Self::BlobError),
            prefix::VERBATIM_STRING => Some(Self::VerbatimString),
            prefix::MAP => Some(Self::Map),
            prefix::SET => Some(Self::Set),
            prefix::PUSH => Some(Self::Push),
            _ => None,
        }
    }

    /// Whether this is an aggregate type (array, map, set, push).
    #[inline]
    pub fn is_aggregate(&self) -> bool {
        matches!(self, Self::Array | Self::Map | Self::Set | Self::Push)
    }

    /// Whether this type uses length-prefixed content (bulk string, blob error, verbatim).
    #[inline]
    pub fn is_length_prefixed(&self) -> bool {
        matches!(self, Self::BulkString | Self::BlobError | Self::VerbatimString)
    }
}

/// A zero-copy frame reference. Holds raw bytes without parsing content.
#[derive(Debug, Clone)]
pub struct RawFrame<'a> {
    /// The complete frame bytes including prefix and CRLF.
    pub bytes: &'a [u8],
    /// Detected frame type.
    pub frame_type: FrameType,
}

impl<'a> RawFrame<'a> {
    /// Get the frame bytes as a `Bytes` instance (zero-copy if from Bytes source).
    pub fn to_bytes(&self, source: &Bytes) -> Bytes {
        // Calculate offset within source buffer
        let start = self.bytes.as_ptr() as usize - source.as_ptr() as usize;
        source.slice(start..start + self.bytes.len())
    }
}

/// Result of frame scanning.
#[derive(Debug)]
pub enum ScannedFrame<'a> {
    /// Zero-copy raw frame reference.
    Raw(RawFrame<'a>),
    /// Fully parsed frame (when parse_content=true).
    Parsed(crate::protocol::decoder::DecoderRespFrame),
}

impl<'a> ScannedFrame<'a> {
    /// Get bytes consumed by this frame.
    pub fn len(&self) -> usize {
        match self {
            Self::Raw(raw) => raw.bytes.len(),
            Self::Parsed(_) => 0, // Caller tracks this separately
        }
    }

    /// Check if this frame consumed zero bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if this is a raw (unparsed) frame.
    pub fn is_raw(&self) -> bool {
        matches!(self, Self::Raw(_))
    }

    /// Get the frame type.
    pub fn frame_type(&self) -> FrameType {
        match self {
            Self::Raw(raw) => raw.frame_type,
            Self::Parsed(frame) => match frame {
                crate::protocol::decoder::DecoderRespFrame::Resp2(f) => {
                    use redis_protocol::resp2::types::OwnedFrame;
                    match f {
                        OwnedFrame::SimpleString(_) => FrameType::SimpleString,
                        OwnedFrame::Error(_) => FrameType::Error,
                        OwnedFrame::Integer(_) => FrameType::Integer,
                        OwnedFrame::BulkString(_) => FrameType::BulkString,
                        OwnedFrame::Array(_) => FrameType::Array,
                        OwnedFrame::Null => FrameType::Null,
                    }
                }
                crate::protocol::decoder::DecoderRespFrame::Resp3(f) => {
                    use redis_protocol::resp3::types::OwnedFrame;
                    match f {
                        OwnedFrame::SimpleString { .. } => FrameType::SimpleString,
                        OwnedFrame::SimpleError { .. } => FrameType::Error,
                        OwnedFrame::Number { .. } => FrameType::Integer,
                        OwnedFrame::BlobString { .. } => FrameType::BulkString,
                        OwnedFrame::Array { .. } => FrameType::Array,
                        OwnedFrame::Null => FrameType::Null,
                        OwnedFrame::Boolean { .. } => FrameType::Boolean,
                        OwnedFrame::Double { .. } => FrameType::Double,
                        OwnedFrame::BigNumber { .. } => FrameType::BigNumber,
                        OwnedFrame::BlobError { .. } => FrameType::BlobError,
                        OwnedFrame::VerbatimString { .. } => FrameType::VerbatimString,
                        OwnedFrame::Map { .. } => FrameType::Map,
                        OwnedFrame::Set { .. } => FrameType::Set,
                        OwnedFrame::Push { .. } => FrameType::Push,
                        _ => FrameType::SimpleString, // Fallback for Hello, ChunkedString
                    }
                }
            },
        }
    }
}

/// Scan for frame boundary without parsing content.
/// Returns (frame_type, bytes_consumed) or None if incomplete.
#[inline]
pub fn scan_frame_boundary(buffer: &[u8]) -> Option<(FrameType, usize)> {
    if buffer.is_empty() {
        return None;
    }

    let frame_type = FrameType::from_byte(buffer[0])?;
    let consumed = scan_frame_length(buffer, frame_type)?;
    Some((frame_type, consumed))
}

/// Core scanning logic - finds end of frame without parsing.
fn scan_frame_length(buffer: &[u8], frame_type: FrameType) -> Option<usize> {
    match frame_type {
        // Simple line types: +OK\r\n, -ERR\r\n, :123\r\n, etc.
        FrameType::SimpleString
        | FrameType::Error
        | FrameType::Integer
        | FrameType::Null
        | FrameType::Boolean
        | FrameType::Double
        | FrameType::BigNumber => scan_simple_line(buffer),

        // Length-prefixed: $5\r\nhello\r\n
        FrameType::BulkString | FrameType::BlobError | FrameType::VerbatimString => scan_bulk_string(buffer),

        // Aggregates: *2\r\n... or %2\r\n...
        FrameType::Array | FrameType::Set | FrameType::Push => scan_array(buffer),
        FrameType::Map => scan_map(buffer),
    }
}

/// Scan a simple line-terminated frame (+, -, :, _, #, ,, ().
#[inline]
fn scan_simple_line(buffer: &[u8]) -> Option<usize> {
    find_crlf(buffer).map(|pos| pos + 2)
}

/// Scan a bulk string: $<len>\r\n<data>\r\n or $-1\r\n for null.
fn scan_bulk_string(buffer: &[u8]) -> Option<usize> {
    let crlf_pos = find_crlf(buffer)?;
    let len_str = std::str::from_utf8(&buffer[1..crlf_pos]).ok()?;
    let len: i64 = len_str.parse().ok()?;

    if len < 0 {
        // Null bulk string: $-1\r\n
        return Some(crlf_pos + 2);
    }

    let len = len as usize;
    let data_start = crlf_pos + 2;
    let frame_end = data_start + len + 2; // +2 for trailing \r\n

    if buffer.len() < frame_end {
        return None; // Incomplete
    }

    Some(frame_end)
}

/// Scan an array/set/push: *<count>\r\n<elements...>
fn scan_array(buffer: &[u8]) -> Option<usize> {
    let crlf_pos = find_crlf(buffer)?;
    let count_str = std::str::from_utf8(&buffer[1..crlf_pos]).ok()?;
    let count: i64 = count_str.parse().ok()?;

    if count < 0 {
        // Null array: *-1\r\n
        return Some(crlf_pos + 2);
    }

    let mut offset = crlf_pos + 2;
    for _ in 0..count {
        if offset >= buffer.len() {
            return None;
        }
        let frame_type = FrameType::from_byte(buffer[offset])?;
        let elem_len = scan_frame_length(&buffer[offset..], frame_type)?;
        offset += elem_len;
    }

    Some(offset)
}

/// Scan a map: %<count>\r\n<key><value>... (count = number of pairs)
fn scan_map(buffer: &[u8]) -> Option<usize> {
    let crlf_pos = find_crlf(buffer)?;
    let count_str = std::str::from_utf8(&buffer[1..crlf_pos]).ok()?;
    let count: i64 = count_str.parse().ok()?;

    if count < 0 {
        return Some(crlf_pos + 2);
    }

    // Prevent overflow: count * 2 must fit in i64
    let num_elements = count.checked_mul(2)?;

    let mut offset = crlf_pos + 2;
    // Maps have count pairs, so 2*count elements
    for _ in 0..num_elements {
        if offset >= buffer.len() {
            return None;
        }
        let frame_type = FrameType::from_byte(buffer[offset])?;
        let elem_len = scan_frame_length(&buffer[offset..], frame_type)?;
        offset += elem_len;
    }

    Some(offset)
}

/// Find \r\n in buffer, return position of \r.
#[inline]
fn find_crlf(buffer: &[u8]) -> Option<usize> {
    buffer.windows(2).position(|w| w == b"\r\n")
}

/// Main entry point: scan frame with optional full parsing.
///
/// # Arguments
/// * `buffer` - Input byte buffer
/// * `parse_content` - If true, fully parse the frame; if false, just return raw bytes
///
/// # Returns
/// * `Ok(Some((frame, consumed)))` - Successfully scanned/parsed frame
/// * `Ok(None)` - Incomplete frame, need more data
/// * `Err(_)` - Parse error (malformed frame)
pub fn scan_frame<'a>(buffer: &'a [u8], parse_content: bool) -> ResultEP<Option<(ScannedFrame<'a>, usize)>> {
    if parse_content {
        // Delegate to existing full parser
        use crate::protocol::RedisProtocol;
        use endpoint_types::protocol::EpProtocol;

        match RedisProtocol::decode_buffer(buffer) {
            Some((frame, consumed)) => Ok(Some((ScannedFrame::Parsed(frame), consumed))),
            None => Ok(None),
        }
    } else {
        // Zero-copy boundary scan only
        match scan_frame_boundary(buffer) {
            Some((frame_type, consumed)) => {
                let raw = RawFrame { bytes: &buffer[..consumed], frame_type };
                Ok(Some((ScannedFrame::Raw(raw), consumed)))
            }
            None => Ok(None),
        }
    }
}

/// Scan multiple frames from a buffer (for pipeline support).
/// Returns frames and total bytes consumed.
pub fn scan_frames<'a>(buffer: &'a [u8], parse_content: bool) -> ResultEP<(Vec<ScannedFrame<'a>>, usize)> {
    let mut frames = Vec::new();
    let mut offset = 0;

    while offset < buffer.len() {
        match scan_frame(&buffer[offset..], parse_content)? {
            Some((frame, consumed)) => {
                frames.push(frame);
                offset += consumed;
            }
            None => break, // Incomplete frame
        }
    }

    Ok((frames, offset))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_simple_string() {
        let buf = b"+OK\r\n";
        let (frame_type, len) = scan_frame_boundary(buf).unwrap();
        assert_eq!(frame_type, FrameType::SimpleString);
        assert_eq!(len, 5);
    }

    #[test]
    fn test_scan_error() {
        let buf = b"-ERR unknown command\r\n";
        let (frame_type, len) = scan_frame_boundary(buf).unwrap();
        assert_eq!(frame_type, FrameType::Error);
        assert_eq!(len, 22);
    }

    #[test]
    fn test_scan_integer() {
        let buf = b":1000\r\n";
        let (frame_type, len) = scan_frame_boundary(buf).unwrap();
        assert_eq!(frame_type, FrameType::Integer);
        assert_eq!(len, 7);
    }

    #[test]
    fn test_scan_bulk_string() {
        let buf = b"$5\r\nhello\r\n";
        let (frame_type, len) = scan_frame_boundary(buf).unwrap();
        assert_eq!(frame_type, FrameType::BulkString);
        assert_eq!(len, 11);
    }

    #[test]
    fn test_scan_null_bulk() {
        let buf = b"$-1\r\n";
        let (frame_type, len) = scan_frame_boundary(buf).unwrap();
        assert_eq!(frame_type, FrameType::BulkString);
        assert_eq!(len, 5);
    }

    #[test]
    fn test_scan_array() {
        // *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
        let buf = b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (frame_type, len) = scan_frame_boundary(buf).unwrap();
        assert_eq!(frame_type, FrameType::Array);
        assert_eq!(len, 22);
    }

    #[test]
    fn test_scan_nested_array() {
        // *2\r\n*1\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
        let buf = b"*2\r\n*1\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (frame_type, len) = scan_frame_boundary(buf).unwrap();
        assert_eq!(frame_type, FrameType::Array);
        assert_eq!(len, 26);
    }

    #[test]
    fn test_scan_incomplete() {
        let buf = b"$5\r\nhel"; // Missing data
        assert!(scan_frame_boundary(buf).is_none());
    }

    #[test]
    fn test_scan_resp3_null() {
        let buf = b"_\r\n";
        let (frame_type, len) = scan_frame_boundary(buf).unwrap();
        assert_eq!(frame_type, FrameType::Null);
        assert_eq!(len, 3);
    }

    #[test]
    fn test_scan_resp3_boolean() {
        let buf = b"#t\r\n";
        let (frame_type, len) = scan_frame_boundary(buf).unwrap();
        assert_eq!(frame_type, FrameType::Boolean);
        assert_eq!(len, 4);
    }

    #[test]
    fn test_scan_resp3_double() {
        let buf = b",1.23\r\n";
        let (frame_type, len) = scan_frame_boundary(buf).unwrap();
        assert_eq!(frame_type, FrameType::Double);
        assert_eq!(len, 7);
    }

    #[test]
    fn test_scan_resp3_map() {
        // %2\r\n$3\r\nfoo\r\n:1\r\n$3\r\nbar\r\n:2\r\n
        let buf = b"%2\r\n$3\r\nfoo\r\n:1\r\n$3\r\nbar\r\n:2\r\n";
        let (frame_type, len) = scan_frame_boundary(buf).unwrap();
        assert_eq!(frame_type, FrameType::Map);
        assert_eq!(len, 30);
    }

    #[test]
    fn test_scan_multiple_frames() {
        let buf = b"+OK\r\n:123\r\n$3\r\nfoo\r\n";
        let (frames, consumed) = scan_frames(buf, false).unwrap();
        assert_eq!(frames.len(), 3);
        assert_eq!(consumed, 20);
    }

    #[test]
    fn test_raw_frame_bytes() {
        let buf = b"$5\r\nhello\r\n+OK\r\n";
        let (frame_type, len) = scan_frame_boundary(buf).unwrap();
        let raw = RawFrame { bytes: &buf[..len], frame_type };
        assert_eq!(raw.bytes, b"$5\r\nhello\r\n");
    }
}
