//! MongoDB Wire Protocol message header.
//!
//! All MongoDB wire protocol messages start with a standard 16-byte header:
//! - messageLength (i32): Total message size including header
//! - requestID (i32): Client-generated identifier
//! - responseTo (i32): requestID from original request (for responses)
//! - opCode (i32): Type of message

use crate::error::MongoWireError;
use wire_stream::{WireRead, WireReadSync};

/// MongoDB Wire Protocol opcodes.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
#[repr(i32)]
pub enum OpCode {
    /// Reply to a client request (deprecated)
    Reply = 1,
    /// Update document (deprecated)
    Update = 2001,
    /// Insert new document (deprecated)
    Insert = 2002,
    /// Query a collection (deprecated)
    Query = 2004,
    /// Get more data from a query (deprecated)
    GetMore = 2005,
    /// Delete documents (deprecated)
    Delete = 2006,
    /// Notify database that cursors are finished (deprecated)
    KillCursors = 2007,
    /// Compressed message
    Compressed = 2012,
    /// Send message using the format introduced in MongoDB 3.6
    Msg = 2013,
}

impl OpCode {
    /// Parse an opcode from its i32 representation.
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            1 => Some(Self::Reply),
            2001 => Some(Self::Update),
            2002 => Some(Self::Insert),
            2004 => Some(Self::Query),
            2005 => Some(Self::GetMore),
            2006 => Some(Self::Delete),
            2007 => Some(Self::KillCursors),
            2012 => Some(Self::Compressed),
            2013 => Some(Self::Msg),
            _ => None,
        }
    }

    /// Returns true if this is a deprecated opcode.
    pub fn is_deprecated(&self) -> bool {
        !matches!(self, Self::Msg | Self::Compressed)
    }
}

/// MongoDB Wire Protocol message header (16 bytes).
#[derive(Copy, Clone, Debug)]
pub struct MessageHeader {
    /// Total message size in bytes, including this header.
    pub message_length: i32,
    /// Client or server-generated identifier for this message.
    pub request_id: i32,
    /// requestID from the original request (for responses).
    pub response_to: i32,
    /// Type of message.
    pub op_code: i32,
}

impl MessageHeader {
    /// Header size in bytes.
    pub const SIZE: usize = 16;

    /// Parse a message header synchronously.
    #[inline]
    pub fn parse_sync<S: WireReadSync + ?Sized>(stream: &S) -> Result<Self, S::ReadError> {
        let bytes = stream.peek_exactly::<16>()?;
        stream.accept_exactly(&bytes)?;

        Ok(Self {
            message_length: i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            request_id: i32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            response_to: i32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            op_code: i32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
        })
    }

    /// Parse a message header asynchronously.
    #[inline]
    pub async fn parse<S: WireRead + ?Sized>(stream: &S) -> Result<Self, S::ReadError> {
        let bytes = stream.peek_read_exactly::<16>().await?;
        stream.accept_exactly(&bytes)?;

        Ok(Self {
            message_length: i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            request_id: i32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            response_to: i32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            op_code: i32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
        })
    }

    /// Get the opcode as an enum.
    pub fn op_code(&self) -> Option<OpCode> {
        OpCode::from_i32(self.op_code)
    }

    /// Get the body length (message length minus header).
    pub fn body_length(&self) -> Result<usize, MongoWireError> {
        if self.message_length < Self::SIZE as i32 {
            return Err(MongoWireError::InvalidMessageLength(self.message_length));
        }
        let length = self.message_length as usize;
        if length > crate::MAX_MESSAGE_SIZE {
            return Err(MongoWireError::MessageTooLarge { length, max: crate::MAX_MESSAGE_SIZE });
        }
        Ok(length - Self::SIZE)
    }

    /// Encode the header to bytes.
    pub fn encode(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[0..4].copy_from_slice(&self.message_length.to_le_bytes());
        buf[4..8].copy_from_slice(&self.request_id.to_le_bytes());
        buf[8..12].copy_from_slice(&self.response_to.to_le_bytes());
        buf[12..16].copy_from_slice(&self.op_code.to_le_bytes());
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_parse_header() {
        // A minimal OP_MSG header
        let data = [
            0x15, 0x00, 0x00, 0x00, // messageLength = 21
            0x01, 0x00, 0x00, 0x00, // requestID = 1
            0x00, 0x00, 0x00, 0x00, // responseTo = 0
            0xDD, 0x07, 0x00, 0x00, // opCode = 2013 (OP_MSG)
        ];
        let stream = SliceStream::new(&data);

        let header = MessageHeader::parse_sync(&stream).expect("Header parsing failed");
        assert_eq!(header.message_length, 21);
        assert_eq!(header.request_id, 1);
        assert_eq!(header.response_to, 0);
        assert_eq!(header.op_code(), Some(OpCode::Msg));
    }

    #[test]
    fn test_roundtrip() {
        let original = MessageHeader {
            message_length: 100,
            request_id: 42,
            response_to: 0,
            op_code: OpCode::Msg as i32,
        };

        let encoded = original.encode();
        let stream = SliceStream::new(&encoded);
        let decoded = MessageHeader::parse_sync(&stream).expect("Header parsing failed");

        assert_eq!(original.message_length, decoded.message_length);
        assert_eq!(original.request_id, decoded.request_id);
        assert_eq!(original.response_to, decoded.response_to);
        assert_eq!(original.op_code, decoded.op_code);
    }
}
