//! TDS 5.0 CAPABILITY token.
//!
//! The CAPABILITY token is used for feature negotiation between client and server.
//! It contains bitmasks indicating which protocol features are supported.

use crate::error::SybaseWireError;
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

/// Capability type identifiers.
pub mod capability_types {
    /// Request capabilities (client to server).
    pub const CAP_REQUEST: u8 = 0x01;
    /// Response capabilities (server to client).
    pub const CAP_RESPONSE: u8 = 0x02;
}

/// Request capability flags (byte 0).
pub mod request_caps_0 {
    pub const REQ_LANG: u8 = 0x01;
    pub const REQ_RPC: u8 = 0x02;
    pub const REQ_EVT: u8 = 0x04;
    pub const REQ_MSTMT: u8 = 0x08;
    pub const REQ_BCP: u8 = 0x10;
    pub const REQ_CURSOR: u8 = 0x20;
    pub const REQ_DYNF: u8 = 0x40;
    pub const REQ_MSG: u8 = 0x80;
}

/// Request capability flags (byte 1).
pub mod request_caps_1 {
    pub const REQ_PARAM: u8 = 0x01;
    pub const DATA_INT1: u8 = 0x02;
    pub const DATA_INT2: u8 = 0x04;
    pub const DATA_INT4: u8 = 0x08;
    pub const DATA_BIT: u8 = 0x10;
    pub const DATA_CHAR: u8 = 0x20;
    pub const DATA_VCHAR: u8 = 0x40;
    pub const DATA_BIN: u8 = 0x80;
}

/// Request capability flags (byte 2).
pub mod request_caps_2 {
    pub const DATA_VBIN: u8 = 0x01;
    pub const DATA_MNY8: u8 = 0x02;
    pub const DATA_MNY4: u8 = 0x04;
    pub const DATA_DATE8: u8 = 0x08;
    pub const DATA_DATE4: u8 = 0x10;
    pub const DATA_FLT4: u8 = 0x20;
    pub const DATA_FLT8: u8 = 0x40;
    pub const DATA_NUM: u8 = 0x80;
}

/// Request capability flags (byte 3).
pub mod request_caps_3 {
    pub const DATA_TEXT: u8 = 0x01;
    pub const DATA_IMAGE: u8 = 0x02;
    pub const DATA_DEC: u8 = 0x04;
    pub const DATA_LCHAR: u8 = 0x08;
    pub const DATA_LBIN: u8 = 0x10;
    pub const DATA_INTN: u8 = 0x20;
    pub const DATA_DATETIMEN: u8 = 0x40;
    pub const DATA_MONEYN: u8 = 0x80;
}

/// Response capability flags (byte 0).
pub mod response_caps_0 {
    pub const RES_NOMSG: u8 = 0x02;
    pub const RES_NOEED: u8 = 0x04;
    pub const RES_NOPARAM: u8 = 0x08;
    pub const DATA_NOINT1: u8 = 0x10;
    pub const DATA_NOINT2: u8 = 0x20;
    pub const DATA_NOINT4: u8 = 0x40;
    pub const DATA_NOBIT: u8 = 0x80;
}

/// CAPABILITY token.
///
/// Used for TDS 5.0 feature negotiation.
#[derive(Clone, Debug)]
pub struct Capability {
    /// Token length.
    pub length: u16,
    /// Capability type (request or response).
    pub cap_type: u8,
    /// Capability mask length.
    pub cap_len: u8,
    /// Capability bitmask.
    pub capabilities: Vec<u8>,
}

impl Capability {
    /// Parse a CAPABILITY token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(
        stream: &'s SliceStream<'s>,
    ) -> Result<Capability, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Capability type (1 byte)
        let cap_type = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // Capability length (1 byte)
        let cap_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // Capability bytes
        let capabilities = if cap_len > 0 {
            let borrow = stream.peek(Some(cap_len as usize)).map_err(SybaseParseError::Stream)?;
            let caps = borrow[..cap_len as usize].to_vec();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            caps
        } else {
            Vec::new()
        };

        Ok(Capability { length, cap_type, cap_len, capabilities })
    }

    /// Check if this is a request capability.
    pub fn is_request(&self) -> bool {
        self.cap_type == capability_types::CAP_REQUEST
    }

    /// Check if this is a response capability.
    pub fn is_response(&self) -> bool {
        self.cap_type == capability_types::CAP_RESPONSE
    }

    /// Check if a specific capability bit is set.
    pub fn has_capability(&self, byte_index: usize, bit_mask: u8) -> bool {
        self.capabilities.get(byte_index).map(|b| b & bit_mask != 0).unwrap_or(false)
    }

    /// Check if RPC is supported.
    pub fn supports_rpc(&self) -> bool {
        self.has_capability(0, request_caps_0::REQ_RPC)
    }

    /// Check if cursors are supported.
    pub fn supports_cursor(&self) -> bool {
        self.has_capability(0, request_caps_0::REQ_CURSOR)
    }

    /// Check if dynamic SQL is supported.
    pub fn supports_dynamic(&self) -> bool {
        self.has_capability(0, request_caps_0::REQ_DYNF)
    }

    /// Check if BCP (bulk copy) is supported.
    pub fn supports_bcp(&self) -> bool {
        self.has_capability(0, request_caps_0::REQ_BCP)
    }
}

/// Builder for creating CAPABILITY tokens.
pub struct CapabilityBuilder {
    cap_type: u8,
    capabilities: Vec<u8>,
}

impl CapabilityBuilder {
    /// Create a new request capability builder.
    pub fn request() -> Self {
        Self {
            cap_type: capability_types::CAP_REQUEST,
            capabilities: vec![0; 14],
        }
    }

    /// Create a new response capability builder.
    pub fn response() -> Self {
        Self {
            cap_type: capability_types::CAP_RESPONSE,
            capabilities: vec![0; 14],
        }
    }

    /// Enable RPC support.
    pub fn with_rpc(mut self) -> Self {
        if !self.capabilities.is_empty() {
            self.capabilities[0] |= request_caps_0::REQ_RPC;
        }
        self
    }

    /// Enable cursor support.
    pub fn with_cursor(mut self) -> Self {
        if !self.capabilities.is_empty() {
            self.capabilities[0] |= request_caps_0::REQ_CURSOR;
        }
        self
    }

    /// Enable dynamic SQL support.
    pub fn with_dynamic(mut self) -> Self {
        if !self.capabilities.is_empty() {
            self.capabilities[0] |= request_caps_0::REQ_DYNF;
        }
        self
    }

    /// Enable BCP support.
    pub fn with_bcp(mut self) -> Self {
        if !self.capabilities.is_empty() {
            self.capabilities[0] |= request_caps_0::REQ_BCP;
        }
        self
    }

    /// Enable all standard data types.
    pub fn with_standard_types(mut self) -> Self {
        if self.capabilities.len() >= 4 {
            // Enable common data types
            self.capabilities[1] = 0xFF; // All byte 1 types
            self.capabilities[2] = 0xFF; // All byte 2 types
            self.capabilities[3] = 0xFF; // All byte 3 types
        }
        self
    }

    /// Build the capability bytes.
    pub fn build(self) -> Vec<u8> {
        let mut result = Vec::new();
        result.push(self.cap_type);
        result.push(self.capabilities.len() as u8);
        result.extend(self.capabilities);
        result
    }
}

impl Default for CapabilityBuilder {
    fn default() -> Self {
        Self::request()
    }
}
