//! Authentication types and helpers.
//!
//! Provides authentication support for Sybase TDS, including
//! SSPI (Security Support Provider Interface) framework.

use crate::error::SybaseWireError;
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use crate::types::packet::PacketType;
use crate::write::PacketBuilder;
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

/// Authentication methods supported by TDS.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AuthMethod {
    /// SQL Server authentication (username/password).
    SqlServer,
    /// SSPI/Windows integrated authentication.
    Sspi,
    /// Kerberos authentication.
    Kerberos,
}

/// SSPI message types.
pub mod sspi_types {
    /// NTLM Negotiate message.
    pub const NTLM_NEGOTIATE: u32 = 1;
    /// NTLM Challenge message.
    pub const NTLM_CHALLENGE: u32 = 2;
    /// NTLM Authenticate message.
    pub const NTLM_AUTHENTICATE: u32 = 3;
}

/// NTLM negotiate flags.
pub mod ntlm_flags {
    /// Unicode character set.
    pub const NEGOTIATE_UNICODE: u32 = 0x00000001;
    /// OEM character set.
    pub const NEGOTIATE_OEM: u32 = 0x00000002;
    /// Request target.
    pub const REQUEST_TARGET: u32 = 0x00000004;
    /// NTLM authentication.
    pub const NEGOTIATE_NTLM: u32 = 0x00000200;
    /// Negotiate local call.
    pub const NEGOTIATE_LOCAL_CALL: u32 = 0x00004000;
    /// Always sign.
    pub const NEGOTIATE_ALWAYS_SIGN: u32 = 0x00008000;
    /// NTLM2 key.
    pub const NEGOTIATE_NTLM2_KEY: u32 = 0x00080000;
    /// 128-bit encryption.
    pub const NEGOTIATE_128: u32 = 0x20000000;
    /// 56-bit encryption.
    pub const NEGOTIATE_56: u32 = 0x80000000;
}

/// SSPI token received from server.
#[derive(Clone, Debug)]
pub struct SspiToken {
    /// Token length.
    pub length: u16,
    /// Token data.
    pub data: Vec<u8>,
}

impl SspiToken {
    /// Parse an SSPI token from the stream.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<SspiToken, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Data
        let data = if length > 0 {
            let borrow = stream.peek(Some(length as usize)).map_err(SybaseParseError::Stream)?;
            let d = borrow[..length as usize].to_vec();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            d
        } else {
            Vec::new()
        };

        Ok(SspiToken { length, data })
    }

    /// Check if this is an NTLM negotiate message.
    pub fn is_ntlm_negotiate(&self) -> bool {
        self.data.len() >= 12 && &self.data[0..8] == b"NTLMSSP\0" && self.data[8] == 1
    }

    /// Check if this is an NTLM challenge message.
    pub fn is_ntlm_challenge(&self) -> bool {
        self.data.len() >= 12 && &self.data[0..8] == b"NTLMSSP\0" && self.data[8] == 2
    }

    /// Check if this is an NTLM authenticate message.
    pub fn is_ntlm_authenticate(&self) -> bool {
        self.data.len() >= 12 && &self.data[0..8] == b"NTLMSSP\0" && self.data[8] == 3
    }
}

/// Builder for SSPI authentication packets.
pub struct SspiBuilder {
    data: Vec<u8>,
}

impl SspiBuilder {
    /// Create a new SSPI builder with raw token data.
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Create an NTLM Type 1 (Negotiate) message.
    ///
    /// This is the initial message sent to the server to start NTLM authentication.
    pub fn ntlm_negotiate() -> Self {
        let mut data = Vec::new();

        // NTLM signature
        data.extend_from_slice(b"NTLMSSP\0");

        // Message type (1 = Negotiate)
        data.extend_from_slice(&1u32.to_le_bytes());

        // Negotiate flags
        let flags =
            ntlm_flags::NEGOTIATE_UNICODE | ntlm_flags::NEGOTIATE_NTLM | ntlm_flags::REQUEST_TARGET | ntlm_flags::NEGOTIATE_ALWAYS_SIGN;
        data.extend_from_slice(&flags.to_le_bytes());

        // Domain name (security buffer - offset, length, max length)
        data.extend_from_slice(&[0; 8]); // Empty domain

        // Workstation name (security buffer)
        data.extend_from_slice(&[0; 8]); // Empty workstation

        Self { data }
    }

    /// Create an NTLM Type 3 (Authenticate) message.
    ///
    /// This is sent after receiving the server's challenge.
    pub fn ntlm_authenticate(domain: &str, username: &str, workstation: &str, lm_response: &[u8], nt_response: &[u8]) -> Self {
        let mut data = Vec::new();

        // NTLM signature
        data.extend_from_slice(b"NTLMSSP\0");

        // Message type (3 = Authenticate)
        data.extend_from_slice(&3u32.to_le_bytes());

        // Calculate offsets
        let header_len = 88u32; // Fixed header size for Type 3
        let mut offset = header_len;

        // LM response security buffer
        let lm_len = lm_response.len() as u16;
        data.extend_from_slice(&lm_len.to_le_bytes()); // Length
        data.extend_from_slice(&lm_len.to_le_bytes()); // Max length
        data.extend_from_slice(&offset.to_le_bytes()); // Offset
        offset += lm_len as u32;

        // NT response security buffer
        let nt_len = nt_response.len() as u16;
        data.extend_from_slice(&nt_len.to_le_bytes());
        data.extend_from_slice(&nt_len.to_le_bytes());
        data.extend_from_slice(&offset.to_le_bytes());
        offset += nt_len as u32;

        // Domain security buffer (Unicode)
        let domain_bytes: Vec<u16> = domain.encode_utf16().collect();
        let domain_len = (domain_bytes.len() * 2) as u16;
        data.extend_from_slice(&domain_len.to_le_bytes());
        data.extend_from_slice(&domain_len.to_le_bytes());
        data.extend_from_slice(&offset.to_le_bytes());
        offset += domain_len as u32;

        // Username security buffer (Unicode)
        let user_bytes: Vec<u16> = username.encode_utf16().collect();
        let user_len = (user_bytes.len() * 2) as u16;
        data.extend_from_slice(&user_len.to_le_bytes());
        data.extend_from_slice(&user_len.to_le_bytes());
        data.extend_from_slice(&offset.to_le_bytes());
        offset += user_len as u32;

        // Workstation security buffer (Unicode)
        let ws_bytes: Vec<u16> = workstation.encode_utf16().collect();
        let ws_len = (ws_bytes.len() * 2) as u16;
        data.extend_from_slice(&ws_len.to_le_bytes());
        data.extend_from_slice(&ws_len.to_le_bytes());
        data.extend_from_slice(&offset.to_le_bytes());
        let _ = offset + ws_len as u32;

        // Encrypted random session key (empty)
        data.extend_from_slice(&[0u8; 8]);

        // Negotiate flags
        let flags = ntlm_flags::NEGOTIATE_UNICODE | ntlm_flags::NEGOTIATE_NTLM;
        data.extend_from_slice(&flags.to_le_bytes());

        // Pad to header length
        while data.len() < header_len as usize {
            data.push(0);
        }

        // Append payload data
        data.extend_from_slice(lm_response);
        data.extend_from_slice(nt_response);
        for &b in &domain_bytes {
            data.extend_from_slice(&b.to_le_bytes());
        }
        for &b in &user_bytes {
            data.extend_from_slice(&b.to_le_bytes());
        }
        for &b in &ws_bytes {
            data.extend_from_slice(&b.to_le_bytes());
        }

        Self { data }
    }

    /// Build the SSPI packet.
    pub fn build(self) -> Vec<u8> {
        PacketBuilder::new(PacketType::Sspi).write_bytes(&self.data).build()
    }
}

/// NTLM challenge data extracted from Type 2 message.
#[derive(Clone, Debug)]
pub struct NtlmChallenge {
    /// Server challenge (8 bytes).
    pub challenge: [u8; 8],
    /// Negotiate flags.
    pub flags: u32,
    /// Target name.
    pub target_name: String,
    /// Target info.
    pub target_info: Vec<u8>,
}

impl NtlmChallenge {
    /// Parse an NTLM Type 2 (Challenge) message.
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 32 {
            return None;
        }

        // Verify signature
        if &data[0..8] != b"NTLMSSP\0" {
            return None;
        }

        // Verify message type
        if u32::from_le_bytes([data[8], data[9], data[10], data[11]]) != 2 {
            return None;
        }

        // Extract challenge
        let mut challenge = [0u8; 8];
        challenge.copy_from_slice(&data[24..32]);

        // Flags
        let flags = u32::from_le_bytes([data[20], data[21], data[22], data[23]]);

        Some(NtlmChallenge {
            challenge,
            flags,
            target_name: String::new(), // Would need full parsing
            target_info: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sspi_negotiate() {
        let builder = SspiBuilder::ntlm_negotiate();
        let packet = builder.build();
        assert!(!packet.is_empty());
    }

    #[test]
    fn test_ntlm_challenge_parse() {
        // Minimal valid Type 2 message
        let mut data = Vec::new();
        data.extend_from_slice(b"NTLMSSP\0"); // Signature
        data.extend_from_slice(&2u32.to_le_bytes()); // Type 2
        data.extend_from_slice(&[0; 8]); // Target name buffer
        data.extend_from_slice(&0u32.to_le_bytes()); // Flags
        data.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]); // Challenge

        let challenge = NtlmChallenge::parse(&data);
        assert!(challenge.is_some());
        let c = challenge.unwrap();
        assert_eq!(c.challenge, [1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn test_sspi_token_type_detection() {
        let mut negotiate = Vec::new();
        negotiate.extend_from_slice(b"NTLMSSP\0");
        negotiate.extend_from_slice(&1u32.to_le_bytes());

        let token = SspiToken { length: negotiate.len() as u16, data: negotiate };
        assert!(token.is_ntlm_negotiate());
        assert!(!token.is_ntlm_challenge());
    }
}
