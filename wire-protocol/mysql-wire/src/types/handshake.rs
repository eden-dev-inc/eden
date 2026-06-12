//! MySQL server initial handshake packet (Protocol v10).
//!
//! This packet is sent by the server upon connection.

use crate::capabilities::CapabilityFlags;
use crate::mysql_ext::MysqlReadSync;
use crate::parse::{MysqlParse, MysqlParseError, MysqlParseSync};
use wire_stream::{WireRead, WireReadSync};

/// Server initial handshake packet (Protocol v10).
///
/// Sent by the server immediately after a client connects.
#[derive(Clone, Debug)]
pub struct HandshakeV10 {
    /// Protocol version (always 10 for modern MySQL).
    pub protocol_version: u8,
    /// Server version string (e.g., "8.0.32-MySQL Community Server").
    pub server_version: String,
    /// Connection ID assigned by the server.
    pub connection_id: u32,
    /// First 8 bytes of auth plugin data (auth-plugin-data-part-1).
    pub auth_plugin_data_part_1: [u8; 8],
    /// Server capability flags.
    pub capabilities: CapabilityFlags,
    /// Default character set.
    pub character_set: u8,
    /// Server status flags.
    pub status_flags: u16,
    /// Remaining auth plugin data (auth-plugin-data-part-2).
    pub auth_plugin_data_part_2: Vec<u8>,
    /// Auth plugin name (if PLUGIN_AUTH capability).
    pub auth_plugin_name: Option<String>,
}

impl HandshakeV10 {
    /// Get the full auth plugin data (part 1 + part 2).
    pub fn auth_plugin_data(&self) -> Vec<u8> {
        let mut data = self.auth_plugin_data_part_1.to_vec();
        data.extend(&self.auth_plugin_data_part_2);
        data
    }

    /// Get the auth plugin data as a 20-byte array (for mysql_native_password).
    ///
    /// Returns None if auth data is shorter than 20 bytes.
    pub fn auth_plugin_data_20(&self) -> Option<[u8; 20]> {
        let data = self.auth_plugin_data();
        if data.len() >= 20 {
            let mut arr = [0u8; 20];
            arr.copy_from_slice(&data[..20]);
            Some(arr)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum HandshakeError {
    #[error("unsupported protocol version: {0} (expected 10)")]
    UnsupportedVersion(u8),
    #[error("invalid server version string")]
    InvalidServerVersion,
    #[error("incomplete auth data")]
    IncompleteAuthData,
    #[error("invalid auth plugin name")]
    InvalidAuthPluginName,
}

impl<S: WireReadSync + ?Sized> MysqlParseSync<S> for HandshakeV10 {
    type ParseError = HandshakeError;
    type Value<'s>
        = HandshakeV10
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // Protocol version (1 byte)
        let protocol_version = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
        if protocol_version != 10 {
            return Err(MysqlParseError::Parse(HandshakeError::UnsupportedVersion(protocol_version)));
        }

        // Server version (NUL-terminated string)
        let version_bytes = stream.read_cstring_sync().map_err(MysqlParseError::Stream)?;
        let server_version = String::from_utf8_lossy(&version_bytes).into_owned();

        // Connection ID (4 bytes LE)
        let connection_id = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;

        // Auth plugin data part 1 (8 bytes)
        let mut auth_plugin_data_part_1 = [0u8; 8];
        for byte in &mut auth_plugin_data_part_1 {
            *byte = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
        }

        // Filler (1 byte, always 0x00)
        let _ = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;

        // Capability flags (lower 2 bytes)
        let cap_lower = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;

        // Character set (1 byte)
        let character_set = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;

        // Status flags (2 bytes)
        let status_flags = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;

        // Capability flags (upper 2 bytes)
        let cap_upper = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;
        let capabilities = CapabilityFlags::from_bits_truncate((cap_upper as u32) << 16 | cap_lower as u32);

        // Auth plugin data length (1 byte) or 0x00
        let auth_data_len = if capabilities.contains(CapabilityFlags::PLUGIN_AUTH) {
            stream.read_u8_sync().map_err(MysqlParseError::Stream)?
        } else {
            stream.read_u8_sync().map_err(MysqlParseError::Stream)?; // consume but ignore
            0
        };

        // Reserved (10 bytes of 0x00)
        for _ in 0..10 {
            let _ = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
        }

        // Auth plugin data part 2 (if SECURE_CONNECTION)
        let auth_plugin_data_part_2 = if capabilities.contains(CapabilityFlags::SECURE_CONNECTION) {
            // Length is max(13, auth_data_len - 8)
            let part_2_len = if auth_data_len > 8 { (auth_data_len - 8) as usize } else { 13 };

            let mut data = Vec::with_capacity(part_2_len);
            for _ in 0..part_2_len {
                let byte = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
                // Stop at NUL terminator
                if byte == 0 {
                    break;
                }
                data.push(byte);
            }
            data
        } else {
            Vec::new()
        };

        // Auth plugin name (if PLUGIN_AUTH)
        let auth_plugin_name = if capabilities.contains(CapabilityFlags::PLUGIN_AUTH) {
            let name_bytes = stream.read_cstring_sync().map_err(MysqlParseError::Stream)?;
            Some(String::from_utf8_lossy(&name_bytes).into_owned())
        } else {
            None
        };

        Ok(HandshakeV10 {
            protocol_version,
            server_version,
            connection_id,
            auth_plugin_data_part_1,
            capabilities,
            character_set,
            status_flags,
            auth_plugin_data_part_2,
            auth_plugin_name,
        })
    }
}

impl<S: WireRead + ?Sized> MysqlParse<S> for HandshakeV10 {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // For complete buffers, use sync version
        Self::parse_sync(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    fn make_handshake_packet() -> Vec<u8> {
        let mut packet = Vec::new();

        // Protocol version
        packet.push(10);

        // Server version (NUL-terminated)
        packet.extend_from_slice(b"8.0.32\0");

        // Connection ID
        packet.extend_from_slice(&42u32.to_le_bytes());

        // Auth plugin data part 1 (8 bytes)
        packet.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);

        // Filler
        packet.push(0);

        // Capability flags lower (PROTOCOL_41 | SECURE_CONNECTION | PLUGIN_AUTH)
        let caps_lower: u16 =
            (CapabilityFlags::PROTOCOL_41 | CapabilityFlags::SECURE_CONNECTION | CapabilityFlags::PLUGIN_AUTH).bits() as u16;
        packet.extend_from_slice(&caps_lower.to_le_bytes());

        // Character set
        packet.push(33); // utf8mb4

        // Status flags
        packet.extend_from_slice(&0u16.to_le_bytes());

        // Capability flags upper
        let caps_upper: u16 =
            ((CapabilityFlags::PROTOCOL_41 | CapabilityFlags::SECURE_CONNECTION | CapabilityFlags::PLUGIN_AUTH).bits() >> 16) as u16;
        packet.extend_from_slice(&caps_upper.to_le_bytes());

        // Auth plugin data length
        packet.push(21); // 8 + 13

        // Reserved (10 bytes)
        packet.extend_from_slice(&[0u8; 10]);

        // Auth plugin data part 2 (12 bytes + NUL)
        packet.extend_from_slice(&[9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 0]);

        // Auth plugin name
        packet.extend_from_slice(b"mysql_native_password\0");

        packet
    }

    #[test]
    fn test_handshake_parse() {
        let data = make_handshake_packet();
        let stream = SliceStream::new(&data);

        let handshake = HandshakeV10::parse_sync(&stream).unwrap();

        assert_eq!(handshake.protocol_version, 10);
        assert_eq!(handshake.server_version, "8.0.32");
        assert_eq!(handshake.connection_id, 42);
        assert!(handshake.capabilities.supports_41());
        assert_eq!(handshake.auth_plugin_name, Some("mysql_native_password".to_string()));
    }

    #[test]
    fn test_auth_plugin_data() {
        let data = make_handshake_packet();
        let stream = SliceStream::new(&data);

        let handshake = HandshakeV10::parse_sync(&stream).unwrap();
        let auth_data = handshake.auth_plugin_data();

        // Should have 8 + 12 = 20 bytes
        assert_eq!(auth_data.len(), 20);
        assert_eq!(&auth_data[..8], &[1, 2, 3, 4, 5, 6, 7, 8]);
    }
}
