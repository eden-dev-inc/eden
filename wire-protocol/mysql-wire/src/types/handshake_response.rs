//! MySQL client handshake response packet.
//!
//! Sent by the client in response to the server's initial handshake.

use crate::capabilities::CapabilityFlags;
use crate::write::{write_cstring, write_lenenc_string, write_u24_le, write_u32_le};
use std::io::{self, Write};

/// Client handshake response packet.
///
/// Sent by the client after receiving the server's initial handshake.
#[derive(Clone, Debug)]
pub struct HandshakeResponse {
    /// Client capability flags (negotiated with server).
    pub capabilities: CapabilityFlags,
    /// Maximum packet size client will accept.
    pub max_packet_size: u32,
    /// Character set.
    pub character_set: u8,
    /// Username.
    pub username: String,
    /// Authentication response data.
    pub auth_response: Vec<u8>,
    /// Database name (if CONNECT_WITH_DB capability).
    pub database: Option<String>,
    /// Authentication plugin name (if PLUGIN_AUTH capability).
    pub auth_plugin_name: Option<String>,
    /// Connection attributes (if CONNECT_ATTRS capability).
    pub connect_attrs: Option<Vec<(String, String)>>,
}

impl HandshakeResponse {
    /// Create a new handshake response.
    pub fn new(capabilities: CapabilityFlags, username: impl Into<String>, auth_response: Vec<u8>) -> Self {
        Self {
            capabilities,
            max_packet_size: 0x01000000, // 16MB
            character_set: 33,           // utf8mb4
            username: username.into(),
            auth_response,
            database: None,
            auth_plugin_name: None,
            connect_attrs: None,
        }
    }

    /// Set the database to use.
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self.capabilities |= CapabilityFlags::CONNECT_WITH_DB;
        self
    }

    /// Set the auth plugin name.
    pub fn with_auth_plugin(mut self, plugin: impl Into<String>) -> Self {
        self.auth_plugin_name = Some(plugin.into());
        self.capabilities |= CapabilityFlags::PLUGIN_AUTH;
        self
    }

    /// Set connection attributes.
    pub fn with_connect_attrs(mut self, attrs: Vec<(String, String)>) -> Self {
        self.connect_attrs = Some(attrs);
        self.capabilities |= CapabilityFlags::CONNECT_ATTRS;
        self
    }

    /// Encode the handshake response to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);
        self.write_to(&mut buf).expect("Vec write should not fail");
        buf
    }

    /// Write the handshake response to a writer.
    pub fn write_to(&self, w: &mut impl Write) -> io::Result<()> {
        // Capability flags (4 bytes)
        write_u32_le(w, self.capabilities.bits())?;

        // Max packet size (4 bytes)
        write_u32_le(w, self.max_packet_size)?;

        // Character set (1 byte)
        w.write_all(&[self.character_set])?;

        // Reserved (23 bytes of 0x00)
        w.write_all(&[0u8; 23])?;

        // Username (NUL-terminated)
        write_cstring(w, self.username.as_bytes())?;

        // Auth response
        if self.capabilities.contains(CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA) {
            // Length-encoded string
            write_lenenc_string(w, &self.auth_response)?;
        } else if self.capabilities.contains(CapabilityFlags::SECURE_CONNECTION) {
            // Length-prefixed (1 byte length)
            w.write_all(&[self.auth_response.len() as u8])?;
            w.write_all(&self.auth_response)?;
        } else {
            // NUL-terminated
            write_cstring(w, &self.auth_response)?;
        }

        // Database (if CONNECT_WITH_DB)
        if self.capabilities.contains(CapabilityFlags::CONNECT_WITH_DB) {
            if let Some(ref db) = self.database {
                write_cstring(w, db.as_bytes())?;
            } else {
                w.write_all(&[0])?; // Empty NUL-terminated string
            }
        }

        // Auth plugin name (if PLUGIN_AUTH)
        if self.capabilities.contains(CapabilityFlags::PLUGIN_AUTH) {
            if let Some(ref plugin) = self.auth_plugin_name {
                write_cstring(w, plugin.as_bytes())?;
            } else {
                write_cstring(w, b"mysql_native_password")?;
            }
        }

        // Connection attributes (if CONNECT_ATTRS)
        if self.capabilities.contains(CapabilityFlags::CONNECT_ATTRS) {
            if let Some(ref attrs) = self.connect_attrs {
                // Calculate total length of attributes
                let mut attr_buf = Vec::new();
                for (key, value) in attrs {
                    write_lenenc_string(&mut attr_buf, key.as_bytes())?;
                    write_lenenc_string(&mut attr_buf, value.as_bytes())?;
                }
                write_lenenc_string(w, &attr_buf)?;
            } else {
                // Zero-length attributes
                w.write_all(&[0])?;
            }
        }

        Ok(())
    }

    /// Build a complete handshake response packet with header.
    pub fn build_packet(&self, sequence_id: u8) -> Vec<u8> {
        let payload = self.encode();
        let mut packet = Vec::with_capacity(4 + payload.len());

        // Header
        write_u24_le(&mut packet, payload.len() as u32).expect("Vec write");
        packet.push(sequence_id);

        // Payload
        packet.extend_from_slice(&payload);

        packet
    }
}

/// SSL request packet (sent before full handshake response when using SSL).
#[derive(Clone, Debug)]
pub struct SslRequest {
    /// Client capability flags.
    pub capabilities: CapabilityFlags,
    /// Maximum packet size.
    pub max_packet_size: u32,
    /// Character set.
    pub character_set: u8,
}

impl SslRequest {
    /// Create a new SSL request.
    pub fn new(capabilities: CapabilityFlags) -> Self {
        Self {
            capabilities: capabilities | CapabilityFlags::SSL,
            max_packet_size: 0x01000000,
            character_set: 33,
        }
    }

    /// Encode the SSL request to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);

        // Capability flags (4 bytes)
        buf.extend_from_slice(&self.capabilities.bits().to_le_bytes());

        // Max packet size (4 bytes)
        buf.extend_from_slice(&self.max_packet_size.to_le_bytes());

        // Character set (1 byte)
        buf.push(self.character_set);

        // Reserved (23 bytes)
        buf.extend_from_slice(&[0u8; 23]);

        buf
    }

    /// Build a complete SSL request packet with header.
    pub fn build_packet(&self, sequence_id: u8) -> Vec<u8> {
        let payload = self.encode();
        let mut packet = Vec::with_capacity(4 + payload.len());

        write_u24_le(&mut packet, payload.len() as u32).expect("Vec write");
        packet.push(sequence_id);
        packet.extend_from_slice(&payload);

        packet
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handshake_response_basic() {
        let response = HandshakeResponse::new(CapabilityFlags::client_default_5x(), "root", vec![1, 2, 3, 4]);

        let encoded = response.encode();

        // Should have capability flags at the start
        let caps = u32::from_le_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert_eq!(caps, CapabilityFlags::client_default_5x().bits());
    }

    #[test]
    fn test_handshake_response_with_database() {
        let response = HandshakeResponse::new(CapabilityFlags::client_default_5x(), "root", vec![1, 2, 3, 4]).with_database("mydb");

        assert!(response.capabilities.contains(CapabilityFlags::CONNECT_WITH_DB));
        assert_eq!(response.database, Some("mydb".to_string()));
    }

    #[test]
    fn test_ssl_request() {
        let request = SslRequest::new(CapabilityFlags::client_default_5x());

        let encoded = request.encode();
        assert_eq!(encoded.len(), 32); // 4 + 4 + 1 + 23

        // SSL flag should be set
        let caps = u32::from_le_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert!(CapabilityFlags::from_bits_truncate(caps).contains(CapabilityFlags::SSL));
    }

    #[test]
    fn test_build_packet() {
        let response = HandshakeResponse::new(CapabilityFlags::minimal(), "user", vec![]);

        let packet = response.build_packet(1);

        // Check sequence ID
        assert_eq!(packet[3], 1);
    }
}
