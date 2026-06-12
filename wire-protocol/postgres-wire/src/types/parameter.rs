//! Parameter and backend key data messages.

use crate::error::backend;
use crate::parse::{PgParse, PgParseError, PgParseSync};
use crate::pg_ext::{PgRead, PgReadSync};
use crate::write::MessageBuilder;
use wire_stream::{WireRead, WireReadSync};

/// ParameterStatus message from the server.
///
/// Reports the current value of a server parameter. Sent during startup
/// and whenever a parameter changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParameterStatus {
    /// The parameter name.
    pub name: String,
    /// The parameter value.
    pub value: String,
}

impl ParameterStatus {
    /// Create a new parameter status.
    pub fn new(name: String, value: String) -> Self {
        Self { name, value }
    }

    /// Encode the parameter status message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(backend::PARAMETER_STATUS).write_cstring_str(&self.name).write_cstring_str(&self.value);
        builder.finish_owned()
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum ParameterStatusError {
    #[error("invalid encoding")]
    InvalidEncoding,
    #[error("unexpected message type: expected 'S', got '{0}'")]
    UnexpectedMessageType(char),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for ParameterStatus {
    type ParseError = ParameterStatusError;
    type Value<'s>
        = ParameterStatus
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::PARAMETER_STATUS {
            return Err(PgParseError::Parse(ParameterStatusError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let name_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
        let name = String::from_utf8(name_bytes).map_err(|_| PgParseError::Parse(ParameterStatusError::InvalidEncoding))?;

        let value_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
        let value = String::from_utf8(value_bytes).map_err(|_| PgParseError::Parse(ParameterStatusError::InvalidEncoding))?;

        Ok(ParameterStatus { name, value })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for ParameterStatus {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::PARAMETER_STATUS {
            return Err(PgParseError::Parse(ParameterStatusError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let name_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
        let name = String::from_utf8(name_bytes).map_err(|_| PgParseError::Parse(ParameterStatusError::InvalidEncoding))?;

        let value_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
        let value = String::from_utf8(value_bytes).map_err(|_| PgParseError::Parse(ParameterStatusError::InvalidEncoding))?;

        Ok(ParameterStatus { name, value })
    }
}

/// BackendKeyData message from the server.
///
/// Contains the process ID and secret key needed to cancel queries.
/// Sent during startup after authentication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackendKeyData {
    /// The backend process ID.
    pub process_id: i32,
    /// The secret key for cancellation.
    pub secret_key: i32,
}

impl BackendKeyData {
    /// Create a new backend key data.
    pub fn new(process_id: i32, secret_key: i32) -> Self {
        Self { process_id, secret_key }
    }

    /// Encode the backend key data message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(backend::BACKEND_KEY_DATA).write_i32_be(self.process_id).write_i32_be(self.secret_key);
        builder.finish_owned()
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum BackendKeyDataError {
    #[error("unexpected message type: expected 'K', got '{0}'")]
    UnexpectedMessageType(char),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for BackendKeyData {
    type ParseError = BackendKeyDataError;
    type Value<'s>
        = BackendKeyData
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::BACKEND_KEY_DATA {
            return Err(PgParseError::Parse(BackendKeyDataError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let process_id = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let secret_key = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        Ok(BackendKeyData { process_id, secret_key })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for BackendKeyData {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::BACKEND_KEY_DATA {
            return Err(PgParseError::Parse(BackendKeyDataError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let process_id = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let secret_key = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        Ok(BackendKeyData { process_id, secret_key })
    }
}

/// BackendKeyData message with variable-length cancel key (Protocol 3.2, PostgreSQL 18+).
///
/// Contains the process ID and a variable-length secret key needed to cancel queries.
/// This is the Protocol 3.2 version that provides improved security over the fixed
/// 4-byte secret key used in Protocol 3.0.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendKeyDataV2 {
    /// The backend process ID.
    pub process_id: i32,
    /// The variable-length secret key for cancellation.
    /// In Protocol 3.2, this is typically 32 bytes for improved security.
    pub secret_key: Vec<u8>,
}

impl BackendKeyDataV2 {
    /// Create a new backend key data with variable-length key.
    pub fn new(process_id: i32, secret_key: Vec<u8>) -> Self {
        Self { process_id, secret_key }
    }

    /// Encode the backend key data message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder
            .begin(backend::BACKEND_KEY_DATA)
            .write_i32_be(self.process_id)
            .write_i32_be(self.secret_key.len() as i32)
            .write_bytes(&self.secret_key);
        builder.finish_owned()
    }

    /// Convert to Protocol 3.0 format if the key is exactly 4 bytes.
    /// Returns None if the key length doesn't match.
    pub fn to_v1(&self) -> Option<BackendKeyData> {
        if self.secret_key.len() == 4 {
            let key = i32::from_be_bytes([self.secret_key[0], self.secret_key[1], self.secret_key[2], self.secret_key[3]]);
            Some(BackendKeyData::new(self.process_id, key))
        } else {
            None
        }
    }
}

impl BackendKeyData {
    /// Convert to Protocol 3.2 format with variable-length key.
    pub fn to_v2(&self) -> BackendKeyDataV2 {
        BackendKeyDataV2 {
            process_id: self.process_id,
            secret_key: self.secret_key.to_be_bytes().to_vec(),
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum BackendKeyDataV2Error {
    #[error("unexpected message type: expected 'K', got '{0}'")]
    UnexpectedMessageType(char),
    #[error("invalid key length: {0}")]
    InvalidKeyLength(i32),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for BackendKeyDataV2 {
    type ParseError = BackendKeyDataV2Error;
    type Value<'s>
        = BackendKeyDataV2
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::BACKEND_KEY_DATA {
            return Err(PgParseError::Parse(BackendKeyDataV2Error::UnexpectedMessageType(msg_type as char)));
        }

        let length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let process_id = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        // Calculate key length from message length
        // length includes itself (4) + process_id (4) + key_length (4) + key_data
        // For Protocol 3.0: length = 12, key is 4 bytes (stored as i32)
        // For Protocol 3.2: length = 12 + key_length, key is variable
        let key_length = length.saturating_sub(8); // length - (length_field + process_id)

        if key_length == 4 {
            // Protocol 3.0 format: 4-byte secret key as i32
            let secret_key = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
            Ok(BackendKeyDataV2 { process_id, secret_key: secret_key.to_be_bytes().to_vec() })
        } else if key_length > 4 {
            // Protocol 3.2 format: length-prefixed variable key
            let key_len = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
            if !(0..=256).contains(&key_len) {
                return Err(PgParseError::Parse(BackendKeyDataV2Error::InvalidKeyLength(key_len)));
            }
            let secret_key = stream.read_bytes_sync(key_len as usize).map_err(PgParseError::Stream)?;
            Ok(BackendKeyDataV2 { process_id, secret_key })
        } else {
            Err(PgParseError::Parse(BackendKeyDataV2Error::InvalidKeyLength(key_length)))
        }
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for BackendKeyDataV2 {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::BACKEND_KEY_DATA {
            return Err(PgParseError::Parse(BackendKeyDataV2Error::UnexpectedMessageType(msg_type as char)));
        }

        let length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let process_id = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let key_length = length.saturating_sub(8);

        if key_length == 4 {
            // Protocol 3.0 format
            let secret_key = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
            Ok(BackendKeyDataV2 { process_id, secret_key: secret_key.to_be_bytes().to_vec() })
        } else if key_length > 4 {
            // Protocol 3.2 format
            let key_len = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
            if !(0..=256).contains(&key_len) {
                return Err(PgParseError::Parse(BackendKeyDataV2Error::InvalidKeyLength(key_len)));
            }
            let secret_key = stream.read_bytes(key_len as usize).await.map_err(PgParseError::Stream)?;
            Ok(BackendKeyDataV2 { process_id, secret_key })
        } else {
            Err(PgParseError::Parse(BackendKeyDataV2Error::InvalidKeyLength(key_length)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_parameter_status() {
        let status = ParameterStatus::new("server_version".to_string(), "14.0".to_string());
        let encoded = status.encode();

        let stream = SliceStream::new(&encoded);
        let decoded = ParameterStatus::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.name, "server_version");
        assert_eq!(decoded.value, "14.0");
    }

    #[test]
    fn test_backend_key_data() {
        let key_data = BackendKeyData::new(12345, 67890);
        let encoded = key_data.encode();

        let stream = SliceStream::new(&encoded);
        let decoded = BackendKeyData::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.process_id, 12345);
        assert_eq!(decoded.secret_key, 67890);
    }

    #[test]
    fn test_backend_key_data_v1_to_v2() {
        let v1 = BackendKeyData::new(12345, 67890);
        let v2 = v1.to_v2();

        assert_eq!(v2.process_id, 12345);
        assert_eq!(v2.secret_key, 67890_i32.to_be_bytes().to_vec());

        // Convert back
        let v1_again = v2.to_v1().expect("should convert back");
        assert_eq!(v1_again.process_id, 12345);
        assert_eq!(v1_again.secret_key, 67890);
    }

    #[test]
    fn test_backend_key_data_v2_variable_length() {
        let secret = vec![
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
        ];
        let v2 = BackendKeyDataV2::new(12345, secret.clone());

        assert_eq!(v2.process_id, 12345);
        assert_eq!(v2.secret_key, secret);

        // Cannot convert to v1 because key is not 4 bytes
        assert!(v2.to_v1().is_none());
    }
}
