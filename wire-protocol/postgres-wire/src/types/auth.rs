//! Authentication messages.
//!
//! PostgreSQL supports multiple authentication methods. The server sends
//! an Authentication message to indicate what authentication is required.

use crate::error::{auth, backend};
use crate::parse::{PgParse, PgParseError, PgParseSync};
use crate::pg_ext::{PgRead, PgReadSync};
use crate::write::MessageBuilder;
use wire_stream::{WireRead, WireReadSync};

/// Authentication request from the server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthenticationRequest {
    /// Authentication successful.
    Ok,
    /// Kerberos V5 authentication required (obsolete).
    KerberosV5,
    /// Cleartext password required.
    CleartextPassword,
    /// MD5 password required. Contains the 4-byte salt.
    MD5Password { salt: [u8; 4] },
    /// SCM credential required (obsolete).
    SCMCredential,
    /// GSS authentication required.
    GSS,
    /// GSS authentication continuation. Contains the authentication data.
    GSSContinue { data: Vec<u8> },
    /// SSPI authentication required.
    SSPI,
    /// SASL authentication required. Contains the list of mechanisms.
    SASL { mechanisms: Vec<String> },
    /// SASL authentication continuation. Contains the server challenge.
    SASLContinue { data: Vec<u8> },
    /// SASL authentication final. Contains the server signature.
    SASLFinal { data: Vec<u8> },
}

impl AuthenticationRequest {
    /// Check if this is a successful authentication.
    pub fn is_ok(&self) -> bool {
        matches!(self, AuthenticationRequest::Ok)
    }

    /// Encode the authentication message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(backend::AUTHENTICATION);

        match self {
            AuthenticationRequest::Ok => {
                builder.write_i32_be(auth::OK);
            }
            AuthenticationRequest::KerberosV5 => {
                builder.write_i32_be(auth::KERBEROS_V5);
            }
            AuthenticationRequest::CleartextPassword => {
                builder.write_i32_be(auth::CLEARTEXT_PASSWORD);
            }
            AuthenticationRequest::MD5Password { salt } => {
                builder.write_i32_be(auth::MD5_PASSWORD);
                builder.write_bytes(salt);
            }
            AuthenticationRequest::SCMCredential => {
                builder.write_i32_be(auth::SCM_CREDENTIAL);
            }
            AuthenticationRequest::GSS => {
                builder.write_i32_be(auth::GSS);
            }
            AuthenticationRequest::GSSContinue { data } => {
                builder.write_i32_be(auth::GSS_CONTINUE);
                builder.write_bytes(data);
            }
            AuthenticationRequest::SSPI => {
                builder.write_i32_be(auth::SSPI);
            }
            AuthenticationRequest::SASL { mechanisms } => {
                builder.write_i32_be(auth::SASL);
                for mechanism in mechanisms {
                    builder.write_cstring_str(mechanism);
                }
                builder.write_u8(0); // Final NUL
            }
            AuthenticationRequest::SASLContinue { data } => {
                builder.write_i32_be(auth::SASL_CONTINUE);
                builder.write_bytes(data);
            }
            AuthenticationRequest::SASLFinal { data } => {
                builder.write_i32_be(auth::SASL_FINAL);
                builder.write_bytes(data);
            }
        }

        builder.finish_owned()
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum AuthenticationError {
    #[error("unknown authentication type: {0}")]
    UnknownType(i32),
    #[error("invalid encoding")]
    InvalidEncoding,
    #[error("unexpected message type: expected 'R', got '{0}'")]
    UnexpectedMessageType(char),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for AuthenticationRequest {
    type ParseError = AuthenticationError;
    type Value<'s>
        = AuthenticationRequest
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // Read message header
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::AUTHENTICATION {
            return Err(PgParseError::Parse(AuthenticationError::UnexpectedMessageType(msg_type as char)));
        }

        let length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let payload_length = (length - 4) as usize;

        // Read authentication type
        let auth_type = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let remaining = payload_length - 4;

        let request = match auth_type {
            auth::OK => AuthenticationRequest::Ok,
            auth::KERBEROS_V5 => AuthenticationRequest::KerberosV5,
            auth::CLEARTEXT_PASSWORD => AuthenticationRequest::CleartextPassword,
            auth::MD5_PASSWORD => {
                let salt_bytes = stream.read_bytes_sync(4).map_err(PgParseError::Stream)?;
                let mut salt = [0u8; 4];
                salt.copy_from_slice(&salt_bytes);
                AuthenticationRequest::MD5Password { salt }
            }
            auth::SCM_CREDENTIAL => AuthenticationRequest::SCMCredential,
            auth::GSS => AuthenticationRequest::GSS,
            auth::GSS_CONTINUE => {
                let data = stream.read_bytes_sync(remaining).map_err(PgParseError::Stream)?;
                AuthenticationRequest::GSSContinue { data }
            }
            auth::SSPI => AuthenticationRequest::SSPI,
            auth::SASL => {
                let mut mechanisms = Vec::new();
                let mut bytes_read = 0;
                while bytes_read < remaining {
                    let mech_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                    bytes_read += mech_bytes.len() + 1;
                    if mech_bytes.is_empty() {
                        break;
                    }
                    let mech = String::from_utf8(mech_bytes).map_err(|_| PgParseError::Parse(AuthenticationError::InvalidEncoding))?;
                    mechanisms.push(mech);
                }
                AuthenticationRequest::SASL { mechanisms }
            }
            auth::SASL_CONTINUE => {
                let data = stream.read_bytes_sync(remaining).map_err(PgParseError::Stream)?;
                AuthenticationRequest::SASLContinue { data }
            }
            auth::SASL_FINAL => {
                let data = stream.read_bytes_sync(remaining).map_err(PgParseError::Stream)?;
                AuthenticationRequest::SASLFinal { data }
            }
            _ => return Err(PgParseError::Parse(AuthenticationError::UnknownType(auth_type))),
        };

        Ok(request)
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for AuthenticationRequest {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::AUTHENTICATION {
            return Err(PgParseError::Parse(AuthenticationError::UnexpectedMessageType(msg_type as char)));
        }

        let length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let payload_length = (length - 4) as usize;

        let auth_type = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let remaining = payload_length - 4;

        let request = match auth_type {
            auth::OK => AuthenticationRequest::Ok,
            auth::KERBEROS_V5 => AuthenticationRequest::KerberosV5,
            auth::CLEARTEXT_PASSWORD => AuthenticationRequest::CleartextPassword,
            auth::MD5_PASSWORD => {
                let salt_bytes = stream.read_bytes(4).await.map_err(PgParseError::Stream)?;
                let mut salt = [0u8; 4];
                salt.copy_from_slice(&salt_bytes);
                AuthenticationRequest::MD5Password { salt }
            }
            auth::SCM_CREDENTIAL => AuthenticationRequest::SCMCredential,
            auth::GSS => AuthenticationRequest::GSS,
            auth::GSS_CONTINUE => {
                let data = stream.read_bytes(remaining).await.map_err(PgParseError::Stream)?;
                AuthenticationRequest::GSSContinue { data }
            }
            auth::SSPI => AuthenticationRequest::SSPI,
            auth::SASL => {
                let mut mechanisms = Vec::new();
                let mut bytes_read = 0;
                while bytes_read < remaining {
                    let mech_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
                    bytes_read += mech_bytes.len() + 1;
                    if mech_bytes.is_empty() {
                        break;
                    }
                    let mech = String::from_utf8(mech_bytes).map_err(|_| PgParseError::Parse(AuthenticationError::InvalidEncoding))?;
                    mechanisms.push(mech);
                }
                AuthenticationRequest::SASL { mechanisms }
            }
            auth::SASL_CONTINUE => {
                let data = stream.read_bytes(remaining).await.map_err(PgParseError::Stream)?;
                AuthenticationRequest::SASLContinue { data }
            }
            auth::SASL_FINAL => {
                let data = stream.read_bytes(remaining).await.map_err(PgParseError::Stream)?;
                AuthenticationRequest::SASLFinal { data }
            }
            _ => return Err(PgParseError::Parse(AuthenticationError::UnknownType(auth_type))),
        };

        Ok(request)
    }
}

/// Authentication response from the client (password message).
///
/// Used for cleartext password, MD5 password, and SASL responses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Authentication {
    /// The password or authentication data.
    pub data: Vec<u8>,
}

impl Authentication {
    /// Create a new password message (cleartext).
    ///
    /// The password is NUL-terminated in the wire format.
    pub fn password(password: &str) -> Self {
        let mut data = password.as_bytes().to_vec();
        data.push(0); // NUL terminator required by protocol
        Self { data }
    }

    /// Create an MD5 password message.
    ///
    /// The MD5 password format is: "md5" + md5(md5(password + user) + salt)
    /// The result is NUL-terminated in the wire format.
    pub fn md5_password(user: &str, password: &str, salt: &[u8; 4]) -> Self {
        use md5::{Digest, Md5};

        // First hash: md5(password + user)
        let mut hasher = Md5::new();
        hasher.update(password.as_bytes());
        hasher.update(user.as_bytes());
        let first_hash = hasher.finalize();

        // Second hash: md5(first_hash_hex + salt)
        let first_hex = hex_encode(&first_hash);
        let mut hasher = Md5::new();
        hasher.update(first_hex.as_bytes());
        hasher.update(salt);
        let second_hash = hasher.finalize();

        // Result: "md5" + second_hash_hex + NUL
        let mut result = Vec::with_capacity(36);
        result.extend_from_slice(b"md5");
        result.extend_from_slice(hex_encode(&second_hash).as_bytes());
        result.push(0); // NUL terminator required by protocol

        Self { data: result }
    }

    /// Create a SASL initial response.
    ///
    /// Format: mechanism_name\0 + i32(data_len) + data
    /// No trailing NUL — SASL data is binary with explicit length.
    pub fn sasl_initial(mechanism: &str, data: &[u8]) -> Self {
        let mut buf = Vec::new();
        buf.extend_from_slice(mechanism.as_bytes());
        buf.push(0);
        buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
        buf.extend_from_slice(data);
        Self { data: buf }
    }

    /// Create a SASL response.
    ///
    /// Contains raw SASL data — no NUL terminator.
    pub fn sasl_response(data: &[u8]) -> Self {
        Self { data: data.to_vec() }
    }

    /// Encode the authentication message as a PasswordMessage ('p').
    ///
    /// The data must already include any required NUL terminators
    /// (password/MD5 include them, SASL does not).
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(crate::error::frontend::PASSWORD_MESSAGE).write_bytes(&self.data);
        builder.finish_owned()
    }
}

/// Convert bytes to lowercase hex string.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum PasswordMessageError {
    #[error("unexpected message type: expected 'p', got '{0}'")]
    UnexpectedMessageType(char),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for Authentication {
    type ParseError = PasswordMessageError;
    type Value<'s>
        = Authentication
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        use crate::error::frontend;

        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != frontend::PASSWORD_MESSAGE {
            return Err(PgParseError::Parse(PasswordMessageError::UnexpectedMessageType(msg_type as char)));
        }

        let length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let payload_length = length.saturating_sub(4) as usize;

        // Read the payload (includes the NUL terminator)
        let data = stream.read_bytes_sync(payload_length).map_err(PgParseError::Stream)?;

        // Strip trailing NUL if present
        let data = if data.last() == Some(&0) {
            data[..data.len() - 1].to_vec()
        } else {
            data
        };

        Ok(Authentication { data })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for Authentication {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        use crate::error::frontend;

        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != frontend::PASSWORD_MESSAGE {
            return Err(PgParseError::Parse(PasswordMessageError::UnexpectedMessageType(msg_type as char)));
        }

        let length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let payload_length = length.saturating_sub(4) as usize;

        // Read the payload (includes the NUL terminator)
        let data = stream.read_bytes(payload_length).await.map_err(PgParseError::Stream)?;

        // Strip trailing NUL if present
        let data = if data.last() == Some(&0) {
            data[..data.len() - 1].to_vec()
        } else {
            data
        };

        Ok(Authentication { data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_authentication_ok() {
        let auth = AuthenticationRequest::Ok;
        let encoded = auth.encode();

        let stream = SliceStream::new(&encoded);
        let decoded = AuthenticationRequest::parse_sync(&stream).expect("parse failed");

        assert!(decoded.is_ok());
    }

    #[test]
    fn test_authentication_md5() {
        let salt = [0x12, 0x34, 0x56, 0x78];
        let auth = AuthenticationRequest::MD5Password { salt };
        let encoded = auth.encode();

        let stream = SliceStream::new(&encoded);
        let decoded = AuthenticationRequest::parse_sync(&stream).expect("parse failed");

        match decoded {
            AuthenticationRequest::MD5Password { salt: decoded_salt } => {
                assert_eq!(decoded_salt, salt);
            }
            _ => panic!("wrong authentication type"),
        }
    }

    #[test]
    fn test_md5_password_generation() {
        let auth = Authentication::md5_password("postgres", "secret", &[0x12, 0x34, 0x56, 0x78]);
        assert!(auth.data.starts_with(b"md5"));
        assert_eq!(auth.data.len(), 36); // "md5" + 32 hex chars + NUL
        assert_eq!(auth.data[35], 0); // NUL terminator
    }

    #[test]
    fn test_password_message_roundtrip() {
        let auth = Authentication::password("mysecretpassword");
        let encoded = auth.encode();
        assert_eq!(encoded[0], b'p');

        let stream = SliceStream::new(&encoded);
        let decoded = Authentication::parse_sync(&stream).expect("parse failed");
        assert_eq!(decoded.data, b"mysecretpassword");
    }

    #[test]
    fn test_sasl_response_roundtrip() {
        let data = b"n,,n=user,r=clientnonce";
        let auth = Authentication::sasl_response(data);
        let encoded = auth.encode();

        let stream = SliceStream::new(&encoded);
        let decoded = Authentication::parse_sync(&stream).expect("parse failed");
        assert_eq!(decoded.data, data);
    }
}
