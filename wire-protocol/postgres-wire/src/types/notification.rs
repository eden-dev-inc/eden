//! Notification and simple response messages.

use crate::error::{backend, frontend};
use crate::parse::{PgParse, PgParseError, PgParseSync};
use crate::pg_ext::{PgRead, PgReadSync};
use crate::write::MessageBuilder;
use wire_stream::{WireRead, WireReadSync};

/// Terminate message (frontend).
///
/// Sent by the client to gracefully close the connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Terminate;

impl Terminate {
    /// Encode the Terminate message.
    pub fn encode() -> [u8; 5] {
        let mut buf = [0u8; 5];
        buf[0] = frontend::TERMINATE;
        buf[1..5].copy_from_slice(&4i32.to_be_bytes()); // Length includes itself
        buf
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum TerminateError {
    #[error("unexpected message type: expected 'X', got '{0}'")]
    UnexpectedMessageType(char),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for Terminate {
    type ParseError = TerminateError;
    type Value<'s>
        = Terminate
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != frontend::TERMINATE {
            return Err(PgParseError::Parse(TerminateError::UnexpectedMessageType(msg_type as char)));
        }
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        Ok(Terminate)
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for Terminate {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != frontend::TERMINATE {
            return Err(PgParseError::Parse(TerminateError::UnexpectedMessageType(msg_type as char)));
        }
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        Ok(Terminate)
    }
}

/// EmptyQueryResponse message (backend).
///
/// Sent when an empty query string is executed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmptyQueryResponse;

impl EmptyQueryResponse {
    /// Encode the EmptyQueryResponse message.
    pub fn encode() -> [u8; 5] {
        let mut buf = [0u8; 5];
        buf[0] = backend::EMPTY_QUERY_RESPONSE;
        buf[1..5].copy_from_slice(&4i32.to_be_bytes());
        buf
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum EmptyQueryResponseError {
    #[error("unexpected message type: expected 'I', got '{0}'")]
    UnexpectedMessageType(char),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for EmptyQueryResponse {
    type ParseError = EmptyQueryResponseError;
    type Value<'s>
        = EmptyQueryResponse
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::EMPTY_QUERY_RESPONSE {
            return Err(PgParseError::Parse(EmptyQueryResponseError::UnexpectedMessageType(msg_type as char)));
        }
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        Ok(EmptyQueryResponse)
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for EmptyQueryResponse {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::EMPTY_QUERY_RESPONSE {
            return Err(PgParseError::Parse(EmptyQueryResponseError::UnexpectedMessageType(msg_type as char)));
        }
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        Ok(EmptyQueryResponse)
    }
}

/// NotificationResponse message (backend).
///
/// Sent when a NOTIFY is triggered on a channel the client is listening to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotificationResponse {
    /// The process ID of the notifying backend.
    pub process_id: i32,
    /// The channel name.
    pub channel: String,
    /// The notification payload (may be empty).
    pub payload: String,
}

impl NotificationResponse {
    /// Create a new notification response.
    pub fn new(process_id: i32, channel: String, payload: String) -> Self {
        Self { process_id, channel, payload }
    }

    /// Encode the NotificationResponse message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder
            .begin(backend::NOTIFICATION_RESPONSE)
            .write_i32_be(self.process_id)
            .write_cstring_str(&self.channel)
            .write_cstring_str(&self.payload);
        builder.finish_owned()
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum NotificationResponseError {
    #[error("unexpected message type: expected 'A', got '{0}'")]
    UnexpectedMessageType(char),
    #[error("invalid encoding")]
    InvalidEncoding,
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for NotificationResponse {
    type ParseError = NotificationResponseError;
    type Value<'s>
        = NotificationResponse
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::NOTIFICATION_RESPONSE {
            return Err(PgParseError::Parse(NotificationResponseError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let process_id = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let channel_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
        let channel = String::from_utf8(channel_bytes).map_err(|_| PgParseError::Parse(NotificationResponseError::InvalidEncoding))?;

        let payload_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
        let payload = String::from_utf8(payload_bytes).map_err(|_| PgParseError::Parse(NotificationResponseError::InvalidEncoding))?;

        Ok(NotificationResponse { process_id, channel, payload })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for NotificationResponse {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::NOTIFICATION_RESPONSE {
            return Err(PgParseError::Parse(NotificationResponseError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let process_id = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let channel_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
        let channel = String::from_utf8(channel_bytes).map_err(|_| PgParseError::Parse(NotificationResponseError::InvalidEncoding))?;

        let payload_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
        let payload = String::from_utf8(payload_bytes).map_err(|_| PgParseError::Parse(NotificationResponseError::InvalidEncoding))?;

        Ok(NotificationResponse { process_id, channel, payload })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_terminate() {
        let encoded = Terminate::encode();
        assert_eq!(encoded[0], b'X');
        assert_eq!(encoded.len(), 5);

        let stream = SliceStream::new(&encoded);
        let _decoded = Terminate::parse_sync(&stream).expect("parse failed");
    }

    #[test]
    fn test_empty_query_response() {
        let encoded = EmptyQueryResponse::encode();
        assert_eq!(encoded[0], b'I');
        assert_eq!(encoded.len(), 5);

        let stream = SliceStream::new(&encoded);
        let _decoded = EmptyQueryResponse::parse_sync(&stream).expect("parse failed");
    }

    #[test]
    fn test_notification_response() {
        let notification = NotificationResponse::new(12345, "my_channel".to_string(), "hello world".to_string());
        let encoded = notification.encode();
        assert_eq!(encoded[0], b'A');

        let stream = SliceStream::new(&encoded);
        let decoded = NotificationResponse::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.process_id, 12345);
        assert_eq!(decoded.channel, "my_channel");
        assert_eq!(decoded.payload, "hello world");
    }

    #[test]
    fn test_notification_empty_payload() {
        let notification = NotificationResponse::new(1, "chan".to_string(), String::new());
        let encoded = notification.encode();

        let stream = SliceStream::new(&encoded);
        let decoded = NotificationResponse::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.payload, "");
    }
}
