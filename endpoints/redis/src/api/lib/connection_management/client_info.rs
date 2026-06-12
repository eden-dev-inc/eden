use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, ClientInfoInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClientInfo,
    "Returns information about the connection",
    ReqType::Read,
    false,
);

/// See official Redis documentation for `CLIENT INFO`
/// https://redis.io/docs/latest/commands/client-info/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClientInfoInput {}

impl Serialize for ClientInfoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClientInfoInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ClientInfoInput, API_INFO);

impl RedisCommandInput for ClientInfoInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        crate::command::cmd(&API_INFO.api.to_string()).get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if !args.is_empty() {
            return Err(EpError::request(format!("CLIENT INFO requires no arguments, given {}", args.len())));
        }

        Ok(Self::default())
    }
}

/// Output for Redis CLIENT INFO command
///
/// Returns information about the current connection.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClientInfoOutput {
    /// Raw info string
    info: String,
    /// Parsed info as key-value pairs
    properties: HashMap<String, String>,
}

impl ClientInfoOutput {
    pub fn new(info: String) -> Self {
        let properties = Self::parse_info(&info);
        Self { info, properties }
    }

    /// Get the raw info string
    pub fn info(&self) -> &str {
        &self.info
    }

    /// Get the client ID
    pub fn id(&self) -> Option<i64> {
        self.properties.get("id").and_then(|v| v.parse().ok())
    }

    /// Get the client address
    pub fn addr(&self) -> Option<&str> {
        self.properties.get("addr").map(|s| s.as_str())
    }

    /// Get the client name
    pub fn name(&self) -> Option<&str> {
        self.properties.get("name").map(|s| s.as_str())
    }

    /// Get the connection age in seconds
    pub fn age(&self) -> Option<i64> {
        self.properties.get("age").and_then(|v| v.parse().ok())
    }

    /// Get a specific property by key
    pub fn get(&self, key: &str) -> Option<&str> {
        self.properties.get(key).map(|s| s.as_str())
    }

    /// Get all properties
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    fn parse_info(info: &str) -> HashMap<String, String> {
        info.split_whitespace()
            .filter_map(|pair| {
                let mut parts = pair.splitn(2, '=');
                match (parts.next(), parts.next()) {
                    (Some(k), Some(v)) => Some((k.to_string(), v.to_string())),
                    _ => None,
                }
            })
            .collect()
    }

    /// Decode the Redis protocol response into a ClientInfoOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let info = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLIENT INFO response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::VerbatimString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLIENT INFO response: {:?}", other)));
                }
            },
        };

        Ok(Self::new(info))
    }
}

impl Serialize for ClientInfoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClientInfoOutput", 2)?;
        state.serialize_field("info", &self.info)?;
        state.serialize_field("properties", &self.properties)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ClientInfoInput {};
            let cmd = input.command();
            assert!(cmd.windows(6).any(|w| w == b"CLIENT"));
            assert!(cmd.windows(4).any(|w| w == b"INFO"));
        }

        #[test]
        fn test_decode_bulk_string() {
            let response = b"$31\r\nid=1 addr=127.0.0.1:6379 age=10\r\n";
            let output = ClientInfoOutput::decode(response).unwrap();
            assert_eq!(output.id(), Some(1));
            assert_eq!(output.addr(), Some("127.0.0.1:6379"));
            assert_eq!(output.age(), Some(10));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ClientInfoOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_parse_info() {
            let output = ClientInfoOutput::new("id=123 addr=127.0.0.1:6379 name=myconn age=100".to_string());
            assert_eq!(output.id(), Some(123));
            assert_eq!(output.addr(), Some("127.0.0.1:6379"));
            assert_eq!(output.name(), Some("myconn"));
            assert_eq!(output.age(), Some(100));
        }

        #[test]
        fn test_get_property() {
            let output = ClientInfoOutput::new("id=1 custom=value".to_string());
            assert_eq!(output.get("custom"), Some("value"));
            assert_eq!(output.get("missing"), None);
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ClientInfoInput::decode(args).unwrap();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::ClientInfo);
        }

        #[test]
        fn test_decode_input_with_args_fails() {
            let args = vec![RedisJsonValue::String("extra".into())];
            let err = ClientInfoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires no arguments"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClientInfoInput {};
            assert!(input.keys().is_empty());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // CLIENT INFO was added in Redis 6.2

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_info_returns_data() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ClientInfoInput {}.command()).await.expect("raw failed");

                    let output = ClientInfoOutput::decode(&result).expect("decode failed");
                    assert!(!output.info().is_empty());
                    assert!(output.id().is_some());
                    assert!(output.addr().is_some());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_info_has_required_fields() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ClientInfoInput {}.command()).await.expect("raw failed");

                    let output = ClientInfoOutput::decode(&result).expect("decode failed");

                    // These fields should always be present
                    assert!(output.id().is_some(), "id should be present");
                    assert!(output.addr().is_some(), "addr should be present");
                    assert!(output.get("fd").is_some(), "fd should be present");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_info_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.0")).await;

            let result = ctx.raw(&ClientInfoInput {}.command()).await.expect("raw failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = ClientInfoOutput::decode(&result).expect("decode failed");
            assert!(output.id().is_some());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_info_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.0")).await;

            let result = ctx.raw(&ClientInfoInput {}.command()).await.expect("raw failed");

            let output = ClientInfoOutput::decode(&result).expect("decode failed");
            assert!(output.id().is_some());

            ctx.stop().await;
        }
    }
}
