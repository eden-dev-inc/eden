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
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ClientCachingInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClientCaching,
    "Instructs the server whether to track the keys in the next request",
    ReqType::Read,
    false,
);

/// See official Redis documentation for `CLIENT CACHING`
/// https://redis.io/docs/latest/commands/client-caching/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClientCachingInput {
    cache: bool,
}

impl Serialize for ClientCachingInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClientCachingInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("cache", &self.cache)?;
        state.end()
    }
}

impl_redis_operation!(ClientCachingInput, API_INFO, { cache });

impl RedisCommandInput for ClientCachingInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(if self.cache { "YES" } else { "NO" });
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("CLIENT CACHING requires 1 argument, given {}", args.len())));
        }

        match &args[0] {
            RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                "YES" => Ok(Self { cache: true }),
                "NO" => Ok(Self { cache: false }),
                _ => Err(EpError::request(format!("Expected 'YES' or 'NO', given {s}"))),
            },
            _ => Err(EpError::request("Expected cache argument to be a string")),
        }
    }
}

/// Output for Redis CLIENT CACHING command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClientCachingOutput {
    result: String,
}

impl ClientCachingOutput {
    pub fn new(result: String) -> Self {
        Self { result }
    }

    pub fn result(&self) -> &str {
        &self.result
    }

    pub fn is_ok(&self) -> bool {
        self.result == "OK"
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for ClientCachingOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClientCachingOutput", 1)?;
        state.serialize_field("result", &self.result)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::decoder::{DecoderRespFrame, RedisCommandArgs};
    use redis_protocol::resp2::decode::decode as decode_resp2;
    use redis_protocol::resp3::decode::complete::decode as decode_resp3;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_yes() {
            let input = ClientCachingInput { cache: true };
            let cmd = input.command();
            assert!(cmd.windows(3).any(|w| w == b"YES"));
        }

        #[test]
        fn test_encode_command_no() {
            let input = ClientCachingInput { cache: false };
            let cmd = input.command();
            assert!(cmd.windows(2).any(|w| w == b"NO"));
        }

        #[test]
        fn test_decode_ok() {
            let output = ClientCachingOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_input_yes() {
            let args = vec![RedisJsonValue::String("YES".into())];
            let input = ClientCachingInput::decode(args).unwrap();
            assert!(input.cache);
        }

        #[test]
        fn test_decode_input_no() {
            let args = vec![RedisJsonValue::String("NO".into())];
            let input = ClientCachingInput::decode(args).unwrap();
            assert!(!input.cache);
        }

        #[test]
        fn test_decode_raw_resp2_yes() {
            let raw = b"*3\r\n$6\r\nCLIENT\r\n$7\r\nCACHING\r\n$3\r\nYES\r\n";
            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();
            assert_eq!(input.command(), &RedisApi::ClientCaching);
            let decoded = ClientCachingInput::decode(input.args().to_vec()).unwrap();
            assert!(decoded.cache);
        }

        #[test]
        fn test_decode_raw_resp3_no() {
            let raw = b"*3\r\n$6\r\nCLIENT\r\n$7\r\nCACHING\r\n$2\r\nNO\r\n";
            let (frame, _) = decode_resp3(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp3(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();
            assert_eq!(input.command(), &RedisApi::ClientCaching);
            let decoded = ClientCachingInput::decode(input.args().to_vec()).unwrap();
            assert!(!decoded.cache);
        }

        #[test]
        fn test_keys_empty() {
            assert!(ClientCachingInput::default().keys().is_empty());
        }
    }
}
