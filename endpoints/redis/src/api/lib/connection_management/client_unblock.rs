use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{UnblockType, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, ClientUnblockInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClientUnblock,
    "Unblocks a client blocked by a blocking command from a different connection",
    ReqType::Write,
    false,
);

/// See official Redis documentation for `CLIENT UNBLOCK`
/// https://redis.io/docs/latest/commands/client-unblock/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClientUnblockInput {
    client_id: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    unblock_type: Option<UnblockType>,
}

impl Serialize for ClientUnblockInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let fields = if self.unblock_type.is_some() { 3 } else { 2 };
        let mut state = serializer.serialize_struct("ClientUnblockInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("client_id", &self.client_id)?;
        if let Some(t) = &self.unblock_type {
            state.serialize_field("unblock_type", t)?;
        }
        state.end()
    }
}

impl_redis_operation!(ClientUnblockInput, API_INFO, { client_id, unblock_type });

impl RedisCommandInput for ClientUnblockInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.client_id);
        if let Some(t) = &self.unblock_type {
            command.arg(match t {
                UnblockType::TIMEOUT => "TIMEOUT",
                UnblockType::ERROR => "ERROR",
            });
        }
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("CLIENT UNBLOCK requires at least 1 argument"));
        }

        let client_id = args[0].clone();
        let unblock_type = args.get(1).and_then(|v| {
            if let RedisJsonValue::String(s) = v {
                match s.to_uppercase().as_str() {
                    "TIMEOUT" => Some(UnblockType::TIMEOUT),
                    "ERROR" => Some(UnblockType::ERROR),
                    _ => None,
                }
            } else {
                None
            }
        });

        Ok(Self { client_id, unblock_type })
    }
}

/// Output for Redis CLIENT UNBLOCK command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClientUnblockOutput {
    /// 1 if client was unblocked, 0 if not found or not blocked
    result: i64,
}

impl ClientUnblockOutput {
    pub fn result(&self) -> i64 {
        self.result
    }

    pub fn was_unblocked(&self) -> bool {
        self.result == 1
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Integer(n)) => n,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => data,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
        };

        Ok(Self { result })
    }
}

impl Serialize for ClientUnblockOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClientUnblockOutput", 1)?;
        state.serialize_field("result", &self.result)?;
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
            let input = ClientUnblockInput { client_id: RedisJsonValue::Integer(123), unblock_type: None };
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"UNBLOCK"));
        }

        #[test]
        fn test_decode_unblocked() {
            let output = ClientUnblockOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_unblocked());
        }

        #[test]
        fn test_decode_not_found() {
            let output = ClientUnblockOutput::decode(b":0\r\n").unwrap();
            assert!(!output.was_unblocked());
        }

        #[test]
        fn test_keys_empty() {
            let input = ClientUnblockInput { client_id: RedisJsonValue::Integer(1), unblock_type: None };
            assert!(input.keys().is_empty());
        }
    }
}
