use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{Pause, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, ClientPauseInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::ClientPause, "Suspends commands processing", ReqType::Write, false);

/// See official Redis documentation for `CLIENT PAUSE`
/// https://redis.io/docs/latest/commands/client-pause/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClientPauseInput {
    timeout: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pause: Option<Pause>,
}

impl Serialize for ClientPauseInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.pause.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ClientPauseInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("timeout", &self.timeout)?;
        if let Some(pause) = &self.pause {
            state.serialize_field("pause", pause)?;
        }
        state.end()
    }
}

impl_redis_operation!(ClientPauseInput, API_INFO, { timeout, pause });

impl RedisCommandInput for ClientPauseInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.timeout);
        if let Some(pause) = &self.pause {
            match pause {
                Pause::WRITE => command.arg("WRITE"),
                Pause::ALL => command.arg("ALL"),
            };
        }
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("CLIENT PAUSE requires at least 1 argument (timeout)"));
        }

        let timeout = args[0].clone();
        let pause = args.get(1).and_then(|v| {
            if let RedisJsonValue::String(s) = v {
                match s.to_uppercase().as_str() {
                    "WRITE" => Some(Pause::WRITE),
                    "ALL" => Some(Pause::ALL),
                    _ => None,
                }
            } else {
                None
            }
        });

        Ok(Self { timeout, pause })
    }
}

/// Output for Redis CLIENT PAUSE command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClientPauseOutput {
    result: String,
}

impl ClientPauseOutput {
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

impl Serialize for ClientPauseOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClientPauseOutput", 1)?;
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
            let input = ClientPauseInput { timeout: RedisJsonValue::Integer(1000), pause: None };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"PAUSE"));
        }

        #[test]
        fn test_decode_ok() {
            let output = ClientPauseOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
        }

        #[test]
        fn test_keys_empty() {
            let input = ClientPauseInput { timeout: RedisJsonValue::Integer(100), pause: None };
            assert!(input.keys().is_empty());
        }
    }
}
