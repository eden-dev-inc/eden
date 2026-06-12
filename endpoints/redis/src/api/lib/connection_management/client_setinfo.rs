use crate::api::lib::connection_management::Info;
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

const API_INFO: ApiInfo<RedisApi, ClientSetinfoInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClientSetinfo,
    "Sets information specific to the client or connection",
    ReqType::Write,
    false,
);

/// See official Redis documentation for `CLIENT SETINFO`
/// https://redis.io/docs/latest/commands/client-setinfo/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClientSetinfoInput {
    info: Info,
}

impl Serialize for ClientSetinfoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClientSetinfoInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        match &self.info {
            Info::LibName(n) => state.serialize_field("lib_name", n)?,
            Info::LibVer(v) => state.serialize_field("lib_ver", v)?,
        }
        state.end()
    }
}

impl_redis_operation!(ClientSetinfoInput, API_INFO, { info });

impl RedisCommandInput for ClientSetinfoInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let api_str = API_INFO.api.to_string();
        let args: Vec<&str> = api_str.split_whitespace().collect();
        let mut command = crate::command::cmd(args[0]);
        command.arg(args[1]);
        match &self.info {
            Info::LibName(n) => command.arg("LIB-NAME").arg(n),
            Info::LibVer(v) => command.arg("LIB-VER").arg(v),
        };
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::request(format!("CLIENT SETINFO requires 2 arguments, given {}", args.len())));
        }

        let info = match &args[0] {
            RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                "LIB-NAME" => Info::LibName(args[1].clone()),
                "LIB-VER" => Info::LibVer(args[1].clone()),
                _ => return Err(EpError::request(format!("Unknown attribute: {}", s))),
            },
            _ => return Err(EpError::request("First argument must be a string")),
        };

        Ok(Self { info })
    }
}

/// Output for Redis CLIENT SETINFO command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClientSetinfoOutput {
    result: String,
}

impl ClientSetinfoOutput {
    pub fn is_ok(&self) -> bool {
        self.result == "OK"
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::SimpleString(s)) => String::from_utf8(s).map_err(EpError::parse)?,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleString { data, .. }) => String::from_utf8(data).map_err(EpError::parse)?,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
        };

        Ok(Self { result })
    }
}

impl Serialize for ClientSetinfoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClientSetinfoOutput", 1)?;
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
        fn test_encode_lib_name() {
            let input = ClientSetinfoInput { info: Info::LibName(RedisJsonValue::String("mylib".into())) };
            let cmd = input.command();
            assert!(cmd.windows(8).any(|w| w == b"LIB-NAME"));
        }

        #[test]
        fn test_decode_ok() {
            assert!(ClientSetinfoOutput::decode(b"+OK\r\n").unwrap().is_ok());
        }

        #[test]
        fn test_keys_empty() {
            let input = ClientSetinfoInput { info: Info::LibName(RedisJsonValue::String("x".into())) };
            assert!(input.keys().is_empty());
        }
    }
}
