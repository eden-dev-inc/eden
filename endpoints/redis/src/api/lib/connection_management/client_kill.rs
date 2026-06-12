use super::{Addr, ClientKillResult, Filter, Input, Type};
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

const API_INFO: ApiInfo<RedisApi, ClientKillInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::ClientKill, "Terminates open connections", ReqType::Write, false);

/// See official Redis documentation for `CLIENT KILL`
/// https://redis.io/docs/latest/commands/client-kill/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClientKillInput {
    filter: Input,
}

impl Serialize for ClientKillInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClientKillInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        match &self.filter {
            Input::Addr(addr) => state.serialize_field("addr", &addr)?,
            Input::Filters(filters) => state.serialize_field("filters", &filters)?,
        }
        state.end()
    }
}

impl_redis_operation!(ClientKillInput, API_INFO, { filter });

impl RedisCommandInput for ClientKillInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        match &self.filter {
            Input::Addr(addr) => addr.cmd(&mut command),
            Input::Filters(filters) => {
                for filter in filters {
                    match filter {
                        Filter::IP(addr) | Filter::ADDR(addr) | Filter::LADDR(addr) => addr.cmd(&mut command),
                        Filter::ID(id) => {
                            command.arg("ID").arg(id);
                        }
                        Filter::TYPE(t) => {
                            command.arg("TYPE");
                            command.arg(match t {
                                Type::NORMAL => "NORMAL",
                                Type::MASTER => "MASTER",
                                Type::SLAVE => "SLAVE",
                                Type::REPLICA => "REPLICA",
                                Type::PUBSUB => "PUBSUB",
                            });
                        }
                        Filter::USER(u) => {
                            command.arg("USER").arg(u);
                        }
                        Filter::SKIPME(b) => {
                            command.arg("SKIPME").arg(if *b { "YES" } else { "NO" });
                        }
                        Filter::MAXAGE(m) => {
                            command.arg("MAXAGE").arg(m);
                        }
                    }
                }
            }
        }
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("CLIENT KILL requires at least 1 argument"));
        }

        // Check old format (just ip:port)
        if args.len() == 1
            && let RedisJsonValue::String(s) = &args[0]
            && let Some((ip, port)) = s.split_once(':')
        {
            return Ok(Self {
                filter: Input::Addr(Addr {
                    ip: RedisJsonValue::String(ip.to_string()),
                    port: RedisJsonValue::String(port.to_string()),
                }),
            });
        }

        // Parse new format with filters (simplified)
        let mut filters = Vec::new();
        let mut i = 0;
        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                match s.to_uppercase().as_str() {
                    "ID" if i + 1 < args.len() => {
                        filters.push(Filter::ID(args[i + 1].clone()));
                        i += 2;
                    }
                    "TYPE" if i + 1 < args.len() => {
                        if let RedisJsonValue::String(ts) = &args[i + 1] {
                            let t = match ts.to_uppercase().as_str() {
                                "MASTER" => Type::MASTER,
                                "SLAVE" => Type::SLAVE,
                                "REPLICA" => Type::REPLICA,
                                "PUBSUB" => Type::PUBSUB,
                                _ => Type::NORMAL,
                            };
                            filters.push(Filter::TYPE(t));
                        }
                        i += 2;
                    }
                    "USER" if i + 1 < args.len() => {
                        filters.push(Filter::USER(args[i + 1].clone()));
                        i += 2;
                    }
                    "SKIPME" if i + 1 < args.len() => {
                        if let RedisJsonValue::String(v) = &args[i + 1] {
                            filters.push(Filter::SKIPME(v.to_uppercase() == "YES"));
                        }
                        i += 2;
                    }
                    _ => i += 1,
                }
            } else {
                i += 1;
            }
        }

        if filters.is_empty() {
            return Err(EpError::request("CLIENT KILL requires valid filters"));
        }

        Ok(Self { filter: Input::Filters(filters) })
    }
}

/// Output for Redis CLIENT KILL command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClientKillOutput {
    /// Number of clients killed (new format) or OK (old format)
    result: ClientKillResult,
}

impl ClientKillOutput {
    pub fn killed_count(&self) -> Option<i64> {
        match &self.result {
            ClientKillResult::Count(n) => Some(*n),
            ClientKillResult::Ok => Some(1),
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => ClientKillResult::Count(n),
                Resp2Frame::SimpleString(s) if s == b"OK" => ClientKillResult::Ok,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => ClientKillResult::Count(data),
                Resp3Frame::SimpleString { data, .. } if data == b"OK" => ClientKillResult::Ok,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for ClientKillOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClientKillOutput", 1)?;
        state.serialize_field("result", &self.killed_count())?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_decode_count() {
            let output = ClientKillOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.killed_count(), Some(5));
        }

        #[test]
        fn test_decode_ok() {
            let output = ClientKillOutput::decode(b"+OK\r\n").unwrap();
            assert_eq!(output.killed_count(), Some(1));
        }

        #[test]
        fn test_keys_empty() {
            let input = ClientKillInput {
                filter: Input::Filters(vec![Filter::ID(RedisJsonValue::Integer(1))]),
            };
            assert!(input.keys().is_empty());
        }
    }
}
