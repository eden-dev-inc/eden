use crate::api::lib::server_management::replication_common::{ReplicationTarget, parse_replication_args};
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::Serializer;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ReplicaofInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Replicaof,
    "Configures a server as replica of another, or promotes it to a master",
    ReqType::Write,
    false,
);

/// See official Redis documentation for `REPLICAOF`
/// https://redis.io/docs/latest/commands/replicaof/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ReplicaofInput {
    target: ReplicationTarget,
}

impl ReplicaofInput {
    pub fn new(target: ReplicationTarget) -> Self {
        Self { target }
    }

    pub fn host_port(host: impl Into<RedisJsonValue>, port: impl Into<RedisJsonValue>) -> Self {
        Self { target: ReplicationTarget::host_port(host, port) }
    }

    pub fn no_one() -> Self {
        Self { target: ReplicationTarget::no_one() }
    }

    pub fn target(&self) -> &ReplicationTarget {
        &self.target
    }
}

impl Default for ReplicaofInput {
    fn default() -> Self {
        Self::no_one()
    }
}

impl Serialize for ReplicaofInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ReplicaofInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("target", &self.target)?;
        state.end()
    }
}

impl_redis_operation!(ReplicaofInput, API_INFO, { target });

impl RedisCommandInput for ReplicaofInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        match &self.target {
            ReplicationTarget::HostPort(addr) => {
                command.arg(&addr.host).arg(&addr.port);
            }
            ReplicationTarget::NoOne => {
                command.arg("NO").arg("ONE");
            }
        };

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let target = parse_replication_args(args, "REPLICAOF")?;
        Ok(Self { target })
    }
}

/// Output for Redis REPLICAOF command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ReplicaofOutput {
    message: String,
}

impl ReplicaofOutput {
    pub fn new(message: String) -> Self {
        Self { message }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn is_ok(&self) -> bool {
        self.message == "OK"
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let message = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected REPLICAOF response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected REPLICAOF response: {:?}", other)));
                }
            },
        };

        Ok(Self { message })
    }
}

impl Serialize for ReplicaofOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ReplicaofOutput", 1)?;
        state.serialize_field("message", &self.message)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_one() {
            let input = ReplicaofInput::no_one();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("REPLICAOF"));
            assert!(cmd_str.contains("NO"));
            assert!(cmd_str.contains("ONE"));
        }

        #[test]
        fn test_encode_command_host_port() {
            let input = ReplicaofInput::host_port(RedisJsonValue::String("192.168.1.1".into()), RedisJsonValue::Integer(6380));
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("REPLICAOF"));
            assert!(cmd_str.contains("192.168.1.1"));
            assert!(cmd_str.contains("6380"));
        }

        #[test]
        fn test_decode_no_one() {
            let args = vec![RedisJsonValue::String("NO".into()), RedisJsonValue::String("ONE".into())];
            let input = ReplicaofInput::decode(args).unwrap();
            assert!(input.target().is_no_one());
        }

        #[test]
        fn test_decode_no_one_lowercase() {
            let args = vec![RedisJsonValue::String("no".into()), RedisJsonValue::String("one".into())];
            let input = ReplicaofInput::decode(args).unwrap();
            assert!(input.target().is_no_one());
        }

        #[test]
        fn test_decode_host_port() {
            let args = vec![RedisJsonValue::String("localhost".into()), RedisJsonValue::Integer(6379)];
            let input = ReplicaofInput::decode(args).unwrap();
            assert!(!input.target().is_no_one());
            let addr = input.target().addr().unwrap();
            assert_eq!(addr.host(), &RedisJsonValue::String("localhost".into()));
            assert_eq!(addr.port(), &RedisJsonValue::Integer(6379));
        }

        #[test]
        fn test_decode_wrong_arg_count() {
            let args = vec![RedisJsonValue::String("localhost".into())];
            let err = ReplicaofInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_three_args() {
            let args = vec![
                RedisJsonValue::String("a".into()),
                RedisJsonValue::String("b".into()),
                RedisJsonValue::String("c".into()),
            ];
            let err = ReplicaofInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ReplicaofInput::no_one();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ReplicaofInput::no_one();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Replicaof);
        }

        #[test]
        fn test_default() {
            let input = ReplicaofInput::default();
            assert!(input.target().is_no_one());
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ReplicaofOutput::decode(b"+OK\r\n").unwrap();
            assert_eq!(output.message(), "OK");
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_error_response() {
            let err = ReplicaofOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_replicaof_no_one() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // REPLICAOF NO ONE on a standalone server should succeed
                    let result = ctx.raw(&ReplicaofInput::no_one().command()).await.expect("raw failed");

                    let output = ReplicaofOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());
                })
            })
            .await;
        }
    }
}
