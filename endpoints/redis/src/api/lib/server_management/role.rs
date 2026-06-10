use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::Serializer;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, RoleInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Role, "Returns the replication role", ReqType::Read, true);

/// See official Redis documentation for `ROLE`
/// https://redis.io/docs/latest/commands/role/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct RoleInput {}

impl RoleInput {
    pub fn new() -> Self {
        Self {}
    }
}

impl Serialize for RoleInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RoleInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(RoleInput, API_INFO);

impl RedisCommandInput for RoleInput {
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
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(_ctx, "ROLE expects no arguments, given {}", audience = LogAudience::Client, args_given = args.len());
        }
        Ok(Self::default())
    }
}

/// The role of the Redis instance in replication
#[derive(Debug, Clone, PartialEq, Eq, ToSchema, JsonSchema)]
pub enum ReplicationRole {
    /// This instance is a master
    Master {
        /// The current replication offset
        replication_offset: i64,
        /// Connected replicas with their state
        replicas: Vec<ReplicaInfo>,
    },
    /// This instance is a replica
    Replica {
        /// The master's host
        master_host: String,
        /// The master's port
        master_port: i64,
        /// Connection state (connect, connecting, sync, connected)
        state: String,
        /// Amount of data received from master
        data_received: i64,
    },
    /// This instance is a sentinel
    Sentinel {
        /// List of master names being monitored
        master_names: Vec<String>,
    },
}

/// Information about a connected replica
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct ReplicaInfo {
    pub host: String,
    pub port: i64,
    pub replication_offset: i64,
}

/// Output for Redis ROLE command
#[derive(Debug, Clone, ToSchema, JsonSchema)]
pub struct RoleOutput {
    role: ReplicationRole,
}

impl RoleOutput {
    pub fn new(role: ReplicationRole) -> Self {
        Self { role }
    }

    pub fn role(&self) -> &ReplicationRole {
        &self.role
    }

    pub fn is_master(&self) -> bool {
        matches!(self.role, ReplicationRole::Master { .. })
    }

    pub fn is_replica(&self) -> bool {
        matches!(self.role, ReplicationRole::Replica { .. })
    }

    pub fn is_sentinel(&self) -> bool {
        matches!(self.role, ReplicationRole::Sentinel { .. })
    }

    /// Decode the Redis protocol response into a RoleOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let role = Self::parse_frame(frame)?;
        Ok(Self { role })
    }

    fn parse_frame(frame: DecoderRespFrame) -> Result<ReplicationRole, EpError> {
        let array: Vec<DecoderRespFrame> = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                arr.into_iter().map(DecoderRespFrame::Resp2).collect::<Vec<DecoderRespFrame>>()
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                data.into_iter().map(DecoderRespFrame::Resp3).collect::<Vec<DecoderRespFrame>>()
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            other => {
                return Err(EpError::parse(format!("unexpected ROLE response: {:?}", other)));
            }
        };

        if array.is_empty() {
            return Err(EpError::parse("ROLE response array is empty"));
        }

        let role_type = Self::extract_string(&array[0])?;

        match role_type.to_lowercase().as_str() {
            "master" => Self::parse_master_role(&array),
            "slave" => Self::parse_replica_role(&array),
            "sentinel" => Self::parse_sentinel_role(&array),
            _ => Err(EpError::parse(format!("unknown role type: {}", role_type))),
        }
    }

    fn parse_master_role(array: &[DecoderRespFrame]) -> Result<ReplicationRole, EpError> {
        if array.len() < 3 {
            return Err(EpError::parse("master role response too short"));
        }

        let replication_offset = Self::extract_integer(&array[1])?;
        let replicas = Self::parse_replicas(&array[2])?;

        Ok(ReplicationRole::Master { replication_offset, replicas })
    }

    fn parse_replica_role(array: &[DecoderRespFrame]) -> Result<ReplicationRole, EpError> {
        if array.len() < 5 {
            return Err(EpError::parse("replica role response too short"));
        }

        let master_host = Self::extract_string(&array[1])?;
        let master_port = Self::extract_integer(&array[2])?;
        let state = Self::extract_string(&array[3])?;
        let data_received = Self::extract_integer(&array[4])?;

        Ok(ReplicationRole::Replica { master_host, master_port, state, data_received })
    }

    fn parse_sentinel_role(array: &[DecoderRespFrame]) -> Result<ReplicationRole, EpError> {
        if array.len() < 2 {
            return Err(EpError::parse("sentinel role response too short"));
        }

        let master_names = Self::extract_string_array(&array[1])?;

        Ok(ReplicationRole::Sentinel { master_names })
    }

    fn parse_replicas(frame: &DecoderRespFrame) -> Result<Vec<ReplicaInfo>, EpError> {
        let array = Self::extract_array(frame)?;

        let mut replicas = Vec::new();
        for replica_frame in array {
            let replica_arr = match replica_frame {
                DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                    arr.into_iter().map(DecoderRespFrame::Resp2).collect::<Vec<DecoderRespFrame>>()
                }
                DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                    data.into_iter().map(DecoderRespFrame::Resp3).collect::<Vec<DecoderRespFrame>>()
                }
                _ => continue,
            };

            if replica_arr.len() >= 3 {
                replicas.push(ReplicaInfo {
                    host: Self::extract_string(&replica_arr[0])?,
                    port: Self::extract_integer(&replica_arr[1])
                        .or_else(|_| Self::extract_string(&replica_arr[1])?.parse().map_err(EpError::parse))?,
                    replication_offset: Self::extract_integer(&replica_arr[2])
                        .or_else(|_| Self::extract_string(&replica_arr[2])?.parse().map_err(EpError::parse))?,
                });
            }
        }

        Ok(replicas)
    }

    fn extract_string(frame: &DecoderRespFrame) -> Result<String, EpError> {
        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::SimpleString(s)) => Ok(String::from_utf8(s.clone()).map_err(EpError::parse)?),
            DecoderRespFrame::Resp2(Resp2Frame::BulkString(s)) => Ok(String::from_utf8(s.clone()).map_err(EpError::parse)?),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleString { data, .. }) => String::from_utf8(data.clone()).map_err(EpError::parse),
            DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => String::from_utf8(data.clone()).map_err(EpError::parse),
            _ => Err(EpError::parse("expected string")),
        }
    }

    fn extract_integer(frame: &DecoderRespFrame) -> Result<i64, EpError> {
        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Integer(i)) => Ok(*i),
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => Ok(*data),
            _ => Err(EpError::parse("expected integer")),
        }
    }

    fn extract_string_array(frame: &DecoderRespFrame) -> Result<Vec<String>, EpError> {
        Self::extract_array(frame)?.iter().map(Self::extract_string).collect()
    }

    fn extract_array(frame: &DecoderRespFrame) -> Result<Vec<DecoderRespFrame>, EpError> {
        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                Ok(arr.iter().cloned().map(DecoderRespFrame::Resp2).collect::<Vec<DecoderRespFrame>>())
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                Ok(data.iter().cloned().map(DecoderRespFrame::Resp3).collect::<Vec<DecoderRespFrame>>())
            }
            _ => Err(EpError::parse("expected array")),
        }
    }
}

impl Serialize for RoleOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("RoleOutput", 1)?;
        match &self.role {
            ReplicationRole::Master { replication_offset, replicas } => {
                state.serialize_field("role", "master")?;
                state.serialize_field("replication_offset", replication_offset)?;
                state.serialize_field("replicas", replicas)?;
            }
            ReplicationRole::Replica { master_host, master_port, state: conn_state, data_received } => {
                state.serialize_field("role", "replica")?;
                state.serialize_field("master_host", master_host)?;
                state.serialize_field("master_port", master_port)?;
                state.serialize_field("state", conn_state)?;
                state.serialize_field("data_received", data_received)?;
            }
            ReplicationRole::Sentinel { master_names } => {
                state.serialize_field("role", "sentinel")?;
                state.serialize_field("master_names", master_names)?;
            }
        }
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
            let input = RoleInput::new();
            assert_eq!(input.command().to_vec(), b"*1\r\n$4\r\nROLE\r\n");
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = RoleInput::decode(args).unwrap();
            assert_eq!(format!("{:?}", input), "RoleInput");
        }

        #[test]
        fn test_decode_input_with_extra_args_succeeds() {
            // ROLE ignores extra args with a warning
            let args = vec![RedisJsonValue::String("extra".into())];
            let result = RoleInput::decode(args);
            assert!(result.is_ok());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = RoleInput::new();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = RoleInput::new();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Role);
        }

        #[test]
        fn test_decode_master_response() {
            // RESP2 master response: *3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n
            let bytes = b"*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n";
            let output = RoleOutput::decode(bytes).unwrap();
            assert!(output.is_master());
            assert!(!output.is_replica());
            assert!(!output.is_sentinel());

            match output.role() {
                ReplicationRole::Master { replication_offset, replicas } => {
                    assert_eq!(*replication_offset, 0);
                    assert!(replicas.is_empty());
                }
                _ => panic!("Expected master role"),
            }
        }

        #[test]
        fn test_decode_master_with_replicas() {
            // Master with one replica
            let bytes = b"*3\r\n$6\r\nmaster\r\n:12345\r\n*1\r\n*3\r\n$9\r\n127.0.0.1\r\n$4\r\n6380\r\n$5\r\n12345\r\n";
            let output = RoleOutput::decode(bytes).unwrap();
            assert!(output.is_master());

            match output.role() {
                ReplicationRole::Master { replication_offset, replicas } => {
                    assert_eq!(*replication_offset, 12345);
                    assert_eq!(replicas.len(), 1);
                    assert_eq!(replicas[0].host, "127.0.0.1");
                    assert_eq!(replicas[0].port, 6380);
                    assert_eq!(replicas[0].replication_offset, 12345);
                }
                _ => panic!("Expected master role"),
            }
        }

        #[test]
        fn test_decode_replica_response() {
            // RESP2 replica response
            let bytes = b"*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:6379\r\n$9\r\nconnected\r\n:12345\r\n";
            let output = RoleOutput::decode(bytes).unwrap();
            assert!(!output.is_master());
            assert!(output.is_replica());

            match output.role() {
                ReplicationRole::Replica { master_host, master_port, state, data_received } => {
                    assert_eq!(master_host, "127.0.0.1");
                    assert_eq!(*master_port, 6379);
                    assert_eq!(state, "connected");
                    assert_eq!(*data_received, 12345);
                }
                _ => panic!("Expected replica role"),
            }
        }

        #[test]
        fn test_decode_error_response() {
            let err = RoleOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_role_output_new() {
            let output = RoleOutput::new(ReplicationRole::Master { replication_offset: 100, replicas: vec![] });
            assert!(output.is_master());
        }

        #[test]
        fn test_replica_info() {
            let info = ReplicaInfo {
                host: "localhost".to_string(),
                port: 6380,
                replication_offset: 1000,
            };
            assert_eq!(info.host, "localhost");
            assert_eq!(info.port, 6380);
            assert_eq!(info.replication_offset, 1000);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_role_standalone() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&RoleInput::new().command()).await.expect("raw failed");

                    let output = RoleOutput::decode(&result).expect("decode failed");
                    // Standalone Redis should be a master
                    assert!(output.is_master());
                })
            })
            .await;
        }
    }
}
