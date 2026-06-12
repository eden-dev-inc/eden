use crate::api::lib::{RedisApi, RedisCommandInput, RedisCommandOutput};
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
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ClusterCountFailureReportsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterCountFailureReports,
    "Returns the number of active failure reports active for a node",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `CLUSTER COUNT-FAILURE-REPORTS`
/// https://redis.io/docs/latest/commands/cluster-count-failure-reports/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterCountFailureReportsInput {
    node_id: RedisJsonValue,
}

impl Serialize for ClusterCountFailureReportsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterCountFailureReportsInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("node_id", &self.node_id)?;
        state.end()
    }
}

impl_redis_operation!(ClusterCountFailureReportsInput, API_INFO, { node_id });

impl RedisCommandInput for ClusterCountFailureReportsInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.node_id);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("CLUSTER COUNT-FAILURE-REPORTS requires 1 argument, given none"));
        }

        if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "CLUSTER COUNT-FAILURE-REPORTS expects 1 argument, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { node_id: args[0].clone() })
    }
}

/// Output for Redis CLUSTER COUNT-FAILURE-REPORTS command
///
/// Returns the number of active failure reports for the specified node.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterCountFailureReportsOutput {
    /// The number of active failure reports
    count: i64,
}

impl ClusterCountFailureReportsOutput {
    pub fn new(count: i64) -> Self {
        Self { count }
    }

    /// Get the number of failure reports
    pub fn count(&self) -> i64 {
        self.count
    }
}

impl Serialize for ClusterCountFailureReportsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterCountFailureReportsOutput", 1)?;
        state.serialize_field("count", &self.count)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterCountFailureReportsOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterCountFailureReports
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER COUNT-FAILURE-REPORTS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER COUNT-FAILURE-REPORTS response: {:?}", other)));
                }
            },
        };

        Ok(Self { count })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ClusterCountFailureReportsInput {
                node_id: RedisJsonValue::String("07c37dfeb235213a872192d90877d0cd55635b91".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("COUNT-FAILURE-REPORTS"));
            assert!(cmd_str.contains("07c37dfeb235213a872192d90877d0cd55635b91"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterCountFailureReportsInput { node_id: RedisJsonValue::String("nodeid".into()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("07c37dfeb235213a872192d90877d0cd55635b91".into())];
            let input = ClusterCountFailureReportsInput::decode(args).unwrap();
            assert_eq!(input.node_id, RedisJsonValue::String("07c37dfeb235213a872192d90877d0cd55635b91".into()));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ClusterCountFailureReportsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_output_integer() {
            let output = ClusterCountFailureReportsOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.count(), 5);
        }

        #[test]
        fn test_decode_output_zero() {
            let output = ClusterCountFailureReportsOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.count(), 0);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = ClusterCountFailureReportsOutput::decode(b"-ERR Unknown node\r\n").unwrap_err();
            assert!(err.to_string().contains("Unknown node"));
        }
    }
}
