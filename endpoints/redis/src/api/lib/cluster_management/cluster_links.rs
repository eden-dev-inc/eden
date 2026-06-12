use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{ClusterLink, key::RedisKey, value::RedisJsonValue};
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
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, ClusterLinksInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterLinks,
    "Returns a list of all TCP links to and from peer nodes",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `CLUSTER LINKS`
/// https://redis.io/docs/latest/commands/cluster-links/
///
/// Available since Redis 7.0.0
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterLinksInput {}

impl Serialize for ClusterLinksInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterLinksInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ClusterLinksInput, API_INFO);

impl RedisCommandInput for ClusterLinksInput {
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
            log_warn!(
                _ctx,
                "CLUSTER LINKS expects no arguments, given {}",
                audience = LogAudience::Internal,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis CLUSTER LINKS command
///
/// Returns a list of all TCP links to and from peer nodes in the cluster.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterLinksOutput {
    /// List of cluster links
    links: Vec<ClusterLink>,
}

impl ClusterLinksOutput {
    pub fn new(links: Vec<ClusterLink>) -> Self {
        Self { links }
    }

    /// Get the list of links
    pub fn links(&self) -> &[ClusterLink] {
        &self.links
    }

    /// Get the number of links
    pub fn len(&self) -> usize {
        self.links.len()
    }

    /// Check if there are no links
    pub fn is_empty(&self) -> bool {
        self.links.is_empty()
    }

    /// Decode the Redis protocol response into a ClusterLinksOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let links = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => Self::parse_links_array_resp2(&arr)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER LINKS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => Self::parse_links_array_resp3(&data)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER LINKS response: {:?}", other)));
                }
            },
        };

        Ok(Self { links })
    }

    fn parse_links_array_resp2(arr: &[Resp2Frame]) -> Result<Vec<ClusterLink>, EpError> {
        let mut links = Vec::new();

        for item in arr {
            if let Resp2Frame::Array(link_arr) = item {
                links.push(Self::parse_single_link_resp2(link_arr)?);
            }
        }

        Ok(links)
    }

    fn parse_single_link_resp2(arr: &[Resp2Frame]) -> Result<ClusterLink, EpError> {
        // CLUSTER LINKS returns an array of key-value pairs
        // ["direction", "to", "node", "abc123...", "create-time", 12345, ...]
        let mut direction = String::new();
        let mut node = String::new();
        let mut create_time = 0i64;
        let mut events = String::new();
        let mut send_buffer_allocated = 0i64;
        let mut send_buffer_used = 0i64;

        let mut i = 0;
        while i < arr.len() - 1 {
            let key = match &arr[i] {
                Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => String::from_utf8_lossy(b).to_string(),
                _ => {
                    i += 1;
                    continue;
                }
            };

            let value = &arr[i + 1];
            match key.as_str() {
                "direction" => {
                    if let Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) = value {
                        direction = String::from_utf8_lossy(b).to_string();
                    }
                }
                "node" => {
                    if let Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) = value {
                        node = String::from_utf8_lossy(b).to_string();
                    }
                }
                "create-time" => {
                    if let Resp2Frame::Integer(n) = value {
                        create_time = *n;
                    }
                }
                "events" => {
                    if let Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) = value {
                        events = String::from_utf8_lossy(b).to_string();
                    }
                }
                "send-buffer-allocated" => {
                    if let Resp2Frame::Integer(n) = value {
                        send_buffer_allocated = *n;
                    }
                }
                "send-buffer-used" => {
                    if let Resp2Frame::Integer(n) = value {
                        send_buffer_used = *n;
                    }
                }
                _ => {}
            }
            i += 2;
        }

        Ok(ClusterLink {
            direction,
            node,
            create_time,
            events,
            send_buffer_allocated,
            send_buffer_used,
        })
    }

    fn parse_links_array_resp3(arr: &[Resp3Frame]) -> Result<Vec<ClusterLink>, EpError> {
        let mut links = Vec::new();

        for item in arr {
            match item {
                Resp3Frame::Array { data, .. } => {
                    links.push(Self::parse_single_link_resp3(data)?);
                }
                Resp3Frame::Map { data, .. } => {
                    links.push(Self::parse_single_link_map_resp3(data)?);
                }
                _ => {}
            }
        }

        Ok(links)
    }

    fn parse_single_link_resp3(arr: &[Resp3Frame]) -> Result<ClusterLink, EpError> {
        let mut direction = String::new();
        let mut node = String::new();
        let mut create_time = 0i64;
        let mut events = String::new();
        let mut send_buffer_allocated = 0i64;
        let mut send_buffer_used = 0i64;

        let mut i = 0;
        while i < arr.len() - 1 {
            let key = match &arr[i] {
                Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => String::from_utf8_lossy(data).to_string(),
                _ => {
                    i += 1;
                    continue;
                }
            };

            let value = &arr[i + 1];
            match key.as_str() {
                "direction" => {
                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = value {
                        direction = String::from_utf8_lossy(data).to_string();
                    }
                }
                "node" => {
                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = value {
                        node = String::from_utf8_lossy(data).to_string();
                    }
                }
                "create-time" => {
                    if let Resp3Frame::Number { data, .. } = value {
                        create_time = *data;
                    }
                }
                "events" => {
                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = value {
                        events = String::from_utf8_lossy(data).to_string();
                    }
                }
                "send-buffer-allocated" => {
                    if let Resp3Frame::Number { data, .. } = value {
                        send_buffer_allocated = *data;
                    }
                }
                "send-buffer-used" => {
                    if let Resp3Frame::Number { data, .. } = value {
                        send_buffer_used = *data;
                    }
                }
                _ => {}
            }
            i += 2;
        }

        Ok(ClusterLink {
            direction,
            node,
            create_time,
            events,
            send_buffer_allocated,
            send_buffer_used,
        })
    }

    fn parse_single_link_map_resp3(map: &HashMap<Resp3Frame, Resp3Frame>) -> Result<ClusterLink, EpError> {
        let mut direction = String::new();
        let mut node = String::new();
        let mut create_time = 0i64;
        let mut events = String::new();
        let mut send_buffer_allocated = 0i64;
        let mut send_buffer_used = 0i64;

        for (key_frame, value_frame) in map.iter() {
            let key = match key_frame {
                Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => String::from_utf8_lossy(data).to_string(),
                _ => continue,
            };

            match key.as_str() {
                "direction" => {
                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = value_frame {
                        direction = String::from_utf8_lossy(data).to_string();
                    }
                }
                "node" => {
                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = value_frame {
                        node = String::from_utf8_lossy(data).to_string();
                    }
                }
                "create-time" => {
                    if let Resp3Frame::Number { data, .. } = value_frame {
                        create_time = *data;
                    }
                }
                "events" => {
                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = value_frame {
                        events = String::from_utf8_lossy(data).to_string();
                    }
                }
                "send-buffer-allocated" => {
                    if let Resp3Frame::Number { data, .. } = value_frame {
                        send_buffer_allocated = *data;
                    }
                }
                "send-buffer-used" => {
                    if let Resp3Frame::Number { data, .. } = value_frame {
                        send_buffer_used = *data;
                    }
                }
                _ => {}
            }
        }

        Ok(ClusterLink {
            direction,
            node,
            create_time,
            events,
            send_buffer_allocated,
            send_buffer_used,
        })
    }
}

impl Serialize for ClusterLinksOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterLinksOutput", 1)?;
        state.serialize_field("links", &self.links)?;
        state.end()
    }
}

impl Serialize for ClusterLink {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterLink", 6)?;
        state.serialize_field("direction", &self.direction)?;
        state.serialize_field("node", &self.node)?;
        state.serialize_field("create_time", &self.create_time)?;
        state.serialize_field("events", &self.events)?;
        state.serialize_field("send_buffer_allocated", &self.send_buffer_allocated)?;
        state.serialize_field("send_buffer_used", &self.send_buffer_used)?;
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
            let input = ClusterLinksInput {};
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("LINKS") || cmd_str.contains("CLUSTER LINKS"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ClusterLinksInput::decode(args).unwrap();
            assert_eq!(input.keys().len(), 0);
        }

        #[test]
        fn test_decode_input_with_extra_args_succeeds() {
            let args = vec![RedisJsonValue::String("extra".into())];
            let result = ClusterLinksInput::decode(args);
            assert!(result.is_ok());
        }

        #[test]
        fn test_decode_output_empty_array() {
            // Empty array response
            let response = b"*0\r\n";
            let output = ClusterLinksOutput::decode(response).unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_output_error_response() {
            let response = b"-ERR This instance has cluster support disabled\r\n";
            let err = ClusterLinksOutput::decode(response).unwrap_err();
            assert!(err.to_string().contains("cluster support disabled"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterLinksInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_cluster_link_serialization() {
            let link = ClusterLink {
                direction: "to".to_string(),
                node: "abc123".to_string(),
                create_time: 1234567890,
                events: "rw".to_string(),
                send_buffer_allocated: 1024,
                send_buffer_used: 512,
            };
            let json = serde_json::to_string(&link).unwrap();
            assert!(json.contains("\"direction\":\"to\""));
            assert!(json.contains("\"node\":\"abc123\""));
        }

        #[test]
        fn test_output_serialization() {
            let output = ClusterLinksOutput::new(vec![]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"links\":[]"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_links_standalone_returns_error() {
            // CLUSTER LINKS requires Redis 7.0+
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ClusterLinksInput {}.command()).await.expect("raw failed");

                    let decode_result = ClusterLinksOutput::decode(&result);

                    match decode_result {
                        Ok(output) => {
                            // Cluster mode - should have links array (possibly empty)
                            let _ = output.links();
                        }
                        Err(e) => {
                            // Standalone mode - should mention cluster disabled
                            let err_msg = e.to_string().to_lowercase();
                            assert!(
                                err_msg.contains("cluster") || err_msg.contains("disabled"),
                                "Expected cluster-related error, got: {}",
                                e
                            );
                        }
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_links_resp2_format() {
            for version in REDIS_VERSIONS {
                if version_is_earlier("7.0", version) {
                    continue; // CLUSTER LINKS requires Redis 7.0+
                }

                let mut ctx = setup(RespVersion::Resp2, Some(version)).await;
                let result = ctx.raw(&ClusterLinksInput {}.command()).await.expect("raw failed");

                // Should get either array or error
                assert!(
                    result.starts_with(b"*") || result.starts_with(b"-"),
                    "Expected array or error, got: {:?}",
                    String::from_utf8_lossy(&result)
                );

                ctx.stop().await;
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_links_resp3_format() {
            for version in REDIS_VERSIONS {
                if version_is_earlier("7.0", version) {
                    continue; // CLUSTER LINKS requires Redis 7.0+
                }

                let mut ctx = setup(RespVersion::Resp3, Some(version)).await;
                let result = ctx.raw(&ClusterLinksInput {}.command()).await.expect("raw failed");

                // RESP3 array or error
                assert!(
                    result.starts_with(b"*") || result.starts_with(b"-") || result.starts_with(b"!"),
                    "Expected array or error, got: {:?}",
                    String::from_utf8_lossy(&result)
                );

                ctx.stop().await;
            }
        }
    }
}
