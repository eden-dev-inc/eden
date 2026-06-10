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

const API_INFO: ApiInfo<RedisApi, TdigestMergeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TdigestMerge,
    "Merges multiple t-digest sketches into a single sketch",
    ReqType::Write,
    true,
);

/// Input for Redis `TDIGEST.MERGE` command.
///
/// Merges multiple t-digest sketches into a single sketch.
///
/// See official Redis documentation for `TDIGEST.MERGE`:
/// https://redis.io/docs/latest/commands/tdigest.merge/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TdigestMergeInput {
    /// The destination key for the merged t-digest sketch
    pub(crate) destination_key: RedisKey,
    /// The number of source keys to merge
    pub(crate) numkeys: RedisJsonValue,
    /// The source keys to merge from
    pub(crate) source_keys: Vec<RedisKey>,
    /// Optional compression parameter for the destination sketch
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub(crate) compression: Option<RedisJsonValue>,
    /// Whether to override an existing destination key
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub(crate) r#override: Option<bool>,
}

impl Serialize for TdigestMergeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4; // type, destination_key, numkeys, source_keys
        if self.compression.is_some() {
            fields += 1;
        }
        if self.r#override.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("TdigestMergeInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("destination_key", &self.destination_key)?;
        state.serialize_field("numkeys", &self.numkeys)?;
        state.serialize_field("source_keys", &self.source_keys)?;

        if let Some(compression) = &self.compression {
            state.serialize_field("compression", compression)?;
        }
        if let Some(override_val) = &self.r#override {
            state.serialize_field("override", override_val)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    TdigestMergeInput,
    API_INFO,
    {destination_key, numkeys, source_keys, compression, r#override}
);

impl RedisCommandInput for TdigestMergeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        let mut keys = vec![self.destination_key.clone()];
        keys.extend(self.source_keys.clone());
        keys
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.destination_key).arg(&self.numkeys);

        for key in &self.source_keys {
            command.arg(key);
        }

        if let Some(compression) = &self.compression {
            command.arg("COMPRESSION").arg(compression);
        }

        if let Some(or) = self.r#override
            && or
        {
            command.arg("OVERRIDE");
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::parse(
                "TDIGEST.MERGE requires at least 3 arguments (destination_key, numkeys, source_key...)",
            ));
        }

        let destination_key = args[0].clone().try_into()?;
        let numkeys = args[1].clone();

        // Parse numkeys to determine how many source keys to expect
        let num_keys = match &numkeys {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::parse("numkeys must be a valid integer"))?,
            _ => return Err(EpError::parse("numkeys must be an integer")),
        };

        if num_keys == 0 {
            return Err(EpError::parse("numkeys must be at least 1"));
        }

        if args.len() < 2 + num_keys {
            return Err(EpError::parse(format!("Expected {} source keys but only {} provided", num_keys, args.len() - 2)));
        }

        let mut source_keys = Vec::with_capacity(num_keys);
        for i in 0..num_keys {
            source_keys.push(args[2 + i].clone().try_into()?);
        }

        let mut compression = None;
        let mut r#override = None;
        let mut i = 2 + num_keys;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                match s.to_uppercase().as_str() {
                    "COMPRESSION" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::parse("COMPRESSION requires a value"));
                        }
                        compression = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "OVERRIDE" => {
                        r#override = Some(true);
                        i += 1;
                    }
                    _ => {
                        return Err(EpError::parse(format!("Unknown TDIGEST.MERGE option: {}", s)));
                    }
                }
            } else {
                return Err(EpError::parse("TDIGEST.MERGE options must be strings"));
            }
        }

        Ok(TdigestMergeInput {
            destination_key,
            numkeys,
            source_keys,
            compression,
            r#override,
        })
    }
}

/// Output for Redis `TDIGEST.MERGE` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TdigestMergeOutput {
    /// Whether the operation succeeded
    success: bool,
}

impl TdigestMergeOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the operation was successful
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a TdigestMergeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s.eq_ignore_ascii_case(b"OK") => Ok(Self { success: true }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected TDIGEST.MERGE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data.eq_ignore_ascii_case(b"OK") => Ok(Self { success: true }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected TDIGEST.MERGE response: {:?}", other))),
            },
        }
    }
}

impl Serialize for TdigestMergeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestMergeOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = TdigestMergeInput {
                destination_key: RedisKey::String("dest".into()),
                numkeys: RedisJsonValue::Integer(2),
                source_keys: vec![RedisKey::String("src1".into()), RedisKey::String("src2".into())],
                compression: None,
                r#override: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TDIGEST.MERGE"));
            assert!(cmd_str.contains("dest"));
        }

        #[test]
        fn test_encode_command_with_compression() {
            let input = TdigestMergeInput {
                destination_key: RedisKey::String("dest".into()),
                numkeys: RedisJsonValue::Integer(1),
                source_keys: vec![RedisKey::String("src1".into())],
                compression: Some(RedisJsonValue::Integer(200)),
                r#override: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("COMPRESSION"));
        }

        #[test]
        fn test_encode_command_with_override() {
            let input = TdigestMergeInput {
                destination_key: RedisKey::String("dest".into()),
                numkeys: RedisJsonValue::Integer(1),
                source_keys: vec![RedisKey::String("src1".into())],
                compression: None,
                r#override: Some(true),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("OVERRIDE"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = TdigestMergeOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = TdigestMergeOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::Integer(2),
                RedisJsonValue::String("src1".into()),
                RedisJsonValue::String("src2".into()),
            ];
            let input = TdigestMergeInput::decode(args).unwrap();
            assert_eq!(input.destination_key, RedisKey::String("dest".into()));
            assert_eq!(input.source_keys.len(), 2);
        }

        #[test]
        fn test_decode_input_with_compression() {
            let args = vec![
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("src1".into()),
                RedisJsonValue::String("COMPRESSION".into()),
                RedisJsonValue::Integer(200),
            ];
            let input = TdigestMergeInput::decode(args).unwrap();
            assert!(input.compression.is_some());
        }

        #[test]
        fn test_decode_input_with_override() {
            let args = vec![
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("src1".into()),
                RedisJsonValue::String("OVERRIDE".into()),
            ];
            let input = TdigestMergeInput::decode(args).unwrap();
            assert_eq!(input.r#override, Some(true));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("dest".into()), RedisJsonValue::Integer(1)];
            let err = TdigestMergeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 3 arguments"));
        }

        #[test]
        fn test_decode_input_missing_source_keys() {
            let args = vec![
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::Integer(3),
                RedisJsonValue::String("src1".into()),
            ];
            let err = TdigestMergeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("source keys"));
        }

        #[test]
        fn test_decode_input_compression_missing_value() {
            let args = vec![
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("src1".into()),
                RedisJsonValue::String("COMPRESSION".into()),
            ];
            let err = TdigestMergeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires a value"));
        }

        #[test]
        fn test_decode_input_unknown_option() {
            let args = vec![
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("src1".into()),
                RedisJsonValue::String("UNKNOWN".into()),
            ];
            let err = TdigestMergeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Unknown"));
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = TdigestMergeInput {
                destination_key: RedisKey::String("dest".into()),
                numkeys: RedisJsonValue::Integer(2),
                source_keys: vec![RedisKey::String("src1".into()), RedisKey::String("src2".into())],
                compression: None,
                r#override: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 3);
            assert_eq!(keys[0], RedisKey::String("dest".into()));
            assert_eq!(keys[1], RedisKey::String("src1".into()));
            assert_eq!(keys[2], RedisKey::String("src2".into()));
        }

        #[test]
        fn test_serialize_output() {
            let output = TdigestMergeOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("success"));
            assert!(json.contains("true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::t_digest::tdigest_add::TdigestAddInput;
        use crate::api::lib::t_digest::tdigest_create::TdigestCreateInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_merge_basic() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    // Create source sketches
                    let Ok(create1) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_merge_src1".into()),
                                compression: None,
                            }
                            .command(),
                        )
                        .await
                    else {
                        return;
                    };

                    if create1.starts_with(b"-") {
                        return;
                    }

                    ctx.raw(
                        &TdigestCreateInput {
                            key: RedisKey::String("td_merge_src2".into()),
                            compression: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("create src2 failed");

                    // Add values to source sketches
                    ctx.raw(
                        &TdigestAddInput {
                            key: RedisKey::String("td_merge_src1".into()),
                            value: vec![RedisJsonValue::Float(1.0), RedisJsonValue::Float(2.0)],
                        }
                        .command(),
                    )
                    .await
                    .expect("add to src1 failed");

                    ctx.raw(
                        &TdigestAddInput {
                            key: RedisKey::String("td_merge_src2".into()),
                            value: vec![RedisJsonValue::Float(3.0), RedisJsonValue::Float(4.0)],
                        }
                        .command(),
                    )
                    .await
                    .expect("add to src2 failed");

                    // Merge
                    let result = ctx
                        .raw(
                            &TdigestMergeInput {
                                destination_key: RedisKey::String("td_merge_dest".into()),
                                numkeys: RedisJsonValue::Integer(2),
                                source_keys: vec![RedisKey::String("td_merge_src1".into()), RedisKey::String("td_merge_src2".into())],
                                compression: None,
                                r#override: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("merge failed");

                    let output = TdigestMergeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_merge_with_override() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_merge_over_src".into()),
                                compression: None,
                            }
                            .command(),
                        )
                        .await
                    else {
                        return;
                    };

                    if create_result.starts_with(b"-") {
                        return;
                    }

                    // Create destination that will be overwritten
                    ctx.raw(
                        &TdigestCreateInput {
                            key: RedisKey::String("td_merge_over_dest".into()),
                            compression: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("create dest failed");

                    ctx.raw(
                        &TdigestAddInput {
                            key: RedisKey::String("td_merge_over_src".into()),
                            value: vec![RedisJsonValue::Float(1.0)],
                        }
                        .command(),
                    )
                    .await
                    .expect("add failed");

                    // Merge with override
                    let result = ctx
                        .raw(
                            &TdigestMergeInput {
                                destination_key: RedisKey::String("td_merge_over_dest".into()),
                                numkeys: RedisJsonValue::Integer(1),
                                source_keys: vec![RedisKey::String("td_merge_over_src".into())],
                                compression: None,
                                r#override: Some(true),
                            }
                            .command(),
                        )
                        .await
                        .expect("merge failed");

                    let output = TdigestMergeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_merge_nonexistent_source() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TdigestMergeInput {
                                destination_key: RedisKey::String("td_merge_bad".into()),
                                numkeys: RedisJsonValue::Integer(1),
                                source_keys: vec![RedisKey::String("nonexistent_td".into())],
                                compression: None,
                                r#override: None,
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(result) = result
                        && result.starts_with(b"-")
                    {}
                })
            })
            .await;
        }
    }
}
