use crate::api::lib::time_series::common::{TsEncoding, TsIgnore, TsLabel, append_labels_to_cmd, parse_labels_from_args};
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, TsCreateInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::TsCreate, "Create a new time series", ReqType::Write, true);

/// Input for Redis `TS.CREATE` command.
///
/// Create a new time series with optional settings.
///
/// See official Redis documentation for `TS.CREATE`:
/// https://redis.io/docs/latest/commands/ts.create/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TsCreateInput {
    pub(crate) key: RedisKey,
    #[builder(default)]
    pub(crate) retention: Option<RedisJsonValue>,
    #[builder(default)]
    pub(crate) encoding: Option<TsEncoding>,
    #[builder(default)]
    pub(crate) chunk_size: Option<RedisJsonValue>,
    #[builder(default)]
    pub(crate) duplicate_policy: Option<RedisJsonValue>,
    #[builder(default)]
    pub(crate) ignore: Option<TsIgnore>,
    #[builder(default)]
    pub(crate) labels: Option<Vec<TsLabel>>,
}

impl Serialize for TsCreateInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.retention.is_some() {
            fields += 1;
        }
        if self.encoding.is_some() {
            fields += 1;
        }
        if self.chunk_size.is_some() {
            fields += 1;
        }
        if self.duplicate_policy.is_some() {
            fields += 1;
        }
        if self.ignore.is_some() {
            fields += 1;
        }
        if self.labels.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("TsCreateInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(v) = &self.retention {
            state.serialize_field("retention", v)?;
        }
        if let Some(v) = &self.encoding {
            state.serialize_field("encoding", v)?;
        }
        if let Some(v) = &self.chunk_size {
            state.serialize_field("chunk_size", v)?;
        }
        if let Some(v) = &self.duplicate_policy {
            state.serialize_field("duplicate_policy", v)?;
        }
        if let Some(v) = &self.ignore {
            state.serialize_field("ignore", v)?;
        }
        if let Some(v) = &self.labels {
            state.serialize_field("labels", v)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    TsCreateInput,
    API_INFO,
    {key, retention, encoding, chunk_size, duplicate_policy, ignore, labels}
);

impl RedisCommandInput for TsCreateInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);
        if let Some(v) = &self.retention {
            command.arg("RETENTION").arg(v);
        }
        if let Some(v) = &self.encoding {
            v.cmd(&mut command);
        }
        if let Some(v) = &self.chunk_size {
            command.arg("CHUNK_SIZE").arg(v);
        }
        if let Some(v) = &self.duplicate_policy {
            command.arg("DUPLICATE_POLICY").arg(v);
        }
        if let Some(v) = &self.ignore {
            v.cmd(&mut command);
        }
        if let Some(v) = &self.labels {
            append_labels_to_cmd(v, &mut command);
        }
        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("TS.CREATE requires at least 1 argument (key)"));
        }

        let key = args[0].clone().try_into()?;
        let mut retention = None;
        let mut encoding = None;
        let mut chunk_size = None;
        let mut duplicate_policy = None;
        let mut ignore = None;
        let mut labels = None;
        let mut i = 1;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                match s.to_uppercase().as_str() {
                    "RETENTION" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("RETENTION requires a value"));
                        }
                        retention = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "ENCODING" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("ENCODING requires a value"));
                        }
                        if let RedisJsonValue::String(enc) = &args[i + 1] {
                            encoding = Some(TsEncoding::from_str(enc)?);
                        } else {
                            return Err(EpError::request("ENCODING value must be a string"));
                        }
                        i += 2;
                    }
                    "CHUNK_SIZE" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("CHUNK_SIZE requires a value"));
                        }
                        chunk_size = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "DUPLICATE_POLICY" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("DUPLICATE_POLICY requires a value"));
                        }
                        duplicate_policy = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "IGNORE" => {
                        if i + 2 >= args.len() {
                            return Err(EpError::request("IGNORE requires two values"));
                        }
                        ignore = Some(TsIgnore {
                            max_time_diff: args[i + 1].clone(),
                            max_val_diff: args[i + 2].clone(),
                        });
                        i += 3;
                    }
                    "LABELS" => {
                        i += 1;
                        let (parsed, new_i) = parse_labels_from_args(&args, i);
                        if !parsed.is_empty() {
                            labels = Some(parsed);
                        }
                        i = new_i;
                    }
                    _ => return Err(EpError::request(format!("Unknown TS.CREATE option: {}", s))),
                }
            } else {
                return Err(EpError::request("TS.CREATE options must be strings"));
            }
        }

        Ok(TsCreateInput {
            key,
            retention,
            encoding,
            chunk_size,
            duplicate_policy,
            ignore,
            labels,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::lib::time_series::common::TsOkOutput;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = TsCreateInput {
                key: RedisKey::String("ts:key".into()),
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: None,
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.CREATE"));
            assert!(cmd_str.contains("ts:key"));
        }

        #[test]
        fn test_encode_command_with_all_options() {
            let input = TsCreateInput {
                key: RedisKey::String("ts:full".into()),
                retention: Some(RedisJsonValue::Integer(3600000)),
                encoding: Some(TsEncoding::COMPRESSED),
                chunk_size: Some(RedisJsonValue::Integer(4096)),
                duplicate_policy: Some(RedisJsonValue::String("LAST".into())),
                ignore: Some(TsIgnore::new(500, 0.05)),
                labels: Some(vec![TsLabel::new("sensor", "temp")]),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("RETENTION"));
            assert!(cmd_str.contains("ENCODING"));
            assert!(cmd_str.contains("CHUNK_SIZE"));
            assert!(cmd_str.contains("DUPLICATE_POLICY"));
            assert!(cmd_str.contains("IGNORE"));
            assert!(cmd_str.contains("LABELS"));
        }

        #[test]
        fn test_decode_input_minimal() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = TsCreateInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_with_options() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("RETENTION".into()),
                RedisJsonValue::Integer(86400000),
                RedisJsonValue::String("ENCODING".into()),
                RedisJsonValue::String("COMPRESSED".into()),
            ];
            let input = TsCreateInput::decode(args).unwrap();
            assert!(input.retention.is_some());
            assert_eq!(input.encoding, Some(TsEncoding::COMPRESSED));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let err = TsCreateInput::decode(vec![]).unwrap_err();
            assert!(err.to_string().contains("requires at least 1"));
        }

        #[test]
        fn test_decode_input_missing_retention_value() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("RETENTION".into())];
            let err = TsCreateInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("RETENTION requires"));
        }

        #[test]
        fn test_decode_input_unknown_option() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("INVALID".into())];
            let err = TsCreateInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Unknown"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = TsOkOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success);
        }

        #[test]
        fn test_decode_output_error() {
            let err = TsOkOutput::decode(b"-ERR key exists\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = TsCreateInput {
                key: RedisKey::String("mykey".into()),
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: None,
                labels: None,
            };
            assert_eq!(input.keys().len(), 1);
        }

        #[test]
        fn test_kind() {
            let input = TsCreateInput {
                key: RedisKey::String("k".into()),
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: None,
                labels: None,
            };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::TsCreate);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_create_basic() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsCreateInput {
                                key: RedisKey::String("ts:create:test".into()),
                                retention: None,
                                encoding: None,
                                chunk_size: None,
                                duplicate_policy: None,
                                ignore: None,
                                labels: None,
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(bytes) => {
                            if bytes.starts_with(b"-") && String::from_utf8_lossy(&bytes).contains("unknown command") {
                                println!("TimeSeries module not available");
                                return;
                            }
                            let output = TsOkOutput::decode(&bytes).expect("decode failed");
                            assert!(output.success);
                        }
                        Err(e) => println!("Skipped: {}", e),
                    }
                })
            })
            .await;
        }
    }
}
