use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{RedisCommandOutput, RestoreResult};
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

const API_INFO: ApiInfo<RedisApi, RestoreInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Restore,
    "Creates a key from the serialized representation of a value",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `RESTORE`
/// https://redis.io/docs/latest/commands/restore/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
#[builder(default)]
pub struct RestoreInput {
    pub(crate) key: RedisKey,
    pub(crate) ttl: RedisJsonValue,
    pub(crate) serialized_value: Vec<u8>,
    #[builder(default)]
    pub(crate) replace: Option<bool>,
    #[builder(default)]
    pub(crate) absttl: Option<bool>,
    #[builder(default)]
    pub(crate) idletime: Option<RedisJsonValue>,
    #[builder(default)]
    pub(crate) freq: Option<RedisJsonValue>,
}

impl Default for RestoreInput {
    fn default() -> Self {
        Self {
            key: RedisKey::String(String::new()),
            ttl: RedisJsonValue::Integer(0),
            serialized_value: Vec::new(),
            replace: None,
            absttl: None,
            idletime: None,
            freq: None,
        }
    }
}

impl Serialize for RestoreInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4; // type, key, ttl, serialized_value
        if self.replace.is_some() {
            fields += 1;
        }
        if self.absttl.is_some() {
            fields += 1;
        }
        if self.idletime.is_some() {
            fields += 1;
        }
        if self.freq.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("RestoreInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("ttl", &self.ttl)?;
        state.serialize_field("serialized_value", &self.serialized_value)?;

        if let Some(replace) = &self.replace {
            state.serialize_field("replace", replace)?;
        }
        if let Some(absttl) = &self.absttl {
            state.serialize_field("absttl", absttl)?;
        }
        if let Some(idletime) = &self.idletime {
            state.serialize_field("idletime", idletime)?;
        }
        if let Some(freq) = &self.freq {
            state.serialize_field("freq", freq)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    RestoreInput,
    API_INFO,
    { key, ttl, serialized_value, replace, absttl, idletime, freq }
);

impl RedisCommandInput for RestoreInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.ttl).arg(&self.serialized_value);

        if let Some(replace) = &self.replace
            && *replace
        {
            command.arg("REPLACE");
        }

        if let Some(absttl) = &self.absttl
            && *absttl
        {
            command.arg("ABSTTL");
        }

        if let Some(idletime) = &self.idletime {
            command.arg("IDLETIME").arg(idletime);
        }

        if let Some(freq) = &self.freq {
            command.arg("FREQ").arg(freq);
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!("RESTORE requires at least 3 arguments, given {}", args.len())));
        }

        let serialized_value = match args[2].clone() {
            RedisJsonValue::Bytes(b) => b,
            RedisJsonValue::Array(arr) => {
                let mut bytes = vec![];

                for i in arr {
                    match i {
                        RedisJsonValue::Integer(i) => {
                            bytes.push(u8::try_from(i).map_err(|_| EpError::parse("serialized_value integers must be in 0..=255"))?)
                        }
                        _ => return Err(EpError::parse("expected u8 found other")),
                    }
                }

                bytes
            }
            _ => {
                return Err(EpError::parse("serialized_value must be bytes or an array of integers"));
            }
        };

        let mut replace = None;
        let mut absttl = None;
        let mut idletime = None;
        let mut freq = None;
        let mut i = 3;

        while i < args.len() {
            match &args[i] {
                RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                    "REPLACE" => {
                        replace = Some(true);
                        i += 1;
                    }
                    "ABSTTL" => {
                        absttl = Some(true);
                        i += 1;
                    }
                    "IDLETIME" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("IDLETIME requires a value"));
                        }
                        idletime = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "FREQ" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("FREQ requires a value"));
                        }
                        freq = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ => {
                        let _ctx = ctx_with_trace!().with_feature("redis");

                        log_warn!(_ctx, "Unknown RESTORE option: {}", audience = LogAudience::Internal, details = format!("{}", s));

                        i += 1;
                    }
                },
                _ => i += 1,
            }
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            ttl: args[1].clone(),
            serialized_value,
            replace,
            absttl,
            idletime,
            freq,
        })
    }
}

/// See official Redis documentation for `RESTORE`
/// https://redis.io/docs/latest/commands/restore/
#[derive(Debug, Clone, Deserialize, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct RestoreOutput {
    result: RestoreResult,
}

impl RestoreOutput {
    pub fn new(result: RestoreResult) -> Self {
        Self { result }
    }

    pub fn result(&self) -> &RestoreResult {
        &self.result
    }

    pub fn is_ok(&self) -> bool {
        matches!(self.result, RestoreResult::Ok)
    }

    pub fn is_busykey(&self) -> bool {
        matches!(self.result, RestoreResult::BusyKey(_))
    }
}

impl Serialize for RestoreOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("RestoreOutput", 1)?;
        state.serialize_field("result", &self.result)?;
        state.end()
    }
}

impl RedisCommandOutput for RestoreOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::Restore
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s == b"OK" => RestoreResult::Ok,
                Resp2Frame::Error(e) => classify_restore_error(&e),
                Resp2Frame::Integer(i) => {
                    if i == 1 {
                        RestoreResult::Ok
                    } else {
                        RestoreResult::Err(format!("Unexpected integer response: {}", i))
                    }
                }
                Resp2Frame::BulkString(s) => {
                    let response_str = String::from_utf8_lossy(&s).to_string();
                    if response_str == "OK" {
                        RestoreResult::Ok
                    } else {
                        RestoreResult::Err(format!("Unexpected bulk string: {}", response_str))
                    }
                }
                Resp2Frame::Null => RestoreResult::Err("Received null response".to_string()),
                _ => RestoreResult::Err(format!("Unexpected RESP2 frame type: {:?}", resp2_frame)),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data == b"OK" => RestoreResult::Ok,
                Resp3Frame::SimpleError { data, .. } => classify_restore_error(&data),
                Resp3Frame::BlobError { data, .. } => {
                    let error_msg = String::from_utf8_lossy(&data).to_string();
                    classify_restore_error(&error_msg)
                }
                Resp3Frame::Number { data, .. } => {
                    if data == 1 {
                        RestoreResult::Ok
                    } else {
                        RestoreResult::Err(format!("Unexpected number response: {}", data))
                    }
                }
                Resp3Frame::BlobString { data, .. } => {
                    let response_str = String::from_utf8_lossy(&data).to_string();
                    if response_str == "OK" {
                        RestoreResult::Ok
                    } else {
                        RestoreResult::Err(format!("Unexpected blob string: {}", response_str))
                    }
                }
                Resp3Frame::Null => RestoreResult::Err("Received null response".to_string()),
                _ => RestoreResult::Err(format!("Unexpected RESP3 frame type: {:?}", resp3_frame)),
            },
        };

        Ok(Self { result })
    }
}

/// Classify RESTORE error messages using Redis error naming conventions
///
/// Redis RESTORE errors:
/// - BUSYKEY: Target key name already exists
/// - ERR: Invalid TTL value, bad data format, invalid RDB, etc.
fn classify_restore_error(error_msg: &str) -> RestoreResult {
    let error_upper = error_msg.to_uppercase();

    if error_upper.contains("BUSYKEY") || error_upper.contains("TARGET KEY NAME ALREADY EXISTS") {
        return RestoreResult::BusyKey(error_msg.to_string());
    }

    if error_upper.contains("INVALID TTL") || error_upper.contains("ERR INVALID TTL") {
        return RestoreResult::InvalidTtl(error_msg.to_string());
    }

    if error_upper.contains("DUMP PAYLOAD")
        || error_upper.contains("BAD DATA FORMAT")
        || error_upper.contains("INVALID RDB")
        || error_upper.contains("CHECKSUM")
        || error_upper.contains("ERR DUMP PAYLOAD")
    {
        return RestoreResult::BadDataFormat(error_msg.to_string());
    }

    RestoreResult::Err(error_msg.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = RestoreInput {
                key: RedisKey::String("mykey".into()),
                ttl: RedisJsonValue::Integer(0),
                serialized_value: vec![0x00, 0x0c, 0x68, 0x65, 0x6c, 0x6c, 0x6f],
                ..Default::default()
            };
            let cmd = input.command();
            // Should produce: *4\r\n$7\r\nRESTORE\r\n$5\r\nmykey\r\n...
            assert!(cmd.starts_with(b"*4\r\n$7\r\nRESTORE\r\n"));
            assert!(cmd.windows(5).any(|w| w == b"mykey"));
        }

        #[test]
        fn test_encode_command_with_replace() {
            let input = RestoreInput {
                key: RedisKey::String("mykey".into()),
                ttl: RedisJsonValue::Integer(0),
                serialized_value: vec![0x00],
                replace: Some(true),
                ..Default::default()
            };
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"REPLACE"));
        }

        #[test]
        fn test_encode_command_with_absttl() {
            let input = RestoreInput {
                key: RedisKey::String("mykey".into()),
                ttl: RedisJsonValue::Integer(1000),
                serialized_value: vec![0x00],
                absttl: Some(true),
                ..Default::default()
            };
            let cmd = input.command();
            assert!(cmd.windows(6).any(|w| w == b"ABSTTL"));
        }

        #[test]
        fn test_encode_command_with_idletime() {
            let input = RestoreInput {
                key: RedisKey::String("mykey".into()),
                ttl: RedisJsonValue::Integer(0),
                serialized_value: vec![0x00],
                idletime: Some(RedisJsonValue::Integer(100)),
                ..Default::default()
            };
            let cmd = input.command();
            assert!(cmd.windows(8).any(|w| w == b"IDLETIME"));
        }

        #[test]
        fn test_encode_command_with_freq() {
            let input = RestoreInput {
                key: RedisKey::String("mykey".into()),
                ttl: RedisJsonValue::Integer(0),
                serialized_value: vec![0x00],
                freq: Some(RedisJsonValue::Integer(50)),
                ..Default::default()
            };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"FREQ"));
        }

        #[test]
        fn test_encode_command_all_options() {
            let input = RestoreInput {
                key: RedisKey::String("mykey".into()),
                ttl: RedisJsonValue::Integer(5000),
                serialized_value: vec![0x00, 0x01, 0x02],
                replace: Some(true),
                absttl: Some(true),
                idletime: Some(RedisJsonValue::Integer(100)),
                freq: Some(RedisJsonValue::Integer(50)),
            };
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"REPLACE"));
            assert!(cmd.windows(6).any(|w| w == b"ABSTTL"));
            assert!(cmd.windows(8).any(|w| w == b"IDLETIME"));
            assert!(cmd.windows(4).any(|w| w == b"FREQ"));
        }

        #[test]
        fn decode_accepts_bytes_payload() {
            let args = vec![
                RedisJsonValue::String("mykey".to_string()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Bytes(vec![0, 1, 2]),
            ];

            let restore = RestoreInput::decode(args).expect("decode should succeed");
            assert_eq!(restore.serialized_value, vec![0, 1, 2]);
        }

        #[test]
        fn decode_accepts_integer_array_payload() {
            let args = vec![
                RedisJsonValue::String("mykey".to_string()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Array(vec![RedisJsonValue::Integer(0), RedisJsonValue::Integer(255)]),
            ];

            let restore = RestoreInput::decode(args).expect("decode should succeed");
            assert_eq!(restore.serialized_value, vec![0, 255]);
        }

        #[test]
        fn decode_rejects_string_payload() {
            let args = vec![
                RedisJsonValue::String("mykey".to_string()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("abc".to_string()),
            ];

            assert!(RestoreInput::decode(args).is_err());
        }

        #[test]
        fn decode_rejects_out_of_range_integer() {
            let args = vec![
                RedisJsonValue::String("mykey".to_string()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Array(vec![RedisJsonValue::Integer(256)]),
            ];

            assert!(RestoreInput::decode(args).is_err());
        }

        #[test]
        fn test_decode_ok_simple_string() {
            let output = RestoreOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.result(), &RestoreResult::Ok);
        }

        #[test]
        fn test_decode_busykey_error() {
            let output = RestoreOutput::decode(b"-BUSYKEY Target key name already exists.\r\n").unwrap();
            assert!(output.is_busykey());
            assert!(matches!(output.result(), RestoreResult::BusyKey(_)));
        }

        #[test]
        fn test_decode_bad_data_format_error() {
            let output = RestoreOutput::decode(b"-ERR DUMP payload version or checksum are wrong\r\n").unwrap();
            assert!(matches!(output.result(), RestoreResult::BadDataFormat(_)));
        }

        #[test]
        fn test_decode_invalid_ttl_error() {
            let output = RestoreOutput::decode(b"-ERR invalid TTL value\r\n").unwrap();
            assert!(matches!(output.result(), RestoreResult::InvalidTtl(_)));
        }

        #[test]
        fn test_decode_generic_error() {
            let output = RestoreOutput::decode(b"-ERR some unknown error\r\n").unwrap();
            assert!(matches!(output.result(), RestoreResult::Err(_)));
        }

        #[test]
        fn test_classify_busykey_variations() {
            assert!(matches!(
                classify_restore_error("BUSYKEY Target key name already exists"),
                RestoreResult::BusyKey(_)
            ));
            assert!(matches!(classify_restore_error("target key name already exists"), RestoreResult::BusyKey(_)));
        }

        #[test]
        fn test_classify_bad_data_variations() {
            assert!(matches!(
                classify_restore_error("ERR DUMP payload version or checksum are wrong"),
                RestoreResult::BadDataFormat(_)
            ));
            assert!(matches!(classify_restore_error("bad data format"), RestoreResult::BadDataFormat(_)));
            assert!(matches!(classify_restore_error("invalid RDB version"), RestoreResult::BadDataFormat(_)));
        }

        #[test]
        fn test_default_impl() {
            let input = RestoreInput::default();
            assert!(input.serialized_value.is_empty());
            assert!(input.replace.is_none());
            assert!(input.absttl.is_none());
            assert!(input.idletime.is_none());
            assert!(input.freq.is_none());
        }

        #[test]
        fn test_serialization() {
            let input = RestoreInput {
                key: RedisKey::String("testkey".into()),
                ttl: RedisJsonValue::Integer(1000),
                serialized_value: vec![0x00, 0x01],
                replace: Some(true),
                ..Default::default()
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"type\":\"RESTORE\""));
            assert!(json.contains("\"key\":"));
            assert!(json.contains("\"replace\":true"));
        }

        #[test]
        fn test_output_serialization() {
            let output = RestoreOutput::new(RestoreResult::Ok);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"result\":\"Ok\""));
        }

        #[test]
        fn test_keys_method() {
            let input = RestoreInput { key: RedisKey::String("mykey".into()), ..Default::default() };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_kind_method() {
            let input = RestoreInput::default();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Restore);

            let output = RestoreOutput::new(RestoreResult::Ok);
            assert_eq!(output.kind(), RedisApi::Restore);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::api::lib::generic::dump::DumpInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_restore_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // First, set a value and dump it
                    ctx.write(SetInput {
                        key: RedisKey::String("source".into()),
                        value: RedisJsonValue::String("hello".into()),
                        ..Default::default()
                    })
                    .await;

                    // Get the dump
                    let dump_result = ctx.raw(&DumpInput { key: RedisKey::String("source".into()) }.command()).await.expect("raw failed");

                    // Parse the dump response to get serialized value
                    let (frame, _) = RedisProtocol::decode_buffer(&dump_result).expect("decode dump response");

                    let serialized = match frame {
                        DecoderRespFrame::Resp2(Resp2Frame::BulkString(data)) => data,
                        DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => data,
                        _ => panic!("unexpected dump response format"),
                    };

                    // Restore to a new key
                    let restore_result = ctx
                        .raw(
                            &RestoreInput {
                                key: RedisKey::String("dest".into()),
                                ttl: RedisJsonValue::Integer(0),
                                serialized_value: serialized,
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RestoreOutput::decode(&restore_result).expect("decode failed");
                    assert!(output.is_ok(), "RESTORE should succeed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_restore_busykey_error() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set up source and destination keys
                    ctx.write(SetInput {
                        key: RedisKey::String("source".into()),
                        value: RedisJsonValue::String("hello".into()),
                        ..Default::default()
                    })
                    .await;

                    ctx.write(SetInput {
                        key: RedisKey::String("existing".into()),
                        value: RedisJsonValue::String("already here".into()),
                        ..Default::default()
                    })
                    .await;

                    // Get the dump
                    let dump_result = ctx.raw(&DumpInput { key: RedisKey::String("source".into()) }.command()).await.expect("raw failed");

                    let (frame, _) = RedisProtocol::decode_buffer(&dump_result).expect("decode dump response");

                    let serialized = match frame {
                        DecoderRespFrame::Resp2(Resp2Frame::BulkString(data)) => data,
                        DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => data,
                        _ => panic!("unexpected dump response format"),
                    };

                    // Try to restore to existing key without REPLACE - should fail
                    let restore_result = ctx
                        .raw(
                            &RestoreInput {
                                key: RedisKey::String("existing".into()),
                                ttl: RedisJsonValue::Integer(0),
                                serialized_value: serialized,
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RestoreOutput::decode(&restore_result).expect("decode failed");
                    assert!(output.is_busykey(), "should return BUSYKEY error");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_restore_with_replace() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set up source and destination keys
                    ctx.write(SetInput {
                        key: RedisKey::String("source".into()),
                        value: RedisJsonValue::String("new value".into()),
                        ..Default::default()
                    })
                    .await;

                    ctx.write(SetInput {
                        key: RedisKey::String("existing".into()),
                        value: RedisJsonValue::String("old value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Get the dump
                    let dump_result = ctx.raw(&DumpInput { key: RedisKey::String("source".into()) }.command()).await.expect("raw failed");

                    let (frame, _) = RedisProtocol::decode_buffer(&dump_result).expect("decode dump response");

                    let serialized = match frame {
                        DecoderRespFrame::Resp2(Resp2Frame::BulkString(data)) => data,
                        DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => data,
                        _ => panic!("unexpected dump response format"),
                    };

                    // Restore with REPLACE - should succeed
                    let restore_result = ctx
                        .raw(
                            &RestoreInput {
                                key: RedisKey::String("existing".into()),
                                ttl: RedisJsonValue::Integer(0),
                                serialized_value: serialized,
                                replace: Some(true),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RestoreOutput::decode(&restore_result).expect("decode failed");
                    assert!(output.is_ok(), "RESTORE with REPLACE should succeed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_restore_bad_data() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let restore_result = ctx
                        .raw(
                            &RestoreInput {
                                key: RedisKey::String("badkey".into()),
                                ttl: RedisJsonValue::Integer(0),
                                serialized_value: vec![0x00, 0x01, 0x02, 0x03], // Invalid dump data
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await;

                    assert!(restore_result.is_err());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_restore_with_ttl() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("source".into()),
                        value: RedisJsonValue::String("hello".into()),
                        ..Default::default()
                    })
                    .await;

                    let dump_result = ctx.raw(&DumpInput { key: RedisKey::String("source".into()) }.command()).await.expect("raw failed");

                    let (frame, _) = RedisProtocol::decode_buffer(&dump_result).expect("decode dump response");

                    let serialized = match frame {
                        DecoderRespFrame::Resp2(Resp2Frame::BulkString(data)) => data,
                        DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => data,
                        _ => panic!("unexpected dump response format"),
                    };

                    // Restore with 10 second TTL
                    let restore_result = ctx
                        .raw(
                            &RestoreInput {
                                key: RedisKey::String("withttl".into()),
                                ttl: RedisJsonValue::Integer(10000), // 10 seconds in ms
                                serialized_value: serialized,
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RestoreOutput::decode(&restore_result).expect("decode failed");
                    assert!(output.is_ok(), "RESTORE with TTL should succeed");
                })
            })
            .await;
        }

        // ABSTTL was added in Redis 5.0
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_restore_with_absttl() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("source".into()),
                        value: RedisJsonValue::String("hello".into()),
                        ..Default::default()
                    })
                    .await;

                    let dump_result = ctx.raw(&DumpInput { key: RedisKey::String("source".into()) }.command()).await.expect("raw failed");

                    let (frame, _) = RedisProtocol::decode_buffer(&dump_result).expect("decode dump response");

                    let serialized = match frame {
                        DecoderRespFrame::Resp2(Resp2Frame::BulkString(data)) => data,
                        DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => data,
                        _ => panic!("unexpected dump response format"),
                    };

                    // Use absolute TTL (Unix timestamp in ms, far in future)
                    let future_timestamp = 2000000000000i64; // Year 2033
                    let restore_result = ctx
                        .raw(
                            &RestoreInput {
                                key: RedisKey::String("absttlkey".into()),
                                ttl: RedisJsonValue::Integer(future_timestamp),
                                serialized_value: serialized,
                                absttl: Some(true),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RestoreOutput::decode(&restore_result).expect("decode failed");
                    assert!(output.is_ok(), "RESTORE with ABSTTL should succeed");
                })
            })
            .await;
        }

        // IDLETIME and FREQ require Redis 5.0+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_restore_with_idletime() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("source".into()),
                        value: RedisJsonValue::String("hello".into()),
                        ..Default::default()
                    })
                    .await;

                    let dump_result = ctx.raw(&DumpInput { key: RedisKey::String("source".into()) }.command()).await.expect("raw failed");

                    let (frame, _) = RedisProtocol::decode_buffer(&dump_result).expect("decode dump response");

                    let serialized = match frame {
                        DecoderRespFrame::Resp2(Resp2Frame::BulkString(data)) => data,
                        DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => data,
                        _ => panic!("unexpected dump response format"),
                    };

                    let restore_result = ctx
                        .raw(
                            &RestoreInput {
                                key: RedisKey::String("idletimekey".into()),
                                ttl: RedisJsonValue::Integer(0),
                                serialized_value: serialized,
                                idletime: Some(RedisJsonValue::Integer(100)),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RestoreOutput::decode(&restore_result).expect("decode failed");
                    assert!(output.is_ok(), "RESTORE with IDLETIME should succeed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_restore_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("source".into()),
                value: RedisJsonValue::String("hello".into()),
                ..Default::default()
            })
            .await;

            let dump_result = ctx.raw(&DumpInput { key: RedisKey::String("source".into()) }.command()).await.expect("raw failed");

            let (frame, _) = RedisProtocol::decode_buffer(&dump_result).expect("decode dump response");

            let serialized = match frame {
                DecoderRespFrame::Resp2(Resp2Frame::BulkString(data)) => data,
                DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => data,
                other => panic!("expected bulk string, got {:?}", other),
            };

            let restore_result = ctx
                .raw(
                    &RestoreInput {
                        key: RedisKey::String("resp2dest".into()),
                        ttl: RedisJsonValue::Integer(0),
                        serialized_value: serialized,
                        ..Default::default()
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(restore_result.starts_with(b"+OK"), "RESP2 should return +OK simple string");

            let output = RestoreOutput::decode(&restore_result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_restore_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("source".into()),
                value: RedisJsonValue::String("hello".into()),
                ..Default::default()
            })
            .await;

            let dump_result = ctx.raw(&DumpInput { key: RedisKey::String("source".into()) }.command()).await.expect("raw failed");

            let (frame, _) = RedisProtocol::decode_buffer(&dump_result).expect("decode dump response");

            let serialized = match frame {
                DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => data,
                _ => panic!("expected RESP3 blob string"),
            };

            let restore_result = ctx
                .raw(
                    &RestoreInput {
                        key: RedisKey::String("resp3dest".into()),
                        ttl: RedisJsonValue::Integer(0),
                        serialized_value: serialized,
                        ..Default::default()
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(restore_result.starts_with(b"+OK"), "RESP3 should return +OK simple string");

            let output = RestoreOutput::decode(&restore_result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_restore_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set up multiple sources
                    ctx.write(SetInput {
                        key: RedisKey::String("src1".into()),
                        value: RedisJsonValue::String("val1".into()),
                        ..Default::default()
                    })
                    .await;

                    ctx.write(SetInput {
                        key: RedisKey::String("src2".into()),
                        value: RedisJsonValue::String("val2".into()),
                        ..Default::default()
                    })
                    .await;

                    // Dump both
                    let dump1 = ctx.raw(&DumpInput { key: RedisKey::String("src1".into()) }.command()).await.expect("raw failed");
                    let dump2 = ctx.raw(&DumpInput { key: RedisKey::String("src2".into()) }.command()).await.expect("raw failed");

                    let ser1 = match RedisProtocol::decode_buffer(&dump1).unwrap().0 {
                        DecoderRespFrame::Resp2(Resp2Frame::BulkString(d)) => d,
                        DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => data,
                        _ => panic!("unexpected"),
                    };
                    let ser2 = match RedisProtocol::decode_buffer(&dump2).unwrap().0 {
                        DecoderRespFrame::Resp2(Resp2Frame::BulkString(d)) => d,
                        DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => data,
                        _ => panic!("unexpected"),
                    };

                    // Pipeline restore
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &RestoreInput {
                            key: RedisKey::String("dst1".into()),
                            ttl: RedisJsonValue::Integer(0),
                            serialized_value: ser1,
                            ..Default::default()
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &RestoreInput {
                            key: RedisKey::String("dst2".into()),
                            ttl: RedisJsonValue::Integer(0),
                            serialized_value: ser2,
                            ..Default::default()
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = RestoreOutput::decode(responses[0]).expect("decode dst1");
                    assert!(out1.is_ok());

                    let out2 = RestoreOutput::decode(responses[1]).expect("decode dst2");
                    assert!(out2.is_ok());
                })
            })
            .await;
        }
    }
}
