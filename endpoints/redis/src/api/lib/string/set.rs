pub(crate) mod args;
pub(crate) mod examples;

use crate::api::lib::string::set::args::*;
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
use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, SetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Set,
    "Sets the string value of a key, ignoring its type. The key is created if it doesn't exist",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `SET`
/// https://redis.io/docs/latest/commands/set/
#[derive(Debug, Default, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SetInput {
    pub(crate) key: RedisKey,
    pub(crate) value: RedisJsonValue,
    pub(crate) rule: Option<Rule>,
    pub(crate) get: Option<bool>,
    pub(crate) options: Option<Options>,
}

impl<'de> Deserialize<'de> for SetInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SetInputVisitor;

        impl<'de> Visitor<'de> for SetInputVisitor {
            type Value = SetInput;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a SET command input object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut key: Option<RedisKey> = None;
                let mut value: Option<RedisJsonValue> = None;
                let mut rule: Option<Rule> = None;
                let mut get: Option<bool> = None;
                let mut options: Option<Options> = None;

                while let Some(field) = map.next_key::<String>()? {
                    match field.as_str() {
                        "type" | "kind" => {
                            let _: de::IgnoredAny = map.next_value()?;
                        }
                        "key" => {
                            key = Some(map.next_value()?);
                        }
                        "value" => {
                            value = Some(map.next_value()?);
                        }
                        "NX" => {
                            let _: de::IgnoredAny = map.next_value()?;
                            rule = Some(Rule::NX);
                        }
                        "XX" => {
                            let _: de::IgnoredAny = map.next_value()?;
                            rule = Some(Rule::XX);
                        }
                        "get" => {
                            get = Some(map.next_value()?);
                        }
                        "EX" => {
                            options = Some(Options::EX(map.next_value()?));
                        }
                        "PX" => {
                            options = Some(Options::PX(map.next_value()?));
                        }
                        "EXAT" => {
                            options = Some(Options::EXAT(map.next_value()?));
                        }
                        "PXAT" => {
                            options = Some(Options::PXAT(map.next_value()?));
                        }
                        "KEEPTTL" => {
                            let _: de::IgnoredAny = map.next_value()?;
                            options = Some(Options::KEEPTTL);
                        }
                        "options" => {
                            options = Some(map.next_value()?);
                        }
                        "rule" => {
                            rule = Some(map.next_value()?);
                        }
                        _ => {
                            let _: de::IgnoredAny = map.next_value()?;
                        }
                    }
                }

                Ok(SetInput {
                    key: key.unwrap_or_default(),
                    value: value.unwrap_or_default(),
                    rule,
                    get,
                    options,
                })
            }
        }

        deserializer.deserialize_map(SetInputVisitor)
    }
}

impl Serialize for SetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut field_count = 3;
        if self.rule.is_some() {
            field_count += 1;
        }
        if self.get.is_some() {
            field_count += 1;
        }
        if self.options.is_some() {
            field_count += 1;
        }

        let mut state = serializer.serialize_struct("SetInput", field_count)?;

        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("value", &self.value)?;

        if let Some(ref rule) = self.rule {
            match rule {
                Rule::NX => state.serialize_field("NX", &true)?,
                Rule::XX => state.serialize_field("XX", &true)?,
            }
        }

        if let Some(get) = self.get {
            state.serialize_field("get", &get)?;
        }

        if let Some(ref option) = self.options {
            match option {
                Options::EX(ex) => state.serialize_field("EX", &ex)?,
                Options::PX(px) => state.serialize_field("PX", &px)?,
                Options::EXAT(exat) => state.serialize_field("EXAT", &exat)?,
                Options::PXAT(pxat) => state.serialize_field("PXAT", &pxat)?,
                Options::KEEPTTL => state.serialize_field("KEEPTTL", &true)?,
            }
        }

        state.end()
    }
}

impl_redis_operation!(
    SetInput,
    API_INFO,
    {key, value, rule, get, options}
);

impl RedisCommandInput for SetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.value);

        if let Some(rule) = &self.rule {
            match rule {
                Rule::NX => command.arg("NX"),
                Rule::XX => command.arg("XX"),
            };
        }

        if let Some(get) = &self.get
            && *get
        {
            command.arg("GET");
        }

        if let Some(options) = &self.options {
            match options {
                Options::EX(e) => command.arg("EX").arg(&e.seconds),
                Options::PX(m) => command.arg("PX").arg(&m.milliseconds),
                Options::EXAT(e) => command.arg("EXAT").arg(&e.unix_time_seconds),
                Options::PXAT(p) => command.arg("PXAT").arg(&p.unix_time_milliseconds),
                Options::KEEPTTL => command.arg("KEEPTTL"),
            };
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("SET requires 2 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let value = args[1].clone();
        let mut rule = None;
        let mut get = None;
        let mut options = None;

        let mut i = 2;
        while i < args.len() {
            match &args[i] {
                RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                    "NX" => {
                        rule = Some(Rule::NX);
                        i += 1;
                    }
                    "XX" => {
                        rule = Some(Rule::XX);
                        i += 1;
                    }
                    "GET" => {
                        get = Some(true);
                        i += 1;
                    }
                    "EX" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("EX requires a value"));
                        }
                        options = Some(Options::EX(EX { seconds: args[i + 1].clone() }));
                        i += 2;
                    }
                    "PX" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("PX requires a value"));
                        }
                        options = Some(Options::PX(PX { milliseconds: args[i + 1].clone() }));
                        i += 2;
                    }
                    "EXAT" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("EXAT requires a value"));
                        }
                        options = Some(Options::EXAT(EXAT { unix_time_seconds: args[i + 1].clone() }));
                        i += 2;
                    }
                    "PXAT" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("PXAT requires a value"));
                        }
                        options = Some(Options::PXAT(PXAT { unix_time_milliseconds: args[i + 1].clone() }));
                        i += 2;
                    }
                    "KEEPTTL" => {
                        options = Some(Options::KEEPTTL);
                        i += 1;
                    }
                    _ => {
                        return Err(EpError::request(format!("Unknown SET option: {}", s)));
                    }
                },
                _ => {
                    return Err(EpError::request("SET options must be strings"));
                }
            }
        }

        Ok(SetInput { key, value, rule, get, options })
    }
}

/// See official Redis documentation for `SET`
/// https://redis.io/docs/latest/commands/set/
#[derive(Debug, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub enum SetResult {
    Ok,
    Nil,
    PreviousValue(RedisJsonValue),
}

#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SetOutput {
    result: SetResult,
}

impl SetOutput {
    pub fn new(result: SetResult) -> Self {
        Self { result }
    }

    pub fn result(&self) -> &SetResult {
        &self.result
    }

    pub fn was_set(&self) -> bool {
        !matches!(self.result, SetResult::Nil)
    }

    pub fn is_ok(&self) -> bool {
        matches!(self.result, SetResult::Ok)
    }

    pub fn is_nil(&self) -> bool {
        matches!(self.result, SetResult::Nil)
    }

    pub fn previous_value(&self) -> Option<&RedisJsonValue> {
        match &self.result {
            SetResult::PreviousValue(v) => Some(v),
            _ => None,
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s.as_slice() == b"OK" => SetResult::Ok,
                Resp2Frame::Null => SetResult::Nil,
                Resp2Frame::BulkString(bytes) => SetResult::PreviousValue(RedisJsonValue::from(bytes)),
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                _ => return Err(EpError::parse("unexpected SET response type")),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data.as_slice() == b"OK" => SetResult::Ok,
                Resp3Frame::Null => SetResult::Nil,
                Resp3Frame::BlobString { data, .. } => SetResult::PreviousValue(RedisJsonValue::from(data)),
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                _ => return Err(EpError::parse("unexpected SET response type")),
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for SetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SetOutput", 1)?;
        state.serialize_field("result", &self.result)?;
        state.end()
    }
}

impl Serialize for SetResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            SetResult::Ok => serializer.serialize_str("OK"),
            SetResult::Nil => serializer.serialize_none(),
            SetResult::PreviousValue(v) => v.serialize(serializer),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = SetInput {
                key: RedisKey::String("mykey".into()),
                value: RedisJsonValue::String("myvalue".into()),
                rule: None,
                get: None,
                options: None,
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n");
        }

        #[test]
        fn test_encode_command_with_nx() {
            let input = SetInput {
                key: RedisKey::String("key".into()),
                value: RedisJsonValue::String("val".into()),
                rule: Some(Rule::NX),
                get: None,
                options: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(2).any(|w| w == b"NX"));
        }

        #[test]
        fn test_encode_command_with_xx() {
            let input = SetInput {
                key: RedisKey::String("key".into()),
                value: RedisJsonValue::String("val".into()),
                rule: Some(Rule::XX),
                get: None,
                options: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(2).any(|w| w == b"XX"));
        }

        #[test]
        fn test_encode_command_with_get() {
            let input = SetInput {
                key: RedisKey::String("key".into()),
                value: RedisJsonValue::String("val".into()),
                rule: None,
                get: Some(true),
                options: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(3).any(|w| w == b"GET"));
        }

        #[test]
        fn test_encode_command_with_ex() {
            let input = SetInput {
                key: RedisKey::String("key".into()),
                value: RedisJsonValue::String("val".into()),
                rule: None,
                get: None,
                options: Some(Options::EX(EX { seconds: RedisJsonValue::Integer(60) })),
            };
            let cmd = input.command();
            assert!(cmd.windows(2).any(|w| w == b"EX"));
        }

        #[test]
        fn test_encode_command_with_px() {
            let input = SetInput {
                key: RedisKey::String("key".into()),
                value: RedisJsonValue::String("val".into()),
                rule: None,
                get: None,
                options: Some(Options::PX(PX { milliseconds: RedisJsonValue::Integer(5000) })),
            };
            let cmd = input.command();
            assert!(cmd.windows(2).any(|w| w == b"PX"));
        }

        #[test]
        fn test_encode_command_with_keepttl() {
            let input = SetInput {
                key: RedisKey::String("key".into()),
                value: RedisJsonValue::String("val".into()),
                rule: None,
                get: None,
                options: Some(Options::KEEPTTL),
            };
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"KEEPTTL"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = SetOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert!(output.was_set());
            assert!(!output.is_nil());
            assert_eq!(output.result(), &SetResult::Ok);
        }

        #[test]
        fn test_decode_nil_response() {
            // RESP2 null bulk string
            let output = SetOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_nil());
            assert!(!output.was_set());
            assert!(!output.is_ok());
        }

        #[test]
        fn test_decode_previous_value() {
            // RESP2 bulk string with previous value
            let output = SetOutput::decode(b"$8\r\noldvalue\r\n").unwrap();
            assert!(output.was_set());
            assert!(!output.is_nil());
            assert!(output.previous_value().is_some());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = SetOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("value".into())];
            let input = SetInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.value, RedisJsonValue::String("value".into()));
            assert!(input.rule.is_none());
            assert!(input.get.is_none());
            assert!(input.options.is_none());
        }

        #[test]
        fn test_decode_input_with_nx() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("value".into()),
                RedisJsonValue::String("NX".into()),
            ];
            let input = SetInput::decode(args).unwrap();
            assert!(matches!(input.rule, Some(Rule::NX)));
        }

        #[test]
        fn test_decode_input_with_xx() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("value".into()),
                RedisJsonValue::String("XX".into()),
            ];
            let input = SetInput::decode(args).unwrap();
            assert!(matches!(input.rule, Some(Rule::XX)));
        }

        #[test]
        fn test_decode_input_with_get() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("value".into()),
                RedisJsonValue::String("GET".into()),
            ];
            let input = SetInput::decode(args).unwrap();
            assert_eq!(input.get, Some(true));
        }

        #[test]
        fn test_decode_input_with_ex() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("value".into()),
                RedisJsonValue::String("EX".into()),
                RedisJsonValue::Integer(60),
            ];
            let input = SetInput::decode(args).unwrap();
            assert!(matches!(input.options, Some(Options::EX(_))));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = SetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_unknown_option() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("value".into()),
                RedisJsonValue::String("UNKNOWN".into()),
            ];
            let err = SetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Unknown SET option"));
        }

        #[test]
        fn test_decode_input_ex_missing_value() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("value".into()),
                RedisJsonValue::String("EX".into()),
            ];
            let err = SetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("EX requires a value"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = SetInput {
                key: RedisKey::String("mykey".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_serialization_nx_field_name() {
            let input = SetInput {
                key: RedisKey::String("k".into()),
                value: RedisJsonValue::String("v".into()),
                rule: Some(Rule::NX),
                get: None,
                options: None,
            };
            let serialized = serde_json::to_string(&input).unwrap();
            assert!(serialized.contains("\"NX\":true"));
            assert!(!serialized.contains("\"NS\""));
        }

        #[test]
        fn test_serialization_xx_field_name() {
            let input = SetInput {
                key: RedisKey::String("k".into()),
                value: RedisJsonValue::String("v".into()),
                rule: Some(Rule::XX),
                get: None,
                options: None,
            };
            let serialized = serde_json::to_string(&input).unwrap();
            assert!(serialized.contains("\"XX\":true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::TtlInput;
        use crate::api::get::GetOutput;
        use crate::api::{GetInput, TtlOutput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_set_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &SetInput {
                                key: RedisKey::String("test_key".into()),
                                value: RedisJsonValue::String("test_value".into()),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());
                    assert!(output.was_set());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_set_and_get() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("sg_key".into()),
                            value: RedisJsonValue::String("sg_value".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let get_result = ctx.raw(&GetInput { key: RedisKey::String("sg_key".into()) }.command()).await.expect("raw failed");

                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert!(get_output.exists());
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("sg_value")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_set_nx_new_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &SetInput {
                                key: RedisKey::String("nx_new".into()),
                                value: RedisJsonValue::String("value".into()),
                                rule: Some(Rule::NX),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok(), "NX on new key should succeed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_set_nx_existing_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // First, create the key
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("nx_existing".into()),
                            value: RedisJsonValue::String("original".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Try to SET with NX - should fail (return nil)
                    let result = ctx
                        .raw(
                            &SetInput {
                                key: RedisKey::String("nx_existing".into()),
                                value: RedisJsonValue::String("new_value".into()),
                                rule: Some(Rule::NX),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_nil(), "NX on existing key should return nil");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_set_xx_existing_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // First, create the key
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("xx_existing".into()),
                            value: RedisJsonValue::String("original".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // SET with XX - should succeed
                    let result = ctx
                        .raw(
                            &SetInput {
                                key: RedisKey::String("xx_existing".into()),
                                value: RedisJsonValue::String("updated".into()),
                                rule: Some(Rule::XX),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok(), "XX on existing key should succeed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_set_xx_new_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &SetInput {
                                key: RedisKey::String("xx_new".into()),
                                value: RedisJsonValue::String("value".into()),
                                rule: Some(Rule::XX),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_nil(), "XX on new key should return nil");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_set_with_get_existing() {
            // GET option for SET requires Redis 6.2.0+
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    // Create key with initial value
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("get_existing".into()),
                            value: RedisJsonValue::String("old_value".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // SET with GET
                    let result = ctx
                        .raw(
                            &SetInput {
                                key: RedisKey::String("get_existing".into()),
                                value: RedisJsonValue::String("new_value".into()),
                                get: Some(true),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set());
                    assert!(output.previous_value().is_some());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_set_with_get_new_key() {
            // GET option for SET requires Redis 6.2.0+
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &SetInput {
                                key: RedisKey::String("get_new".into()),
                                value: RedisJsonValue::String("value".into()),
                                get: Some(true),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetOutput::decode(&result).expect("decode failed");
                    // GET on new key returns nil but key is still set
                    assert!(output.is_nil());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_set_with_ex() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("ex_key".into()),
                            value: RedisJsonValue::String("ex_value".into()),
                            options: Some(Options::EX(EX { seconds: RedisJsonValue::Integer(300) })),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Check TTL
                    let ttl_result = ctx.raw(&TtlInput { key: RedisKey::String("ex_key".into()) }.command()).await.expect("raw failed");

                    let ttl_output = TtlOutput::decode(&ttl_result).expect("decode failed");
                    assert!(ttl_output.has_expiration());
                    let seconds = ttl_output.seconds().expect("should have TTL");
                    assert!(seconds > 0 && seconds <= 300);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_set_with_px() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("px_key".into()),
                            value: RedisJsonValue::String("px_value".into()),
                            options: Some(Options::PX(PX { milliseconds: RedisJsonValue::Integer(60000) })),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let ttl_result = ctx.raw(&TtlInput { key: RedisKey::String("px_key".into()) }.command()).await.expect("raw failed");

                    let ttl_output = TtlOutput::decode(&ttl_result).expect("decode failed");
                    assert!(ttl_output.has_expiration());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_set_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &SetInput {
                            key: RedisKey::String("pipe1".into()),
                            value: RedisJsonValue::String("v1".into()),
                            ..Default::default()
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &SetInput {
                            key: RedisKey::String("pipe2".into()),
                            value: RedisJsonValue::String("v2".into()),
                            ..Default::default()
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(&GetInput { key: RedisKey::String("pipe1".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = SetOutput::decode(responses[0]).expect("decode set1");
                    assert!(out1.is_ok());

                    let out2 = SetOutput::decode(responses[1]).expect("decode set2");
                    assert!(out2.is_ok());

                    let get_out = GetOutput::decode(responses[2]).expect("decode get");
                    assert_eq!(get_out.value(), Some(&RedisJsonValue::from("v1")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_set_resp2_ok_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx
                .raw(
                    &SetInput {
                        key: RedisKey::String("r2key".into()),
                        value: RedisJsonValue::String("r2val".into()),
                        ..Default::default()
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string OK format");
            let output = SetOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_set_resp3_ok_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx
                .raw(
                    &SetInput {
                        key: RedisKey::String("r3key".into()),
                        value: RedisJsonValue::String("r3val".into()),
                        ..Default::default()
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP3 simple string OK format");
            let output = SetOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }
    }
}
