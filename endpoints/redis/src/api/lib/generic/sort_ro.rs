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

// Re-export shared types
use super::sort_common::parse_sort_options;
pub use super::sort_common::{SortLimit, SortOrder, SortRoOutput};

const API_INFO: ApiInfo<RedisApi, SortRoInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::SortRo,
    "Returns the sorted elements of a list, a set, or a sorted set. Read-only variant of SORT.",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `SORT_RO`
/// https://redis.io/docs/latest/commands/sort_ro/
///
/// Note: SORT_RO was introduced in Redis 7.0.0 as a read-only variant of SORT.
/// It does not support the STORE option.
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SortRoInput {
    pub(crate) key: RedisKey,
    pub(crate) by: Option<RedisJsonValue>,
    pub(crate) limit: Option<SortLimit>,
    pub(crate) get: Option<Vec<RedisJsonValue>>,
    pub(crate) ord: Option<SortOrder>,
    pub(crate) alpha: Option<bool>,
}

impl Serialize for SortRoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2; // type, key
        if self.by.is_some() {
            fields += 1;
        }
        if self.limit.is_some() {
            fields += 1;
        }
        if self.get.is_some() {
            fields += 1;
        }
        if self.ord.is_some() {
            fields += 1;
        }
        if self.alpha.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("SortRoInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;

        if let Some(by) = &self.by {
            state.serialize_field("by", by)?;
        }
        if let Some(limit) = &self.limit {
            state.serialize_field("limit", limit)?;
        }
        if let Some(get) = &self.get {
            state.serialize_field("get", get)?;
        }
        if let Some(ord) = &self.ord {
            state.serialize_field("ord", ord)?;
        }
        if let Some(alpha) = &self.alpha {
            state.serialize_field("alpha", alpha)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    SortRoInput,
    API_INFO,
    { key, by, limit, get, ord, alpha }
);

impl RedisCommandInput for SortRoInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(by) = &self.by {
            command.arg("BY").arg(by);
        }

        if let Some(limit) = &self.limit {
            command.arg("LIMIT").arg(&limit.offset).arg(&limit.count);
        }

        if let Some(get) = &self.get {
            for g in get {
                command.arg("GET").arg(g);
            }
        }

        if let Some(ord) = &self.ord {
            match ord {
                SortOrder::ASC => command.arg("ASC"),
                SortOrder::DESC => command.arg("DESC"),
            };
        }

        if let Some(alpha) = &self.alpha
            && *alpha
        {
            command.arg("ALPHA");
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("SORT_RO requires at least 1 argument"));
        }

        let key = args[0].clone().try_into()?;
        let (by, limit, get, ord, alpha, i) = parse_sort_options(&args, 1)?;

        // Check for any remaining unknown options
        if i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                return Err(EpError::request(format!("Unknown SORT_RO option: {}", s)));
            } else {
                return Err(EpError::request("SORT_RO options must be strings"));
            }
        }

        Ok(SortRoInput { key, by, limit, get, ord, alpha })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = SortRoInput {
                key: RedisKey::String("mylist".into()),
                by: None,
                limit: None,
                get: None,
                ord: None,
                alpha: None,
            };
            assert_eq!(input.command().to_vec(), b"*2\r\n$7\r\nSORT_RO\r\n$6\r\nmylist\r\n");
        }

        #[test]
        fn test_encode_command_desc_alpha() {
            let input = SortRoInput {
                key: RedisKey::String("mylist".into()),
                by: None,
                limit: None,
                get: None,
                ord: Some(SortOrder::DESC),
                alpha: Some(true),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SORT_RO"));
            assert!(cmd_str.contains("DESC"));
            assert!(cmd_str.contains("ALPHA"));
        }

        #[test]
        fn test_encode_command_with_limit() {
            let input = SortRoInput {
                key: RedisKey::String("mylist".into()),
                by: None,
                limit: Some(SortLimit {
                    offset: RedisJsonValue::Integer(0),
                    count: RedisJsonValue::Integer(10),
                }),
                get: None,
                ord: None,
                alpha: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("LIMIT"));
        }

        #[test]
        fn test_encode_command_with_by() {
            let input = SortRoInput {
                key: RedisKey::String("mylist".into()),
                by: Some(RedisJsonValue::String("weight_*".into())),
                limit: None,
                get: None,
                ord: None,
                alpha: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("BY"));
            assert!(cmd_str.contains("weight_*"));
        }

        #[test]
        fn test_encode_command_with_get() {
            let input = SortRoInput {
                key: RedisKey::String("mylist".into()),
                by: None,
                limit: None,
                get: Some(vec![RedisJsonValue::String("object_*".into()), RedisJsonValue::String("#".into())]),
                ord: None,
                alpha: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert_eq!(cmd_str.matches("GET").count(), 2);
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = SortRoInput {
                key: RedisKey::String("mylist".into()),
                by: None,
                limit: None,
                get: None,
                ord: None,
                alpha: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mylist".into()));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("mylist".into())];
            let input = SortRoInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mylist".into()));
            assert!(input.by.is_none());
        }

        #[test]
        fn test_decode_input_with_options() {
            let args = vec![
                RedisJsonValue::String("mylist".into()),
                RedisJsonValue::String("DESC".into()),
                RedisJsonValue::String("ALPHA".into()),
                RedisJsonValue::String("LIMIT".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(5),
            ];
            let input = SortRoInput::decode(args).unwrap();
            assert_eq!(input.ord, Some(SortOrder::DESC));
            assert_eq!(input.alpha, Some(true));
            assert!(input.limit.is_some());
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = SortRoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_decode_input_unknown_option_fails() {
            let args = vec![
                RedisJsonValue::String("mylist".into()),
                RedisJsonValue::String("STORE".into()), // STORE not valid for SORT_RO
                RedisJsonValue::String("dest".into()),
            ];
            let err = SortRoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Unknown SORT_RO option"));
        }

        #[test]
        fn test_decode_output_array() {
            let output = SortRoOutput::decode(b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.elements()[0], Some(RedisJsonValue::from("1")));
            assert_eq!(output.elements()[1], Some(RedisJsonValue::from("2")));
            assert_eq!(output.elements()[2], Some(RedisJsonValue::from("3")));
        }

        #[test]
        fn test_decode_output_empty_array() {
            let output = SortRoOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_output_with_nulls() {
            let output = SortRoOutput::decode(b"*2\r\n$1\r\na\r\n$-1\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.elements()[0], Some(RedisJsonValue::from("a")));
            assert_eq!(output.elements()[1], None);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::list::rpush::RpushInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        // SORT_RO requires Redis 7.0+
        const MIN_VERSION: &str = "7.0";

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_ro_basic_numeric() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("rolist".into()),
                            elements: vec![
                                RedisJsonValue::String("3".into()),
                                RedisJsonValue::String("1".into()),
                                RedisJsonValue::String("2".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &SortRoInput {
                                key: RedisKey::String("rolist".into()),
                                by: None,
                                limit: None,
                                get: None,
                                ord: None,
                                alpha: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SortRoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);
                    assert_eq!(output.elements()[0], Some(RedisJsonValue::from("1")));
                    assert_eq!(output.elements()[1], Some(RedisJsonValue::from("2")));
                    assert_eq!(output.elements()[2], Some(RedisJsonValue::from("3")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_ro_desc() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("rodesc".into()),
                            elements: vec![
                                RedisJsonValue::String("1".into()),
                                RedisJsonValue::String("3".into()),
                                RedisJsonValue::String("2".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &SortRoInput {
                                key: RedisKey::String("rodesc".into()),
                                by: None,
                                limit: None,
                                get: None,
                                ord: Some(SortOrder::DESC),
                                alpha: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SortRoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.elements()[0], Some(RedisJsonValue::from("3")));
                    assert_eq!(output.elements()[1], Some(RedisJsonValue::from("2")));
                    assert_eq!(output.elements()[2], Some(RedisJsonValue::from("1")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_ro_alpha() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("roalpha".into()),
                            elements: vec![
                                RedisJsonValue::String("banana".into()),
                                RedisJsonValue::String("apple".into()),
                                RedisJsonValue::String("cherry".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &SortRoInput {
                                key: RedisKey::String("roalpha".into()),
                                by: None,
                                limit: None,
                                get: None,
                                ord: None,
                                alpha: Some(true),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SortRoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.elements()[0], Some(RedisJsonValue::from("apple")));
                    assert_eq!(output.elements()[1], Some(RedisJsonValue::from("banana")));
                    assert_eq!(output.elements()[2], Some(RedisJsonValue::from("cherry")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_ro_limit() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("rolimit".into()),
                            elements: vec![
                                RedisJsonValue::String("5".into()),
                                RedisJsonValue::String("1".into()),
                                RedisJsonValue::String("4".into()),
                                RedisJsonValue::String("2".into()),
                                RedisJsonValue::String("3".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &SortRoInput {
                                key: RedisKey::String("rolimit".into()),
                                by: None,
                                limit: Some(SortLimit {
                                    offset: RedisJsonValue::Integer(1),
                                    count: RedisJsonValue::Integer(2),
                                }),
                                get: None,
                                ord: None,
                                alpha: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SortRoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.elements()[0], Some(RedisJsonValue::from("2")));
                    assert_eq!(output.elements()[1], Some(RedisJsonValue::from("3")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_ro_empty_list() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &SortRoInput {
                                key: RedisKey::String("roempty".into()),
                                by: None,
                                limit: None,
                                get: None,
                                ord: None,
                                alpha: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SortRoOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_ro_pipeline() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &RpushInput {
                            key: RedisKey::String("ropipe".into()),
                            elements: vec![
                                RedisJsonValue::String("2".into()),
                                RedisJsonValue::String("1".into()),
                                RedisJsonValue::String("3".into()),
                            ],
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &SortRoInput {
                            key: RedisKey::String("ropipe".into()),
                            by: None,
                            limit: None,
                            get: None,
                            ord: None,
                            alpha: None,
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &SortRoInput {
                            key: RedisKey::String("ropipe".into()),
                            by: None,
                            limit: None,
                            get: None,
                            ord: Some(SortOrder::DESC),
                            alpha: None,
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    // ASC sort
                    let asc = SortRoOutput::decode(responses[1]).expect("decode asc");
                    assert_eq!(asc.elements()[0], Some(RedisJsonValue::from("1")));

                    // DESC sort
                    let desc = SortRoOutput::decode(responses[2]).expect("decode desc");
                    assert_eq!(desc.elements()[0], Some(RedisJsonValue::from("3")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_ro_resp2_array_format() {
            for version in REDIS_VERSIONS {
                if version_is_earlier(MIN_VERSION, version) {
                    continue;
                }

                let mut ctx = setup(RespVersion::Resp2, Some(version)).await;

                ctx.raw(
                    &RpushInput {
                        key: RedisKey::String("r2rolist".into()),
                        elements: vec![RedisJsonValue::String("2".into()), RedisJsonValue::String("1".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

                let result = ctx
                    .raw(
                        &SortRoInput {
                            key: RedisKey::String("r2rolist".into()),
                            by: None,
                            limit: None,
                            get: None,
                            ord: None,
                            alpha: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                assert!(result.starts_with(b"*2\r\n"), "RESP2 array format");
                let output = SortRoOutput::decode(&result).expect("decode failed");
                assert_eq!(output.len(), 2);
                ctx.stop().await;
                break;
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_ro_resp3_array_format() {
            for version in REDIS_VERSIONS {
                if version_is_earlier(MIN_VERSION, version) {
                    continue;
                }

                let mut ctx = setup(RespVersion::Resp3, Some(version)).await;

                ctx.raw(
                    &RpushInput {
                        key: RedisKey::String("r3rolist".into()),
                        elements: vec![RedisJsonValue::String("2".into()), RedisJsonValue::String("1".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

                let result = ctx
                    .raw(
                        &SortRoInput {
                            key: RedisKey::String("r3rolist".into()),
                            by: None,
                            limit: None,
                            get: None,
                            ord: None,
                            alpha: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                assert!(result.starts_with(b"*2\r\n"), "RESP3 array format");
                let output = SortRoOutput::decode(&result).expect("decode failed");
                assert_eq!(output.len(), 2);
                ctx.stop().await;
                break;
            }
        }
    }
}
