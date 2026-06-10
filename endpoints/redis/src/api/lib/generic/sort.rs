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
pub use super::sort_common::{SortLimit, SortOrder, SortOutput};

const API_INFO: ApiInfo<RedisApi, SortInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Sort,
    "Sorts the elements in a list, a set, or a sorted set, optionally storing the result",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `SORT`
/// https://redis.io/docs/latest/commands/sort/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SortInput {
    pub(crate) key: RedisKey,
    pub(crate) by: Option<RedisJsonValue>,
    pub(crate) limit: Option<SortLimit>,
    pub(crate) get: Option<Vec<RedisJsonValue>>,
    pub(crate) ord: Option<SortOrder>,
    pub(crate) alpha: Option<bool>,
    pub(crate) store: Option<RedisKey>,
}

impl Serialize for SortInput {
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
        if self.store.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("SortInput", fields)?;
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
        if let Some(store) = &self.store {
            state.serialize_field("store", store)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    SortInput,
    API_INFO,
    { key, by, limit, get, ord, alpha, store }
);

impl RedisCommandInput for SortInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        let mut keys = vec![self.key.clone()];
        // STORE destination is also a key that gets written
        if let Some(store_key) = &self.store {
            keys.push(store_key.clone());
        }
        keys
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

        if let Some(store) = &self.store {
            command.arg("STORE").arg(store);
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("SORT requires at least 1 argument"));
        }

        let key = args[0].clone().try_into()?;
        let (by, limit, get, ord, alpha, mut i) = parse_sort_options(&args, 1)?;
        let mut store = None;

        // Handle STORE option (SORT-specific)
        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                match s.to_uppercase().as_str() {
                    "STORE" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("STORE requires a destination key"));
                        }
                        store = Some(args[i + 1].clone().try_into()?);
                        i += 2;
                    }
                    _ => {
                        return Err(EpError::request(format!("Unknown SORT option: {}", s)));
                    }
                }
            } else {
                return Err(EpError::request("SORT options must be strings"));
            }
        }

        Ok(SortInput { key, by, limit, get, ord, alpha, store })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = SortInput {
                key: RedisKey::String("mylist".into()),
                by: None,
                limit: None,
                get: None,
                ord: None,
                alpha: None,
                store: None,
            };
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nSORT\r\n$6\r\nmylist\r\n");
        }

        #[test]
        fn test_encode_command_desc_alpha() {
            let input = SortInput {
                key: RedisKey::String("mylist".into()),
                by: None,
                limit: None,
                get: None,
                ord: Some(SortOrder::DESC),
                alpha: Some(true),
                store: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("DESC"));
            assert!(cmd_str.contains("ALPHA"));
        }

        #[test]
        fn test_encode_command_with_limit() {
            let input = SortInput {
                key: RedisKey::String("mylist".into()),
                by: None,
                limit: Some(SortLimit {
                    offset: RedisJsonValue::Integer(0),
                    count: RedisJsonValue::Integer(10),
                }),
                get: None,
                ord: None,
                alpha: None,
                store: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("LIMIT"));
        }

        #[test]
        fn test_encode_command_with_by() {
            let input = SortInput {
                key: RedisKey::String("mylist".into()),
                by: Some(RedisJsonValue::String("weight_*".into())),
                limit: None,
                get: None,
                ord: None,
                alpha: None,
                store: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("BY"));
            assert!(cmd_str.contains("weight_*"));
        }

        #[test]
        fn test_encode_command_with_get() {
            let input = SortInput {
                key: RedisKey::String("mylist".into()),
                by: None,
                limit: None,
                get: Some(vec![RedisJsonValue::String("object_*".into()), RedisJsonValue::String("#".into())]),
                ord: None,
                alpha: None,
                store: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            // Two GET arguments
            assert_eq!(cmd_str.matches("GET").count(), 2);
        }

        #[test]
        fn test_encode_command_with_store() {
            let input = SortInput {
                key: RedisKey::String("mylist".into()),
                by: None,
                limit: None,
                get: None,
                ord: None,
                alpha: None,
                store: Some(RedisKey::String("destkey".into())),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("STORE"));
            assert!(cmd_str.contains("destkey"));
        }

        #[test]
        fn test_keys_without_store() {
            let input = SortInput {
                key: RedisKey::String("mylist".into()),
                by: None,
                limit: None,
                get: None,
                ord: None,
                alpha: None,
                store: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mylist".into()));
        }

        #[test]
        fn test_keys_with_store() {
            let input = SortInput {
                key: RedisKey::String("mylist".into()),
                by: None,
                limit: None,
                get: None,
                ord: None,
                alpha: None,
                store: Some(RedisKey::String("destkey".into())),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0], RedisKey::String("mylist".into()));
            assert_eq!(keys[1], RedisKey::String("destkey".into()));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("mylist".into())];
            let input = SortInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mylist".into()));
            assert!(input.by.is_none());
            assert!(input.store.is_none());
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
            let input = SortInput::decode(args).unwrap();
            assert_eq!(input.ord, Some(SortOrder::DESC));
            assert_eq!(input.alpha, Some(true));
            assert!(input.limit.is_some());
        }

        #[test]
        fn test_decode_input_with_store() {
            let args = vec![
                RedisJsonValue::String("mylist".into()),
                RedisJsonValue::String("STORE".into()),
                RedisJsonValue::String("dest".into()),
            ];
            let input = SortInput::decode(args).unwrap();
            assert_eq!(input.store, Some(RedisKey::String("dest".into())));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = SortInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_decode_input_store_missing_dest() {
            let args = vec![RedisJsonValue::String("mylist".into()), RedisJsonValue::String("STORE".into())];
            let err = SortInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("STORE requires"));
        }

        #[test]
        fn test_decode_output_array() {
            let output = SortOutput::decode(b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n").unwrap();
            assert!(!output.is_store_result());
            let elements = output.elements().unwrap();
            assert_eq!(elements.len(), 3);
        }

        #[test]
        fn test_decode_output_stored_count() {
            let output = SortOutput::decode(b":10\r\n").unwrap();
            assert!(output.is_store_result());
            assert_eq!(output.stored_count(), Some(10));
        }

        #[test]
        fn test_decode_output_empty_array() {
            let output = SortOutput::decode(b"*0\r\n").unwrap();
            assert!(!output.is_store_result());
            assert_eq!(output.elements().unwrap().len(), 0);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::RpushInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_basic_numeric() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create list with numeric values
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("sortlist".into()),
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
                            &SortInput {
                                key: RedisKey::String("sortlist".into()),
                                by: None,
                                limit: None,
                                get: None,
                                ord: None,
                                alpha: None,
                                store: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SortOutput::decode(&result).expect("decode failed");
                    let elements = output.elements().expect("should be elements");
                    assert_eq!(elements.len(), 3);
                    assert_eq!(elements[0], Some(RedisJsonValue::from("1")));
                    assert_eq!(elements[1], Some(RedisJsonValue::from("2")));
                    assert_eq!(elements[2], Some(RedisJsonValue::from("3")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_desc() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("desclist".into()),
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
                            &SortInput {
                                key: RedisKey::String("desclist".into()),
                                by: None,
                                limit: None,
                                get: None,
                                ord: Some(SortOrder::DESC),
                                alpha: None,
                                store: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SortOutput::decode(&result).expect("decode failed");
                    let elements = output.elements().expect("should be elements");
                    assert_eq!(elements[0], Some(RedisJsonValue::from("3")));
                    assert_eq!(elements[1], Some(RedisJsonValue::from("2")));
                    assert_eq!(elements[2], Some(RedisJsonValue::from("1")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_alpha() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("alphalist".into()),
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
                            &SortInput {
                                key: RedisKey::String("alphalist".into()),
                                by: None,
                                limit: None,
                                get: None,
                                ord: None,
                                alpha: Some(true),
                                store: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SortOutput::decode(&result).expect("decode failed");
                    let elements = output.elements().expect("should be elements");
                    assert_eq!(elements[0], Some(RedisJsonValue::from("apple")));
                    assert_eq!(elements[1], Some(RedisJsonValue::from("banana")));
                    assert_eq!(elements[2], Some(RedisJsonValue::from("cherry")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_limit() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("limitlist".into()),
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
                            &SortInput {
                                key: RedisKey::String("limitlist".into()),
                                by: None,
                                limit: Some(SortLimit {
                                    offset: RedisJsonValue::Integer(1),
                                    count: RedisJsonValue::Integer(2),
                                }),
                                get: None,
                                ord: None,
                                alpha: None,
                                store: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SortOutput::decode(&result).expect("decode failed");
                    let elements = output.elements().expect("should be elements");
                    // Sorted: 1,2,3,4,5 -> offset 1, count 2 -> [2,3]
                    assert_eq!(elements.len(), 2);
                    assert_eq!(elements[0], Some(RedisJsonValue::from("2")));
                    assert_eq!(elements[1], Some(RedisJsonValue::from("3")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_store() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("srclist".into()),
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
                            &SortInput {
                                key: RedisKey::String("srclist".into()),
                                by: None,
                                limit: None,
                                get: None,
                                ord: None,
                                alpha: None,
                                store: Some(RedisKey::String("destlist".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SortOutput::decode(&result).expect("decode failed");
                    assert!(output.is_store_result());
                    assert_eq!(output.stored_count(), Some(3));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_empty_list() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Sort a non-existent key (treated as empty list)
                    let result = ctx
                        .raw(
                            &SortInput {
                                key: RedisKey::String("emptylist".into()),
                                by: None,
                                limit: None,
                                get: None,
                                ord: None,
                                alpha: None,
                                store: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SortOutput::decode(&result).expect("decode failed");
                    let elements = output.elements().expect("should be elements");
                    assert_eq!(elements.len(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &RpushInput {
                            key: RedisKey::String("pipelist".into()),
                            elements: vec![
                                RedisJsonValue::String("2".into()),
                                RedisJsonValue::String("1".into()),
                                RedisJsonValue::String("3".into()),
                            ],
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &SortInput {
                            key: RedisKey::String("pipelist".into()),
                            by: None,
                            limit: None,
                            get: None,
                            ord: None,
                            alpha: None,
                            store: None,
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    // RPUSH returns count
                    let rpush_str = String::from_utf8_lossy(responses[0]);
                    assert!(rpush_str.contains("3"));

                    // SORT returns sorted array
                    let sort_output = SortOutput::decode(responses[1]).expect("decode sort");
                    let elements = sort_output.elements().expect("should be elements");
                    assert_eq!(elements.len(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_resp2_array_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(
                &RpushInput {
                    key: RedisKey::String("resp2sort".into()),
                    elements: vec![RedisJsonValue::String("2".into()), RedisJsonValue::String("1".into())],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &SortInput {
                        key: RedisKey::String("resp2sort".into()),
                        by: None,
                        limit: None,
                        get: None,
                        ord: None,
                        alpha: None,
                        store: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            // RESP2 array format: *2\r\n...
            assert!(result.starts_with(b"*2\r\n"), "RESP2 array format");
            let output = SortOutput::decode(&result).expect("decode failed");
            assert!(!output.is_store_result());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_resp3_array_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(
                &RpushInput {
                    key: RedisKey::String("resp3sort".into()),
                    elements: vec![RedisJsonValue::String("2".into()), RedisJsonValue::String("1".into())],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &SortInput {
                        key: RedisKey::String("resp3sort".into()),
                        by: None,
                        limit: None,
                        get: None,
                        ord: None,
                        alpha: None,
                        store: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            // RESP3 also uses *N\r\n for arrays
            assert!(result.starts_with(b"*2\r\n"), "RESP3 array format");
            let output = SortOutput::decode(&result).expect("decode failed");
            assert!(!output.is_store_result());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_resp2_integer_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(
                &RpushInput {
                    key: RedisKey::String("storesrc2".into()),
                    elements: vec![RedisJsonValue::String("1".into())],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &SortInput {
                        key: RedisKey::String("storesrc2".into()),
                        by: None,
                        limit: None,
                        get: None,
                        ord: None,
                        alpha: None,
                        store: Some(RedisKey::String("storedest2".into())),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            // RESP2 integer: :1\r\n
            assert!(result.starts_with(b":"), "RESP2 integer format");
            let output = SortOutput::decode(&result).expect("decode failed");
            assert!(output.is_store_result());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sort_resp3_integer_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(
                &RpushInput {
                    key: RedisKey::String("storesrc3".into()),
                    elements: vec![RedisJsonValue::String("1".into())],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &SortInput {
                        key: RedisKey::String("storesrc3".into()),
                        by: None,
                        limit: None,
                        get: None,
                        ord: None,
                        alpha: None,
                        store: Some(RedisKey::String("storedest3".into())),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            // RESP3 integer: :1\r\n
            assert!(result.starts_with(b":"), "RESP3 integer format");
            let output = SortOutput::decode(&result).expect("decode failed");
            assert!(output.is_store_result());
            ctx.stop().await;
        }
    }
}
