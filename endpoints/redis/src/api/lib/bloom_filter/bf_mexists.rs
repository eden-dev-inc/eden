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

const API_INFO: ApiInfo<RedisApi, BfMexistsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::BfMexists,
    "Checks whether one or more items exist in a Bloom Filter",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `BF.MEXISTS`
/// https://redis.io/docs/latest/commands/bf.mexists/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BfMexistsInput {
    pub(crate) key: RedisKey,
    pub(crate) items: Vec<RedisJsonValue>,
}

impl Serialize for BfMexistsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BfMexistsInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("items", &self.items)?;
        state.end()
    }
}

impl_redis_operation!(BfMexistsInput, API_INFO, { key, items });

impl RedisCommandInput for BfMexistsInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);
        for item in &self.items {
            command.arg(item);
        }
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse(format!(
                "BF.MEXISTS requires at least 2 arguments (key + items), given {}",
                args.len()
            )));
        }

        Ok(Self { key: args[0].clone().try_into()?, items: args[1..].to_vec() })
    }
}

/// Output for Redis BF.MEXISTS command
///
/// Returns an array of integers, where each integer indicates whether
/// the corresponding item may exist (1) or definitely does not exist (0).
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BfMexistsOutput {
    /// Results for each item: 1 if may exist, 0 if definitely not
    results: Vec<i64>,
}

impl BfMexistsOutput {
    pub fn new(results: Vec<i64>) -> Self {
        Self { results }
    }

    /// Get the results array
    pub fn results(&self) -> &[i64] {
        &self.results
    }

    /// Get result for a specific index
    pub fn get(&self, index: usize) -> Option<i64> {
        self.results.get(index).copied()
    }

    /// Check if item at index may exist
    pub fn may_exist(&self, index: usize) -> Option<bool> {
        self.results.get(index).map(|&r| r == 1)
    }

    /// Count how many items may exist
    pub fn existing_count(&self) -> usize {
        self.results.iter().filter(|&&r| r == 1).count()
    }

    /// Check if item at index definitely does not exist
    pub fn definitely_not_exists(&self, index: usize) -> Option<bool> {
        self.results.get(index).map(|&r| r == 0)
    }

    /// Decode the Redis protocol response into a BfMexistsOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let results = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut results = Vec::with_capacity(arr.len());
                    for item in arr {
                        match item {
                            Resp2Frame::Integer(i) => results.push(i),
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in BF.MEXISTS response: {:?}", other)));
                            }
                        }
                    }
                    results
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected BF.MEXISTS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut results = Vec::with_capacity(data.len());
                    for item in data {
                        match item {
                            Resp3Frame::Boolean { data, .. } => results.push(data as i64),
                            Resp3Frame::Number { data, .. } => results.push(data),
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in BF.MEXISTS response: {:?}", other)));
                            }
                        }
                    }
                    results
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected BF.MEXISTS response: {:?}", other)));
                }
            },
        };

        Ok(Self { results })
    }
}

impl Serialize for BfMexistsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BfMexistsOutput", 1)?;
        state.serialize_field("results", &self.results)?;
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
            let input = BfMexistsInput {
                key: RedisKey::String("myfilter".into()),
                items: vec![RedisJsonValue::String("item1".into()), RedisJsonValue::String("item2".into())],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$10\r\nBF.MEXISTS\r\n"));
        }

        #[test]
        fn test_decode_array_response() {
            let output = BfMexistsOutput::decode(b"*2\r\n:1\r\n:0\r\n").unwrap();
            assert_eq!(output.results(), &[1, 0]);
            assert_eq!(output.may_exist(0), Some(true));
            assert_eq!(output.may_exist(1), Some(false));
            assert_eq!(output.definitely_not_exists(1), Some(true));
            assert_eq!(output.existing_count(), 1);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = BfMexistsOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("filter".into()),
                RedisJsonValue::String("item1".into()),
                RedisJsonValue::String("item2".into()),
            ];
            let input = BfMexistsInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("filter".into()));
            assert_eq!(input.items.len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("filter".into())];
            let err = BfMexistsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = BfMexistsInput {
                key: RedisKey::String("testkey".into()),
                items: vec![RedisJsonValue::String("item".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::bloom_filter::bf_madd::BfMaddInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_mexists_nonexistent() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &BfMexistsInput {
                                key: RedisKey::String("bf_mexists_missing".into()),
                                items: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BfMexistsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.existing_count(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_mexists_after_madd() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Add some items
                    ctx.raw(
                        &BfMaddInput {
                            key: RedisKey::String("bf_mexists_test".into()),
                            items: vec![RedisJsonValue::String("exists1".into()), RedisJsonValue::String("exists2".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Check mix of existing and non-existing
                    let result = ctx
                        .raw(
                            &BfMexistsInput {
                                key: RedisKey::String("bf_mexists_test".into()),
                                items: vec![
                                    RedisJsonValue::String("exists1".into()),
                                    RedisJsonValue::String("missing".into()),
                                    RedisJsonValue::String("exists2".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BfMexistsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.may_exist(0), Some(true));
                    assert_eq!(output.may_exist(1), Some(false));
                    assert_eq!(output.may_exist(2), Some(true));
                })
            })
            .await;
        }
    }
}
