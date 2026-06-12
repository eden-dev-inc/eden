use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{BfInfoArg, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, BfInfoInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::BfInfo, "Returns information about a Bloom Filter", ReqType::Read, true);

/// See official Redis documentation for `BF.INFO`
/// https://redis.io/docs/latest/commands/bf.info/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BfInfoInput {
    pub(crate) key: RedisKey,
    pub(crate) option: Option<BfInfoArg>,
}

impl Serialize for BfInfoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.option.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("BfInfoInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(option) = &self.option {
            state.serialize_field("option", &option)?;
        }
        state.end()
    }
}

impl_redis_operation!(BfInfoInput, API_INFO, { key, option });

impl RedisCommandInput for BfInfoInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(opt) = &self.option {
            match opt {
                BfInfoArg::CAPACITY => command.arg("CAPACITY"),
                BfInfoArg::SIZE => command.arg("SIZE"),
                BfInfoArg::FILTERS => command.arg("FILTERS"),
                BfInfoArg::ITEMS => command.arg("ITEMS"),
                BfInfoArg::EXPANSION => command.arg("EXPANSION"),
            };
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("BF.INFO requires at least 1 argument, given none"));
        }

        if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "BF.INFO expects at most 2 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let option = if args.len() > 1 {
            Some(BfInfoArg::try_from(args[1].clone())?)
        } else {
            None
        };

        Ok(Self { key: args[0].clone().try_into()?, option })
    }
}

/// Output for Redis BF.INFO command
///
/// Returns information about a Bloom filter. When called without an option,
/// returns all information as a map/array.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BfInfoOutput {
    /// Array response with a single item
    pub singleton: Option<i64>,
    /// Capacity (when queried with CAPACITY option or full info)
    pub capacity: Option<i64>,
    /// Size in bytes (when queried with SIZE option or full info)
    pub size: Option<i64>,
    /// Number of filters (when queried with FILTERS option or full info)
    pub filters: Option<i64>,
    /// Number of items inserted (when queried with ITEMS option or full info)
    pub items: Option<i64>,
    /// Expansion rate (when queried with EXPANSION option or full info)
    pub expansion: Option<i64>,
}

impl BfInfoOutput {
    pub fn new() -> Self {
        Self {
            singleton: None,
            capacity: None,
            size: None,
            filters: None,
            items: None,
            expansion: None,
        }
    }

    /// Decode the Redis protocol response into a BfInfoOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame),
        }
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Self, EpError> {
        match frame {
            // Full info response as array
            Resp2Frame::Array(arr) => Self::parse_info_array(&arr),
            // Single value response (when specific option requested)
            Resp2Frame::Integer(i) => Ok(Self {
                singleton: Some(i),
                capacity: None,
                size: None,
                filters: None,
                items: None,
                expansion: None,
            }),
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected BF.INFO response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            // Full info as map or array
            Resp3Frame::Map { data, .. } => Self::parse_info_map(&data),
            Resp3Frame::Array { data, .. } => Self::parse_info_array_resp3(&data),
            // Single value response
            Resp3Frame::Number { data, .. } => Ok(Self {
                singleton: Some(data),
                capacity: None,
                size: None,
                filters: None,
                items: None,
                expansion: None,
            }),
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected BF.INFO response: {:?}", other))),
        }
    }

    fn parse_info_array(arr: &[Resp2Frame]) -> Result<Self, EpError> {
        let mut output = Self::new();

        if arr.len() == 1
            && let Resp2Frame::Integer(i) = &arr[0]
        {
            output.singleton = Some(*i);
            return Ok(output);
        }

        // Parse key-value pairs from array [key1, val1, key2, val2, ...]
        let mut i = 0;
        while i + 1 < arr.len() {
            let key = match &arr[i] {
                Resp2Frame::SimpleString(s) => String::from_utf8_lossy(s).to_uppercase(),
                Resp2Frame::BulkString(s) => String::from_utf8_lossy(s).to_uppercase(),
                _ => {
                    i += 2;
                    continue;
                }
            };

            let value = match &arr[i + 1] {
                Resp2Frame::Integer(v) => *v,
                _ => {
                    i += 2;
                    continue;
                }
            };

            match key.as_str() {
                "CAPACITY" => output.capacity = Some(value),
                "SIZE" => output.size = Some(value),
                "FILTERS" | "NUMBER OF FILTERS" => output.filters = Some(value),
                "ITEMS" | "NUMBER OF ITEMS INSERTED" => output.items = Some(value),
                "EXPANSION" | "EXPANSION RATE" => output.expansion = Some(value),
                _ => {}
            }

            i += 2;
        }

        Ok(output)
    }

    fn parse_info_map<K, V>(map: &std::collections::HashMap<K, V>) -> Result<Self, EpError>
    where
        K: std::borrow::Borrow<Resp3Frame>,
        V: std::borrow::Borrow<Resp3Frame>,
    {
        let mut output = Self::new();

        for (key_frame, val_frame) in map.iter() {
            let key_frame = key_frame.borrow();
            let val_frame = val_frame.borrow();

            let key = match key_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8_lossy(data).to_uppercase(),
                Resp3Frame::BlobString { data, .. } => String::from_utf8_lossy(data).to_uppercase(),
                _ => continue,
            };

            let value = match val_frame {
                Resp3Frame::Number { data, .. } => *data,
                _ => continue,
            };

            match key.as_str() {
                "CAPACITY" => output.capacity = Some(value),
                "SIZE" => output.size = Some(value),
                "FILTERS" | "NUMBER OF FILTERS" => output.filters = Some(value),
                "ITEMS" | "NUMBER OF ITEMS INSERTED" => output.items = Some(value),
                "EXPANSION" | "EXPANSION RATE" => output.expansion = Some(value),
                _ => {}
            }
        }

        Ok(output)
    }

    fn parse_info_array_resp3(arr: &[Resp3Frame]) -> Result<Self, EpError> {
        let mut output = Self::new();

        if arr.len() == 1
            && let Resp3Frame::Number { data, .. } = &arr[0]
        {
            output.singleton = Some(*data);
            return Ok(output);
        }

        let mut i = 0;
        while i + 1 < arr.len() {
            let key = match &arr[i] {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8_lossy(data).to_uppercase(),
                Resp3Frame::BlobString { data, .. } => String::from_utf8_lossy(data).to_uppercase(),
                _ => {
                    i += 2;
                    continue;
                }
            };

            let value = match &arr[i + 1] {
                Resp3Frame::Number { data, .. } => *data,
                _ => {
                    i += 2;
                    continue;
                }
            };

            match key.as_str() {
                "CAPACITY" => output.capacity = Some(value),
                "SIZE" => output.size = Some(value),
                "FILTERS" | "NUMBER OF FILTERS" => output.filters = Some(value),
                "ITEMS" | "NUMBER OF ITEMS INSERTED" => output.items = Some(value),
                "EXPANSION" | "EXPANSION RATE" => output.expansion = Some(value),
                _ => {}
            }

            i += 2;
        }

        Ok(output)
    }
}

impl Default for BfInfoOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for BfInfoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut fields = 0;
        if self.capacity.is_some() {
            fields += 1;
        }
        if self.size.is_some() {
            fields += 1;
        }
        if self.filters.is_some() {
            fields += 1;
        }
        if self.items.is_some() {
            fields += 1;
        }
        if self.expansion.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("BfInfoOutput", fields)?;
        if let Some(v) = self.capacity {
            state.serialize_field("capacity", &v)?;
        }
        if let Some(v) = self.size {
            state.serialize_field("size", &v)?;
        }
        if let Some(v) = self.filters {
            state.serialize_field("filters", &v)?;
        }
        if let Some(v) = self.items {
            state.serialize_field("items", &v)?;
        }
        if let Some(v) = self.expansion {
            state.serialize_field("expansion", &v)?;
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
        fn test_encode_command_no_option() {
            let input = BfInfoInput { key: RedisKey::String("myfilter".into()), option: None };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*2\r\n$7\r\nBF.INFO\r\n"));
            assert!(cmd.windows(8).any(|w| w == b"myfilter"));
        }

        #[test]
        fn test_encode_command_with_option() {
            let input = BfInfoInput {
                key: RedisKey::String("myfilter".into()),
                option: Some(BfInfoArg::CAPACITY),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$7\r\nBF.INFO\r\n"));
        }

        #[test]
        fn test_decode_single_integer() {
            let output = BfInfoOutput::decode(b":1000\r\n").unwrap();
            assert_eq!(output.singleton, Some(1000));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = BfInfoOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_key_only() {
            let args = vec![RedisJsonValue::String("filter".into())];
            let input = BfInfoInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("filter".into()));
            assert!(input.option.is_none());
        }

        #[test]
        fn test_decode_input_with_option() {
            let args = vec![RedisJsonValue::String("filter".into()), RedisJsonValue::String("CAPACITY".into())];
            let input = BfInfoInput::decode(args).unwrap();
            assert_eq!(input.option, Some(BfInfoArg::CAPACITY));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = BfInfoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = BfInfoInput { key: RedisKey::String("testkey".into()), option: None };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
        }

        #[test]
        fn test_arg_try_from() {
            assert_eq!(BfInfoArg::try_from(RedisJsonValue::String("capacity".into())).unwrap(), BfInfoArg::CAPACITY);
            assert_eq!(BfInfoArg::try_from(RedisJsonValue::String("SIZE".into())).unwrap(), BfInfoArg::SIZE);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::bloom_filter::bf_reserve::BfReserveInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_info_after_reserve() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Create filter first
                    ctx.raw(
                        &BfReserveInput {
                            key: RedisKey::String("bf_info_test".into()),
                            error_rate: RedisJsonValue::String("0.01".into()),
                            capacity: RedisJsonValue::Integer(1000),
                            expansion: None,
                            non_scaling: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Get full info
                    let result = ctx
                        .raw(&BfInfoInput { key: RedisKey::String("bf_info_test".into()), option: None }.command())
                        .await
                        .expect("raw failed");

                    let output = BfInfoOutput::decode(&result).expect("decode failed");
                    assert!(output.capacity.is_some() || output.size.is_some());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_info_capacity_option() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &BfReserveInput {
                            key: RedisKey::String("bf_info_cap".into()),
                            error_rate: RedisJsonValue::String("0.01".into()),
                            capacity: RedisJsonValue::Integer(500),
                            expansion: None,
                            non_scaling: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &BfInfoInput {
                                key: RedisKey::String("bf_info_cap".into()),
                                option: Some(BfInfoArg::CAPACITY),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BfInfoOutput::decode(&result).expect("decode failed");
                    assert!(output.capacity.is_some() || output.singleton.is_some());
                })
            })
            .await;
        }
    }
}
