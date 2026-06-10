use crate::api::lib::sorted_set::common::Aggregate;
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

const API_INFO: ApiInfo<RedisApi, ZunionstoreInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zunionstore,
    "Stores the union of multiple sorted sets in a key",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `ZUNIONSTORE`
/// https://redis.io/docs/latest/commands/zunionstore/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZunionstoreInput {
    destination: RedisKey,
    numkeys: RedisJsonValue,
    keys: Vec<RedisKey>,
    weights: Option<Vec<RedisJsonValue>>,
    aggregate: Option<Aggregate>,
}

impl ZunionstoreInput {
    pub fn new(destination: impl Into<RedisKey>, keys: Vec<impl Into<RedisKey>>) -> Self {
        let keys: Vec<RedisKey> = keys.into_iter().map(|k| k.into()).collect();
        let numkeys = RedisJsonValue::Integer(keys.len() as i64);
        Self {
            destination: destination.into(),
            numkeys,
            keys,
            weights: None,
            aggregate: None,
        }
    }

    pub fn with_weights(mut self, weights: Vec<impl Into<RedisJsonValue>>) -> Self {
        self.weights = Some(weights.into_iter().map(|w| w.into()).collect());
        self
    }

    pub fn with_aggregate(mut self, aggregate: Aggregate) -> Self {
        self.aggregate = Some(aggregate);
        self
    }
}

impl Serialize for ZunionstoreInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;
        if self.weights.is_some() {
            fields += 1;
        }
        if self.aggregate.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ZunionstoreInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("destination", &self.destination)?;
        state.serialize_field("numkeys", &self.numkeys)?;
        state.serialize_field("keys", &self.keys)?;
        if let Some(weights) = &self.weights {
            state.serialize_field("weights", weights)?;
        }
        if let Some(aggregate) = &self.aggregate {
            state.serialize_field("aggregate", aggregate)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    ZunionstoreInput,
    API_INFO,
    {destination, numkeys, keys, weights, aggregate }
);

impl RedisCommandInput for ZunionstoreInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        let mut keys = self.keys.clone();
        keys.push(self.destination.clone());
        keys
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.destination).arg(&self.numkeys);
        for key in &self.keys {
            command.arg(key);
        }

        if let Some(weights) = &self.weights {
            command.arg("WEIGHTS");
            for w in weights {
                command.arg(w);
            }
        }

        if let Some(aggregate) = &self.aggregate {
            command.arg("AGGREGATE").arg(aggregate.as_str());
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request("ZUNIONSTORE requires at least destination and numkeys arguments"));
        }

        let destination = args[0].clone().try_into()?;
        let numkeys = args[1].clone();
        let num_keys = match &numkeys {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::parse("numkeys must be a valid integer"))?,
            _ => return Err(EpError::parse("numkeys must be integer")),
        };

        if args.len() < 2 + num_keys {
            return Err(EpError::request("Insufficient keys"));
        }

        let mut keys = Vec::new();
        for i in 0..num_keys {
            keys.push(args[2 + i].clone().try_into()?);
        }

        let mut weights = None;
        let mut aggregate = None;
        let mut i = 2 + num_keys;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                match s.to_uppercase().as_str() {
                    "WEIGHTS" => {
                        i += 1;
                        let mut weight_values = Vec::new();
                        while i < args.len() {
                            if let RedisJsonValue::String(s) = &args[i]
                                && s.to_uppercase() == "AGGREGATE"
                            {
                                break;
                            }
                            weight_values.push(args[i].clone());
                            i += 1;
                        }
                        weights = Some(weight_values);
                    }
                    "AGGREGATE" if i + 1 < args.len() => {
                        i += 1;
                        if let RedisJsonValue::String(agg_str) = &args[i] {
                            aggregate = Aggregate::from_str(agg_str);
                        }
                        i += 1;
                    }
                    _ => i += 1,
                }
            } else {
                i += 1;
            }
        }

        Ok(Self { destination, numkeys, keys, weights, aggregate })
    }
}

/// Output for Redis ZUNIONSTORE command
///
/// Returns the number of elements in the resulting sorted set.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZunionstoreOutput {
    count: i64,
}

impl ZunionstoreOutput {
    pub fn new(count: i64) -> Self {
        Self { count }
    }

    /// Get the number of elements in the resulting sorted set
    pub fn count(&self) -> i64 {
        self.count
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Integer(n)) => n,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => data,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("expected integer response")),
        };

        Ok(Self { count })
    }
}

impl Serialize for ZunionstoreOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZunionstoreOutput", 1)?;
        state.serialize_field("count", &self.count)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;
        use crate::api::lib::sorted_set::common::Aggregate;

        #[test]
        fn test_encode_command_basic() {
            let input = ZunionstoreInput::new(
                RedisKey::String("dest".into()),
                vec![RedisKey::String("zset1".into()), RedisKey::String("zset2".into())],
            );
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZUNIONSTORE"));
            assert!(cmd_str.contains("dest"));
            assert!(cmd_str.contains("2")); // numkeys
        }

        #[test]
        fn test_encode_command_with_weights() {
            let input =
                ZunionstoreInput::new(RedisKey::String("dest".into()), vec![RedisKey::String("zset1".into())]).with_weights(vec![2.0]);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("WEIGHTS"));
        }

        #[test]
        fn test_encode_command_with_aggregate() {
            let input = ZunionstoreInput::new(RedisKey::String("dest".into()), vec![RedisKey::String("zset1".into())])
                .with_aggregate(Aggregate::MIN);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("AGGREGATE"));
            assert!(cmd_str.contains("MIN"));
        }

        #[test]
        fn test_decode_output() {
            let output = ZunionstoreOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.count(), 5);
        }

        #[test]
        fn test_decode_output_zero() {
            let output = ZunionstoreOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.count(), 0);
        }

        #[test]
        fn test_decode_error() {
            let err = ZunionstoreOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::Integer(2),
                RedisJsonValue::String("zset1".into()),
                RedisJsonValue::String("zset2".into()),
            ];
            let input = ZunionstoreInput::decode(args).unwrap();
            assert_eq!(input.destination, RedisKey::String("dest".into()));
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("dest".into()), RedisJsonValue::Integer(1)];
            let err = ZunionstoreInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Insufficient keys"));
        }

        #[test]
        fn test_keys_includes_destination() {
            let input = ZunionstoreInput::new(RedisKey::String("dest".into()), vec![RedisKey::String("src".into())]);
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
            assert!(keys.contains(&RedisKey::String("dest".into())));
            assert!(keys.contains(&RedisKey::String("src".into())));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::sorted_set::common::Aggregate;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zunionstore_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nzunionstoretest1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nzunionstoretest2\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nzunionstoreresult\r\n").await.expect("raw failed");

                    // ZADD zunionstoretest1 1 a 2 b
                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$16\r\nzunionstoretest1\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n")
                        .await
                        .expect("raw failed");

                    // ZADD zunionstoretest2 3 b 4 c
                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$16\r\nzunionstoretest2\r\n$1\r\n3\r\n$1\r\nb\r\n$1\r\n4\r\n$1\r\nc\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZunionstoreInput::new(
                                RedisKey::String("zunionstoreresult".into()),
                                vec![
                                    RedisKey::String("zunionstoretest1".into()),
                                    RedisKey::String("zunionstoretest2".into()),
                                ],
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZunionstoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 3); // a, b, c
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zunionstore_with_weights() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nzustore_wgt_s1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nzustore_wgt_s2\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nzustore_wgt_dest\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$14\r\nzustore_wgt_s1\r\n$1\r\n1\r\n$1\r\na\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$14\r\nzustore_wgt_s2\r\n$1\r\n1\r\n$1\r\na\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZunionstoreInput::new(
                                RedisKey::String("zustore_wgt_dest".into()),
                                vec![RedisKey::String("zustore_wgt_s1".into()), RedisKey::String("zustore_wgt_s2".into())],
                            )
                            .with_weights(vec![2, 3])
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZunionstoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 1);

                    // Verify score: 1*2 + 1*3 = 5
                    let score_result = ctx.raw(b"*3\r\n$6\r\nZSCORE\r\n$16\r\nzustore_wgt_dest\r\n$1\r\na\r\n").await.expect("raw failed");
                    let score_str = String::from_utf8_lossy(&score_result);
                    assert!(score_str.contains("5"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zunionstore_aggregate_min() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nzustore_min_s1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nzustore_min_s2\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nzustore_min_dest\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$14\r\nzustore_min_s1\r\n$1\r\n5\r\n$1\r\na\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$14\r\nzustore_min_s2\r\n$1\r\n3\r\n$1\r\na\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZunionstoreInput::new(
                                RedisKey::String("zustore_min_dest".into()),
                                vec![RedisKey::String("zustore_min_s1".into()), RedisKey::String("zustore_min_s2".into())],
                            )
                            .with_aggregate(Aggregate::MIN)
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZunionstoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 1);

                    // Verify score: MIN(5, 3) = 3
                    let score_result = ctx.raw(b"*3\r\n$6\r\nZSCORE\r\n$16\r\nzustore_min_dest\r\n$1\r\na\r\n").await.expect("raw failed");
                    let score_str = String::from_utf8_lossy(&score_result);
                    assert!(score_str.contains("3"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zunionstore_empty_sets() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nzustore_empty_dest\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nzustore_empty_s1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nzustore_empty_s2\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZunionstoreInput::new(
                                RedisKey::String("zustore_empty_dest".into()),
                                vec![
                                    RedisKey::String("zustore_empty_s1".into()),
                                    RedisKey::String("zustore_empty_s2".into()),
                                ],
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZunionstoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zunionstore_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$13\r\nzustore_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZunionstoreInput::new(RedisKey::String("zustore_dest".into()), vec![RedisKey::String("zustore_wrong".into())])
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = ZunionstoreOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }
    }
}
