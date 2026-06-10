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

const API_INFO: ApiInfo<RedisApi, MoveInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Move,
    "Moves a key to another database. Returns 1 if the key was moved, 0 if it was not found or already exists in the destination database.",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `MOVE`
/// https://redis.io/docs/latest/commands/move/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct MoveInput {
    pub(crate) key: RedisKey,
    /// Target database index (0-15 by default, configurable via `databases` in redis.conf)
    pub(crate) db: u32,
}

impl Serialize for MoveInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("MoveInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("db", &self.db)?;
        state.end()
    }
}

impl_redis_operation!(MoveInput, API_INFO, { key, db });

impl RedisCommandInput for MoveInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(self.db);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::parse(format!("MOVE requires exactly 2 arguments, given {}", args.len())));
        }

        let db = match &args[1] {
            RedisJsonValue::Integer(i) => u32::try_from(*i).ok().ok_or_else(|| EpError::parse("db must be a non-negative integer"))?,
            RedisJsonValue::String(s) => s.parse::<u32>().map_err(|_| EpError::parse("db must be a valid integer"))?,
            _ => return Err(EpError::parse("db must be an integer")),
        };

        Ok(Self { key: args[0].clone().try_into()?, db })
    }
}

/// Output for Redis MOVE command
///
/// Returns 1 if the key was moved successfully, 0 if the key was not found
/// or already exists in the destination database.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct MoveOutput {
    /// 1 if key was moved, 0 otherwise
    moved: bool,
}

impl MoveOutput {
    pub fn new(moved: bool) -> Self {
        Self { moved }
    }

    /// Returns true if the key was successfully moved
    pub fn moved(&self) -> bool {
        self.moved
    }

    /// Decode the Redis protocol response into a MoveOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let moved = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i == 1,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected MOVE response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data == 1,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected MOVE response: {:?}", other)));
                }
            },
        };

        Ok(Self { moved })
    }
}

impl Serialize for MoveOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("MoveOutput", 1)?;
        state.serialize_field("moved", &self.moved)?;
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
            let input = MoveInput { key: RedisKey::String("mykey".into()), db: 1 };
            assert_eq!(input.command().to_vec(), b"*3\r\n$4\r\nMOVE\r\n$5\r\nmykey\r\n$1\r\n1\r\n");
        }

        #[test]
        fn test_encode_command_db_15() {
            let input = MoveInput { key: RedisKey::String("testkey".into()), db: 15 };
            assert_eq!(input.command().to_vec(), b"*3\r\n$4\r\nMOVE\r\n$7\r\ntestkey\r\n$2\r\n15\r\n");
        }

        #[test]
        fn test_decode_success() {
            // RESP2 integer :1\r\n
            let output = MoveOutput::decode(b":1\r\n").unwrap();
            assert!(output.moved());
        }

        #[test]
        fn test_decode_not_moved() {
            // RESP2 integer :0\r\n
            let output = MoveOutput::decode(b":0\r\n").unwrap();
            assert!(!output.moved());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = MoveOutput::decode(b"-ERR no such key\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::Integer(1)];
            let input = MoveInput::decode(args).unwrap();
            assert_eq!(input.db, 1);
        }

        #[test]
        fn test_decode_input_string_db() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("5".into())];
            let input = MoveInput::decode(args).unwrap();
            assert_eq!(input.db, 5);
        }

        #[test]
        fn test_decode_input_wrong_arg_count() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = MoveInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("2 arguments"));
        }

        #[test]
        fn test_decode_input_invalid_db() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("notanumber".into())];
            let err = MoveInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("integer"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::test_utils::*;
        use serial_test::serial;
        // Note: MOVE command requires multiple databases to be available.
        // By default Redis has 16 databases (0-15).
        // These tests assume db 0 (default) and db 1 are available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_move_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&MoveInput { key: RedisKey::String("nonexistent".into()), db: 1 }.command()).await.expect("raw failed");

                    let output = MoveOutput::decode(&result).expect("decode failed");
                    assert!(!output.moved(), "nonexistent key should return 0");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_move_existing_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set a key in db 0
                    ctx.write(SetInput {
                        key: RedisKey::String("movekey".into()),
                        value: RedisJsonValue::String("movevalue".into()),
                        ..Default::default()
                    })
                    .await;

                    // Move it to db 1
                    let result =
                        ctx.raw(&MoveInput { key: RedisKey::String("movekey".into()), db: 1 }.command()).await.expect("raw failed");

                    let output = MoveOutput::decode(&result).expect("decode failed");
                    assert!(output.moved(), "existing key should be moved");

                    // Verify key no longer exists in db 0
                    let get_result = ctx
                        .raw(&crate::api::lib::string::get::GetInput { key: RedisKey::String("movekey".into()) }.command())
                        .await
                        .expect("raw failed");

                    let get_output = crate::api::lib::string::get::GetOutput::decode(&get_result).expect("decode failed");
                    assert!(!get_output.exists(), "key should not exist in source db after move");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_move_to_same_db_fails() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("samedbkey".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Attempt to move to db 0 (same as current)
                    let result =
                        ctx.raw(&MoveInput { key: RedisKey::String("samedbkey".into()), db: 0 }.command()).await.expect("raw failed");

                    // Redis returns an error when moving to the same db
                    let err = MoveOutput::decode(&result);
                    assert!(err.is_err(), "moving to same db should error");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_move_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("resp2key".into()),
                value: RedisJsonValue::String("value".into()),
                ..Default::default()
            })
            .await;

            let result = ctx.raw(&MoveInput { key: RedisKey::String("resp2key".into()), db: 1 }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP2 integer format");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_move_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("resp3key".into()),
                value: RedisJsonValue::String("value".into()),
                ..Default::default()
            })
            .await;

            let result = ctx.raw(&MoveInput { key: RedisKey::String("resp3key".into()), db: 1 }.command()).await.expect("raw failed");

            // RESP3 uses same integer format
            assert_eq!(&result[..], b":1\r\n", "RESP3 integer format");
            ctx.stop().await;
        }
    }
}
