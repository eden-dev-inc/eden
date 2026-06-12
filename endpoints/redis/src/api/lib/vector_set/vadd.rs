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

const API_INFO: ApiInfo<RedisApi, VaddInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Vadd,
    "Add a new element to a vector set, or update its vector if it already exists",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `VADD`
/// https://redis.io/docs/latest/commands/vadd/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct VaddInput {
    key: RedisKey,
    reduce: Option<RedisJsonValue>,
    value: Value,
    vector: RedisJsonValue,
    element: RedisJsonValue,
    cas: Option<bool>,
    noquant: Option<NoquantArg>,
    ef: Option<RedisJsonValue>,
    setattr: Option<RedisJsonValue>,
    m: Option<RedisJsonValue>,
}

impl VaddInput {
    pub fn new(key: impl Into<RedisKey>, vector: impl Into<RedisJsonValue>, element: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            reduce: None,
            value: Value::FP32,
            vector: vector.into(),
            element: element.into(),
            cas: None,
            noquant: None,
            ef: None,
            setattr: None,
            m: None,
        }
    }

    pub fn with_reduce(mut self, reduce: impl Into<RedisJsonValue>) -> Self {
        self.reduce = Some(reduce.into());
        self
    }

    pub fn with_values(mut self, dim: impl Into<RedisJsonValue>) -> Self {
        self.value = Value::VALUES(dim.into());
        self
    }

    pub fn with_cas(mut self) -> Self {
        self.cas = Some(true);
        self
    }

    pub fn with_noquant(mut self, arg: NoquantArg) -> Self {
        self.noquant = Some(arg);
        self
    }

    pub fn with_ef(mut self, ef: impl Into<RedisJsonValue>) -> Self {
        self.ef = Some(ef.into());
        self
    }

    pub fn with_setattr(mut self, attr: impl Into<RedisJsonValue>) -> Self {
        self.setattr = Some(attr.into());
        self
    }

    pub fn with_m(mut self, m: impl Into<RedisJsonValue>) -> Self {
        self.m = Some(m.into());
        self
    }
}

impl Serialize for VaddInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 5;
        if self.reduce.is_some() {
            fields += 1;
        }
        if self.cas.is_some() {
            fields += 1;
        }
        if self.noquant.is_some() {
            fields += 1;
        }
        if self.ef.is_some() {
            fields += 1;
        }
        if self.setattr.is_some() {
            fields += 1;
        }
        if self.m.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("VaddInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(reduce) = &self.reduce {
            state.serialize_field("reduce", reduce)?;
        }
        state.serialize_field("value", &self.value)?;
        state.serialize_field("vector", &self.vector)?;
        state.serialize_field("element", &self.element)?;
        if let Some(cas) = &self.cas {
            state.serialize_field("cas", cas)?;
        }
        if let Some(noquant) = &self.noquant {
            state.serialize_field("noquant", noquant)?;
        }
        if let Some(ef) = &self.ef {
            state.serialize_field("ef", ef)?;
        }
        if let Some(setattr) = &self.setattr {
            state.serialize_field("setattr", setattr)?;
        }
        if let Some(m) = &self.m {
            state.serialize_field("m", m)?;
        }
        state.end()
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum Value {
    #[default]
    FP32,
    VALUES(RedisJsonValue),
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum NoquantArg {
    NOQUANT,
    Q8,
    BIN,
}

impl_redis_operation!(VaddInput, API_INFO, {key, reduce, value, vector, element, cas, noquant, ef, setattr, m});

impl RedisCommandInput for VaddInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);

        if let Some(reduce) = &self.reduce {
            command.arg("REDUCE").arg(reduce);
        }

        match &self.value {
            Value::FP32 => {
                command.arg("FP32").arg(&self.vector);
            }
            Value::VALUES(_) => {
                // Parse vector array and emit: VALUES <count> <v1> <v2> ...
                if let RedisJsonValue::Array(vals) = &self.vector {
                    command.arg("VALUES").arg(vals.len());
                    for v in vals {
                        command.arg(v);
                    }
                }
            }
        };

        // Add element after vector
        command.arg(&self.element);

        if self.cas.is_some() {
            command.arg("CAS");
        }

        if let Some(noquant) = &self.noquant {
            match noquant {
                NoquantArg::NOQUANT => command.arg("NOQUANT"),
                NoquantArg::Q8 => command.arg("Q8"),
                NoquantArg::BIN => command.arg("BIN"),
            };
        }

        if let Some(ef) = &self.ef {
            command.arg("EF").arg(ef);
        }
        if let Some(setattr) = &self.setattr {
            command.arg("SETATTR").arg(setattr);
        }
        if let Some(m) = &self.m {
            command.arg("M").arg(m);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 4 {
            return Err(EpError::request("VADD requires at least 4 arguments"));
        }

        let key = args[0].clone().try_into()?;
        let mut reduce = None;
        #[allow(unused_assignments)]
        let mut value = Value::FP32;
        #[allow(unused_assignments)]
        let mut vector = None;
        #[allow(unused_assignments)]
        let mut element = None;
        let mut cas = None;
        let mut noquant = None;
        let mut ef = None;
        let mut setattr = None;
        let mut m = None;
        let mut i = 1;

        // Parse optional REDUCE
        if let Some(RedisJsonValue::String(s)) = args.get(i)
            && s.to_uppercase() == "REDUCE"
        {
            if i + 1 >= args.len() {
                return Err(EpError::request("REDUCE requires a value"));
            }
            reduce = Some(args[i + 1].clone());
            i += 2;
        }

        // Parse VALUE type
        if let Some(RedisJsonValue::String(s)) = args.get(i) {
            match s.to_uppercase().as_str() {
                "FP32" => {
                    value = Value::FP32;
                    i += 1;
                    if i + 1 >= args.len() {
                        return Err(EpError::request("Missing vector and element"));
                    }
                    vector = Some(args[i].clone());
                    element = Some(args[i + 1].clone());
                    i += 2;
                }
                "VALUES" => {
                    if i + 1 >= args.len() {
                        return Err(EpError::request("VALUES requires a count"));
                    }
                    // Parse count
                    let count_val = &args[i + 1];
                    let count: usize = match count_val {
                        RedisJsonValue::String(s) => s.parse().map_err(|_| EpError::request(format!("Invalid VALUES count: {}", s)))?,
                        RedisJsonValue::Integer(n) => *n as usize,
                        _ => return Err(EpError::request("VALUES count must be a number")),
                    };
                    value = Value::VALUES(count_val.clone());
                    i += 2;

                    if i + count >= args.len() {
                        return Err(EpError::request(format!(
                            "VALUES expects {} values plus element, but only {} args remain",
                            count,
                            args.len() - i
                        )));
                    }
                    let mut vec_values = Vec::with_capacity(count);
                    for _ in 0..count {
                        vec_values.push(args[i].clone());
                        i += 1;
                    }
                    vector = Some(RedisJsonValue::Array(vec_values));

                    // Next is the element
                    if i >= args.len() {
                        return Err(EpError::request("Missing element after vector values"));
                    }
                    element = Some(args[i].clone());
                    i += 1;
                }
                _ => return Err(EpError::request("Expected FP32 or VALUES")),
            }
        } else {
            return Err(EpError::request("Expected FP32 or VALUES"));
        }

        // Parse optional parameters
        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                match s.to_uppercase().as_str() {
                    "CAS" => {
                        cas = Some(true);
                        i += 1;
                    }
                    "NOQUANT" => {
                        noquant = Some(NoquantArg::NOQUANT);
                        i += 1;
                    }
                    "Q8" => {
                        noquant = Some(NoquantArg::Q8);
                        i += 1;
                    }
                    "BIN" => {
                        noquant = Some(NoquantArg::BIN);
                        i += 1;
                    }
                    "EF" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("EF requires a value"));
                        }
                        ef = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "SETATTR" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("SETATTR requires a value"));
                        }
                        setattr = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "M" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("M requires a value"));
                        }
                        m = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ => return Err(EpError::request(format!("Unknown VADD option: {}", s))),
                }
            } else {
                return Err(EpError::request("VADD options must be strings"));
            }
        }

        Ok(VaddInput {
            key,
            reduce,
            value,
            vector: vector.unwrap_or_default(),
            element: element.unwrap_or_default(),
            cas,
            noquant,
            ef,
            setattr,
            m,
        })
    }
}

/// Output for Redis VADD command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct VaddOutput {
    /// 1 if new element added, 0 if updated existing
    result: i64,
}

impl VaddOutput {
    pub fn new(result: i64) -> Self {
        Self { result }
    }

    pub fn was_added(&self) -> bool {
        self.result > 0
    }

    pub fn result(&self) -> i64 {
        self.result
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected VADD response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::Boolean { data, .. } => {
                    if data {
                        1
                    } else {
                        0
                    }
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected VADD response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for VaddOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("VaddOutput", 1)?;
        state.serialize_field("result", &self.result)?;
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
            let input = VaddInput::new("myvset", "[1.0,2.0,3.0]", "elem1");
            let cmd = input.command();
            assert!(cmd.starts_with(b"*5\r\n$4\r\nVADD\r\n"));
        }

        #[test]
        fn test_decode_added() {
            let output = VaddOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_added());
            assert_eq!(output.result(), 1);
        }

        #[test]
        fn test_decode_updated() {
            let output = VaddOutput::decode(b":0\r\n").unwrap();
            assert!(!output.was_added());
            assert_eq!(output.result(), 0);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = VaddOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("FP32".into())];
            let err = VaddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 4"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vadd_new_element() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nvadd_testvset\r\n").await.expect("del failed");

                    let input = VaddInput::new(
                        "vadd_testvset",
                        RedisJsonValue::Array(vec![RedisJsonValue::from(1.0), RedisJsonValue::from(2.0), RedisJsonValue::from(3.0)]),
                        "elem1",
                    )
                    .with_values(3);
                    let result = ctx.raw(&input.command()).await.expect("raw failed");
                    let output = VaddOutput::decode(&result).expect("decode failed");
                    assert!(output.was_added());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vadd_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("8")).await;
            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nvadd_r2key\r\n").await.expect("del failed");

            let input = VaddInput::new(
                "vadd_r2key",
                RedisJsonValue::Array(vec![RedisJsonValue::from(1.0), RedisJsonValue::from(2.0), RedisJsonValue::from(3.0)]),
                "elem",
            )
            .with_values(3);
            let result = ctx.raw(&input.command()).await.expect("raw failed");
            assert!(result.starts_with(b":"), "RESP2 should return integer");
            ctx.stop().await;
        }
    }
}
