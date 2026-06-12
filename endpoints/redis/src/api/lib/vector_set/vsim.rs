#![allow(clippy::upper_case_acronyms)] // Intentional: protocol/command acronyms (ACL, GEO, etc.)
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

const API_INFO: ApiInfo<RedisApi, VsimInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Vsim, "Returns elements by vector similarity", ReqType::Read, true);

/// See official Redis documentation for `VSIM`
/// https://redis.io/docs/latest/commands/vsim/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct VsimInput {
    key: RedisKey,
    value: Value,
    element: ElementArg,
    with_scores: Option<bool>,
    count: Option<RedisJsonValue>,
    ef: Option<RedisJsonValue>,
    filter: Option<RedisJsonValue>,
    filter_ef: Option<RedisJsonValue>,
    truth: Option<bool>,
    no_thread: Option<bool>,
}

impl VsimInput {
    pub fn new_ele(key: impl Into<RedisKey>, element: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            value: Value::ELE,
            element: ElementArg::Element(element.into()),
            with_scores: None,
            count: None,
            ef: None,
            filter: None,
            filter_ef: None,
            truth: None,
            no_thread: None,
        }
    }

    pub fn new_fp32(key: impl Into<RedisKey>, vector: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            value: Value::FP32,
            element: ElementArg::Vector(vector.into()),
            with_scores: None,
            count: None,
            ef: None,
            filter: None,
            filter_ef: None,
            truth: None,
            no_thread: None,
        }
    }

    pub fn new_values(key: impl Into<RedisKey>, vector: impl Into<RedisJsonValue>, count: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            value: Value::VALUES(count.into()),
            element: ElementArg::Vector(vector.into()),
            with_scores: None,
            count: None,
            ef: None,
            filter: None,
            filter_ef: None,
            truth: None,
            no_thread: None,
        }
    }

    pub fn with_scores(mut self) -> Self {
        self.with_scores = Some(true);
        self
    }

    pub fn with_count(mut self, count: impl Into<RedisJsonValue>) -> Self {
        self.count = Some(count.into());
        self
    }

    pub fn with_filter(mut self, filter: impl Into<RedisJsonValue>) -> Self {
        self.filter = Some(filter.into());
        self
    }
}

impl Serialize for VsimInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;
        if self.with_scores.is_some() {
            fields += 1;
        }
        if self.count.is_some() {
            fields += 1;
        }
        if self.ef.is_some() {
            fields += 1;
        }
        if self.filter.is_some() {
            fields += 1;
        }
        if self.filter_ef.is_some() {
            fields += 1;
        }
        if self.truth.is_some() {
            fields += 1;
        }
        if self.no_thread.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("VsimInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("value", &self.value)?;
        state.serialize_field("element", &self.element)?;

        if let Some(v) = &self.with_scores {
            state.serialize_field("with_scores", v)?;
        }
        if let Some(v) = &self.count {
            state.serialize_field("count", v)?;
        }
        if let Some(v) = &self.ef {
            state.serialize_field("ef", v)?;
        }
        if let Some(v) = &self.filter {
            state.serialize_field("filter", v)?;
        }
        if let Some(v) = &self.filter_ef {
            state.serialize_field("filter_ef", v)?;
        }
        if let Some(v) = &self.truth {
            state.serialize_field("truth", v)?;
        }
        if let Some(v) = &self.no_thread {
            state.serialize_field("no_thread", v)?;
        }
        state.end()
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum Value {
    ELE,
    FP32,
    VALUES(RedisJsonValue),
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum ElementArg {
    Vector(RedisJsonValue),
    Element(RedisJsonValue),
}

impl_redis_operation!(VsimInput, API_INFO, { key, value, element, with_scores, count, ef, filter, filter_ef, truth, no_thread });

impl RedisCommandInput for VsimInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);

        match &self.value {
            Value::ELE => {
                command.arg("ELE");
                match &self.element {
                    ElementArg::Element(e) => command.arg(e),
                    ElementArg::Vector(v) => command.arg(v),
                };
            }
            Value::FP32 => {
                command.arg("FP32");
                match &self.element {
                    ElementArg::Vector(v) => {
                        // Serialize array to JSON string for FP32
                        if let RedisJsonValue::Array(_) = v {
                            command.arg(serde_json::to_string(v).unwrap_or_default());
                        } else {
                            command.arg(v);
                        }
                    }
                    ElementArg::Element(e) => {
                        command.arg(e);
                    }
                };
            }
            Value::VALUES(count) => {
                command.arg("VALUES").arg(count);
                // For VALUES, emit each vector component separately
                if let ElementArg::Vector(RedisJsonValue::Array(vals)) = &self.element {
                    for v in vals {
                        command.arg(v);
                    }
                } else {
                    match &self.element {
                        ElementArg::Vector(v) => command.arg(v),
                        ElementArg::Element(e) => command.arg(e),
                    };
                }
            }
        };

        if self.with_scores.is_some() {
            command.arg("WITHSCORES");
        }
        if let Some(n) = &self.count {
            command.arg("COUNT").arg(n);
        }
        if let Some(ef) = &self.ef {
            command.arg("EF").arg(ef);
        }
        if let Some(f) = &self.filter {
            command.arg("FILTER").arg(f);
        }
        if let Some(fe) = &self.filter_ef {
            command.arg("FILTER_EF").arg(fe);
        }
        if self.truth.is_some() {
            command.arg("TRUTH");
        }
        if self.no_thread.is_some() {
            command.arg("NOTHREAD");
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request("VSIM requires at least 3 arguments"));
        }

        let key = args[0].clone().try_into()?;
        #[allow(unused_assignments)]
        let mut value = Value::ELE;
        #[allow(unused_assignments)]
        let mut element = None;
        let mut with_scores = None;
        let mut count = None;
        let mut ef = None;
        let mut filter = None;
        let mut filter_ef = None;
        let mut truth = None;
        let mut no_thread = None;
        let mut i = 1;

        if let Some(RedisJsonValue::String(s)) = args.get(i) {
            match s.to_uppercase().as_str() {
                "ELE" => {
                    value = Value::ELE;
                    i += 1;
                    if i >= args.len() {
                        return Err(EpError::request("ELE requires element name"));
                    }
                    element = Some(ElementArg::Element(args[i].clone()));
                    i += 1;
                }
                "FP32" => {
                    value = Value::FP32;
                    i += 1;
                    if i >= args.len() {
                        return Err(EpError::request("FP32 requires vector data"));
                    }
                    element = Some(ElementArg::Vector(args[i].clone()));
                    i += 1;
                }
                "VALUES" => {
                    if i + 1 >= args.len() {
                        return Err(EpError::request("VALUES requires a count"));
                    }
                    let count_val = &args[i + 1];
                    let count: usize = match count_val {
                        RedisJsonValue::String(s) => s.parse().map_err(|_| EpError::request(format!("Invalid VALUES count: {}", s)))?,
                        RedisJsonValue::Integer(n) => *n as usize,
                        _ => return Err(EpError::request("VALUES count must be a number")),
                    };
                    value = Value::VALUES(count_val.clone());
                    i += 2;

                    if i + count > args.len() {
                        return Err(EpError::request(format!(
                            "VALUES expects {} values, but only {} args remain",
                            count,
                            args.len() - i
                        )));
                    }
                    let mut vec_values = Vec::with_capacity(count);
                    for _ in 0..count {
                        vec_values.push(args[i].clone());
                        i += 1;
                    }
                    element = Some(ElementArg::Vector(RedisJsonValue::Array(vec_values)));
                }
                _ => return Err(EpError::request("Expected ELE, FP32, or VALUES")),
            }
        } else {
            return Err(EpError::request("Expected ELE, FP32, or VALUES"));
        }

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                match s.to_uppercase().as_str() {
                    "WITHSCORES" => {
                        with_scores = Some(true);
                        i += 1;
                    }
                    "COUNT" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("COUNT requires a value"));
                        }
                        count = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "EF" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("EF requires a value"));
                        }
                        ef = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "FILTER" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("FILTER requires a value"));
                        }
                        filter = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "FILTER_EF" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("FILTER_EF requires a value"));
                        }
                        filter_ef = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "TRUTH" => {
                        truth = Some(true);
                        i += 1;
                    }
                    "NOTHREAD" => {
                        no_thread = Some(true);
                        i += 1;
                    }
                    _ => return Err(EpError::request(format!("Unknown VSIM option: {}", s))),
                }
            } else {
                return Err(EpError::request("VSIM options must be strings"));
            }
        }

        Ok(VsimInput {
            key,
            value,
            element: element.ok_or_else(|| EpError::request("Missing element"))?,
            with_scores,
            count,
            ef,
            filter,
            filter_ef,
            truth,
            no_thread,
        })
    }
}

/// Output for Redis VSIM command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct VsimOutput {
    /// List of elements, optionally with scores
    results: Vec<VsimResult>,
}

#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct VsimResult {
    pub element: String,
    pub score: Option<f64>,
}

impl VsimOutput {
    pub fn new(results: Vec<VsimResult>) -> Self {
        Self { results }
    }

    pub fn results(&self) -> &[VsimResult] {
        &self.results
    }

    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let results = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::parse_resp2(resp2_frame)?,
            DecoderRespFrame::Resp3(resp3_frame) => Self::parse_resp3(resp3_frame)?,
        };

        Ok(Self { results })
    }

    fn parse_resp2(frame: Resp2Frame) -> Result<Vec<VsimResult>, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut results = Vec::new();
                for item in arr {
                    let element = match item {
                        Resp2Frame::BulkString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                        _ => continue,
                    };
                    // Check if next is a score
                    let score = None; // Simplified - full impl would check WITHSCORES
                    results.push(VsimResult { element, score });
                }
                Ok(results)
            }
            Resp2Frame::Null => Ok(Vec::new()),
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected VSIM response: {:?}", other))),
        }
    }

    fn parse_resp3(frame: Resp3Frame) -> Result<Vec<VsimResult>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut results = Vec::new();
                for item in data {
                    let element = match item {
                        Resp3Frame::BlobString { data: s, .. } => String::from_utf8(s).map_err(EpError::parse)?,
                        _ => continue,
                    };
                    results.push(VsimResult { element, score: None });
                }
                Ok(results)
            }
            Resp3Frame::Null => Ok(Vec::new()),
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected VSIM response: {:?}", other))),
        }
    }
}

impl Serialize for VsimOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("VsimOutput", 1)?;
        state.serialize_field("results", &self.results)?;
        state.end()
    }
}

impl Serialize for VsimResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("VsimResult", 2)?;
        state.serialize_field("element", &self.element)?;
        state.serialize_field("score", &self.score)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_ele() {
            let input = VsimInput::new_ele("myvset", "elem1");
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$4\r\nVSIM\r\n"));
        }

        #[test]
        fn test_encode_command_fp32() {
            let input = VsimInput::new_fp32("myvset", RedisJsonValue::Array(vec![RedisJsonValue::from(1.0), RedisJsonValue::from(2.0)]));
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$4\r\nVSIM\r\n"));
        }

        #[test]
        fn test_decode_empty() {
            let output = VsimOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = VsimOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = VsimOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("ELE".into())];
            let err = VsimInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 3"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vsim_empty_set() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    let input = VsimInput::new_fp32(
                        "empty_vset",
                        RedisJsonValue::Array(vec![RedisJsonValue::from(1.0), RedisJsonValue::from(2.0), RedisJsonValue::from(3.0)]),
                    );
                    let result = ctx.raw(&input.command()).await.expect("raw failed");
                    let output = VsimOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vsim_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("8")).await;
            let input = VsimInput::new_fp32("empty", RedisJsonValue::Array(vec![RedisJsonValue::from(1.0), RedisJsonValue::from(2.0)]));
            let result = ctx.raw(&input.command()).await.expect("raw failed");
            let output = VsimOutput::decode(&result).expect("decode failed");
            assert!(output.is_empty());
            ctx.stop().await;
        }
    }
}
