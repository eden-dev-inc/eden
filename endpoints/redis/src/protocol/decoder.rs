use crate::api::{RedisApi, RedisJsonValue};
use error::{EpError, ParseError, ResultEP};
pub use redis_protocol::resp2::types::OwnedFrame as Resp2Frame;
pub use redis_protocol::resp3::types::OwnedFrame as Resp3Frame;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RedisCommandArgs {
    pub(crate) command: RedisApi,
    pub(crate) args: Vec<RedisJsonValue>,
}

impl RedisCommandArgs {
    pub fn new(command: RedisApi, args: Vec<RedisJsonValue>) -> Self {
        Self { command, args }
    }
    pub fn command(&self) -> &RedisApi {
        &self.command
    }
    pub fn args(&self) -> &[RedisJsonValue] {
        &self.args
    }

    /// Estimate the number of keys targeted by this command.
    ///
    /// Multi-key commands like MGET/DEL/UNLINK use all args as keys,
    /// MSET uses every other arg as a key, and most commands target 1 key.
    pub fn key_count(&self) -> usize {
        use RedisApi::*;
        match self.command {
            Mget | Del | Unlink | Exists | Touch | Watch | Sdiff | Sinter | Sunion | Pfcount | Pfmerge => self.args.len(),
            Mset | Msetnx => self.args.len() / 2,
            _ => 1,
        }
    }
}

#[derive(Debug)]
pub enum DecoderRespFrame {
    Resp2(Resp2Frame),
    Resp3(Resp3Frame),
}

impl TryFrom<DecoderRespFrame> for RedisCommandArgs {
    type Error = EpError;

    fn try_from(frame: DecoderRespFrame) -> Result<Self, Self::Error> {
        let (command, args) = match frame {
            DecoderRespFrame::Resp2(frame) => decode_resp2_command(frame)?,
            DecoderRespFrame::Resp3(frame) => decode_resp3_command(frame)?,
        };

        Ok(Self { command, args })
    }
}

impl TryFrom<DecoderRespFrame> for RedisJsonValue {
    type Error = EpError;

    fn try_from(frame: DecoderRespFrame) -> Result<Self, Self::Error> {
        match frame {
            DecoderRespFrame::Resp2(frame) => convert_resp2_to_json(&frame),
            DecoderRespFrame::Resp3(frame) => convert_resp3_to_json(&frame),
        }
    }
}

impl TryFrom<Resp2Frame> for RedisJsonValue {
    type Error = EpError;

    fn try_from(frame: Resp2Frame) -> Result<Self, Self::Error> {
        convert_resp2_to_json(&frame)
    }
}

impl TryFrom<&Resp2Frame> for RedisJsonValue {
    type Error = EpError;

    fn try_from(frame: &Resp2Frame) -> Result<Self, Self::Error> {
        convert_resp2_to_json(frame)
    }
}

impl TryFrom<Resp3Frame> for RedisJsonValue {
    type Error = EpError;

    fn try_from(frame: Resp3Frame) -> Result<Self, Self::Error> {
        convert_resp3_to_json(&frame)
    }
}

impl TryFrom<&Resp3Frame> for RedisJsonValue {
    type Error = EpError;

    fn try_from(frame: &Resp3Frame) -> Result<Self, Self::Error> {
        convert_resp3_to_json(frame)
    }
}

fn decode_resp2_command(frame: Resp2Frame) -> ResultEP<(RedisApi, Vec<RedisJsonValue>)> {
    match frame {
        Resp2Frame::Array(args) => {
            if args.is_empty() {
                return Err(EpError::Parse(ParseError::Custom("empty command array".to_string())));
            }

            let (command, remaining_args) = parse_command_resp2(&args)?;
            let args = if !remaining_args.is_empty() {
                extract_args_from_resp2_frames(remaining_args)?
            } else {
                Vec::new()
            };
            Ok((command, args))
        }
        _ => Err(EpError::Parse(ParseError::Custom("expected array frame for Redis command".to_string()))),
    }
}

fn decode_resp3_command(frame: Resp3Frame) -> ResultEP<(RedisApi, Vec<RedisJsonValue>)> {
    match frame {
        Resp3Frame::Array { data, .. } => {
            if data.is_empty() {
                return Err(EpError::Parse(ParseError::Custom("empty command array".to_string())));
            }

            let (command, remaining_args) = parse_command_resp3(&data)?;
            let args = if !remaining_args.is_empty() {
                extract_args_from_resp3_frames(remaining_args)?
            } else {
                Vec::new()
            };
            Ok((command, args))
        }
        _ => Err(EpError::Parse(ParseError::Custom("expected array frame for Redis command".to_string()))),
    }
}

fn parse_command_resp2(frames: &[Resp2Frame]) -> ResultEP<(RedisApi, &[Resp2Frame])> {
    let base_cmd = extract_bytes_resp2(&frames[0])?;
    let subcommand = frames.get(1).and_then(|frame| extract_bytes_resp2(frame).ok());

    if let Ok((api, words_consumed)) = RedisApi::try_from_command_words_bytes(base_cmd, subcommand) {
        return Ok((api, &frames[words_consumed..]));
    }

    Err(EpError::Parse(ParseError::Custom(format!(
        "unknown Redis command: {}",
        String::from_utf8_lossy(base_cmd)
    ))))
}

fn parse_command_resp3(frames: &[Resp3Frame]) -> ResultEP<(RedisApi, &[Resp3Frame])> {
    let base_cmd = extract_bytes_resp3(&frames[0])?;
    let subcommand = frames.get(1).and_then(|frame| extract_bytes_resp3(frame).ok());

    if let Ok((api, words_consumed)) = RedisApi::try_from_command_words_bytes(base_cmd, subcommand) {
        return Ok((api, &frames[words_consumed..]));
    }

    Err(EpError::Parse(ParseError::Custom(format!(
        "unknown Redis command: {}",
        String::from_utf8_lossy(base_cmd)
    ))))
}

fn extract_bytes_resp2(frame: &Resp2Frame) -> ResultEP<&[u8]> {
    match frame {
        Resp2Frame::BulkString(data) | Resp2Frame::SimpleString(data) => Ok(data),
        _ => Err(EpError::Parse(ParseError::Custom("expected string frame".to_string()))),
    }
}

fn extract_bytes_resp3(frame: &Resp3Frame) -> ResultEP<&[u8]> {
    match frame {
        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } | Resp3Frame::VerbatimString { data, .. } => Ok(data),
        _ => Err(EpError::Parse(ParseError::Custom("expected string frame".to_string()))),
    }
}

fn extract_args_from_resp2_frames(frames: &[Resp2Frame]) -> ResultEP<Vec<RedisJsonValue>> {
    frames.iter().map(convert_resp2_to_json).collect()
}

fn extract_args_from_resp3_frames(frames: &[Resp3Frame]) -> ResultEP<Vec<RedisJsonValue>> {
    frames.iter().map(convert_resp3_to_json).collect()
}

fn convert_resp2_to_json(frame: &Resp2Frame) -> ResultEP<RedisJsonValue> {
    match frame {
        Resp2Frame::Array(elements) => {
            let array: Result<Vec<_>, _> = elements.iter().map(convert_resp2_to_json).collect();
            Ok(RedisJsonValue::Array(array?))
        }
        Resp2Frame::BulkString(data) | Resp2Frame::SimpleString(data) => match String::from_utf8(data.clone()) {
            Ok(s) => Ok(RedisJsonValue::String(s)),
            Err(_) => Ok(RedisJsonValue::Bytes(data.clone())),
        },
        Resp2Frame::Integer(data) => Ok(RedisJsonValue::Integer(*data)),
        Resp2Frame::Error(error) => Err(EpError::Parse(ParseError::Custom(error.to_string()))),
        Resp2Frame::Null => Ok(RedisJsonValue::Null),
    }
}

fn convert_resp3_to_json(frame: &Resp3Frame) -> ResultEP<RedisJsonValue> {
    match frame {
        Resp3Frame::Array { data, .. } => {
            let array: Result<Vec<_>, _> = data.iter().map(convert_resp3_to_json).collect();
            Ok(RedisJsonValue::Array(array?))
        }
        Resp3Frame::BlobString { data, .. }
        | Resp3Frame::SimpleString { data, .. }
        | Resp3Frame::VerbatimString { data, .. }
        | Resp3Frame::BigNumber { data, .. } => match String::from_utf8(data.clone()) {
            Ok(s) => Ok(RedisJsonValue::String(s)),
            Err(_) => Ok(RedisJsonValue::Bytes(data.clone())),
        },
        Resp3Frame::Number { data, .. } => Ok(RedisJsonValue::Integer(*data)),
        Resp3Frame::Double { data, .. } => Ok(RedisJsonValue::Float(*data)),
        Resp3Frame::Boolean { data, .. } => Ok(RedisJsonValue::Bool(*data)),
        Resp3Frame::SimpleError { data, .. } => Err(EpError::Parse(ParseError::Custom(data.to_string()))),
        Resp3Frame::BlobError { data, .. } => Err(EpError::Parse(ParseError::Custom(str::from_utf8(data).unwrap_or_default().to_string()))),
        Resp3Frame::Null => Ok(RedisJsonValue::Null),
        Resp3Frame::Map { data, .. } => {
            let mut object = HashMap::new();
            for (key, value) in data {
                let key_str = match convert_resp3_to_json(key)? {
                    RedisJsonValue::String(s) => s,
                    other => format!("{:?}", other), // Convert non-string keys to string representation
                };
                object.insert(key_str, convert_resp3_to_json(value)?);
            }
            Ok(RedisJsonValue::Object(object))
        }
        Resp3Frame::Set { data, .. } => {
            let array: Result<Vec<_>, _> = data.iter().map(convert_resp3_to_json).collect();
            Ok(RedisJsonValue::Array(array?))
        }
        Resp3Frame::Push { data, .. } => {
            let array: Result<Vec<_>, _> = data.iter().map(convert_resp3_to_json).collect();
            Ok(RedisJsonValue::Array(array?))
        }
        Resp3Frame::Hello { .. } => Err(EpError::Parse(ParseError::Custom("HELLO frames not supported as command arguments".to_string()))),
        Resp3Frame::ChunkedString(data) => match String::from_utf8(data.clone()) {
            Ok(s) => Ok(RedisJsonValue::String(s)),
            Err(_) => Ok(RedisJsonValue::Bytes(data.clone())),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resp2_bulk_string_with_invalid_utf8_is_bytes() {
        let frame = Resp2Frame::BulkString(vec![0xff, 0x00]);
        let value = RedisJsonValue::try_from(frame).expect("conversion should succeed");

        assert!(matches!(value, RedisJsonValue::Bytes(b) if b == vec![0xff, 0x00]));
    }

    #[test]
    fn resp3_blob_string_with_invalid_utf8_is_bytes() {
        let frame = Resp3Frame::BlobString { data: vec![0xff, 0x00], attributes: None };
        let value = RedisJsonValue::try_from(frame).expect("conversion should succeed");

        assert!(matches!(value, RedisJsonValue::Bytes(b) if b == vec![0xff, 0x00]));
    }
}
