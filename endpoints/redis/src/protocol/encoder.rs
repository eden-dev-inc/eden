use crate::api::value::RedisJsonValue;
use crate::protocol::RedisProtocol;
use endpoint_types::protocol::EpProtocol;
use error::{EpError, ResultEP};
use redis_protocol::resp2::types::OwnedFrame as Resp2OwnedFrame;
use redis_protocol::resp3::types::{FrameMap, OwnedFrame as Resp3OwnedFrame};

pub enum EncoderRespFrame {
    Resp2(Resp2OwnedFrame),
    Resp3(Resp3OwnedFrame),
}

pub fn encode_redis_command_resp2(command: &str, args: &[RedisJsonValue]) -> ResultEP<Vec<u8>> {
    let mut frames = Vec::with_capacity(args.len() + 1);
    frames.push(Resp2OwnedFrame::BulkString(command.as_bytes().to_vec()));

    for arg in args {
        frames.push(convert_json_to_resp2(arg)?);
    }

    let array_frame = Resp2OwnedFrame::Array(frames);
    RedisProtocol::encode_to_buffer(&EncoderRespFrame::Resp2(array_frame))
}

pub fn encode_redis_command_resp3(command: &str, args: &[RedisJsonValue]) -> ResultEP<Vec<u8>> {
    let mut frames = Vec::with_capacity(args.len() + 1);
    frames.push(Resp3OwnedFrame::BlobString { data: command.as_bytes().to_vec(), attributes: None });

    for arg in args {
        frames.push(convert_json_to_resp3(arg)?);
    }

    let array_frame = Resp3OwnedFrame::Array { data: frames, attributes: None };
    RedisProtocol::encode_to_buffer(&EncoderRespFrame::Resp3(array_frame))
}

fn convert_json_to_resp2(value: &RedisJsonValue) -> ResultEP<Resp2OwnedFrame> {
    match value {
        RedisJsonValue::String(s) => Ok(Resp2OwnedFrame::BulkString(s.as_bytes().to_vec())),
        RedisJsonValue::Bytes(b) => Ok(Resp2OwnedFrame::BulkString(b.to_vec())),
        RedisJsonValue::Integer(i) => Ok(Resp2OwnedFrame::Integer(*i)),
        RedisJsonValue::Float(f) => Ok(Resp2OwnedFrame::BulkString(f.to_string().as_bytes().to_vec())),
        RedisJsonValue::Bool(b) => Ok(Resp2OwnedFrame::BulkString(b.to_string().as_bytes().to_vec())),
        RedisJsonValue::Null => Ok(Resp2OwnedFrame::Null),
        RedisJsonValue::Array(arr) => {
            let frames: Result<Vec<_>, _> = arr.iter().map(convert_json_to_resp2).collect();
            Ok(Resp2OwnedFrame::Array(frames?))
        }
        RedisJsonValue::Object(_) => Err(EpError::parse("RESP2 does not support object types")),
    }
}

pub fn convert_json_to_resp3(value: &RedisJsonValue) -> ResultEP<Resp3OwnedFrame> {
    match value {
        RedisJsonValue::String(s) => Ok(Resp3OwnedFrame::BlobString { data: s.as_bytes().to_vec(), attributes: None }),
        RedisJsonValue::Bytes(b) => Ok(Resp3OwnedFrame::ChunkedString(b.to_vec())),
        RedisJsonValue::Integer(i) => Ok(Resp3OwnedFrame::Number { data: *i, attributes: None }),
        RedisJsonValue::Float(f) => Ok(Resp3OwnedFrame::Double { data: *f, attributes: None }),
        RedisJsonValue::Bool(b) => Ok(Resp3OwnedFrame::Boolean { data: *b, attributes: None }),
        RedisJsonValue::Null => Ok(Resp3OwnedFrame::Null),
        RedisJsonValue::Array(arr) => {
            let frames: Result<Vec<_>, _> = arr.iter().map(convert_json_to_resp3).collect();
            Ok(Resp3OwnedFrame::Array { data: frames?, attributes: None })
        }
        RedisJsonValue::Object(obj) => {
            let mut map_data = FrameMap::with_capacity(obj.len());
            for (key, val) in obj {
                let key_frame = Resp3OwnedFrame::BlobString { data: key.as_bytes().to_vec(), attributes: None };
                let val_frame = convert_json_to_resp3(val)?;
                map_data.insert(key_frame, val_frame);
            }
            Ok(Resp3OwnedFrame::Map { data: map_data, attributes: None })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_encode_resp2_simple_command() {
        let result = encode_redis_command_resp2("PING", &[]).unwrap();
        let expected = b"*1\r\n$4\r\nPING\r\n";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_encode_resp2_command_with_string_args() {
        let args = vec![
            RedisJsonValue::String("key".to_string()),
            RedisJsonValue::String("value".to_string()),
        ];
        let result = encode_redis_command_resp2("SET", &args).unwrap();
        let expected = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_encode_resp2_command_with_integer() {
        let args = vec![RedisJsonValue::Integer(42)];
        let result = encode_redis_command_resp2("INCR", &args).unwrap();
        let expected = b"*2\r\n$4\r\nINCR\r\n:42\r\n";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_encode_resp2_object_fails() {
        let mut obj = HashMap::new();
        obj.insert("key".to_string(), RedisJsonValue::String("value".to_string()));
        let args = vec![RedisJsonValue::Object(obj)];
        let result = encode_redis_command_resp2("TEST", &args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("RESP2 does not support object types"));
    }

    #[test]
    fn test_encode_resp3_simple_command() {
        let result = encode_redis_command_resp3("PING", &[]).unwrap();
        let expected = b"*1\r\n$4\r\nPING\r\n";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_encode_resp3_command_with_types() {
        let args = vec![
            RedisJsonValue::String("key".to_string()),
            RedisJsonValue::Integer(123),
            RedisJsonValue::Float(1.23),
            RedisJsonValue::Bool(true),
            RedisJsonValue::Null,
        ];
        let result = encode_redis_command_resp3("TEST", &args).unwrap();
        let result_str = std::str::from_utf8(&result).unwrap();

        // Should contain the command and all argument types
        assert!(result.starts_with(b"*6\r\n$4\r\nTEST\r\n"));
        assert!(result_str.contains("$3\r\nkey\r\n")); // String
        assert!(result_str.contains(":123\r\n")); // Integer
        assert!(result_str.contains(",1.23\r\n")); // Float
        assert!(result_str.contains("#t\r\n")); // Boolean true
        assert!(result_str.contains("_\r\n")); // Null
    }

    #[test]
    fn test_encode_resp3_with_array() {
        let array = vec![
            RedisJsonValue::String("item1".to_string()),
            RedisJsonValue::String("item2".to_string()),
        ];
        let args = vec![RedisJsonValue::Array(array)];
        let result = encode_redis_command_resp3("LPUSH", &args).unwrap();
        let result_str = std::str::from_utf8(&result).unwrap();

        assert!(result.starts_with(b"*2\r\n$5\r\nLPUSH\r\n"));
        assert!(result_str.contains("*2\r\n")); // Array with 2 elements
        assert!(result_str.contains("$5\r\nitem1\r\n"));
        assert!(result_str.contains("$5\r\nitem2\r\n"));
    }

    #[test]
    fn test_encode_resp3_with_object() {
        let mut obj = HashMap::new();
        obj.insert("field1".to_string(), RedisJsonValue::String("value1".to_string()));
        obj.insert("field2".to_string(), RedisJsonValue::Integer(42));
        let args = vec![RedisJsonValue::Object(obj)];
        let result = encode_redis_command_resp3("HSET", &args).unwrap();
        let result_str = std::str::from_utf8(&result).unwrap();

        assert!(result.starts_with(b"*2\r\n$4\r\nHSET\r\n"));
        assert!(result_str.contains("%2\r\n")); // Map with 2 pairs
    }

    #[test]
    fn test_convert_json_to_resp2_types() {
        assert!(matches!(
            convert_json_to_resp2(&RedisJsonValue::String("test".to_string())).unwrap(),
            Resp2OwnedFrame::BulkString(_)
        ));

        assert!(matches!(convert_json_to_resp2(&RedisJsonValue::Integer(42)).unwrap(), Resp2OwnedFrame::Integer(42)));

        assert!(matches!(convert_json_to_resp2(&RedisJsonValue::Null).unwrap(), Resp2OwnedFrame::Null));
    }

    #[test]
    fn test_convert_json_to_resp3_types() {
        assert!(matches!(
            convert_json_to_resp3(&RedisJsonValue::String("test".to_string())).unwrap(),
            Resp3OwnedFrame::BlobString { .. }
        ));

        assert!(matches!(
            convert_json_to_resp3(&RedisJsonValue::Integer(42)).unwrap(),
            Resp3OwnedFrame::Number { data: 42, .. }
        ));

        assert!(matches!(
            convert_json_to_resp3(&RedisJsonValue::Float(1.23)).unwrap(),
            Resp3OwnedFrame::Double { .. }
        ));

        assert!(matches!(
            convert_json_to_resp3(&RedisJsonValue::Bool(true)).unwrap(),
            Resp3OwnedFrame::Boolean { data: true, .. }
        ));

        assert!(matches!(convert_json_to_resp3(&RedisJsonValue::Null).unwrap(), Resp3OwnedFrame::Null));
    }

    #[test]
    fn test_resp_frame_encode() {
        let frame = EncoderRespFrame::Resp2(Resp2OwnedFrame::BulkString(b"test".to_vec()));
        let result = RedisProtocol::encode_to_buffer(&frame).unwrap();
        assert_eq!(result, b"$4\r\ntest\r\n");

        let frame = EncoderRespFrame::Resp3(Resp3OwnedFrame::BlobString { data: b"test".to_vec(), attributes: None });
        let result = RedisProtocol::encode_to_buffer(&frame).unwrap();
        assert_eq!(result, b"$4\r\ntest\r\n");
    }
}
