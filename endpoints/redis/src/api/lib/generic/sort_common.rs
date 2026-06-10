//! Shared types for SORT and SORT_RO commands.

use crate::api::value::RedisJsonValue;
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use derive_builder::Builder;
use endpoint_types::protocol::EpProtocol;
use error::EpError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Sort order for SORT/SORT_RO commands
#[derive(
    Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, PartialEq, Eq, ToSchema, JsonSchema,
)]
pub enum SortOrder {
    #[default]
    ASC,
    DESC,
}

/// Limit clause for SORT/SORT_RO commands
#[derive(
    Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, PartialEq, Builder, ToSchema, JsonSchema,
)]
pub struct SortLimit {
    pub offset: RedisJsonValue,
    pub count: RedisJsonValue,
}

type SortOptionParseOutput = (
    Option<RedisJsonValue>,
    Option<SortLimit>,
    Option<Vec<RedisJsonValue>>,
    Option<SortOrder>,
    Option<bool>,
    usize,
);

/// Output for SORT_RO command (always returns array)
///
/// Returns the sorted elements as an array.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SortRoOutput {
    /// The sorted elements
    elements: Vec<Option<RedisJsonValue>>,
}

impl SortRoOutput {
    pub fn new(elements: Vec<Option<RedisJsonValue>>) -> Self {
        Self { elements }
    }

    /// Get the sorted elements
    pub fn elements(&self) -> &[Option<RedisJsonValue>] {
        &self.elements
    }

    /// Get count of elements
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Decode the Redis protocol response
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let elements = decode_array_frame(frame)?;
        Ok(Self { elements })
    }
}

impl Serialize for SortRoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("SortRoOutput", 1)?;
        state.serialize_field("elements", &self.elements)?;
        state.end()
    }
}

/// Output for SORT command
///
/// Returns either sorted elements (no STORE) or count of stored elements (with STORE).
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub enum SortOutput {
    /// Sorted elements when no STORE option is used
    Elements(Vec<Option<RedisJsonValue>>),
    /// Number of elements stored when STORE option is used
    StoredCount(i64),
}

impl SortOutput {
    /// Get elements if this is an Elements variant
    pub fn elements(&self) -> Option<&[Option<RedisJsonValue>]> {
        match self {
            SortOutput::Elements(e) => Some(e),
            SortOutput::StoredCount(_) => None,
        }
    }

    /// Get stored count if this is a StoredCount variant
    pub fn stored_count(&self) -> Option<i64> {
        match self {
            SortOutput::Elements(_) => None,
            SortOutput::StoredCount(c) => Some(*c),
        }
    }

    /// Check if result is from a STORE operation
    pub fn is_store_result(&self) -> bool {
        matches!(self, SortOutput::StoredCount(_))
    }

    /// Decode the Redis protocol response
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match &frame {
            // Integer response = STORE was used
            DecoderRespFrame::Resp2(Resp2Frame::Integer(i)) => Ok(SortOutput::StoredCount(*i)),
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => Ok(SortOutput::StoredCount(*data)),
            // Array response = no STORE
            _ => {
                let elements = decode_array_frame(frame)?;
                Ok(SortOutput::Elements(elements))
            }
        }
    }
}

impl Serialize for SortOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        match self {
            SortOutput::Elements(elements) => {
                let mut state = serializer.serialize_struct("SortOutput", 1)?;
                state.serialize_field("elements", elements)?;
                state.end()
            }
            SortOutput::StoredCount(count) => {
                let mut state = serializer.serialize_struct("SortOutput", 1)?;
                state.serialize_field("stored_count", count)?;
                state.end()
            }
        }
    }
}

/// Helper to decode array frames from RESP2/RESP3
fn decode_array_frame(frame: DecoderRespFrame) -> Result<Vec<Option<RedisJsonValue>>, EpError> {
    match frame {
        DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
            Resp2Frame::Array(items) => {
                let mut elements = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Resp2Frame::BulkString(bytes) => {
                            elements.push(Some(RedisJsonValue::from(String::from_utf8(bytes).map_err(EpError::parse)?)));
                        }
                        Resp2Frame::SimpleString(s) => {
                            elements.push(Some(RedisJsonValue::from(String::from_utf8(s).map_err(EpError::parse)?)));
                        }
                        Resp2Frame::Integer(i) => {
                            elements.push(Some(RedisJsonValue::Integer(i)));
                        }
                        Resp2Frame::Null => {
                            elements.push(None);
                        }
                        other => {
                            return Err(EpError::parse(format!("unexpected array element: {:?}", other)));
                        }
                    }
                }
                Ok(elements)
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected SORT response: {:?}", other))),
        },
        DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
            Resp3Frame::Array { data, .. } => {
                let mut elements = Vec::with_capacity(data.len());
                for item in data {
                    match item {
                        Resp3Frame::BlobString { data, .. } => {
                            elements.push(Some(RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?)));
                        }
                        Resp3Frame::SimpleString { data, .. } => {
                            elements.push(Some(RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?)));
                        }
                        Resp3Frame::Number { data, .. } => {
                            elements.push(Some(RedisJsonValue::Integer(data)));
                        }
                        Resp3Frame::Null => {
                            elements.push(None);
                        }
                        other => {
                            return Err(EpError::parse(format!("unexpected array element: {:?}", other)));
                        }
                    }
                }
                Ok(elements)
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected SORT response: {:?}", other))),
        },
    }
}

/// Parse common SORT options from argument list
/// Returns (by, limit, get, ord, alpha, next_index)
pub fn parse_sort_options(args: &[RedisJsonValue], start_index: usize) -> Result<SortOptionParseOutput, EpError> {
    let mut by = None;
    let mut limit = None;
    let mut get = None;
    let mut ord = None;
    let mut alpha = None;
    let mut i = start_index;

    while i < args.len() {
        if let RedisJsonValue::String(s) = &args[i] {
            match s.to_uppercase().as_str() {
                "BY" => {
                    if i + 1 >= args.len() {
                        return Err(EpError::request("BY requires a pattern"));
                    }
                    by = Some(args[i + 1].clone());
                    i += 2;
                }
                "LIMIT" => {
                    if i + 2 >= args.len() {
                        return Err(EpError::request("LIMIT requires offset and count"));
                    }
                    limit = Some(SortLimit { offset: args[i + 1].clone(), count: args[i + 2].clone() });
                    i += 3;
                }
                "GET" => {
                    if i + 1 >= args.len() {
                        return Err(EpError::request("GET requires a pattern"));
                    }
                    get.get_or_insert_with(Vec::new).push(args[i + 1].clone());
                    i += 2;
                }
                "ASC" => {
                    ord = Some(SortOrder::ASC);
                    i += 1;
                }
                "DESC" => {
                    ord = Some(SortOrder::DESC);
                    i += 1;
                }
                "ALPHA" => {
                    alpha = Some(true);
                    i += 1;
                }
                _ => {
                    // Unknown option - let caller handle it
                    break;
                }
            }
        } else {
            break;
        }
    }

    Ok((by, limit, get, ord, alpha, i))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_sort_ro_empty_array() {
        let output = SortRoOutput::decode(b"*0\r\n").unwrap();
        assert!(output.is_empty());
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn test_decode_sort_ro_array() {
        // *3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n
        let output = SortRoOutput::decode(b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n").unwrap();
        assert_eq!(output.len(), 3);
        assert_eq!(output.elements()[0], Some(RedisJsonValue::from("1")));
        assert_eq!(output.elements()[1], Some(RedisJsonValue::from("2")));
        assert_eq!(output.elements()[2], Some(RedisJsonValue::from("3")));
    }

    #[test]
    fn test_decode_sort_ro_with_nulls() {
        // Array with null element (from GET on missing key)
        let output = SortRoOutput::decode(b"*2\r\n$1\r\na\r\n$-1\r\n").unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(output.elements()[0], Some(RedisJsonValue::from("a")));
        assert_eq!(output.elements()[1], None);
    }

    #[test]
    fn test_decode_sort_elements() {
        let output = SortOutput::decode(b"*2\r\n$1\r\na\r\n$1\r\nb\r\n").unwrap();
        assert!(!output.is_store_result());
        assert!(output.elements().is_some());
        assert_eq!(output.elements().unwrap().len(), 2);
    }

    #[test]
    fn test_decode_sort_stored_count() {
        let output = SortOutput::decode(b":5\r\n").unwrap();
        assert!(output.is_store_result());
        assert_eq!(output.stored_count(), Some(5));
        assert!(output.elements().is_none());
    }

    #[test]
    fn test_decode_sort_error() {
        let err = SortRoOutput::decode(b"-WRONGTYPE Operation against a key\r\n").unwrap_err();
        assert!(err.to_string().contains("WRONGTYPE"));
    }

    #[test]
    fn test_parse_sort_options_basic() {
        let args = vec![
            RedisJsonValue::String("mykey".into()),
            RedisJsonValue::String("DESC".into()),
            RedisJsonValue::String("ALPHA".into()),
        ];
        let (by, limit, get, ord, alpha, idx) = parse_sort_options(&args, 1).unwrap();
        assert!(by.is_none());
        assert!(limit.is_none());
        assert!(get.is_none());
        assert_eq!(ord, Some(SortOrder::DESC));
        assert_eq!(alpha, Some(true));
        assert_eq!(idx, 3);
    }

    #[test]
    fn test_parse_sort_options_with_limit() {
        let args = vec![
            RedisJsonValue::String("key".into()),
            RedisJsonValue::String("LIMIT".into()),
            RedisJsonValue::Integer(0),
            RedisJsonValue::Integer(10),
        ];
        let (_, limit, _, _, _, _) = parse_sort_options(&args, 1).unwrap();
        assert!(limit.is_some());
        let l = limit.unwrap();
        assert_eq!(l.offset, RedisJsonValue::Integer(0));
        assert_eq!(l.count, RedisJsonValue::Integer(10));
    }

    #[test]
    fn test_parse_sort_options_multiple_get() {
        let args = vec![
            RedisJsonValue::String("key".into()),
            RedisJsonValue::String("GET".into()),
            RedisJsonValue::String("pattern1".into()),
            RedisJsonValue::String("GET".into()),
            RedisJsonValue::String("pattern2".into()),
        ];
        let (_, _, get, _, _, _) = parse_sort_options(&args, 1).unwrap();
        assert!(get.is_some());
        let g = get.unwrap();
        assert_eq!(g.len(), 2);
    }
}
