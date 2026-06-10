use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use bytes::Bytes;
use error::{EpError, ResultEP};
use redis_protocol::resp2::decode::decode as decode_resp2;
use redis_protocol::resp2::encode::encode as encode_resp2;
use redis_protocol::resp2::types::Resp2Frame as Resp2FrameTrait;
use redis_protocol::resp3::decode::complete::decode as decode_resp3;
use redis_protocol::resp3::encode::complete::encode as encode_resp3;
use redis_protocol::resp3::types::Resp3Frame as Resp3FrameTrait;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseCombiner {
    SumIntegers,
    ConcatArrayPreservingNils { wrongtype_as_nil: bool },
    AllOk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RespWireVersion {
    Resp2,
    Resp3,
}

impl RespWireVersion {
    pub fn from_resp3_flag(is_resp3: bool) -> Self {
        if is_resp3 { Self::Resp3 } else { Self::Resp2 }
    }

    pub fn from_protocol_version(version: u8) -> Self {
        match version {
            2 => Self::Resp2,
            _ => Self::Resp3,
        }
    }

    pub fn require_consistent(slot: &mut Option<Self>, next: Self) -> ResultEP<()> {
        match slot {
            Some(existing) if *existing != next => Err(EpError::parse("split parts produced mixed RESP versions")),
            Some(_) => Ok(()),
            None => {
                *slot = Some(next);
                Ok(())
            }
        }
    }
}

impl ResponseCombiner {
    pub fn combine_frames(&self, parts: Vec<DecoderRespFrame>) -> ResultEP<Bytes> {
        let Some(first) = parts.first() else {
            return Err(EpError::parse("cannot combine empty Redis response parts"));
        };

        match first {
            DecoderRespFrame::Resp2(_) => self.combine_resp2(parts),
            DecoderRespFrame::Resp3(_) => self.combine_resp3(parts),
        }
    }

    pub fn combine_bytes(&self, parts: Vec<Bytes>, protocol: RespWireVersion) -> ResultEP<Bytes> {
        let mut frames = Vec::with_capacity(parts.len());
        for part in parts {
            let (frame, consumed) = decode_bytes_with_version(&part, protocol)?;
            if consumed != part.len() {
                return Err(EpError::parse("RESP response part contains trailing bytes"));
            }
            frames.push(frame);
        }

        self.combine_frames(frames)
    }

    fn combine_resp2(&self, parts: Vec<DecoderRespFrame>) -> ResultEP<Bytes> {
        let mut resp2_parts = Vec::with_capacity(parts.len());
        for part in parts {
            let DecoderRespFrame::Resp2(frame) = part else {
                return Err(EpError::parse("cannot combine mixed RESP versions"));
            };
            resp2_parts.push(frame);
        }

        let frame = match self {
            ResponseCombiner::SumIntegers => combine_resp2_sum(resp2_parts),
            ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil } => combine_resp2_array(resp2_parts, *wrongtype_as_nil),
            ResponseCombiner::AllOk => combine_resp2_all_ok(resp2_parts),
        }?;

        encode_resp2_frame(&frame)
    }

    fn combine_resp3(&self, parts: Vec<DecoderRespFrame>) -> ResultEP<Bytes> {
        let mut resp3_parts = Vec::with_capacity(parts.len());
        for part in parts {
            let DecoderRespFrame::Resp3(frame) = part else {
                return Err(EpError::parse("cannot combine mixed RESP versions"));
            };
            resp3_parts.push(frame);
        }

        let frame = match self {
            ResponseCombiner::SumIntegers => combine_resp3_sum(resp3_parts),
            ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil } => combine_resp3_array(resp3_parts, *wrongtype_as_nil),
            ResponseCombiner::AllOk => combine_resp3_all_ok(resp3_parts),
        }?;

        encode_resp3_frame(&frame)
    }
}

fn combine_resp2_sum(parts: Vec<Resp2Frame>) -> ResultEP<Resp2Frame> {
    let mut sum = 0;
    for part in parts {
        match part {
            Resp2Frame::Integer(value) => sum += value,
            Resp2Frame::Error(_) => return Ok(part),
            other => return Err(EpError::parse(format!("unexpected integer response part: {other:?}"))),
        }
    }

    Ok(Resp2Frame::Integer(sum))
}

fn combine_resp3_sum(parts: Vec<Resp3Frame>) -> ResultEP<Resp3Frame> {
    let mut sum = 0;
    for part in parts {
        match part {
            Resp3Frame::Number { data, .. } => sum += data,
            Resp3Frame::SimpleError { .. } | Resp3Frame::BlobError { .. } => return Ok(part),
            other => return Err(EpError::parse(format!("unexpected integer response part: {other:?}"))),
        }
    }

    Ok(Resp3Frame::Number { data: sum, attributes: None })
}

fn combine_resp2_array(parts: Vec<Resp2Frame>, wrongtype_as_nil: bool) -> ResultEP<Resp2Frame> {
    let mut values = Vec::with_capacity(parts.len());
    for part in parts {
        if wrongtype_as_nil && resp2_is_wrongtype(&part) {
            values.push(Resp2Frame::Null);
            continue;
        }

        if matches!(part, Resp2Frame::Error(_)) {
            return Ok(part);
        }

        values.push(part);
    }

    Ok(Resp2Frame::Array(values))
}

fn combine_resp3_array(parts: Vec<Resp3Frame>, wrongtype_as_nil: bool) -> ResultEP<Resp3Frame> {
    let mut values = Vec::with_capacity(parts.len());
    for part in parts {
        if wrongtype_as_nil && resp3_is_wrongtype(&part) {
            values.push(Resp3Frame::Null);
            continue;
        }

        if matches!(part, Resp3Frame::SimpleError { .. } | Resp3Frame::BlobError { .. }) {
            return Ok(part);
        }

        values.push(part);
    }

    Ok(Resp3Frame::Array { data: values, attributes: None })
}

fn combine_resp2_all_ok(parts: Vec<Resp2Frame>) -> ResultEP<Resp2Frame> {
    for part in parts {
        match part {
            Resp2Frame::SimpleString(value) if value == b"OK" => {}
            Resp2Frame::Error(_) => return Ok(part),
            other => return Err(EpError::parse(format!("unexpected WATCH response part: {other:?}"))),
        }
    }

    Ok(Resp2Frame::SimpleString(b"OK".to_vec()))
}

fn combine_resp3_all_ok(parts: Vec<Resp3Frame>) -> ResultEP<Resp3Frame> {
    for part in parts {
        match part {
            Resp3Frame::SimpleString { data, .. } if data == b"OK" => {}
            Resp3Frame::SimpleError { .. } | Resp3Frame::BlobError { .. } => return Ok(part),
            other => return Err(EpError::parse(format!("unexpected WATCH response part: {other:?}"))),
        }
    }

    Ok(Resp3Frame::SimpleString { data: b"OK".to_vec(), attributes: None })
}

fn resp2_is_wrongtype(frame: &Resp2Frame) -> bool {
    matches!(frame, Resp2Frame::Error(error) if error_code_is_wrongtype(error))
}

fn resp3_is_wrongtype(frame: &Resp3Frame) -> bool {
    match frame {
        Resp3Frame::SimpleError { data, .. } => error_code_is_wrongtype(data),
        Resp3Frame::BlobError { data, .. } => error_code_bytes_is_wrongtype(data),
        _ => false,
    }
}

fn error_code_is_wrongtype(error: &str) -> bool {
    error.split_ascii_whitespace().next().is_some_and(|code| code.eq_ignore_ascii_case("WRONGTYPE"))
}

fn error_code_bytes_is_wrongtype(error: &[u8]) -> bool {
    error
        .split(|byte| byte.is_ascii_whitespace())
        .find(|token| !token.is_empty())
        .is_some_and(|code| code.eq_ignore_ascii_case(b"WRONGTYPE"))
}

fn encode_resp2_frame(frame: &Resp2Frame) -> ResultEP<Bytes> {
    let mut buf = vec![0; frame.encode_len(false)];
    let len = encode_resp2(&mut buf, frame, false).map_err(|error| EpError::parse(format!("RESP2 encode error: {error:?}")))?;
    buf.truncate(len);
    Ok(Bytes::from(buf))
}

fn encode_resp3_frame(frame: &Resp3Frame) -> ResultEP<Bytes> {
    let mut buf = vec![0; frame.encode_len(false)];
    let len = encode_resp3(&mut buf, frame, false).map_err(|error| EpError::parse(format!("RESP3 encode error: {error:?}")))?;
    buf.truncate(len);
    Ok(Bytes::from(buf))
}

fn decode_bytes_with_version(bytes: &[u8], protocol: RespWireVersion) -> ResultEP<(DecoderRespFrame, usize)> {
    match protocol {
        RespWireVersion::Resp3 => decode_resp3(bytes)
            .map_err(|error| EpError::parse(format!("RESP3 decode error: {error:?}")))?
            .map(|(frame, size)| (DecoderRespFrame::Resp3(frame), size))
            .ok_or_else(|| EpError::parse("incomplete RESP frame")),
        RespWireVersion::Resp2 => decode_resp2(bytes)
            .map_err(|error| EpError::parse(format!("RESP2 decode error: {error:?}")))?
            .map(|(frame, size)| (DecoderRespFrame::Resp2(frame), size))
            .ok_or_else(|| EpError::parse("incomplete RESP frame")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resp2_frame(frame: Resp2Frame) -> DecoderRespFrame {
        DecoderRespFrame::Resp2(frame)
    }

    fn resp3_frame(frame: Resp3Frame) -> DecoderRespFrame {
        DecoderRespFrame::Resp3(frame)
    }

    #[test]
    fn sum_integers_combines_resp2_and_resp3() {
        let resp2 = ResponseCombiner::SumIntegers
            .combine_frames(vec![
                resp2_frame(Resp2Frame::Integer(1)),
                resp2_frame(Resp2Frame::Integer(2)),
                resp2_frame(Resp2Frame::Integer(3)),
            ])
            .expect("combine RESP2");
        let resp3 = ResponseCombiner::SumIntegers
            .combine_frames(vec![
                resp3_frame(Resp3Frame::Number { data: 1, attributes: None }),
                resp3_frame(Resp3Frame::Number { data: 2, attributes: None }),
                resp3_frame(Resp3Frame::Number { data: 3, attributes: None }),
            ])
            .expect("combine RESP3");

        assert_eq!(resp2, Bytes::from_static(b":6\r\n"));
        assert_eq!(resp3, Bytes::from_static(b":6\r\n"));
    }

    #[test]
    fn sum_integers_propagates_first_error() {
        let output = ResponseCombiner::SumIntegers
            .combine_frames(vec![
                resp2_frame(Resp2Frame::Integer(1)),
                resp2_frame(Resp2Frame::Error("ERR no".to_string())),
                resp2_frame(Resp2Frame::Integer(3)),
            ])
            .expect("combine");

        assert_eq!(output, Bytes::from_static(b"-ERR no\r\n"));
    }

    #[test]
    fn concat_array_preserves_values_and_nils() {
        let output = ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil: true }
            .combine_frames(vec![
                resp2_frame(Resp2Frame::BulkString(b"foo".to_vec())),
                resp2_frame(Resp2Frame::Null),
                resp2_frame(Resp2Frame::BulkString(b"bar".to_vec())),
            ])
            .expect("combine");

        assert_eq!(output, Bytes::from_static(b"*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n"));
    }

    #[test]
    fn concat_array_maps_wrongtype_to_nil_when_enabled() {
        let output = ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil: true }
            .combine_frames(vec![
                resp2_frame(Resp2Frame::BulkString(b"foo".to_vec())),
                resp2_frame(Resp2Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())),
            ])
            .expect("combine");

        assert_eq!(output, Bytes::from_static(b"*2\r\n$3\r\nfoo\r\n$-1\r\n"));
    }

    #[test]
    fn concat_array_maps_wrongtype_code_case_insensitively() {
        let output = ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil: true }
            .combine_frames(vec![
                resp2_frame(Resp2Frame::BulkString(b"foo".to_vec())),
                resp2_frame(Resp2Frame::Error("wrongtype Operation against a key holding the wrong kind of value".to_string())),
            ])
            .expect("combine");

        assert_eq!(output, Bytes::from_static(b"*2\r\n$3\r\nfoo\r\n$-1\r\n"));
    }

    #[test]
    fn concat_array_propagates_non_wrongtype_errors() {
        let output = ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil: true }
            .combine_frames(vec![
                resp2_frame(Resp2Frame::BulkString(b"foo".to_vec())),
                resp2_frame(Resp2Frame::Error("ERR no".to_string())),
            ])
            .expect("combine");

        assert_eq!(output, Bytes::from_static(b"-ERR no\r\n"));
    }

    #[test]
    fn concat_array_preserves_errors_that_only_contain_wrongtype_later() {
        let output = ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil: true }
            .combine_frames(vec![
                resp2_frame(Resp2Frame::BulkString(b"foo".to_vec())),
                resp2_frame(Resp2Frame::Error("ERR wrongtype appears in the message".to_string())),
            ])
            .expect("combine");

        assert_eq!(output, Bytes::from_static(b"-ERR wrongtype appears in the message\r\n"));
    }

    #[test]
    fn all_ok_combines_resp2_and_resp3() {
        let resp2 = ResponseCombiner::AllOk
            .combine_frames(vec![
                resp2_frame(Resp2Frame::SimpleString(b"OK".to_vec())),
                resp2_frame(Resp2Frame::SimpleString(b"OK".to_vec())),
            ])
            .expect("combine RESP2");
        let resp3 = ResponseCombiner::AllOk
            .combine_frames(vec![
                resp3_frame(Resp3Frame::SimpleString { data: b"OK".to_vec(), attributes: None }),
                resp3_frame(Resp3Frame::SimpleString { data: b"OK".to_vec(), attributes: None }),
            ])
            .expect("combine RESP3");

        assert_eq!(resp2, Bytes::from_static(b"+OK\r\n"));
        assert_eq!(resp3, Bytes::from_static(b"+OK\r\n"));
    }

    #[test]
    fn all_ok_propagates_error() {
        let output = ResponseCombiner::AllOk
            .combine_frames(vec![
                resp2_frame(Resp2Frame::SimpleString(b"OK".to_vec())),
                resp2_frame(Resp2Frame::Error("ERR watch inside multi".to_string())),
            ])
            .expect("combine");

        assert_eq!(output, Bytes::from_static(b"-ERR watch inside multi\r\n"));
    }

    #[test]
    fn combine_bytes_decodes_and_combines() {
        let output = ResponseCombiner::SumIntegers
            .combine_bytes(vec![Bytes::from_static(b":4\r\n"), Bytes::from_static(b":5\r\n")], RespWireVersion::Resp2)
            .expect("combine bytes");

        assert_eq!(output, Bytes::from_static(b":9\r\n"));
    }

    #[test]
    fn concat_array_resp3_wrongtype_only_emits_resp3_nulls() {
        let output = ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil: true }
            .combine_bytes(
                vec![
                    Bytes::from_static(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
                    Bytes::from_static(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
                ],
                RespWireVersion::Resp3,
            )
            .expect("combine RESP3");

        assert_eq!(output, Bytes::from_static(b"*2\r\n_\r\n_\r\n"));
    }

    #[test]
    fn concat_array_resp2_wrongtype_only_emits_resp2_nulls() {
        let output = ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil: true }
            .combine_bytes(
                vec![
                    Bytes::from_static(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
                    Bytes::from_static(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
                ],
                RespWireVersion::Resp2,
            )
            .expect("combine RESP2");

        assert_eq!(output, Bytes::from_static(b"*2\r\n$-1\r\n$-1\r\n"));
    }

    #[test]
    fn concat_array_drops_trailing_parts_after_error() {
        let output = ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil: true }
            .combine_frames(vec![
                resp2_frame(Resp2Frame::BulkString(b"v1".to_vec())),
                resp2_frame(Resp2Frame::BulkString(b"v2".to_vec())),
                resp2_frame(Resp2Frame::Error("ERR mid-stream".to_string())),
                resp2_frame(Resp2Frame::BulkString(b"leak?".to_vec())),
            ])
            .expect("combine");

        assert_eq!(output, Bytes::from_static(b"-ERR mid-stream\r\n"));
    }

    #[test]
    fn concat_array_resp3_preserves_values_and_nils_and_wrongtype() {
        let output = ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil: true }
            .combine_frames(vec![
                resp3_frame(Resp3Frame::BlobString { data: b"foo".to_vec(), attributes: None }),
                resp3_frame(Resp3Frame::Null),
                resp3_frame(Resp3Frame::SimpleError {
                    data: "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                    attributes: None,
                }),
                resp3_frame(Resp3Frame::BlobString { data: b"bar".to_vec(), attributes: None }),
            ])
            .expect("combine RESP3");

        assert_eq!(output, Bytes::from_static(b"*4\r\n$3\r\nfoo\r\n_\r\n_\r\n$3\r\nbar\r\n"));
    }

    #[test]
    fn concat_array_resp3_preserves_errors_that_only_contain_wrongtype_later() {
        let simple = ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil: true }
            .combine_frames(vec![
                resp3_frame(Resp3Frame::BlobString { data: b"foo".to_vec(), attributes: None }),
                resp3_frame(Resp3Frame::SimpleError {
                    data: "ERR wrongtype appears in the message".to_string(),
                    attributes: None,
                }),
            ])
            .expect("combine simple error");
        let blob = ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil: true }
            .combine_frames(vec![
                resp3_frame(Resp3Frame::BlobString { data: b"foo".to_vec(), attributes: None }),
                resp3_frame(Resp3Frame::BlobError {
                    data: b"ERR wrongtype appears in the message".to_vec(),
                    attributes: None,
                }),
            ])
            .expect("combine blob error");

        assert_eq!(simple, Bytes::from_static(b"-ERR wrongtype appears in the message\r\n"));
        assert_eq!(blob, Bytes::from_static(b"!36\r\nERR wrongtype appears in the message\r\n"));
    }

    #[test]
    fn combine_bytes_rejects_trailing_bytes_in_part() {
        let result = ResponseCombiner::SumIntegers
            .combine_bytes(vec![Bytes::from_static(b":1\r\n"), Bytes::from_static(b":2\r\n:3\r\n")], RespWireVersion::Resp2);

        let err = result.expect_err("expected trailing-bytes error");
        assert!(err.to_string().contains("trailing bytes"), "unexpected error: {err}");
    }

    #[test]
    fn require_consistent_rejects_mixed_resp_versions() {
        let mut slot = None;
        RespWireVersion::require_consistent(&mut slot, RespWireVersion::Resp3).expect("first set");
        let err = RespWireVersion::require_consistent(&mut slot, RespWireVersion::Resp2).expect_err("mixed should fail");
        assert!(err.to_string().contains("mixed RESP versions"), "unexpected error: {err}");
    }
}
