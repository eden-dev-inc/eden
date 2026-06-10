//! Zero-allocation pipeline support for extracting RESP values as borrowed slices.

use crate::RespParseError;
use crate::error::InvalidLength;
use wire_stream::{WireReadSync, WireReadSyncExt};

/// A single RESP value as a borrowed slice from the stream.
#[derive(Copy, Clone, Debug)]
pub struct RespSlice<'a> {
    /// The RESP type tag (e.g., b'+', b':', b'$', etc.)
    pub tag: u8,
    /// The complete raw bytes of this value (including tag and CRLF terminators).
    pub raw: &'a [u8],
}

impl<'a> RespSlice<'a> {
    /// Returns the payload without the tag byte.
    #[inline]
    pub fn payload(&self) -> &'a [u8] {
        &self.raw[1..]
    }

    /// Returns true if this is an aggregate type (array, map, set, push, attributes).
    #[inline]
    pub fn is_aggregate(&self) -> bool {
        matches!(self.tag, b'*' | b'%' | b'~' | b'>' | b'|')
    }
}

/// Zero-allocation pipeline iterator over RESP values.
pub struct Pipeline<'s, S: WireReadSync + ?Sized> {
    stream: &'s S,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("unexpected end of data")]
    UnexpectedEnd,

    #[error(transparent)]
    InvalidLength(#[from] InvalidLength),

    #[error("invalid CRLF terminator")]
    InvalidTerminator,

    #[error("unknown tag: {0}")]
    UnknownTag(u8),
}

impl<'s, S: WireReadSync + ?Sized> Pipeline<'s, S> {
    #[inline]
    pub fn new(stream: &'s S) -> Self {
        Self { stream }
    }

    #[inline]
    pub fn stream(&self) -> &'s S {
        self.stream
    }

    /// Skip a complete RESP value (including nested elements for aggregates).
    /// Returns `false` if no data remains.
    pub fn skip(&mut self) -> Result<bool, RespParseError<S::ReadError, PipelineError>> {
        let peek = match self.stream.peek(Some(1)) {
            Ok(b) if b.is_empty() => return Ok(false),
            Ok(b) => b,
            Err(e) => return Err(RespParseError::Stream(e)),
        };

        let tag = peek[0];
        self.stream.accept(&peek, None).map_err(RespParseError::Stream)?;

        match tag {
            // Simple types: read to CRLF
            b'+' | b'-' | b':' | b',' | b'(' | b'#' | b'_' => {
                self.skip_to_crlf()?;
            }

            // Bulk types: length + CRLF + data + CRLF
            b'$' | b'!' | b'=' => {
                let len = self.read_length()?;
                if len >= 0 {
                    self.stream.advance_by(len as usize + 2).map_err(RespParseError::Stream)?;
                }
            }

            // Aggregate types - recursively skip elements
            b'*' | b'~' | b'>' => {
                let len = self.read_length()?;
                if len >= 0 {
                    for _ in 0..len {
                        self.skip()?;
                    }
                }
            }

            // Map-like (key-value pairs)
            b'%' | b'|' => {
                let len = self.read_length()?;
                if len >= 0 {
                    for _ in 0..(len * 2) {
                        self.skip()?;
                    }
                }
            }

            _ => return Err(RespParseError::Parse(PipelineError::UnknownTag(tag))),
        }

        Ok(true)
    }

    /// Skip a complete RESP value and return its consumed byte length.
    /// Returns `None` if no data remains.
    pub fn skip_len(&mut self) -> Result<Option<usize>, RespParseError<S::ReadError, PipelineError>> {
        let start = self.stream.position();

        if !self.skip()? {
            return Ok(None);
        }

        self.stream.offset_from(&start).map(Some).map_err(RespParseError::Stream)
    }

    /// Returns the next complete RESP value as raw bytes.
    pub fn next_raw(&mut self) -> Result<Option<&'s [u8]>, RespParseError<S::ReadError, PipelineError>> {
        let start = self.stream.position();

        if !self.skip()? {
            return Ok(None);
        }

        let offset = self.stream.offset_from(&start).map_err(RespParseError::Stream)?;

        self.stream.restore_to(&start).map_err(RespParseError::Stream)?;

        let raw_borrow = self.stream.peek(Some(offset)).map_err(RespParseError::Stream)?;

        self.stream.accept(&raw_borrow, None).map_err(RespParseError::Stream)?;

        // SAFETY: The borrow references data from the stream's underlying buffer
        // which has lifetime 's.
        let raw: &'s [u8] = unsafe { std::mem::transmute::<&[u8], &'s [u8]>(&*raw_borrow) };

        Ok(Some(raw))
    }

    /// Returns the next value with its tag for easy dispatch.
    pub fn next_tagged(&mut self) -> Result<Option<RespSlice<'s>>, RespParseError<S::ReadError, PipelineError>> {
        match self.next_raw()? {
            Some(raw) if !raw.is_empty() => Ok(Some(RespSlice { tag: raw[0], raw })),
            _ => Ok(None),
        }
    }

    /// Iterate over all top-level values without allocating.
    pub fn for_each<F>(&mut self, mut f: F) -> Result<(), RespParseError<S::ReadError, PipelineError>>
    where
        F: FnMut(&'s [u8]) -> bool,
    {
        while let Some(raw) = self.next_raw()? {
            if !f(raw) {
                break;
            }
        }
        Ok(())
    }

    /// Iterate with tag info for easy dispatch.
    pub fn for_each_tagged<F>(&mut self, mut f: F) -> Result<(), RespParseError<S::ReadError, PipelineError>>
    where
        F: FnMut(RespSlice<'s>) -> bool,
    {
        while let Some(slice) = self.next_tagged()? {
            if !f(slice) {
                break;
            }
        }
        Ok(())
    }

    /// Count remaining top-level RESP values.
    pub fn count(&mut self) -> Result<usize, RespParseError<S::ReadError, PipelineError>> {
        let mut count = 0;
        while self.skip()? {
            count += 1;
        }
        Ok(count)
    }

    #[inline]
    fn skip_to_crlf(&mut self) -> Result<(), RespParseError<S::ReadError, PipelineError>> {
        match self.stream.read_to_crlf_sync(None).map_err(RespParseError::Stream)? {
            Ok(_) => Ok(()),
            Err(_) => Err(RespParseError::Parse(PipelineError::UnexpectedEnd)),
        }
    }

    #[inline]
    fn read_length(&mut self) -> Result<i64, RespParseError<S::ReadError, PipelineError>> {
        let line = match self.stream.read_to_crlf_sync(Some(22)).map_err(RespParseError::Stream)? {
            Ok(line) => line,
            Err(partial) if partial.len() >= 22 => {
                return Err(RespParseError::Parse(PipelineError::InvalidLength(InvalidLength::TooLarge)));
            }
            Err(_) => return Err(RespParseError::Parse(PipelineError::UnexpectedEnd)),
        };

        parse_length(&line).map_err(|e| RespParseError::Parse(e.into()))
    }
}

#[inline]
fn parse_length(bytes: &[u8]) -> Result<i64, InvalidLength> {
    if bytes.is_empty() {
        return Err(InvalidLength::NonNumeric);
    }

    // Handle null: -1
    if bytes.len() == 2 && bytes[0] == b'-' && bytes[1] == b'1' {
        return Ok(-1);
    }

    let mut len: i64 = 0;
    for &b in bytes {
        let digit = b.wrapping_sub(b'0');
        if digit > 9 {
            return Err(InvalidLength::NonNumeric);
        }
        len = len.checked_mul(10).and_then(|v| v.checked_add(digit as i64)).ok_or(InvalidLength::TooLarge)?;
    }
    Ok(len)
}

/// Extension trait for creating pipelines from streams.
pub trait PipelineExt: WireReadSync {
    fn pipeline(&self) -> Pipeline<'_, Self> {
        Pipeline::new(self)
    }
}

impl<S: WireReadSync + ?Sized> PipelineExt for S {}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_pipeline_next_raw() {
        let data = b"+OK\r\n:42\r\n$5\r\nhello\r\n";
        let stream = SliceStream::new(data);
        let mut pipeline = stream.pipeline();

        assert_eq!(pipeline.next_raw().expect("").expect(""), b"+OK\r\n");
        assert_eq!(pipeline.next_raw().expect("").expect(""), b":42\r\n");
        assert_eq!(pipeline.next_raw().expect("").expect(""), b"$5\r\nhello\r\n");
        assert!(pipeline.next_raw().expect("").is_none());
    }

    #[test]
    fn test_pipeline_skip_array() {
        let data = b"*3\r\n:1\r\n:2\r\n:3\r\n+NEXT\r\n";
        let stream = SliceStream::new(data);
        let mut pipeline = stream.pipeline();

        assert!(pipeline.skip().expect(""));
        assert_eq!(pipeline.next_raw().expect("").expect(""), b"+NEXT\r\n");
    }

    #[test]
    fn test_pipeline_skip_len() {
        let data = b"+OK\r\n:42\r\n$5\r\nhello\r\n";
        let stream = SliceStream::new(data);
        let mut pipeline = stream.pipeline();

        assert_eq!(pipeline.skip_len().expect(""), Some(b"+OK\r\n".len()));
        assert_eq!(pipeline.skip_len().expect(""), Some(b":42\r\n".len()));
        assert_eq!(pipeline.skip_len().expect(""), Some(b"$5\r\nhello\r\n".len()));
        assert_eq!(pipeline.skip_len().expect(""), None);
    }

    #[test]
    fn test_pipeline_skip_len_nested_array() {
        let data = b"*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n+NEXT\r\n";
        let stream = SliceStream::new(data);
        let mut pipeline = stream.pipeline();

        assert_eq!(pipeline.skip_len().expect(""), Some(data.len() - b"+NEXT\r\n".len()));
        assert_eq!(pipeline.next_raw().expect("").expect(""), b"+NEXT\r\n");
    }

    #[test]
    fn test_pipeline_partial_frame_is_unexpected_end() {
        let data = b"$5\r\nhe";
        let stream = SliceStream::new(data);
        let mut pipeline = stream.pipeline();

        assert!(matches!(
            pipeline.skip_len(),
            Err(RespParseError::Stream(wire_stream::SliceReadError::NotEnoughData))
                | Err(RespParseError::Parse(PipelineError::UnexpectedEnd))
        ));
    }

    #[test]
    fn test_pipeline_nested_array() {
        let data = b"*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n";
        let stream = SliceStream::new(data);
        let mut pipeline = stream.pipeline();

        assert_eq!(pipeline.next_raw().expect("").expect(""), data.as_slice());
    }

    #[test]
    fn test_for_each_tagged() {
        let data = b"+OK\r\n:42\r\n$5\r\nhello\r\n";
        let stream = SliceStream::new(data);
        let mut pipeline = stream.pipeline();

        let mut tags = Vec::new();
        pipeline
            .for_each_tagged(|slice| {
                tags.push(slice.tag);
                true
            })
            .expect("");

        assert_eq!(tags, vec![b'+', b':', b'$']);
    }
}
