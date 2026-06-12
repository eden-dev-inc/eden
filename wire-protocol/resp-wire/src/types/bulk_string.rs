use crate::error::{IncorrectTag, InvalidLength};
use crate::{
    Resp2Builder, RespConstruct, RespConstructError, RespParse, RespParseError, RespParseSync, RespRead, RespReadSync, RespStringBuilder,
};
use std::cell::Cell;
use wire_stream::{WireReadExt, WireReadSyncExt};

pub enum BulkString {}

#[derive(Clone, Debug, thiserror::Error)]
pub enum BulkStringParseError {
    #[error(transparent)]
    IncorrectTag(#[from] IncorrectTag),
    #[error(transparent)]
    InvalidLength(#[from] InvalidLength),
    #[error("missing CRLF terminator")]
    MissingTerminator,
}

/// Result of parsing a bulk string - either null or a reader
pub enum BulkStringValue<'s, S: RespReadSync + ?Sized + 's> {
    Null,
    String(BulkStringReader<'s, S>),
}

impl<S: RespReadSync + ?Sized> RespParseSync<S> for BulkString {
    type ParseError = BulkStringParseError;
    type Value<'s>
        = BulkStringValue<'s, S>
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag_sync(b'$')
            .map_err(RespParseError::Stream)?
            .map_err(BulkStringParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let line = stream
            .read_to_crlf_sync(Some(22))
            .map_err(RespParseError::Stream)?
            .map_err(|_| RespParseError::Parse(BulkStringParseError::InvalidLength(InvalidLength::TooLarge)))?;

        // Check for null: $-1\r\n
        if line.len() == 2 && line[0] == b'-' && line[1] == b'1' {
            return Ok(BulkStringValue::Null);
        }

        // Fast path: manual parsing without UTF-8 conversion
        let mut len: usize = 0;
        for &b in line.iter() {
            let digit = b.wrapping_sub(b'0');
            if digit > 9 {
                return Err(RespParseError::Parse(BulkStringParseError::InvalidLength(InvalidLength::NonNumeric)));
            }
            len = len
                .checked_mul(10)
                .and_then(|v| v.checked_add(digit as usize))
                .ok_or(RespParseError::Parse(BulkStringParseError::InvalidLength(InvalidLength::TooLarge)))?;
        }

        Ok(BulkStringValue::String(BulkStringReader::new(stream, len)))
    }
}

impl<S: RespRead + ?Sized> RespParse<S> for BulkString {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag(b'$')
            .await
            .map_err(RespParseError::Stream)?
            .map_err(BulkStringParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let line = stream
            .read_to_crlf(Some(22))
            .await
            .map_err(RespParseError::Stream)?
            .map_err(|_| RespParseError::Parse(BulkStringParseError::InvalidLength(InvalidLength::TooLarge)))?;

        // Check for null: $-1\r\n
        if line.len() == 2 && line[0] == b'-' && line[1] == b'1' {
            return Ok(BulkStringValue::Null);
        }

        // Fast path: manual parsing without UTF-8 conversion
        let mut len: usize = 0;
        for &b in line.iter() {
            let digit = b.wrapping_sub(b'0');
            if digit > 9 {
                return Err(RespParseError::Parse(BulkStringParseError::InvalidLength(InvalidLength::NonNumeric)));
            }
            len = len
                .checked_mul(10)
                .and_then(|v| v.checked_add(digit as usize))
                .ok_or(RespParseError::Parse(BulkStringParseError::InvalidLength(InvalidLength::TooLarge)))?;
        }

        Ok(BulkStringValue::String(BulkStringReader::new(stream, len)))
    }
}

#[derive(Debug)]
pub struct BulkStringReader<'s, S: RespReadSync + ?Sized + 's> {
    stream: &'s S,
    remaining: Cell<usize>,
    finished: Cell<bool>,
}

impl<'s, S: RespReadSync + ?Sized> BulkStringReader<'s, S> {
    fn new(stream: &'s S, len: usize) -> Self {
        Self {
            stream,
            remaining: Cell::new(len),
            finished: Cell::new(false),
        }
    }

    pub fn remaining(&self) -> usize {
        self.remaining.get()
    }

    /// Returns true if the reader successfully consumed all data and the CRLF terminator.
    /// If this returns false after iteration completes, the input was incomplete.
    pub fn is_finished(&self) -> bool {
        self.finished.get()
    }

    /// Consume all remaining bytes at once. Only works if all data is available.
    /// This is faster than iterating with next_sync() when you have a complete buffer.
    #[inline]
    pub fn consume_all(&mut self) -> Result<Option<S::ReadBorrow<'s>>, S::ReadError> {
        if self.finished.get() {
            return Ok(None);
        }

        let remaining = self.remaining.get();
        if remaining == 0 {
            let crlf = self.stream.peek_exactly::<2>()?;
            self.stream.accept_exactly(&crlf)?;
            self.finished.set(true);
            return Ok(None);
        }

        // Try to get all data + CRLF at once
        let data = self.stream.peek(Some(remaining + 2))?;
        if data.len() >= remaining + 2 {
            let content = self.stream.subslice(&data, None, Some(remaining))?;
            self.stream.accept(&data, Some(remaining + 2))?;
            self.remaining.set(0);
            self.finished.set(true);
            return Ok(Some(content));
        }

        // Fall back to regular iteration
        self.next_sync()
    }

    pub fn next_sync(&mut self) -> Result<Option<S::ReadBorrow<'s>>, S::ReadError> {
        if self.finished.get() {
            return Ok(None);
        }

        let remaining = self.remaining.get();
        if remaining == 0 {
            let crlf = self.stream.peek_exactly::<2>()?;
            self.stream.accept_exactly(&crlf)?;
            self.finished.set(true);
            return Ok(None);
        }

        let data = self.stream.peek(Some(remaining))?;
        let available = data.len();

        // Fast path: got all remaining data in one chunk
        if available >= remaining {
            let chunk = self.stream.subslice(&data, None, Some(remaining))?;
            self.stream.accept(&data, Some(remaining))?;
            self.remaining.set(0);
            return Ok(Some(chunk));
        }

        // Stream exhausted before getting all expected data
        if available == 0 {
            return Ok(None);
        }

        // Slow path: partial data (streaming case)
        self.stream.accept(&data, None)?;
        self.remaining.set(remaining - available);

        Ok(Some(data))
    }
}

impl<'s, S: RespRead + ?Sized> BulkStringReader<'s, S> {
    pub async fn next(&mut self) -> Result<Option<S::ReadBorrow<'s>>, S::ReadError> {
        if self.finished.get() {
            return Ok(None);
        }

        let remaining = self.remaining.get();
        if remaining == 0 {
            let crlf = self.stream.peek_read_exactly::<2>().await?;
            self.stream.accept_exactly(&crlf)?;
            self.finished.set(true);
            return Ok(None);
        }

        let data = self.stream.peek_read(Some(remaining)).await?;
        let to_consume = data.len().min(remaining);
        let chunk = self.stream.subslice(&data, None, Some(to_consume))?;
        self.stream.accept(&chunk, None)?;
        self.remaining.set(remaining - to_consume);

        Ok(Some(chunk))
    }
}

impl<'s, S: RespRead + ?Sized + 's, B: Resp2Builder> RespConstruct<'s, S, B> for BulkString
where
    for<'c> <B as Resp2Builder>::BulkStringBuilder: RespStringBuilder<Chunk<'c> = &'c [u8]>,
{
    type ConstructError = <BulkString as RespParseSync<S>>::ParseError;

    async fn construct(stream: &'s S, builder: B) -> Result<B::Output, RespConstructError<S::ReadError, B::Error, Self::ConstructError>> {
        match Self::parse(stream).await? {
            BulkStringValue::Null => Ok(builder
                .bulk_string(0)
                .await
                .map_err(RespConstructError::Builder)?
                .finish()
                .await
                .map_err(RespConstructError::Builder)?),
            BulkStringValue::String(mut reader) => {
                let len = reader.remaining();
                let mut builder = builder.bulk_string(len).await.map_err(RespConstructError::Builder)?;

                while let Some(chunk) = reader.next().await.map_err(RespConstructError::Stream)? {
                    builder.push_chunk(&*chunk).await.map_err(RespConstructError::Builder)?;
                }

                Ok(builder.finish().await.map_err(RespConstructError::Builder)?)
            }
        }
    }
}
