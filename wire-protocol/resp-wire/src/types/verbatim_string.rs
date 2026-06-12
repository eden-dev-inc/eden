use crate::error::{IncorrectTag, InvalidLength};
use crate::{
    Resp3Builder, RespConstruct, RespConstructError, RespParse, RespParseError, RespParseSync, RespRead, RespReadSync, RespStringBuilder,
};
use std::cell::Cell;

pub enum VerbatimString {}

#[derive(Clone, Debug, thiserror::Error)]
pub enum VerbatimStringParseError {
    #[error(transparent)]
    IncorrectTag(#[from] IncorrectTag),
    #[error(transparent)]
    InvalidLength(#[from] InvalidLength),
    #[error("invalid encoding format - expected 3 bytes followed by ':'")]
    InvalidEncoding,
    #[error("incomplete input: missing data or CRLF terminator")]
    IncompleteInput,
}

impl<S: RespReadSync + ?Sized> RespParseSync<S> for VerbatimString {
    type ParseError = VerbatimStringParseError;
    type Value<'s>
        = VerbatimStringReader<'s, S>
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag_sync(b'=')
            .map_err(RespParseError::Stream)?
            .map_err(VerbatimStringParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let total_len = stream
            .expect_length_sync()
            .map_err(RespParseError::Stream)?
            .map_err(VerbatimStringParseError::InvalidLength)
            .map_err(RespParseError::Parse)?;

        if total_len < 4 {
            return Err(RespParseError::Parse(VerbatimStringParseError::InvalidEncoding));
        }

        let prefix = stream.peek_exactly::<4>().map_err(RespParseError::Stream)?;

        if prefix[3] != b':' {
            return Err(RespParseError::Parse(VerbatimStringParseError::InvalidEncoding));
        }

        let encoding = [prefix[0], prefix[1], prefix[2]];
        stream.accept_exactly(&prefix).map_err(RespParseError::Stream)?;

        let data_len = total_len - 4;

        Ok(VerbatimStringReader::new(stream, encoding, data_len))
    }
}

impl<S: RespRead + ?Sized> RespParse<S> for VerbatimString {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag(b'=')
            .await
            .map_err(RespParseError::Stream)?
            .map_err(VerbatimStringParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let total_len = stream
            .expect_length()
            .await
            .map_err(RespParseError::Stream)?
            .map_err(VerbatimStringParseError::InvalidLength)
            .map_err(RespParseError::Parse)?;

        if total_len < 4 {
            return Err(RespParseError::Parse(VerbatimStringParseError::InvalidEncoding));
        }

        let prefix = stream.peek_read_exactly::<4>().await.map_err(RespParseError::Stream)?;

        if prefix[3] != b':' {
            return Err(RespParseError::Parse(VerbatimStringParseError::InvalidEncoding));
        }

        let encoding = [prefix[0], prefix[1], prefix[2]];
        stream.accept_exactly(&prefix).map_err(RespParseError::Stream)?;

        let data_len = total_len - 4;

        Ok(VerbatimStringReader::new(stream, encoding, data_len))
    }
}

#[derive(Debug)]
pub struct VerbatimStringReader<'s, S: RespReadSync + ?Sized + 's> {
    stream: &'s S,
    encoding: [u8; 3],
    remaining: Cell<usize>,
    finished: Cell<bool>,
}

impl<'s, S: RespReadSync + ?Sized> VerbatimStringReader<'s, S> {
    fn new(stream: &'s S, encoding: [u8; 3], len: usize) -> Self {
        Self {
            stream,
            encoding,
            remaining: Cell::new(len),
            finished: Cell::new(false),
        }
    }

    pub fn encoding(&self) -> [u8; 3] {
        self.encoding
    }

    pub fn remaining(&self) -> usize {
        self.remaining.get()
    }

    /// Returns true if the reader successfully consumed all data and the CRLF terminator.
    pub fn is_finished(&self) -> bool {
        self.finished.get()
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
        let to_consume = data.len().min(remaining);

        // Stream exhausted before getting all expected data
        if to_consume == 0 {
            return Ok(None);
        }

        let chunk = self.stream.subslice(&data, None, Some(to_consume))?;
        self.stream.accept(&chunk, None)?;
        self.remaining.set(remaining - to_consume);

        Ok(Some(chunk))
    }
}

impl<'s, S: RespRead + ?Sized> VerbatimStringReader<'s, S> {
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

impl<'s, S: RespRead + ?Sized + 's, B: Resp3Builder> RespConstruct<'s, S, B> for VerbatimString
where
    for<'c> <B as Resp3Builder>::VerbatimStringBuilder: RespStringBuilder<Chunk<'c> = &'c [u8]>,
{
    type ConstructError = <VerbatimString as RespParseSync<S>>::ParseError;

    async fn construct(stream: &'s S, builder: B) -> Result<B::Output, RespConstructError<S::ReadError, B::Error, Self::ConstructError>> {
        let mut reader = Self::parse(stream).await?;
        let len = reader.remaining();
        let encoding = reader.encoding();
        let mut builder = builder.verbatim_string(len, encoding).await.map_err(RespConstructError::Builder)?;

        while let Some(chunk) = reader.next().await.map_err(RespConstructError::Stream)? {
            builder.push_chunk(&*chunk).await.map_err(RespConstructError::Builder)?;
        }

        builder.finish().await.map_err(RespConstructError::Builder)
    }
}
