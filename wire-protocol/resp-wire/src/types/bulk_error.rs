use crate::error::{IncorrectTag, InvalidLength};
use crate::{
    Resp3Builder, RespConstruct, RespConstructError, RespParse, RespParseError, RespParseSync, RespRead, RespReadSync, RespStringBuilder,
};
use std::cell::Cell;

pub enum BulkError {}

#[derive(Clone, Debug, thiserror::Error)]
pub enum BulkErrorParseError {
    #[error(transparent)]
    IncorrectTag(#[from] IncorrectTag),
    #[error(transparent)]
    InvalidLength(#[from] InvalidLength),
    #[error("incomplete input: missing data or CRLF terminator")]
    IncompleteInput,
}

impl<S: RespReadSync + ?Sized> RespParseSync<S> for BulkError {
    type ParseError = BulkErrorParseError;
    type Value<'s>
        = BulkErrorReader<'s, S>
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag_sync(b'!')
            .map_err(RespParseError::Stream)?
            .map_err(BulkErrorParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let len = stream
            .expect_length_sync()
            .map_err(RespParseError::Stream)?
            .map_err(BulkErrorParseError::InvalidLength)
            .map_err(RespParseError::Parse)?;

        Ok(BulkErrorReader::new(stream, len))
    }
}

impl<S: RespRead + ?Sized> RespParse<S> for BulkError {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag(b'!')
            .await
            .map_err(RespParseError::Stream)?
            .map_err(BulkErrorParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let len = stream
            .expect_length()
            .await
            .map_err(RespParseError::Stream)?
            .map_err(BulkErrorParseError::InvalidLength)
            .map_err(RespParseError::Parse)?;

        Ok(BulkErrorReader::new(stream, len))
    }
}

#[derive(Debug)]
pub struct BulkErrorReader<'s, S: RespReadSync + ?Sized + 's> {
    stream: &'s S,
    remaining: Cell<usize>,
    finished: Cell<bool>,
}

impl<'s, S: RespReadSync + ?Sized> BulkErrorReader<'s, S> {
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

impl<'s, S: RespRead + ?Sized> BulkErrorReader<'s, S> {
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

impl<'s, S: RespRead + ?Sized + 's, B: Resp3Builder> RespConstruct<'s, S, B> for BulkError
where
    for<'c> <B as Resp3Builder>::BulkErrorBuilder: RespStringBuilder<Chunk<'c> = &'c [u8]>,
{
    type ConstructError = <BulkError as RespParseSync<S>>::ParseError;

    async fn construct(stream: &'s S, builder: B) -> Result<B::Output, RespConstructError<S::ReadError, B::Error, Self::ConstructError>> {
        let mut reader = Self::parse(stream).await?;
        let len = reader.remaining();
        let mut builder = builder.bulk_error(len).await.map_err(RespConstructError::Builder)?;

        while let Some(chunk) = reader.next().await.map_err(RespConstructError::Stream)? {
            builder.push_chunk(&*chunk).await.map_err(RespConstructError::Builder)?;
        }

        builder.finish().await.map_err(RespConstructError::Builder)
    }
}
