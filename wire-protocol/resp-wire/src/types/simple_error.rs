use crate::error::IncorrectTag;
use crate::{
    Resp2Builder, RespConstruct, RespConstructError, RespParse, RespParseError, RespParseSync, RespRead, RespReadSync, RespStringBuilder,
};
use wire_stream::{WireReadExt, WireReadSyncExt};

pub enum SimpleError {}

#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum SimpleErrorParseError {
    #[error(transparent)]
    IncorrectTag(#[from] IncorrectTag),
    #[error("incomplete input: missing CRLF terminator")]
    IncompleteInput,
}

impl<S: RespReadSync + ?Sized> RespParseSync<S> for SimpleError {
    type ParseError = SimpleErrorParseError;
    type Value<'s>
        = SimpleErrorReader<'s, S>
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag_sync(b'-')
            .map_err(RespParseError::Stream)?
            .map_err(SimpleErrorParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        Ok(SimpleErrorReader::new(stream))
    }
}

impl<S: RespRead + ?Sized> RespParse<S> for SimpleError {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag(b'-')
            .await
            .map_err(RespParseError::Stream)?
            .map_err(SimpleErrorParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        Ok(SimpleErrorReader::new(stream))
    }
}

#[derive(Debug)]
pub struct SimpleErrorReader<'s, S: RespReadSync + ?Sized + 's> {
    stream: &'s S,
    finished: bool,
}

impl<'s, S: RespReadSync + ?Sized> SimpleErrorReader<'s, S> {
    fn new(stream: &'s S) -> Self {
        Self { stream, finished: false }
    }

    /// Returns true if the reader successfully found the CRLF terminator.
    /// If this returns false after iteration completes, the input was incomplete.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub fn next_sync(&mut self) -> Result<Option<S::ReadBorrow<'s>>, S::ReadError> {
        Ok(if self.finished {
            None
        } else {
            match self.stream.read_to_crlf_sync(None)? {
                Ok(data) => {
                    self.finished = true;
                    Some(data)
                }
                Err(data) => {
                    if data.is_empty() {
                        // Stream exhausted without finding CRLF terminator.
                        None
                    } else {
                        self.stream.accept(&data, None)?;
                        Some(data)
                    }
                }
            }
        })
    }
}

impl<'s, S: RespRead + ?Sized> SimpleErrorReader<'s, S> {
    pub async fn next(&mut self) -> Result<Option<S::ReadBorrow<'s>>, S::ReadError> {
        Ok(if self.finished {
            None
        } else {
            match self.stream.read_to_crlf(None).await? {
                Ok(data) => {
                    self.finished = true;
                    Some(data)
                }
                Err(data) => {
                    self.stream.accept(&data, None)?;
                    Some(data)
                }
            }
        })
    }
}

impl<'s, S: RespRead + ?Sized + 's, B: Resp2Builder> RespConstruct<'s, S, B> for SimpleError
where
    for<'c> <B as Resp2Builder>::SimpleErrorBuilder: RespStringBuilder<Chunk<'c> = &'c [u8]>,
{
    type ConstructError = <SimpleError as RespParseSync<S>>::ParseError;

    async fn construct(stream: &'s S, builder: B) -> Result<B::Output, RespConstructError<S::ReadError, B::Error, Self::ConstructError>> {
        let mut parser = Self::parse(stream).await?;
        let mut builder = builder.simple_error().await.map_err(RespConstructError::Builder)?;

        while let Some(chunk) = parser.next().await.map_err(RespConstructError::Stream)? {
            builder.push_chunk(&*chunk).await.map_err(RespConstructError::Builder)?;
        }

        builder.finish().await.map_err(RespConstructError::Builder)
    }
}
