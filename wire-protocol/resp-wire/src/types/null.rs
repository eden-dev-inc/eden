use crate::error::IncorrectTag;
use crate::{Resp3Builder, RespConstruct, RespConstructError, RespParse, RespParseError, RespParseSync, RespRead, RespReadSync};

pub enum Null {}

#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum NullParseError {
    #[error(transparent)]
    IncorrectTag(#[from] IncorrectTag),
    #[error("invalid null format - expected CRLF")]
    InvalidFormat,
}

impl<S: RespReadSync + ?Sized> RespParseSync<S> for Null {
    type ParseError = NullParseError;
    type Value<'s>
        = ()
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag_sync(b'_')
            .map_err(RespParseError::Stream)?
            .map_err(NullParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let crlf = stream.peek_exactly::<2>().map_err(RespParseError::Stream)?;

        if *crlf != *b"\r\n" {
            return Err(RespParseError::Parse(NullParseError::InvalidFormat));
        }

        stream.accept_exactly(&crlf).map_err(RespParseError::Stream)?;
        Ok(())
    }
}

impl<S: RespRead + ?Sized> RespParse<S> for Null {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag(b'_')
            .await
            .map_err(RespParseError::Stream)?
            .map_err(NullParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let crlf = stream.peek_read_exactly::<2>().await.map_err(RespParseError::Stream)?;

        if *crlf != *b"\r\n" {
            return Err(RespParseError::Parse(NullParseError::InvalidFormat));
        }

        stream.accept_exactly(&crlf).map_err(RespParseError::Stream)?;
        Ok(())
    }
}

impl<'s, S: RespRead + ?Sized + 's, B: Resp3Builder> RespConstruct<'s, S, B> for Null {
    type ConstructError = <Null as RespParseSync<S>>::ParseError;

    async fn construct(stream: &'s S, builder: B) -> Result<B::Output, RespConstructError<S::ReadError, B::Error, Self::ConstructError>> {
        Self::parse(stream).await?;
        builder.null().await.map_err(RespConstructError::Builder)
    }
}
