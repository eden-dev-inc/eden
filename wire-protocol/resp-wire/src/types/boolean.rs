use crate::error::IncorrectTag;
use crate::{Resp3Builder, RespConstruct, RespConstructError, RespParse, RespParseError, RespParseSync, RespRead, RespReadSync};

pub enum Boolean {}

#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum BooleanParseError {
    #[error(transparent)]
    IncorrectTag(#[from] IncorrectTag),
    #[error("invalid boolean value - expected 't' or 'f'")]
    InvalidValue,
}

impl<S: RespReadSync + ?Sized> RespParseSync<S> for Boolean {
    type ParseError = BooleanParseError;
    type Value<'s>
        = bool
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag_sync(b'#')
            .map_err(RespParseError::Stream)?
            .map_err(BooleanParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let data = stream.peek_exactly::<3>().map_err(RespParseError::Stream)?;

        let value = match data[0] {
            b't' => true,
            b'f' => false,
            _ => return Err(RespParseError::Parse(BooleanParseError::InvalidValue)),
        };

        if data[1] != b'\r' || data[2] != b'\n' {
            return Err(RespParseError::Parse(BooleanParseError::InvalidValue));
        }

        stream.accept_exactly(&data).map_err(RespParseError::Stream)?;
        Ok(value)
    }
}

impl<S: RespRead + ?Sized> RespParse<S> for Boolean {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag(b'#')
            .await
            .map_err(RespParseError::Stream)?
            .map_err(BooleanParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let data = stream.peek_read_exactly::<3>().await.map_err(RespParseError::Stream)?;

        let value = match data[0] {
            b't' => true,
            b'f' => false,
            _ => return Err(RespParseError::Parse(BooleanParseError::InvalidValue)),
        };

        if data[1] != b'\r' || data[2] != b'\n' {
            return Err(RespParseError::Parse(BooleanParseError::InvalidValue));
        }

        stream.accept_exactly(&data).map_err(RespParseError::Stream)?;
        Ok(value)
    }
}

impl<'s, S: RespRead + ?Sized + 's, B: Resp3Builder> RespConstruct<'s, S, B> for Boolean {
    type ConstructError = <Boolean as RespParseSync<S>>::ParseError;

    async fn construct(stream: &'s S, builder: B) -> Result<B::Output, RespConstructError<S::ReadError, B::Error, Self::ConstructError>> {
        let value = Self::parse(stream).await?;
        builder.bool(value).await.map_err(RespConstructError::Builder)
    }
}
