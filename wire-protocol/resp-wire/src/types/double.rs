use crate::error::IncorrectTag;
use crate::{Resp3Builder, RespConstruct, RespConstructError, RespParse, RespParseError, RespParseSync, RespRead, RespReadSync};
use wire_stream::{WireReadExt, WireReadSyncExt};

pub enum Double {}

#[derive(Clone, Debug, thiserror::Error)]
pub enum DoubleParseError {
    #[error(transparent)]
    IncorrectTag(#[from] IncorrectTag),
    #[error("invalid double format")]
    InvalidFormat,
    #[error("line too long")]
    TooLong,
}

impl<S: RespReadSync + ?Sized> RespParseSync<S> for Double {
    type ParseError = DoubleParseError;
    type Value<'s>
        = f64
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag_sync(b',')
            .map_err(RespParseError::Stream)?
            .map_err(DoubleParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let line = stream
            .read_to_crlf_sync(Some(350))
            .map_err(RespParseError::Stream)?
            .map_err(|_| RespParseError::Parse(DoubleParseError::TooLong))?;

        let str = std::str::from_utf8(&line).map_err(|_| RespParseError::Parse(DoubleParseError::InvalidFormat))?;

        let value = match str {
            "inf" => f64::INFINITY,
            "-inf" => f64::NEG_INFINITY,
            "nan" => f64::NAN,
            _ => str.parse::<f64>().map_err(|_| RespParseError::Parse(DoubleParseError::InvalidFormat))?,
        };

        Ok(value)
    }
}

impl<S: RespRead + ?Sized> RespParse<S> for Double {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag(b',')
            .await
            .map_err(RespParseError::Stream)?
            .map_err(DoubleParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let line = stream
            .read_to_crlf(Some(350))
            .await
            .map_err(RespParseError::Stream)?
            .map_err(|_| RespParseError::Parse(DoubleParseError::TooLong))?;

        let str = std::str::from_utf8(&line).map_err(|_| RespParseError::Parse(DoubleParseError::InvalidFormat))?;

        let value = match str {
            "inf" => f64::INFINITY,
            "-inf" => f64::NEG_INFINITY,
            "nan" => f64::NAN,
            _ => str.parse::<f64>().map_err(|_| RespParseError::Parse(DoubleParseError::InvalidFormat))?,
        };

        Ok(value)
    }
}

impl<'s, S: RespRead + ?Sized + 's, B: Resp3Builder> RespConstruct<'s, S, B> for Double {
    type ConstructError = <Double as RespParseSync<S>>::ParseError;

    async fn construct(stream: &'s S, builder: B) -> Result<B::Output, RespConstructError<S::ReadError, B::Error, Self::ConstructError>> {
        let value = Self::parse(stream).await?;
        let bytes = value.to_string();
        builder.bignum(bytes.as_bytes()).await.map_err(RespConstructError::Builder)
    }
}
