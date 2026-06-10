use crate::error::IncorrectTag;
use crate::{Resp3Builder, RespConstruct, RespConstructError, RespParse, RespParseError, RespParseSync, RespRead, RespReadSync};
use wire_stream::{WireReadExt, WireReadSyncExt};

pub enum BigNumber {}

#[derive(Clone, Debug, thiserror::Error)]
pub enum BigNumberParseError {
    #[error(transparent)]
    IncorrectTag(#[from] IncorrectTag),
    #[error("invalid big number format")]
    InvalidFormat,
    #[error("line too long")]
    TooLong,
}

impl<S: RespReadSync + ?Sized> RespParseSync<S> for BigNumber {
    type ParseError = BigNumberParseError;
    type Value<'s>
        = S::ReadBorrow<'s>
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag_sync(b'(')
            .map_err(RespParseError::Stream)?
            .map_err(BigNumberParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let line = stream
            .read_to_crlf_sync(Some(1000))
            .map_err(RespParseError::Stream)?
            .map_err(|_| RespParseError::Parse(BigNumberParseError::TooLong))?;

        let bytes = &*line;
        if bytes.is_empty() {
            return Err(RespParseError::Parse(BigNumberParseError::InvalidFormat));
        }

        let digits = match bytes[0] {
            b'+' | b'-' => &bytes[1..],
            _ => bytes,
        };

        if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
            return Err(RespParseError::Parse(BigNumberParseError::InvalidFormat));
        }

        Ok(line)
    }
}

impl<S: RespRead + ?Sized> RespParse<S> for BigNumber {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag(b'(')
            .await
            .map_err(RespParseError::Stream)?
            .map_err(BigNumberParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let line = stream
            .read_to_crlf(Some(1000))
            .await
            .map_err(RespParseError::Stream)?
            .map_err(|_| RespParseError::Parse(BigNumberParseError::TooLong))?;

        let bytes = &*line;
        if bytes.is_empty() {
            return Err(RespParseError::Parse(BigNumberParseError::InvalidFormat));
        }

        let digits = match bytes[0] {
            b'+' | b'-' => &bytes[1..],
            _ => bytes,
        };

        if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
            return Err(RespParseError::Parse(BigNumberParseError::InvalidFormat));
        }

        Ok(line)
    }
}

impl<'s, S: RespRead + ?Sized + 's, B: Resp3Builder> RespConstruct<'s, S, B> for BigNumber {
    type ConstructError = <BigNumber as RespParseSync<S>>::ParseError;

    async fn construct(stream: &'s S, builder: B) -> Result<B::Output, RespConstructError<S::ReadError, B::Error, Self::ConstructError>> {
        let value = Self::parse(stream).await?;
        builder.bignum(&value).await.map_err(RespConstructError::Builder)
    }
}
