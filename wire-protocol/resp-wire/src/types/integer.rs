use crate::error::IncorrectTag;
use crate::{Resp2Builder, RespConstruct, RespConstructError, RespParse, RespParseError, RespParseSync, RespRead, RespReadSync};
use wire_stream::{WireReadExt, WireReadSyncExt};

pub enum Integer {}

#[derive(Clone, Debug, thiserror::Error)]
pub enum IntegerParseError {
    #[error(transparent)]
    IncorrectTag(#[from] IncorrectTag),
    #[error("invalid integer format")]
    InvalidFormat,
    #[error("integer overflow")]
    Overflow,
}

impl<S: RespReadSync + ?Sized> RespParseSync<S> for Integer {
    type ParseError = IntegerParseError;
    type Value<'s>
        = i128
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag_sync(b':')
            .map_err(RespParseError::Stream)?
            .map_err(IntegerParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let line = stream
            .read_to_crlf_sync(Some(41))
            .map_err(RespParseError::Stream)?
            .map_err(|_| RespParseError::Parse(IntegerParseError::Overflow))?;

        parse_signed_integer(&line).map_err(RespParseError::Parse)
    }
}

impl<S: RespRead + ?Sized> RespParse<S> for Integer {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag(b':')
            .await
            .map_err(RespParseError::Stream)?
            .map_err(IntegerParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let line = stream
            .read_to_crlf(Some(41))
            .await
            .map_err(RespParseError::Stream)?
            .map_err(|_| RespParseError::Parse(IntegerParseError::Overflow))?;

        parse_signed_integer(&line).map_err(RespParseError::Parse)
    }
}

pub(crate) fn parse_signed_integer(bytes: &[u8]) -> Result<i128, IntegerParseError> {
    if bytes.is_empty() {
        return Err(IntegerParseError::InvalidFormat);
    }

    let (negative, digits) = match bytes[0] {
        b'-' => (true, &bytes[1..]),
        b'+' => (false, &bytes[1..]),
        _ => (false, bytes),
    };

    if digits.is_empty() {
        return Err(IntegerParseError::InvalidFormat);
    }

    // Manual parsing - faster than str::parse()
    // Build as negative to handle i128::MIN correctly (|i128::MIN| > i128::MAX)
    let mut value: i128 = 0;
    for &b in digits {
        let digit = b.wrapping_sub(b'0');
        if digit > 9 {
            return Err(IntegerParseError::InvalidFormat);
        }
        value = value.checked_mul(10).and_then(|v| v.checked_sub(digit as i128)).ok_or(IntegerParseError::Overflow)?;
    }

    if negative {
        Ok(value)
    } else {
        value.checked_neg().ok_or(IntegerParseError::Overflow)
    }
}

impl<'s, S: RespRead + ?Sized + 's, B: Resp2Builder> RespConstruct<'s, S, B> for Integer {
    type ConstructError = <Integer as RespParseSync<S>>::ParseError;

    async fn construct(stream: &'s S, builder: B) -> Result<B::Output, RespConstructError<S::ReadError, B::Error, Self::ConstructError>> {
        let value = Self::parse(stream).await?;
        builder.integer(value).await.map_err(RespConstructError::Builder)
    }
}
