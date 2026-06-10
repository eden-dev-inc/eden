//! RESP parsing traits and error types.

use crate::RespBuilderParserError;
use crate::builder::RespBuilder;
use wire_stream::{WireRead, WireReadSync};

/// Error during RESP parsing.
#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum RespParseError<Serror: std::error::Error, Perror: std::error::Error> {
    #[error(transparent)]
    Stream(Serror),
    #[error(transparent)]
    Parse(Perror),
}

impl<Serror: std::error::Error, Perror: std::error::Error> RespParseError<Serror, Perror> {
    pub fn map_parse<P2: std::error::Error>(self, f: impl FnOnce(Perror) -> P2) -> RespParseError<Serror, P2> {
        match self {
            Self::Stream(e) => RespParseError::Stream(e),
            Self::Parse(e) => RespParseError::Parse(f(e)),
        }
    }
}

impl<Serror, Berror, Cerror, Perror> From<RespBuilderParserError<Serror, Perror, Berror>> for RespConstructError<Serror, Berror, Cerror>
where
    Serror: std::error::Error,
    Berror: std::error::Error,
    Cerror: std::error::Error + From<Perror>,
    Perror: std::error::Error,
{
    fn from(value: RespBuilderParserError<Serror, Perror, Berror>) -> Self {
        match value {
            RespBuilderParserError::Stream(e) => Self::Stream(e),
            RespBuilderParserError::Parser(e) => Self::Construct(e.into()),
            RespBuilderParserError::Builder(e) => Self::Builder(e),
        }
    }
}

/// Synchronous parsing trait for complete buffers.
///
/// Use this when you have a complete message in memory and want
/// maximum performance without async overhead.
pub trait RespParseSync<S: WireReadSync + ?Sized> {
    type ParseError: std::error::Error;
    type Value<'s>
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's;
}

/// Asynchronous parsing trait for streaming I/O.
///
/// Use this when parsing from a network stream where you may need
/// to await more data.
pub trait RespParse<S: WireRead + ?Sized>: RespParseSync<S> {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's;
}

/// Error during RESP construction (parsing + building).
#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum RespConstructError<Serror: std::error::Error, Berror: std::error::Error, Cerror: std::error::Error> {
    #[error(transparent)]
    Stream(Serror),
    #[error(transparent)]
    Builder(Berror),
    #[error(transparent)]
    Construct(Cerror),
}

impl<Serror: std::error::Error, Berror: std::error::Error, Cerror: std::error::Error> From<RespParseError<Serror, Cerror>>
    for RespConstructError<Serror, Berror, Cerror>
{
    fn from(value: RespParseError<Serror, Cerror>) -> Self {
        match value {
            RespParseError::Stream(e) => Self::Stream(e),
            RespParseError::Parse(e) => Self::Construct(e),
        }
    }
}

impl<Serror: std::error::Error, Berror: std::error::Error, Cerror: std::error::Error> RespConstructError<Serror, Berror, Cerror> {
    pub fn map_from<Perror: std::error::Error, E: std::error::Error>(error: E, func: impl FnOnce(Perror) -> Cerror) -> Self
    where
        RespConstructError<Serror, Berror, Perror>: From<E>,
    {
        match RespConstructError::from(error) {
            RespConstructError::Stream(e) => Self::Stream(e),
            RespConstructError::Builder(e) => Self::Builder(e),
            RespConstructError::Construct(e) => Self::Construct(func(e)),
        }
    }

    pub fn map_result<T, Perror: std::error::Error, E: std::error::Error>(
        result: Result<T, E>,
        func: impl FnOnce(Perror) -> Cerror,
    ) -> Result<T, Self>
    where
        RespConstructError<Serror, Berror, Perror>: From<E>,
    {
        result.map_err(move |e| Self::map_from(e, func))
    }

    pub fn map_builder<E: std::error::Error>(error: Self, func: impl FnOnce(Berror) -> E) -> RespConstructError<Serror, E, Cerror> {
        match error {
            Self::Stream(e) => RespConstructError::Stream(e),
            Self::Builder(e) => RespConstructError::Builder(func(e)),
            Self::Construct(e) => RespConstructError::Construct(e),
        }
    }

    pub fn map_builder_result<T, E: std::error::Error>(
        result: Result<T, Self>,
        func: impl FnOnce(Berror) -> E,
    ) -> Result<T, RespConstructError<Serror, E, Cerror>> {
        result.map_err(move |e| Self::map_builder(e, func))
    }
}

/// Trait for constructing RESP values using a builder.
pub trait RespConstruct<'s, S: WireRead + ?Sized + 's, B: RespBuilder>: RespParse<S> {
    type ConstructError: std::error::Error;

    async fn construct(stream: &'s S, builder: B) -> Result<B::Output, RespConstructError<S::ReadError, B::Error, Self::ConstructError>>;
}
