//! Oracle TNS parsing traits and error types.

use wire_stream::{WireRead, WireReadSync};

/// Error during Oracle TNS parsing.
#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum OracleParseError<Serror: std::error::Error, Perror: std::error::Error> {
    #[error(transparent)]
    Stream(Serror),
    #[error(transparent)]
    Parse(Perror),
}

impl<Serror: std::error::Error, Perror: std::error::Error> OracleParseError<Serror, Perror> {
    pub fn map_parse<P2: std::error::Error>(self, f: impl FnOnce(Perror) -> P2) -> OracleParseError<Serror, P2> {
        match self {
            Self::Stream(e) => OracleParseError::Stream(e),
            Self::Parse(e) => OracleParseError::Parse(f(e)),
        }
    }
}

/// Synchronous parsing trait for complete buffers.
///
/// Use this when you have a complete TNS message in memory and want
/// maximum performance without async overhead.
pub trait OracleParseSync<S: WireReadSync + ?Sized> {
    type ParseError: std::error::Error;
    type Value<'s>
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's;
}

/// Asynchronous parsing trait for streaming I/O.
///
/// Use this when parsing from a network stream where you may need
/// to await more data.
pub trait OracleParse<S: WireRead + ?Sized>: OracleParseSync<S> {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's;
}

/// Error during Oracle construction (parsing + building).
#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum OracleConstructError<Serror: std::error::Error, Berror: std::error::Error, Cerror: std::error::Error> {
    #[error(transparent)]
    Stream(Serror),
    #[error(transparent)]
    Builder(Berror),
    #[error(transparent)]
    Construct(Cerror),
}

impl<Serror: std::error::Error, Berror: std::error::Error, Cerror: std::error::Error> From<OracleParseError<Serror, Cerror>>
    for OracleConstructError<Serror, Berror, Cerror>
{
    fn from(value: OracleParseError<Serror, Cerror>) -> Self {
        match value {
            OracleParseError::Stream(e) => Self::Stream(e),
            OracleParseError::Parse(e) => Self::Construct(e),
        }
    }
}

impl<Serror: std::error::Error, Berror: std::error::Error, Cerror: std::error::Error> OracleConstructError<Serror, Berror, Cerror> {
    pub fn map_from<Perror: std::error::Error, E: std::error::Error>(error: E, func: impl FnOnce(Perror) -> Cerror) -> Self
    where
        OracleConstructError<Serror, Berror, Perror>: From<E>,
    {
        match OracleConstructError::from(error) {
            OracleConstructError::Stream(e) => Self::Stream(e),
            OracleConstructError::Builder(e) => Self::Builder(e),
            OracleConstructError::Construct(e) => Self::Construct(func(e)),
        }
    }

    pub fn map_result<T, Perror: std::error::Error, E: std::error::Error>(
        result: Result<T, E>,
        func: impl FnOnce(Perror) -> Cerror,
    ) -> Result<T, Self>
    where
        OracleConstructError<Serror, Berror, Perror>: From<E>,
    {
        result.map_err(move |e| Self::map_from(e, func))
    }

    pub fn map_builder<E: std::error::Error>(error: Self, func: impl FnOnce(Berror) -> E) -> OracleConstructError<Serror, E, Cerror> {
        match error {
            Self::Stream(e) => OracleConstructError::Stream(e),
            Self::Builder(e) => OracleConstructError::Builder(func(e)),
            Self::Construct(e) => OracleConstructError::Construct(e),
        }
    }

    pub fn map_builder_result<T, E: std::error::Error>(
        result: Result<T, Self>,
        func: impl FnOnce(Berror) -> E,
    ) -> Result<T, OracleConstructError<Serror, E, Cerror>> {
        result.map_err(move |e| Self::map_builder(e, func))
    }
}

/// Trait for constructing Oracle values using a builder.
pub trait OracleConstruct<'s, S: WireRead + ?Sized + 's, B>: OracleParse<S>
where
    B: OracleBuilder,
{
    type ConstructError: std::error::Error;

    async fn construct(stream: &'s S, builder: B) -> Result<B::Output, OracleConstructError<S::ReadError, B::Error, Self::ConstructError>>;
}

/// Builder trait for constructing Oracle values.
pub trait OracleBuilder {
    type Output;
    type Error: std::error::Error;
}
