//! Sybase TDS parsing traits and error types.

use wire_stream::{WireRead, WireReadSync};

/// Error during Sybase TDS parsing.
#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum SybaseParseError<Serror: std::error::Error, Perror: std::error::Error> {
    #[error(transparent)]
    Stream(Serror),
    #[error(transparent)]
    Parse(Perror),
}

impl<Serror: std::error::Error, Perror: std::error::Error> SybaseParseError<Serror, Perror> {
    pub fn map_parse<P2: std::error::Error>(self, f: impl FnOnce(Perror) -> P2) -> SybaseParseError<Serror, P2> {
        match self {
            Self::Stream(e) => SybaseParseError::Stream(e),
            Self::Parse(e) => SybaseParseError::Parse(f(e)),
        }
    }
}

/// Synchronous parsing trait for complete buffers.
///
/// Use this when you have a complete TDS message in memory and want
/// maximum performance without async overhead.
pub trait SybaseParseSync<S: WireReadSync + ?Sized> {
    type ParseError: std::error::Error;
    type Value<'s>
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, SybaseParseError<S::ReadError, Self::ParseError>>
    where
        S: 's;
}

/// Asynchronous parsing trait for streaming I/O.
///
/// Use this when parsing from a network stream where you may need
/// to await more data.
pub trait SybaseParse<S: WireRead + ?Sized>: SybaseParseSync<S> {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, SybaseParseError<S::ReadError, Self::ParseError>>
    where
        S: 's;
}

/// Error during Sybase construction (parsing + building).
#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum SybaseConstructError<Serror: std::error::Error, Berror: std::error::Error, Cerror: std::error::Error> {
    #[error(transparent)]
    Stream(Serror),
    #[error(transparent)]
    Builder(Berror),
    #[error(transparent)]
    Construct(Cerror),
}

impl<Serror: std::error::Error, Berror: std::error::Error, Cerror: std::error::Error> From<SybaseParseError<Serror, Cerror>>
    for SybaseConstructError<Serror, Berror, Cerror>
{
    fn from(value: SybaseParseError<Serror, Cerror>) -> Self {
        match value {
            SybaseParseError::Stream(e) => Self::Stream(e),
            SybaseParseError::Parse(e) => Self::Construct(e),
        }
    }
}

impl<Serror: std::error::Error, Berror: std::error::Error, Cerror: std::error::Error> SybaseConstructError<Serror, Berror, Cerror> {
    pub fn map_from<Perror: std::error::Error, E: std::error::Error>(error: E, func: impl FnOnce(Perror) -> Cerror) -> Self
    where
        SybaseConstructError<Serror, Berror, Perror>: From<E>,
    {
        match SybaseConstructError::from(error) {
            SybaseConstructError::Stream(e) => Self::Stream(e),
            SybaseConstructError::Builder(e) => Self::Builder(e),
            SybaseConstructError::Construct(e) => Self::Construct(func(e)),
        }
    }

    pub fn map_result<T, Perror: std::error::Error, E: std::error::Error>(
        result: Result<T, E>,
        func: impl FnOnce(Perror) -> Cerror,
    ) -> Result<T, Self>
    where
        SybaseConstructError<Serror, Berror, Perror>: From<E>,
    {
        result.map_err(move |e| Self::map_from(e, func))
    }

    pub fn map_builder<E: std::error::Error>(error: Self, func: impl FnOnce(Berror) -> E) -> SybaseConstructError<Serror, E, Cerror> {
        match error {
            Self::Stream(e) => SybaseConstructError::Stream(e),
            Self::Builder(e) => SybaseConstructError::Builder(func(e)),
            Self::Construct(e) => SybaseConstructError::Construct(e),
        }
    }

    pub fn map_builder_result<T, E: std::error::Error>(
        result: Result<T, Self>,
        func: impl FnOnce(Berror) -> E,
    ) -> Result<T, SybaseConstructError<Serror, E, Cerror>> {
        result.map_err(move |e| Self::map_builder(e, func))
    }
}

/// Trait for constructing Sybase values using a builder.
pub trait SybaseConstruct<'s, S: WireRead + ?Sized + 's, B>: SybaseParse<S>
where
    B: SybaseBuilder,
{
    type ConstructError: std::error::Error;

    async fn construct(stream: &'s S, builder: B) -> Result<B::Output, SybaseConstructError<S::ReadError, B::Error, Self::ConstructError>>;
}

/// Builder trait for constructing Sybase values.
pub trait SybaseBuilder {
    type Output;
    type Error: std::error::Error;
}
