//! MySQL wire protocol parsing traits and error types.

use wire_stream::{WireRead, WireReadSync};

/// Error during MySQL parsing.
#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum MysqlParseError<Serror: std::error::Error, Perror: std::error::Error> {
    #[error(transparent)]
    Stream(Serror),
    #[error(transparent)]
    Parse(Perror),
}

impl<Serror: std::error::Error, Perror: std::error::Error> MysqlParseError<Serror, Perror> {
    pub fn map_parse<P2: std::error::Error>(self, f: impl FnOnce(Perror) -> P2) -> MysqlParseError<Serror, P2> {
        match self {
            Self::Stream(e) => MysqlParseError::Stream(e),
            Self::Parse(e) => MysqlParseError::Parse(f(e)),
        }
    }
}

/// Synchronous parsing trait for complete buffers.
///
/// Use this when you have a complete MySQL message in memory and want
/// maximum performance without async overhead.
pub trait MysqlParseSync<S: WireReadSync + ?Sized> {
    type ParseError: std::error::Error;
    type Value<'s>
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's;
}

/// Asynchronous parsing trait for streaming I/O.
///
/// Use this when parsing from a network stream where you may need
/// to await more data.
pub trait MysqlParse<S: WireRead + ?Sized>: MysqlParseSync<S> {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's;
}

/// Error during MySQL construction (parsing + building).
#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum MysqlConstructError<Serror: std::error::Error, Berror: std::error::Error, Cerror: std::error::Error> {
    #[error(transparent)]
    Stream(Serror),
    #[error(transparent)]
    Builder(Berror),
    #[error(transparent)]
    Construct(Cerror),
}

impl<Serror: std::error::Error, Berror: std::error::Error, Cerror: std::error::Error> From<MysqlParseError<Serror, Cerror>>
    for MysqlConstructError<Serror, Berror, Cerror>
{
    fn from(value: MysqlParseError<Serror, Cerror>) -> Self {
        match value {
            MysqlParseError::Stream(e) => Self::Stream(e),
            MysqlParseError::Parse(e) => Self::Construct(e),
        }
    }
}

impl<Serror: std::error::Error, Berror: std::error::Error, Cerror: std::error::Error> MysqlConstructError<Serror, Berror, Cerror> {
    pub fn map_from<Perror: std::error::Error, E: std::error::Error>(error: E, func: impl FnOnce(Perror) -> Cerror) -> Self
    where
        MysqlConstructError<Serror, Berror, Perror>: From<E>,
    {
        match MysqlConstructError::from(error) {
            MysqlConstructError::Stream(e) => Self::Stream(e),
            MysqlConstructError::Builder(e) => Self::Builder(e),
            MysqlConstructError::Construct(e) => Self::Construct(func(e)),
        }
    }

    pub fn map_result<T, Perror: std::error::Error, E: std::error::Error>(
        result: Result<T, E>,
        func: impl FnOnce(Perror) -> Cerror,
    ) -> Result<T, Self>
    where
        MysqlConstructError<Serror, Berror, Perror>: From<E>,
    {
        result.map_err(move |e| Self::map_from(e, func))
    }

    pub fn map_builder<E: std::error::Error>(error: Self, func: impl FnOnce(Berror) -> E) -> MysqlConstructError<Serror, E, Cerror> {
        match error {
            Self::Stream(e) => MysqlConstructError::Stream(e),
            Self::Builder(e) => MysqlConstructError::Builder(func(e)),
            Self::Construct(e) => MysqlConstructError::Construct(e),
        }
    }

    pub fn map_builder_result<T, E: std::error::Error>(
        result: Result<T, Self>,
        func: impl FnOnce(Berror) -> E,
    ) -> Result<T, MysqlConstructError<Serror, E, Cerror>> {
        result.map_err(move |e| Self::map_builder(e, func))
    }
}

/// Trait for constructing MySQL values using a builder.
pub trait MysqlConstruct<'s, S: WireRead + ?Sized + 's, B>: MysqlParse<S>
where
    B: MysqlBuilder,
{
    type ConstructError: std::error::Error;

    async fn construct(stream: &'s S, builder: B) -> Result<B::Output, MysqlConstructError<S::ReadError, B::Error, Self::ConstructError>>;
}

/// Builder trait for constructing MySQL values.
pub trait MysqlBuilder {
    type Output;
    type Error: std::error::Error;
}
