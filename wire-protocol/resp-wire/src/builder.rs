//! RESP builder traits for constructing RESP values.

use wire_stream::WireRead;

/// Base trait for RESP builders.
pub trait RespBuilder {
    type Error: std::error::Error;
    type Output;
}

/// Builder for RESP2 types.
pub trait Resp2Builder: RespBuilder {
    type SimpleStringBuilder: RespStringBuilder<Error = Self::Error, Output = Self::Output>;
    type SimpleErrorBuilder: RespStringBuilder<Error = Self::Error, Output = Self::Output>;
    type BulkStringBuilder: RespStringBuilder<Error = Self::Error, Output = Self::Output>;
    type ArrayBuilder: RespArrayBuilder<Error = Self::Error, Output = Self::Output>;

    async fn simple_string(self) -> Result<Self::SimpleStringBuilder, Self::Error>;
    async fn simple_error(self) -> Result<Self::SimpleErrorBuilder, Self::Error>;
    async fn integer(self, value: i128) -> Result<Self::Output, Self::Error>;
    async fn bulk_string(self, len: usize) -> Result<Self::BulkStringBuilder, Self::Error>;
    async fn array(self, len: usize) -> Result<Self::ArrayBuilder, Self::Error>;
}

/// Builder for RESP3 types (extends RESP2).
pub trait Resp3Builder: Resp2Builder {
    type BulkErrorBuilder: RespStringBuilder<Error = Self::Error, Output = Self::Output>;
    type VerbatimStringBuilder: RespStringBuilder<Error = Self::Error, Output = Self::Output>;
    type MapBuilder: RespMapBuilder<Error = Self::Error, Output = Self::Output>;
    type AttributesBuilder: RespMapBuilder<Error = Self::Error, Output = Self::Output>;
    type SetBuilder: RespArrayBuilder<Error = Self::Error, Output = Self::Output>;
    type PushBuilder: RespArrayBuilder<Error = Self::Error, Output = Self::Output>;

    async fn null(self) -> Result<Self::Output, Self::Error>;
    async fn bool(self, value: bool) -> Result<Self::Output, Self::Error>;
    async fn bignum(self, value: &'_ [u8]) -> Result<Self::Output, Self::Error>;
    async fn bulk_error(self, len: usize) -> Result<Self::BulkErrorBuilder, Self::Error>;
    async fn verbatim_string(self, len: usize, encoding: [u8; 3]) -> Result<Self::VerbatimStringBuilder, Self::Error>;
    async fn map(self, len: usize) -> Result<Self::MapBuilder, Self::Error>;
    async fn attributes(self, len: usize) -> Result<Self::AttributesBuilder, Self::Error>;
    async fn set(self, len: usize) -> Result<Self::SetBuilder, Self::Error>;
    async fn push(self, len: usize) -> Result<Self::PushBuilder, Self::Error>;
}

/// Builder for string-like RESP types.
pub trait RespStringBuilder: RespBuilder {
    type Chunk<'c>;

    async fn push_chunk(&mut self, item: Self::Chunk<'_>) -> Result<(), Self::Error>;
    async fn finish(self) -> Result<Self::Output, Self::Error>;
}

/// Builder for array-like RESP types.
pub trait RespArrayBuilder: RespBuilder {
    type Element;

    async fn visit_element(&mut self, item: Self::Element) -> Result<&Self::Element, Self::Error>;
    async fn finish(self) -> Result<Self::Output, Self::Error>;
}

/// Builder for map-like RESP types.
pub trait RespMapBuilder: RespBuilder {
    type Key;
    type EntryBuilder<'b>: RespMapEntryBuilder<'b, Error = Self::Error, Key = Self::Key>
    where
        Self: 'b;

    async fn visit_key<'b>(&'b mut self, key: Self::Key) -> Result<Self::EntryBuilder<'b>, Self::Error>;
    async fn finish(self) -> Result<Self::Output, Self::Error>;
}

/// Builder for map entries.
pub trait RespMapEntryBuilder<'b>: RespBuilder<Output = &'b (Self::Key, Self::Value)> {
    type Key: 'b;
    type Value: 'b;

    fn key<'k>(&'k mut self) -> &'k Self::Key
    where
        'b: 'k;
    async fn visit_value(self, value: Self::Value) -> Result<Self::Output, Self::Error>;
}

/// Parser trait for builders.
pub trait RespParser<S: WireRead + ?Sized>: RespBuilder {
    type ParseError: std::error::Error;
}

/// Combined error type for builder+parser operations.
#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum RespBuilderParserError<Serror: std::error::Error, Perror: std::error::Error, Berror: std::error::Error> {
    #[error(transparent)]
    Stream(Serror),
    #[error(transparent)]
    Parser(Perror),
    #[error(transparent)]
    Builder(Berror),
}

impl<Serror: std::error::Error, Perror: std::error::Error, Berror: std::error::Error> From<crate::RespParseError<Serror, Perror>>
    for RespBuilderParserError<Serror, Perror, Berror>
{
    fn from(value: crate::RespParseError<Serror, Perror>) -> Self {
        match value {
            crate::RespParseError::Stream(e) => Self::Stream(e),
            crate::RespParseError::Parse(e) => Self::Parser(e),
        }
    }
}

/// Array parser trait.
pub trait RespArrayParser<'s, S: WireRead + ?Sized + 's>: RespArrayBuilder + RespParser<S> {
    async fn parse_element<'b>(
        &'b mut self,
        stream: &'s S,
    ) -> Result<&'b Self::Element, RespBuilderParserError<S::ReadError, Self::ParseError, Self::Error>>;
}

/// Map parser trait.
pub trait RespMapParser<'s, S: WireRead + ?Sized + 's>: RespMapBuilder + RespParser<S> {
    async fn parse_key_from_stream<'b>(
        &'b mut self,
        stream: &'s S,
    ) -> Result<Self::EntryBuilder<'b>, RespBuilderParserError<S::ReadError, Self::ParseError, Self::Error>>
    where
        's: 'b;
}

/// Map entry parser trait.
pub trait RespMapEntryParser<'s: 'b, 'b, S: WireRead + ?Sized + 's>: RespMapEntryBuilder<'b> + RespParser<S> {
    async fn parse_value_from_stream(
        self,
        stream: &'s S,
    ) -> Result<<Self as RespBuilder>::Output, RespBuilderParserError<S::ReadError, Self::ParseError, <Self as RespBuilder>::Error>>;
}

/// Helper trait for map construction.
pub trait RespMapConstruct<'s, S: WireRead + ?Sized + 's>: RespMapParser<'s, S> + Sized {
    type EntryParseError: std::error::Error;

    async fn parse_entry<'b>(
        &'b mut self,
        stream: &'s S,
    ) -> Result<
        &'b (Self::Key, <Self::EntryBuilder<'b> as RespMapEntryBuilder<'b>>::Value),
        RespBuilderParserError<S::ReadError, Self::EntryParseError, Self::Error>,
    >
    where
        's: 'b;
}
