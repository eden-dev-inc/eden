use crate::error::{IncorrectTag, InvalidLength};
use crate::{
    Resp2Builder, RespArrayBuilder, RespArrayParser, RespBuilderParserError, RespConstruct, RespConstructError, RespParse, RespParseError,
    RespParseSync, RespParser, RespRead, RespReadSync,
};
use std::cell::Cell;

pub enum Array {}

#[derive(Clone, Debug, thiserror::Error)]
pub enum ArrayParseError {
    #[error(transparent)]
    IncorrectTag(#[from] IncorrectTag),
    #[error(transparent)]
    InvalidLength(#[from] InvalidLength),
}

impl<S: RespReadSync + ?Sized> RespParseSync<S> for Array {
    type ParseError = ArrayParseError;
    type Value<'s>
        = ArrayReader<'s, S>
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag_sync(b'*')
            .map_err(RespParseError::Stream)?
            .map_err(ArrayParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let len = stream
            .expect_length_sync()
            .map_err(RespParseError::Stream)?
            .map_err(ArrayParseError::InvalidLength)
            .map_err(RespParseError::Parse)?;

        Ok(ArrayReader::new(stream, len))
    }
}

impl<S: RespRead + ?Sized> RespParse<S> for Array {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag(b'*')
            .await
            .map_err(RespParseError::Stream)?
            .map_err(ArrayParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let len = stream
            .expect_length()
            .await
            .map_err(RespParseError::Stream)?
            .map_err(ArrayParseError::InvalidLength)
            .map_err(RespParseError::Parse)?;

        Ok(ArrayReader::new(stream, len))
    }
}

#[derive(Debug)]
pub struct ArrayReader<'s, S: RespReadSync + ?Sized + 's> {
    stream: &'s S,
    remaining: Cell<usize>,
    valid: Cell<bool>,
}

#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum ArrayReaderError {
    #[error("invalidated")]
    Invalidated,
}

impl<'s, S: RespReadSync + ?Sized> ArrayReader<'s, S> {
    fn new(stream: &'s S, remaining: usize) -> Self {
        Self {
            stream,
            remaining: Cell::new(remaining),
            valid: Cell::new(true),
        }
    }

    pub fn remaining(&self) -> usize {
        self.remaining.get()
    }

    pub fn next_sync<'r>(&'r mut self) -> Result<Option<ArrayElementReader<'s, 'r, S>>, ArrayReaderError> {
        if self.valid.get() {
            Ok(if let Some(remaining) = self.remaining.get().checked_sub(1) {
                self.remaining.set(remaining);
                self.valid.set(false);
                Some(ArrayElementReader::new(self.stream, &self.valid))
            } else {
                None
            })
        } else {
            Err(ArrayReaderError::Invalidated)
        }
    }
}

impl<'s, S: RespRead + ?Sized> ArrayReader<'s, S> {
    pub async fn next<'r>(&'r mut self) -> Result<Option<ArrayElementReader<'s, 'r, S>>, ArrayReaderError> {
        self.next_sync()
    }
}

pub struct ArrayElementReader<'s: 'r, 'r, S: RespReadSync + ?Sized + 's> {
    stream: &'s S,
    success: &'r Cell<bool>,
}

impl<'s: 'r, 'r, S: RespReadSync + ?Sized + 's> ArrayElementReader<'s, 'r, S> {
    fn new(stream: &'s S, success: &'r Cell<bool>) -> Self {
        Self { stream, success }
    }

    pub fn parse_sync<T: RespParseSync<S>>(self) -> Result<T::Value<'s>, RespParseError<S::ReadError, T::ParseError>> {
        let Self { stream, success } = self;
        let value = T::parse_sync(stream)?;
        success.set(true);
        Ok(value)
    }
}

impl<'s: 'r, 'r, S: RespRead + ?Sized + 's> ArrayElementReader<'s, 'r, S> {
    pub async fn parse<T: RespParse<S>>(self) -> Result<T::Value<'s>, RespParseError<S::ReadError, T::ParseError>> {
        let Self { stream, success } = self;
        let value = T::parse(stream).await?;
        success.set(true);
        Ok(value)
    }

    pub async fn parse_from_stream<'p, P: RespArrayParser<'s, S>>(
        self,
        builder: &'p mut P,
    ) -> Result<&'p P::Element, RespBuilderParserError<S::ReadError, P::ParseError, P::Error>> {
        let Self { stream, success } = self;
        let output = builder.parse_element(stream).await?;
        success.set(true);
        Ok(output)
    }
}

#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum ArrayConstructError<Perror: std::error::Error, Rerror: std::error::Error, Eerror: std::error::Error> {
    #[error(transparent)]
    Parse(Perror),
    #[error(transparent)]
    ArrayReader(Rerror),
    #[error(transparent)]
    ElementParse(Eerror),
}

impl<'s, S: RespRead + ?Sized + 's, B: Resp2Builder> RespConstruct<'s, S, B> for Array
where
    <B as Resp2Builder>::ArrayBuilder: RespArrayParser<'s, S>,
{
    type ConstructError = ArrayConstructError<
        <Array as RespParseSync<S>>::ParseError,
        ArrayReaderError,
        <<B as Resp2Builder>::ArrayBuilder as RespParser<S>>::ParseError,
    >;

    async fn construct(stream: &'s S, builder: B) -> Result<B::Output, RespConstructError<S::ReadError, B::Error, Self::ConstructError>> {
        let mut reader = RespConstructError::map_result(Self::parse(stream).await, ArrayConstructError::Parse)?;

        let mut builder = builder.array(reader.remaining()).await.map_err(RespConstructError::Builder)?;

        while let Some(element_reader) =
            reader.next().await.map_err(ArrayConstructError::ArrayReader).map_err(RespConstructError::Construct)?
        {
            RespConstructError::map_result(element_reader.parse_from_stream(&mut builder).await, ArrayConstructError::ElementParse)?;
        }

        builder.finish().await.map_err(RespConstructError::Builder)
    }
}
