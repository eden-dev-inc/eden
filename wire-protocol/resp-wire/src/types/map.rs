use crate::error::{IncorrectTag, InvalidLength};
use crate::{
    Resp3Builder, RespBuilder, RespBuilderParserError, RespConstruct, RespConstructError, RespMapBuilder, RespMapConstruct,
    RespMapEntryParser, RespMapParser, RespParse, RespParseError, RespParseSync, RespParser, RespRead, RespReadSync,
};
use std::cell::Cell;

pub enum Map {}

#[derive(Clone, Debug, thiserror::Error)]
pub enum MapParseError {
    #[error(transparent)]
    IncorrectTag(#[from] IncorrectTag),
    #[error(transparent)]
    InvalidLength(#[from] InvalidLength),
}

impl<S: RespReadSync + ?Sized> RespParseSync<S> for Map {
    type ParseError = MapParseError;
    type Value<'s>
        = MapReader<'s, S>
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag_sync(b'%')
            .map_err(RespParseError::Stream)?
            .map_err(MapParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let len = stream
            .expect_length_sync()
            .map_err(RespParseError::Stream)?
            .map_err(MapParseError::InvalidLength)
            .map_err(RespParseError::Parse)?;

        Ok(MapReader::new(stream, len))
    }
}

impl<S: RespRead + ?Sized> RespParse<S> for Map {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        stream
            .expect_tag(b'%')
            .await
            .map_err(RespParseError::Stream)?
            .map_err(MapParseError::IncorrectTag)
            .map_err(RespParseError::Parse)?;

        let len = stream
            .expect_length()
            .await
            .map_err(RespParseError::Stream)?
            .map_err(MapParseError::InvalidLength)
            .map_err(RespParseError::Parse)?;

        Ok(MapReader::new(stream, len))
    }
}

#[derive(Debug)]
pub struct MapReader<'s, S: RespReadSync + ?Sized + 's> {
    stream: &'s S,
    remaining: Cell<usize>,
    valid: Cell<bool>,
}

#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum MapReaderError {
    #[error("invalidated")]
    Invalidated,
}

impl<'s, S: RespReadSync + ?Sized> MapReader<'s, S> {
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

    pub fn next_sync<'r>(&'r mut self) -> Result<Option<MapKeyReader<'s, 'r, S>>, MapReaderError> {
        if self.valid.get() {
            Ok(if let Some(remaining) = self.remaining.get().checked_sub(1) {
                self.remaining.set(remaining);
                self.valid.set(false);
                Some(MapKeyReader::new(self.stream, &self.valid))
            } else {
                None
            })
        } else {
            Err(MapReaderError::Invalidated)
        }
    }
}

impl<'s, S: RespRead + ?Sized> MapReader<'s, S> {
    pub async fn next<'r>(&'r mut self) -> Result<Option<MapKeyReader<'s, 'r, S>>, MapReaderError> {
        self.next_sync()
    }
}

pub struct MapKeyReader<'s: 'r, 'r, S: RespReadSync + ?Sized + 's> {
    stream: &'s S,
    success: &'r Cell<bool>,
}

pub struct MapValueReader<'s: 'r, 'r, S: RespReadSync + ?Sized + 's, K: 'r> {
    stream: &'s S,
    success: &'r Cell<bool>,
    key: K,
}

impl<'s: 'r, 'r, S: RespReadSync + ?Sized + 's> MapKeyReader<'s, 'r, S> {
    fn new(stream: &'s S, success: &'r Cell<bool>) -> Self {
        Self { stream, success }
    }

    // Intrinsic to the multiple type/lifetime parameters
    #[allow(clippy::type_complexity)]
    pub fn parse_key_sync<K: RespParseSync<S> + 'r>(
        self,
    ) -> Result<MapValueReader<'s, 'r, S, K::Value<'s>>, RespParseError<S::ReadError, K::ParseError>> {
        let Self { stream, success } = self;
        let key = K::parse_sync(stream)?;
        Ok(MapValueReader::new(stream, success, key))
    }
}

impl<'s: 'r, 'r, S: RespRead + ?Sized + 's> MapKeyReader<'s, 'r, S> {
    pub async fn parse_key<K: RespParse<S> + 'r>(
        self,
    ) -> Result<MapValueReader<'s, 'r, S, K::Value<'s>>, RespParseError<S::ReadError, K::ParseError>> {
        let Self { stream, success } = self;
        let key = K::parse(stream).await?;
        Ok(MapValueReader::new(stream, success, key))
    }

    pub async fn parse_key_from_stream<P: RespMapParser<'s, S>>(
        self,
        builder: &'r mut P,
    ) -> Result<MapValueReader<'s, 'r, S, P::EntryBuilder<'r>>, RespBuilderParserError<S::ReadError, P::ParseError, P::Error>> {
        let Self { stream, success } = self;
        let entry_builder = builder.parse_key_from_stream(stream).await?;
        Ok(MapValueReader::new(stream, success, entry_builder))
    }
}

impl<'s: 'r, 'r, S: RespReadSync + ?Sized + 's, K: 'r> MapValueReader<'s, 'r, S, K> {
    fn new(stream: &'s S, success: &'r Cell<bool>, key: K) -> Self {
        Self { stream, success, key }
    }

    // Intrinsic to the multiple type/lifetime parameters
    #[allow(clippy::type_complexity)]
    pub fn parse_value_sync<V: RespParseSync<S>>(self) -> Result<(K, V::Value<'s>), RespParseError<S::ReadError, V::ParseError>> {
        let Self { stream, success, key } = self;
        let value = V::parse_sync(stream)?;
        success.set(true);
        Ok((key, value))
    }
}

impl<'s: 'r, 'r, S: RespRead + ?Sized + 's, K: 'r> MapValueReader<'s, 'r, S, K> {
    pub async fn parse_value<V: RespParse<S>>(self) -> Result<(K, V::Value<'s>), RespParseError<S::ReadError, V::ParseError>> {
        let Self { stream, success, key } = self;
        let value = V::parse(stream).await?;
        success.set(true);
        Ok((key, value))
    }
}

impl<'s: 'r, 'r, S: RespRead + ?Sized + 's, K: RespMapEntryParser<'s, 'r, S>> MapValueReader<'s, 'r, S, K> {
    pub async fn parse_value_from_stream(
        self,
    ) -> Result<<K as RespBuilder>::Output, RespBuilderParserError<S::ReadError, K::ParseError, <K as RespBuilder>::Error>> {
        let Self { stream, success, key: entry_builder } = self;
        let output = entry_builder.parse_value_from_stream(stream).await?;
        success.set(true);
        Ok(output)
    }
}

#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum MapConstructError<Perror: std::error::Error, Rerror: std::error::Error, KVerror: std::error::Error> {
    #[error(transparent)]
    Parse(Perror),
    #[error(transparent)]
    MapReader(Rerror),
    #[error(transparent)]
    EntryParse(KVerror),
}

impl<'s, S: RespRead + ?Sized + 's, B: Resp3Builder> RespConstruct<'s, S, B> for Map
where
    <B as Resp3Builder>::MapBuilder:
        RespMapConstruct<'s, S, EntryParseError = <<B as Resp3Builder>::MapBuilder as RespParser<S>>::ParseError>,
{
    type ConstructError = MapConstructError<
        <Map as RespParseSync<S>>::ParseError,
        MapReaderError,
        <<B as Resp3Builder>::MapBuilder as RespParser<S>>::ParseError,
    >;

    async fn construct(stream: &'s S, builder: B) -> Result<B::Output, RespConstructError<S::ReadError, B::Error, Self::ConstructError>> {
        let reader = RespConstructError::map_result(Self::parse(stream).await, MapConstructError::Parse)?;

        let mut builder = builder.map(reader.remaining()).await.map_err(RespConstructError::Builder)?;

        let mut count = reader.remaining();
        while count > 0 {
            count -= 1;

            RespConstructError::map_result(builder.parse_entry(stream).await, MapConstructError::EntryParse)?;
        }

        builder.finish().await.map_err(RespConstructError::Builder)
    }
}
