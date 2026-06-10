use crate::types::array::{Array, ArrayConstructError, ArrayParseError, ArrayReaderError};
use crate::types::attributes::{Attributes, AttributesConstructError, AttributesParseError, AttributesReaderError};
use crate::types::bignum::{BigNumber, BigNumberParseError};
use crate::types::boolean::{Boolean, BooleanParseError};
use crate::types::bulk_error::{BulkError, BulkErrorParseError};
use crate::types::bulk_string::{BulkString, BulkStringParseError, BulkStringValue};
use crate::types::double::{Double, DoubleParseError};
use crate::types::integer::{Integer, IntegerParseError};
use crate::types::map::{Map, MapConstructError, MapParseError, MapReaderError};
use crate::types::null::{Null, NullParseError};
use crate::types::push::{Push, PushConstructError, PushParseError, PushReaderError};
use crate::types::set::{Set, SetConstructError, SetParseError, SetReaderError};
use crate::types::simple_error::{SimpleError, SimpleErrorParseError};
use crate::types::simple_string::{SimpleString, SimpleStringParseError};
use crate::types::verbatim_string::{VerbatimString, VerbatimStringParseError};
use crate::{
    Resp2Builder, Resp3Builder, RespArrayBuilder, RespArrayParser, RespBuilder, RespBuilderParserError, RespConstruct, RespConstructError,
    RespMapBuilder, RespMapConstruct, RespMapEntryBuilder, RespMapEntryParser, RespMapParser, RespParse, RespParseError, RespParseSync,
    RespParser, RespRead, RespReadSync, RespStringBuilder,
};

// Cap pre-allocation to prevent capacity overflow panics from malicious length fields.
// The actual parsing will still fail gracefully if there isn't enough data.
const MAX_PREALLOC: usize = 8 * 1024; // 8K elements max pre-allocation

#[inline]
fn capped_capacity(requested: usize) -> usize {
    requested.min(MAX_PREALLOC)
}

// Thread-local depth tracking for sync parsing to prevent stack overflow from deeply nested structures.
// We use thread-local because the sync parsing is inherently single-threaded and the recursive calls
// go through trait methods that we can't easily add parameters to.
use std::cell::Cell;
thread_local! {
    static PARSE_DEPTH: Cell<usize> = const { Cell::new(0) };
}

struct DepthGuard;

impl DepthGuard {
    #[inline]
    fn enter() -> Result<Self, ()> {
        PARSE_DEPTH.with(|depth| {
            let current = depth.get();
            if current >= crate::limits::MAX_DEPTH {
                Err(())
            } else {
                depth.set(current + 1);
                Ok(DepthGuard)
            }
        })
    }
}

impl Drop for DepthGuard {
    #[inline]
    fn drop(&mut self) {
        PARSE_DEPTH.with(|depth| {
            depth.set(depth.get().saturating_sub(1));
        });
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum Dynamic {
    // RESP2:
    // simple strings: u8 chunks
    // simple errors: u8 chunks
    // integers: i128
    // bulk strings: u8 chunks
    // arrays: nested elements
    SimpleString { value: Box<[u8]> },
    SimpleError { value: Box<[u8]> },
    Integer { value: i128 },
    NullBulkString,
    BulkString { value: Box<[u8]> },
    NullArray,
    Array { elements: Box<[Dynamic]> },

    // RESP3:
    // nulls: ()
    // booleans: bool
    // doubles: f64
    // big numbers: u8 line (for now)
    // bulk errors: u8 chunks
    // verbatim strings: encoding then u8 chunks
    // maps: nested elements (kv pairs)
    // attributes: nested elements (kv pairs)
    // sets: nested elements
    // pushes: nested elements
    Null,
    Boolean { value: bool },
    Double { value: f64 },
    Bignum { value: Box<[u8]> },
    BulkError { value: Box<[u8]> },
    VerbatimString { encoding: [u8; 3], value: Box<[u8]> },
    Map { entries: Box<[(Dynamic, Dynamic)]> },
    Attributes { entries: Box<[(Dynamic, Dynamic)]> },
    Set { items: Box<[Dynamic]> },
    Push { elements: Box<[Dynamic]> },
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum DynamicParseError {
    #[error(transparent)]
    Build(DynamicBuildError),
    // RESP2 types
    #[error(transparent)]
    SimpleString(SimpleStringParseError),
    #[error(transparent)]
    SimpleError(SimpleErrorParseError),
    #[error(transparent)]
    Integer(IntegerParseError),
    #[error(transparent)]
    BulkString(BulkStringParseError),
    #[error(transparent)]
    Array(Box<ArrayConstructError<ArrayParseError, ArrayReaderError, DynamicParseError>>),
    // RESP3 types
    #[error(transparent)]
    Null(NullParseError),
    #[error(transparent)]
    Boolean(BooleanParseError),
    #[error(transparent)]
    Double(DoubleParseError),
    #[error(transparent)]
    BigNumber(BigNumberParseError),
    #[error(transparent)]
    BulkError(BulkErrorParseError),
    #[error(transparent)]
    VerbatimString(VerbatimStringParseError),
    #[error(transparent)]
    Map(Box<MapConstructError<MapParseError, MapReaderError, DynamicParseError>>),
    #[error(transparent)]
    Attributes(Box<AttributesConstructError<AttributesParseError, AttributesReaderError, DynamicParseError>>),
    #[error(transparent)]
    Set(Box<SetConstructError<SetParseError, SetReaderError, DynamicParseError>>),
    #[error(transparent)]
    Push(Box<PushConstructError<PushParseError, PushReaderError, DynamicParseError>>),
    #[error("unknown type tag: {0}")]
    UnknownTag(u8),
    #[error("nesting depth exceeds maximum limit")]
    ExceedsMaxDepth,
}

impl Dynamic {
    fn map_construct_result<Serror: std::error::Error, Cerror: std::error::Error>(
        result: Result<Dynamic, RespConstructError<Serror, DynamicBuildError, Cerror>>,
        construct_error: impl FnOnce(Cerror) -> DynamicParseError,
    ) -> Result<Dynamic, RespParseError<Serror, DynamicParseError>> {
        match result {
            Ok(dynamic) => Ok(dynamic),
            Err(RespConstructError::Stream(e)) => Err(RespParseError::Stream(e)),
            Err(RespConstructError::Builder(e)) => Err(RespParseError::Parse(DynamicParseError::Build(e))),
            Err(RespConstructError::Construct(e)) => Err(RespParseError::Parse(construct_error(e))),
        }
    }

    async fn construct<'s, S: RespRead + ?Sized + 's, T: RespConstruct<'s, S, DynamicBuilder<()>>>(
        stream: &'s S,
        construct_error: impl FnOnce(T::ConstructError) -> DynamicParseError,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        Self::map_construct_result(T::construct(stream, DynamicBuilder(())).await, construct_error)
    }

    fn boxing<Cerror: std::error::Error>(func: impl FnOnce(Box<Cerror>) -> DynamicParseError) -> impl FnOnce(Cerror) -> DynamicParseError {
        move |e| func(Box::new(e))
    }

    async fn box_construct<'s, S: RespRead + ?Sized + 's, T: RespConstruct<'s, S, DynamicBuilder<()>>>(
        stream: &'s S,
        construct_error: impl FnOnce(Box<T::ConstructError>) -> DynamicParseError,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        Self::map_construct_result(Box::pin(T::construct(stream, DynamicBuilder(()))).await, Self::boxing(construct_error))
    }

    async fn parse_integer<'s, S: RespRead + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let value = Integer::parse(stream).await.map_err(|e| e.map_parse(DynamicParseError::Integer))?;
        Ok(Dynamic::Integer { value })
    }

    async fn parse_bulk_string<'s, S: RespRead + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        match BulkString::parse(stream).await.map_err(|e| e.map_parse(DynamicParseError::BulkString))? {
            BulkStringValue::Null => Ok(Dynamic::NullBulkString),
            BulkStringValue::String(mut reader) => {
                if reader.remaining() > crate::limits::MAX_STRING_BYTES {
                    return Err(RespParseError::Parse(DynamicParseError::BulkString(BulkStringParseError::InvalidLength(
                        crate::InvalidLength::TooLarge,
                    ))));
                }
                let mut data = Vec::with_capacity(capped_capacity(reader.remaining()));
                while let Some(chunk) = reader.next().await.map_err(RespParseError::Stream)? {
                    data.extend_from_slice(&chunk);
                }
                Ok(Dynamic::BulkString { value: data.into_boxed_slice() })
            }
        }
    }

    async fn parse_null<'s, S: RespRead + ?Sized + 's>(stream: &'s S) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        Null::parse(stream).await.map_err(|e| e.map_parse(DynamicParseError::Null))?;
        Ok(Dynamic::Null)
    }

    async fn parse_boolean<'s, S: RespRead + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let value = Boolean::parse(stream).await.map_err(|e| e.map_parse(DynamicParseError::Boolean))?;
        Ok(Dynamic::Boolean { value })
    }

    async fn parse_double<'s, S: RespRead + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let value = Double::parse(stream).await.map_err(|e| e.map_parse(DynamicParseError::Double))?;
        Ok(Dynamic::Double { value })
    }

    async fn parse_bignum<'s, S: RespRead + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let borrow = BigNumber::parse(stream).await.map_err(|e| e.map_parse(DynamicParseError::BigNumber))?;
        let value = (*borrow).to_vec().into_boxed_slice();
        Ok(Dynamic::Bignum { value })
    }

    async fn parse_simple_error<'s, S: RespRead + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let mut reader = SimpleError::parse(stream).await.map_err(|e| e.map_parse(DynamicParseError::SimpleError))?;

        let mut data = Vec::new();
        while let Some(chunk) = reader.next().await.map_err(RespParseError::Stream)? {
            data.extend_from_slice(&chunk);
        }
        Ok(Dynamic::SimpleError { value: data.into_boxed_slice() })
    }

    async fn parse_bulk_error<'s, S: RespRead + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let mut reader = BulkError::parse(stream).await.map_err(|e| e.map_parse(DynamicParseError::BulkError))?;

        let mut data = Vec::with_capacity(capped_capacity(reader.remaining()));
        while let Some(chunk) = reader.next().await.map_err(RespParseError::Stream)? {
            data.extend_from_slice(&chunk);
        }
        Ok(Dynamic::BulkError { value: data.into_boxed_slice() })
    }

    async fn parse_verbatim_string<'s, S: RespRead + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let mut reader = VerbatimString::parse(stream).await.map_err(|e| e.map_parse(DynamicParseError::VerbatimString))?;

        let encoding = reader.encoding();
        let mut data = Vec::with_capacity(capped_capacity(reader.remaining()));
        while let Some(chunk) = reader.next().await.map_err(RespParseError::Stream)? {
            data.extend_from_slice(&chunk);
        }
        Ok(Dynamic::VerbatimString { encoding, value: data.into_boxed_slice() })
    }

    // Sync parsing helpers

    fn parse_simple_string_sync<'s, S: RespReadSync + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let mut reader = SimpleString::parse_sync(stream).map_err(|e| e.map_parse(DynamicParseError::SimpleString))?;

        let mut data = Vec::new();
        while let Some(chunk) = reader.next_sync().map_err(RespParseError::Stream)? {
            data.extend_from_slice(&chunk);
        }
        if !reader.is_finished() {
            return Err(RespParseError::Parse(DynamicParseError::SimpleString(SimpleStringParseError::IncompleteInput)));
        }
        Ok(Dynamic::SimpleString { value: data.into_boxed_slice() })
    }

    fn parse_simple_error_sync<'s, S: RespReadSync + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let mut reader = SimpleError::parse_sync(stream).map_err(|e| e.map_parse(DynamicParseError::SimpleError))?;

        let mut data = Vec::new();
        while let Some(chunk) = reader.next_sync().map_err(RespParseError::Stream)? {
            data.extend_from_slice(&chunk);
        }
        if !reader.is_finished() {
            return Err(RespParseError::Parse(DynamicParseError::SimpleError(SimpleErrorParseError::IncompleteInput)));
        }
        Ok(Dynamic::SimpleError { value: data.into_boxed_slice() })
    }

    fn parse_integer_sync<'s, S: RespReadSync + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let value = Integer::parse_sync(stream).map_err(|e| e.map_parse(DynamicParseError::Integer))?;
        Ok(Dynamic::Integer { value })
    }

    fn parse_bulk_string_sync<'s, S: RespReadSync + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        match BulkString::parse_sync(stream).map_err(|e| e.map_parse(DynamicParseError::BulkString))? {
            BulkStringValue::Null => Ok(Dynamic::NullBulkString),
            BulkStringValue::String(mut reader) => {
                if reader.remaining() > crate::limits::MAX_STRING_BYTES {
                    return Err(RespParseError::Parse(DynamicParseError::BulkString(BulkStringParseError::InvalidLength(
                        crate::InvalidLength::TooLarge,
                    ))));
                }
                let mut data = Vec::with_capacity(capped_capacity(reader.remaining()));
                while let Some(chunk) = reader.next_sync().map_err(RespParseError::Stream)? {
                    data.extend_from_slice(&chunk);
                }
                if !reader.is_finished() {
                    return Err(RespParseError::Parse(DynamicParseError::BulkString(BulkStringParseError::MissingTerminator)));
                }
                Ok(Dynamic::BulkString { value: data.into_boxed_slice() })
            }
        }
    }

    fn parse_array_sync<'s, S: RespReadSync + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let mut reader =
            Array::parse_sync(stream).map_err(|e| e.map_parse(|e| DynamicParseError::Array(Box::new(ArrayConstructError::Parse(e)))))?;

        let mut elements = Vec::with_capacity(capped_capacity(reader.remaining()));
        while let Some(element_reader) = reader
            .next_sync()
            .map_err(|e| RespParseError::Parse(DynamicParseError::Array(Box::new(ArrayConstructError::ArrayReader(e)))))?
        {
            let element = element_reader
                .parse_sync::<Dynamic>()
                .map_err(|e| e.map_parse(|e| DynamicParseError::Array(Box::new(ArrayConstructError::ElementParse(e)))))?;
            elements.push(element);
        }
        Ok(Dynamic::Array { elements: elements.into_boxed_slice() })
    }

    fn parse_null_sync<'s, S: RespReadSync + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        Null::parse_sync(stream).map_err(|e| e.map_parse(DynamicParseError::Null))?;
        Ok(Dynamic::Null)
    }

    fn parse_boolean_sync<'s, S: RespReadSync + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let value = Boolean::parse_sync(stream).map_err(|e| e.map_parse(DynamicParseError::Boolean))?;
        Ok(Dynamic::Boolean { value })
    }

    fn parse_double_sync<'s, S: RespReadSync + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let value = Double::parse_sync(stream).map_err(|e| e.map_parse(DynamicParseError::Double))?;
        Ok(Dynamic::Double { value })
    }

    fn parse_bignum_sync<'s, S: RespReadSync + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let borrow = BigNumber::parse_sync(stream).map_err(|e| e.map_parse(DynamicParseError::BigNumber))?;
        let value = (*borrow).to_vec().into_boxed_slice();
        Ok(Dynamic::Bignum { value })
    }

    fn parse_bulk_error_sync<'s, S: RespReadSync + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let mut reader = BulkError::parse_sync(stream).map_err(|e| e.map_parse(DynamicParseError::BulkError))?;

        let mut data = Vec::with_capacity(capped_capacity(reader.remaining()));
        while let Some(chunk) = reader.next_sync().map_err(RespParseError::Stream)? {
            data.extend_from_slice(&chunk);
        }
        if !reader.is_finished() {
            return Err(RespParseError::Parse(DynamicParseError::BulkError(BulkErrorParseError::IncompleteInput)));
        }
        Ok(Dynamic::BulkError { value: data.into_boxed_slice() })
    }

    fn parse_verbatim_string_sync<'s, S: RespReadSync + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let mut reader = VerbatimString::parse_sync(stream).map_err(|e| e.map_parse(DynamicParseError::VerbatimString))?;

        let encoding = reader.encoding();
        let mut data = Vec::with_capacity(capped_capacity(reader.remaining()));
        while let Some(chunk) = reader.next_sync().map_err(RespParseError::Stream)? {
            data.extend_from_slice(&chunk);
        }
        if !reader.is_finished() {
            return Err(RespParseError::Parse(DynamicParseError::VerbatimString(VerbatimStringParseError::IncompleteInput)));
        }
        Ok(Dynamic::VerbatimString { encoding, value: data.into_boxed_slice() })
    }

    fn parse_map_sync<'s, S: RespReadSync + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let mut reader =
            Map::parse_sync(stream).map_err(|e| e.map_parse(|e| DynamicParseError::Map(Box::new(MapConstructError::Parse(e)))))?;

        let mut entries = Vec::with_capacity(capped_capacity(reader.remaining()));
        while let Some(key_reader) = reader
            .next_sync()
            .map_err(|e| RespParseError::Parse(DynamicParseError::Map(Box::new(MapConstructError::MapReader(e)))))?
        {
            let value_reader = key_reader
                .parse_key_sync::<Dynamic>()
                .map_err(|e| e.map_parse(|e| DynamicParseError::Map(Box::new(MapConstructError::EntryParse(e)))))?;
            let (key, value) = value_reader
                .parse_value_sync::<Dynamic>()
                .map_err(|e| e.map_parse(|e| DynamicParseError::Map(Box::new(MapConstructError::EntryParse(e)))))?;
            entries.push((key, value));
        }
        Ok(Dynamic::Map { entries: entries.into_boxed_slice() })
    }

    fn parse_attributes_sync<'s, S: RespReadSync + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let mut reader = Attributes::parse_sync(stream)
            .map_err(|e| e.map_parse(|e| DynamicParseError::Attributes(Box::new(AttributesConstructError::Parse(e)))))?;

        let mut entries = Vec::with_capacity(capped_capacity(reader.remaining()));
        while let Some(key_reader) = reader
            .next_sync()
            .map_err(|e| RespParseError::Parse(DynamicParseError::Attributes(Box::new(AttributesConstructError::AttributesReader(e)))))?
        {
            let value_reader = key_reader
                .parse_key_sync::<Dynamic>()
                .map_err(|e| e.map_parse(|e| DynamicParseError::Attributes(Box::new(AttributesConstructError::EntryParse(e)))))?;
            let (key, value) = value_reader
                .parse_value_sync::<Dynamic>()
                .map_err(|e| e.map_parse(|e| DynamicParseError::Attributes(Box::new(AttributesConstructError::EntryParse(e)))))?;
            entries.push((key, value));
        }
        Ok(Dynamic::Attributes { entries: entries.into_boxed_slice() })
    }

    fn parse_set_sync<'s, S: RespReadSync + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let mut reader =
            Set::parse_sync(stream).map_err(|e| e.map_parse(|e| DynamicParseError::Set(Box::new(SetConstructError::Parse(e)))))?;

        let mut items = Vec::with_capacity(capped_capacity(reader.remaining()));
        while let Some(element_reader) = reader
            .next_sync()
            .map_err(|e| RespParseError::Parse(DynamicParseError::Set(Box::new(SetConstructError::SetReader(e)))))?
        {
            let item = element_reader
                .parse_sync::<Dynamic>()
                .map_err(|e| e.map_parse(|e| DynamicParseError::Set(Box::new(SetConstructError::ElementParse(e)))))?;
            items.push(item);
        }
        Ok(Dynamic::Set { items: items.into_boxed_slice() })
    }

    fn parse_push_sync<'s, S: RespReadSync + ?Sized + 's>(
        stream: &'s S,
    ) -> Result<Dynamic, RespParseError<S::ReadError, DynamicParseError>> {
        let mut reader =
            Push::parse_sync(stream).map_err(|e| e.map_parse(|e| DynamicParseError::Push(Box::new(PushConstructError::Parse(e)))))?;

        let mut elements = Vec::with_capacity(capped_capacity(reader.remaining()));
        while let Some(element_reader) = reader
            .next_sync()
            .map_err(|e| RespParseError::Parse(DynamicParseError::Push(Box::new(PushConstructError::PushReader(e)))))?
        {
            let element = element_reader
                .parse_sync::<Dynamic>()
                .map_err(|e| e.map_parse(|e| DynamicParseError::Push(Box::new(PushConstructError::ElementParse(e)))))?;
            elements.push(element);
        }
        Ok(Dynamic::Push { elements: elements.into_boxed_slice() })
    }
}

impl<S: RespReadSync + ?Sized> RespParseSync<S> for Dynamic {
    type ParseError = DynamicParseError;
    type Value<'s>
        = Dynamic
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let borrow = stream.peek_exactly::<1>().map_err(RespParseError::Stream)?;

        match borrow[0] {
            // RESP2 types
            b'+' => Self::parse_simple_string_sync(stream),
            b'-' => Self::parse_simple_error_sync(stream),
            b':' => Self::parse_integer_sync(stream),
            b'$' => Self::parse_bulk_string_sync(stream),
            b'*' => {
                let _guard = DepthGuard::enter().map_err(|()| RespParseError::Parse(DynamicParseError::ExceedsMaxDepth))?;
                Self::parse_array_sync(stream)
            }
            // RESP3 types
            b'_' => Self::parse_null_sync(stream),
            b'#' => Self::parse_boolean_sync(stream),
            b',' => Self::parse_double_sync(stream),
            b'(' => Self::parse_bignum_sync(stream),
            b'!' => Self::parse_bulk_error_sync(stream),
            b'=' => Self::parse_verbatim_string_sync(stream),
            b'%' => {
                let _guard = DepthGuard::enter().map_err(|()| RespParseError::Parse(DynamicParseError::ExceedsMaxDepth))?;
                Self::parse_map_sync(stream)
            }
            b'|' => {
                let _guard = DepthGuard::enter().map_err(|()| RespParseError::Parse(DynamicParseError::ExceedsMaxDepth))?;
                Self::parse_attributes_sync(stream)
            }
            b'~' => {
                let _guard = DepthGuard::enter().map_err(|()| RespParseError::Parse(DynamicParseError::ExceedsMaxDepth))?;
                Self::parse_set_sync(stream)
            }
            b'>' => {
                let _guard = DepthGuard::enter().map_err(|()| RespParseError::Parse(DynamicParseError::ExceedsMaxDepth))?;
                Self::parse_push_sync(stream)
            }
            tag => Err(RespParseError::Parse(DynamicParseError::UnknownTag(tag))),
        }
    }
}

impl<S: RespRead + ?Sized> RespParse<S> for Dynamic {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, RespParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let borrow = stream.peek_read_exactly::<1>().await.map_err(RespParseError::Stream)?;

        match borrow[0] {
            // RESP2 types
            b'+' => Self::construct::<S, SimpleString>(stream, DynamicParseError::SimpleString).await,
            b'-' => Self::parse_simple_error(stream).await,
            b':' => Self::parse_integer(stream).await,
            b'$' => Self::parse_bulk_string(stream).await,
            b'*' => {
                let _guard = DepthGuard::enter().map_err(|()| RespParseError::Parse(DynamicParseError::ExceedsMaxDepth))?;
                Self::box_construct::<S, Array>(stream, DynamicParseError::Array).await
            }
            // RESP3 types
            b'_' => Self::parse_null(stream).await,
            b'#' => Self::parse_boolean(stream).await,
            b',' => Self::parse_double(stream).await,
            b'(' => Self::parse_bignum(stream).await,
            b'!' => Self::parse_bulk_error(stream).await,
            b'=' => Self::parse_verbatim_string(stream).await,
            b'%' => {
                let _guard = DepthGuard::enter().map_err(|()| RespParseError::Parse(DynamicParseError::ExceedsMaxDepth))?;
                Self::box_construct::<S, Map>(stream, DynamicParseError::Map).await
            }
            b'|' => {
                let _guard = DepthGuard::enter().map_err(|()| RespParseError::Parse(DynamicParseError::ExceedsMaxDepth))?;
                Self::box_construct::<S, Attributes>(stream, DynamicParseError::Attributes).await
            }
            b'~' => {
                let _guard = DepthGuard::enter().map_err(|()| RespParseError::Parse(DynamicParseError::ExceedsMaxDepth))?;
                Self::box_construct::<S, Set>(stream, DynamicParseError::Set).await
            }
            b'>' => {
                let _guard = DepthGuard::enter().map_err(|()| RespParseError::Parse(DynamicParseError::ExceedsMaxDepth))?;
                Self::box_construct::<S, Push>(stream, DynamicParseError::Push).await
            }
            tag => Err(RespParseError::Parse(DynamicParseError::UnknownTag(tag))),
        }
    }
}

#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum DynamicBuildError {
    #[error("bulk string length is incorrect")]
    IncorrectBulkStringLength,

    #[error("array length is incorrect")]
    IncorrectArrayLength,

    #[error("bulk error length is incorrect")]
    IncorrectBulkErrorLength,

    #[error("verbatim string length is incorrect")]
    IncorrectVerbatimStringLength,

    #[error("incomplete map entry")]
    IncompleteMapEntry,

    #[error("map length is incorrect")]
    IncorrectMapLength,

    #[error("incomplete attributes entry")]
    IncompleteAttributesEntry,

    #[error("attributes length is incorrect")]
    IncorrectAttributesLength,

    #[error("set length is incorrect")]
    IncorrectSetLength,

    #[error("push length is incorrect")]
    IncorrectPushLength,

    #[error("bulk string exceeds maximum size limit")]
    ExceedsMaxStringBytes,

    #[error("array exceeds maximum element limit")]
    ExceedsMaxElements,

    #[error("map exceeds maximum entry limit")]
    ExceedsMaxMapEntries,
}

#[derive(Clone, Debug)]
pub struct DynamicBuilder<T>(pub T);

impl<T> RespBuilder for DynamicBuilder<T> {
    type Error = DynamicBuildError;
    type Output = Dynamic;
}

impl<S: RespRead + ?Sized, T> RespParser<S> for DynamicBuilder<T> {
    type ParseError = DynamicParseError;
}

impl Resp2Builder for DynamicBuilder<()> {
    type SimpleStringBuilder = DynamicBuilder<BuildSimpleStringLike>;
    type SimpleErrorBuilder = DynamicBuilder<BuildSimpleStringLike>;
    type BulkStringBuilder = DynamicBuilder<BuildBulkStringLike>;
    type ArrayBuilder = DynamicBuilder<BuildArrayLike>;

    async fn simple_string(self) -> Result<Self::SimpleStringBuilder, Self::Error> {
        Ok(DynamicBuilder::simple_string())
    }

    async fn simple_error(self) -> Result<Self::SimpleErrorBuilder, Self::Error> {
        Ok(DynamicBuilder::simple_error())
    }

    async fn integer(self, value: i128) -> Result<Self::Output, Self::Error> {
        Ok(Dynamic::Integer { value })
    }

    async fn bulk_string(self, len: usize) -> Result<Self::BulkStringBuilder, Self::Error> {
        Ok(DynamicBuilder::bulk_string(len))
    }

    async fn array(self, len: usize) -> Result<Self::ArrayBuilder, Self::Error> {
        Ok(DynamicBuilder::array(len))
    }
}

#[derive(Copy, Clone, Debug)]
enum SimpleStringLike {
    SimpleString,
    SimpleError,
}

#[derive(Clone, Debug)]
pub struct BuildSimpleStringLike {
    which: SimpleStringLike,
    data: Vec<u8>,
}

impl RespStringBuilder for DynamicBuilder<BuildSimpleStringLike> {
    type Chunk<'c> = &'c [u8];

    async fn push_chunk(&mut self, item: Self::Chunk<'_>) -> Result<(), Self::Error> {
        self.0.data.extend_from_slice(item);
        Ok(())
    }

    async fn finish(self) -> Result<Self::Output, Self::Error> {
        let value = self.0.data.into_boxed_slice();

        Ok(match self.0.which {
            SimpleStringLike::SimpleString => Dynamic::SimpleString { value },
            SimpleStringLike::SimpleError => Dynamic::SimpleError { value },
        })
    }
}

impl DynamicBuilder<BuildSimpleStringLike> {
    pub fn simple_string() -> Self {
        Self(BuildSimpleStringLike { which: SimpleStringLike::SimpleString, data: Vec::new() })
    }

    pub fn simple_error() -> Self {
        Self(BuildSimpleStringLike { which: SimpleStringLike::SimpleError, data: Vec::new() })
    }
}

#[derive(Copy, Clone, Debug)]
enum BulkStringLike {
    BulkString,
    BulkError,
    VerbatimString { encoding: [u8; 3] },
}

impl BulkStringLike {
    pub fn incorrect_length(&self) -> DynamicBuildError {
        match self {
            Self::BulkString => DynamicBuildError::IncorrectBulkStringLength,
            Self::BulkError => DynamicBuildError::IncorrectBulkErrorLength,
            Self::VerbatimString { .. } => DynamicBuildError::IncorrectVerbatimStringLength,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BuildBulkStringLike {
    which: BulkStringLike,
    data: Vec<u8>,
    declared_len: usize,
}

impl RespStringBuilder for DynamicBuilder<BuildBulkStringLike> {
    type Chunk<'c> = &'c [u8];

    async fn push_chunk(&mut self, item: Self::Chunk<'_>) -> Result<(), Self::Error> {
        use crate::limits::MAX_STRING_BYTES;
        let BuildBulkStringLike { which, data, declared_len } = &mut self.0;

        let new_len = data.len() + item.len();

        // Check if we would exceed the max string bytes limit
        if new_len > MAX_STRING_BYTES {
            return Err(DynamicBuildError::ExceedsMaxStringBytes);
        }

        // Check if we would exceed the declared length
        if new_len > *declared_len {
            return Err(which.incorrect_length());
        }

        data.extend_from_slice(item);
        Ok(())
    }

    async fn finish(self) -> Result<Self::Output, Self::Error> {
        let BuildBulkStringLike { which, data, declared_len } = self.0;

        if data.len() == declared_len {
            let value = data.into_boxed_slice();

            Ok(match which {
                BulkStringLike::BulkString => Dynamic::BulkString { value },
                BulkStringLike::BulkError => Dynamic::BulkError { value },
                BulkStringLike::VerbatimString { encoding } => Dynamic::VerbatimString { encoding, value },
            })
        } else {
            Err(which.incorrect_length())
        }
    }
}

impl DynamicBuilder<BuildBulkStringLike> {
    pub fn bulk_string(len: usize) -> Self {
        Self(BuildBulkStringLike {
            which: BulkStringLike::BulkString,
            data: Vec::with_capacity(capped_capacity(len)),
            declared_len: len,
        })
    }

    pub fn bulk_error(len: usize) -> Self {
        Self(BuildBulkStringLike {
            which: BulkStringLike::BulkError,
            data: Vec::with_capacity(capped_capacity(len)),
            declared_len: len,
        })
    }

    pub fn verbatim_string(len: usize, encoding: [u8; 3]) -> Self {
        Self(BuildBulkStringLike {
            which: BulkStringLike::VerbatimString { encoding },
            data: Vec::with_capacity(capped_capacity(len)),
            declared_len: len,
        })
    }
}

#[derive(Copy, Clone, Debug)]
enum ArrayLike {
    Array,
    Set,
    Push,
}

impl ArrayLike {
    pub fn incorrect_length(&self) -> DynamicBuildError {
        match self {
            Self::Array => DynamicBuildError::IncorrectArrayLength,
            Self::Set => DynamicBuildError::IncorrectSetLength,
            Self::Push => DynamicBuildError::IncorrectPushLength,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BuildArrayLike {
    which: ArrayLike,
    elements: Vec<Dynamic>,
    declared_len: usize,
}

impl RespArrayBuilder for DynamicBuilder<BuildArrayLike> {
    type Element = Dynamic;

    async fn visit_element(&mut self, item: Self::Element) -> Result<&Self::Element, Self::Error> {
        use crate::limits::MAX_ELEMENTS;
        let BuildArrayLike { which, elements, declared_len } = &mut self.0;

        // Check if we've hit the max elements limit
        if elements.len() >= MAX_ELEMENTS {
            return Err(DynamicBuildError::ExceedsMaxElements);
        }

        // Check if we've hit the declared length
        if elements.len() >= *declared_len {
            return Err(which.incorrect_length());
        }

        elements.push(item);
        Ok(elements.last().expect("we just pushed the element"))
    }

    async fn finish(self) -> Result<Self::Output, Self::Error> {
        let BuildArrayLike { which, elements, declared_len } = self.0;

        if elements.len() == declared_len {
            let elements = elements.into_boxed_slice();

            Ok(match which {
                ArrayLike::Array => Dynamic::Array { elements },
                ArrayLike::Set => Dynamic::Set { items: elements },
                ArrayLike::Push => Dynamic::Push { elements },
            })
        } else {
            Err(which.incorrect_length())
        }
    }
}

impl<'s, S: RespRead + ?Sized + 's> RespArrayParser<'s, S> for DynamicBuilder<BuildArrayLike> {
    async fn parse_element<'b>(
        &'b mut self,
        stream: &'s S,
    ) -> Result<&'b Self::Element, RespBuilderParserError<S::ReadError, Self::ParseError, Self::Error>> {
        let element = Dynamic::parse(stream).await?;
        self.visit_element(element).await.map_err(RespBuilderParserError::Builder)
    }
}

impl DynamicBuilder<BuildArrayLike> {
    pub fn array(len: usize) -> Self {
        Self(BuildArrayLike {
            which: ArrayLike::Array,
            elements: Vec::with_capacity(capped_capacity(len)),
            declared_len: len,
        })
    }

    pub fn set(len: usize) -> Self {
        Self(BuildArrayLike {
            which: ArrayLike::Set,
            elements: Vec::with_capacity(capped_capacity(len)),
            declared_len: len,
        })
    }

    pub fn push(len: usize) -> Self {
        Self(BuildArrayLike {
            which: ArrayLike::Push,
            elements: Vec::with_capacity(capped_capacity(len)),
            declared_len: len,
        })
    }
}

impl Resp3Builder for DynamicBuilder<()> {
    type BulkErrorBuilder = DynamicBuilder<BuildBulkStringLike>;
    type VerbatimStringBuilder = DynamicBuilder<BuildBulkStringLike>;
    type MapBuilder = DynamicBuilder<BuildMapLike>;
    type AttributesBuilder = DynamicBuilder<BuildMapLike>;
    type SetBuilder = DynamicBuilder<BuildArrayLike>;
    type PushBuilder = DynamicBuilder<BuildArrayLike>;

    async fn null(self) -> Result<Self::Output, Self::Error> {
        Ok(Dynamic::Null)
    }

    async fn bool(self, value: bool) -> Result<Self::Output, Self::Error> {
        Ok(Dynamic::Boolean { value })
    }

    async fn bignum(self, value: &'_ [u8]) -> Result<Self::Output, Self::Error> {
        Ok(Dynamic::Bignum { value: value.into() })
    }

    async fn bulk_error(self, len: usize) -> Result<Self::BulkErrorBuilder, Self::Error> {
        Ok(DynamicBuilder::bulk_error(len))
    }

    async fn verbatim_string(self, len: usize, encoding: [u8; 3]) -> Result<Self::VerbatimStringBuilder, Self::Error> {
        Ok(DynamicBuilder::verbatim_string(len, encoding))
    }

    async fn map(self, len: usize) -> Result<Self::MapBuilder, Self::Error> {
        Ok(DynamicBuilder::map(len))
    }

    async fn attributes(self, len: usize) -> Result<Self::AttributesBuilder, Self::Error> {
        Ok(DynamicBuilder::attributes(len))
    }

    async fn set(self, len: usize) -> Result<Self::SetBuilder, Self::Error> {
        Ok(DynamicBuilder::set(len))
    }

    async fn push(self, len: usize) -> Result<Self::PushBuilder, Self::Error> {
        Ok(DynamicBuilder::push(len))
    }
}

#[derive(Copy, Clone, Debug)]
enum MapLike {
    Map,
    Attributes,
}

impl MapLike {
    pub fn incomplete_entry(&self) -> DynamicBuildError {
        match self {
            Self::Map => DynamicBuildError::IncompleteMapEntry,
            Self::Attributes => DynamicBuildError::IncompleteAttributesEntry,
        }
    }

    pub fn incorrect_length(&self) -> DynamicBuildError {
        match self {
            Self::Map => DynamicBuildError::IncorrectMapLength,
            Self::Attributes => DynamicBuildError::IncorrectAttributesLength,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BuildMapLike {
    which: MapLike,
    entries: Vec<(Dynamic, Dynamic)>,
    success: bool,
    declared_len: usize,
}

impl RespMapBuilder for DynamicBuilder<BuildMapLike> {
    type Key = Dynamic;
    type EntryBuilder<'b>
        = DynamicMapLikeEntryBuilder<'b>
    where
        Self: 'b;

    async fn visit_key<'b>(&'b mut self, key: Self::Key) -> Result<Self::EntryBuilder<'b>, Self::Error> {
        use crate::limits::MAX_MAP_ENTRIES;
        let BuildMapLike { which, entries, success, declared_len } = &mut self.0;

        // Check if we've hit the max map entries limit
        if entries.len() >= MAX_MAP_ENTRIES {
            return Err(DynamicBuildError::ExceedsMaxMapEntries);
        }

        // Check if we've hit the declared length
        if entries.len() >= *declared_len {
            return Err(which.incorrect_length());
        }

        *success = false;
        Ok(DynamicMapLikeEntryBuilder::new(key, entries, success))
    }

    async fn finish(self) -> Result<Self::Output, Self::Error> {
        let BuildMapLike { which, entries, success, declared_len } = self.0;

        if !success {
            Err(which.incomplete_entry())
        } else if entries.len() != declared_len {
            Err(which.incorrect_length())
        } else {
            let entries = entries.into_boxed_slice();

            Ok(match which {
                MapLike::Map => Dynamic::Map { entries },
                MapLike::Attributes => Dynamic::Attributes { entries },
            })
        }
    }
}

impl<'s, S: RespRead + ?Sized + 's> RespMapParser<'s, S> for DynamicBuilder<BuildMapLike> {
    async fn parse_key_from_stream<'b>(
        &'b mut self,
        stream: &'s S,
    ) -> Result<Self::EntryBuilder<'b>, RespBuilderParserError<S::ReadError, Self::ParseError, Self::Error>>
    where
        's: 'b,
    {
        let key = Dynamic::parse(stream).await?;
        self.visit_key(key).await.map_err(RespBuilderParserError::Builder)
    }
}

// Implement the new RespMapConstruct trait - this encapsulates the entry parsing
// at the concrete type level, avoiding the need for HRTB
impl<'s, S: RespRead + ?Sized + 's> RespMapConstruct<'s, S> for DynamicBuilder<BuildMapLike> {
    type EntryParseError = DynamicParseError;

    async fn parse_entry<'b>(
        &'b mut self,
        stream: &'s S,
    ) -> Result<&'b (Dynamic, Dynamic), RespBuilderParserError<S::ReadError, Self::EntryParseError, Self::Error>>
    where
        's: 'b,
    {
        use crate::limits::MAX_MAP_ENTRIES;
        let key = Dynamic::parse(stream).await?;
        let value = Dynamic::parse(stream).await?;

        let BuildMapLike { which, entries, success, declared_len } = &mut self.0;

        // Check if we've hit the max map entries limit
        if entries.len() >= MAX_MAP_ENTRIES {
            return Err(RespBuilderParserError::Builder(DynamicBuildError::ExceedsMaxMapEntries));
        }

        // Check if we've hit the declared length
        if entries.len() >= *declared_len {
            return Err(RespBuilderParserError::Builder(which.incorrect_length()));
        }

        entries.push((key, value));
        *success = true;
        Ok(entries.last().expect("we just pushed the entry"))
    }
}

#[derive(Debug)]
pub struct DynamicMapLikeEntryBuilder<'b> {
    key: Dynamic,
    entries: &'b mut Vec<(Dynamic, Dynamic)>,
    success: &'b mut bool,
}

impl<'b> DynamicMapLikeEntryBuilder<'b> {
    fn new(key: Dynamic, entries: &'b mut Vec<(Dynamic, Dynamic)>, success: &'b mut bool) -> Self {
        Self { key, entries, success }
    }
}

impl<'b> RespBuilder for DynamicMapLikeEntryBuilder<'b> {
    type Error = DynamicBuildError;
    type Output = &'b (Dynamic, Dynamic);
}

impl<'b> RespMapEntryBuilder<'b> for DynamicMapLikeEntryBuilder<'b> {
    type Key = Dynamic;
    type Value = Dynamic;

    fn key<'k>(&'k mut self) -> &'k Self::Key
    where
        'b: 'k,
    {
        &self.key
    }

    async fn visit_value(self, value: Self::Value) -> Result<Self::Output, Self::Error> {
        let Self { key, entries, success } = self;
        entries.push((key, value));
        *success = true;
        Ok(entries.last().expect("we just pushed the entry"))
    }
}

impl<'b, S: RespRead + ?Sized> RespParser<S> for DynamicMapLikeEntryBuilder<'b> {
    type ParseError = DynamicParseError;
}

impl<'s: 'b, 'b, S: RespRead + ?Sized + 's> RespMapEntryParser<'s, 'b, S> for DynamicMapLikeEntryBuilder<'b> {
    async fn parse_value_from_stream(
        self,
        stream: &'s S,
    ) -> Result<<Self as RespBuilder>::Output, RespBuilderParserError<S::ReadError, Self::ParseError, <Self as RespBuilder>::Error>> {
        let value = Dynamic::parse(stream).await?;
        self.visit_value(value).await.map_err(RespBuilderParserError::Builder)
    }
}

impl DynamicBuilder<BuildMapLike> {
    pub fn map(len: usize) -> Self {
        Self(BuildMapLike {
            which: MapLike::Map,
            entries: Vec::with_capacity(capped_capacity(len)),
            success: true,
            declared_len: len,
        })
    }

    pub fn attributes(len: usize) -> Self {
        Self(BuildMapLike {
            which: MapLike::Attributes,
            entries: Vec::with_capacity(capped_capacity(len)),
            success: true,
            declared_len: len,
        })
    }
}
