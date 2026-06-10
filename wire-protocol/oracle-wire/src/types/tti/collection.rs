//! Oracle collection type deserialization (VARRAY and NESTED TABLE).
//!
//! Oracle supports two collection types:
//! - **VARRAY**: Variable-size arrays with a maximum size limit
//! - **NESTED TABLE**: Unbounded tables that can have gaps (sparse)
//!
//! # Wire Format
//!
//! Collections are serialized with:
//! - Type indicator byte
//! - Element count (varint)
//! - Null bitmap (for sparse collections)
//! - Element data (type-specific encoding)
//!
//! # Example
//!
//! ```rust,ignore
//! use oracle_wire::types::tti::collection::{parse_collection, CollectionType};
//!
//! let data = &[/* collection bytes */];
//! let collection = parse_collection(data, CollectionType::Varray)?;
//! for element in collection.elements() {
//!     println!("{:?}", element);
//! }
//! ```

use std::fmt;

use super::data_types::OracleDataType;

/// Collection type identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CollectionType {
    /// VARRAY - variable-size array with maximum limit.
    Varray,
    /// NESTED TABLE - unbounded table, can be sparse.
    NestedTable,
}

impl CollectionType {
    /// Create from type code.
    pub fn from_u8(code: u8) -> Option<Self> {
        match code {
            0x6E => Some(Self::Varray),      // TYPE_VARRAY
            0x6F => Some(Self::NestedTable), // TYPE_NESTED_TABLE
            _ => None,
        }
    }

    /// Convert to type code.
    pub fn as_u8(&self) -> u8 {
        match self {
            Self::Varray => 0x6E,
            Self::NestedTable => 0x6F,
        }
    }

    /// Check if this collection type can be sparse.
    pub fn can_be_sparse(&self) -> bool {
        matches!(self, Self::NestedTable)
    }
}

/// Collection parsing error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CollectionError {
    /// Unexpected end of data.
    UnexpectedEof { expected: usize, available: usize },
    /// Invalid collection type code.
    InvalidType(u8),
    /// Invalid element encoding.
    InvalidElement(String),
    /// Collection size exceeds limit.
    SizeExceeded { size: usize, max: usize },
    /// Null bitmap invalid.
    InvalidNullBitmap,
    /// Nested collection depth exceeded.
    NestingTooDeep,
    /// Type mismatch in collection.
    TypeMismatch { expected: String, found: String },
}

impl std::error::Error for CollectionError {}

impl fmt::Display for CollectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { expected, available } => {
                write!(f, "unexpected EOF: need {} bytes, have {}", expected, available)
            }
            Self::InvalidType(code) => write!(f, "invalid collection type: 0x{:02X}", code),
            Self::InvalidElement(msg) => write!(f, "invalid element: {}", msg),
            Self::SizeExceeded { size, max } => {
                write!(f, "collection size {} exceeds maximum {}", size, max)
            }
            Self::InvalidNullBitmap => write!(f, "invalid null bitmap"),
            Self::NestingTooDeep => write!(f, "nested collection depth exceeded"),
            Self::TypeMismatch { expected, found } => {
                write!(f, "type mismatch: expected {}, found {}", expected, found)
            }
        }
    }
}

/// Maximum collection size.
pub const MAX_COLLECTION_SIZE: usize = 1_000_000;

/// Maximum nesting depth for nested collections.
pub const MAX_COLLECTION_DEPTH: usize = 32;

/// A single element in a collection.
#[derive(Clone, Debug, PartialEq)]
pub enum CollectionElement {
    /// Null element (sparse collection gap).
    Null,
    /// Integer value.
    Int(i64),
    /// Floating-point value.
    Float(f64),
    /// String value.
    String(String),
    /// Binary data.
    Binary(Vec<u8>),
    /// Nested collection.
    Collection(Box<OracleCollection>),
    /// Raw bytes (for complex types).
    Raw(Vec<u8>),
}

impl CollectionElement {
    /// Check if this element is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Get as integer.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(n) => Some(*n),
            _ => None,
        }
    }

    /// Get as float.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(f) => Some(*f),
            Self::Int(n) => Some(*n as f64),
            _ => None,
        }
    }

    /// Get as string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    /// Get as bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Binary(b) | Self::Raw(b) => Some(b),
            _ => None,
        }
    }

    /// Get as nested collection.
    pub fn as_collection(&self) -> Option<&OracleCollection> {
        match self {
            Self::Collection(c) => Some(c),
            _ => None,
        }
    }
}

/// Parsed Oracle collection.
#[derive(Clone, Debug, PartialEq)]
pub struct OracleCollection {
    /// Collection type.
    pub collection_type: CollectionType,
    /// Element data type.
    pub element_type: OracleDataType,
    /// Elements in the collection.
    pub elements: Vec<CollectionElement>,
    /// Maximum size (for VARRAY).
    pub max_size: Option<usize>,
}

impl OracleCollection {
    /// Create a new empty collection.
    pub fn new(collection_type: CollectionType, element_type: OracleDataType) -> Self {
        Self {
            collection_type,
            element_type,
            elements: Vec::new(),
            max_size: None,
        }
    }

    /// Create a VARRAY with maximum size.
    pub fn varray(element_type: OracleDataType, max_size: usize) -> Self {
        Self {
            collection_type: CollectionType::Varray,
            element_type,
            elements: Vec::new(),
            max_size: Some(max_size),
        }
    }

    /// Create a NESTED TABLE.
    pub fn nested_table(element_type: OracleDataType) -> Self {
        Self {
            collection_type: CollectionType::NestedTable,
            element_type,
            elements: Vec::new(),
            max_size: None,
        }
    }

    /// Get the number of elements.
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Get elements as a slice.
    pub fn elements(&self) -> &[CollectionElement] {
        &self.elements
    }

    /// Get a specific element by index.
    pub fn get(&self, index: usize) -> Option<&CollectionElement> {
        self.elements.get(index)
    }

    /// Add an element.
    pub fn push(&mut self, element: CollectionElement) -> Result<(), CollectionError> {
        if let Some(max) = self.max_size
            && self.elements.len() >= max
        {
            return Err(CollectionError::SizeExceeded { size: self.elements.len() + 1, max });
        }
        self.elements.push(element);
        Ok(())
    }

    /// Count non-null elements.
    pub fn count_non_null(&self) -> usize {
        self.elements.iter().filter(|e| !e.is_null()).count()
    }

    /// Check if collection is sparse (has null gaps).
    pub fn is_sparse(&self) -> bool {
        self.elements.iter().any(|e| e.is_null())
    }

    /// Iterator over non-null elements.
    pub fn non_null_elements(&self) -> impl Iterator<Item = &CollectionElement> {
        self.elements.iter().filter(|e| !e.is_null())
    }

    /// Convert to vector of optional values.
    pub fn to_optional_vec<T, F>(&self, f: F) -> Vec<Option<T>>
    where
        F: Fn(&CollectionElement) -> Option<T>,
    {
        self.elements.iter().map(|e| if e.is_null() { None } else { f(e) }).collect()
    }
}

/// VARRAY-specific wrapper.
#[derive(Clone, Debug)]
pub struct Varray {
    inner: OracleCollection,
}

impl Varray {
    /// Create a new VARRAY.
    pub fn new(element_type: OracleDataType, max_size: usize) -> Self {
        Self { inner: OracleCollection::varray(element_type, max_size) }
    }

    /// Get the maximum size.
    pub fn max_size(&self) -> usize {
        self.inner.max_size.unwrap_or(0)
    }

    /// Get the current size.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Get elements.
    pub fn elements(&self) -> &[CollectionElement] {
        self.inner.elements()
    }

    /// Add an element.
    pub fn push(&mut self, element: CollectionElement) -> Result<(), CollectionError> {
        self.inner.push(element)
    }

    /// Get the inner collection.
    pub fn into_inner(self) -> OracleCollection {
        self.inner
    }
}

/// NESTED TABLE-specific wrapper.
#[derive(Clone, Debug)]
pub struct NestedTable {
    inner: OracleCollection,
}

impl NestedTable {
    /// Create a new NESTED TABLE.
    pub fn new(element_type: OracleDataType) -> Self {
        Self { inner: OracleCollection::nested_table(element_type) }
    }

    /// Get the current size.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Check if sparse.
    pub fn is_sparse(&self) -> bool {
        self.inner.is_sparse()
    }

    /// Get elements.
    pub fn elements(&self) -> &[CollectionElement] {
        self.inner.elements()
    }

    /// Add an element.
    pub fn push(&mut self, element: CollectionElement) {
        // Nested tables have no size limit
        self.inner.elements.push(element);
    }

    /// Delete an element (make it null - creates sparse table).
    pub fn delete(&mut self, index: usize) -> bool {
        if index < self.inner.elements.len() {
            self.inner.elements[index] = CollectionElement::Null;
            true
        } else {
            false
        }
    }

    /// Get the inner collection.
    pub fn into_inner(self) -> OracleCollection {
        self.inner
    }
}

/// Collection parser.
pub struct CollectionParser<'a> {
    data: &'a [u8],
    pos: usize,
    depth: usize,
}

impl<'a> CollectionParser<'a> {
    /// Create a new parser.
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0, depth: 0 }
    }

    /// Parse a collection.
    pub fn parse(&mut self, element_type: OracleDataType) -> Result<OracleCollection, CollectionError> {
        self.depth += 1;
        if self.depth > MAX_COLLECTION_DEPTH {
            return Err(CollectionError::NestingTooDeep);
        }

        // Read collection type
        let type_byte = self.read_u8()?;
        let collection_type = CollectionType::from_u8(type_byte).ok_or(CollectionError::InvalidType(type_byte))?;

        // Read element count
        let count = self.read_varint()? as usize;
        if count > MAX_COLLECTION_SIZE {
            return Err(CollectionError::SizeExceeded { size: count, max: MAX_COLLECTION_SIZE });
        }

        // Read max size for VARRAY
        let max_size = if collection_type == CollectionType::Varray {
            Some(self.read_varint()? as usize)
        } else {
            None
        };

        // Read null bitmap for sparse collections
        let null_bitmap = if collection_type.can_be_sparse() && count > 0 {
            let bitmap_bytes = count.div_ceil(8);
            Some(self.read_bytes(bitmap_bytes)?)
        } else {
            None
        };

        // Read elements
        let mut elements = Vec::with_capacity(count);
        for i in 0..count {
            let is_null = null_bitmap.as_ref().map(|bm| (bm[i / 8] >> (i % 8)) & 1 != 0).unwrap_or(false);

            if is_null {
                elements.push(CollectionElement::Null);
            } else {
                let element = self.parse_element(&element_type)?;
                elements.push(element);
            }
        }

        self.depth -= 1;

        Ok(OracleCollection { collection_type, element_type, elements, max_size })
    }

    fn parse_element(&mut self, data_type: &OracleDataType) -> Result<CollectionElement, CollectionError> {
        match data_type {
            OracleDataType::Number | OracleDataType::BinaryDouble | OracleDataType::BinaryFloat => {
                // Read number length and data
                let len = self.read_u8()? as usize;
                if len == 0 {
                    return Ok(CollectionElement::Null);
                }
                let bytes = self.read_bytes(len)?;
                // Parse as Oracle NUMBER and convert to float
                // Simplified: just store as raw for now
                Ok(CollectionElement::Raw(bytes))
            }
            OracleDataType::Varchar2 | OracleDataType::Char | OracleDataType::Nchar => {
                let len = self.read_varint()? as usize;
                if len == 0 {
                    return Ok(CollectionElement::String(String::new()));
                }
                let bytes = self.read_bytes(len)?;
                let s = String::from_utf8(bytes).map_err(|_| CollectionError::InvalidElement("invalid UTF-8".to_string()))?;
                Ok(CollectionElement::String(s))
            }
            OracleDataType::Raw | OracleDataType::LongRaw => {
                let len = self.read_varint()? as usize;
                let bytes = self.read_bytes(len)?;
                Ok(CollectionElement::Binary(bytes))
            }
            OracleDataType::Date | OracleDataType::Timestamp | OracleDataType::TimestampTz => {
                // Fixed size date/timestamp
                let len = match data_type {
                    OracleDataType::Date => 7,
                    OracleDataType::Timestamp => 11,
                    OracleDataType::TimestampTz => 13,
                    _ => 7,
                };
                let bytes = self.read_bytes(len)?;
                Ok(CollectionElement::Raw(bytes))
            }
            _ => {
                // Unknown type - read length-prefixed raw data
                let len = self.read_varint()? as usize;
                let bytes = self.read_bytes(len)?;
                Ok(CollectionElement::Raw(bytes))
            }
        }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn read_u8(&mut self) -> Result<u8, CollectionError> {
        if self.pos >= self.data.len() {
            return Err(CollectionError::UnexpectedEof { expected: 1, available: 0 });
        }
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn read_bytes(&mut self, n: usize) -> Result<Vec<u8>, CollectionError> {
        if self.remaining() < n {
            return Err(CollectionError::UnexpectedEof { expected: n, available: self.remaining() });
        }
        let bytes = self.data[self.pos..self.pos + n].to_vec();
        self.pos += n;
        Ok(bytes)
    }

    fn read_varint(&mut self) -> Result<u64, CollectionError> {
        let mut result: u64 = 0;
        let mut shift = 0;

        loop {
            let byte = self.read_u8()?;
            result |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift > 63 {
                return Err(CollectionError::InvalidElement("varint overflow".to_string()));
            }
        }

        Ok(result)
    }
}

/// Parse a collection from raw bytes.
pub fn parse_collection(data: &[u8], element_type: OracleDataType) -> Result<OracleCollection, CollectionError> {
    let mut parser = CollectionParser::new(data);
    parser.parse(element_type)
}

/// Builder for creating collection wire format.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct CollectionBuilder {
    collection_type: CollectionType,
    element_type: OracleDataType,
    elements: Vec<CollectionElement>,
    max_size: Option<usize>,
}

impl CollectionBuilder {
    /// Create a VARRAY builder.
    pub fn varray(element_type: OracleDataType, max_size: usize) -> Self {
        Self {
            collection_type: CollectionType::Varray,
            element_type,
            elements: Vec::new(),
            max_size: Some(max_size),
        }
    }

    /// Create a NESTED TABLE builder.
    pub fn nested_table(element_type: OracleDataType) -> Self {
        Self {
            collection_type: CollectionType::NestedTable,
            element_type,
            elements: Vec::new(),
            max_size: None,
        }
    }

    /// Add an element.
    pub fn push(mut self, element: CollectionElement) -> Self {
        self.elements.push(element);
        self
    }

    /// Add multiple elements.
    pub fn extend(mut self, elements: impl IntoIterator<Item = CollectionElement>) -> Self {
        self.elements.extend(elements);
        self
    }

    /// Build the wire format bytes.
    pub fn build(self) -> Vec<u8> {
        let mut data = Vec::new();

        // Collection type
        data.push(self.collection_type.as_u8());

        // Element count
        write_varint(&mut data, self.elements.len() as u64);

        // Max size for VARRAY
        if let Some(max) = self.max_size {
            write_varint(&mut data, max as u64);
        }

        // Null bitmap for nested tables
        if self.collection_type.can_be_sparse() && !self.elements.is_empty() {
            let bitmap_bytes = self.elements.len().div_ceil(8);
            let mut bitmap = vec![0u8; bitmap_bytes];
            for (i, elem) in self.elements.iter().enumerate() {
                if elem.is_null() {
                    bitmap[i / 8] |= 1 << (i % 8);
                }
            }
            data.extend_from_slice(&bitmap);
        }

        // Elements
        for element in &self.elements {
            if !element.is_null() {
                self.write_element(&mut data, element);
            }
        }

        data
    }

    fn write_element(&self, data: &mut Vec<u8>, element: &CollectionElement) {
        match element {
            CollectionElement::Null => {
                // Already handled by null bitmap
            }
            CollectionElement::Int(n) => {
                // Write as Oracle NUMBER (simplified)
                let bytes = n.to_be_bytes();
                data.push(bytes.len() as u8);
                data.extend_from_slice(&bytes);
            }
            CollectionElement::Float(f) => {
                let bytes = f.to_be_bytes();
                data.push(bytes.len() as u8);
                data.extend_from_slice(&bytes);
            }
            CollectionElement::String(s) => {
                let bytes = s.as_bytes();
                write_varint(data, bytes.len() as u64);
                data.extend_from_slice(bytes);
            }
            CollectionElement::Binary(b) | CollectionElement::Raw(b) => {
                write_varint(data, b.len() as u64);
                data.extend_from_slice(b);
            }
            CollectionElement::Collection(c) => {
                // Recursively build nested collection
                let nested = CollectionBuilder {
                    collection_type: c.collection_type,
                    element_type: c.element_type,
                    elements: c.elements.clone(),
                    max_size: c.max_size,
                }
                .build();
                data.extend_from_slice(&nested);
            }
        }
    }
}

fn write_varint(data: &mut Vec<u8>, mut n: u64) {
    loop {
        let mut byte = (n & 0x7F) as u8;
        n >>= 7;
        if n != 0 {
            byte |= 0x80;
        }
        data.push(byte);
        if n == 0 {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collection_type_roundtrip() {
        assert_eq!(CollectionType::from_u8(0x6E), Some(CollectionType::Varray));
        assert_eq!(CollectionType::from_u8(0x6F), Some(CollectionType::NestedTable));
        assert_eq!(CollectionType::Varray.as_u8(), 0x6E);
        assert_eq!(CollectionType::NestedTable.as_u8(), 0x6F);
    }

    #[test]
    fn test_collection_type_sparse() {
        assert!(!CollectionType::Varray.can_be_sparse());
        assert!(CollectionType::NestedTable.can_be_sparse());
    }

    #[test]
    fn test_varray_creation() {
        let mut varray = Varray::new(OracleDataType::Number, 10);
        assert_eq!(varray.max_size(), 10);
        assert!(varray.is_empty());

        varray.push(CollectionElement::Int(42)).unwrap();
        assert_eq!(varray.len(), 1);
    }

    #[test]
    fn test_varray_size_limit() {
        let mut varray = Varray::new(OracleDataType::Number, 2);
        varray.push(CollectionElement::Int(1)).unwrap();
        varray.push(CollectionElement::Int(2)).unwrap();

        let result = varray.push(CollectionElement::Int(3));
        assert!(matches!(result, Err(CollectionError::SizeExceeded { .. })));
    }

    #[test]
    fn test_nested_table_creation() {
        let mut table = NestedTable::new(OracleDataType::Varchar2);
        assert!(table.is_empty());
        assert!(!table.is_sparse());

        table.push(CollectionElement::String("a".to_string()));
        table.push(CollectionElement::String("b".to_string()));
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn test_nested_table_sparse() {
        let mut table = NestedTable::new(OracleDataType::Number);
        table.push(CollectionElement::Int(1));
        table.push(CollectionElement::Int(2));
        table.push(CollectionElement::Int(3));

        assert!(!table.is_sparse());
        table.delete(1);
        assert!(table.is_sparse());
    }

    #[test]
    fn test_collection_element_accessors() {
        let null = CollectionElement::Null;
        assert!(null.is_null());
        assert!(null.as_i64().is_none());

        let int = CollectionElement::Int(42);
        assert!(!int.is_null());
        assert_eq!(int.as_i64(), Some(42));
        assert_eq!(int.as_f64(), Some(42.0));

        let float = CollectionElement::Float(3.25);
        assert_eq!(float.as_f64(), Some(3.25));

        let string = CollectionElement::String("hello".to_string());
        assert_eq!(string.as_str(), Some("hello"));

        let binary = CollectionElement::Binary(vec![1, 2, 3]);
        assert_eq!(binary.as_bytes(), Some(&[1, 2, 3][..]));
    }

    #[test]
    fn test_oracle_collection_non_null() {
        let mut coll = OracleCollection::nested_table(OracleDataType::Number);
        coll.elements.push(CollectionElement::Int(1));
        coll.elements.push(CollectionElement::Null);
        coll.elements.push(CollectionElement::Int(3));

        assert_eq!(coll.count_non_null(), 2);
        assert!(coll.is_sparse());

        let non_null: Vec<_> = coll.non_null_elements().collect();
        assert_eq!(non_null.len(), 2);
    }

    #[test]
    fn test_to_optional_vec() {
        let mut coll = OracleCollection::varray(OracleDataType::Number, 10);
        coll.elements.push(CollectionElement::Int(1));
        coll.elements.push(CollectionElement::Null);
        coll.elements.push(CollectionElement::Int(3));

        let vec = coll.to_optional_vec(|e| e.as_i64());
        assert_eq!(vec, vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn test_builder_varray() {
        let data = CollectionBuilder::varray(OracleDataType::Number, 10).push(CollectionElement::String("test".to_string())).build();

        // Should have: type byte, count, max_size, element
        assert!(!data.is_empty());
        assert_eq!(data[0], CollectionType::Varray.as_u8());
    }

    #[test]
    fn test_builder_nested_table_with_nulls() {
        let data = CollectionBuilder::nested_table(OracleDataType::Varchar2)
            .push(CollectionElement::String("a".to_string()))
            .push(CollectionElement::Null)
            .push(CollectionElement::String("c".to_string()))
            .build();

        // Should have null bitmap
        assert!(!data.is_empty());
        assert_eq!(data[0], CollectionType::NestedTable.as_u8());
    }

    #[test]
    fn test_varint_encoding() {
        let mut data = Vec::new();
        write_varint(&mut data, 127);
        assert_eq!(data, vec![127]);

        data.clear();
        write_varint(&mut data, 128);
        assert_eq!(data, vec![0x80, 0x01]);

        data.clear();
        write_varint(&mut data, 16384);
        assert_eq!(data, vec![0x80, 0x80, 0x01]);
    }
}
