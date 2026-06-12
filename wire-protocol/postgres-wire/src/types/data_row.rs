//! DataRow message.

use crate::error::backend;
use crate::parse::{PgParse, PgParseError, PgParseSync};
use crate::pg_ext::{PgRead, PgReadSync};
use crate::write::MessageBuilder;
use wire_stream::{WireRead, WireReadSync};

/// A single column value in a data row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnValue {
    /// NULL value.
    Null,
    /// Non-null value (bytes).
    Value(Vec<u8>),
}

impl ColumnValue {
    /// Check if this is NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, ColumnValue::Null)
    }

    /// Get the value bytes, or None if NULL.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            ColumnValue::Null => None,
            ColumnValue::Value(v) => Some(v),
        }
    }

    /// Get the value as a UTF-8 string, or None if NULL or invalid UTF-8.
    pub fn as_str(&self) -> Option<&str> {
        self.as_bytes().and_then(|b| std::str::from_utf8(b).ok())
    }

    /// Get the length of the value, or None if NULL.
    pub fn len(&self) -> Option<usize> {
        match self {
            ColumnValue::Null => None,
            ColumnValue::Value(v) => Some(v.len()),
        }
    }

    /// Check if the value is empty, or None if NULL.
    pub fn is_empty(&self) -> Option<bool> {
        self.len().map(|len| len == 0)
    }
}

/// DataRow message from the server.
///
/// Contains the values for one row of a result set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataRow {
    /// The column values.
    pub columns: Vec<ColumnValue>,
}

impl DataRow {
    /// Create a new data row.
    pub fn new(columns: Vec<ColumnValue>) -> Self {
        Self { columns }
    }

    /// Get the number of columns.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Check if there are no columns.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Get a column by index.
    pub fn get(&self, index: usize) -> Option<&ColumnValue> {
        self.columns.get(index)
    }

    /// Encode the data row message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(backend::DATA_ROW).write_i16_be(self.columns.len() as i16);

        for col in &self.columns {
            match col {
                ColumnValue::Null => {
                    builder.write_i32_be(-1);
                }
                ColumnValue::Value(v) => {
                    builder.write_i32_be(v.len() as i32);
                    builder.write_bytes(v);
                }
            }
        }

        builder.finish_owned()
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum DataRowError {
    #[error("unexpected message type: expected 'D', got '{0}'")]
    UnexpectedMessageType(char),
    #[error("negative column count: {0}")]
    NegativeColumnCount(i16),
    #[error("invalid column length: {0}")]
    InvalidColumnLength(i32),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for DataRow {
    type ParseError = DataRowError;
    type Value<'s>
        = DataRow
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::DATA_ROW {
            return Err(PgParseError::Parse(DataRowError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let column_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
        if column_count < 0 {
            return Err(PgParseError::Parse(DataRowError::NegativeColumnCount(column_count)));
        }

        let mut columns = Vec::with_capacity(column_count as usize);

        for _ in 0..column_count {
            let len = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

            let value = if len == -1 {
                ColumnValue::Null
            } else if len < -1 {
                return Err(PgParseError::Parse(DataRowError::InvalidColumnLength(len)));
            } else {
                let data = stream.read_bytes_sync(len as usize).map_err(PgParseError::Stream)?;
                ColumnValue::Value(data)
            };

            columns.push(value);
        }

        Ok(DataRow { columns })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for DataRow {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::DATA_ROW {
            return Err(PgParseError::Parse(DataRowError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let column_count = stream.read_i16_be().await.map_err(PgParseError::Stream)?;
        if column_count < 0 {
            return Err(PgParseError::Parse(DataRowError::NegativeColumnCount(column_count)));
        }

        let mut columns = Vec::with_capacity(column_count as usize);

        for _ in 0..column_count {
            let len = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

            let value = if len == -1 {
                ColumnValue::Null
            } else if len < -1 {
                return Err(PgParseError::Parse(DataRowError::InvalidColumnLength(len)));
            } else {
                let data = stream.read_bytes(len as usize).await.map_err(PgParseError::Stream)?;
                ColumnValue::Value(data)
            };

            columns.push(value);
        }

        Ok(DataRow { columns })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_data_row() {
        let row = DataRow::new(vec![
            ColumnValue::Value(b"1".to_vec()),
            ColumnValue::Value(b"Alice".to_vec()),
            ColumnValue::Null,
        ]);

        let encoded = row.encode();
        assert_eq!(encoded[0], b'D');

        let stream = SliceStream::new(&encoded);
        let decoded = DataRow::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded.columns[0].as_str(), Some("1"));
        assert_eq!(decoded.columns[1].as_str(), Some("Alice"));
        assert!(decoded.columns[2].is_null());
    }

    #[test]
    fn test_empty_data_row() {
        let row = DataRow::new(vec![]);

        let encoded = row.encode();
        let stream = SliceStream::new(&encoded);
        let decoded = DataRow::parse_sync(&stream).expect("parse failed");

        assert!(decoded.is_empty());
    }
}
