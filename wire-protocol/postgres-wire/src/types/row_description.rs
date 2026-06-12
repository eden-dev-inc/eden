//! RowDescription message.

use crate::error::backend;
use crate::parse::{PgParse, PgParseError, PgParseSync};
use crate::pg_ext::{PgRead, PgReadSync};
use crate::write::MessageBuilder;
use wire_stream::{WireRead, WireReadSync};

/// Description of a single field (column) in a result set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldDescription {
    /// The field name.
    pub name: String,
    /// The table OID (0 if not from a table).
    pub table_oid: i32,
    /// The column attribute number (0 if not from a table).
    pub column_id: i16,
    /// The data type OID.
    pub type_oid: i32,
    /// The data type size (negative for variable-length types).
    pub type_size: i16,
    /// The type modifier (e.g., VARCHAR length).
    pub type_modifier: i32,
    /// The format code (0 = text, 1 = binary).
    pub format_code: i16,
}

impl FieldDescription {
    /// Check if this field uses text format.
    pub fn is_text_format(&self) -> bool {
        self.format_code == 0
    }

    /// Check if this field uses binary format.
    pub fn is_binary_format(&self) -> bool {
        self.format_code == 1
    }

    /// Check if this field has a variable-length type.
    pub fn is_variable_length(&self) -> bool {
        self.type_size < 0
    }
}

/// RowDescription message from the server.
///
/// Describes the columns in a result set. Sent before DataRow messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowDescription {
    /// The field descriptions.
    pub fields: Vec<FieldDescription>,
}

impl RowDescription {
    /// Create a new row description.
    pub fn new(fields: Vec<FieldDescription>) -> Self {
        Self { fields }
    }

    /// Get the number of fields.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Check if there are no fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Get a field by index.
    pub fn get(&self, index: usize) -> Option<&FieldDescription> {
        self.fields.get(index)
    }

    /// Get a field by name.
    pub fn get_by_name(&self, name: &str) -> Option<&FieldDescription> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Encode the row description message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(backend::ROW_DESCRIPTION).write_i16_be(self.fields.len() as i16);

        for field in &self.fields {
            builder
                .write_cstring_str(&field.name)
                .write_i32_be(field.table_oid)
                .write_i16_be(field.column_id)
                .write_i32_be(field.type_oid)
                .write_i16_be(field.type_size)
                .write_i32_be(field.type_modifier)
                .write_i16_be(field.format_code);
        }

        builder.finish_owned()
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum RowDescriptionError {
    #[error("invalid encoding")]
    InvalidEncoding,
    #[error("unexpected message type: expected 'T', got '{0}'")]
    UnexpectedMessageType(char),
    #[error("too many fields: {0}")]
    TooManyFields(i16),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for RowDescription {
    type ParseError = RowDescriptionError;
    type Value<'s>
        = RowDescription
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::ROW_DESCRIPTION {
            return Err(PgParseError::Parse(RowDescriptionError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let field_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
        if field_count < 0 {
            return Err(PgParseError::Parse(RowDescriptionError::TooManyFields(field_count)));
        }

        let mut fields = Vec::with_capacity(field_count as usize);

        for _ in 0..field_count {
            let name_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
            let name = String::from_utf8(name_bytes).map_err(|_| PgParseError::Parse(RowDescriptionError::InvalidEncoding))?;

            let table_oid = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
            let column_id = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
            let type_oid = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
            let type_size = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
            let type_modifier = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
            let format_code = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;

            fields.push(FieldDescription {
                name,
                table_oid,
                column_id,
                type_oid,
                type_size,
                type_modifier,
                format_code,
            });
        }

        Ok(RowDescription { fields })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for RowDescription {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::ROW_DESCRIPTION {
            return Err(PgParseError::Parse(RowDescriptionError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let field_count = stream.read_i16_be().await.map_err(PgParseError::Stream)?;
        if field_count < 0 {
            return Err(PgParseError::Parse(RowDescriptionError::TooManyFields(field_count)));
        }

        let mut fields = Vec::with_capacity(field_count as usize);

        for _ in 0..field_count {
            let name_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
            let name = String::from_utf8(name_bytes).map_err(|_| PgParseError::Parse(RowDescriptionError::InvalidEncoding))?;

            let table_oid = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
            let column_id = stream.read_i16_be().await.map_err(PgParseError::Stream)?;
            let type_oid = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
            let type_size = stream.read_i16_be().await.map_err(PgParseError::Stream)?;
            let type_modifier = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
            let format_code = stream.read_i16_be().await.map_err(PgParseError::Stream)?;

            fields.push(FieldDescription {
                name,
                table_oid,
                column_id,
                type_oid,
                type_size,
                type_modifier,
                format_code,
            });
        }

        Ok(RowDescription { fields })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_row_description() {
        let desc = RowDescription::new(vec![
            FieldDescription {
                name: "id".to_string(),
                table_oid: 12345,
                column_id: 1,
                type_oid: 23, // INT4
                type_size: 4,
                type_modifier: -1,
                format_code: 0,
            },
            FieldDescription {
                name: "name".to_string(),
                table_oid: 12345,
                column_id: 2,
                type_oid: 25, // TEXT
                type_size: -1,
                type_modifier: -1,
                format_code: 0,
            },
        ]);

        let encoded = desc.encode();
        assert_eq!(encoded[0], b'T');

        let stream = SliceStream::new(&encoded);
        let decoded = RowDescription::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.fields[0].name, "id");
        assert_eq!(decoded.fields[0].type_oid, 23);
        assert!(!decoded.fields[0].is_variable_length());
        assert_eq!(decoded.fields[1].name, "name");
        assert_eq!(decoded.fields[1].type_oid, 25);
        assert!(decoded.fields[1].is_variable_length());
    }
}
