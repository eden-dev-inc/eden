//! MySQL column definition packet.
//!
//! Describes the metadata for a column in a result set.

use crate::mysql_ext::MysqlReadSync;
use crate::parse::{MysqlParse, MysqlParseError, MysqlParseSync};
use wire_stream::{WireRead, WireReadSync};

/// Column definition (Protocol::ColumnDefinition41).
#[derive(Clone, Debug)]
pub struct ColumnDefinition {
    /// Catalog name (usually "def").
    pub catalog: String,
    /// Database name.
    pub schema: String,
    /// Virtual table name (alias).
    pub table: String,
    /// Physical table name.
    pub org_table: String,
    /// Virtual column name (alias).
    pub name: String,
    /// Physical column name.
    pub org_name: String,
    /// Character set number.
    pub character_set: u16,
    /// Maximum column length.
    pub column_length: u32,
    /// Column type (see column_types).
    pub column_type: u8,
    /// Column flags.
    pub flags: u16,
    /// Decimal places (for numeric types).
    pub decimals: u8,
}

impl ColumnDefinition {
    /// Check if the column is NOT NULL.
    pub fn is_not_null(&self) -> bool {
        self.flags & 0x0001 != 0
    }

    /// Check if the column is a primary key.
    pub fn is_primary_key(&self) -> bool {
        self.flags & 0x0002 != 0
    }

    /// Check if the column is unique.
    pub fn is_unique(&self) -> bool {
        self.flags & 0x0004 != 0
    }

    /// Check if the column is indexed.
    pub fn is_indexed(&self) -> bool {
        self.flags & 0x0008 != 0
    }

    /// Check if the column is UNSIGNED.
    pub fn is_unsigned(&self) -> bool {
        self.flags & 0x0020 != 0
    }

    /// Check if the column is ZEROFILL.
    pub fn is_zerofill(&self) -> bool {
        self.flags & 0x0040 != 0
    }

    /// Check if the column is BINARY.
    pub fn is_binary(&self) -> bool {
        self.flags & 0x0080 != 0
    }

    /// Check if the column is ENUM.
    pub fn is_enum(&self) -> bool {
        self.flags & 0x0100 != 0
    }

    /// Check if the column is AUTO_INCREMENT.
    pub fn is_auto_increment(&self) -> bool {
        self.flags & 0x0200 != 0
    }

    /// Check if the column is TIMESTAMP.
    pub fn is_timestamp(&self) -> bool {
        self.flags & 0x0400 != 0
    }

    /// Check if the column is SET.
    pub fn is_set(&self) -> bool {
        self.flags & 0x0800 != 0
    }

    /// Get the column type name.
    pub fn type_name(&self) -> &'static str {
        crate::error::column_type_name(self.column_type)
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum ColumnDefinitionError {
    #[error("invalid length-encoded integer")]
    InvalidLenEnc,
    #[error("invalid string encoding")]
    InvalidStringEncoding,
    #[error("column definition too short")]
    TooShort,
}

impl<S: WireReadSync + ?Sized> MysqlParseSync<S> for ColumnDefinition {
    type ParseError = ColumnDefinitionError;
    type Value<'s>
        = ColumnDefinition
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // catalog (length-encoded string)
        let catalog = stream
            .read_lenenc_string_sync()
            .map_err(MysqlParseError::Stream)?
            .map_err(|_| MysqlParseError::Parse(ColumnDefinitionError::InvalidLenEnc))?;
        let catalog = String::from_utf8_lossy(&catalog).into_owned();

        // schema (length-encoded string)
        let schema = stream
            .read_lenenc_string_sync()
            .map_err(MysqlParseError::Stream)?
            .map_err(|_| MysqlParseError::Parse(ColumnDefinitionError::InvalidLenEnc))?;
        let schema = String::from_utf8_lossy(&schema).into_owned();

        // table (length-encoded string)
        let table = stream
            .read_lenenc_string_sync()
            .map_err(MysqlParseError::Stream)?
            .map_err(|_| MysqlParseError::Parse(ColumnDefinitionError::InvalidLenEnc))?;
        let table = String::from_utf8_lossy(&table).into_owned();

        // org_table (length-encoded string)
        let org_table = stream
            .read_lenenc_string_sync()
            .map_err(MysqlParseError::Stream)?
            .map_err(|_| MysqlParseError::Parse(ColumnDefinitionError::InvalidLenEnc))?;
        let org_table = String::from_utf8_lossy(&org_table).into_owned();

        // name (length-encoded string)
        let name = stream
            .read_lenenc_string_sync()
            .map_err(MysqlParseError::Stream)?
            .map_err(|_| MysqlParseError::Parse(ColumnDefinitionError::InvalidLenEnc))?;
        let name = String::from_utf8_lossy(&name).into_owned();

        // org_name (length-encoded string)
        let org_name = stream
            .read_lenenc_string_sync()
            .map_err(MysqlParseError::Stream)?
            .map_err(|_| MysqlParseError::Parse(ColumnDefinitionError::InvalidLenEnc))?;
        let org_name = String::from_utf8_lossy(&org_name).into_owned();

        // Fixed-length fields indicator (always 0x0C)
        let _length_of_fixed_fields = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;

        // character_set (2 bytes)
        let character_set = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;

        // column_length (4 bytes)
        let column_length = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;

        // column_type (1 byte)
        let column_type = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;

        // flags (2 bytes)
        let flags = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;

        // decimals (1 byte)
        let decimals = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;

        // filler (2 bytes)
        let _ = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;

        Ok(ColumnDefinition {
            catalog,
            schema,
            table,
            org_table,
            name,
            org_name,
            character_set,
            column_length,
            column_type,
            flags,
            decimals,
        })
    }
}

impl<S: WireRead + ?Sized> MysqlParse<S> for ColumnDefinition {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_sync(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::write_lenenc_string;
    use wire_stream::SliceStream;

    fn make_column_definition(name: &str, col_type: u8) -> Vec<u8> {
        let mut data = Vec::new();

        // catalog
        write_lenenc_string(&mut data, b"def").unwrap();
        // schema
        write_lenenc_string(&mut data, b"test").unwrap();
        // table
        write_lenenc_string(&mut data, b"users").unwrap();
        // org_table
        write_lenenc_string(&mut data, b"users").unwrap();
        // name
        write_lenenc_string(&mut data, name.as_bytes()).unwrap();
        // org_name
        write_lenenc_string(&mut data, name.as_bytes()).unwrap();

        // Fixed length fields indicator
        data.push(0x0C);

        // character_set (utf8mb4 = 255)
        data.extend_from_slice(&255u16.to_le_bytes());

        // column_length
        data.extend_from_slice(&255u32.to_le_bytes());

        // column_type
        data.push(col_type);

        // flags (NOT_NULL = 0x0001)
        data.extend_from_slice(&0x0001u16.to_le_bytes());

        // decimals
        data.push(0);

        // filler
        data.extend_from_slice(&0u16.to_le_bytes());

        data
    }

    #[test]
    fn test_parse_column_definition() {
        let data = make_column_definition("id", crate::error::column_types::MYSQL_TYPE_LONG);
        let stream = SliceStream::new(&data);

        let col = ColumnDefinition::parse_sync(&stream).unwrap();

        assert_eq!(col.catalog, "def");
        assert_eq!(col.schema, "test");
        assert_eq!(col.table, "users");
        assert_eq!(col.name, "id");
        assert_eq!(col.column_type, crate::error::column_types::MYSQL_TYPE_LONG);
        assert!(col.is_not_null());
    }

    #[test]
    fn test_column_flags() {
        let mut data = make_column_definition("id", 0x03);
        // Modify flags to include PRIMARY_KEY | NOT_NULL | AUTO_INCREMENT
        let flags_offset = data.len() - 5; // flags are 5 bytes from end
        data[flags_offset] = 0x03; // NOT_NULL | PRIMARY_KEY
        data[flags_offset + 1] = 0x02; // AUTO_INCREMENT (0x0200)

        let stream = SliceStream::new(&data);
        let col = ColumnDefinition::parse_sync(&stream).unwrap();

        assert!(col.is_not_null());
        assert!(col.is_primary_key());
        assert!(col.is_auto_increment());
    }
}
