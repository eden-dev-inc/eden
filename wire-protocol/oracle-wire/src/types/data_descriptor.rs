//! TNS Data Descriptor packet type (TNS v12+).
//!
//! Data descriptor packets are used in Oracle 12c+ for describing
//! data formats in multitenant architectures.

use crate::oracle_ext::{OracleRead, OracleReadSync};
use crate::parse::{OracleParse, OracleParseError, OracleParseSync};
use wire_stream::{WireRead, WireReadSync, WireReadSyncExt};

/// TNS Data Descriptor packet.
///
/// Introduced in TNS v12 to support Oracle 12c multitenant architecture.
/// Used to describe data formats and negotiate data representation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataDescriptor {
    /// Descriptor flags.
    pub flags: u16,
    /// Descriptor type.
    pub descriptor_type: u16,
    /// Number of elements described.
    pub element_count: u16,
    /// Descriptor data.
    pub data: Vec<u8>,
}

/// Descriptor types.
pub mod descriptor_types {
    /// Column descriptor.
    pub const COLUMN: u16 = 1;
    /// Parameter descriptor.
    pub const PARAMETER: u16 = 2;
    /// Return value descriptor.
    pub const RETURN_VALUE: u16 = 3;
    /// Metadata descriptor.
    pub const METADATA: u16 = 4;
}

/// Error when parsing a DataDescriptor packet.
#[derive(Clone, Debug, thiserror::Error)]
pub enum DataDescriptorError {
    #[error("unknown descriptor type: {0}")]
    UnknownType(u16),
    #[error("descriptor data extends beyond packet")]
    DataBeyondPacket,
}

impl DataDescriptor {
    /// Check if this describes columns.
    pub fn is_column_descriptor(&self) -> bool {
        self.descriptor_type == descriptor_types::COLUMN
    }

    /// Check if this describes parameters.
    pub fn is_parameter_descriptor(&self) -> bool {
        self.descriptor_type == descriptor_types::PARAMETER
    }
}

impl<S: WireReadSync + ?Sized> OracleParseSync<S> for DataDescriptor {
    type ParseError = DataDescriptorError;
    type Value<'s>
        = DataDescriptor
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let flags = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let descriptor_type = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let element_count = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let data_length = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;

        let data = stream.read_bytes_sync(data_length as usize).map_err(OracleParseError::Stream)?.to_vec();

        Ok(DataDescriptor { flags, descriptor_type, element_count, data })
    }
}

impl<S: WireRead + ?Sized> OracleParse<S> for DataDescriptor {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let flags = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let descriptor_type = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let element_count = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let data_length = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;

        let data = stream.read_bytes(data_length as usize).await.map_err(OracleParseError::Stream)?.to_vec();

        Ok(DataDescriptor { flags, descriptor_type, element_count, data })
    }
}
