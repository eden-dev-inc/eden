//! TNS Connect packet type.
//!
//! The Connect packet is sent by the client to initiate a connection.
//! Its structure varies slightly between TNS versions.

use crate::error::versions;
use crate::oracle_ext::{OracleRead, OracleReadSync};
use crate::parse::{OracleParse, OracleParseError, OracleParseSync};
use wire_stream::{WireRead, WireReadSync, WireReadSyncExt};

/// TNS Connect packet.
///
/// # Version Differences
///
/// - **TNS v8-v10**: Basic connect structure
/// - **TNS v11+**: Adds DRCP (connection pooling) fields
/// - **TNS v12+**: Extended connect data for multitenant
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Connect {
    /// TNS version requested by client.
    pub version: u16,
    /// Compatible version (minimum acceptable).
    pub version_compatible: u16,
    /// Service options flags.
    pub service_options: u16,
    /// Session data unit size.
    pub sdu_size: u16,
    /// Maximum transmission data unit size.
    pub tdu_size: u16,
    /// NT protocol characteristics.
    pub nt_proto_characteristics: u16,
    /// Line turnaround value.
    pub line_turnaround: u16,
    /// Hardware type (byte order indicator).
    pub hardware_type: u16,
    /// Connect data length.
    pub connect_data_length: u16,
    /// Connect data offset from start of packet.
    pub connect_data_offset: u16,
    /// Maximum receivable connect data size.
    pub max_receivable_connect_data: u32,
    /// Connect flags byte 1.
    pub connect_flags_1: u8,
    /// Connect flags byte 2.
    pub connect_flags_2: u8,
    /// Trace cross facility item 1.
    pub trace_cross_facility_1: u32,
    /// Trace cross facility item 2.
    pub trace_cross_facility_2: u32,
    /// Trace unique connection ID.
    pub trace_unique_conn_id: u64,
    /// Extended connect data (TNS v11+ features).
    pub extended: Option<ConnectExtended>,
    /// Connect data string (e.g., TNS descriptor).
    pub connect_data: Vec<u8>,
}

/// Extended connect fields for TNS v11+.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ConnectExtended {
    /// DRCP connection class (TNS v11+).
    pub connection_class: Option<Vec<u8>>,
    /// Purity flags for DRCP (TNS v11+).
    pub purity: Option<u8>,
    /// Connection class length.
    pub connection_class_len: u8,
}

impl ConnectExtended {
    /// Create new extended fields with a connection class.
    pub fn with_connection_class(class: impl Into<Vec<u8>>, purity: u8) -> Self {
        let class_bytes = class.into();
        Self {
            connection_class_len: class_bytes.len() as u8,
            connection_class: Some(class_bytes),
            purity: Some(purity),
        }
    }

    /// Get connection class as string if valid UTF-8.
    pub fn connection_class_str(&self) -> Option<&str> {
        self.connection_class.as_ref().and_then(|c| std::str::from_utf8(c).ok())
    }
}

/// Error when parsing a Connect packet.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("connect data offset beyond packet: offset {offset}, packet length {packet_length}")]
    InvalidOffset { offset: u16, packet_length: u16 },
    #[error("connect data extends beyond packet: offset {offset} + length {length} > {packet_length}")]
    DataBeyondPacket { offset: u16, length: u16, packet_length: u16 },
    #[error("unsupported TNS version: {0}")]
    UnsupportedVersion(u16),
}

impl Connect {
    /// Check if this connect request supports DRCP (TNS v11+).
    pub fn supports_drcp(&self) -> bool {
        self.version >= versions::TNS_V11
    }

    /// Check if this connect request supports multitenant features (TNS v12+).
    pub fn supports_multitenant(&self) -> bool {
        self.version >= versions::TNS_V12
    }

    /// Returns the connect data as a UTF-8 string if valid.
    pub fn connect_data_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.connect_data).ok()
    }

    /// Get DRCP connection class if present.
    pub fn connection_class(&self) -> Option<&[u8]> {
        self.extended.as_ref().and_then(|e| e.connection_class.as_deref())
    }

    /// Get DRCP purity if present.
    pub fn purity(&self) -> Option<u8> {
        self.extended.as_ref().and_then(|e| e.purity)
    }
}

/// Parse extended fields from remaining connect data for v11+.
fn parse_extended_fields(data: &[u8], version: u16) -> Option<ConnectExtended> {
    if version < versions::TNS_V11 || data.is_empty() {
        return None;
    }

    // Extended format: [connection_class_len: u8] [connection_class: bytes] [purity: u8]
    let class_len = data[0] as usize;
    if data.len() < 1 + class_len + 1 {
        return None;
    }

    let connection_class = if class_len > 0 {
        Some(data[1..1 + class_len].to_vec())
    } else {
        None
    };

    let purity = data.get(1 + class_len).copied();

    Some(ConnectExtended {
        connection_class_len: class_len as u8,
        connection_class,
        purity,
    })
}

impl<S: WireReadSync + ?Sized> OracleParseSync<S> for Connect {
    type ParseError = ConnectError;
    type Value<'s>
        = Connect
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let version = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let version_compatible = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;

        if !versions::is_supported(version) {
            return Err(OracleParseError::Parse(ConnectError::UnsupportedVersion(version)));
        }

        let service_options = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let sdu_size = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let tdu_size = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let nt_proto_characteristics = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let line_turnaround = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let hardware_type = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let connect_data_length = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let connect_data_offset = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let max_receivable_connect_data = stream.read_u32_be_sync().map_err(OracleParseError::Stream)?;
        let connect_flags_1 = stream.read_u8_sync().map_err(OracleParseError::Stream)?;
        let connect_flags_2 = stream.read_u8_sync().map_err(OracleParseError::Stream)?;
        let trace_cross_facility_1 = stream.read_u32_be_sync().map_err(OracleParseError::Stream)?;
        let trace_cross_facility_2 = stream.read_u32_be_sync().map_err(OracleParseError::Stream)?;
        let trace_unique_conn_id = stream.read_u64_be_sync().map_err(OracleParseError::Stream)?;

        // Read connect data
        let connect_data = stream.read_bytes_sync(connect_data_length as usize).map_err(OracleParseError::Stream)?.to_vec();

        // Parse extended fields for v11+ (embedded in connect_data or trailing)
        let extended = parse_extended_fields(&connect_data, version);

        Ok(Connect {
            version,
            version_compatible,
            service_options,
            sdu_size,
            tdu_size,
            nt_proto_characteristics,
            line_turnaround,
            hardware_type,
            connect_data_length,
            connect_data_offset,
            max_receivable_connect_data,
            connect_flags_1,
            connect_flags_2,
            trace_cross_facility_1,
            trace_cross_facility_2,
            trace_unique_conn_id,
            extended,
            connect_data,
        })
    }
}

impl<S: WireRead + ?Sized> OracleParse<S> for Connect {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let version = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let version_compatible = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;

        if !versions::is_supported(version) {
            return Err(OracleParseError::Parse(ConnectError::UnsupportedVersion(version)));
        }

        let service_options = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let sdu_size = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let tdu_size = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let nt_proto_characteristics = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let line_turnaround = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let hardware_type = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let connect_data_length = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let connect_data_offset = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let max_receivable_connect_data = stream.read_u32_be().await.map_err(OracleParseError::Stream)?;
        let connect_flags_1 = stream.read_u8().await.map_err(OracleParseError::Stream)?;
        let connect_flags_2 = stream.read_u8().await.map_err(OracleParseError::Stream)?;
        let trace_cross_facility_1 = stream.read_u32_be().await.map_err(OracleParseError::Stream)?;
        let trace_cross_facility_2 = stream.read_u32_be().await.map_err(OracleParseError::Stream)?;
        let trace_unique_conn_id = stream.read_u64_be().await.map_err(OracleParseError::Stream)?;

        // Read connect data
        let connect_data = stream.read_bytes(connect_data_length as usize).await.map_err(OracleParseError::Stream)?.to_vec();

        // Parse extended fields for v11+ (embedded in connect_data or trailing)
        let extended = parse_extended_fields(&connect_data, version);

        Ok(Connect {
            version,
            version_compatible,
            service_options,
            sdu_size,
            tdu_size,
            nt_proto_characteristics,
            line_turnaround,
            hardware_type,
            connect_data_length,
            connect_data_offset,
            max_receivable_connect_data,
            connect_flags_1,
            connect_flags_2,
            trace_cross_facility_1,
            trace_cross_facility_2,
            trace_unique_conn_id,
            extended,
            connect_data,
        })
    }
}
