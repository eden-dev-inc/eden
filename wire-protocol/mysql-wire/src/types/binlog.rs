//! MySQL binlog event types for replication.
//!
//! This module provides types for parsing MySQL binary log events,
//! used in replication and change data capture scenarios.

use crate::mysql_ext::MysqlReadSync;
use crate::parse::MysqlParseError;
use wire_stream::WireReadSync;

/// Binlog event types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum BinlogEventType {
    /// Unknown event type.
    Unknown = 0,
    /// Start event (v3).
    StartEventV3 = 1,
    /// Query event.
    QueryEvent = 2,
    /// Stop event.
    StopEvent = 3,
    /// Rotate event.
    RotateEvent = 4,
    /// Int variable event.
    IntvarEvent = 5,
    /// Load event.
    LoadEvent = 6,
    /// Slave event.
    SlaveEvent = 7,
    /// Create file event.
    CreateFileEvent = 8,
    /// Append block event.
    AppendBlockEvent = 9,
    /// Exec load event.
    ExecLoadEvent = 10,
    /// Delete file event.
    DeleteFileEvent = 11,
    /// New load event.
    NewLoadEvent = 12,
    /// Random event.
    RandEvent = 13,
    /// User variable event.
    UserVarEvent = 14,
    /// Format description event.
    FormatDescriptionEvent = 15,
    /// XID event (transaction commit).
    XidEvent = 16,
    /// Begin load query event.
    BeginLoadQueryEvent = 17,
    /// Execute load query event.
    ExecuteLoadQueryEvent = 18,
    /// Table map event.
    TableMapEvent = 19,
    /// Pre-GA write rows event.
    PreGaWriteRowsEvent = 20,
    /// Pre-GA update rows event.
    PreGaUpdateRowsEvent = 21,
    /// Pre-GA delete rows event.
    PreGaDeleteRowsEvent = 22,
    /// Write rows event v1.
    WriteRowsEventV1 = 23,
    /// Update rows event v1.
    UpdateRowsEventV1 = 24,
    /// Delete rows event v1.
    DeleteRowsEventV1 = 25,
    /// Incident event.
    IncidentEvent = 26,
    /// Heartbeat log event.
    HeartbeatLogEvent = 27,
    /// Ignorable log event.
    IgnorableLogEvent = 28,
    /// Rows query log event.
    RowsQueryLogEvent = 29,
    /// Write rows event v2.
    WriteRowsEventV2 = 30,
    /// Update rows event v2.
    UpdateRowsEventV2 = 31,
    /// Delete rows event v2.
    DeleteRowsEventV2 = 32,
    /// GTID log event.
    GtidLogEvent = 33,
    /// Anonymous GTID log event.
    AnonymousGtidLogEvent = 34,
    /// Previous GTIDs log event.
    PreviousGtidsLogEvent = 35,
    /// Transaction context event.
    TransactionContextEvent = 36,
    /// View change event.
    ViewChangeEvent = 37,
    /// XA prepare event.
    XaPrepareLogEvent = 38,
    /// Partial update rows event.
    PartialUpdateRowsEvent = 39,
    /// Transaction payload event.
    TransactionPayloadEvent = 40,
    /// Heartbeat log event v2.
    HeartbeatLogEventV2 = 41,
}

impl BinlogEventType {
    /// Parse from a byte value.
    pub fn from_byte(byte: u8) -> Self {
        match byte {
            1 => Self::StartEventV3,
            2 => Self::QueryEvent,
            3 => Self::StopEvent,
            4 => Self::RotateEvent,
            5 => Self::IntvarEvent,
            6 => Self::LoadEvent,
            7 => Self::SlaveEvent,
            8 => Self::CreateFileEvent,
            9 => Self::AppendBlockEvent,
            10 => Self::ExecLoadEvent,
            11 => Self::DeleteFileEvent,
            12 => Self::NewLoadEvent,
            13 => Self::RandEvent,
            14 => Self::UserVarEvent,
            15 => Self::FormatDescriptionEvent,
            16 => Self::XidEvent,
            17 => Self::BeginLoadQueryEvent,
            18 => Self::ExecuteLoadQueryEvent,
            19 => Self::TableMapEvent,
            20 => Self::PreGaWriteRowsEvent,
            21 => Self::PreGaUpdateRowsEvent,
            22 => Self::PreGaDeleteRowsEvent,
            23 => Self::WriteRowsEventV1,
            24 => Self::UpdateRowsEventV1,
            25 => Self::DeleteRowsEventV1,
            26 => Self::IncidentEvent,
            27 => Self::HeartbeatLogEvent,
            28 => Self::IgnorableLogEvent,
            29 => Self::RowsQueryLogEvent,
            30 => Self::WriteRowsEventV2,
            31 => Self::UpdateRowsEventV2,
            32 => Self::DeleteRowsEventV2,
            33 => Self::GtidLogEvent,
            34 => Self::AnonymousGtidLogEvent,
            35 => Self::PreviousGtidsLogEvent,
            36 => Self::TransactionContextEvent,
            37 => Self::ViewChangeEvent,
            38 => Self::XaPrepareLogEvent,
            39 => Self::PartialUpdateRowsEvent,
            40 => Self::TransactionPayloadEvent,
            41 => Self::HeartbeatLogEventV2,
            _ => Self::Unknown,
        }
    }

    /// Check if this is a rows event (INSERT/UPDATE/DELETE).
    pub fn is_rows_event(&self) -> bool {
        matches!(
            self,
            Self::WriteRowsEventV1
                | Self::WriteRowsEventV2
                | Self::UpdateRowsEventV1
                | Self::UpdateRowsEventV2
                | Self::DeleteRowsEventV1
                | Self::DeleteRowsEventV2
                | Self::PartialUpdateRowsEvent
        )
    }

    /// Check if this is a GTID event.
    pub fn is_gtid_event(&self) -> bool {
        matches!(self, Self::GtidLogEvent | Self::AnonymousGtidLogEvent | Self::PreviousGtidsLogEvent)
    }

    /// Get the event name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::StartEventV3 => "START_EVENT_V3",
            Self::QueryEvent => "QUERY_EVENT",
            Self::StopEvent => "STOP_EVENT",
            Self::RotateEvent => "ROTATE_EVENT",
            Self::IntvarEvent => "INTVAR_EVENT",
            Self::LoadEvent => "LOAD_EVENT",
            Self::SlaveEvent => "SLAVE_EVENT",
            Self::CreateFileEvent => "CREATE_FILE_EVENT",
            Self::AppendBlockEvent => "APPEND_BLOCK_EVENT",
            Self::ExecLoadEvent => "EXEC_LOAD_EVENT",
            Self::DeleteFileEvent => "DELETE_FILE_EVENT",
            Self::NewLoadEvent => "NEW_LOAD_EVENT",
            Self::RandEvent => "RAND_EVENT",
            Self::UserVarEvent => "USER_VAR_EVENT",
            Self::FormatDescriptionEvent => "FORMAT_DESCRIPTION_EVENT",
            Self::XidEvent => "XID_EVENT",
            Self::BeginLoadQueryEvent => "BEGIN_LOAD_QUERY_EVENT",
            Self::ExecuteLoadQueryEvent => "EXECUTE_LOAD_QUERY_EVENT",
            Self::TableMapEvent => "TABLE_MAP_EVENT",
            Self::PreGaWriteRowsEvent => "PRE_GA_WRITE_ROWS_EVENT",
            Self::PreGaUpdateRowsEvent => "PRE_GA_UPDATE_ROWS_EVENT",
            Self::PreGaDeleteRowsEvent => "PRE_GA_DELETE_ROWS_EVENT",
            Self::WriteRowsEventV1 => "WRITE_ROWS_EVENT_V1",
            Self::UpdateRowsEventV1 => "UPDATE_ROWS_EVENT_V1",
            Self::DeleteRowsEventV1 => "DELETE_ROWS_EVENT_V1",
            Self::IncidentEvent => "INCIDENT_EVENT",
            Self::HeartbeatLogEvent => "HEARTBEAT_LOG_EVENT",
            Self::IgnorableLogEvent => "IGNORABLE_LOG_EVENT",
            Self::RowsQueryLogEvent => "ROWS_QUERY_LOG_EVENT",
            Self::WriteRowsEventV2 => "WRITE_ROWS_EVENT_V2",
            Self::UpdateRowsEventV2 => "UPDATE_ROWS_EVENT_V2",
            Self::DeleteRowsEventV2 => "DELETE_ROWS_EVENT_V2",
            Self::GtidLogEvent => "GTID_LOG_EVENT",
            Self::AnonymousGtidLogEvent => "ANONYMOUS_GTID_LOG_EVENT",
            Self::PreviousGtidsLogEvent => "PREVIOUS_GTIDS_LOG_EVENT",
            Self::TransactionContextEvent => "TRANSACTION_CONTEXT_EVENT",
            Self::ViewChangeEvent => "VIEW_CHANGE_EVENT",
            Self::XaPrepareLogEvent => "XA_PREPARE_LOG_EVENT",
            Self::PartialUpdateRowsEvent => "PARTIAL_UPDATE_ROWS_EVENT",
            Self::TransactionPayloadEvent => "TRANSACTION_PAYLOAD_EVENT",
            Self::HeartbeatLogEventV2 => "HEARTBEAT_LOG_EVENT_V2",
        }
    }
}

/// Binlog event header (common to all events).
///
/// The header is 19 bytes for MySQL 4.0+ (13 bytes for older versions).
#[derive(Clone, Debug)]
pub struct BinlogEventHeader {
    /// Timestamp when the event was created.
    pub timestamp: u32,
    /// Event type.
    pub event_type: BinlogEventType,
    /// Server ID that created the event.
    pub server_id: u32,
    /// Total size of the event (header + data + checksum).
    pub event_length: u32,
    /// Position of the next event in the binlog.
    pub next_position: u32,
    /// Event flags.
    pub flags: u16,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum BinlogError {
    #[error("invalid binlog event header")]
    InvalidHeader,
    #[error("unsupported binlog version: {0}")]
    UnsupportedVersion(u8),
    #[error("checksum mismatch")]
    ChecksumMismatch,
}

impl BinlogEventHeader {
    /// Header size in bytes (MySQL 4.0+).
    pub const SIZE: usize = 19;

    /// Parse a binlog event header from a stream.
    pub fn parse_sync<S: WireReadSync + ?Sized>(stream: &S) -> Result<Self, MysqlParseError<S::ReadError, BinlogError>> {
        let timestamp = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
        let event_type_byte = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
        let server_id = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
        let event_length = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
        let next_position = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
        let flags = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;

        Ok(Self {
            timestamp,
            event_type: BinlogEventType::from_byte(event_type_byte),
            server_id,
            event_length,
            next_position,
            flags,
        })
    }

    /// Get the payload size (event_length - header size - checksum).
    pub fn payload_size(&self, checksum_size: usize) -> usize {
        self.event_length as usize - Self::SIZE - checksum_size
    }
}

/// Binlog event flags.
pub mod event_flags {
    /// Event is part of a transaction that spans multiple binlog files.
    pub const LOG_EVENT_BINLOG_IN_USE_F: u16 = 0x0001;
    /// Event was forced to be written.
    pub const LOG_EVENT_FORCED_ROTATE_F: u16 = 0x0002;
    /// Event uses thread-specific data.
    pub const LOG_EVENT_THREAD_SPECIFIC_F: u16 = 0x0004;
    /// Event contains AUTOINCREMENT value.
    pub const LOG_EVENT_SUPPRESS_USE_F: u16 = 0x0008;
    /// Event is an artificial event.
    pub const LOG_EVENT_ARTIFICIAL_F: u16 = 0x0020;
    /// Event is a relay log event.
    pub const LOG_EVENT_RELAY_LOG_F: u16 = 0x0040;
    /// Event is ignorable.
    pub const LOG_EVENT_IGNORABLE_F: u16 = 0x0080;
    /// Event should not be logged to relay log.
    pub const LOG_EVENT_NO_FILTER_F: u16 = 0x0100;
    /// MTS can parallelize this event.
    pub const LOG_EVENT_MTS_ISOLATE_F: u16 = 0x0200;
}

/// GTID structure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Gtid {
    /// UUID of the server that originally committed the transaction.
    pub uuid: [u8; 16],
    /// Transaction number (GNO).
    pub gno: u64,
}

impl Gtid {
    /// Create a new GTID.
    pub fn new(uuid: [u8; 16], gno: u64) -> Self {
        Self { uuid, gno }
    }

    /// Parse from binary data.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 24 {
            return None;
        }

        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(&data[0..16]);
        let gno = u64::from_le_bytes([data[16], data[17], data[18], data[19], data[20], data[21], data[22], data[23]]);

        Some(Self { uuid, gno })
    }

    /// Format the UUID as a string (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
    pub fn uuid_string(&self) -> String {
        format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            self.uuid[0],
            self.uuid[1],
            self.uuid[2],
            self.uuid[3],
            self.uuid[4],
            self.uuid[5],
            self.uuid[6],
            self.uuid[7],
            self.uuid[8],
            self.uuid[9],
            self.uuid[10],
            self.uuid[11],
            self.uuid[12],
            self.uuid[13],
            self.uuid[14],
            self.uuid[15]
        )
    }
}

impl std::fmt::Display for Gtid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.uuid_string(), self.gno)
    }
}

/// Rotate event data.
#[derive(Clone, Debug)]
pub struct RotateEventData {
    /// Position in the next binlog file.
    pub position: u64,
    /// Name of the next binlog file.
    pub next_binlog: String,
}

impl RotateEventData {
    /// Parse from event payload.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }

        let position = u64::from_le_bytes([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]]);
        let next_binlog = String::from_utf8_lossy(&data[8..]).into_owned();

        Some(Self { position, next_binlog })
    }
}

/// Query event data.
#[derive(Clone, Debug)]
pub struct QueryEventData {
    /// Thread ID that executed the query.
    pub thread_id: u32,
    /// Execution time in seconds.
    pub exec_time: u32,
    /// Error code (0 if successful).
    pub error_code: u16,
    /// Database name.
    pub database: String,
    /// Query text.
    pub query: String,
}

impl QueryEventData {
    /// Parse from event payload.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 13 {
            return None;
        }

        let thread_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let exec_time = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        let db_len = data[8] as usize;
        let error_code = u16::from_le_bytes([data[9], data[10]]);
        let status_vars_len = u16::from_le_bytes([data[11], data[12]]) as usize;

        let offset = 13 + status_vars_len;
        if offset + db_len + 1 > data.len() {
            return None;
        }

        let database = String::from_utf8_lossy(&data[offset..offset + db_len]).into_owned();
        // Skip NUL terminator
        let query_start = offset + db_len + 1;
        let query = String::from_utf8_lossy(&data[query_start..]).into_owned();

        Some(Self { thread_id, exec_time, error_code, database, query })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_from_byte() {
        assert_eq!(BinlogEventType::from_byte(2), BinlogEventType::QueryEvent);
        assert_eq!(BinlogEventType::from_byte(4), BinlogEventType::RotateEvent);
        assert_eq!(BinlogEventType::from_byte(16), BinlogEventType::XidEvent);
        assert_eq!(BinlogEventType::from_byte(33), BinlogEventType::GtidLogEvent);
        assert_eq!(BinlogEventType::from_byte(255), BinlogEventType::Unknown);
    }

    #[test]
    fn test_is_rows_event() {
        assert!(BinlogEventType::WriteRowsEventV2.is_rows_event());
        assert!(BinlogEventType::UpdateRowsEventV2.is_rows_event());
        assert!(BinlogEventType::DeleteRowsEventV2.is_rows_event());
        assert!(!BinlogEventType::QueryEvent.is_rows_event());
    }

    #[test]
    fn test_gtid() {
        let uuid = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
        ];
        let gtid = Gtid::new(uuid, 42);

        assert_eq!(gtid.uuid_string(), "12345678-9abc-def0-1234-56789abcdef0");
        assert_eq!(gtid.to_string(), "12345678-9abc-def0-1234-56789abcdef0:42");
    }

    #[test]
    fn test_gtid_from_bytes() {
        let mut data = vec![
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
        ];
        data.extend_from_slice(&42u64.to_le_bytes());

        let gtid = Gtid::from_bytes(&data).unwrap();
        assert_eq!(gtid.gno, 42);
    }

    #[test]
    fn test_event_type_name() {
        assert_eq!(BinlogEventType::QueryEvent.name(), "QUERY_EVENT");
        assert_eq!(BinlogEventType::XidEvent.name(), "XID_EVENT");
    }
}
