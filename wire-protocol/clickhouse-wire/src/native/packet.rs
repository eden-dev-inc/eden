//! ClickHouse native protocol packet types.
//!
//! Defines the packet type identifiers for client-to-server and server-to-client
//! communication in the ClickHouse native protocol.

/// Client packet types (sent by client to server).
#[derive(Copy, Clone, Eq, PartialEq, Debug, Hash)]
#[repr(u64)]
pub enum ClientPacketType {
    /// Initial handshake packet with client version and credentials.
    Hello = 0,
    /// SQL query with settings and client info.
    Query = 1,
    /// Data block (for INSERT operations).
    Data = 2,
    /// Cancel the current query.
    Cancel = 3,
    /// Ping the server.
    Ping = 4,
    /// Request status of tables.
    TablesStatusRequest = 5,
    /// Keep the connection alive.
    KeepAlive = 6,
    /// Scalar value (single value, not a block).
    Scalar = 7,
    /// Ignored part UUIDs for distributed queries.
    IgnoredPartUUIDs = 8,
    /// Response to server's read task request.
    ReadTaskResponse = 9,
    /// Response to MergeTree read task request.
    MergeTreeReadTaskResponse = 10,
}

impl ClientPacketType {
    /// Convert from u64 to packet type.
    #[inline]
    pub fn from_u64(value: u64) -> Option<Self> {
        match value {
            0 => Some(Self::Hello),
            1 => Some(Self::Query),
            2 => Some(Self::Data),
            3 => Some(Self::Cancel),
            4 => Some(Self::Ping),
            5 => Some(Self::TablesStatusRequest),
            6 => Some(Self::KeepAlive),
            7 => Some(Self::Scalar),
            8 => Some(Self::IgnoredPartUUIDs),
            9 => Some(Self::ReadTaskResponse),
            10 => Some(Self::MergeTreeReadTaskResponse),
            _ => None,
        }
    }

    /// Convert to u64.
    #[inline]
    pub const fn as_u64(self) -> u64 {
        self as u64
    }

    /// Get a human-readable name for the packet type.
    #[inline]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Hello => "Hello",
            Self::Query => "Query",
            Self::Data => "Data",
            Self::Cancel => "Cancel",
            Self::Ping => "Ping",
            Self::TablesStatusRequest => "TablesStatusRequest",
            Self::KeepAlive => "KeepAlive",
            Self::Scalar => "Scalar",
            Self::IgnoredPartUUIDs => "IgnoredPartUUIDs",
            Self::ReadTaskResponse => "ReadTaskResponse",
            Self::MergeTreeReadTaskResponse => "MergeTreeReadTaskResponse",
        }
    }
}

impl std::fmt::Display for ClientPacketType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Server packet types (sent by server to client).
#[derive(Copy, Clone, Eq, PartialEq, Debug, Hash)]
#[repr(u64)]
pub enum ServerPacketType {
    /// Server hello response with version info.
    Hello = 0,
    /// Data block (query results).
    Data = 1,
    /// Exception/error from server.
    Exception = 2,
    /// Query execution progress.
    Progress = 3,
    /// Pong response to client Ping.
    Pong = 4,
    /// End of data stream.
    EndOfStream = 5,
    /// Query profile information.
    ProfileInfo = 6,
    /// Totals for aggregation queries.
    Totals = 7,
    /// Extremes (min/max) for columns.
    Extremes = 8,
    /// Response to TablesStatusRequest.
    TablesStatusResponse = 9,
    /// Server log messages.
    Log = 10,
    /// Table column information.
    TableColumns = 11,
    /// Part UUIDs for distributed queries.
    PartUUIDs = 12,
    /// Request for client to process a read task.
    ReadTaskRequest = 13,
    /// Profile events data.
    ProfileEvents = 14,
    /// MergeTree ranges announcement.
    MergeTreeAllRangesAnnouncement = 15,
    /// MergeTree read task request.
    MergeTreeReadTaskRequest = 16,
    /// Timezone update from server.
    TimezoneUpdate = 17,
}

impl ServerPacketType {
    /// Convert from u64 to packet type.
    #[inline]
    pub fn from_u64(value: u64) -> Option<Self> {
        match value {
            0 => Some(Self::Hello),
            1 => Some(Self::Data),
            2 => Some(Self::Exception),
            3 => Some(Self::Progress),
            4 => Some(Self::Pong),
            5 => Some(Self::EndOfStream),
            6 => Some(Self::ProfileInfo),
            7 => Some(Self::Totals),
            8 => Some(Self::Extremes),
            9 => Some(Self::TablesStatusResponse),
            10 => Some(Self::Log),
            11 => Some(Self::TableColumns),
            12 => Some(Self::PartUUIDs),
            13 => Some(Self::ReadTaskRequest),
            14 => Some(Self::ProfileEvents),
            15 => Some(Self::MergeTreeAllRangesAnnouncement),
            16 => Some(Self::MergeTreeReadTaskRequest),
            17 => Some(Self::TimezoneUpdate),
            _ => None,
        }
    }

    /// Convert to u64.
    #[inline]
    pub const fn as_u64(self) -> u64 {
        self as u64
    }

    /// Get a human-readable name for the packet type.
    #[inline]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Hello => "Hello",
            Self::Data => "Data",
            Self::Exception => "Exception",
            Self::Progress => "Progress",
            Self::Pong => "Pong",
            Self::EndOfStream => "EndOfStream",
            Self::ProfileInfo => "ProfileInfo",
            Self::Totals => "Totals",
            Self::Extremes => "Extremes",
            Self::TablesStatusResponse => "TablesStatusResponse",
            Self::Log => "Log",
            Self::TableColumns => "TableColumns",
            Self::PartUUIDs => "PartUUIDs",
            Self::ReadTaskRequest => "ReadTaskRequest",
            Self::ProfileEvents => "ProfileEvents",
            Self::MergeTreeAllRangesAnnouncement => "MergeTreeAllRangesAnnouncement",
            Self::MergeTreeReadTaskRequest => "MergeTreeReadTaskRequest",
            Self::TimezoneUpdate => "TimezoneUpdate",
        }
    }

    /// Check if this packet type contains data blocks.
    #[inline]
    pub const fn is_data(self) -> bool {
        matches!(self, Self::Data | Self::Totals | Self::Extremes | Self::ProfileEvents | Self::Log)
    }

    /// Check if this packet type indicates an error.
    #[inline]
    pub const fn is_error(self) -> bool {
        matches!(self, Self::Exception)
    }

    /// Check if this packet type indicates end of stream.
    #[inline]
    pub const fn is_end(self) -> bool {
        matches!(self, Self::EndOfStream)
    }
}

impl std::fmt::Display for ServerPacketType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_packet_type_roundtrip() {
        let types = [
            ClientPacketType::Hello,
            ClientPacketType::Query,
            ClientPacketType::Data,
            ClientPacketType::Cancel,
            ClientPacketType::Ping,
            ClientPacketType::TablesStatusRequest,
            ClientPacketType::KeepAlive,
            ClientPacketType::Scalar,
            ClientPacketType::IgnoredPartUUIDs,
            ClientPacketType::ReadTaskResponse,
            ClientPacketType::MergeTreeReadTaskResponse,
        ];

        for pkt in types {
            let value = pkt.as_u64();
            let decoded = ClientPacketType::from_u64(value);
            assert_eq!(decoded, Some(pkt));
        }
    }

    #[test]
    fn test_server_packet_type_roundtrip() {
        let types = [
            ServerPacketType::Hello,
            ServerPacketType::Data,
            ServerPacketType::Exception,
            ServerPacketType::Progress,
            ServerPacketType::Pong,
            ServerPacketType::EndOfStream,
            ServerPacketType::ProfileInfo,
            ServerPacketType::Totals,
            ServerPacketType::Extremes,
            ServerPacketType::TablesStatusResponse,
            ServerPacketType::Log,
            ServerPacketType::TableColumns,
            ServerPacketType::PartUUIDs,
            ServerPacketType::ReadTaskRequest,
            ServerPacketType::ProfileEvents,
            ServerPacketType::MergeTreeAllRangesAnnouncement,
            ServerPacketType::MergeTreeReadTaskRequest,
            ServerPacketType::TimezoneUpdate,
        ];

        for pkt in types {
            let value = pkt.as_u64();
            let decoded = ServerPacketType::from_u64(value);
            assert_eq!(decoded, Some(pkt));
        }
    }

    #[test]
    fn test_unknown_packet_types() {
        assert_eq!(ClientPacketType::from_u64(999), None);
        assert_eq!(ServerPacketType::from_u64(999), None);
    }

    #[test]
    fn test_is_data() {
        assert!(ServerPacketType::Data.is_data());
        assert!(ServerPacketType::Totals.is_data());
        assert!(ServerPacketType::Extremes.is_data());
        assert!(!ServerPacketType::Hello.is_data());
        assert!(!ServerPacketType::Exception.is_data());
    }

    #[test]
    fn test_is_error() {
        assert!(ServerPacketType::Exception.is_error());
        assert!(!ServerPacketType::Data.is_error());
    }

    #[test]
    fn test_is_end() {
        assert!(ServerPacketType::EndOfStream.is_end());
        assert!(!ServerPacketType::Data.is_end());
    }
}
