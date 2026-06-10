//! ClientInfo structure for ClickHouse native protocol.
//!
//! Contains information about the client executing a query.

use crate::error::ClickhouseWireError;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Query kind enum.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
#[repr(u8)]
pub enum QueryKind {
    /// No query (placeholder).
    #[default]
    None = 0,
    /// Initial query from user.
    Initial = 1,
    /// Secondary query (e.g., from distributed table).
    Secondary = 2,
}

impl QueryKind {
    /// Convert from u8.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::None),
            1 => Some(Self::Initial),
            2 => Some(Self::Secondary),
            _ => None,
        }
    }
}

/// Interface type (how the query was initiated).
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
#[repr(u8)]
pub enum Interface {
    /// TCP interface.
    #[default]
    Tcp = 1,
    /// HTTP interface.
    Http = 2,
}

impl Interface {
    /// Convert from u8.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Tcp),
            2 => Some(Self::Http),
            _ => None,
        }
    }
}

/// Client information sent with queries.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClientInfo {
    /// Query kind (initial, secondary, etc.).
    pub query_kind: QueryKind,
    /// Initial user (for distributed queries).
    pub initial_user: String,
    /// Initial query ID.
    pub initial_query_id: String,
    /// Initial address (IP:port).
    pub initial_address: String,
    /// Initial query start time (microseconds since epoch).
    pub initial_query_start_time_microseconds: u64,
    /// Interface type.
    pub interface: Interface,
    /// OS user running the client.
    pub os_user: String,
    /// Client hostname.
    pub client_hostname: String,
    /// Client name.
    pub client_name: String,
    /// Client version major.
    pub client_version_major: u64,
    /// Client version minor.
    pub client_version_minor: u64,
    /// Client revision.
    pub client_revision: u64,
    /// Quota key.
    pub quota_key: String,
    /// Distributed depth.
    pub distributed_depth: u64,
    /// Client version patch.
    pub client_version_patch: u64,
    /// OpenTelemetry trace context (if enabled).
    pub otel_trace_id: u128,
    /// OpenTelemetry span ID.
    pub otel_span_id: u64,
    /// OpenTelemetry trace state.
    pub otel_tracestate: String,
    /// OpenTelemetry trace flags.
    pub otel_trace_flags: u8,
    /// Collaborate with initiator (for parallel replicas).
    pub collaborate_with_initiator: bool,
    /// Count participating replicas.
    pub count_participating_replicas: u64,
    /// Number of current replica.
    pub number_of_current_replica: u64,
}

/// Protocol revision thresholds for conditional parsing.
pub mod revisions {
    /// Minimum revision with initial query start time.
    pub const INITIAL_QUERY_START_TIME: u64 = 54310;
    /// Minimum revision with quota key in ClientInfo.
    pub const QUOTA_KEY: u64 = 54060;
    /// Minimum revision with distributed depth.
    pub const DISTRIBUTED_DEPTH: u64 = 54448;
    /// Minimum revision with client version patch.
    pub const VERSION_PATCH: u64 = 54401;
    /// Minimum revision with OpenTelemetry support.
    pub const OPENTELEMETRY: u64 = 54442;
    /// Minimum revision with parallel replicas.
    pub const PARALLEL_REPLICAS: u64 = 54453;
}

impl ClientInfo {
    /// Create a new ClientInfo with default values.
    pub fn new() -> Self {
        Self {
            query_kind: QueryKind::Initial,
            interface: Interface::Tcp,
            ..Default::default()
        }
    }

    /// Parse ClientInfo from a synchronous stream.
    pub fn parse_sync<S>(stream: &S, protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let query_kind_byte = stream.read_u8_ch_sync()?;
        let query_kind = QueryKind::from_u8(query_kind_byte).unwrap_or(QueryKind::None);

        let initial_user = stream.read_ch_string_utf8_sync()?;
        let initial_query_id = stream.read_ch_string_utf8_sync()?;
        let initial_address = stream.read_ch_string_utf8_sync()?;

        let initial_query_start_time_microseconds = if protocol_version >= revisions::INITIAL_QUERY_START_TIME {
            stream.read_u64_le_ch_sync()?
        } else {
            0
        };

        let interface_byte = stream.read_u8_ch_sync()?;
        let interface = Interface::from_u8(interface_byte).unwrap_or(Interface::Tcp);

        let os_user = stream.read_ch_string_utf8_sync()?;
        let client_hostname = stream.read_ch_string_utf8_sync()?;
        let client_name = stream.read_ch_string_utf8_sync()?;
        let client_version_major = stream.read_varuint_sync()?;
        let client_version_minor = stream.read_varuint_sync()?;
        let client_revision = stream.read_varuint_sync()?;

        let quota_key = if protocol_version >= revisions::QUOTA_KEY {
            stream.read_ch_string_utf8_sync()?
        } else {
            String::new()
        };

        let distributed_depth = if protocol_version >= revisions::DISTRIBUTED_DEPTH {
            stream.read_varuint_sync()?
        } else {
            0
        };

        let client_version_patch = if protocol_version >= revisions::VERSION_PATCH {
            stream.read_varuint_sync()?
        } else {
            client_revision
        };

        let (otel_trace_id, otel_span_id, otel_tracestate, otel_trace_flags) = if protocol_version >= revisions::OPENTELEMETRY {
            let trace_flags = stream.read_u8_ch_sync()?;
            if trace_flags != 0 {
                let trace_id = stream.read_u128_le_ch_sync()?;
                let span_id = stream.read_u64_le_ch_sync()?;
                let tracestate = stream.read_ch_string_utf8_sync()?;
                (trace_id, span_id, tracestate, trace_flags)
            } else {
                (0, 0, String::new(), 0)
            }
        } else {
            (0, 0, String::new(), 0)
        };

        let (collaborate_with_initiator, count_participating_replicas, number_of_current_replica) =
            if protocol_version >= revisions::PARALLEL_REPLICAS {
                let collaborate = stream.read_bool_ch_sync()?;
                let count = stream.read_varuint_sync()?;
                let number = stream.read_varuint_sync()?;
                (collaborate, count, number)
            } else {
                (false, 0, 0)
            };

        Ok(Self {
            query_kind,
            initial_user,
            initial_query_id,
            initial_address,
            initial_query_start_time_microseconds,
            interface,
            os_user,
            client_hostname,
            client_name,
            client_version_major,
            client_version_minor,
            client_revision,
            quota_key,
            distributed_depth,
            client_version_patch,
            otel_trace_id,
            otel_span_id,
            otel_tracestate,
            otel_trace_flags,
            collaborate_with_initiator,
            count_participating_replicas,
            number_of_current_replica,
        })
    }

    /// Parse ClientInfo asynchronously.
    pub async fn parse<S>(stream: &S, protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let query_kind_byte = stream.read_u8_ch().await?;
        let query_kind = QueryKind::from_u8(query_kind_byte).unwrap_or(QueryKind::None);

        let initial_user = stream.read_ch_string_utf8().await?;
        let initial_query_id = stream.read_ch_string_utf8().await?;
        let initial_address = stream.read_ch_string_utf8().await?;

        let initial_query_start_time_microseconds = if protocol_version >= revisions::INITIAL_QUERY_START_TIME {
            stream.read_u64_le_ch().await?
        } else {
            0
        };

        let interface_byte = stream.read_u8_ch().await?;
        let interface = Interface::from_u8(interface_byte).unwrap_or(Interface::Tcp);

        let os_user = stream.read_ch_string_utf8().await?;
        let client_hostname = stream.read_ch_string_utf8().await?;
        let client_name = stream.read_ch_string_utf8().await?;
        let client_version_major = stream.read_varuint().await?;
        let client_version_minor = stream.read_varuint().await?;
        let client_revision = stream.read_varuint().await?;

        let quota_key = if protocol_version >= revisions::QUOTA_KEY {
            stream.read_ch_string_utf8().await?
        } else {
            String::new()
        };

        let distributed_depth = if protocol_version >= revisions::DISTRIBUTED_DEPTH {
            stream.read_varuint().await?
        } else {
            0
        };

        let client_version_patch = if protocol_version >= revisions::VERSION_PATCH {
            stream.read_varuint().await?
        } else {
            client_revision
        };

        let (otel_trace_id, otel_span_id, otel_tracestate, otel_trace_flags) = if protocol_version >= revisions::OPENTELEMETRY {
            let trace_flags = stream.read_u8_ch().await?;
            if trace_flags != 0 {
                let trace_id = stream.read_u128_le_ch().await?;
                let span_id = stream.read_u64_le_ch().await?;
                let tracestate = stream.read_ch_string_utf8().await?;
                (trace_id, span_id, tracestate, trace_flags)
            } else {
                (0, 0, String::new(), 0)
            }
        } else {
            (0, 0, String::new(), 0)
        };

        let (collaborate_with_initiator, count_participating_replicas, number_of_current_replica) =
            if protocol_version >= revisions::PARALLEL_REPLICAS {
                let collaborate = stream.read_bool_ch().await?;
                let count = stream.read_varuint().await?;
                let number = stream.read_varuint().await?;
                (collaborate, count, number)
            } else {
                (false, 0, 0)
            };

        Ok(Self {
            query_kind,
            initial_user,
            initial_query_id,
            initial_address,
            initial_query_start_time_microseconds,
            interface,
            os_user,
            client_hostname,
            client_name,
            client_version_major,
            client_version_minor,
            client_revision,
            quota_key,
            distributed_depth,
            client_version_patch,
            otel_trace_id,
            otel_span_id,
            otel_tracestate,
            otel_trace_flags,
            collaborate_with_initiator,
            count_participating_replicas,
            number_of_current_replica,
        })
    }

    /// Encode ClientInfo to a writer.
    pub fn encode<W: Write>(&self, w: &mut W, protocol_version: u64) -> io::Result<()> {
        w.write_u8_ch(self.query_kind as u8)?;
        w.write_ch_string_utf8(&self.initial_user)?;
        w.write_ch_string_utf8(&self.initial_query_id)?;
        w.write_ch_string_utf8(&self.initial_address)?;

        if protocol_version >= revisions::INITIAL_QUERY_START_TIME {
            w.write_u64_le_ch(self.initial_query_start_time_microseconds)?;
        }

        w.write_u8_ch(self.interface as u8)?;
        w.write_ch_string_utf8(&self.os_user)?;
        w.write_ch_string_utf8(&self.client_hostname)?;
        w.write_ch_string_utf8(&self.client_name)?;
        w.write_varuint(self.client_version_major)?;
        w.write_varuint(self.client_version_minor)?;
        w.write_varuint(self.client_revision)?;

        if protocol_version >= revisions::QUOTA_KEY {
            w.write_ch_string_utf8(&self.quota_key)?;
        }

        if protocol_version >= revisions::DISTRIBUTED_DEPTH {
            w.write_varuint(self.distributed_depth)?;
        }

        if protocol_version >= revisions::VERSION_PATCH {
            w.write_varuint(self.client_version_patch)?;
        }

        if protocol_version >= revisions::OPENTELEMETRY {
            w.write_u8_ch(self.otel_trace_flags)?;
            if self.otel_trace_flags != 0 {
                w.write_u128_le_ch(self.otel_trace_id)?;
                w.write_u64_le_ch(self.otel_span_id)?;
                w.write_ch_string_utf8(&self.otel_tracestate)?;
            }
        }

        if protocol_version >= revisions::PARALLEL_REPLICAS {
            w.write_bool_ch(self.collaborate_with_initiator)?;
            w.write_varuint(self.count_participating_replicas)?;
            w.write_varuint(self.number_of_current_replica)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DBMS_TCP_PROTOCOL_VERSION;
    use wire_stream::SliceStream;

    #[test]
    fn test_client_info_roundtrip() {
        let info = ClientInfo {
            query_kind: QueryKind::Initial,
            initial_user: "testuser".to_string(),
            initial_query_id: "test-query-123".to_string(),
            initial_address: "127.0.0.1:9000".to_string(),
            initial_query_start_time_microseconds: 1234567890,
            interface: Interface::Tcp,
            os_user: "osuser".to_string(),
            client_hostname: "localhost".to_string(),
            client_name: "TestClient".to_string(),
            client_version_major: 21,
            client_version_minor: 8,
            client_revision: 54448,
            quota_key: "".to_string(),
            distributed_depth: 0,
            client_version_patch: 54448,
            otel_trace_id: 0,
            otel_span_id: 0,
            otel_tracestate: String::new(),
            otel_trace_flags: 0,
            collaborate_with_initiator: false,
            count_participating_replicas: 0,
            number_of_current_replica: 0,
        };

        let mut buf = Vec::new();
        info.encode(&mut buf, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        let stream = SliceStream::new(&buf);
        let decoded = ClientInfo::parse_sync(&stream, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        assert_eq!(decoded.query_kind, info.query_kind);
        assert_eq!(decoded.initial_user, info.initial_user);
        assert_eq!(decoded.initial_query_id, info.initial_query_id);
        assert_eq!(decoded.client_name, info.client_name);
    }
}
