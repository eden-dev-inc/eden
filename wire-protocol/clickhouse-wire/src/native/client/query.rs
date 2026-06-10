//! Client Query packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::client_info::ClientInfo;
use crate::native::packet::ClientPacketType;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::settings::Settings;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Query processing stage.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
#[repr(u64)]
pub enum QueryStage {
    /// Only fetch column metadata.
    FetchColumns = 0,
    /// Fetch with mergeable state (for distributed queries).
    WithMergeableState = 1,
    /// Complete query execution.
    #[default]
    Complete = 2,
    /// With mergeable state after aggregation.
    WithMergeableStateAfterAggregation = 3,
    /// With mergeable state after aggregation and limit.
    WithMergeableStateAfterAggregationAndLimit = 4,
}

impl QueryStage {
    /// Convert from u64.
    pub fn from_u64(value: u64) -> Option<Self> {
        match value {
            0 => Some(Self::FetchColumns),
            1 => Some(Self::WithMergeableState),
            2 => Some(Self::Complete),
            3 => Some(Self::WithMergeableStateAfterAggregation),
            4 => Some(Self::WithMergeableStateAfterAggregationAndLimit),
            _ => None,
        }
    }
}

/// Client Query packet (type 1).
///
/// Sends a SQL query to the server for execution.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    /// Query ID (UUID or custom string for tracking).
    pub query_id: String,
    /// Client information.
    pub client_info: ClientInfo,
    /// Query settings.
    pub settings: Settings,
    /// Secret for inter-server communication.
    pub secret: String,
    /// Query processing stage.
    pub stage: QueryStage,
    /// Whether compression is enabled for data blocks.
    pub compression: bool,
    /// SQL query text.
    pub query: String,
}

impl Query {
    /// Create a new Query with the given SQL.
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            query_id: String::new(),
            client_info: ClientInfo::new(),
            settings: Settings::new(),
            secret: String::new(),
            stage: QueryStage::Complete,
            compression: false,
            query: sql.into(),
        }
    }

    /// Create a Query with a specific query ID.
    pub fn with_id(query_id: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            query_id: query_id.into(),
            client_info: ClientInfo::new(),
            settings: Settings::new(),
            secret: String::new(),
            stage: QueryStage::Complete,
            compression: false,
            query: sql.into(),
        }
    }

    /// Set compression enabled.
    pub fn with_compression(mut self, compression: bool) -> Self {
        self.compression = compression;
        self
    }

    /// Set the query stage.
    pub fn with_stage(mut self, stage: QueryStage) -> Self {
        self.stage = stage;
        self
    }

    /// Set client info.
    pub fn with_client_info(mut self, client_info: ClientInfo) -> Self {
        self.client_info = client_info;
        self
    }

    /// Set settings.
    pub fn with_settings(mut self, settings: Settings) -> Self {
        self.settings = settings;
        self
    }

    /// Parse a Query packet from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S, protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let query_id = stream.read_ch_string_utf8_sync()?;
        let client_info = ClientInfo::parse_sync(stream, protocol_version)?;
        let settings = Settings::parse_sync(stream, protocol_version)?;
        let secret = stream.read_ch_string_utf8_sync()?;

        let stage_value = stream.read_varuint_sync()?;
        let stage = QueryStage::from_u64(stage_value)
            .ok_or_else(|| ClickhouseWireError::InvalidBlock(format!("invalid query stage: {}", stage_value)))?;

        let compression = stream.read_varuint_sync()? != 0;
        let query = stream.read_ch_string_utf8_sync()?;

        Ok(Self {
            query_id,
            client_info,
            settings,
            secret,
            stage,
            compression,
            query,
        })
    }

    /// Parse a Query packet asynchronously.
    pub async fn parse<S>(stream: &S, protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let query_id = stream.read_ch_string_utf8().await?;
        let client_info = ClientInfo::parse(stream, protocol_version).await?;
        let settings = Settings::parse(stream, protocol_version).await?;
        let secret = stream.read_ch_string_utf8().await?;

        let stage_value = stream.read_varuint().await?;
        let stage = QueryStage::from_u64(stage_value)
            .ok_or_else(|| ClickhouseWireError::InvalidBlock(format!("invalid query stage: {}", stage_value)))?;

        let compression = stream.read_varuint().await? != 0;
        let query = stream.read_ch_string_utf8().await?;

        Ok(Self {
            query_id,
            client_info,
            settings,
            secret,
            stage,
            compression,
            query,
        })
    }

    /// Encode the Query packet (including packet type).
    pub fn encode<W: Write>(&self, w: &mut W, protocol_version: u64) -> io::Result<()> {
        w.write_varuint(ClientPacketType::Query.as_u64())?;
        self.encode_body(w, protocol_version)
    }

    /// Encode the Query packet body (without packet type).
    pub fn encode_body<W: Write>(&self, w: &mut W, protocol_version: u64) -> io::Result<()> {
        w.write_ch_string_utf8(&self.query_id)?;
        self.client_info.encode(w, protocol_version)?;
        self.settings.encode(w, protocol_version)?;
        w.write_ch_string_utf8(&self.secret)?;
        w.write_varuint(self.stage as u64)?;
        w.write_varuint(if self.compression { 1 } else { 0 })?;
        w.write_ch_string_utf8(&self.query)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DBMS_TCP_PROTOCOL_VERSION;
    use wire_stream::SliceStream;

    #[test]
    fn test_query_new() {
        let query = Query::new("SELECT 1");
        assert_eq!(query.query, "SELECT 1");
        assert_eq!(query.stage, QueryStage::Complete);
        assert!(!query.compression);
    }

    #[test]
    fn test_query_with_compression() {
        let query = Query::new("SELECT 1").with_compression(true);
        assert!(query.compression);
    }

    #[test]
    fn test_query_roundtrip() {
        let query = Query::with_id("test-123", "SELECT * FROM test").with_compression(true).with_stage(QueryStage::Complete);

        let mut buf = Vec::new();
        query.encode(&mut buf, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        // Skip packet type byte
        let stream = SliceStream::new(&buf[1..]);
        let decoded = Query::parse_sync(&stream, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        assert_eq!(decoded.query_id, query.query_id);
        assert_eq!(decoded.query, query.query);
        assert_eq!(decoded.compression, query.compression);
        assert_eq!(decoded.stage, query.stage);
    }
}
