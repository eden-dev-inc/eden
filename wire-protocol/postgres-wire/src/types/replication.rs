//! Replication protocol messages.
//!
//! PostgreSQL's streaming replication protocol is used for physical and logical
//! replication. This module provides types for handling replication messages,
//! which are essential for extensions that use logical decoding (e.g., pgoutput,
//! wal2json, Debezium).
//!
//! # Protocol Flow
//!
//! 1. Client sends `IDENTIFY_SYSTEM` or `START_REPLICATION` via simple query
//! 2. Server responds with `CopyBothResponse` to enter replication mode
//! 3. Server sends `XLogData` and `PrimaryKeepalive` messages
//! 4. Client sends `StandbyStatusUpdate` messages
//!
//! # Message Format
//!
//! Replication messages are sent within CopyData messages. The first byte
//! identifies the replication message type.

/// Replication message type identifiers.
pub mod replication_message {
    /// XLogData message from primary.
    pub const XLOG_DATA: u8 = b'w';
    /// Primary keepalive message.
    pub const PRIMARY_KEEPALIVE: u8 = b'k';
    /// Standby status update (from standby to primary).
    pub const STANDBY_STATUS_UPDATE: u8 = b'r';
    /// Hot standby feedback message.
    pub const HOT_STANDBY_FEEDBACK: u8 = b'h';
}

/// XLogData message from the primary server.
///
/// Contains WAL data for physical or logical replication.
/// The data section contains the actual WAL records or logical decoding output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct XLogData {
    /// The starting point of the WAL data in this message.
    pub wal_start: u64,
    /// The current end of WAL on the server.
    pub wal_end: u64,
    /// The server's system clock at send time (microseconds since 2000-01-01).
    pub send_time: i64,
    /// The WAL data (physical) or logical decoding output.
    pub data: Vec<u8>,
}

impl XLogData {
    /// Create a new XLogData message.
    pub fn new(wal_start: u64, wal_end: u64, send_time: i64, data: Vec<u8>) -> Self {
        Self { wal_start, wal_end, send_time, data }
    }

    /// Encode as a CopyData payload.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(25 + self.data.len());
        buf.push(replication_message::XLOG_DATA);
        buf.extend_from_slice(&self.wal_start.to_be_bytes());
        buf.extend_from_slice(&self.wal_end.to_be_bytes());
        buf.extend_from_slice(&self.send_time.to_be_bytes());
        buf.extend_from_slice(&self.data);
        buf
    }

    /// Parse from a CopyData payload (without the 'w' type byte).
    pub fn parse_payload(data: &[u8]) -> Option<Self> {
        if data.len() < 24 {
            return None;
        }
        let wal_start = u64::from_be_bytes(data[0..8].try_into().ok()?);
        let wal_end = u64::from_be_bytes(data[8..16].try_into().ok()?);
        let send_time = i64::from_be_bytes(data[16..24].try_into().ok()?);
        let payload = data[24..].to_vec();
        Some(Self::new(wal_start, wal_end, send_time, payload))
    }
}

/// Primary keepalive message.
///
/// Sent by the primary to check if the standby is still alive.
/// The standby should respond with a StandbyStatusUpdate if reply_requested is true.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PrimaryKeepalive {
    /// The current end of WAL on the server.
    pub wal_end: u64,
    /// The server's system clock (microseconds since 2000-01-01).
    pub send_time: i64,
    /// True if a reply is requested immediately.
    pub reply_requested: bool,
}

impl PrimaryKeepalive {
    /// Create a new PrimaryKeepalive message.
    pub fn new(wal_end: u64, send_time: i64, reply_requested: bool) -> Self {
        Self { wal_end, send_time, reply_requested }
    }

    /// Encode as a CopyData payload.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(18);
        buf.push(replication_message::PRIMARY_KEEPALIVE);
        buf.extend_from_slice(&self.wal_end.to_be_bytes());
        buf.extend_from_slice(&self.send_time.to_be_bytes());
        buf.push(if self.reply_requested { 1 } else { 0 });
        buf
    }

    /// Parse from a CopyData payload (without the 'k' type byte).
    pub fn parse_payload(data: &[u8]) -> Option<Self> {
        if data.len() < 17 {
            return None;
        }
        let wal_end = u64::from_be_bytes(data[0..8].try_into().ok()?);
        let send_time = i64::from_be_bytes(data[8..16].try_into().ok()?);
        let reply_requested = data[16] != 0;
        Some(Self::new(wal_end, send_time, reply_requested))
    }
}

/// Standby status update message.
///
/// Sent by the standby to update the primary on replication progress.
/// This should be sent periodically and when the primary requests a reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StandbyStatusUpdate {
    /// The location of the last WAL byte + 1 received and written to disk.
    pub write_lsn: u64,
    /// The location of the last WAL byte + 1 flushed to disk.
    pub flush_lsn: u64,
    /// The location of the last WAL byte + 1 applied (replayed).
    pub apply_lsn: u64,
    /// The client's system clock (microseconds since 2000-01-01).
    pub send_time: i64,
    /// If true, requests the server to reply immediately.
    pub reply_requested: bool,
}

impl StandbyStatusUpdate {
    /// Create a new StandbyStatusUpdate message.
    pub fn new(write_lsn: u64, flush_lsn: u64, apply_lsn: u64, send_time: i64, reply_requested: bool) -> Self {
        Self { write_lsn, flush_lsn, apply_lsn, send_time, reply_requested }
    }

    /// Create a simple status update where all LSN positions are the same.
    pub fn simple(lsn: u64, send_time: i64) -> Self {
        Self::new(lsn, lsn, lsn, send_time, false)
    }

    /// Encode as a CopyData payload.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(34);
        buf.push(replication_message::STANDBY_STATUS_UPDATE);
        buf.extend_from_slice(&self.write_lsn.to_be_bytes());
        buf.extend_from_slice(&self.flush_lsn.to_be_bytes());
        buf.extend_from_slice(&self.apply_lsn.to_be_bytes());
        buf.extend_from_slice(&self.send_time.to_be_bytes());
        buf.push(if self.reply_requested { 1 } else { 0 });
        buf
    }

    /// Parse from a CopyData payload (without the 'r' type byte).
    pub fn parse_payload(data: &[u8]) -> Option<Self> {
        if data.len() < 33 {
            return None;
        }
        let write_lsn = u64::from_be_bytes(data[0..8].try_into().ok()?);
        let flush_lsn = u64::from_be_bytes(data[8..16].try_into().ok()?);
        let apply_lsn = u64::from_be_bytes(data[16..24].try_into().ok()?);
        let send_time = i64::from_be_bytes(data[24..32].try_into().ok()?);
        let reply_requested = data[32] != 0;
        Some(Self::new(write_lsn, flush_lsn, apply_lsn, send_time, reply_requested))
    }
}

/// Hot standby feedback message.
///
/// Sent by the standby to inform the primary about the oldest transaction
/// that the standby needs, preventing the primary from vacuuming needed data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HotStandbyFeedback {
    /// The client's system clock (microseconds since 2000-01-01).
    pub send_time: i64,
    /// The standby's current global xmin, or 0 if not available.
    pub xmin: u32,
    /// The standby's current global xmin epoch.
    pub xmin_epoch: u32,
    /// The standby's catalog xmin, or 0 if not available.
    pub catalog_xmin: u32,
    /// The standby's catalog xmin epoch.
    pub catalog_xmin_epoch: u32,
}

impl HotStandbyFeedback {
    /// Create a new HotStandbyFeedback message.
    pub fn new(send_time: i64, xmin: u32, xmin_epoch: u32, catalog_xmin: u32, catalog_xmin_epoch: u32) -> Self {
        Self {
            send_time,
            xmin,
            xmin_epoch,
            catalog_xmin,
            catalog_xmin_epoch,
        }
    }

    /// Encode as a CopyData payload.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(25);
        buf.push(replication_message::HOT_STANDBY_FEEDBACK);
        buf.extend_from_slice(&self.send_time.to_be_bytes());
        buf.extend_from_slice(&self.xmin.to_be_bytes());
        buf.extend_from_slice(&self.xmin_epoch.to_be_bytes());
        buf.extend_from_slice(&self.catalog_xmin.to_be_bytes());
        buf.extend_from_slice(&self.catalog_xmin_epoch.to_be_bytes());
        buf
    }

    /// Parse from a CopyData payload (without the 'h' type byte).
    pub fn parse_payload(data: &[u8]) -> Option<Self> {
        if data.len() < 24 {
            return None;
        }
        let send_time = i64::from_be_bytes(data[0..8].try_into().ok()?);
        let xmin = u32::from_be_bytes(data[8..12].try_into().ok()?);
        let xmin_epoch = u32::from_be_bytes(data[12..16].try_into().ok()?);
        let catalog_xmin = u32::from_be_bytes(data[16..20].try_into().ok()?);
        let catalog_xmin_epoch = u32::from_be_bytes(data[20..24].try_into().ok()?);
        Some(Self::new(send_time, xmin, xmin_epoch, catalog_xmin, catalog_xmin_epoch))
    }
}

/// Unified replication message enum.
///
/// Used to parse any replication message from a CopyData payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicationMessage {
    /// XLogData from primary.
    XLogData(XLogData),
    /// Keepalive from primary.
    PrimaryKeepalive(PrimaryKeepalive),
    /// Status update from standby.
    StandbyStatusUpdate(StandbyStatusUpdate),
    /// Hot standby feedback.
    HotStandbyFeedback(HotStandbyFeedback),
    /// Unknown replication message type.
    Unknown { message_type: u8, data: Vec<u8> },
}

impl ReplicationMessage {
    /// Parse a replication message from a CopyData payload.
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let msg_type = data[0];
        let payload = &data[1..];

        match msg_type {
            replication_message::XLOG_DATA => XLogData::parse_payload(payload).map(ReplicationMessage::XLogData),
            replication_message::PRIMARY_KEEPALIVE => PrimaryKeepalive::parse_payload(payload).map(ReplicationMessage::PrimaryKeepalive),
            replication_message::STANDBY_STATUS_UPDATE => {
                StandbyStatusUpdate::parse_payload(payload).map(ReplicationMessage::StandbyStatusUpdate)
            }
            replication_message::HOT_STANDBY_FEEDBACK => {
                HotStandbyFeedback::parse_payload(payload).map(ReplicationMessage::HotStandbyFeedback)
            }
            _ => Some(ReplicationMessage::Unknown { message_type: msg_type, data: payload.to_vec() }),
        }
    }
}

/// Helper to parse LSN (Log Sequence Number) from string format.
///
/// PostgreSQL LSN format: "XXXXXXXX/YYYYYYYY" where X and Y are hex digits.
pub fn parse_lsn(s: &str) -> Option<u64> {
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() != 2 {
        return None;
    }
    let high = u32::from_str_radix(parts[0], 16).ok()?;
    let low = u32::from_str_radix(parts[1], 16).ok()?;
    Some(((high as u64) << 32) | (low as u64))
}

/// Helper to format LSN (Log Sequence Number) to string format.
pub fn format_lsn(lsn: u64) -> String {
    let high = (lsn >> 32) as u32;
    let low = lsn as u32;
    format!("{:X}/{:X}", high, low)
}

/// Calculate send_time in PostgreSQL's timestamp format.
///
/// Returns microseconds since 2000-01-01 00:00:00 UTC.
pub fn pg_timestamp_now() -> i64 {
    // PostgreSQL epoch: 2000-01-01 00:00:00 UTC
    // Unix epoch offset from PG epoch: 946684800 seconds
    const PG_EPOCH_OFFSET_SECS: i64 = 946_684_800;

    // Get current Unix timestamp in microseconds
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();

    let unix_micros = now.as_micros() as i64;
    unix_micros - (PG_EPOCH_OFFSET_SECS * 1_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_parse_format() {
        let lsn_str = "16/3002D50";
        let lsn = parse_lsn(lsn_str).unwrap();
        assert_eq!(lsn, 0x0000001603002D50);

        let formatted = format_lsn(lsn);
        assert_eq!(formatted, "16/3002D50");
    }

    #[test]
    fn test_xlog_data_encode_parse() {
        let msg = XLogData::new(100, 200, 12345678, b"hello".to_vec());
        let encoded = msg.encode();

        assert_eq!(encoded[0], replication_message::XLOG_DATA);

        let parsed = XLogData::parse_payload(&encoded[1..]).unwrap();
        assert_eq!(parsed.wal_start, 100);
        assert_eq!(parsed.wal_end, 200);
        assert_eq!(parsed.send_time, 12345678);
        assert_eq!(parsed.data, b"hello");
    }

    #[test]
    fn test_primary_keepalive_encode_parse() {
        let msg = PrimaryKeepalive::new(500, 99999, true);
        let encoded = msg.encode();

        assert_eq!(encoded[0], replication_message::PRIMARY_KEEPALIVE);

        let parsed = PrimaryKeepalive::parse_payload(&encoded[1..]).unwrap();
        assert_eq!(parsed.wal_end, 500);
        assert_eq!(parsed.send_time, 99999);
        assert!(parsed.reply_requested);
    }

    #[test]
    fn test_standby_status_update_encode_parse() {
        let msg = StandbyStatusUpdate::new(100, 90, 80, 12345, false);
        let encoded = msg.encode();

        assert_eq!(encoded[0], replication_message::STANDBY_STATUS_UPDATE);

        let parsed = StandbyStatusUpdate::parse_payload(&encoded[1..]).unwrap();
        assert_eq!(parsed.write_lsn, 100);
        assert_eq!(parsed.flush_lsn, 90);
        assert_eq!(parsed.apply_lsn, 80);
        assert_eq!(parsed.send_time, 12345);
        assert!(!parsed.reply_requested);
    }

    #[test]
    fn test_replication_message_dispatch() {
        let keepalive = PrimaryKeepalive::new(1000, 2000, false);
        let encoded = keepalive.encode();

        let parsed = ReplicationMessage::parse(&encoded).unwrap();
        match parsed {
            ReplicationMessage::PrimaryKeepalive(k) => {
                assert_eq!(k.wal_end, 1000);
            }
            _ => panic!("wrong message type"),
        }
    }

    #[test]
    fn test_unknown_replication_message() {
        let data = vec![b'Z', 1, 2, 3, 4];
        let parsed = ReplicationMessage::parse(&data).unwrap();
        match parsed {
            ReplicationMessage::Unknown { message_type, data } => {
                assert_eq!(message_type, b'Z');
                assert_eq!(data, vec![1, 2, 3, 4]);
            }
            _ => panic!("should be unknown"),
        }
    }
}
