//! MySQL session state change tracking.
//!
//! MySQL 5.7+ can track session state changes and report them in OK packets
//! when the CLIENT_SESSION_TRACK capability is enabled.

use crate::mysql_ext::MysqlReadSync;
use crate::parse::MysqlParseError;
use wire_stream::WireReadSync;

/// Session state change types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum SessionTrackType {
    /// System variables changed.
    SystemVariables = 0,
    /// Schema changed (USE database).
    Schema = 1,
    /// Session state changed (other).
    StateChange = 2,
    /// GTIDs changed.
    Gtids = 3,
    /// Transaction characteristics changed.
    TransactionCharacteristics = 4,
    /// Transaction state changed.
    TransactionState = 5,
}

impl SessionTrackType {
    /// Parse from a byte value.
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::SystemVariables),
            1 => Some(Self::Schema),
            2 => Some(Self::StateChange),
            3 => Some(Self::Gtids),
            4 => Some(Self::TransactionCharacteristics),
            5 => Some(Self::TransactionState),
            _ => None,
        }
    }
}

/// A single session state change entry.
#[derive(Clone, Debug)]
pub enum SessionStateChange {
    /// System variable changed (name, value).
    SystemVariable { name: String, value: String },
    /// Current schema changed.
    Schema(String),
    /// General state change indicator.
    StateChange(String),
    /// GTID changed.
    Gtid(String),
    /// Transaction characteristics.
    TransactionCharacteristics(String),
    /// Transaction state.
    TransactionState(String),
    /// Unknown type.
    Unknown { track_type: u8, data: Vec<u8> },
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum SessionStateError {
    #[error("invalid length-encoded integer")]
    InvalidLenEnc,
    #[error("invalid UTF-8 in session state")]
    InvalidUtf8,
    #[error("unexpected end of session state data")]
    UnexpectedEnd,
}

/// Parse session state changes from an OK packet's session state info.
///
/// The format is:
/// - Length-encoded total length
/// - Repeated entries:
///   - 1 byte: track type
///   - Length-encoded string: data (format depends on type)
pub fn parse_session_state_changes<S: WireReadSync + ?Sized>(
    stream: &S,
) -> Result<Vec<SessionStateChange>, MysqlParseError<S::ReadError, SessionStateError>> {
    let mut changes = Vec::new();

    // Read total length
    let total_len = stream
        .read_lenenc_int_sync()
        .map_err(MysqlParseError::Stream)?
        .map_err(|_| MysqlParseError::Parse(SessionStateError::InvalidLenEnc))?;

    if total_len == u64::MAX || total_len == 0 {
        return Ok(changes);
    }

    let mut remaining = total_len as usize;

    while remaining > 0 {
        // Read track type
        let track_type = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
        remaining = remaining.saturating_sub(1);

        // Read data length
        let data_len = stream
            .read_lenenc_int_sync()
            .map_err(MysqlParseError::Stream)?
            .map_err(|_| MysqlParseError::Parse(SessionStateError::InvalidLenEnc))?;

        if data_len == u64::MAX {
            continue;
        }

        let data_len = data_len as usize;
        // Approximate bytes used for length encoding
        let len_bytes = if data_len < 251 {
            1
        } else if data_len < 65536 {
            3
        } else {
            4
        };
        remaining = remaining.saturating_sub(len_bytes);

        // Read data
        let data = stream.read_bytes_sync(data_len).map_err(MysqlParseError::Stream)?;
        remaining = remaining.saturating_sub(data_len);

        // Parse based on type
        let change = match SessionTrackType::from_byte(track_type) {
            Some(SessionTrackType::SystemVariables) => parse_system_variable(&data).map_err(MysqlParseError::Parse)?,
            Some(SessionTrackType::Schema) => {
                let schema = parse_lenenc_string_from_slice(&data).map_err(MysqlParseError::Parse)?;
                SessionStateChange::Schema(schema)
            }
            Some(SessionTrackType::StateChange) => {
                let state = parse_lenenc_string_from_slice(&data).map_err(MysqlParseError::Parse)?;
                SessionStateChange::StateChange(state)
            }
            Some(SessionTrackType::Gtids) => {
                let gtid = parse_lenenc_string_from_slice(&data).map_err(MysqlParseError::Parse)?;
                SessionStateChange::Gtid(gtid)
            }
            Some(SessionTrackType::TransactionCharacteristics) => {
                let chars = parse_lenenc_string_from_slice(&data).map_err(MysqlParseError::Parse)?;
                SessionStateChange::TransactionCharacteristics(chars)
            }
            Some(SessionTrackType::TransactionState) => {
                let state = parse_lenenc_string_from_slice(&data).map_err(MysqlParseError::Parse)?;
                SessionStateChange::TransactionState(state)
            }
            None => SessionStateChange::Unknown { track_type, data },
        };

        changes.push(change);
    }

    Ok(changes)
}

/// Parse a system variable change (name, value pair).
fn parse_system_variable(data: &[u8]) -> Result<SessionStateChange, SessionStateError> {
    let mut offset = 0;

    // Parse name (length-encoded string)
    let (name, consumed) = parse_lenenc_string_at(data, offset)?;
    offset += consumed;

    // Parse value (length-encoded string)
    let (value, _) = parse_lenenc_string_at(data, offset)?;

    Ok(SessionStateChange::SystemVariable { name, value })
}

/// Parse a length-encoded string from a slice, returning the string and bytes consumed.
fn parse_lenenc_string_at(data: &[u8], offset: usize) -> Result<(String, usize), SessionStateError> {
    if offset >= data.len() {
        return Err(SessionStateError::UnexpectedEnd);
    }

    let first = data[offset];
    let (len, header_size) = match first {
        0..=0xFA => (first as usize, 1),
        0xFC if offset + 3 <= data.len() => {
            let len = u16::from_le_bytes([data[offset + 1], data[offset + 2]]) as usize;
            (len, 3)
        }
        0xFD if offset + 4 <= data.len() => {
            let len = u32::from_le_bytes([data[offset + 1], data[offset + 2], data[offset + 3], 0]) as usize;
            (len, 4)
        }
        0xFB => return Ok((String::new(), 1)), // NULL
        _ => return Err(SessionStateError::InvalidLenEnc),
    };

    let start = offset + header_size;
    let end = start + len;

    if end > data.len() {
        return Err(SessionStateError::UnexpectedEnd);
    }

    let s = String::from_utf8(data[start..end].to_vec()).map_err(|_| SessionStateError::InvalidUtf8)?;

    Ok((s, header_size + len))
}

/// Parse a length-encoded string from a slice.
fn parse_lenenc_string_from_slice(data: &[u8]) -> Result<String, SessionStateError> {
    let (s, _) = parse_lenenc_string_at(data, 0)?;
    Ok(s)
}

/// Session state changes collection.
#[derive(Clone, Debug, Default)]
pub struct SessionStateInfo {
    /// All session state changes.
    pub changes: Vec<SessionStateChange>,
}

impl SessionStateInfo {
    /// Create empty session state info.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create from a list of changes.
    pub fn from_changes(changes: Vec<SessionStateChange>) -> Self {
        Self { changes }
    }

    /// Check if any schema change occurred.
    pub fn has_schema_change(&self) -> bool {
        self.changes.iter().any(|c| matches!(c, SessionStateChange::Schema(_)))
    }

    /// Get the new schema if one was set.
    pub fn get_schema(&self) -> Option<&str> {
        self.changes.iter().find_map(|c| {
            if let SessionStateChange::Schema(s) = c {
                Some(s.as_str())
            } else {
                None
            }
        })
    }

    /// Check if any system variables changed.
    pub fn has_variable_changes(&self) -> bool {
        self.changes.iter().any(|c| matches!(c, SessionStateChange::SystemVariable { .. }))
    }

    /// Get all changed system variables.
    pub fn get_variables(&self) -> impl Iterator<Item = (&str, &str)> {
        self.changes.iter().filter_map(|c| {
            if let SessionStateChange::SystemVariable { name, value } = c {
                Some((name.as_str(), value.as_str()))
            } else {
                None
            }
        })
    }

    /// Get a specific variable's new value.
    pub fn get_variable(&self, name: &str) -> Option<&str> {
        self.changes.iter().find_map(|c| {
            if let SessionStateChange::SystemVariable { name: n, value } = c {
                if n == name { Some(value.as_str()) } else { None }
            } else {
                None
            }
        })
    }

    /// Check if any GTID changes occurred.
    pub fn has_gtid_changes(&self) -> bool {
        self.changes.iter().any(|c| matches!(c, SessionStateChange::Gtid(_)))
    }

    /// Get the GTID if one was reported.
    pub fn get_gtid(&self) -> Option<&str> {
        self.changes.iter().find_map(|c| {
            if let SessionStateChange::Gtid(g) = c {
                Some(g.as_str())
            } else {
                None
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_track_type() {
        assert_eq!(SessionTrackType::from_byte(0), Some(SessionTrackType::SystemVariables));
        assert_eq!(SessionTrackType::from_byte(1), Some(SessionTrackType::Schema));
        assert_eq!(SessionTrackType::from_byte(5), Some(SessionTrackType::TransactionState));
        assert_eq!(SessionTrackType::from_byte(255), None);
    }

    #[test]
    fn test_parse_lenenc_string_at() {
        // Short string
        let data = [5, b'h', b'e', b'l', b'l', b'o'];
        let (s, consumed) = parse_lenenc_string_at(&data, 0).unwrap();
        assert_eq!(s, "hello");
        assert_eq!(consumed, 6);

        // Empty string
        let data = [0];
        let (s, consumed) = parse_lenenc_string_at(&data, 0).unwrap();
        assert_eq!(s, "");
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_session_state_info() {
        let changes = vec![
            SessionStateChange::Schema("mydb".to_string()),
            SessionStateChange::SystemVariable { name: "autocommit".to_string(), value: "ON".to_string() },
            SessionStateChange::Gtid("uuid:1-5".to_string()),
        ];

        let info = SessionStateInfo::from_changes(changes);

        assert!(info.has_schema_change());
        assert_eq!(info.get_schema(), Some("mydb"));

        assert!(info.has_variable_changes());
        assert_eq!(info.get_variable("autocommit"), Some("ON"));
        assert_eq!(info.get_variable("nonexistent"), None);

        assert!(info.has_gtid_changes());
        assert_eq!(info.get_gtid(), Some("uuid:1-5"));
    }

    #[test]
    fn test_session_state_info_empty() {
        let info = SessionStateInfo::new();

        assert!(!info.has_schema_change());
        assert!(!info.has_variable_changes());
        assert!(!info.has_gtid_changes());
        assert_eq!(info.get_schema(), None);
    }
}
