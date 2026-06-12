//! Query settings for ClickHouse native protocol.
//!
//! Settings are sent as key-value pairs before query execution.

use crate::error::ClickhouseWireError;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::collections::HashMap;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Setting value type.
#[derive(Clone, Debug, PartialEq)]
pub enum SettingValue {
    /// String value.
    String(String),
    /// UInt64 value.
    UInt64(u64),
    /// Int64 value.
    Int64(i64),
    /// Float64 value.
    Float64(f64),
    /// Boolean value.
    Bool(bool),
}

impl SettingValue {
    /// Encode the setting value.
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        match self {
            SettingValue::String(s) => w.write_ch_string_utf8(s).map(|_| ()),
            SettingValue::UInt64(v) => w.write_ch_string_utf8(&v.to_string()).map(|_| ()),
            SettingValue::Int64(v) => w.write_ch_string_utf8(&v.to_string()).map(|_| ()),
            SettingValue::Float64(v) => w.write_ch_string_utf8(&v.to_string()).map(|_| ()),
            SettingValue::Bool(v) => w.write_ch_string_utf8(if *v { "1" } else { "0" }).map(|_| ()),
        }
    }
}

/// Query settings collection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Settings {
    /// Settings as key-value pairs.
    pub values: HashMap<String, SettingValue>,
    /// Important flag (affects how settings are applied).
    pub important: bool,
}

impl Settings {
    /// Create new empty settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a string setting.
    pub fn set_string(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.values.insert(key.into(), SettingValue::String(value.into()));
    }

    /// Set a u64 setting.
    pub fn set_u64(&mut self, key: impl Into<String>, value: u64) {
        self.values.insert(key.into(), SettingValue::UInt64(value));
    }

    /// Set a bool setting.
    pub fn set_bool(&mut self, key: impl Into<String>, value: bool) {
        self.values.insert(key.into(), SettingValue::Bool(value));
    }

    /// Parse settings from a synchronous stream.
    ///
    /// Settings are encoded as a sequence of (name, value) string pairs,
    /// terminated by an empty string name.
    pub fn parse_sync<S>(stream: &S, _protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let mut settings = Self::new();

        loop {
            let name = stream.read_ch_string_utf8_sync()?;
            if name.is_empty() {
                break;
            }

            let value = stream.read_ch_string_utf8_sync()?;
            settings.values.insert(name, SettingValue::String(value));
        }

        Ok(settings)
    }

    /// Parse settings asynchronously.
    pub async fn parse<S>(stream: &S, _protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let mut settings = Self::new();

        loop {
            let name = stream.read_ch_string_utf8().await?;
            if name.is_empty() {
                break;
            }

            let value = stream.read_ch_string_utf8().await?;
            settings.values.insert(name, SettingValue::String(value));
        }

        Ok(settings)
    }

    /// Encode settings to a writer.
    ///
    /// Each setting is encoded as (name, value) string pairs.
    /// The sequence is terminated by an empty string name.
    pub fn encode<W: Write>(&self, w: &mut W, _protocol_version: u64) -> io::Result<()> {
        for (name, value) in &self.values {
            w.write_ch_string_utf8(name)?;
            value.encode(w)?;
        }

        // Terminate with empty string
        w.write_ch_string(b"")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DBMS_TCP_PROTOCOL_VERSION;
    use wire_stream::SliceStream;

    #[test]
    fn test_empty_settings_roundtrip() {
        let settings = Settings::new();

        let mut buf = Vec::new();
        settings.encode(&mut buf, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        // Should just be an empty string (length 0)
        assert_eq!(buf, vec![0x00]);

        let stream = SliceStream::new(&buf);
        let decoded = Settings::parse_sync(&stream, DBMS_TCP_PROTOCOL_VERSION).unwrap();
        assert!(decoded.values.is_empty());
    }

    #[test]
    fn test_settings_with_values() {
        let mut settings = Settings::new();
        settings.set_string("max_threads", "4");
        settings.set_bool("optimize_read_in_order", true);

        let mut buf = Vec::new();
        settings.encode(&mut buf, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        let stream = SliceStream::new(&buf);
        let decoded = Settings::parse_sync(&stream, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        assert_eq!(decoded.values.len(), 2);
    }
}
