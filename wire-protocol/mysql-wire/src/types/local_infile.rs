//! MySQL LOCAL INFILE handling.
//!
//! When the server sends a LOCAL_INFILE_REQUEST packet (0xFB header),
//! the client should respond with the file contents followed by an empty packet.

use crate::error::packet_types;
use crate::mysql_ext::MysqlReadSync;
use crate::parse::{MysqlParse, MysqlParseError, MysqlParseSync};
use crate::write::write_u24_le;
use std::path::Path;
use wire_stream::{WireRead, WireReadSync};

/// Local infile request from server.
///
/// Sent when the server wants the client to send a local file's contents.
#[derive(Clone, Debug)]
pub struct LocalInfileRequest {
    /// The filename requested by the server.
    pub filename: String,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum LocalInfileError {
    #[error("invalid local infile header: expected 0xFB, got {0:#04x}")]
    InvalidHeader(u8),
    #[error("invalid filename encoding")]
    InvalidFilename,
    #[error("file not found: {0}")]
    FileNotFound(String),
    #[error("file read error: {0}")]
    ReadError(String),
    #[error("file access denied: {0}")]
    AccessDenied(String),
}

impl LocalInfileRequest {
    /// Create a new local infile request.
    pub fn new(filename: impl Into<String>) -> Self {
        Self { filename: filename.into() }
    }

    /// Parse a local infile request from a stream.
    pub fn parse_from_sync<S: WireReadSync + ?Sized>(stream: &S) -> Result<Self, MysqlParseError<S::ReadError, LocalInfileError>> {
        // Check header byte
        let header = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
        if header != packet_types::LOCAL_INFILE {
            return Err(MysqlParseError::Parse(LocalInfileError::InvalidHeader(header)));
        }

        // Read filename (rest of packet)
        let mut filename_bytes = Vec::new();
        while let Ok(b) = stream.read_u8_sync() {
            filename_bytes.push(b);
        }

        let filename = String::from_utf8(filename_bytes).map_err(|_| MysqlParseError::Parse(LocalInfileError::InvalidFilename))?;

        Ok(Self { filename })
    }

    /// Check if the filename is safe to access.
    ///
    /// This performs basic security checks to prevent path traversal attacks.
    pub fn is_safe_path(&self) -> bool {
        let path = Path::new(&self.filename);

        // Reject absolute paths
        if path.is_absolute() {
            return false;
        }

        // Reject paths with parent directory references
        for component in path.components() {
            if let std::path::Component::ParentDir = component {
                return false;
            }
        }

        // Reject paths starting with certain prefixes
        let filename_lower = self.filename.to_lowercase();
        if filename_lower.starts_with("/etc/")
            || filename_lower.starts_with("/proc/")
            || filename_lower.starts_with("/sys/")
            || filename_lower.contains("../")
            || filename_lower.contains("..\\")
        {
            return false;
        }

        true
    }
}

impl<S: WireReadSync + ?Sized> MysqlParseSync<S> for LocalInfileRequest {
    type ParseError = LocalInfileError;
    type Value<'s>
        = LocalInfileRequest
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_from_sync(stream)
    }
}

impl<S: WireRead + ?Sized> MysqlParse<S> for LocalInfileRequest {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_sync(stream)
    }
}

/// Local infile response builder.
///
/// Builds the response packets for a LOCAL INFILE request.
#[derive(Debug)]
pub struct LocalInfileResponse {
    /// File data chunks.
    chunks: Vec<Vec<u8>>,
    /// Current sequence ID.
    sequence_id: u8,
}

impl LocalInfileResponse {
    /// Maximum chunk size (slightly less than max packet size for safety).
    pub const MAX_CHUNK_SIZE: usize = 16 * 1024 * 1024 - 1024;

    /// Create a new local infile response.
    pub fn new(sequence_id: u8) -> Self {
        Self { chunks: Vec::new(), sequence_id }
    }

    /// Add file data to the response.
    ///
    /// Data will be split into appropriately sized chunks.
    pub fn add_data(&mut self, data: &[u8]) {
        for chunk in data.chunks(Self::MAX_CHUNK_SIZE) {
            self.chunks.push(chunk.to_vec());
        }
    }

    /// Build the response packets.
    ///
    /// Returns a vector of packets: data chunks followed by an empty packet.
    pub fn build_packets(self) -> Vec<Vec<u8>> {
        let mut packets = Vec::new();
        let mut seq = self.sequence_id;

        // Data packets
        for chunk in self.chunks {
            let mut packet = Vec::with_capacity(4 + chunk.len());
            write_u24_le(&mut packet, chunk.len() as u32).expect("write to Vec is infallible");
            packet.push(seq);
            packet.extend_from_slice(&chunk);
            packets.push(packet);
            seq = seq.wrapping_add(1);
        }

        // Empty packet to signal end
        let mut empty_packet = Vec::with_capacity(4);
        write_u24_le(&mut empty_packet, 0).expect("write to Vec is infallible");
        empty_packet.push(seq);
        packets.push(empty_packet);

        packets
    }

    /// Create a response from file contents.
    pub fn from_bytes(data: &[u8], sequence_id: u8) -> Self {
        let mut response = Self::new(sequence_id);
        response.add_data(data);
        response
    }

    /// Create an empty response (file not found or access denied).
    pub fn empty(sequence_id: u8) -> Vec<Vec<u8>> {
        let response = Self::new(sequence_id);
        response.build_packets()
    }
}

/// Handler trait for LOCAL INFILE requests.
///
/// Implement this trait to handle LOCAL INFILE requests in your application.
pub trait LocalInfileHandler {
    /// Error type for file operations.
    type Error: std::error::Error;

    /// Check if the file should be allowed.
    ///
    /// Return `false` to deny access to the file.
    fn allow_file(&self, filename: &str) -> bool;

    /// Read the file contents.
    ///
    /// Return `None` if the file doesn't exist or can't be read.
    fn read_file(&self, filename: &str) -> Result<Option<Vec<u8>>, Self::Error>;
}

/// A simple file handler that allows files from a whitelist of directories.
#[derive(Debug, Clone)]
pub struct WhitelistHandler {
    /// Allowed directory prefixes.
    allowed_dirs: Vec<String>,
}

impl WhitelistHandler {
    /// Create a new whitelist handler.
    pub fn new() -> Self {
        Self { allowed_dirs: Vec::new() }
    }

    /// Add an allowed directory.
    pub fn allow_dir(mut self, dir: impl Into<String>) -> Self {
        self.allowed_dirs.push(dir.into());
        self
    }
}

impl Default for WhitelistHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalInfileHandler for WhitelistHandler {
    type Error = std::io::Error;

    fn allow_file(&self, filename: &str) -> bool {
        // Reject any path traversal attempts
        if filename.contains("..") {
            return false;
        }

        // Check against whitelist
        for dir in &self.allowed_dirs {
            if filename.starts_with(dir) {
                return true;
            }
        }

        false
    }

    fn read_file(&self, filename: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        if !self.allow_file(filename) {
            return Ok(None);
        }

        match std::fs::read(filename) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// A handler that denies all LOCAL INFILE requests.
#[derive(Debug, Clone, Copy, Default)]
pub struct DenyAllHandler;

impl LocalInfileHandler for DenyAllHandler {
    type Error = std::convert::Infallible;

    fn allow_file(&self, _filename: &str) -> bool {
        false
    }

    fn read_file(&self, _filename: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_local_infile_request() {
        let mut data = Vec::new();
        data.push(0xFB); // LOCAL_INFILE marker
        data.extend_from_slice(b"/tmp/data.csv");

        let stream = SliceStream::new(&data);
        let request = LocalInfileRequest::parse_sync(&stream).unwrap();

        assert_eq!(request.filename, "/tmp/data.csv");
    }

    #[test]
    fn test_safe_path() {
        assert!(LocalInfileRequest::new("data.csv").is_safe_path());
        assert!(LocalInfileRequest::new("subdir/data.csv").is_safe_path());

        assert!(!LocalInfileRequest::new("/etc/passwd").is_safe_path());
        assert!(!LocalInfileRequest::new("../secret.txt").is_safe_path());
        assert!(!LocalInfileRequest::new("data/../../../etc/passwd").is_safe_path());
    }

    #[test]
    fn test_response_builder() {
        let data = b"line1\nline2\nline3\n";
        let response = LocalInfileResponse::from_bytes(data, 5);
        let packets = response.build_packets();

        // Should have data packet + empty packet
        assert_eq!(packets.len(), 2);

        // Check sequence IDs
        assert_eq!(packets[0][3], 5);
        assert_eq!(packets[1][3], 6);

        // Empty packet should have 0 length
        assert_eq!(packets[1].len(), 4);
        assert_eq!(packets[1][0], 0);
        assert_eq!(packets[1][1], 0);
        assert_eq!(packets[1][2], 0);
    }

    #[test]
    fn test_deny_all_handler() {
        let handler = DenyAllHandler;
        assert!(!handler.allow_file("any_file.txt"));
        assert_eq!(handler.read_file("any_file.txt").unwrap(), None);
    }

    #[test]
    fn test_whitelist_handler() {
        let handler = WhitelistHandler::new().allow_dir("/tmp/uploads/").allow_dir("/var/data/");

        assert!(handler.allow_file("/tmp/uploads/data.csv"));
        assert!(handler.allow_file("/var/data/export.txt"));
        assert!(!handler.allow_file("/etc/passwd"));
        assert!(!handler.allow_file("/tmp/uploads/../../../etc/passwd"));
    }
}
