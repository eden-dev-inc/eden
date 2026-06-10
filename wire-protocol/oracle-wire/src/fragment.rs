//! TNS packet fragmentation and reassembly.
//!
//! Oracle TNS supports fragmenting large payloads across multiple Data packets.
//! The `MORE_DATA` flag indicates that additional fragments follow, while
//! `EOF` or the absence of `MORE_DATA` indicates the final fragment.
//!
//! # Reassembly
//!
//! The [`FragmentReassembler`] collects fragments and combines them into a
//! complete payload once all fragments are received.
//!
//! # Fragmentation
//!
//! The [`fragment`] function splits a large payload into multiple fragments,
//! each sized according to the negotiated SDU (Session Data Unit) size.
//!
//! # Example
//!
//! ```rust
//! use oracle_wire::fragment::{FragmentReassembler, fragment, FragmentConfig};
//! use oracle_wire::types::data::{Data, DataFlags};
//!
//! // Reassemble fragments
//! let mut reassembler = FragmentReassembler::new();
//!
//! // Simulate receiving fragmented data
//! let fragment1 = Data::with_flags(
//!     vec![1, 2, 3],
//!     DataFlags::from_raw(0x0020), // MORE_DATA
//! );
//! let fragment2 = Data::with_flags(
//!     vec![4, 5, 6],
//!     DataFlags::from_raw(0x0040), // EOF
//! );
//!
//! reassembler.add_fragment(fragment1);
//! assert!(!reassembler.is_complete());
//!
//! reassembler.add_fragment(fragment2);
//! assert!(reassembler.is_complete());
//!
//! let complete = reassembler.take_payload().unwrap();
//! assert_eq!(complete, vec![1, 2, 3, 4, 5, 6]);
//!
//! // Fragment a large payload
//! let large_payload = vec![0u8; 10000];
//! let config = FragmentConfig::new(8192); // 8KB SDU
//! let fragments = fragment(&large_payload, &config);
//! assert_eq!(fragments.len(), 2);
//! ```

use crate::error::data_flags;
use crate::types::data::{Data, DataFlags};
use crate::types::packet::{HEADER_SIZE, MAX_PACKET_SIZE, PacketType, TnsHeader};

/// Configuration for packet fragmentation.
#[derive(Clone, Copy, Debug)]
pub struct FragmentConfig {
    /// Maximum payload size per fragment (excluding headers).
    pub max_payload_size: usize,
    /// Whether to set EOF flag on final fragment.
    pub use_eof_flag: bool,
}

impl FragmentConfig {
    /// Create a new fragment config with the given SDU size.
    ///
    /// The SDU (Session Data Unit) is the maximum packet size including headers.
    /// The max payload size is calculated by subtracting headers and data flags.
    pub fn new(sdu_size: u16) -> Self {
        // SDU includes: TNS header (8) + data flags (2) + payload
        let max_payload = (sdu_size as usize).saturating_sub(HEADER_SIZE + 2);
        Self { max_payload_size: max_payload, use_eof_flag: true }
    }

    /// Create with explicit max payload size.
    pub fn with_max_payload(max_payload_size: usize) -> Self {
        Self { max_payload_size, use_eof_flag: true }
    }

    /// Set whether to use EOF flag on final fragment.
    pub fn use_eof(mut self, use_eof: bool) -> Self {
        self.use_eof_flag = use_eof;
        self
    }

    /// Get the default config for a standard SDU (8192 bytes).
    pub fn default_sdu() -> Self {
        Self::new(8192)
    }

    /// Get the maximum SDU config (32767 bytes).
    pub fn max_sdu() -> Self {
        Self::new(MAX_PACKET_SIZE as u16)
    }
}

impl Default for FragmentConfig {
    fn default() -> Self {
        Self::default_sdu()
    }
}

/// Fragment a payload into multiple Data packets.
///
/// Returns a vector of Data packets with appropriate flags set:
/// - All non-final fragments have `MORE_DATA` flag set
/// - Final fragment has `EOF` flag set (if configured)
///
/// If the payload fits in a single packet, returns a single-element vector.
pub fn fragment(payload: &[u8], config: &FragmentConfig) -> Vec<Data> {
    if payload.is_empty() {
        return vec![Data::new(Vec::new())];
    }

    if payload.len() <= config.max_payload_size {
        // Fits in one packet
        let flags = if config.use_eof_flag {
            DataFlags::from_raw(data_flags::EOF)
        } else {
            DataFlags::default()
        };
        return vec![Data::with_flags(payload.to_vec(), flags)];
    }

    let mut fragments = Vec::new();
    let mut offset = 0;

    while offset < payload.len() {
        let remaining = payload.len() - offset;
        let chunk_size = remaining.min(config.max_payload_size);
        let is_last = offset + chunk_size >= payload.len();

        let flags = if is_last {
            if config.use_eof_flag {
                DataFlags::from_raw(data_flags::EOF)
            } else {
                DataFlags::default()
            }
        } else {
            DataFlags::from_raw(data_flags::MORE_DATA)
        };

        let chunk = payload[offset..offset + chunk_size].to_vec();
        fragments.push(Data::with_flags(chunk, flags));

        offset += chunk_size;
    }

    fragments
}

/// Fragment a payload and return complete TNS packets (with headers).
pub fn fragment_to_packets(payload: &[u8], config: &FragmentConfig) -> Vec<Vec<u8>> {
    fragment(payload, config)
        .into_iter()
        .map(|data| {
            let data_len = 2 + data.payload.len(); // flags + payload
            let header = TnsHeader::new(PacketType::Data, data_len as u16);

            let mut packet = Vec::with_capacity(HEADER_SIZE + data_len);
            packet.extend_from_slice(&header.to_bytes());
            packet.extend_from_slice(&data.flags.raw().to_be_bytes());
            packet.extend_from_slice(&data.payload);
            packet
        })
        .collect()
}

/// Reassembler for fragmented TNS data packets.
#[derive(Clone, Debug, Default)]
pub struct FragmentReassembler {
    /// Accumulated payload from fragments.
    payload: Vec<u8>,
    /// Whether reassembly is complete.
    complete: bool,
    /// Number of fragments received.
    fragment_count: usize,
    /// Expected total size (if known from first fragment).
    expected_size: Option<usize>,
}

impl FragmentReassembler {
    /// Create a new reassembler.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new reassembler with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            payload: Vec::with_capacity(capacity),
            complete: false,
            fragment_count: 0,
            expected_size: None,
        }
    }

    /// Create a new reassembler with expected size hint.
    pub fn with_expected_size(size: usize) -> Self {
        Self {
            payload: Vec::with_capacity(size),
            complete: false,
            fragment_count: 0,
            expected_size: Some(size),
        }
    }

    /// Add a fragment to the reassembly buffer.
    ///
    /// Returns `true` if this was the final fragment.
    pub fn add_fragment(&mut self, data: Data) -> bool {
        self.payload.extend_from_slice(&data.payload);
        self.fragment_count += 1;

        // Check if this is the final fragment
        if data.is_final() {
            self.complete = true;
        }

        self.complete
    }

    /// Add a fragment from raw bytes (payload only, without flags).
    pub fn add_fragment_raw(&mut self, payload: &[u8], is_final: bool) {
        self.payload.extend_from_slice(payload);
        self.fragment_count += 1;

        if is_final {
            self.complete = true;
        }
    }

    /// Check if reassembly is complete.
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// Get the number of fragments received.
    pub fn fragment_count(&self) -> usize {
        self.fragment_count
    }

    /// Get the current accumulated size.
    pub fn current_size(&self) -> usize {
        self.payload.len()
    }

    /// Get the expected total size (if known).
    pub fn expected_size(&self) -> Option<usize> {
        self.expected_size
    }

    /// Get a reference to the accumulated payload.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Take the completed payload, resetting the reassembler.
    ///
    /// Returns `None` if reassembly is not complete.
    pub fn take_payload(&mut self) -> Option<Vec<u8>> {
        if self.complete {
            self.complete = false;
            self.fragment_count = 0;
            Some(std::mem::take(&mut self.payload))
        } else {
            None
        }
    }

    /// Take the payload regardless of completion state.
    ///
    /// This can be used to handle partial data or errors.
    pub fn take_partial(&mut self) -> Vec<u8> {
        self.complete = false;
        self.fragment_count = 0;
        std::mem::take(&mut self.payload)
    }

    /// Reset the reassembler, discarding any accumulated data.
    pub fn reset(&mut self) {
        self.payload.clear();
        self.complete = false;
        self.fragment_count = 0;
        self.expected_size = None;
    }

    /// Check if this appears to be a fragmented stream.
    ///
    /// Returns `true` if more than one fragment has been received.
    pub fn is_fragmented(&self) -> bool {
        self.fragment_count > 1
    }
}

/// Result of processing a Data packet.
#[derive(Clone, Debug)]
pub enum FragmentResult {
    /// The packet was complete (single fragment or final fragment).
    Complete(Vec<u8>),
    /// More fragments are expected.
    Incomplete {
        /// Current accumulated size.
        accumulated: usize,
        /// Number of fragments received so far.
        fragments: usize,
    },
}

impl FragmentResult {
    /// Check if the result is complete.
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete(_))
    }

    /// Get the payload if complete.
    pub fn into_payload(self) -> Option<Vec<u8>> {
        match self {
            Self::Complete(payload) => Some(payload),
            Self::Incomplete { .. } => None,
        }
    }
}

/// Process a single Data packet through the reassembler.
///
/// This is a convenience function for processing packets one at a time.
pub fn process_fragment(reassembler: &mut FragmentReassembler, data: Data) -> FragmentResult {
    reassembler.add_fragment(data);

    if reassembler.is_complete() {
        FragmentResult::Complete(reassembler.take_payload().unwrap_or_default())
    } else {
        FragmentResult::Incomplete {
            accumulated: reassembler.current_size(),
            fragments: reassembler.fragment_count(),
        }
    }
}

/// Stream processor for handling fragmented data with a size limit.
#[derive(Clone, Debug)]
pub struct FragmentStream {
    reassembler: FragmentReassembler,
    max_total_size: usize,
}

impl FragmentStream {
    /// Create a new fragment stream with default max size (16MB).
    pub fn new() -> Self {
        Self {
            reassembler: FragmentReassembler::new(),
            max_total_size: 16 * 1024 * 1024,
        }
    }

    /// Create with a custom max total size.
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            reassembler: FragmentReassembler::new(),
            max_total_size: max_size,
        }
    }

    /// Process a Data packet.
    ///
    /// Returns an error if the accumulated size exceeds the limit.
    pub fn process(&mut self, data: Data) -> Result<FragmentResult, FragmentError> {
        let new_size = self.reassembler.current_size() + data.payload.len();
        if new_size > self.max_total_size {
            return Err(FragmentError::SizeExceeded { accumulated: new_size, limit: self.max_total_size });
        }

        Ok(process_fragment(&mut self.reassembler, data))
    }

    /// Get a reference to the underlying reassembler.
    pub fn reassembler(&self) -> &FragmentReassembler {
        &self.reassembler
    }

    /// Reset the stream.
    pub fn reset(&mut self) {
        self.reassembler.reset();
    }
}

impl Default for FragmentStream {
    fn default() -> Self {
        Self::new()
    }
}

/// Error during fragment processing.
#[derive(Clone, Debug, thiserror::Error)]
pub enum FragmentError {
    #[error("fragment size exceeded limit: {accumulated} bytes (limit: {limit})")]
    SizeExceeded { accumulated: usize, limit: usize },

    #[error("unexpected end of fragment stream")]
    UnexpectedEnd,

    #[error("invalid fragment sequence")]
    InvalidSequence,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fragment_config_sdu() {
        let config = FragmentConfig::new(8192);
        // 8192 - 8 (header) - 2 (flags) = 8182
        assert_eq!(config.max_payload_size, 8182);
    }

    #[test]
    fn test_fragment_config_max_sdu() {
        let config = FragmentConfig::max_sdu();
        // 32767 - 8 - 2 = 32757
        assert_eq!(config.max_payload_size, 32757);
    }

    #[test]
    fn test_fragment_small_payload() {
        let payload = vec![1, 2, 3, 4, 5];
        let config = FragmentConfig::with_max_payload(1000);

        let fragments = fragment(&payload, &config);
        assert_eq!(fragments.len(), 1);
        assert_eq!(fragments[0].payload, payload);
        assert!(fragments[0].flags.eof());
    }

    #[test]
    fn test_fragment_large_payload() {
        let payload = vec![0u8; 1000];
        let config = FragmentConfig::with_max_payload(300);

        let fragments = fragment(&payload, &config);
        assert_eq!(fragments.len(), 4); // 300 + 300 + 300 + 100

        // First 3 fragments should have MORE_DATA
        assert!(fragments[0].flags.more_data());
        assert!(!fragments[0].flags.eof());
        assert!(fragments[1].flags.more_data());
        assert!(fragments[2].flags.more_data());

        // Last fragment should have EOF
        assert!(!fragments[3].flags.more_data());
        assert!(fragments[3].flags.eof());

        // Total size should match
        let total: usize = fragments.iter().map(|f| f.payload.len()).sum();
        assert_eq!(total, 1000);
    }

    #[test]
    fn test_fragment_empty() {
        let payload: Vec<u8> = vec![];
        let config = FragmentConfig::default();

        let fragments = fragment(&payload, &config);
        assert_eq!(fragments.len(), 1);
        assert!(fragments[0].payload.is_empty());
    }

    #[test]
    fn test_fragment_to_packets() {
        let payload = vec![0u8; 500];
        let config = FragmentConfig::with_max_payload(200);

        let packets = fragment_to_packets(&payload, &config);
        assert_eq!(packets.len(), 3);

        // Each packet should have valid header
        for packet in &packets {
            assert!(packet.len() >= HEADER_SIZE);
            // Packet type should be Data (6)
            assert_eq!(packet[4], 6);
        }
    }

    #[test]
    fn test_reassembler_single() {
        let mut reassembler = FragmentReassembler::new();

        let data = Data::with_flags(vec![1, 2, 3], DataFlags::from_raw(data_flags::EOF));

        let is_complete = reassembler.add_fragment(data);
        assert!(is_complete);
        assert!(reassembler.is_complete());
        assert!(!reassembler.is_fragmented());

        let payload = reassembler.take_payload().unwrap();
        assert_eq!(payload, vec![1, 2, 3]);
    }

    #[test]
    fn test_reassembler_multiple() {
        let mut reassembler = FragmentReassembler::new();

        // First fragment
        let data1 = Data::with_flags(vec![1, 2, 3], DataFlags::from_raw(data_flags::MORE_DATA));
        assert!(!reassembler.add_fragment(data1));
        assert!(!reassembler.is_complete());

        // Second fragment
        let data2 = Data::with_flags(vec![4, 5, 6], DataFlags::from_raw(data_flags::MORE_DATA));
        assert!(!reassembler.add_fragment(data2));
        assert!(!reassembler.is_complete());

        // Final fragment
        let data3 = Data::with_flags(vec![7, 8, 9], DataFlags::from_raw(data_flags::EOF));
        assert!(reassembler.add_fragment(data3));
        assert!(reassembler.is_complete());
        assert!(reassembler.is_fragmented());

        let payload = reassembler.take_payload().unwrap();
        assert_eq!(payload, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(reassembler.fragment_count(), 0); // Reset after take
    }

    #[test]
    fn test_reassembler_reset() {
        let mut reassembler = FragmentReassembler::new();

        reassembler.add_fragment(Data::new(vec![1, 2, 3]));
        assert_eq!(reassembler.current_size(), 3);

        reassembler.reset();
        assert_eq!(reassembler.current_size(), 0);
        assert_eq!(reassembler.fragment_count(), 0);
        assert!(!reassembler.is_complete());
    }

    #[test]
    fn test_reassembler_take_partial() {
        let mut reassembler = FragmentReassembler::new();

        // Add incomplete data
        reassembler.add_fragment(Data::with_flags(vec![1, 2, 3], DataFlags::from_raw(data_flags::MORE_DATA)));

        // take_payload returns None for incomplete
        assert!(reassembler.take_payload().is_none());

        // take_partial works regardless
        let partial = reassembler.take_partial();
        assert_eq!(partial, vec![1, 2, 3]);
    }

    #[test]
    fn test_process_fragment() {
        let mut reassembler = FragmentReassembler::new();

        let result = process_fragment(&mut reassembler, Data::with_flags(vec![1, 2], DataFlags::from_raw(data_flags::MORE_DATA)));

        match result {
            FragmentResult::Incomplete { accumulated, fragments } => {
                assert_eq!(accumulated, 2);
                assert_eq!(fragments, 1);
            }
            _ => panic!("Expected Incomplete"),
        }

        let result = process_fragment(&mut reassembler, Data::with_flags(vec![3, 4], DataFlags::from_raw(data_flags::EOF)));

        match result {
            FragmentResult::Complete(payload) => {
                assert_eq!(payload, vec![1, 2, 3, 4]);
            }
            _ => panic!("Expected Complete"),
        }
    }

    #[test]
    fn test_fragment_stream_limit() {
        let mut stream = FragmentStream::with_max_size(100);

        // First fragment OK
        let result = stream.process(Data::with_flags(vec![0u8; 50], DataFlags::from_raw(data_flags::MORE_DATA)));
        assert!(result.is_ok());

        // Second fragment exceeds limit
        let result = stream.process(Data::with_flags(vec![0u8; 60], DataFlags::from_raw(data_flags::EOF)));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), FragmentError::SizeExceeded { .. }));
    }

    #[test]
    fn test_roundtrip() {
        // Create a large payload
        let original: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        // Fragment it
        let config = FragmentConfig::with_max_payload(1024);
        let fragments = fragment(&original, &config);

        // Reassemble
        let mut reassembler = FragmentReassembler::new();
        for frag in fragments {
            reassembler.add_fragment(frag);
        }

        assert!(reassembler.is_complete());
        let reassembled = reassembler.take_payload().unwrap();
        assert_eq!(reassembled, original);
    }
}
