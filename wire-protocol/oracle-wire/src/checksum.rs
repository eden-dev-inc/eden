//! TNS packet checksum calculation and validation.
//!
//! Oracle TNS uses a simple 16-bit additive checksum for packet integrity.
//! The checksum is calculated as the ones' complement of the 16-bit sum of
//! all 16-bit words in the data.
//!
//! # Checksum Fields
//!
//! The TNS header contains two checksum fields:
//! - **Packet checksum** (bytes 2-3): Covers the packet data (after header)
//! - **Header checksum** (bytes 6-7): Covers the header itself (bytes 0-5)
//!
//! # Usage
//!
//! Note: Many modern Oracle implementations set checksums to 0, relying on
//! TCP/IP layer checksums for integrity. These functions are primarily useful
//! for:
//! - Protocol compliance verification
//! - Debugging packet corruption issues
//! - Environments with strict TNS configuration
//!
//! # Example
//!
//! ```rust
//! use oracle_wire::checksum::{compute_checksum, validate_header_checksum};
//! use oracle_wire::types::packet::TnsHeader;
//!
//! // Compute checksum for data
//! let data = b"Hello, Oracle!";
//! let checksum = compute_checksum(data);
//!
//! // Validate a packet
//! let header = TnsHeader::new(oracle_wire::types::packet::PacketType::Data, 14);
//! // Most packets have 0 checksums (disabled)
//! assert!(validate_header_checksum(&header));
//! ```

use crate::types::packet::{HEADER_SIZE, TnsHeader};

/// Compute the TNS checksum for a byte slice.
///
/// Uses the ones' complement 16-bit additive checksum algorithm.
/// This is the same algorithm used by IP/TCP checksums.
///
/// # Algorithm
///
/// 1. Sum all 16-bit words (handling odd-length data by padding)
/// 2. Add any overflow (carry) back into the sum
/// 3. Take the ones' complement of the result
///
/// # Returns
///
/// Returns 0 if the checksum is disabled (all zeros input or empty),
/// otherwise returns the calculated checksum.
pub fn compute_checksum(data: &[u8]) -> u16 {
    if data.is_empty() {
        return 0;
    }

    let mut sum: u32 = 0;

    // Process 16-bit words
    let mut i = 0;
    while i + 1 < data.len() {
        let word = u16::from_be_bytes([data[i], data[i + 1]]);
        sum = sum.wrapping_add(word as u32);
        i += 2;
    }

    // Handle odd byte
    if i < data.len() {
        let word = u16::from_be_bytes([data[i], 0]);
        sum = sum.wrapping_add(word as u32);
    }

    // Fold 32-bit sum to 16 bits (add carries)
    while sum >> 16 != 0 {
        sum = (sum & 0xFFFF) + (sum >> 16);
    }

    // Ones' complement
    !sum as u16
}

/// Verify a checksum against data.
///
/// # Returns
///
/// Returns `true` if:
/// - The expected checksum is 0 (checksums disabled)
/// - The computed checksum matches the expected value
pub fn verify_checksum(data: &[u8], expected: u16) -> bool {
    // Checksum of 0 means disabled
    if expected == 0 {
        return true;
    }

    let computed = compute_checksum(data);
    computed == expected
}

/// Compute the header checksum for a TNS header.
///
/// The header checksum covers bytes 0-5 of the header (length, packet checksum,
/// type, and flags). Bytes 6-7 are the checksum field itself.
pub fn compute_header_checksum(header: &TnsHeader) -> u16 {
    let bytes = header.to_bytes();
    // Header checksum covers bytes 0-5
    compute_checksum(&bytes[0..6])
}

/// Validate the header checksum.
///
/// # Returns
///
/// Returns `true` if the header checksum is valid or disabled (0).
pub fn validate_header_checksum(header: &TnsHeader) -> bool {
    if header.header_checksum == 0 {
        return true;
    }

    let computed = compute_header_checksum(header);
    computed == header.header_checksum
}

/// Validate the packet data checksum.
///
/// # Returns
///
/// Returns `true` if the packet checksum is valid or disabled (0).
pub fn validate_packet_checksum(header: &TnsHeader, data: &[u8]) -> bool {
    if header.packet_checksum == 0 {
        return true;
    }

    let computed = compute_checksum(data);
    computed == header.packet_checksum
}

/// Validate both header and packet checksums.
///
/// # Returns
///
/// Returns `true` if both checksums are valid or disabled.
pub fn validate_packet(header: &TnsHeader, data: &[u8]) -> bool {
    validate_header_checksum(header) && validate_packet_checksum(header, data)
}

/// Create a header with computed checksums.
///
/// This is useful when constructing packets that require valid checksums.
pub fn header_with_checksums(header: &TnsHeader, data: &[u8], enable_header_checksum: bool, enable_packet_checksum: bool) -> TnsHeader {
    let mut new_header = *header;

    if enable_packet_checksum && !data.is_empty() {
        new_header.packet_checksum = compute_checksum(data);
    }

    if enable_header_checksum {
        // First set header checksum to 0 for computation
        new_header.header_checksum = 0;
        new_header.header_checksum = compute_header_checksum(&new_header);
    }

    new_header
}

/// Result of checksum validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChecksumResult {
    /// Whether the header checksum is valid.
    pub header_valid: bool,
    /// Whether the packet checksum is valid.
    pub packet_valid: bool,
    /// Whether the header checksum was enabled (non-zero).
    pub header_enabled: bool,
    /// Whether the packet checksum was enabled (non-zero).
    pub packet_enabled: bool,
    /// The computed header checksum (if enabled).
    pub computed_header: Option<u16>,
    /// The computed packet checksum (if enabled).
    pub computed_packet: Option<u16>,
}

impl ChecksumResult {
    /// Check if all enabled checksums are valid.
    pub fn is_valid(&self) -> bool {
        self.header_valid && self.packet_valid
    }

    /// Check if any checksums are enabled.
    pub fn any_enabled(&self) -> bool {
        self.header_enabled || self.packet_enabled
    }
}

/// Perform detailed checksum validation.
///
/// Returns detailed information about checksum validity.
pub fn validate_packet_detailed(header: &TnsHeader, data: &[u8]) -> ChecksumResult {
    let header_enabled = header.header_checksum != 0;
    let packet_enabled = header.packet_checksum != 0;

    let computed_header = if header_enabled {
        Some(compute_header_checksum(header))
    } else {
        None
    };

    let computed_packet = if packet_enabled { Some(compute_checksum(data)) } else { None };

    let header_valid = !header_enabled || computed_header == Some(header.header_checksum);
    let packet_valid = !packet_enabled || computed_packet == Some(header.packet_checksum);

    ChecksumResult {
        header_valid,
        packet_valid,
        header_enabled,
        packet_enabled,
        computed_header,
        computed_packet,
    }
}

/// Builder for creating packets with checksums.
#[derive(Clone, Debug)]
pub struct ChecksumBuilder {
    packet_type: crate::types::packet::PacketType,
    flags: u8,
    data: Vec<u8>,
    enable_header_checksum: bool,
    enable_packet_checksum: bool,
}

impl ChecksumBuilder {
    /// Create a new builder.
    pub fn new(packet_type: crate::types::packet::PacketType) -> Self {
        Self {
            packet_type,
            flags: 0,
            data: Vec::new(),
            enable_header_checksum: false,
            enable_packet_checksum: false,
        }
    }

    /// Set the flags byte.
    pub fn flags(mut self, flags: u8) -> Self {
        self.flags = flags;
        self
    }

    /// Set the packet data.
    pub fn data(mut self, data: Vec<u8>) -> Self {
        self.data = data;
        self
    }

    /// Enable header checksum computation.
    pub fn with_header_checksum(mut self) -> Self {
        self.enable_header_checksum = true;
        self
    }

    /// Enable packet data checksum computation.
    pub fn with_packet_checksum(mut self) -> Self {
        self.enable_packet_checksum = true;
        self
    }

    /// Enable both checksums.
    pub fn with_checksums(mut self) -> Self {
        self.enable_header_checksum = true;
        self.enable_packet_checksum = true;
        self
    }

    /// Build the complete packet with header.
    pub fn build(self) -> Vec<u8> {
        let header = TnsHeader {
            packet_length: (HEADER_SIZE + self.data.len()) as u16,
            packet_checksum: 0,
            packet_type: self.packet_type,
            flags: self.flags,
            header_checksum: 0,
        };

        let header = header_with_checksums(&header, &self.data, self.enable_header_checksum, self.enable_packet_checksum);

        let mut packet = Vec::with_capacity(HEADER_SIZE + self.data.len());
        packet.extend_from_slice(&header.to_bytes());
        packet.extend_from_slice(&self.data);
        packet
    }

    /// Build and return both header and data separately.
    pub fn build_parts(self) -> (TnsHeader, Vec<u8>) {
        let header = TnsHeader {
            packet_length: (HEADER_SIZE + self.data.len()) as u16,
            packet_checksum: 0,
            packet_type: self.packet_type,
            flags: self.flags,
            header_checksum: 0,
        };

        let header = header_with_checksums(&header, &self.data, self.enable_header_checksum, self.enable_packet_checksum);

        (header, self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::packet::PacketType;

    #[test]
    fn test_compute_checksum_empty() {
        assert_eq!(compute_checksum(&[]), 0);
    }

    #[test]
    fn test_compute_checksum_simple() {
        // Simple test data
        let data = [0x00, 0x01, 0x00, 0x02];
        let checksum = compute_checksum(&data);
        // Sum: 0x0001 + 0x0002 = 0x0003
        // Ones' complement: 0xFFFC
        assert_eq!(checksum, 0xFFFC);
    }

    #[test]
    fn test_compute_checksum_odd_length() {
        let data = [0x00, 0x01, 0x02];
        let checksum = compute_checksum(&data);
        // Treated as [0x00, 0x01, 0x02, 0x00]
        // Sum: 0x0001 + 0x0200 = 0x0201
        // Ones' complement: 0xFDFE
        assert_eq!(checksum, 0xFDFE);
    }

    #[test]
    fn test_verify_checksum_disabled() {
        // Checksum of 0 means disabled, always valid
        assert!(verify_checksum(&[1, 2, 3], 0));
    }

    #[test]
    fn test_verify_checksum_valid() {
        let data = [0x00, 0x01, 0x00, 0x02];
        let checksum = compute_checksum(&data);
        assert!(verify_checksum(&data, checksum));
    }

    #[test]
    fn test_verify_checksum_invalid() {
        let data = [0x00, 0x01, 0x00, 0x02];
        assert!(!verify_checksum(&data, 0x1234)); // Wrong checksum
    }

    #[test]
    fn test_validate_header_checksum_disabled() {
        let header = TnsHeader::new(PacketType::Data, 10);
        // Default checksum is 0 (disabled)
        assert!(validate_header_checksum(&header));
    }

    #[test]
    fn test_validate_packet_disabled() {
        let header = TnsHeader::new(PacketType::Data, 4);
        let data = [1, 2, 3, 4];
        assert!(validate_packet(&header, &data));
    }

    #[test]
    fn test_header_with_checksums() {
        let header = TnsHeader::new(PacketType::Data, 4);
        let data = [1, 2, 3, 4];

        let header_with = header_with_checksums(&header, &data, true, true);

        assert_ne!(header_with.header_checksum, 0);
        assert_ne!(header_with.packet_checksum, 0);
        assert!(validate_packet(&header_with, &data));
    }

    #[test]
    fn test_checksum_result_detailed() {
        let header = TnsHeader::new(PacketType::Data, 4);
        let data = [1, 2, 3, 4];

        let result = validate_packet_detailed(&header, &data);
        assert!(result.is_valid());
        assert!(!result.any_enabled()); // Checksums disabled by default
        assert!(result.computed_header.is_none());
        assert!(result.computed_packet.is_none());
    }

    #[test]
    fn test_checksum_builder() {
        let packet = ChecksumBuilder::new(PacketType::Data).data(vec![0x01, 0x02, 0x03, 0x04]).with_checksums().build();

        // Header (8 bytes) + data (4 bytes)
        assert_eq!(packet.len(), 12);

        // Parse the header back
        let header = TnsHeader {
            packet_length: u16::from_be_bytes([packet[0], packet[1]]),
            packet_checksum: u16::from_be_bytes([packet[2], packet[3]]),
            packet_type: PacketType::from_u8(packet[4]),
            flags: packet[5],
            header_checksum: u16::from_be_bytes([packet[6], packet[7]]),
        };

        // Validate checksums
        assert!(validate_packet(&header, &packet[8..]));
    }

    #[test]
    fn test_checksum_builder_parts() {
        let (header, data) = ChecksumBuilder::new(PacketType::Data).data(vec![0xAB, 0xCD]).with_header_checksum().build_parts();

        assert!(validate_header_checksum(&header));
        assert_eq!(data, vec![0xAB, 0xCD]);
    }

    #[test]
    fn test_checksum_known_value() {
        // Test with all 0xFF bytes - edge case
        let data = [0xFF, 0xFF, 0xFF, 0xFF];
        let checksum = compute_checksum(&data);
        // Sum: 0xFFFF + 0xFFFF = 0x1FFFE
        // Fold: 0xFFFE + 1 = 0xFFFF
        // Ones' complement: 0x0000
        assert_eq!(checksum, 0x0000);
    }

    #[test]
    fn test_validate_invalid_header_checksum() {
        let mut header = TnsHeader::new(PacketType::Data, 4);
        header.header_checksum = 0x1234; // Invalid non-zero checksum

        // Detailed validation should show invalid header
        let result = validate_packet_detailed(&header, &[1, 2, 3, 4]);
        assert!(result.header_enabled);
        assert!(!result.header_valid);
    }
}
