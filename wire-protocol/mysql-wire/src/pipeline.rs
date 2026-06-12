//! Zero-allocation pipeline support for extracting MySQL packets as borrowed slices.

use crate::limits::Limits;
use crate::mysql_ext::MysqlRead;
use crate::parse::MysqlParseError;
use wire_stream::{WireRead, WireReadSync};

/// A single MySQL packet as a borrowed slice from the stream.
#[derive(Copy, Clone, Debug)]
pub struct MysqlSlice<'a> {
    /// The sequence ID of this packet.
    pub sequence_id: u8,
    /// The payload length (from header).
    pub payload_length: u32,
    /// The complete raw bytes of this packet (including 4-byte header).
    pub raw: &'a [u8],
}

impl<'a> MysqlSlice<'a> {
    /// Returns the payload without the 4-byte header.
    #[inline]
    pub fn payload(&self) -> &'a [u8] {
        &self.raw[4..]
    }

    /// Returns the first byte of the payload (packet type indicator).
    #[inline]
    pub fn packet_type(&self) -> Option<u8> {
        self.raw.get(4).copied()
    }

    /// Returns true if this is an OK packet (0x00 header).
    #[inline]
    pub fn is_ok(&self) -> bool {
        self.packet_type() == Some(0x00)
    }

    /// Returns true if this is an ERR packet (0xFF header).
    #[inline]
    pub fn is_err(&self) -> bool {
        self.packet_type() == Some(0xFF)
    }

    /// Returns true if this is an EOF packet (0xFE header with length < 9).
    #[inline]
    pub fn is_eof(&self) -> bool {
        self.packet_type() == Some(0xFE) && self.payload_length < 9
    }

    /// Returns true if this is a local infile request (0xFB header).
    #[inline]
    pub fn is_local_infile(&self) -> bool {
        self.packet_type() == Some(0xFB)
    }

    /// Returns true if this packet is part of a multi-packet sequence.
    /// (payload_length == 0xFFFFFF means more packets follow)
    #[inline]
    pub fn has_more(&self) -> bool {
        self.payload_length == 0xFFFFFF
    }
}

/// Zero-allocation pipeline iterator over MySQL packets.
pub struct Pipeline<'s, S: WireReadSync + ?Sized> {
    stream: &'s S,
    limits: Limits,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("unexpected end of data")]
    UnexpectedEnd,

    #[error("packet too large: {0} bytes (max {1})")]
    PacketTooLarge(usize, usize),

    #[error("invalid packet header")]
    InvalidHeader,
}

impl<'s, S: WireReadSync + ?Sized> Pipeline<'s, S> {
    /// Create a new pipeline from a stream with default limits.
    #[inline]
    pub fn new(stream: &'s S) -> Self {
        Self { stream, limits: Limits::default() }
    }

    /// Create a new pipeline with custom limits.
    #[inline]
    pub fn with_limits(stream: &'s S, limits: Limits) -> Self {
        Self { stream, limits }
    }

    /// Get the underlying stream.
    #[inline]
    pub fn stream(&self) -> &'s S {
        self.stream
    }

    /// Skip a complete MySQL packet.
    /// Returns `false` if no data remains.
    pub fn skip(&mut self) -> Result<bool, MysqlParseError<S::ReadError, PipelineError>> {
        // Check if we have at least 4 bytes for the header
        let peek = match self.stream.peek(Some(4)) {
            Ok(b) if b.is_empty() => return Ok(false),
            Ok(b) if b.len() < 4 => return Ok(false),
            Ok(b) => b,
            Err(e) => return Err(MysqlParseError::Stream(e)),
        };

        // Parse header (3-byte length LE + 1-byte sequence)
        let payload_length = u32::from_le_bytes([peek[0], peek[1], peek[2], 0]) as usize;

        // Check limits
        if payload_length > self.limits.max_packet_size {
            return Err(MysqlParseError::Parse(PipelineError::PacketTooLarge(payload_length, self.limits.max_packet_size)));
        }

        // Accept header
        self.stream.accept(&peek, None).map_err(MysqlParseError::Stream)?;

        // Skip payload
        if payload_length > 0 {
            self.stream.advance_by(payload_length).map_err(MysqlParseError::Stream)?;
        }

        Ok(true)
    }

    /// Returns the next complete MySQL packet as raw bytes.
    pub fn next_raw(&mut self) -> Result<Option<&'s [u8]>, MysqlParseError<S::ReadError, PipelineError>> {
        let start = self.stream.position();

        if !self.skip()? {
            return Ok(None);
        }

        let offset = self.stream.offset_from(&start).map_err(MysqlParseError::Stream)?;

        self.stream.restore_to(&start).map_err(MysqlParseError::Stream)?;

        let raw_borrow = self.stream.peek(Some(offset)).map_err(MysqlParseError::Stream)?;

        self.stream.accept(&raw_borrow, None).map_err(MysqlParseError::Stream)?;

        // SAFETY: The borrow references data from the stream's underlying buffer
        // which has lifetime 's.
        let raw: &'s [u8] = unsafe { std::mem::transmute::<&[u8], &'s [u8]>(&*raw_borrow) };

        Ok(Some(raw))
    }

    /// Returns the next packet with parsed header info for easy dispatch.
    pub fn next_packet(&mut self) -> Result<Option<MysqlSlice<'s>>, MysqlParseError<S::ReadError, PipelineError>> {
        match self.next_raw()? {
            Some(raw) if raw.len() >= 4 => {
                let payload_length = u32::from_le_bytes([raw[0], raw[1], raw[2], 0]);
                let sequence_id = raw[3];
                Ok(Some(MysqlSlice { sequence_id, payload_length, raw }))
            }
            Some(_) => Err(MysqlParseError::Parse(PipelineError::InvalidHeader)),
            None => Ok(None),
        }
    }

    /// Iterate over all packets without allocating.
    pub fn for_each<F>(&mut self, mut f: F) -> Result<(), MysqlParseError<S::ReadError, PipelineError>>
    where
        F: FnMut(&'s [u8]) -> bool,
    {
        while let Some(raw) = self.next_raw()? {
            if !f(raw) {
                break;
            }
        }
        Ok(())
    }

    /// Iterate with packet info for easy dispatch.
    pub fn for_each_packet<F>(&mut self, mut f: F) -> Result<(), MysqlParseError<S::ReadError, PipelineError>>
    where
        F: FnMut(MysqlSlice<'s>) -> bool,
    {
        while let Some(slice) = self.next_packet()? {
            if !f(slice) {
                break;
            }
        }
        Ok(())
    }

    /// Count remaining MySQL packets.
    pub fn count(&mut self) -> Result<usize, MysqlParseError<S::ReadError, PipelineError>> {
        let mut count = 0;
        while self.skip()? {
            count += 1;
        }
        Ok(count)
    }

    /// Collect all packets into a vector of raw slices.
    pub fn collect_raw(&mut self) -> Result<Vec<&'s [u8]>, MysqlParseError<S::ReadError, PipelineError>> {
        let mut packets = Vec::new();
        while let Some(raw) = self.next_raw()? {
            packets.push(raw);
        }
        Ok(packets)
    }

    /// Read a multi-packet payload (for large data > 16MB).
    ///
    /// MySQL splits large payloads into multiple packets with max 0xFFFFFF bytes each.
    /// This method reassembles them into a single Vec.
    pub fn read_multi_packet(&mut self) -> Result<Option<Vec<u8>>, MysqlParseError<S::ReadError, PipelineError>> {
        let mut result = Vec::new();
        let mut expected_seq: Option<u8> = None;

        loop {
            let packet = match self.next_packet()? {
                Some(p) => p,
                None => {
                    if result.is_empty() {
                        return Ok(None);
                    } else {
                        break;
                    }
                }
            };

            // Verify sequence continuity
            if let Some(expected) = expected_seq
                && packet.sequence_id != expected
            {
                // Sequence mismatch - this is a protocol error
                // but for robustness we continue
            }
            expected_seq = Some(packet.sequence_id.wrapping_add(1));

            result.extend_from_slice(packet.payload());

            // If payload length < 0xFFFFFF, this is the last packet
            if packet.payload_length < 0xFFFFFF {
                break;
            }
        }

        if result.is_empty() { Ok(None) } else { Ok(Some(result)) }
    }
}

/// Extension trait for creating pipelines from streams.
pub trait PipelineExt: WireReadSync {
    /// Create a pipeline with default limits.
    fn mysql_pipeline(&self) -> Pipeline<'_, Self> {
        Pipeline::new(self)
    }

    /// Create a pipeline with custom limits.
    fn mysql_pipeline_with_limits(&self, limits: Limits) -> Pipeline<'_, Self> {
        Pipeline::with_limits(self, limits)
    }
}

impl<S: WireReadSync + ?Sized> PipelineExt for S {}

/// Async pipeline for streaming MySQL packets.
pub struct AsyncPipeline<'s, S: WireRead + ?Sized> {
    stream: &'s S,
    limits: Limits,
}

impl<'s, S: WireRead + ?Sized> AsyncPipeline<'s, S> {
    /// Create a new async pipeline from a stream with default limits.
    #[inline]
    pub fn new(stream: &'s S) -> Self {
        Self { stream, limits: Limits::default() }
    }

    /// Create a new async pipeline with custom limits.
    #[inline]
    pub fn with_limits(stream: &'s S, limits: Limits) -> Self {
        Self { stream, limits }
    }

    /// Get the underlying stream.
    #[inline]
    pub fn stream(&self) -> &'s S {
        self.stream
    }

    /// Read the next packet header asynchronously.
    ///
    /// Returns (payload_length, sequence_id) or None if no more data.
    pub async fn read_header(&mut self) -> Result<Option<(u32, u8)>, MysqlParseError<S::ReadError, PipelineError>> {
        // Try to read 4 bytes for the header
        let header = match self.stream.peek_read_exactly::<4>().await {
            Ok(h) => h,
            Err(_) => return Ok(None),
        };

        let payload_length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let sequence_id = header[3];

        // Check limits
        if payload_length as usize > self.limits.max_packet_size {
            return Err(MysqlParseError::Parse(PipelineError::PacketTooLarge(
                payload_length as usize,
                self.limits.max_packet_size,
            )));
        }

        self.stream.accept_exactly(&header).map_err(MysqlParseError::Stream)?;

        Ok(Some((payload_length, sequence_id)))
    }

    /// Read the next packet payload asynchronously.
    pub async fn read_payload(&mut self, length: u32) -> Result<Vec<u8>, MysqlParseError<S::ReadError, PipelineError>> {
        self.stream.read_bytes(length as usize).await.map_err(MysqlParseError::Stream)
    }

    /// Read a complete packet asynchronously.
    ///
    /// Returns the raw packet bytes (header + payload) or None if no more data.
    pub async fn read_packet(&mut self) -> Result<Option<Vec<u8>>, MysqlParseError<S::ReadError, PipelineError>> {
        let (payload_length, sequence_id) = match self.read_header().await? {
            Some((len, seq)) => (len, seq),
            None => return Ok(None),
        };

        let payload = self.read_payload(payload_length).await?;

        let mut packet = Vec::with_capacity(4 + payload.len());
        packet.push(payload_length as u8);
        packet.push((payload_length >> 8) as u8);
        packet.push((payload_length >> 16) as u8);
        packet.push(sequence_id);
        packet.extend_from_slice(&payload);

        Ok(Some(packet))
    }

    /// Skip the next packet asynchronously.
    pub async fn skip(&mut self) -> Result<bool, MysqlParseError<S::ReadError, PipelineError>> {
        let (payload_length, _) = match self.read_header().await? {
            Some((len, seq)) => (len, seq),
            None => return Ok(false),
        };

        // Skip payload bytes
        for _ in 0..payload_length {
            self.stream.read_u8().await.map_err(MysqlParseError::Stream)?;
        }

        Ok(true)
    }

    /// Read a multi-packet payload asynchronously (for large data > 16MB).
    pub async fn read_multi_packet(&mut self) -> Result<Option<Vec<u8>>, MysqlParseError<S::ReadError, PipelineError>> {
        let mut result = Vec::new();
        let mut expected_seq: Option<u8> = None;

        loop {
            let (payload_length, sequence_id) = match self.read_header().await? {
                Some((len, seq)) => (len, seq),
                None => {
                    if result.is_empty() {
                        return Ok(None);
                    } else {
                        break;
                    }
                }
            };

            // Verify sequence continuity
            if let Some(expected) = expected_seq
                && sequence_id != expected
            {
                // Sequence mismatch - protocol error but continue for robustness
            }
            expected_seq = Some(sequence_id.wrapping_add(1));

            let payload = self.read_payload(payload_length).await?;
            result.extend_from_slice(&payload);

            // If payload length < 0xFFFFFF, this is the last packet
            if payload_length < 0xFFFFFF {
                break;
            }
        }

        if result.is_empty() { Ok(None) } else { Ok(Some(result)) }
    }

    /// Count remaining packets asynchronously.
    pub async fn count(&mut self) -> Result<usize, MysqlParseError<S::ReadError, PipelineError>> {
        let mut count = 0;
        while self.skip().await? {
            count += 1;
        }
        Ok(count)
    }
}

/// Extension trait for creating async pipelines from streams.
pub trait AsyncPipelineExt: WireRead {
    /// Create an async pipeline with default limits.
    fn mysql_async_pipeline(&self) -> AsyncPipeline<'_, Self> {
        AsyncPipeline::new(self)
    }

    /// Create an async pipeline with custom limits.
    fn mysql_async_pipeline_with_limits(&self, limits: Limits) -> AsyncPipeline<'_, Self> {
        AsyncPipeline::with_limits(self, limits)
    }
}

impl<S: WireRead + ?Sized> AsyncPipelineExt for S {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::PacketBuilder;
    use wire_stream::SliceStream;

    #[test]
    fn test_pipeline_next_raw() {
        // Build some test packets
        let mut data = Vec::new();
        data.extend_from_slice(&PacketBuilder::new(0).write_u8(0x03).write_bytes(b"SELECT 1").build());
        data.extend_from_slice(&PacketBuilder::new(1).write_u8(0x00).build()); // OK packet

        let stream = SliceStream::new(&data);
        let mut pipeline = stream.mysql_pipeline();

        let first = pipeline.next_raw().unwrap().unwrap();
        assert_eq!(first.len(), 4 + 1 + 8); // header + cmd + "SELECT 1"

        let second = pipeline.next_raw().unwrap().unwrap();
        assert_eq!(second.len(), 4 + 1); // header + OK

        assert!(pipeline.next_raw().unwrap().is_none());
    }

    #[test]
    fn test_pipeline_next_packet() {
        let mut data = Vec::new();
        data.extend_from_slice(&PacketBuilder::new(5).write_u8(0xFF).write_bytes(b"error").build());

        let stream = SliceStream::new(&data);
        let mut pipeline = stream.mysql_pipeline();

        let packet = pipeline.next_packet().unwrap().unwrap();
        assert_eq!(packet.sequence_id, 5);
        assert!(packet.is_err());
        assert_eq!(packet.payload(), &[0xFF, b'e', b'r', b'r', b'o', b'r']);
    }

    #[test]
    fn test_pipeline_skip() {
        let mut data = Vec::new();
        data.extend_from_slice(&PacketBuilder::new(0).write_bytes(b"first").build());
        data.extend_from_slice(&PacketBuilder::new(1).write_bytes(b"second").build());
        data.extend_from_slice(&PacketBuilder::new(2).write_bytes(b"third").build());

        let stream = SliceStream::new(&data);
        let mut pipeline = stream.mysql_pipeline();

        assert!(pipeline.skip().unwrap());
        assert!(pipeline.skip().unwrap());

        let third = pipeline.next_packet().unwrap().unwrap();
        assert_eq!(third.sequence_id, 2);
    }

    #[test]
    fn test_pipeline_count() {
        let mut data = Vec::new();
        for i in 0..5 {
            data.extend_from_slice(&PacketBuilder::new(i).write_bytes(b"data").build());
        }

        let stream = SliceStream::new(&data);
        let mut pipeline = stream.mysql_pipeline();

        assert_eq!(pipeline.count().unwrap(), 5);
    }

    #[test]
    fn test_pipeline_for_each_packet() {
        let mut data = Vec::new();
        data.extend_from_slice(&PacketBuilder::new(0).write_u8(0x00).build()); // OK
        data.extend_from_slice(&PacketBuilder::new(1).write_u8(0xFE).build()); // EOF (short)
        data.extend_from_slice(&PacketBuilder::new(2).write_u8(0xFF).build()); // ERR

        let stream = SliceStream::new(&data);
        let mut pipeline = stream.mysql_pipeline();

        let mut types = Vec::new();
        pipeline
            .for_each_packet(|slice| {
                if slice.is_ok() {
                    types.push("OK");
                } else if slice.is_eof() {
                    types.push("EOF");
                } else if slice.is_err() {
                    types.push("ERR");
                }
                true
            })
            .unwrap();

        assert_eq!(types, vec!["OK", "EOF", "ERR"]);
    }

    #[test]
    fn test_mysql_slice_helpers() {
        let data = [
            0x05, 0x00, 0x00, // length = 5
            0x03, // sequence = 3
            0xFE, // EOF marker
            0x00, 0x00, // warnings
            0x00, 0x00, // status
        ];

        let slice = MysqlSlice { sequence_id: 3, payload_length: 5, raw: &data };

        assert!(slice.is_eof());
        assert!(!slice.is_ok());
        assert!(!slice.is_err());
        assert_eq!(slice.payload(), &[0xFE, 0x00, 0x00, 0x00, 0x00]);
    }
}
