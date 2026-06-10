//! Dynamic TNS packet parsing.
//!
//! This module provides version-agnostic parsing that can handle
//! any TNS packet type and version.

use crate::oracle_ext::OracleRead;
use crate::parse::{OracleParse, OracleParseError, OracleParseSync};
use crate::types::{
    accept::Accept,
    connect::Connect,
    data::Data,
    data_descriptor::DataDescriptor,
    marker::Marker,
    packet::{PacketType, TnsHeader},
    redirect::Redirect,
    refuse::Refuse,
};
use wire_stream::{WireRead, WireReadSync, WireReadSyncExt};

/// A dynamically-typed TNS packet.
///
/// This enum can represent any TNS packet type across all supported versions.
#[derive(Clone, Debug)]
pub enum TnsPacket {
    /// Connect request packet.
    Connect(Connect),
    /// Accept packet.
    Accept(Accept),
    /// Refuse packet.
    Refuse(Refuse),
    /// Redirect packet (used for RAC load balancing).
    Redirect(Redirect),
    /// Data packet.
    Data(Data),
    /// Marker packet (TNS v11+).
    Marker(Marker),
    /// Data descriptor packet (TNS v12+).
    DataDescriptor(DataDescriptor),
    /// Null packet (no payload).
    Null,
    /// Resend request.
    Resend,
    /// Abort packet.
    Abort,
    /// Attention packet.
    Attention,
    /// Unknown packet type with raw data.
    Unknown { packet_type: u8, data: Vec<u8> },
}

impl TnsPacket {
    /// Get the packet type.
    pub fn packet_type(&self) -> PacketType {
        match self {
            Self::Connect(_) => PacketType::Connect,
            Self::Accept(_) => PacketType::Accept,
            Self::Refuse(_) => PacketType::Refuse,
            Self::Redirect(_) => PacketType::Redirect,
            Self::Data(_) => PacketType::Data,
            Self::Marker(_) => PacketType::Marker,
            Self::DataDescriptor(_) => PacketType::DataDescriptor,
            Self::Null => PacketType::Null,
            Self::Resend => PacketType::Resend,
            Self::Abort => PacketType::Abort,
            Self::Attention => PacketType::Attention,
            Self::Unknown { packet_type, .. } => PacketType::Unknown(*packet_type),
        }
    }

    /// Check if this packet requires TNS v11+.
    pub fn requires_v11(&self) -> bool {
        matches!(self, Self::Marker(_))
    }

    /// Check if this packet requires TNS v12+.
    pub fn requires_v12(&self) -> bool {
        matches!(self, Self::DataDescriptor(_))
    }
}

/// Parsed TNS packet with header.
#[derive(Clone, Debug)]
pub struct ParsedPacket {
    /// The packet header.
    pub header: TnsHeader,
    /// The packet body.
    pub body: TnsPacket,
}

impl ParsedPacket {
    /// Get the negotiated/requested version from Connect/Accept packets.
    pub fn version(&self) -> Option<u16> {
        match &self.body {
            TnsPacket::Connect(c) => Some(c.version),
            TnsPacket::Accept(a) => Some(a.version),
            _ => None,
        }
    }
}

/// Error when parsing a dynamic TNS packet.
#[derive(Clone, Debug, thiserror::Error)]
pub enum DynamicParseError {
    #[error("header parse error: {0}")]
    Header(#[from] crate::types::packet::TnsHeaderError),
    #[error("connect parse error: {0}")]
    Connect(#[from] crate::types::connect::ConnectError),
    #[error("accept parse error: {0}")]
    Accept(#[from] crate::types::accept::AcceptError),
    #[error("refuse parse error: {0}")]
    Refuse(#[from] crate::types::refuse::RefuseError),
    #[error("redirect parse error: {0}")]
    Redirect(#[from] crate::types::redirect::RedirectError),
    #[error("data parse error: {0}")]
    Data(#[from] crate::types::data::DataError),
    #[error("marker parse error: {0}")]
    Marker(#[from] crate::types::marker::MarkerError),
    #[error("data descriptor parse error: {0}")]
    DataDescriptor(#[from] crate::types::data_descriptor::DataDescriptorError),
}

impl<S: WireReadSync + ?Sized> OracleParseSync<S> for ParsedPacket {
    type ParseError = DynamicParseError;
    type Value<'s>
        = ParsedPacket
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // Parse header first
        let header = TnsHeader::parse_sync(stream).map_err(|e| match e {
            OracleParseError::Stream(e) => OracleParseError::Stream(e),
            OracleParseError::Parse(e) => OracleParseError::Parse(DynamicParseError::Header(e)),
        })?;

        let data_length = header.data_length() as usize;

        // Parse body based on packet type
        let body = match header.packet_type {
            PacketType::Connect => {
                let connect = Connect::parse_sync(stream).map_err(|e| match e {
                    OracleParseError::Stream(e) => OracleParseError::Stream(e),
                    OracleParseError::Parse(e) => OracleParseError::Parse(DynamicParseError::Connect(e)),
                })?;
                TnsPacket::Connect(connect)
            }
            PacketType::Accept => {
                let accept = Accept::parse_sync(stream).map_err(|e| match e {
                    OracleParseError::Stream(e) => OracleParseError::Stream(e),
                    OracleParseError::Parse(e) => OracleParseError::Parse(DynamicParseError::Accept(e)),
                })?;
                TnsPacket::Accept(accept)
            }
            PacketType::Refuse => {
                let refuse = Refuse::parse_sync(stream).map_err(|e| match e {
                    OracleParseError::Stream(e) => OracleParseError::Stream(e),
                    OracleParseError::Parse(e) => OracleParseError::Parse(DynamicParseError::Refuse(e)),
                })?;
                TnsPacket::Refuse(refuse)
            }
            PacketType::Redirect => {
                let redirect = Redirect::parse_with_length_sync(stream, data_length).map_err(|e| match e {
                    OracleParseError::Stream(e) => OracleParseError::Stream(e),
                    OracleParseError::Parse(e) => OracleParseError::Parse(DynamicParseError::Redirect(e)),
                })?;
                TnsPacket::Redirect(redirect)
            }
            PacketType::Data => {
                let data = Data::parse_with_length_sync(stream, Some(data_length)).map_err(|e| match e {
                    OracleParseError::Stream(e) => OracleParseError::Stream(e),
                    OracleParseError::Parse(e) => OracleParseError::Parse(DynamicParseError::Data(e)),
                })?;
                TnsPacket::Data(data)
            }
            PacketType::Marker => {
                let marker = Marker::parse_sync(stream).map_err(|e| match e {
                    OracleParseError::Stream(e) => OracleParseError::Stream(e),
                    OracleParseError::Parse(e) => OracleParseError::Parse(DynamicParseError::Marker(e)),
                })?;
                TnsPacket::Marker(marker)
            }
            PacketType::DataDescriptor => {
                let dd = DataDescriptor::parse_sync(stream).map_err(|e| match e {
                    OracleParseError::Stream(e) => OracleParseError::Stream(e),
                    OracleParseError::Parse(e) => OracleParseError::Parse(DynamicParseError::DataDescriptor(e)),
                })?;
                TnsPacket::DataDescriptor(dd)
            }
            PacketType::Null => TnsPacket::Null,
            PacketType::Resend => TnsPacket::Resend,
            PacketType::Abort => TnsPacket::Abort,
            PacketType::Attention => TnsPacket::Attention,
            PacketType::Ack | PacketType::Control | PacketType::Unknown(_) => {
                // Read remaining data as raw bytes
                let data = stream.read_bytes_sync(data_length).map_err(OracleParseError::Stream)?.to_vec();
                TnsPacket::Unknown { packet_type: header.packet_type.as_u8(), data }
            }
        };

        Ok(ParsedPacket { header, body })
    }
}

impl<S: WireRead + ?Sized> OracleParse<S> for ParsedPacket {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // Parse header first
        let header = TnsHeader::parse(stream).await.map_err(|e| match e {
            OracleParseError::Stream(e) => OracleParseError::Stream(e),
            OracleParseError::Parse(e) => OracleParseError::Parse(DynamicParseError::Header(e)),
        })?;

        let data_length = header.data_length() as usize;

        // Parse body based on packet type
        let body = match header.packet_type {
            PacketType::Connect => {
                let connect = Connect::parse(stream).await.map_err(|e| match e {
                    OracleParseError::Stream(e) => OracleParseError::Stream(e),
                    OracleParseError::Parse(e) => OracleParseError::Parse(DynamicParseError::Connect(e)),
                })?;
                TnsPacket::Connect(connect)
            }
            PacketType::Accept => {
                let accept = Accept::parse(stream).await.map_err(|e| match e {
                    OracleParseError::Stream(e) => OracleParseError::Stream(e),
                    OracleParseError::Parse(e) => OracleParseError::Parse(DynamicParseError::Accept(e)),
                })?;
                TnsPacket::Accept(accept)
            }
            PacketType::Refuse => {
                let refuse = Refuse::parse(stream).await.map_err(|e| match e {
                    OracleParseError::Stream(e) => OracleParseError::Stream(e),
                    OracleParseError::Parse(e) => OracleParseError::Parse(DynamicParseError::Refuse(e)),
                })?;
                TnsPacket::Refuse(refuse)
            }
            PacketType::Redirect => {
                let redirect = Redirect::parse_with_length(stream, data_length).await.map_err(|e| match e {
                    OracleParseError::Stream(e) => OracleParseError::Stream(e),
                    OracleParseError::Parse(e) => OracleParseError::Parse(DynamicParseError::Redirect(e)),
                })?;
                TnsPacket::Redirect(redirect)
            }
            PacketType::Data => {
                let data = Data::parse_with_length(stream, Some(data_length)).await.map_err(|e| match e {
                    OracleParseError::Stream(e) => OracleParseError::Stream(e),
                    OracleParseError::Parse(e) => OracleParseError::Parse(DynamicParseError::Data(e)),
                })?;
                TnsPacket::Data(data)
            }
            PacketType::Marker => {
                let marker = Marker::parse(stream).await.map_err(|e| match e {
                    OracleParseError::Stream(e) => OracleParseError::Stream(e),
                    OracleParseError::Parse(e) => OracleParseError::Parse(DynamicParseError::Marker(e)),
                })?;
                TnsPacket::Marker(marker)
            }
            PacketType::DataDescriptor => {
                let dd = DataDescriptor::parse(stream).await.map_err(|e| match e {
                    OracleParseError::Stream(e) => OracleParseError::Stream(e),
                    OracleParseError::Parse(e) => OracleParseError::Parse(DynamicParseError::DataDescriptor(e)),
                })?;
                TnsPacket::DataDescriptor(dd)
            }
            PacketType::Null => TnsPacket::Null,
            PacketType::Resend => TnsPacket::Resend,
            PacketType::Abort => TnsPacket::Abort,
            PacketType::Attention => TnsPacket::Attention,
            PacketType::Ack | PacketType::Control | PacketType::Unknown(_) => {
                // Read remaining data as raw bytes
                let data = stream.read_bytes(data_length).await.map_err(OracleParseError::Stream)?.to_vec();
                TnsPacket::Unknown { packet_type: header.packet_type.as_u8(), data }
            }
        };

        Ok(ParsedPacket { header, body })
    }
}
