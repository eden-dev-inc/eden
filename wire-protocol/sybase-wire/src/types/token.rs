//! TDS token stream types.
//!
//! Tokens are the building blocks of TDS server responses. They appear
//! within REPLY packets and describe the data that follows.

use crate::error::{SybaseWireError, token_types};
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

use super::altrow::AltRow;
use super::capability::Capability;
use super::colmetadata::ColMetaData;
use super::cursor::{CurClose, CurDeclare, CurDelete, CurFetch, CurInfo, CurUpdate};
use super::done::Done;
use super::dynamic::Dynamic;
use super::eed::Eed;
use super::envchange::EnvChange;
use super::error::{ErrorInfo, InfoMessage};
use super::loginack::LoginAck;
use super::metadata::{ColInfo, Control, OrderBy, TabName};
use super::msg::Msg;
use super::offset::Offset;
use super::paramfmt::{ParamFmt, ParamFmt2};
use super::returnvalue::ReturnValue;
use super::row::Row;
use super::rowfmt::{RowFmt, RowFmt2};

/// A parsed TDS token.
#[derive(Clone, Debug)]
pub enum Token {
    /// Column metadata for result sets.
    ColMetaData(ColMetaData),
    /// Row data.
    Row(Row),
    /// Error message from server.
    Error(ErrorInfo),
    /// Informational message from server.
    Info(InfoMessage),
    /// Login acknowledgment.
    LoginAck(LoginAck),
    /// Environment change notification.
    EnvChange(EnvChange),
    /// End of result set.
    Done(Done),
    /// End of stored procedure.
    DoneProc(Done),
    /// End of statement in batch.
    DoneInProc(Done),
    /// Return status from stored procedure.
    ReturnStatus(i32),

    // TDS 5.0 tokens
    /// Capability exchange (TDS 5.0).
    Capability(Capability),
    /// Parameter format (TDS 5.0).
    ParamFmt(ParamFmt),
    /// Extended parameter format (TDS 5.0).
    ParamFmt2(ParamFmt2),
    /// Row format (TDS 5.0).
    RowFmt(RowFmt),
    /// Extended row format (TDS 5.0).
    RowFmt2(RowFmt2),
    /// Return value from stored procedure.
    ReturnValue(ReturnValue),
    /// Extended error data (TDS 5.0).
    Eed(Eed),
    /// Table name metadata.
    TabName(TabName),
    /// Column info metadata.
    ColInfo(ColInfo),
    /// Order by information.
    OrderBy(OrderBy),
    /// Control format.
    Control(Control),
    /// Cursor close.
    CurClose(CurClose),
    /// Cursor info.
    CurInfo(CurInfo),
    /// Cursor declare/open.
    CurDeclare(CurDeclare),
    /// Cursor fetch.
    CurFetch(CurFetch),
    /// Cursor delete.
    CurDelete(CurDelete),
    /// Cursor update.
    CurUpdate(CurUpdate),
    /// Dynamic SQL (prepared statements).
    Dynamic(Dynamic),
    /// Server message.
    Msg(Msg),
    /// SQL batch offset.
    Offset(Offset),
    /// Alternate row (COMPUTE BY results).
    AltRow(AltRow),

    /// Unknown or unsupported token.
    Unknown { token_type: u8, data: Vec<u8> },
}

impl Token {
    /// Get the token type byte.
    pub fn token_type(&self) -> u8 {
        match self {
            Token::ColMetaData(_) => token_types::COLMETADATA,
            Token::Row(_) => token_types::ROW,
            Token::Error(_) => token_types::ERROR,
            Token::Info(_) => token_types::INFO,
            Token::LoginAck(_) => token_types::LOGINACK,
            Token::EnvChange(_) => token_types::ENVCHANGE,
            Token::Done(_) => token_types::DONE,
            Token::DoneProc(_) => token_types::DONEPROC,
            Token::DoneInProc(_) => token_types::DONEINPROC,
            Token::ReturnStatus(_) => token_types::RETURNSTATUS,
            Token::Capability(_) => token_types::CAPABILITY,
            Token::ParamFmt(_) => token_types::PARAMFMT,
            Token::ParamFmt2(_) => token_types::PARAMFMT2,
            Token::RowFmt(_) => token_types::ROWFMT,
            Token::RowFmt2(_) => token_types::ROWFMT2,
            Token::ReturnValue(_) => token_types::RETURNVALUE,
            Token::Eed(_) => token_types::EED,
            Token::TabName(_) => token_types::TABNAME,
            Token::ColInfo(_) => token_types::COLINFO,
            Token::OrderBy(_) => token_types::ORDER,
            Token::Control(_) => token_types::CONTROL,
            Token::CurClose(_) => token_types::CURCLOSE,
            Token::CurInfo(_) => token_types::CURINFO,
            Token::CurDeclare(_) => token_types::CUROPEN,
            Token::CurFetch(_) => token_types::CURFETCH,
            Token::CurDelete(_) => token_types::CURDELETE,
            Token::CurUpdate(_) => token_types::CURUPDATE,
            Token::Dynamic(_) => token_types::DYNAMIC,
            Token::Msg(_) => token_types::MSG,
            Token::Offset(_) => token_types::OFFSET,
            Token::AltRow(_) => token_types::ALTROW,
            Token::Unknown { token_type, .. } => *token_type,
        }
    }
}

/// Token stream parser.
///
/// Parses a sequence of tokens from a TDS response packet.
#[derive(Clone, Debug)]
pub struct TokenStream {
    /// Current column metadata (needed for parsing rows).
    pub current_columns: Option<ColMetaData>,
}

impl TokenStream {
    /// Create a new token stream parser.
    pub fn new() -> Self {
        Self { current_columns: None }
    }

    /// Parse the next token from the stream.
    pub fn parse_next_sync<'s>(
        &mut self,
        stream: &'s SliceStream<'s>,
    ) -> Result<Option<Token>, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Check if we have more data
        let borrow = match stream.peek(Some(1)) {
            Ok(b) => b,
            Err(_) => return Ok(None), // End of stream
        };

        let token_type = borrow[0];
        stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;

        match token_type {
            token_types::COLMETADATA => {
                let colmeta = ColMetaData::parse_after_token_sync(stream)?;
                self.current_columns = Some(colmeta.clone());
                Ok(Some(Token::ColMetaData(colmeta)))
            }

            token_types::ROW => {
                let columns =
                    self.current_columns.as_ref().ok_or(SybaseParseError::Parse(SybaseWireError::InvalidTokenType(token_type)))?;
                let row = Row::parse_with_columns_sync(stream, columns)?;
                Ok(Some(Token::Row(row)))
            }

            token_types::ERROR => {
                let error = ErrorInfo::parse_after_token_sync(stream)?;
                Ok(Some(Token::Error(error)))
            }

            token_types::INFO => {
                let info = InfoMessage::parse_after_token_sync(stream)?;
                Ok(Some(Token::Info(info)))
            }

            token_types::LOGINACK => {
                let ack = LoginAck::parse_after_token_sync(stream)?;
                Ok(Some(Token::LoginAck(ack)))
            }

            token_types::ENVCHANGE => {
                let change = EnvChange::parse_after_token_sync(stream)?;
                Ok(Some(Token::EnvChange(change)))
            }

            token_types::DONE => {
                let done = Done::parse_after_token_sync(stream)?;
                Ok(Some(Token::Done(done)))
            }

            token_types::DONEPROC => {
                let done = Done::parse_after_token_sync(stream)?;
                Ok(Some(Token::DoneProc(done)))
            }

            token_types::DONEINPROC => {
                let done = Done::parse_after_token_sync(stream)?;
                Ok(Some(Token::DoneInProc(done)))
            }

            token_types::RETURNSTATUS => {
                let status = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)? as i32;
                Ok(Some(Token::ReturnStatus(status)))
            }

            // TDS 5.0 tokens
            token_types::CAPABILITY => {
                let cap = Capability::parse_after_token_sync(stream)?;
                Ok(Some(Token::Capability(cap)))
            }

            token_types::PARAMFMT => {
                let fmt = ParamFmt::parse_after_token_sync(stream)?;
                Ok(Some(Token::ParamFmt(fmt)))
            }

            token_types::PARAMFMT2 => {
                let fmt = ParamFmt2::parse_after_token_sync(stream)?;
                Ok(Some(Token::ParamFmt2(fmt)))
            }

            token_types::ROWFMT => {
                let fmt = RowFmt::parse_after_token_sync(stream)?;
                Ok(Some(Token::RowFmt(fmt)))
            }

            token_types::ROWFMT2 => {
                let fmt = RowFmt2::parse_after_token_sync(stream)?;
                Ok(Some(Token::RowFmt2(fmt)))
            }

            token_types::RETURNVALUE => {
                let ret = ReturnValue::parse_after_token_sync(stream)?;
                Ok(Some(Token::ReturnValue(ret)))
            }

            token_types::EED => {
                let eed = Eed::parse_after_token_sync(stream)?;
                Ok(Some(Token::Eed(eed)))
            }

            token_types::TABNAME => {
                let tab = TabName::parse_after_token_sync(stream)?;
                Ok(Some(Token::TabName(tab)))
            }

            token_types::COLINFO => {
                let col = ColInfo::parse_after_token_sync(stream)?;
                Ok(Some(Token::ColInfo(col)))
            }

            token_types::ORDER => {
                let order = OrderBy::parse_after_token_sync(stream)?;
                Ok(Some(Token::OrderBy(order)))
            }

            token_types::CONTROL => {
                let ctrl = Control::parse_after_token_sync(stream)?;
                Ok(Some(Token::Control(ctrl)))
            }

            token_types::CURCLOSE => {
                let cur = CurClose::parse_after_token_sync(stream)?;
                Ok(Some(Token::CurClose(cur)))
            }

            token_types::CURINFO => {
                let cur = CurInfo::parse_after_token_sync(stream)?;
                Ok(Some(Token::CurInfo(cur)))
            }

            token_types::CUROPEN => {
                let cur = CurDeclare::parse_after_token_sync(stream)?;
                Ok(Some(Token::CurDeclare(cur)))
            }

            token_types::DYNAMIC => {
                let dyn_sql = Dynamic::parse_after_token_sync(stream)?;
                Ok(Some(Token::Dynamic(dyn_sql)))
            }

            token_types::CURFETCH => {
                let cur = CurFetch::parse_after_token_sync(stream)?;
                Ok(Some(Token::CurFetch(cur)))
            }

            // Note: CURDELETE (0x81) conflicts with COLMETADATA (0x81) - they are
            // context-dependent in the protocol. COLMETADATA takes precedence.
            // CurDelete struct is available for manual parsing when context is known.
            token_types::CURUPDATE => {
                let cur = CurUpdate::parse_after_token_sync(stream)?;
                Ok(Some(Token::CurUpdate(cur)))
            }

            token_types::MSG => {
                let msg = Msg::parse_after_token_sync(stream)?;
                Ok(Some(Token::Msg(msg)))
            }

            token_types::OFFSET => {
                let off = Offset::parse_after_token_sync(stream)?;
                Ok(Some(Token::Offset(off)))
            }

            token_types::ALTROW => {
                let alt = AltRow::parse_after_token_sync(stream)?;
                Ok(Some(Token::AltRow(alt)))
            }

            _ => {
                // Unknown token - try to skip it
                // Variable-length tokens (0x80-0xFF) have a 2-byte length prefix
                let data = if token_type >= 0x80 {
                    let len = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)? as usize;
                    if len > 0 {
                        let borrow = stream.peek(Some(len)).map_err(SybaseParseError::Stream)?;
                        let data = borrow[..len].to_vec();
                        stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                        data
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };

                Ok(Some(Token::Unknown { token_type, data }))
            }
        }
    }

    /// Parse all tokens from the stream.
    pub fn parse_all_sync<'s>(
        &mut self,
        stream: &'s SliceStream<'s>,
    ) -> Result<Vec<Token>, SybaseParseError<SliceReadError, SybaseWireError>> {
        let mut tokens = Vec::new();
        while let Some(token) = self.parse_next_sync(stream)? {
            tokens.push(token);
        }
        Ok(tokens)
    }
}

impl Default for TokenStream {
    fn default() -> Self {
        Self::new()
    }
}
