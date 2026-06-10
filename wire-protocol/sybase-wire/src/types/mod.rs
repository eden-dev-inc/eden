//! Sybase TDS packet types.
//!
//! This module organizes TDS packet types by version compatibility:
//!
//! # Common Types (All Versions)
//! - [`packet`]: Base packet header and type definitions
//! - [`login`]: Login packets for authentication
//! - [`query`]: SQL command packets
//! - [`reply`]: Server response packets
//!
//! # TDS 4.2 Types
//! - Basic TDS protocol support
//! - Simple login and query packets
//!
//! # TDS 5.0 Types
//! - [`capability`]: Feature negotiation
//! - [`rpc`]: Remote procedure calls
//! - [`cursor`]: Server-side cursors
//! - [`dynamic`]: Dynamic SQL (prepared statements)
//! - [`paramfmt`]: Parameter format metadata
//! - [`rowfmt`]: Row format metadata
//! - Extended data types
//!
//! # Token Types
//! - [`token`]: Token stream parsing for server responses
//! - [`done`]: DONE/DONEPROC/DONEINPROC tokens
//! - [`error`]: Error and info message tokens
//! - [`eed`]: Extended error data (TDS 5.0)
//! - [`loginack`]: Login acknowledgment tokens
//! - [`envchange`]: Environment change notifications
//! - [`colmetadata`]: Column metadata for result sets
//! - [`row`]: Row data tokens
//! - [`returnvalue`]: Output parameter values
//! - [`metadata`]: TABNAME, COLINFO, ORDERBY, CONTROL tokens
//!
//! # Large Object Types
//! - [`textptr`]: TEXT/IMAGE handling (TEXTPTR, WRITETEXT, READTEXT)
//!
//! # Bulk Operations
//! - [`bcp`]: Bulk Copy Protocol for high-performance data transfer
//!
//! # Utilities
//! - [`convert`]: Data type conversion between TDS bytes and Rust types
//!
//! # Authentication
//! - [`auth`]: SSPI and NTLM authentication support
//!
//! # Transaction Management
//! - [`transaction`]: Transaction control helpers

// Core packet types
pub mod packet;

// Token types for server responses
pub mod colmetadata;
pub mod done;
pub mod dynamic;
pub mod envchange;
pub mod error;
pub mod login;
pub mod loginack;
pub mod query;
pub mod row;
pub mod token;

// TDS 5.0 extended types
pub mod altrow;
pub mod capability;
pub mod cursor;
pub mod eed;
pub mod metadata;
pub mod msg;
pub mod offset;
pub mod paramfmt;
pub mod returnvalue;
pub mod rowfmt;
pub mod rpc;

// Large object and bulk operations
pub mod bcp;
pub mod textptr;

// Utilities
pub mod convert;

// Authentication
pub mod auth;

// Transaction management
pub mod transaction;

// Re-exports for convenience
pub use colmetadata::ColMetaData;
pub use done::Done;
pub use envchange::EnvChange;
pub use error::{ErrorInfo, InfoMessage};
pub use login::{Login, Login5};
pub use loginack::LoginAck;
pub use packet::{PacketType, TdsHeader, TdsPacket};
pub use query::Query;
pub use row::Row;
pub use token::Token;

// TDS 5.0 re-exports
pub use altrow::AltRow;
pub use capability::{Capability, CapabilityBuilder};
pub use cursor::{CurClose, CurDeclare, CurDelete, CurFetch, CurInfo, CurUpdate, CursorBuilder};
pub use dynamic::{Dynamic, DynamicBuilder, DynamicOperation};
pub use eed::Eed;
pub use metadata::{ColInfo, Control, OrderBy, TabName};
pub use msg::Msg;
pub use offset::Offset;
pub use paramfmt::{ParamFmt, ParamFmt2, ParamInfo};
pub use returnvalue::ReturnValue;
pub use rowfmt::{RowFmt, RowFmt2, RowFmt2Column, RowFmtColumn};
pub use rpc::{Rpc, RpcBuilder, RpcParameter};

// Large object re-exports
pub use textptr::{ReadTextBuilder, TextPtr, TextTimestamp, TextValue, UpdateTextBuilder, WriteTextBuilder};

// Bulk operations re-exports
pub use bcp::{BcpColumn, BcpDirection, BcpFormatFile, BcpRow, BulkInsertBuilder};

// Conversion utilities re-exports
pub use convert::{TdsValue, decode_value, encode_value};

// Authentication re-exports
pub use auth::{AuthMethod, NtlmChallenge, SspiBuilder, SspiToken};

// Transaction re-exports
pub use transaction::{IsolationLevel, TransactionBuilder, TransactionState};
