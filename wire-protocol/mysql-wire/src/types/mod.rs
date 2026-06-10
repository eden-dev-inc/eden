//! MySQL packet type implementations.
//!
//! This module contains parsers and builders for all MySQL packet types.

pub mod auth;
pub mod auth_switch;
pub mod binlog;
pub mod column_definition;
pub mod command;
pub mod datetime;
pub mod dynamic;
pub mod eof;
pub mod err;
pub mod handshake;
pub mod handshake_response;
pub mod local_infile;
pub mod ok;
pub mod packet;
pub mod prepared;
pub mod query_attrs;
pub mod resultset;
pub mod row;
pub mod session_state;

// Re-export commonly used types
pub use auth::AuthPlugin;
pub use auth_switch::{AuthMoreData, AuthSwitchRequest, AuthSwitchResponse};
pub use binlog::{BinlogEventHeader, BinlogEventType, Gtid, QueryEventData, RotateEventData};
pub use column_definition::ColumnDefinition;
pub use command::Command;
pub use datetime::{DateTimeExt, MysqlDate, MysqlDateTime, MysqlTime, MysqlYear};
pub use dynamic::MysqlPacket;
pub use eof::EofPacket;
pub use err::ErrPacket;
pub use handshake::HandshakeV10;
pub use handshake_response::{HandshakeResponse, SslRequest};
pub use local_infile::{LocalInfileHandler, LocalInfileRequest, LocalInfileResponse};
pub use ok::OkPacket;
pub use packet::MysqlPacketHeader;
pub use prepared::{StmtExecute, StmtPrepareOk};
pub use query_attrs::{QueryAttribute, QueryAttributeValue, QueryAttributes};
pub use resultset::ResultSetMetadata;
pub use row::{BinaryRow, TextRow};
pub use session_state::{SessionStateChange, SessionStateInfo, SessionTrackType};
