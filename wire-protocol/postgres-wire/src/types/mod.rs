//! PostgreSQL message types.
//!
//! This module contains parsers and builders for all PostgreSQL wire protocol messages.
//! Messages are organized by their role in the protocol.

pub mod auth;
pub mod command_complete;
pub mod copy;
pub mod data_row;
pub mod error_response;
pub mod extended;
pub mod function_call;
pub mod message;
pub mod negotiate;
pub mod negotiation;
pub mod notification;
pub mod parameter;
pub mod query;
pub mod ready_for_query;
pub mod replication;
pub mod row_description;
pub mod startup;
pub mod unknown;

// Re-exports for convenience
pub use auth::{Authentication, AuthenticationRequest};
pub use command_complete::CommandComplete;
pub use copy::{CopyBothResponse, CopyData, CopyDone, CopyFail, CopyInResponse, CopyOutResponse, FormatCode};
pub use data_row::{ColumnValue, DataRow};
pub use error_response::{ErrorResponse, NoticeResponse};
pub use extended::{
    Bind, BindComplete, Close, CloseComplete, Describe, Execute, Flush, NoData, ParameterDescription, Parse, ParseComplete,
    PortalSuspended, Sync,
};
#[allow(deprecated)]
pub use function_call::{FunctionCall, FunctionCallResponse};
pub use message::{BackendMessage, FrontendMessage};
pub use negotiate::NegotiateProtocolVersion;
pub use negotiation::{GSSResponse, SSLResponse};
pub use notification::{EmptyQueryResponse, NotificationResponse, Terminate};
pub use parameter::{BackendKeyData, BackendKeyDataV2, ParameterStatus};
pub use query::Query;
pub use ready_for_query::{ReadyForQuery, TransactionStatus};
pub use replication::{HotStandbyFeedback, PrimaryKeepalive, ReplicationMessage, StandbyStatusUpdate, XLogData};
pub use row_description::{FieldDescription, RowDescription};
pub use startup::{CancelRequest, CancelRequestV2, GSSEncRequest, SSLRequest, StartupMessage};
pub use unknown::{MessageCategory, UnknownMessage};
