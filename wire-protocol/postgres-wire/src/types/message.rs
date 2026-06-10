//! Unified message dispatch enums for PostgreSQL protocol.
//!
//! These enums provide a convenient way to handle any message type from either
//! the frontend (client) or backend (server) direction.

use crate::error::{backend, frontend};
use crate::parse::{PgParseError, PgParseSync};
use crate::pg_ext::PgReadSync;
use crate::types::error_response::ResponseFields;
use crate::types::*;
use wire_stream::WireReadSync;

type RawErrorFields = Vec<(u8, String)>;

/// A message from the backend (server) to the frontend (client).
///
/// This enum covers all standard PostgreSQL backend messages.
#[derive(Clone, Debug)]
pub enum BackendMessage {
    /// Authentication request or completion.
    Authentication(AuthenticationRequest),
    /// Backend key data for query cancellation.
    BackendKeyData(BackendKeyData),
    /// Bind operation completed.
    BindComplete(BindComplete),
    /// Close operation completed.
    CloseComplete(CloseComplete),
    /// Command completed with tag.
    CommandComplete(CommandComplete),
    /// Copy data from COPY operation.
    CopyData(CopyData),
    /// COPY operation completed.
    CopyDone(CopyDone),
    /// COPY IN response.
    CopyInResponse(CopyInResponse),
    /// COPY OUT response.
    CopyOutResponse(CopyOutResponse),
    /// COPY BOTH response (replication).
    CopyBothResponse(CopyBothResponse),
    /// Data row from query result.
    DataRow(DataRow),
    /// Empty query response.
    EmptyQueryResponse(EmptyQueryResponse),
    /// Error response.
    ErrorResponse(ErrorResponse),
    /// Function call response (deprecated).
    #[allow(deprecated)]
    FunctionCallResponse(FunctionCallResponse),
    /// Negotiate protocol version.
    NegotiateProtocolVersion(NegotiateProtocolVersion),
    /// No data available for query.
    NoData(NoData),
    /// Notice/warning from server.
    NoticeResponse(NoticeResponse),
    /// Notification from NOTIFY.
    NotificationResponse(NotificationResponse),
    /// Parameter description.
    ParameterDescription(ParameterDescription),
    /// Parameter status.
    ParameterStatus(ParameterStatus),
    /// Parse operation completed.
    ParseComplete(ParseComplete),
    /// Portal suspended (more rows available).
    PortalSuspended(PortalSuspended),
    /// Server is ready for next query.
    ReadyForQuery(ReadyForQuery),
    /// Row description with column metadata.
    RowDescription(RowDescription),
    /// Unknown message type (forward compatibility).
    Unknown(UnknownMessage),
}

/// A message from the frontend (client) to the backend (server).
///
/// This enum covers all standard PostgreSQL frontend messages.
/// Note: Startup messages (StartupMessage, SSLRequest, CancelRequest, GSSEncRequest)
/// are NOT included as they lack a type byte and require special handling.
#[derive(Clone, Debug)]
pub enum FrontendMessage {
    /// Bind parameters to a prepared statement.
    Bind(Bind),
    /// Close a portal or prepared statement.
    Close(Close),
    /// Copy data to server.
    CopyData(CopyData),
    /// COPY operation completed.
    CopyDone(CopyDone),
    /// COPY operation failed.
    CopyFail(CopyFail),
    /// Describe a portal or prepared statement.
    Describe(Describe),
    /// Execute a portal.
    Execute(Execute),
    /// Flush output.
    Flush(Flush),
    /// Function call (deprecated).
    #[allow(deprecated)]
    FunctionCall(FunctionCall),
    /// Parse a query into a prepared statement.
    Parse(Parse),
    /// Password or SASL authentication response.
    Password(Authentication),
    /// Simple query.
    Query(Query),
    /// Sync for extended query pipeline.
    Sync(Sync),
    /// Terminate connection.
    Terminate(Terminate),
    /// Unknown message type (forward compatibility).
    Unknown(UnknownMessage),
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum BackendMessageError {
    #[error("invalid encoding")]
    InvalidEncoding,
    #[error("unknown authentication type: {0}")]
    UnknownAuthType(i32),
    #[error("invalid transaction status: {0}")]
    InvalidTransactionStatus(char),
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum FrontendMessageError {
    #[error("invalid encoding")]
    InvalidEncoding,
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for BackendMessage {
    type ParseError = BackendMessageError;
    type Value<'s>
        = BackendMessage
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        let length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let payload_len = length.saturating_sub(4) as usize;

        match msg_type {
            backend::AUTHENTICATION => {
                let auth_type = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
                let remaining = payload_len.saturating_sub(4);
                let auth = parse_authentication_body_sync(stream, auth_type, remaining)?;
                Ok(BackendMessage::Authentication(auth))
            }
            backend::BACKEND_KEY_DATA => {
                let process_id = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
                let secret_key = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
                Ok(BackendMessage::BackendKeyData(BackendKeyData::new(process_id, secret_key)))
            }
            backend::BIND_COMPLETE => Ok(BackendMessage::BindComplete(BindComplete)),
            backend::CLOSE_COMPLETE => Ok(BackendMessage::CloseComplete(CloseComplete)),
            backend::COMMAND_COMPLETE => {
                let tag_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                let tag = String::from_utf8(tag_bytes).map_err(|_| PgParseError::Parse(BackendMessageError::InvalidEncoding))?;
                Ok(BackendMessage::CommandComplete(CommandComplete::new(tag)))
            }
            backend::COPY_DATA => {
                let data = stream.read_bytes_sync(payload_len).map_err(PgParseError::Stream)?;
                Ok(BackendMessage::CopyData(CopyData::new(data)))
            }
            backend::COPY_DONE => Ok(BackendMessage::CopyDone(CopyDone)),
            backend::COPY_IN_RESPONSE => {
                let (format, column_formats) = parse_copy_response_body_sync(stream)?;
                Ok(BackendMessage::CopyInResponse(CopyInResponse::new(format, column_formats)))
            }
            backend::COPY_OUT_RESPONSE => {
                let (format, column_formats) = parse_copy_response_body_sync(stream)?;
                Ok(BackendMessage::CopyOutResponse(CopyOutResponse::new(format, column_formats)))
            }
            backend::COPY_BOTH_RESPONSE => {
                let (format, column_formats) = parse_copy_response_body_sync(stream)?;
                Ok(BackendMessage::CopyBothResponse(CopyBothResponse::new(format, column_formats)))
            }
            backend::DATA_ROW => {
                let column_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
                let count = column_count.clamp(0, 10000) as usize;
                let mut columns = Vec::with_capacity(count);
                for _ in 0..column_count {
                    let len = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
                    let value = if len < 0 {
                        ColumnValue::Null
                    } else {
                        let data = stream.read_bytes_sync(len as usize).map_err(PgParseError::Stream)?;
                        ColumnValue::Value(data)
                    };
                    columns.push(value);
                }
                Ok(BackendMessage::DataRow(DataRow::new(columns)))
            }
            backend::EMPTY_QUERY_RESPONSE => Ok(BackendMessage::EmptyQueryResponse(EmptyQueryResponse)),
            backend::ERROR_RESPONSE => {
                let raw_fields = parse_error_fields_sync(stream)?;
                let fields = build_response_fields(raw_fields);
                Ok(BackendMessage::ErrorResponse(ErrorResponse::new(fields)))
            }
            backend::FUNCTION_CALL_RESPONSE => {
                let len = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
                #[allow(deprecated)]
                let response = if len < 0 {
                    FunctionCallResponse::null()
                } else {
                    let data = stream.read_bytes_sync(len as usize).map_err(PgParseError::Stream)?;
                    FunctionCallResponse::with_value(data)
                };
                Ok(BackendMessage::FunctionCallResponse(response))
            }
            backend::NEGOTIATE_PROTOCOL_VERSION => {
                let newest_minor = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
                let num_unrecognized = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
                let count = num_unrecognized.clamp(0, 1024) as usize;
                let mut options = Vec::with_capacity(count);
                for _ in 0..num_unrecognized {
                    let opt_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                    let opt = String::from_utf8(opt_bytes).map_err(|_| PgParseError::Parse(BackendMessageError::InvalidEncoding))?;
                    options.push(opt);
                }
                Ok(BackendMessage::NegotiateProtocolVersion(NegotiateProtocolVersion::new(newest_minor, options)))
            }
            backend::NO_DATA => Ok(BackendMessage::NoData(NoData)),
            backend::NOTICE_RESPONSE => {
                let raw_fields = parse_error_fields_sync(stream)?;
                let fields = build_response_fields(raw_fields);
                Ok(BackendMessage::NoticeResponse(NoticeResponse::new(fields)))
            }
            backend::NOTIFICATION_RESPONSE => {
                let process_id = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
                let channel_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                let channel = String::from_utf8(channel_bytes).map_err(|_| PgParseError::Parse(BackendMessageError::InvalidEncoding))?;
                let payload_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                let payload = String::from_utf8(payload_bytes).map_err(|_| PgParseError::Parse(BackendMessageError::InvalidEncoding))?;
                Ok(BackendMessage::NotificationResponse(NotificationResponse::new(process_id, channel, payload)))
            }
            backend::PARAMETER_DESCRIPTION => {
                let count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
                let count = count.clamp(0, 10000) as usize;
                let mut oids = Vec::with_capacity(count);
                for _ in 0..count {
                    let oid = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
                    oids.push(oid);
                }
                Ok(BackendMessage::ParameterDescription(ParameterDescription::new(oids)))
            }
            backend::PARAMETER_STATUS => {
                let name_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                let name = String::from_utf8(name_bytes).map_err(|_| PgParseError::Parse(BackendMessageError::InvalidEncoding))?;
                let value_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                let value = String::from_utf8(value_bytes).map_err(|_| PgParseError::Parse(BackendMessageError::InvalidEncoding))?;
                Ok(BackendMessage::ParameterStatus(ParameterStatus::new(name, value)))
            }
            backend::PARSE_COMPLETE => Ok(BackendMessage::ParseComplete(ParseComplete)),
            backend::PORTAL_SUSPENDED => Ok(BackendMessage::PortalSuspended(PortalSuspended)),
            backend::READY_FOR_QUERY => {
                let status_byte = stream.read_u8_sync().map_err(PgParseError::Stream)?;
                let status = match status_byte {
                    b'I' => TransactionStatus::Idle,
                    b'T' => TransactionStatus::InTransaction,
                    b'E' => TransactionStatus::Failed,
                    _ => return Err(PgParseError::Parse(BackendMessageError::InvalidTransactionStatus(status_byte as char))),
                };
                Ok(BackendMessage::ReadyForQuery(ReadyForQuery { status }))
            }
            backend::ROW_DESCRIPTION => {
                let field_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
                let count = field_count.clamp(0, 10000) as usize;
                let mut fields = Vec::with_capacity(count);
                for _ in 0..field_count {
                    let name_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                    let name = String::from_utf8(name_bytes).map_err(|_| PgParseError::Parse(BackendMessageError::InvalidEncoding))?;
                    let table_oid = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
                    let column_id = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
                    let type_oid = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
                    let type_size = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
                    let type_modifier = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
                    let format_code = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
                    fields.push(FieldDescription {
                        name,
                        table_oid,
                        column_id,
                        type_oid,
                        type_size,
                        type_modifier,
                        format_code,
                    });
                }
                Ok(BackendMessage::RowDescription(RowDescription::new(fields)))
            }
            _ => {
                let payload = stream.read_bytes_sync(payload_len).map_err(PgParseError::Stream)?;
                Ok(BackendMessage::Unknown(UnknownMessage::new(msg_type, payload)))
            }
        }
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for FrontendMessage {
    type ParseError = FrontendMessageError;
    type Value<'s>
        = FrontendMessage
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        let length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let payload_len = length.saturating_sub(4) as usize;

        match msg_type {
            frontend::BIND => {
                let portal_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                let portal = String::from_utf8(portal_bytes).map_err(|_| PgParseError::Parse(FrontendMessageError::InvalidEncoding))?;
                let statement_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                let statement =
                    String::from_utf8(statement_bytes).map_err(|_| PgParseError::Parse(FrontendMessageError::InvalidEncoding))?;

                // Parameter formats
                let format_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
                let count = format_count.clamp(0, 10000) as usize;
                let mut param_formats = Vec::with_capacity(count);
                for _ in 0..format_count {
                    let format = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
                    param_formats.push(format);
                }

                // Parameter values
                let param_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
                let count = param_count.clamp(0, 10000) as usize;
                let mut param_values = Vec::with_capacity(count);
                for _ in 0..param_count {
                    let len = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
                    let value = if len < 0 {
                        None
                    } else {
                        Some(stream.read_bytes_sync(len as usize).map_err(PgParseError::Stream)?)
                    };
                    param_values.push(value);
                }

                // Result formats
                let result_format_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
                let count = result_format_count.clamp(0, 10000) as usize;
                let mut result_formats = Vec::with_capacity(count);
                for _ in 0..result_format_count {
                    let format = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
                    result_formats.push(format);
                }

                Ok(FrontendMessage::Bind(Bind {
                    portal,
                    statement,
                    param_formats,
                    param_values,
                    result_formats,
                }))
            }
            frontend::CLOSE => {
                let target_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
                let name_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                let name = String::from_utf8(name_bytes).map_err(|_| PgParseError::Parse(FrontendMessageError::InvalidEncoding))?;
                let close = if target_type == b'S' {
                    Close::statement(name)
                } else {
                    Close::portal(name)
                };
                Ok(FrontendMessage::Close(close))
            }
            frontend::COPY_DATA => {
                let data = stream.read_bytes_sync(payload_len).map_err(PgParseError::Stream)?;
                Ok(FrontendMessage::CopyData(CopyData::new(data)))
            }
            frontend::COPY_DONE => Ok(FrontendMessage::CopyDone(CopyDone)),
            frontend::COPY_FAIL => {
                let msg_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                let message = String::from_utf8(msg_bytes).map_err(|_| PgParseError::Parse(FrontendMessageError::InvalidEncoding))?;
                Ok(FrontendMessage::CopyFail(CopyFail::new(message)))
            }
            frontend::DESCRIBE => {
                let target_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
                let name_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                let name = String::from_utf8(name_bytes).map_err(|_| PgParseError::Parse(FrontendMessageError::InvalidEncoding))?;
                let describe = if target_type == b'S' {
                    Describe::statement(name)
                } else {
                    Describe::portal(name)
                };
                Ok(FrontendMessage::Describe(describe))
            }
            frontend::EXECUTE => {
                let portal_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                let portal = String::from_utf8(portal_bytes).map_err(|_| PgParseError::Parse(FrontendMessageError::InvalidEncoding))?;
                let max_rows = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
                Ok(FrontendMessage::Execute(Execute::named(portal, max_rows)))
            }
            frontend::FLUSH => Ok(FrontendMessage::Flush(Flush)),
            frontend::FUNCTION_CALL => {
                let oid = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

                // Argument formats
                let format_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
                let count = format_count.clamp(0, 10000) as usize;
                let mut formats = Vec::with_capacity(count);
                for _ in 0..format_count {
                    let format = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
                    formats.push(format);
                }

                // Arguments
                let arg_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
                let count = arg_count.clamp(0, 10000) as usize;
                let mut args = Vec::with_capacity(count);
                for _ in 0..arg_count {
                    let len = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
                    let arg = if len < 0 {
                        None
                    } else {
                        Some(stream.read_bytes_sync(len as usize).map_err(PgParseError::Stream)?)
                    };
                    args.push(arg);
                }

                let result_format = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;

                #[allow(deprecated)]
                Ok(FrontendMessage::FunctionCall(FunctionCall::new(oid, formats, args, result_format)))
            }
            frontend::PARSE => {
                let name_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                let name = String::from_utf8(name_bytes).map_err(|_| PgParseError::Parse(FrontendMessageError::InvalidEncoding))?;
                let query_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                let query = String::from_utf8(query_bytes).map_err(|_| PgParseError::Parse(FrontendMessageError::InvalidEncoding))?;
                let type_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
                let count = type_count.clamp(0, 10000) as usize;
                let mut param_types = Vec::with_capacity(count);
                for _ in 0..type_count {
                    let oid = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
                    param_types.push(oid);
                }
                Ok(FrontendMessage::Parse(Parse::new(name, query, param_types)))
            }
            frontend::PASSWORD_MESSAGE => {
                let data = stream.read_bytes_sync(payload_len).map_err(PgParseError::Stream)?;
                // Strip trailing NUL if present
                let data = if data.last() == Some(&0) {
                    data[..data.len() - 1].to_vec()
                } else {
                    data
                };
                Ok(FrontendMessage::Password(Authentication { data }))
            }
            frontend::QUERY => {
                let query_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                let query = String::from_utf8(query_bytes).map_err(|_| PgParseError::Parse(FrontendMessageError::InvalidEncoding))?;
                Ok(FrontendMessage::Query(Query::new(query)))
            }
            frontend::SYNC => Ok(FrontendMessage::Sync(Sync)),
            frontend::TERMINATE => Ok(FrontendMessage::Terminate(Terminate)),
            _ => {
                let payload = stream.read_bytes_sync(payload_len).map_err(PgParseError::Stream)?;
                Ok(FrontendMessage::Unknown(UnknownMessage::new(msg_type, payload)))
            }
        }
    }
}

// Helper function to parse authentication body
fn parse_authentication_body_sync<S: WireReadSync + ?Sized>(
    stream: &S,
    auth_type: i32,
    remaining: usize,
) -> Result<AuthenticationRequest, PgParseError<S::ReadError, BackendMessageError>> {
    use crate::error::auth as auth_const;

    match auth_type {
        auth_const::OK => Ok(AuthenticationRequest::Ok),
        auth_const::KERBEROS_V5 => Ok(AuthenticationRequest::KerberosV5),
        auth_const::CLEARTEXT_PASSWORD => Ok(AuthenticationRequest::CleartextPassword),
        auth_const::MD5_PASSWORD => {
            let salt_bytes = stream.read_bytes_sync(4).map_err(PgParseError::Stream)?;
            let mut salt = [0u8; 4];
            salt.copy_from_slice(&salt_bytes);
            Ok(AuthenticationRequest::MD5Password { salt })
        }
        auth_const::SCM_CREDENTIAL => Ok(AuthenticationRequest::SCMCredential),
        auth_const::GSS => Ok(AuthenticationRequest::GSS),
        auth_const::GSS_CONTINUE => {
            let data = stream.read_bytes_sync(remaining).map_err(PgParseError::Stream)?;
            Ok(AuthenticationRequest::GSSContinue { data })
        }
        auth_const::SSPI => Ok(AuthenticationRequest::SSPI),
        auth_const::SASL => {
            let mut mechanisms = Vec::new();
            let mut bytes_read = 0;
            while bytes_read < remaining {
                let mech_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
                bytes_read += mech_bytes.len() + 1;
                if mech_bytes.is_empty() {
                    break;
                }
                let mech = String::from_utf8(mech_bytes).map_err(|_| PgParseError::Parse(BackendMessageError::InvalidEncoding))?;
                mechanisms.push(mech);
            }
            Ok(AuthenticationRequest::SASL { mechanisms })
        }
        auth_const::SASL_CONTINUE => {
            let data = stream.read_bytes_sync(remaining).map_err(PgParseError::Stream)?;
            Ok(AuthenticationRequest::SASLContinue { data })
        }
        auth_const::SASL_FINAL => {
            let data = stream.read_bytes_sync(remaining).map_err(PgParseError::Stream)?;
            Ok(AuthenticationRequest::SASLFinal { data })
        }
        _ => Err(PgParseError::Parse(BackendMessageError::UnknownAuthType(auth_type))),
    }
}

// Helper function to parse COPY response body
fn parse_copy_response_body_sync<S: WireReadSync + ?Sized>(
    stream: &S,
) -> Result<(FormatCode, Vec<FormatCode>), PgParseError<S::ReadError, BackendMessageError>> {
    let format_byte = stream.read_u8_sync().map_err(PgParseError::Stream)?;
    let format = FormatCode::from_i16(format_byte as i16);

    let column_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
    let count = column_count.clamp(0, 10000) as usize;
    let mut column_formats = Vec::with_capacity(count);
    for _ in 0..column_count {
        let col_format = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
        column_formats.push(FormatCode::from_i16(col_format));
    }

    Ok((format, column_formats))
}

// Helper function to parse error/notice fields
fn parse_error_fields_sync<S: WireReadSync + ?Sized>(
    stream: &S,
) -> Result<RawErrorFields, PgParseError<S::ReadError, BackendMessageError>> {
    let mut fields = Vec::new();
    loop {
        let field_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if field_type == 0 {
            break;
        }
        let value_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
        let value = String::from_utf8(value_bytes).map_err(|_| PgParseError::Parse(BackendMessageError::InvalidEncoding))?;
        fields.push((field_type, value));
    }
    Ok(fields)
}

// Helper function to build ResponseFields from raw field tuples
fn build_response_fields(raw_fields: Vec<(u8, String)>) -> ResponseFields {
    use crate::error::error_field;

    let mut fields = ResponseFields::default();
    for (field_type, value) in raw_fields {
        match field_type {
            error_field::SEVERITY_LOCALIZED => fields.severity_localized = Some(value),
            error_field::SEVERITY => fields.severity = Some(value),
            error_field::CODE => fields.code = Some(value),
            error_field::MESSAGE => fields.message = Some(value),
            error_field::DETAIL => fields.detail = Some(value),
            error_field::HINT => fields.hint = Some(value),
            error_field::POSITION => fields.position = value.parse().ok(),
            error_field::INTERNAL_POSITION => fields.internal_position = value.parse().ok(),
            error_field::INTERNAL_QUERY => fields.internal_query = Some(value),
            error_field::WHERE => fields.where_ = Some(value),
            error_field::SCHEMA => fields.schema = Some(value),
            error_field::TABLE => fields.table = Some(value),
            error_field::COLUMN => fields.column = Some(value),
            error_field::DATATYPE => fields.data_type = Some(value),
            error_field::CONSTRAINT => fields.constraint = Some(value),
            error_field::FILE => fields.file = Some(value),
            error_field::LINE => fields.line = value.parse().ok(),
            error_field::ROUTINE => fields.routine = Some(value),
            _ => {} // Ignore unknown fields for forward compatibility
        }
    }
    fields
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_backend_message_ready_for_query() {
        let ready = ReadyForQuery::idle();
        let encoded = ready.encode();

        let stream = SliceStream::new(&encoded);
        let msg = BackendMessage::parse_sync(&stream).expect("parse failed");

        match msg {
            BackendMessage::ReadyForQuery(r) => {
                assert_eq!(r.status, TransactionStatus::Idle);
            }
            _ => panic!("wrong message type"),
        }
    }

    #[test]
    fn test_backend_message_command_complete() {
        let complete = CommandComplete::new("SELECT 1".to_string());
        let encoded = complete.encode();

        let stream = SliceStream::new(&encoded);
        let msg = BackendMessage::parse_sync(&stream).expect("parse failed");

        match msg {
            BackendMessage::CommandComplete(c) => {
                assert_eq!(c.tag, "SELECT 1");
            }
            _ => panic!("wrong message type"),
        }
    }

    #[test]
    fn test_frontend_message_query() {
        let query = Query::new("SELECT 1".to_string());
        let encoded = query.encode();

        let stream = SliceStream::new(&encoded);
        let msg = FrontendMessage::parse_sync(&stream).expect("parse failed");

        match msg {
            FrontendMessage::Query(q) => {
                assert_eq!(q.query, "SELECT 1");
            }
            _ => panic!("wrong message type"),
        }
    }

    #[test]
    fn test_backend_message_unknown() {
        // Unknown message type 'X' with some payload
        let data = [b'X', 0, 0, 0, 8, 1, 2, 3, 4];
        let stream = SliceStream::new(&data);
        let msg = BackendMessage::parse_sync(&stream).expect("parse failed");

        match msg {
            BackendMessage::Unknown(u) => {
                assert_eq!(u.message_type, b'X');
                assert_eq!(u.payload, vec![1, 2, 3, 4]);
            }
            _ => panic!("wrong message type"),
        }
    }

    #[test]
    fn test_frontend_message_parse() {
        let parse = Parse::new("stmt1", "SELECT $1", vec![25]); // 25 = text OID
        let encoded = parse.encode();

        let stream = SliceStream::new(&encoded);
        let msg = FrontendMessage::parse_sync(&stream).expect("parse failed");

        match msg {
            FrontendMessage::Parse(p) => {
                assert_eq!(p.name, "stmt1");
                assert_eq!(p.query, "SELECT $1");
                assert_eq!(p.param_types, vec![25]);
            }
            _ => panic!("wrong message type"),
        }
    }

    #[test]
    fn test_backend_message_authentication_ok() {
        let auth = AuthenticationRequest::Ok;
        let encoded = auth.encode();

        let stream = SliceStream::new(&encoded);
        let msg = BackendMessage::parse_sync(&stream).expect("parse failed");

        match msg {
            BackendMessage::Authentication(a) => {
                assert!(a.is_ok());
            }
            _ => panic!("wrong message type"),
        }
    }
}
