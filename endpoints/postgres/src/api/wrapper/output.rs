use bytes::Bytes;
use ep_core::{EndpointOutput, EndpointResponse, ToOutput};
use error::ProtocolError;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::openapi::{Array, Object, OneOfBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

#[derive(ToSchema)]
pub enum PostgresOutput {
    #[schema(title = "Postgres empty output")]
    EmptyOutput(EmptyOutput),
    #[schema(title = "Postgres bool output")]
    BoolOutput(BoolOutput),
    #[schema(title = "Postgres rows output")]
    RowsOutput(PostgresRowsOutput),
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct EmptyOutput(pub ());

impl ToOutput for EmptyOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Postgres, EndpointResponse::Ok("success".to_string()))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    #[allow(clippy::unit_arg)]
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        borsh::to_vec(&self.0).map_err(EpError::serde)
    }
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct BoolOutput(pub bool);

impl ToOutput for BoolOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Postgres, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        borsh::to_vec(&self.0).map_err(EpError::serde)
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct CopyInWriterOutput {
    rows: u64,
}

impl From<u64> for CopyInWriterOutput {
    fn from(rows: u64) -> Self {
        Self { rows }
    }
}

unsafe impl Send for CopyInWriterOutput {}
unsafe impl Sync for CopyInWriterOutput {}

impl ToOutput for CopyInWriterOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Postgres, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        Ok(serde_json::json!({
            "type": "copy_in",
            "rows": self.rows,
        }))
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        borsh::to_vec(&self.rows).map_err(EpError::serde)
    }
}

#[derive(Serialize, Deserialize)]
pub struct CopyOutReaderOutput {
    #[serde(skip)]
    buf: Vec<u8>,
}

impl CopyOutReaderOutput {
    pub fn new(buf: Vec<u8>) -> Self {
        Self { buf }
    }
}

unsafe impl Send for CopyOutReaderOutput {}
unsafe impl Sync for CopyOutReaderOutput {}

impl ToOutput for CopyOutReaderOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Postgres, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        Ok(if let Ok(s) = std::str::from_utf8(&self.buf) {
            serde_json::json!({
                "type": "copy_out",
                "value": s,
            })
        } else {
            serde_json::json!({
                "type": "copy_out",
                "value": &self.buf,
            })
        })
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for CopyOutReader"))
    }
}

/// Query result wrapping raw PG wire protocol response bytes.
///
/// The raw bytes contain the server's response (RowDescription, DataRow*, CommandComplete,
/// ReadyForQuery). Parsing into JSON rows happens lazily in `try_serde_serialize()`.
pub struct PostgresRowsOutput(pub Bytes);

impl ToOutput for PostgresRowsOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Postgres, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        // Return the raw wire bytes directly — useful for wire protocol passthrough
        Ok(self.0)
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        let rows = postgres_core::parse_simple_query_response(&self.0)?;
        match rows.len() {
            0 => Ok(Value::Null),
            1 => Ok(rows[0].to_json()),
            _ => {
                let arr: Vec<Value> = rows.iter().map(|r| r.to_json()).collect();
                Ok(Value::Array(arr))
            }
        }
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Database"))
    }
}

impl ToSchema for PostgresRowsOutput {}
impl PartialSchema for PostgresRowsOutput {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::OneOf(
            OneOfBuilder::new().item(Schema::Object(Object::default())).item(Schema::Array(Array::default())).build(),
        ))
    }
}

/// Single-row query result wrapping raw PG wire protocol response bytes.
pub(crate) struct PostgresRowOutput(pub Bytes);

impl ToOutput for PostgresRowOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Postgres, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Ok(self.0)
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        let rows = postgres_core::parse_simple_query_response(&self.0)?;
        match rows.len() {
            1 => Ok(rows[0].to_json()),
            0 => Err(EpError::request("Expected exactly one row but got none")),
            n => Err(EpError::request(format!("Expected exactly one row but got {n}"))),
        }
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Database"))
    }
}

/// Optional single-row query result wrapping raw PG wire protocol response bytes.
pub(crate) struct PostgresOptionRowOutput(pub Bytes);

impl ToOutput for PostgresOptionRowOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Postgres, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Ok(self.0)
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        let rows = postgres_core::parse_simple_query_response(&self.0)?;
        match rows.first() {
            Some(row) => Ok(row.to_json()),
            None => Ok(Value::Null),
        }
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Database"))
    }
}

pub struct U64Output(pub u64);

impl ToOutput for U64Output {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Postgres, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        borsh::to_vec(&self.0).map_err(EpError::serde)
    }
}

/// Simple query result wrapping raw PG wire protocol response bytes.
///
/// Handles multi-statement responses: each statement may produce rows and/or
/// a CommandComplete with affected row count.
pub struct PostgresSimpleQueryOutput(pub Bytes);

impl ToOutput for PostgresSimpleQueryOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Postgres, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Ok(self.0)
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        use postgres_core::{StatementResult, parse_simple_query_statements};

        let statements = parse_simple_query_statements(&self.0)?;

        let serialize_statement = |stmt: &StatementResult| -> Value {
            match stmt {
                StatementResult::Rows(rows) => match rows.len() {
                    0 => Value::Null,
                    1 => rows[0].to_json(),
                    _ => Value::Array(rows.iter().map(|r| r.to_json()).collect()),
                },
                StatementResult::Command { affected_rows } => {
                    serde_json::json!({"affected_rows": *affected_rows})
                }
            }
        };

        match statements.len() {
            0 => Ok(Value::Null),
            1 => Ok(serialize_statement(&statements[0])),
            _ => {
                let arr: Vec<Value> = statements.iter().map(serialize_statement).collect();
                Ok(Value::Array(arr))
            }
        }
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Database"))
    }
}

/// Cancel token placeholder — cancellation support via BackendKeyData.
pub struct CancelTokenAsyncOutput;

impl ToOutput for CancelTokenAsyncOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Postgres, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        Ok(serde_json::json!({
            "type": "CancelToken",
            "status": "active"
        }))
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for CancelToken"))
    }
}
