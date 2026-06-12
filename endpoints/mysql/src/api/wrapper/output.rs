use base64::Engine;
use ep_core::{EndpointOutput, EndpointResponse, ToOutput};
use error::{EpError, ProtocolError, ResultEP, SerdeError};
use format::endpoint::EpKind;
use mysql_async::Row; // Using the custom Row from mysql_async
use serde_json::Value;
use utoipa::openapi::{Array, Object, OneOfBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

pub struct MySqlRowsOutput(pub Vec<Row>);

impl ToOutput for MySqlRowsOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mysql, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        let rows = &self.0;

        match rows.len() {
            0 => Ok(Value::Null),
            1 => match rows.first() {
                Some(row) => {
                    let row_data = mysql_async_row_to_row_data(row).map_err(EpError::serde)?;
                    Ok(serde_json::to_value(row_data).map_err(EpError::serde)?)
                }
                None => Err(EpError::Serde(SerdeError::FailedToParseRow)),
            },
            _ => {
                let mut result = Vec::with_capacity(rows.len());
                for row in rows {
                    let row_data = mysql_async_row_to_row_data(row).map_err(EpError::serde)?;
                    result.push(Value::Object(row_data));
                }
                Ok(Value::Array(result))
            }
        }
    }

    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for MySQL Database"))
    }
}

impl ToSchema for MySqlRowsOutput {}
impl PartialSchema for MySqlRowsOutput {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::OneOf(
            OneOfBuilder::new().item(Schema::Object(Object::default())).item(Schema::Array(Array::default())).build(),
        ))
    }
}

#[allow(dead_code)]
pub(crate) struct MySqlRowOutput(pub Row);

impl ToOutput for MySqlRowOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mysql, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        let row_data = mysql_async_row_to_row_data(&self.0).map_err(EpError::serde)?;
        serde_json::to_value(row_data).map_err(EpError::serde)
    }

    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for MySQL Database"))
    }
}

pub(crate) struct MySqlOptionRowOutput(pub Option<Row>);

impl ToOutput for MySqlOptionRowOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mysql, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        match &self.0 {
            None => Ok(Value::Null),
            Some(row) => {
                let row_data = mysql_async_row_to_row_data(row).map_err(EpError::serde)?;
                serde_json::to_value(row_data).map_err(EpError::serde)
            }
        }
    }

    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for MySQL Database"))
    }
}

// Convert mysql_async Row to JSON-serializable data
fn mysql_async_row_to_row_data(row: &Row) -> Result<serde_json::Map<String, Value>, String> {
    use serde_json::Map;

    let mut map = Map::new();
    let columns = row.columns_ref();

    for (i, column) in columns.iter().enumerate() {
        let column_name = column.name_str().to_string();

        // Get the value using the row's as_ref method
        let value = match row.as_ref(i) {
            Some(mysql_value) => mysql_async_value_to_json_value(mysql_value)?,
            None => Value::Null,
        };

        map.insert(column_name, value);
    }

    Ok(map)
}

// Convert mysql_async Value to serde_json Value
fn mysql_async_value_to_json_value(mysql_value: &mysql_async::Value) -> Result<Value, String> {
    use mysql_async::Value as MySqlValue;
    use serde_json::Number;

    match mysql_value {
        MySqlValue::NULL => Ok(Value::Null),

        // Integer types
        MySqlValue::Bytes(bytes) => {
            // For bytes, we'll convert to base64 string
            let base64_str = base64::engine::general_purpose::STANDARD.encode(bytes);
            Ok(Value::String(base64_str))
        }

        MySqlValue::Int(val) => Ok(Value::Number(Number::from(*val))),

        MySqlValue::UInt(val) => Ok(Value::Number(Number::from(*val))),

        MySqlValue::Float(val) => Number::from_f64(*val as f64).map(Value::Number).ok_or_else(|| "Invalid float value".to_string()),

        MySqlValue::Double(val) => Number::from_f64(*val).map(Value::Number).ok_or_else(|| "Invalid double value".to_string()),

        MySqlValue::Date(year, month, day, hour, minute, second, micro) => {
            let date_str = format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}", year, month, day, hour, minute, second, micro);
            Ok(Value::String(date_str))
        }

        MySqlValue::Time(is_negative, days, hours, minutes, seconds, microseconds) => {
            let sign = if *is_negative { "-" } else { "" };
            let time_str = format!("{}{}:{:02}:{:02}.{:06}", sign, (days * 24) + *hours as u32, minutes, seconds, microseconds);
            Ok(Value::String(time_str))
        }
    }
}
