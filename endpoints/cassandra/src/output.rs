#![allow(unexpected_cfgs)]
use base64::Engine;
use bigdecimal::num_bigint::BigInt;
use ep_core::{EndpointOutput, EndpointResponse, ToOutput};
use error::{EpError, ProtocolError, ResultEP};
use format::endpoint::EpKind;
use scylla::cluster::metadata::{CollectionType, NativeType};
use scylla::deserialize::row::DeserializeRow;
use scylla::deserialize::value::DeserializeValue;
use scylla::frame::response::result::{ColumnSpec, ColumnType};
use scylla::response::PagingStateResponse;
use scylla::response::query_result::{QueryResult, QueryRowsResult};
use scylla::value::CqlValue;
use serde::Serialize;
use serde_json::{Map, Number, Value, json, to_value};
use std::io::Error;
use utoipa::openapi::{ArrayBuilder, Object, ObjectBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

/// Custom error types for Cassandra value processing
#[derive(Debug, thiserror::Error)]
pub enum CassandraValueError {
    #[error("Value overflow: {0}")]
    Overflow(String),

    #[error("Type mismatch: expected {expected}, found {found}")]
    TypeMismatch { expected: String, found: String },

    #[error("Invalid format for {0}: {1}")]
    InvalidFormat(String, String),

    #[error("Unsupported column type: {0}")]
    UnsupportedType(String),

    #[error("Missing or null value for required column: {0}")]
    MissingValue(String),

    #[error("Failed to parse column '{column}': {details}")]
    ParseError { column: String, details: String },
}

impl From<CassandraValueError> for EpError {
    fn from(err: CassandraValueError) -> Self {
        EpError::parse(err.to_string())
    }
}
//
// struct ParsedRows(Vec<(String, RowInfo)>);
//
// impl ParsedRows {
//     fn new() -> Self {
//         Self(Vec::new())
//     }
//
//     fn get_index(&self, index: usize) -> Option<&RowInfo> {
//         self.0.get(index).map(|(_, row)| row)
//     }
//
//     fn get_name(&self, name: &str) -> Option<&RowInfo> {
//         for (n, row) in &self.0 {
//             if n == name {
//                 return Some(row);
//             }
//         }
//         None
//     }
//
//     fn push(&mut self, name: &str, row: RowInfo) {
//         self.0.push((name.to_string(), row))
//     }
//
//     fn insert(&mut self, index: usize, name: &str, row: RowInfo) {
//         self.0.insert(index, (name.to_string(), row))
//     }
//
//     fn len(&self) -> usize {
//         self.0.len()
//     }
//
//     fn iter(&self) -> impl Iterator<Item = &(String, RowInfo)> {
//         self.0.iter()
//     }
//
//     fn parse_rows(result: QueryResult) -> ResultEP<Self> {
//         let rows_result = result.into_rows_result().map_err(EpError::metadata)?;
//
//         let columns = rows_result.column_specs().as_slice();
//
//         let mut parsed_rows = Self::new();
//
//         for (index, row) in rows_result.rows().into_iter().enumerate() {
//             let row: CqlValue = row
//                 .map_err(EpError::metadata)?
//                 .columns
//                 .get(index)
//                 .ok_or_else(|| EpError::metadata("Missing columns".to_string()))?
//                 .ok_or_else(|| EpError::metadata("Missing columns".to_string()))?;
//
//             let row_info = RowInfo::new(
//                 match columns.get(index) {
//                     Some(column) => column.clone(),
//                     None => return Err(EpError::metadata("Missing column")),
//                 },
//                 row,
//             );
//
//             let name = row_info.name();
//             parsed_rows.push(name, row_info);
//         }
//
//         Ok(parsed_rows)
//     }
// }
//
// struct RowInfo {
//     column_spec: ColumnSpec<'static>, // Store reference to all column specs
//     row: CqlValue,
// }
//
// impl RowInfo {
//     fn new(column_spec: ColumnSpec<'static>, row: CqlValue) -> Self {
//         Self { column_spec, row }
//     }
//
//     fn name(&self) -> &str {
//         self.column_spec.name()
//     }
// }

#[derive(ToSchema)]
pub enum CassandraOutput {
    #[schema(title = "Cassandra paged output")]
    PagedOutput(CassandraQueryPagedOutput),
    #[schema(title = "Cassandra output")]
    Output(CassandraQueryOutput),
}

pub struct CassandraQueryPagedOutput(pub (QueryResult, PagingStateResponse));

impl ToOutput for CassandraQueryPagedOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Cassandra, EndpointResponse::Response(self))
    }

    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }

    fn try_serde_serialize(&self) -> ResultEP<Value> {
        let (query_result, paging_state) = &self.0;
        let query_result = query_result.clone(); // Clone only what's needed

        let rows = match query_result.into_rows_result() {
            Ok(rows_result) => map_rows_result(rows_result)?,
            Err(scylla::errors::IntoRowsResultError::ResultNotRows(_)) => {
                json!([])
            }
            Err(e) => {
                return Err(EpError::database(format!("Failed to get rows result: {}", e)));
            }
        };

        let paging_state_response = paging_state
            .clone() // Clone only what's needed
            .into_paging_control_flow()
            .continue_value()
            .unwrap_or_default();
        let paging_state_as_slice = paging_state_response.as_bytes_slice();

        Ok(json!({
            "rows": rows,
            "paging_state": paging_state_as_slice,
        }))
    }

    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Database"))
    }
}

impl ToSchema for CassandraQueryPagedOutput {}
impl PartialSchema for CassandraQueryPagedOutput {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .property("rows", Schema::Object(Object::default()))
                .property("paging_state", Schema::Object(Object::default()))
                .required("rows")
                .required("paging_state")
                .build(),
        ))
    }
}

pub struct CassandraQueryOutput(pub QueryResult);

impl ToOutput for CassandraQueryOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Cassandra, EndpointResponse::Response(self))
    }

    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }

    fn try_serde_serialize(&self) -> ResultEP<Value> {
        let query_result = self.0.clone(); // Clone only when needed

        Ok(match query_result.into_rows_result() {
            Ok(rows_result) => map_rows_result(rows_result)?,
            Err(scylla::errors::IntoRowsResultError::ResultNotRows(_)) => {
                json!([])
            }
            Err(e) => {
                return Err(EpError::database(format!("Failed to get rows result: {}", e)));
            }
        })
    }

    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Database"))
    }
}

impl ToSchema for CassandraQueryOutput {}
impl PartialSchema for CassandraQueryOutput {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Array(ArrayBuilder::new().title(Some("row".to_string())).build()))
    }
}

/// Maps query rows to a JSON Value array
fn map_rows_result(rows_result: QueryRowsResult) -> ResultEP<Value> {
    let mut rows = Vec::new();

    let row_iter = rows_result.rows::<RowValue>().map_err(|e| EpError::request(format!("Failed to create row iterator: {}", e)))?;

    for (i, row_result) in row_iter.into_iter().enumerate() {
        let row = row_result.map_err(|e| EpError::serde(format!("Failed to deserialize row {}: {}", i, e)))?;
        rows.push(to_value(&row).map_err(|e| EpError::serde(format!("Failed to serialize row {} to JSON: {}", i, e)))?);
    }

    Ok(Value::Array(rows))
}

struct RowValue(Value);

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for RowValue {
    fn type_check(specs: &[ColumnSpec]) -> Result<(), scylla::deserialize::TypeCheckError> {
        // Type checking implementation
        for spec in specs {
            if !is_supported_type(spec.typ()) {
                return Err(scylla::deserialize::TypeCheckError::new(Error::other(format!(
                    "Unsupported column type '{:?}' for column '{}'",
                    spec.typ(),
                    spec.name()
                ))));
            }
        }
        Ok(())
    }

    fn deserialize(
        row: scylla::deserialize::row::ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, scylla::deserialize::DeserializationError> {
        let mut col_values = Map::new();

        for col_result in row {
            let col = match col_result {
                Ok(col) => col,
                Err(e) => {
                    return Err(scylla::deserialize::DeserializationError::new(Error::other(format!(
                        "Failed to read column data: {}",
                        e
                    ))));
                }
            };

            let column_name = col.spec.name().to_owned();
            let column_type = col.spec.typ();

            // Deserialize the CQL value
            let cql_value = match CqlValue::deserialize(column_type, col.slice) {
                Ok(val) => val,
                Err(e) => {
                    return Err(scylla::deserialize::DeserializationError::new(Error::other(format!(
                        "Failed to deserialize CQL value for column '{}': {}",
                        column_name, e
                    ))));
                }
            };

            // Convert CQL value to JSON value
            match convert_column_to_json(&column_name, &cql_value, column_type) {
                Ok(Some(json_value)) => {
                    col_values.insert(column_name, json_value);
                }
                Ok(None) => {
                    // Skip null values if desired, or include them as null
                    col_values.insert(column_name, Value::Null);
                }
                Err(e) => {
                    return Err(scylla::deserialize::DeserializationError::new(Error::other(format!(
                        "Failed to convert column '{}' to JSON: {}",
                        column_name, e
                    ))));
                }
            }
        }

        Ok(Self(Value::Object(col_values)))
    }
}

// Helper function to check if a type is supported
fn is_supported_type(typ: &ColumnType) -> bool {
    match typ {
        // ColumnType::Custom(_) => true, // We'll handle custom types as base64 blobs
        ColumnType::Native(NativeType::Ascii) => true,
        ColumnType::Native(NativeType::BigInt) => true,
        ColumnType::Native(NativeType::Blob) => true,
        ColumnType::Native(NativeType::Boolean) => true,
        ColumnType::Native(NativeType::Counter) => true,
        ColumnType::Native(NativeType::Decimal) => true,
        ColumnType::Native(NativeType::Double) => true,
        ColumnType::Native(NativeType::Float) => true,
        ColumnType::Native(NativeType::Int) => true,
        ColumnType::Native(NativeType::Timestamp) => true,
        ColumnType::Native(NativeType::Uuid) => true,
        ColumnType::Native(NativeType::Varint) => true,
        ColumnType::Native(NativeType::Timeuuid) => true,
        ColumnType::Native(NativeType::Inet) => true,
        ColumnType::Native(NativeType::Date) => true,
        ColumnType::Native(NativeType::Time) => true,
        ColumnType::Native(NativeType::SmallInt) => true,
        ColumnType::Native(NativeType::TinyInt) => true,
        ColumnType::Native(NativeType::Duration) => true,
        ColumnType::Vector { typ, dimensions: _ } => is_supported_type(typ),
        ColumnType::Collection { frozen: _, typ } => match typ {
            CollectionType::List(inner) => is_supported_type(inner),
            CollectionType::Map(key, value) => is_supported_type(key) && is_supported_type(value),
            CollectionType::Set(inner) => is_supported_type(inner),
            _ => false, // collection type not supported
        },
        ColumnType::Tuple(types) => types.iter().all(is_supported_type),
        ColumnType::UserDefinedType { definition, .. } => definition.field_types.iter().all(|(_, typ)| is_supported_type(typ)),
        ColumnType::Native(NativeType::Text) => true,
        // Add any other types that might be added in future versions
        _ => false,
    }
}

fn convert_column_to_json(column_name: &str, value: &CqlValue, typ: &ColumnType) -> Result<Option<Value>, CassandraValueError> {
    if matches!(value, CqlValue::Empty) {
        return Ok(None);
    }

    convert_non_empty_cql_value_to_json(value, typ, column_name).map(Some)
}

#[allow(dead_code)]
fn convert_cql_value_to_json(value: &CqlValue, typ: &ColumnType) -> Result<Value, CassandraValueError> {
    match value {
        CqlValue::Empty => Ok(Value::Null),
        _ => convert_non_empty_cql_value_to_json(value, typ, ""),
    }
}

fn convert_non_empty_cql_value_to_json(value: &CqlValue, typ: &ColumnType, column_prefix: &str) -> Result<Value, CassandraValueError> {
    match typ {
        ColumnType::Native(NativeType::Int) => {
            let value = value
                .as_int()
                .ok_or_else(|| CassandraValueError::TypeMismatch { expected: "Int".to_string(), found: format!("{:?}", value) })?;

            Number::from_i128(value as i128)
                .ok_or_else(|| {
                    CassandraValueError::Overflow(format!("Int value {} for column '{}' is out of range for JSON", value, column_prefix))
                })
                .map(Value::Number)
        }
        ColumnType::Native(NativeType::Ascii) | ColumnType::Native(NativeType::Text) => {
            let str_value = value.as_text().or_else(|| value.as_ascii()).ok_or_else(|| CassandraValueError::TypeMismatch {
                expected: "Text/Ascii".to_string(),
                found: format!("{:?}", value),
            })?;
            Ok(Value::String(str_value.to_owned()))
        }
        ColumnType::Native(NativeType::Boolean) => {
            let bool_value = value.as_boolean().ok_or_else(|| CassandraValueError::TypeMismatch {
                expected: "Boolean".to_string(),
                found: format!("{:?}", value),
            })?;
            Ok(Value::Bool(bool_value))
        }
        ColumnType::Native(NativeType::Blob) => {
            let blob = value
                .as_blob()
                .ok_or_else(|| CassandraValueError::TypeMismatch { expected: "Blob".to_string(), found: format!("{:?}", value) })?;
            Ok(Value::String(base64::engine::general_purpose::STANDARD.encode(blob)))
        }
        ColumnType::Native(NativeType::Counter) => {
            let counter = value.as_counter().ok_or_else(|| CassandraValueError::TypeMismatch {
                expected: "Counter".to_string(),
                found: format!("{:?}", value),
            })?;

            Number::from_i128(counter.0 as i128)
                .ok_or_else(|| {
                    CassandraValueError::Overflow(format!(
                        "Counter value {} for column '{}' is out of range for JSON",
                        counter.0, column_prefix
                    ))
                })
                .map(Value::Number)
        }
        ColumnType::Native(NativeType::Date) => {
            let date = value
                .as_cql_date()
                .ok_or_else(|| CassandraValueError::TypeMismatch { expected: "Date".to_string(), found: format!("{:?}", value) })?;

            Ok(chrono::NaiveDate::from_num_days_from_ce_opt(date.0 as i32)
                .map(|d| Value::String(d.to_string()))
                .unwrap_or_else(|| Value::String(format!("DATE({})", date.0))))
        }
        ColumnType::Native(NativeType::Decimal) => {
            let decimal = value.clone().into_cql_decimal().ok_or_else(|| CassandraValueError::TypeMismatch {
                expected: "Decimal".to_string(),
                found: format!("{:?}", value),
            })?;

            let (bytes, scale) = decimal.as_signed_be_bytes_slice_and_exponent();

            Ok(Value::String(
                bigdecimal::BigDecimal::from_bigint(BigInt::from_signed_bytes_be(bytes), scale as i64).to_string(),
            ))
        }
        ColumnType::Native(NativeType::Double) => {
            let double = value.as_double().ok_or_else(|| CassandraValueError::TypeMismatch {
                expected: "Double".to_string(),
                found: format!("{:?}", value),
            })?;

            // Handle special floating point values
            if double.is_nan() {
                return Err(CassandraValueError::InvalidFormat(
                    column_prefix.to_string(),
                    "NaN cannot be represented in JSON".to_string(),
                ));
            }

            if double.is_infinite() {
                return Err(CassandraValueError::InvalidFormat(
                    column_prefix.to_string(),
                    "Infinity cannot be represented in JSON".to_string(),
                ));
            }

            Number::from_f64(double)
                .ok_or_else(|| {
                    CassandraValueError::InvalidFormat(
                        column_prefix.to_string(),
                        format!("Double value {} cannot be represented in JSON", double),
                    )
                })
                .map(Value::Number)
        }
        ColumnType::Native(NativeType::Duration) => {
            let duration = value.as_cql_duration().ok_or_else(|| CassandraValueError::TypeMismatch {
                expected: "Duration".to_string(),
                found: format!("{:?}", value),
            })?;
            let mut map = Map::new();
            map.insert("months".to_string(), Value::from(duration.months));
            map.insert("days".to_string(), Value::from(duration.days));
            map.insert("nanoseconds".to_string(), Value::from(duration.nanoseconds));

            Ok(Value::Object(map))
        }
        ColumnType::Native(NativeType::Float) => {
            let float = value
                .as_float()
                .ok_or_else(|| CassandraValueError::TypeMismatch { expected: "Float".to_string(), found: format!("{:?}", value) })?;

            // Handle special floating point values
            if float.is_nan() {
                return Err(CassandraValueError::InvalidFormat(
                    column_prefix.to_string(),
                    "NaN cannot be represented in JSON".to_string(),
                ));
            }

            if float.is_infinite() {
                return Err(CassandraValueError::InvalidFormat(
                    column_prefix.to_string(),
                    "Infinity cannot be represented in JSON".to_string(),
                ));
            }

            // Convert f32 to f64 for serde_json Number
            let float_val = float as f64;
            Number::from_f64(float_val)
                .ok_or_else(|| {
                    CassandraValueError::InvalidFormat(
                        column_prefix.to_string(),
                        format!("Float value {} cannot be represented in JSON", float),
                    )
                })
                .map(Value::Number)
        }
        ColumnType::Native(NativeType::BigInt) => {
            let bigint = value.as_bigint().ok_or_else(|| CassandraValueError::TypeMismatch {
                expected: "BigInt".to_string(),
                found: format!("{:?}", value),
            })?;

            Number::from_i128(bigint as i128)
                .ok_or_else(|| {
                    CassandraValueError::Overflow(format!(
                        "BigInt value {} for column '{}' is out of range for JSON",
                        bigint, column_prefix
                    ))
                })
                .map(Value::Number)
        }
        ColumnType::Native(NativeType::Timestamp) => {
            let timestamp = value.as_cql_timestamp().ok_or_else(|| CassandraValueError::TypeMismatch {
                expected: "Timestamp".to_string(),
                found: format!("{:?}", value),
            })?;

            Ok(chrono::DateTime::<chrono::Utc>::from_timestamp_millis(timestamp.0)
                .map(|dt| Value::String(dt.to_rfc3339()))
                .unwrap_or_else(|| Value::String(format!("TIMESTAMP({})", timestamp.0))))
        }
        ColumnType::Native(NativeType::Inet) => {
            let inet = value
                .as_inet()
                .ok_or_else(|| CassandraValueError::TypeMismatch { expected: "Inet".to_string(), found: format!("{:?}", value) })?;
            Ok(Value::String(inet.to_string()))
        }
        ColumnType::Collection { frozen: _, typ: CollectionType::List(elem_type) } => {
            let list = value
                .as_list()
                .ok_or_else(|| CassandraValueError::TypeMismatch { expected: "List".to_string(), found: format!("{:?}", value) })?;

            let mut values = Vec::with_capacity(list.len());
            for (i, item) in list.iter().enumerate() {
                let item_prefix = if column_prefix.is_empty() {
                    format!("list[{}]", i)
                } else {
                    format!("{}[{}]", column_prefix, i)
                };

                match convert_non_empty_cql_value_to_json(item, elem_type, &item_prefix) {
                    Ok(json_value) => values.push(json_value),
                    Err(e) => {
                        return Err(CassandraValueError::ParseError { column: item_prefix, details: e.to_string() });
                    }
                }
            }

            Ok(Value::Array(values))
        }
        ColumnType::Collection { frozen: _, typ: CollectionType::Map(key_type, value_type) } => {
            let map = value
                .as_map()
                .ok_or_else(|| CassandraValueError::TypeMismatch { expected: "Map".to_string(), found: format!("{:?}", value) })?;

            let mut obj = Map::new();
            for (i, (k, v)) in map.iter().enumerate() {
                let key = match cql_value_to_string(k, key_type) {
                    Ok(key_str) => key_str,
                    Err(e) => {
                        let key_prefix = if column_prefix.is_empty() {
                            format!("map[key {}]", i)
                        } else {
                            format!("{}[key {}]", column_prefix, i)
                        };

                        return Err(CassandraValueError::ParseError { column: key_prefix, details: e.to_string() });
                    }
                };

                let value_prefix = if column_prefix.is_empty() {
                    format!("map[{}]", key)
                } else {
                    format!("{}[{}]", column_prefix, key)
                };

                let value = match convert_non_empty_cql_value_to_json(v, value_type, &value_prefix) {
                    Ok(json_value) => json_value,
                    Err(e) => {
                        return Err(CassandraValueError::ParseError { column: value_prefix, details: e.to_string() });
                    }
                };

                obj.insert(key, value);
            }

            Ok(Value::Object(obj))
        }
        ColumnType::Collection { frozen: _, typ: CollectionType::Set(elem_type) } => {
            let set = value
                .as_set()
                .ok_or_else(|| CassandraValueError::TypeMismatch { expected: "Set".to_string(), found: format!("{:?}", value) })?;

            let mut values = Vec::with_capacity(set.len());
            for (i, item) in set.iter().enumerate() {
                let item_prefix = if column_prefix.is_empty() {
                    format!("set[{}]", i)
                } else {
                    format!("{}[{}]", column_prefix, i)
                };

                match convert_non_empty_cql_value_to_json(item, elem_type, &item_prefix) {
                    Ok(json_value) => values.push(json_value),
                    Err(e) => {
                        return Err(CassandraValueError::ParseError { column: item_prefix, details: e.to_string() });
                    }
                }
            }

            Ok(Value::Array(values))
        }
        ColumnType::UserDefinedType { definition, .. } => {
            let udt = value.as_udt().ok_or_else(|| CassandraValueError::TypeMismatch {
                expected: "UserDefinedType".to_string(),
                found: format!("{:?}", value),
            })?;

            let mut obj = Map::new();
            for (name, value) in udt {
                // Find the matching field type
                if let Some((_, field_type)) = definition.field_types.iter().find(|(field_name, _)| field_name == name) {
                    if let Some(val) = value {
                        let field_prefix = if column_prefix.is_empty() {
                            format!("udt.{}", name)
                        } else {
                            format!("{}.{}", column_prefix, name)
                        };

                        match convert_non_empty_cql_value_to_json(val, field_type, &field_prefix) {
                            Ok(json_value) => {
                                obj.insert(name.clone(), json_value);
                            }
                            Err(e) => {
                                return Err(CassandraValueError::ParseError { column: field_prefix, details: e.to_string() });
                            }
                        }
                    } else {
                        obj.insert(name.clone(), Value::Null);
                    }
                } else {
                    // Field in value but not in type definition
                    return Err(CassandraValueError::TypeMismatch {
                        expected: format!("UDT with defined field '{}'", name),
                        found: "UDT without matching field type".to_string(),
                    });
                }
            }

            Ok(Value::Object(obj))
        }
        ColumnType::Native(NativeType::SmallInt) => {
            let smallint = value.as_smallint().ok_or_else(|| CassandraValueError::TypeMismatch {
                expected: "SmallInt".to_string(),
                found: format!("{:?}", value),
            })?;

            Number::from_i128(smallint as i128)
                .ok_or_else(|| {
                    CassandraValueError::Overflow(format!(
                        "SmallInt value {} for column '{}' is out of range for JSON",
                        smallint, column_prefix
                    ))
                })
                .map(Value::Number)
        }
        ColumnType::Native(NativeType::TinyInt) => {
            let tinyint = value.as_tinyint().ok_or_else(|| CassandraValueError::TypeMismatch {
                expected: "TinyInt".to_string(),
                found: format!("{:?}", value),
            })?;

            Number::from_i128(tinyint as i128)
                .ok_or_else(|| {
                    CassandraValueError::Overflow(format!(
                        "TinyInt value {} for column '{}' is out of range for JSON",
                        tinyint, column_prefix
                    ))
                })
                .map(Value::Number)
        }
        ColumnType::Native(NativeType::Time) => {
            let time = value
                .as_cql_time()
                .ok_or_else(|| CassandraValueError::TypeMismatch { expected: "Time".to_string(), found: format!("{:?}", value) })?;

            // Format time values consistently
            #[allow(unexpected_cfgs)]
            #[cfg(feature = "chrono-04")]
            {
                let secs = time.0 / 1_000_000_000;
                let nanos = (time.0 % 1_000_000_000) as u32;

                match chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs.try_into().unwrap_or(0), nanos) {
                    Some(t) => Ok(Value::String(t.to_string())),
                    None => Ok(Value::String(format!("{}ns", time.0))),
                }
            }

            #[allow(unexpected_cfgs)]
            #[cfg(not(feature = "chrono-04"))]
            {
                Number::from_i128(time.0 as i128)
                    .ok_or_else(|| {
                        CassandraValueError::Overflow(format!(
                            "Time value {} for column '{}' is out of range for JSON",
                            time.0, column_prefix
                        ))
                    })
                    .map(Value::Number)
            }
        }
        ColumnType::Native(NativeType::Timeuuid) => {
            let timeuuid = value.as_timeuuid().ok_or_else(|| CassandraValueError::TypeMismatch {
                expected: "Timeuuid".to_string(),
                found: format!("{:?}", value),
            })?;
            Ok(Value::String(timeuuid.to_string()))
        }
        ColumnType::Tuple(types) => {
            let tuple_items = match value {
                CqlValue::Tuple(items) => items,
                _ => {
                    return Err(CassandraValueError::TypeMismatch { expected: "Tuple".to_string(), found: format!("{:?}", value) });
                }
            };

            if tuple_items.len() > types.len() {
                return Err(CassandraValueError::TypeMismatch {
                    expected: format!("Tuple with {} elements", types.len()),
                    found: format!("Tuple with {} elements", tuple_items.len()),
                });
            }

            let mut values = Vec::with_capacity(types.len());
            for (i, item_type) in types.iter().enumerate() {
                if i < tuple_items.len() {
                    if let Some(item) = &tuple_items[i] {
                        let item_prefix = if column_prefix.is_empty() {
                            format!("tuple[{}]", i)
                        } else {
                            format!("{}[{}]", column_prefix, i)
                        };

                        match convert_non_empty_cql_value_to_json(item, item_type, &item_prefix) {
                            Ok(json_value) => values.push(json_value),
                            Err(e) => {
                                return Err(CassandraValueError::ParseError { column: item_prefix, details: e.to_string() });
                            }
                        }
                    } else {
                        values.push(Value::Null);
                    }
                } else {
                    // Fill with nulls for missing values
                    values.push(Value::Null);
                }
            }

            Ok(Value::Array(values))
        }
        ColumnType::Native(NativeType::Uuid) => {
            let uuid = value
                .as_uuid()
                .ok_or_else(|| CassandraValueError::TypeMismatch { expected: "Uuid".to_string(), found: format!("{:?}", value) })?;
            Ok(Value::String(uuid.to_string()))
        }
        ColumnType::Native(NativeType::Varint) => {
            let varint = value.to_owned().into_cql_varint().ok_or_else(|| CassandraValueError::TypeMismatch {
                expected: "Varint".to_string(),
                found: format!("{:?}", value),
            })?;

            // For varint, stringify since it might not fit in JSON number
            Ok(Value::String(BigInt::from_signed_bytes_be(varint.as_signed_bytes_be_slice()).to_string()))
        }
        _ => Err(CassandraValueError::UnsupportedType("type not implemented".to_string())),
        // Custom type not implemented in scylla@1.0
        // ColumnType::Native(NativeType::Custom(type_name)) => {
        //     // Handle custom type by encoding to base64 and including type information
        //     let blob_data = value.as_blob().map(base64::encode).ok_or_else(|| {
        //         CassandraValueError::TypeMismatch {
        //             expected: format!("Custom type ({})", type_name),
        //             found: format!("{:?}", value),
        //         }
        //     })?;

        //     let mut obj = Map::new();
        //     obj.insert("type".to_string(), Value::String(type_name.to_string()));
        //     obj.insert("data".to_string(), Value::String(blob_data));

        //     Ok(Value::Object(obj))
        // }
    }
}

fn cql_value_to_string(value: &CqlValue, typ: &ColumnType) -> Result<String, CassandraValueError> {
    match value {
        CqlValue::Ascii(s) | CqlValue::Text(s) => Ok(s.clone()),
        CqlValue::Boolean(b) => Ok(b.to_string()),
        CqlValue::Int(i) => Ok(i.to_string()),
        CqlValue::BigInt(i) => Ok(i.to_string()),
        CqlValue::SmallInt(i) => Ok(i.to_string()),
        CqlValue::TinyInt(i) => Ok(i.to_string()),
        CqlValue::Uuid(u) => Ok(u.to_string()),
        CqlValue::Timeuuid(tu) => Ok(tu.to_string()),
        CqlValue::Varint(v) => {
            let big_int = BigInt::from_signed_bytes_be(v.as_signed_bytes_be_slice());
            Ok(big_int.to_string())
        }
        CqlValue::Float(f) => {
            if f.is_nan() || f.is_infinite() {
                return Err(CassandraValueError::InvalidFormat(
                    "float".to_string(),
                    format!("Value {} cannot be used as a map key", f),
                ));
            }
            Ok(f.to_string())
        }
        CqlValue::Double(d) => {
            if d.is_nan() || d.is_infinite() {
                return Err(CassandraValueError::InvalidFormat(
                    "double".to_string(),
                    format!("Value {} cannot be used as a map key", d),
                ));
            }
            Ok(d.to_string())
        }
        CqlValue::Inet(addr) => Ok(addr.to_string()),
        CqlValue::Timestamp(ts) => Ok(format!("{}", ts.0)),
        CqlValue::Date(d) => Ok(format!("{}", d.0)),
        CqlValue::Time(t) => Ok(t.0.to_string()),
        _ => Err(CassandraValueError::InvalidFormat(
            format!("{:?}", typ),
            "This type cannot be converted to a string for a map key".to_string(),
        )),
    }
}

impl Serialize for RowValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigdecimal::num_bigint::BigInt;
    use scylla::cluster::metadata::UserDefinedType;
    use scylla::frame::response::result::ColumnType;
    use scylla::value::{Counter, CqlDate, CqlDecimal, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlValue, CqlVarint};
    use std::net::IpAddr;
    use std::str::FromStr;
    use std::sync::Arc;
    use uuid::Uuid;

    // Helper function to create a test UDT value
    // Add a lifetime parameter to the function
    fn create_test_udt<'a>() -> (CqlValue, ColumnType<'a>) {
        use std::borrow::Cow;

        let fields = vec![
            ("name".to_string(), Some(CqlValue::Text("test_name".to_string()))),
            ("age".to_string(), Some(CqlValue::Int(30))),
            ("active".to_string(), Some(CqlValue::Boolean(true))),
        ];

        let udt_value = CqlValue::UserDefinedType {
            name: "test_type".to_string(),
            keyspace: "test_keyspace".to_string(),
            fields,
        };

        let field_types = vec![
            (Cow::Borrowed("name"), ColumnType::Native(NativeType::Text)),
            (Cow::Borrowed("age"), ColumnType::Native(NativeType::Int)),
            (Cow::Borrowed("active"), ColumnType::Native(NativeType::Boolean)),
        ];

        // Create UDT type with the correct structure
        let udt_type = ColumnType::UserDefinedType {
            frozen: false,
            definition: Arc::new(UserDefinedType {
                name: Cow::Borrowed("test_type"),
                keyspace: Cow::Borrowed("test_keyspace"),
                field_types,
            }),
        };

        (udt_value, udt_type)
    }

    #[test]
    fn test_primitive_types() {
        // Test integer types
        let int_val = CqlValue::Int(42);
        assert_eq!(
            convert_cql_value_to_json(&int_val, &ColumnType::Native(NativeType::Int)).unwrap(),
            Value::Number(Number::from(42))
        );

        let bigint_val = CqlValue::BigInt(9223372036854775807);
        assert_eq!(
            convert_cql_value_to_json(&bigint_val, &ColumnType::Native(NativeType::BigInt)).unwrap(),
            Value::Number(Number::from_i128(9223372036854775807).unwrap())
        );

        let smallint_val = CqlValue::SmallInt(32767);
        assert_eq!(
            convert_cql_value_to_json(&smallint_val, &ColumnType::Native(NativeType::SmallInt)).unwrap(),
            Value::Number(Number::from(32767))
        );

        let tinyint_val = CqlValue::TinyInt(127);
        assert_eq!(
            convert_cql_value_to_json(&tinyint_val, &ColumnType::Native(NativeType::TinyInt)).unwrap(),
            Value::Number(Number::from(127))
        );

        // Test text types
        let text_val = CqlValue::Text("hello world".to_string());
        assert_eq!(
            convert_cql_value_to_json(&text_val, &ColumnType::Native(NativeType::Text)).unwrap(),
            Value::String("hello world".to_string())
        );

        let ascii_val = CqlValue::Ascii("ASCII text".to_string());
        assert_eq!(
            convert_cql_value_to_json(&ascii_val, &ColumnType::Native(NativeType::Ascii)).unwrap(),
            Value::String("ASCII text".to_string())
        );

        // Test boolean
        let bool_val = CqlValue::Boolean(true);
        assert_eq!(
            convert_cql_value_to_json(&bool_val, &ColumnType::Native(NativeType::Boolean)).unwrap(),
            Value::Bool(true)
        );

        // Test floating point

        let pi = std::f32::consts::PI;
        let float_val = CqlValue::Float(pi);
        assert_eq!(
            convert_cql_value_to_json(&float_val, &ColumnType::Native(NativeType::Float)).unwrap(),
            Value::Number(Number::from_f64(pi as f64).unwrap())
        );

        let double_val = CqlValue::Double(std::f64::consts::E);
        assert_eq!(
            convert_cql_value_to_json(&double_val, &ColumnType::Native(NativeType::Double)).unwrap(),
            Value::Number(Number::from_f64(std::f64::consts::E).unwrap())
        );

        // Test counter
        let counter_val = CqlValue::Counter(Counter(100));
        assert_eq!(
            convert_cql_value_to_json(&counter_val, &ColumnType::Native(NativeType::Counter)).unwrap(),
            Value::Number(Number::from(100))
        );
    }

    #[test]
    fn test_blob_and_custom_types() {
        // Test blob
        let blob_data = vec![0x01, 0x02, 0x03, 0x04];
        let blob_val = CqlValue::Blob(blob_data.clone());
        assert_eq!(
            convert_cql_value_to_json(&blob_val, &ColumnType::Native(NativeType::Blob)).unwrap(),
            Value::String(base64::engine::general_purpose::STANDARD.encode(&blob_data))
        );

        // Test custom type
        // let custom_val = CqlValue::Blob(blob_data.clone());
        // let custom_type = ColumnType::Custom(Cow::Borrowed("com.example.custom"));

        // let result = convert_cql_value_to_json(&custom_val, &custom_type).unwrap();
        // if let Value::Object(map) = result {
        //     assert_eq!(
        //         map.get("type").unwrap(),
        //         &Value::String("com.example.custom".to_string())
        //     );
        //     assert_eq!(
        //         map.get("data").unwrap(),
        //         &Value::String(base64::encode(&blob_data))
        //     );
        // } else {
        //     panic!("Expected Object, got {:?}", result);
        // }
    }

    #[test]
    fn test_numeric_special_types() {
        // Test Decimal
        // Note: The actual CqlDecimal structure might be different depending on the scylla crate version
        // This test may need adaptation based on the actual implementation
        let decimal_bytes = BigInt::from(12345).to_signed_bytes_be();
        let decimal = match CqlValue::Decimal(CqlDecimal::from_signed_be_bytes_and_exponent(decimal_bytes.clone(), 3)) {
            d => d,
            #[allow(unreachable_patterns)]
            _ => {
                // Fallback in case the signature changed
                println!("Warning: Decimal test skipped due to API mismatch");
                return;
            }
        };

        let result = convert_cql_value_to_json(&decimal, &ColumnType::Native(NativeType::Decimal)).unwrap();
        assert!(matches!(result, Value::String(_))); // Just check it's a string

        // Test Varint
        let varint_bytes = BigInt::from(9876543210u64).to_signed_bytes_be();
        let varint = match CqlValue::Varint(CqlVarint::from_signed_bytes_be(varint_bytes.clone())) {
            v => v,
            #[allow(unreachable_patterns)]
            _ => {
                // Fallback in case the signature changed
                println!("Warning: Varint test skipped due to API mismatch");
                return;
            }
        };

        let result = convert_cql_value_to_json(&varint, &ColumnType::Native(NativeType::Varint)).unwrap();
        assert!(matches!(result, Value::String(_))); // Just check it's a string
    }

    #[test]
    fn test_collection_types() {
        // Test List
        let list_items = vec![CqlValue::Int(1), CqlValue::Int(2), CqlValue::Int(3)];
        let list_val = CqlValue::List(list_items);

        let result = convert_cql_value_to_json(
            &list_val,
            &ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int))),
            },
        )
        .unwrap();
        if let Value::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Value::Number(Number::from(1)));
            assert_eq!(arr[1], Value::Number(Number::from(2)));
            assert_eq!(arr[2], Value::Number(Number::from(3)));
        } else {
            panic!("Expected Array, got {:?}", result);
        }

        // Test Set
        let set_items = vec![
            CqlValue::Text("item1".to_string()),
            CqlValue::Text("item2".to_string()),
            CqlValue::Text("item3".to_string()),
        ];
        let set_val = CqlValue::Set(set_items);

        let result = convert_cql_value_to_json(
            &set_val,
            &ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Text))),
            },
        )
        .unwrap();
        if let Value::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Value::String("item1".to_string()));
            assert_eq!(arr[1], Value::String("item2".to_string()));
            assert_eq!(arr[2], Value::String("item3".to_string()));
        } else {
            panic!("Expected Array, got {:?}", result);
        }

        // Test Map
        let map_items = vec![
            (CqlValue::Text("key1".to_string()), CqlValue::Int(101)),
            (CqlValue::Text("key2".to_string()), CqlValue::Int(102)),
            (CqlValue::Text("key3".to_string()), CqlValue::Int(103)),
        ];
        let map_val = CqlValue::Map(map_items);

        let result = convert_cql_value_to_json(
            &map_val,
            &ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(Box::new(ColumnType::Native(NativeType::Text)), Box::new(ColumnType::Native(NativeType::Int))),
            },
        )
        .unwrap();

        if let Value::Object(obj) = result {
            assert_eq!(obj.len(), 3);
            assert_eq!(obj.get("key1").unwrap(), &Value::Number(Number::from(101)));
            assert_eq!(obj.get("key2").unwrap(), &Value::Number(Number::from(102)));
            assert_eq!(obj.get("key3").unwrap(), &Value::Number(Number::from(103)));
        } else {
            panic!("Expected Object, got {:?}", result);
        }
    }

    #[test]
    #[allow(unexpected_cfgs)]
    fn test_timestamp_date_time() {
        // Test Timestamp
        // 2023-01-01 12:00:00 UTC in milliseconds since epoch
        let timestamp_millis = 1672574400000;

        // Based on the original code, it appears the timestamp is represented directly
        let timestamp_val = CqlValue::Timestamp(CqlTimestamp(timestamp_millis));

        let result = convert_cql_value_to_json(&timestamp_val, &ColumnType::Native(NativeType::Timestamp)).unwrap();
        assert!(matches!(result, Value::String(_))); // Just verify it's a string

        // Test Date
        // Days since epoch + 2^31, for 2023-01-01
        let date_val = CqlValue::Date(CqlDate(2_147_570_047)); // Direct value without wrapper

        let result = convert_cql_value_to_json(&date_val, &ColumnType::Native(NativeType::Date)).unwrap();
        assert!(matches!(result, Value::String(_))); // Just verify it's a string

        // Test Time (nanoseconds past midnight)
        let time_val = CqlValue::Time(CqlTime(43200000000000)); // 12:00:00, direct value

        let result = convert_cql_value_to_json(&time_val, &ColumnType::Native(NativeType::Time)).unwrap();
        #[cfg(not(feature = "chrono-04"))]
        assert!(matches!(result, Value::String(_)) || matches!(result, Value::Number(_)));
    }

    #[test]
    fn test_uuid_and_special_types() {
        // Test UUID
        let uuid_str = "f3b4958c-52a1-11e7-802a-010203040506";
        let uuid = Uuid::parse_str(uuid_str).unwrap();
        let uuid_val = CqlValue::Uuid(uuid);

        let result = convert_cql_value_to_json(&uuid_val, &ColumnType::Native(NativeType::Uuid)).unwrap();
        assert_eq!(result, Value::String(uuid_str.to_string()));

        // Test TimeUUID
        let timeuuid_val = CqlValue::Timeuuid(CqlTimeuuid::from(uuid));

        let result = convert_cql_value_to_json(&timeuuid_val, &ColumnType::Native(NativeType::Timeuuid)).unwrap();
        assert_eq!(result, Value::String(uuid_str.to_string()));

        // Test Inet
        let ip_str = "192.168.1.1";
        let ip = IpAddr::from_str(ip_str).unwrap();
        let inet_val = CqlValue::Inet(ip);

        let result = convert_cql_value_to_json(&inet_val, &ColumnType::Native(NativeType::Inet)).unwrap();
        assert_eq!(result, Value::String(ip_str.to_string()));
    }

    #[test]
    fn test_complex_types() {
        // Test UDT (User Defined Type)
        let (udt_val, udt_type) = create_test_udt();

        let result = convert_cql_value_to_json(&udt_val, &udt_type).unwrap();
        if let Value::Object(obj) = result {
            assert_eq!(obj.len(), 3);
            assert_eq!(obj.get("name").unwrap(), &Value::String("test_name".to_string()));
            assert_eq!(obj.get("age").unwrap(), &Value::Number(Number::from(30)));
            assert_eq!(obj.get("active").unwrap(), &Value::Bool(true));
        } else {
            panic!("Expected Object, got {:?}", result);
        }

        // Test Tuple
        // Note: The actual Tuple structure might be different depending on the scylla crate version
        // This test may need adaptation based on the actual implementation
        let tuple_items = vec![
            Some(CqlValue::Text("tuple_text".to_string())),
            Some(CqlValue::Int(42)),
            Some(CqlValue::Boolean(false)),
        ];

        // Try to create a tuple value in a way that should work with various API versions
        let tuple_val = match CqlValue::Tuple(tuple_items) {
            t => t,
            #[allow(unreachable_patterns)]
            _ => {
                // Fallback if signature changed
                println!("Warning: Tuple test skipped due to API mismatch");
                return;
            }
        };

        let tuple_types = vec![
            ColumnType::Native(NativeType::Text),
            ColumnType::Native(NativeType::Int),
            ColumnType::Native(NativeType::Boolean),
        ];
        let tuple_type = ColumnType::Tuple(tuple_types);

        let result = convert_cql_value_to_json(&tuple_val, &tuple_type).unwrap();
        if let Value::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Value::String("tuple_text".to_string()));
            assert_eq!(arr[1], Value::Number(Number::from(42)));
            assert_eq!(arr[2], Value::Bool(false));
        } else {
            panic!("Expected Array, got {:?}", result);
        }
    }

    #[test]
    fn test_duration() {
        // Test Duration
        // Create a CqlDuration with the correct structure based on the codebase
        let duration_val = match CqlValue::Duration(CqlDuration {
            months: 1,
            days: 15,
            nanoseconds: 86400000000000, // 1 day in nanos
        }) {
            d => d,
            #[allow(unreachable_patterns)]
            _ => {
                // Fallback in case the signature changed
                println!("Warning: Duration test skipped due to API mismatch");
                return;
            }
        };

        let result = convert_cql_value_to_json(&duration_val, &ColumnType::Native(NativeType::Duration)).unwrap();
        if let Value::Object(obj) = result {
            assert_eq!(obj.len(), 3);
            assert_eq!(obj.get("months").unwrap(), &Value::Number(Number::from(1)));
            assert_eq!(obj.get("days").unwrap(), &Value::Number(Number::from(15)));
            assert_eq!(obj.get("nanoseconds").unwrap(), &Value::Number(Number::from(86400000000000i64)));
        } else {
            panic!("Expected Object, got {:?}", result);
        }
    }

    #[test]
    fn test_nested_collections() {
        // Test nested list (list of lists)
        let inner_list1 = CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)]);
        let inner_list2 = CqlValue::List(vec![CqlValue::Int(3), CqlValue::Int(4)]);
        let outer_list = CqlValue::List(vec![inner_list1, inner_list2]);

        let inner_type = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int))),
        };
        let outer_type = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(inner_type)),
        };

        let result = convert_cql_value_to_json(&outer_list, &outer_type).unwrap();
        if let Value::Array(outer_arr) = result {
            assert_eq!(outer_arr.len(), 2);

            if let Value::Array(inner_arr1) = &outer_arr[0] {
                assert_eq!(inner_arr1.len(), 2);
                assert_eq!(inner_arr1[0], Value::Number(Number::from(1)));
                assert_eq!(inner_arr1[1], Value::Number(Number::from(2)));
            } else {
                panic!("Expected Array for inner_arr1, got {:?}", outer_arr[0]);
            }

            if let Value::Array(inner_arr2) = &outer_arr[1] {
                assert_eq!(inner_arr2.len(), 2);
                assert_eq!(inner_arr2[0], Value::Number(Number::from(3)));
                assert_eq!(inner_arr2[1], Value::Number(Number::from(4)));
            } else {
                panic!("Expected Array for inner_arr2, got {:?}", outer_arr[1]);
            }
        } else {
            panic!("Expected Array, got {:?}", result);
        }

        // Test map with list values
        let map_items = vec![
            (
                CqlValue::Text("list1".to_string()),
                CqlValue::List(vec![CqlValue::Text("a".to_string()), CqlValue::Text("b".to_string())]),
            ),
            (
                CqlValue::Text("list2".to_string()),
                CqlValue::List(vec![CqlValue::Text("c".to_string()), CqlValue::Text("d".to_string())]),
            ),
        ];
        let map_val = CqlValue::Map(map_items);

        let value_type = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Text))),
        };
        let map_type = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(Box::new(ColumnType::Native(NativeType::Text)), Box::new(value_type)),
        };

        let result = convert_cql_value_to_json(&map_val, &map_type).unwrap();
        if let Value::Object(obj) = result {
            assert_eq!(obj.len(), 2);

            if let Value::Array(list1) = obj.get("list1").unwrap() {
                assert_eq!(list1.len(), 2);
                assert_eq!(list1[0], Value::String("a".to_string()));
                assert_eq!(list1[1], Value::String("b".to_string()));
            } else {
                panic!("Expected Array for list1");
            }

            if let Value::Array(list2) = obj.get("list2").unwrap() {
                assert_eq!(list2.len(), 2);
                assert_eq!(list2[0], Value::String("c".to_string()));
                assert_eq!(list2[1], Value::String("d".to_string()));
            } else {
                panic!("Expected Array for list2");
            }
        } else {
            panic!("Expected Object, got {:?}", result);
        }
    }

    #[test]
    fn test_error_cases() {
        // Test type mismatch
        let text_val = CqlValue::Text("not a number".to_string());
        let result = convert_cql_value_to_json(&text_val, &ColumnType::Native(NativeType::Int));
        assert!(matches!(result, Err(CassandraValueError::TypeMismatch { .. })));

        // Test NaN/Infinity
        let nan_val = CqlValue::Float(f32::NAN);
        let result = convert_cql_value_to_json(&nan_val, &ColumnType::Native(NativeType::Float));
        assert!(matches!(result, Err(CassandraValueError::InvalidFormat { .. })));

        let inf_val = CqlValue::Double(f64::INFINITY);
        let result = convert_cql_value_to_json(&inf_val, &ColumnType::Native(NativeType::Double));
        assert!(matches!(result, Err(CassandraValueError::InvalidFormat { .. })));

        // Test UDT with missing field
        let fields = vec![("unknown_field".to_string(), Some(CqlValue::Text("test".to_string())))];
        let invalid_udt = CqlValue::UserDefinedType {
            name: "test_type".to_string(),
            keyspace: "test_keyspace".to_string(),
            fields,
        };

        let (_, valid_udt_type) = create_test_udt();
        let result = convert_cql_value_to_json(&invalid_udt, &valid_udt_type);
        assert!(matches!(result, Err(CassandraValueError::TypeMismatch { .. })));
    }

    #[test]
    fn test_empty_and_null_values() {
        // Test Empty value
        let empty_val = CqlValue::Empty;
        let result = convert_column_to_json("test_col", &empty_val, &ColumnType::Native(NativeType::Text)).unwrap();
        assert_eq!(result, None);

        // Test None in a tuple
        let tuple_items = vec![Some(CqlValue::Text("text".to_string())), None, Some(CqlValue::Boolean(true))];

        // Create tuple with a compatible signature
        let tuple_val = match CqlValue::Tuple(tuple_items) {
            t => t,
            #[allow(unreachable_patterns)]
            _ => {
                println!("Warning: Tuple with nulls test skipped due to API mismatch");
                return;
            }
        };

        let tuple_types = vec![
            ColumnType::Native(NativeType::Text),
            ColumnType::Native(NativeType::Int),
            ColumnType::Native(NativeType::Boolean),
        ];
        let tuple_type = ColumnType::Tuple(tuple_types);

        let result = convert_cql_value_to_json(&tuple_val, &tuple_type).unwrap();
        if let Value::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Value::String("text".to_string()));
            assert_eq!(arr[1], Value::Null);
            assert_eq!(arr[2], Value::Bool(true));
        } else {
            panic!("Expected Array, got {:?}", result);
        }

        // Test None in a UDT
        let fields = vec![
            ("name".to_string(), Some(CqlValue::Text("test_name".to_string()))),
            ("age".to_string(), None),
            ("active".to_string(), Some(CqlValue::Boolean(true))),
        ];

        let udt_with_null = CqlValue::UserDefinedType {
            name: "test_type".to_string(),
            keyspace: "test_keyspace".to_string(),
            fields,
        };
        let (_, udt_type) = create_test_udt();

        let result = convert_cql_value_to_json(&udt_with_null, &udt_type).unwrap();
        if let Value::Object(obj) = result {
            assert_eq!(obj.len(), 3);
            assert_eq!(obj.get("name").unwrap(), &Value::String("test_name".to_string()));
            assert_eq!(obj.get("age").unwrap(), &Value::Null);
            assert_eq!(obj.get("active").unwrap(), &Value::Bool(true));
        } else {
            panic!("Expected Object, got {:?}", result);
        }
    }

    #[test]
    fn test_partial_tuple() {
        // Test partially filled tuple (fewer values than types)
        let tuple_items = vec![Some(CqlValue::Text("only_item".to_string()))];

        // Create tuple with a compatible signature
        let tuple_val = match CqlValue::Tuple(tuple_items) {
            t => t,
            #[allow(unreachable_patterns)]
            _ => {
                println!("Warning: Partial tuple test skipped due to API mismatch");
                return;
            }
        };

        // Define a tuple type with more elements than we have values
        let tuple_types = vec![
            ColumnType::Native(NativeType::Text),
            ColumnType::Native(NativeType::Int),
            ColumnType::Native(NativeType::Boolean),
        ];
        let tuple_type = ColumnType::Tuple(tuple_types);

        let result = convert_cql_value_to_json(&tuple_val, &tuple_type).unwrap();
        if let Value::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Value::String("only_item".to_string()));
            assert_eq!(arr[1], Value::Null); // Missing values should be null
            assert_eq!(arr[2], Value::Null); // Missing values should be null
        } else {
            panic!("Expected Array, got {:?}", result);
        }
    }

    #[test]
    fn test_map_with_non_string_keys() {
        // Test map with integer keys (should be converted to strings)
        let map_items = vec![
            (CqlValue::Int(1), CqlValue::Text("value1".to_string())),
            (CqlValue::Int(2), CqlValue::Text("value2".to_string())),
        ];
        let map_val = CqlValue::Map(map_items);

        let map_type = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(Box::new(ColumnType::Native(NativeType::Int)), Box::new(ColumnType::Native(NativeType::Text))),
        };

        let result = convert_cql_value_to_json(&map_val, &map_type).unwrap();
        if let Value::Object(obj) = result {
            assert_eq!(obj.len(), 2);
            assert_eq!(obj.get("1").unwrap(), &Value::String("value1".to_string()));
            assert_eq!(obj.get("2").unwrap(), &Value::String("value2".to_string()));
        } else {
            panic!("Expected Object, got {:?}", result);
        }
    }

    #[test]
    fn test_udt_with_collection_fields() {
        use std::borrow::Cow;

        // Create a UDT with collection fields
        let list_val = CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)]);

        let fields = vec![
            ("name".to_string(), Some(CqlValue::Text("test_name".to_string()))),
            ("tags".to_string(), Some(list_val)),
        ];

        let udt_val = CqlValue::UserDefinedType {
            name: "test_type".to_string(),
            keyspace: "test_keyspace".to_string(),
            fields,
        };
        let field_types = vec![
            (Cow::Borrowed("name"), ColumnType::Native(NativeType::Text)),
            (
                Cow::Borrowed("tags"),
                ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int))),
                },
            ),
        ];

        let udt_type = ColumnType::UserDefinedType {
            frozen: false,
            definition: Arc::new(UserDefinedType {
                name: Cow::Borrowed("test_type"),
                keyspace: Cow::Borrowed("test_keyspace"),
                field_types,
            }),
        };

        let result = convert_cql_value_to_json(&udt_val, &udt_type).unwrap();
        if let Value::Object(obj) = result {
            assert_eq!(obj.len(), 2);
            assert_eq!(obj.get("name").unwrap(), &Value::String("test_name".to_string()));

            if let Value::Array(tags) = obj.get("tags").unwrap() {
                assert_eq!(tags.len(), 2);
                assert_eq!(tags[0], Value::Number(Number::from(1)));
                assert_eq!(tags[1], Value::Number(Number::from(2)));
            } else {
                panic!("Expected Array for tags, got {:?}", obj.get("tags").unwrap());
            }
        } else {
            panic!("Expected Object, got {:?}", result);
        }
    }

    #[test]
    fn test_empty_collections() {
        // Test empty list
        let empty_list = CqlValue::List(vec![]);
        let result = convert_cql_value_to_json(
            &empty_list,
            &ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int))),
            },
        )
        .unwrap();
        assert_eq!(result, Value::Array(vec![]));

        // Test empty set
        let empty_set = CqlValue::Set(vec![]);
        let result = convert_cql_value_to_json(
            &empty_set,
            &ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Text))),
            },
        )
        .unwrap();
        assert_eq!(result, Value::Array(vec![]));

        // Test empty map
        let empty_map = CqlValue::Map(vec![]);
        let map_type = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(Box::new(ColumnType::Native(NativeType::Text)), Box::new(ColumnType::Native(NativeType::Int))),
        };
        let result = convert_cql_value_to_json(&empty_map, &map_type).unwrap();
        assert_eq!(result, Value::Object(Map::new()));
    }

    #[test]
    fn test_overflow_detection() {
        // Test bigint overflow in JSON (this should work as we handle it gracefully)
        let max_bigint = CqlValue::BigInt(i64::MAX);
        let result = convert_cql_value_to_json(&max_bigint, &ColumnType::Native(NativeType::BigInt)).unwrap();
        assert!(matches!(result, Value::Number(_)));
    }

    #[test]
    fn test_is_supported_type() {
        // Test all the supported types
        assert!(is_supported_type(&ColumnType::Native(NativeType::Int)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::Text)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::Boolean)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::Blob)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::BigInt)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::Counter)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::Date)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::Decimal)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::Double)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::Float)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::Inet)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::SmallInt)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::TinyInt)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::Time)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::Timestamp)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::Timeuuid)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::Uuid)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::Varint)));
        assert!(is_supported_type(&ColumnType::Native(NativeType::Duration)));
        assert!(is_supported_type(&ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int)))
        }));
        assert!(is_supported_type(&ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Text)))
        }));
        assert!(is_supported_type(&ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(Box::new(ColumnType::Native(NativeType::Text)), Box::new(ColumnType::Native(NativeType::Int)))
        }));
        assert!(is_supported_type(&ColumnType::Tuple(vec![
            ColumnType::Native(NativeType::Text),
            ColumnType::Native(NativeType::Int)
        ])));

        // Test a complex nested type
        let nested_type = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Text)),
                Box::new(ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::List(Box::new(ColumnType::Collection {
                        frozen: false,
                        typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Int))),
                    })),
                }),
            ),
        };
        assert!(is_supported_type(&nested_type));

        // Test custom type
        // assert!(is_supported_type(&ColumnType::Custom(Cow::Borrowed(
        //     "com.example.custom"
        // ))));
    }

    #[test]
    fn test_cql_value_to_string() {
        // Test primitives
        assert_eq!(
            cql_value_to_string(&CqlValue::Text("text".to_string()), &ColumnType::Native(NativeType::Text)).unwrap(),
            "text"
        );
        assert_eq!(cql_value_to_string(&CqlValue::Int(42), &ColumnType::Native(NativeType::Int)).unwrap(), "42");
        assert_eq!(
            cql_value_to_string(&CqlValue::Boolean(true), &ColumnType::Native(NativeType::Boolean)).unwrap(),
            "true"
        );

        // Test special types
        let uuid_str = "f3b4958c-52a1-11e7-802a-010203040506";
        let uuid = Uuid::parse_str(uuid_str).unwrap();
        assert_eq!(cql_value_to_string(&CqlValue::Uuid(uuid), &ColumnType::Native(NativeType::Uuid)).unwrap(), uuid_str);

        // Test floats
        assert!(
            cql_value_to_string(&CqlValue::Float(std::f32::consts::PI), &ColumnType::Native(NativeType::Float))
                .unwrap()
                .starts_with("3.14")
        );

        // Test invalid conversions
        let result = cql_value_to_string(&CqlValue::Float(f32::NAN), &ColumnType::Native(NativeType::Float));
        assert!(matches!(result, Err(CassandraValueError::InvalidFormat { .. })));

        // Test unsupported types for string conversion
        let result = cql_value_to_string(
            &CqlValue::List(vec![CqlValue::Int(1)]),
            &ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int))),
            },
        );

        assert!(matches!(result, Err(CassandraValueError::InvalidFormat { .. })));
    }
}
