use bit_vec::BitVec;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use error::EpError;
use serde_json::Map;
use std::net::IpAddr;
use tokio_postgres::types::{FromSql, Type};
use tokio_postgres::{Column, Row};
use uuid::Uuid;

pub type JSONValue = serde_json::Value;
pub type RowData = Map<String, JSONValue>;

pub fn postgres_row_to_row_data(row: Row) -> Result<RowData, EpError> {
    let mut result: Map<String, JSONValue> = Map::new();
    for (i, column) in row.columns().iter().enumerate() {
        let name = column.name();
        let json_value = pg_cell_to_json_value(&row, column, i)?;
        result.insert(name.to_string(), json_value);
    }
    Ok(result)
}

pub fn error(column: &Column) -> Result<JSONValue, EpError> {
    Err(EpError::data(format!(
        "Cannot convert pg-cell \"{}\" of type \"{}\" to a JSONValue.",
        column.name(),
        column.type_().name(),
    )))
}

fn f64_to_json_number(f: f64) -> JSONValue {
    serde_json::Number::from_f64(f).into()
}

pub fn pg_cell_to_json_value(row: &Row, column: &Column, column_i: usize) -> Result<JSONValue, EpError> {
    match *column.type_() {
        Type::ACLITEM => error(column),
        Type::ACLITEM_ARRAY => error(column),
        Type::ANY => error(column),
        Type::ANYARRAY => error(column),
        Type::ANYCOMPATIBLE => error(column),
        Type::ANYCOMPATIBLEARRAY => error(column),
        Type::ANYCOMPATIBLEMULTI_RANGE => error(column),
        Type::ANYCOMPATIBLENONARRAY => error(column),
        Type::ANYCOMPATIBLE_RANGE => error(column),
        Type::ANYELEMENT => error(column),
        Type::ANYENUM => error(column),
        Type::ANYMULTI_RANGE => error(column),
        Type::ANYNONARRAY => error(column),
        Type::ANY_RANGE => error(column),
        Type::BIT => get_basic(row, column, column_i, |bit: BitVec| {
            Ok(JSONValue::Array(bit.into_iter().map(serde_json::Value::Bool).collect()))
        }),
        Type::BIT_ARRAY => get_array(row, column, column_i, |bit: BitVec| {
            Ok(JSONValue::Array(bit.into_iter().map(serde_json::Value::Bool).collect()))
        }),
        Type::BOOL => get_basic(row, column, column_i, |bool: bool| Ok(JSONValue::Bool(bool))),
        Type::BOOL_ARRAY => get_array(row, column, column_i, |bool: bool| Ok(JSONValue::Bool(bool))),
        Type::BOX => error(column),
        Type::BOX_ARRAY => error(column),
        Type::BPCHAR => get_basic(row, column, column_i, |char: String| Ok(JSONValue::String(char))),
        Type::BPCHAR_ARRAY => get_array(row, column, column_i, |char: String| Ok(JSONValue::String(char))),
        Type::BYTEA => error(column),
        Type::BYTEA_ARRAY => error(column),
        Type::CHAR => get_basic(row, column, column_i, |char: String| Ok(JSONValue::String(char))),
        Type::CHAR_ARRAY => get_array(row, column, column_i, |char: String| Ok(JSONValue::String(char))),
        Type::CID => error(column),
        Type::CID_ARRAY => error(column),
        Type::CIDR => error(column),
        Type::CIDR_ARRAY => error(column),
        Type::CIRCLE => error(column),
        Type::CIRCLE_ARRAY => error(column),
        Type::CSTRING => error(column),
        Type::CSTRING_ARRAY => error(column),
        Type::DATE => get_basic(row, column, column_i, |date: NaiveDate| Ok(JSONValue::String(date.to_string()))),
        Type::DATE_ARRAY => get_array(row, column, column_i, |date: NaiveDate| Ok(JSONValue::String(date.to_string()))),
        Type::DATE_RANGE => error(column),
        Type::DATE_RANGE_ARRAY => error(column),
        Type::DATEMULTI_RANGE => error(column),
        Type::DATEMULTI_RANGE_ARRAY => error(column),
        Type::EVENT_TRIGGER => error(column),
        Type::FDW_HANDLER => error(column),
        Type::FLOAT4 => get_basic(row, column, column_i, |f: f32| Ok(f64_to_json_number(f as f64))),
        Type::FLOAT4_ARRAY => get_array(row, column, column_i, |f: f32| Ok(f64_to_json_number(f as f64))),
        Type::FLOAT8 => get_basic(row, column, column_i, |f: f64| Ok(f64_to_json_number(f))),
        Type::FLOAT8_ARRAY => get_array(row, column, column_i, |f: f64| Ok(f64_to_json_number(f))),
        Type::GTS_VECTOR => error(column),
        Type::GTS_VECTOR_ARRAY => error(column),
        Type::INDEX_AM_HANDLER => error(column),
        Type::INET => get_basic(row, column, column_i, |ip: IpAddr| Ok(JSONValue::String(ip.to_string()))),
        Type::INET_ARRAY => get_array(row, column, column_i, |ip: IpAddr| Ok(JSONValue::String(ip.to_string()))),
        Type::INT2 => get_basic(row, column, column_i, |a: i16| Ok(JSONValue::Number(serde_json::Number::from(a)))),
        Type::INT2_ARRAY => get_array(row, column, column_i, |a: i16| Ok(JSONValue::Number(serde_json::Number::from(a)))),
        Type::INT2_VECTOR => get_array(row, column, column_i, |a: i16| Ok(JSONValue::Number(serde_json::Number::from(a)))),
        Type::INT2_VECTOR_ARRAY => error(column),
        Type::INT4 => get_basic(row, column, column_i, |a: i32| Ok(JSONValue::Number(serde_json::Number::from(a)))),
        Type::INT4_ARRAY => get_array(row, column, column_i, |a: i32| Ok(JSONValue::Number(serde_json::Number::from(a)))),
        Type::INT4MULTI_RANGE => error(column),
        Type::INT4MULTI_RANGE_ARRAY => error(column),
        Type::INT4_RANGE => error(column),
        Type::INT4_RANGE_ARRAY => error(column),
        Type::INT8 => get_basic(row, column, column_i, |a: i64| Ok(JSONValue::Number(serde_json::Number::from(a)))),
        Type::INT8_ARRAY => get_array(row, column, column_i, |a: i64| Ok(JSONValue::Number(serde_json::Number::from(a)))),
        Type::INT8MULTI_RANGE => error(column),
        Type::INT8MULTI_RANGE_ARRAY => error(column),
        Type::INT8_RANGE => error(column),
        Type::INT8_RANGE_ARRAY => error(column),
        Type::INTERNAL => error(column),
        Type::INTERVAL => error(column),
        Type::INTERVAL_ARRAY => error(column),
        Type::JSON => get_basic(row, column, column_i, |json: JSONValue| Ok(json)),
        Type::JSON_ARRAY => get_array(row, column, column_i, |json: JSONValue| Ok(json)),
        Type::JSONB => get_basic(row, column, column_i, |json: JSONValue| Ok(json)),
        Type::JSONB_ARRAY => get_array(row, column, column_i, |json: JSONValue| Ok(json)),
        Type::JSONPATH => error(column),
        Type::JSONPATH_ARRAY => error(column),
        Type::LANGUAGE_HANDLER => error(column),
        Type::LINE => error(column),
        Type::LINE_ARRAY => error(column),
        Type::LSEG => error(column),
        Type::LSEG_ARRAY => error(column),
        Type::MACADDR => error(column),
        Type::MACADDR8 => error(column),
        Type::MACADDR_ARRAY => error(column),
        Type::MACADDR8_ARRAY => error(column),
        Type::MONEY => get_basic(row, column, column_i, |money: String| Ok(JSONValue::String(money))),
        Type::MONEY_ARRAY => get_array(row, column, column_i, |money: String| Ok(JSONValue::String(money))),
        Type::NAME => get_basic(row, column, column_i, |name: String| Ok(JSONValue::String(name))),
        Type::NAME_ARRAY => get_array(row, column, column_i, |name: String| Ok(JSONValue::String(name))),
        Type::NUMERIC => {
            // PostgreSQL NUMERIC needs special handling
            // Try to get as f64 first (which postgres supports for NUMERIC)
            match row.try_get::<_, Option<f64>>(column_i) {
                Ok(Some(val)) => Ok(f64_to_json_number(val)),
                Ok(None) => Ok(JSONValue::Null),
                Err(_) => {
                    // If f64 fails, try to get the raw value and convert to string
                    // This handles cases where the numeric value is outside f64 range
                    Ok(JSONValue::String(format!("NUMERIC conversion error at column {}", column.name())))
                }
            }
        }
        Type::NUMERIC_ARRAY => match row.try_get::<_, Option<Vec<f64>>>(column_i) {
            Ok(Some(vals)) => {
                let mut result = vec![];
                for val in vals {
                    match serde_json::Number::from_f64(val) {
                        Some(num) => result.push(JSONValue::Number(num)),
                        None => result.push(JSONValue::String(val.to_string())),
                    }
                }
                Ok(JSONValue::Array(result))
            }
            Ok(None) => Ok(JSONValue::Null),
            Err(_) => Ok(JSONValue::Array(vec![])),
        },
        Type::NUMMULTI_RANGE => error(column),
        Type::NUMMULTI_RANGE_ARRAY => error(column),
        Type::NUM_RANGE => error(column),
        Type::NUM_RANGE_ARRAY => error(column),
        Type::OID => get_basic(row, column, column_i, |oid: u32| Ok(JSONValue::Number(serde_json::Number::from(oid)))),
        Type::OID_ARRAY => get_array(row, column, column_i, |oid: u32| Ok(JSONValue::Number(serde_json::Number::from(oid)))),
        Type::OID_VECTOR => error(column),
        Type::OID_VECTOR_ARRAY => error(column),
        Type::PATH => error(column),
        Type::PATH_ARRAY => error(column),
        Type::PG_BRIN_BLOOM_SUMMARY => error(column),
        Type::PG_BRIN_MINMAX_MULTI_SUMMARY => error(column),
        Type::PG_DDL_COMMAND => error(column),
        Type::PG_DEPENDENCIES => error(column),
        Type::PG_LSN => error(column),
        Type::PG_LSN_ARRAY => error(column),
        Type::PG_MCV_LIST => error(column),
        Type::PG_NDISTINCT => error(column),
        Type::PG_NODE_TREE => error(column),
        Type::PG_SNAPSHOT => error(column),
        Type::PG_SNAPSHOT_ARRAY => error(column),
        Type::POINT => error(column),
        Type::POINT_ARRAY => error(column),
        Type::POLYGON => error(column),
        Type::POLYGON_ARRAY => error(column),
        Type::RECORD => error(column),
        Type::RECORD_ARRAY => error(column),
        Type::REFCURSOR => error(column),
        Type::REFCURSOR_ARRAY => error(column),
        Type::REGCLASS => error(column),
        Type::REGCLASS_ARRAY => error(column),
        Type::REGCOLLATION => error(column),
        Type::REGCOLLATION_ARRAY => error(column),
        Type::REGCONFIG => error(column),
        Type::REGCONFIG_ARRAY => error(column),
        Type::REGDICTIONARY => error(column),
        Type::REGDICTIONARY_ARRAY => error(column),
        Type::REGNAMESPACE => error(column),
        Type::REGNAMESPACE_ARRAY => error(column),
        Type::REGOPER => error(column),
        Type::REGOPERATOR => error(column),
        Type::REGOPERATOR_ARRAY => error(column),
        Type::REGOPER_ARRAY => error(column),
        Type::REGPROC => error(column),
        Type::REGPROCEDURE => error(column),
        Type::REGPROCEDURE_ARRAY => error(column),
        Type::REGPROC_ARRAY => error(column),
        Type::REGROLE => error(column),
        Type::REGROLE_ARRAY => error(column),
        Type::REGTYPE => error(column),
        Type::REGTYPE_ARRAY => error(column),
        Type::TABLE_AM_HANDLER => error(column),
        Type::TEXT => get_basic(row, column, column_i, |text: String| Ok(JSONValue::String(text))),
        Type::TEXT_ARRAY => get_array(row, column, column_i, |text: String| Ok(JSONValue::String(text))),
        Type::TID => error(column),
        Type::TID_ARRAY => error(column),
        Type::TIME => get_basic(row, column, column_i, |time: NaiveTime| Ok(JSONValue::String(time.to_string()))),
        Type::TIME_ARRAY => get_array(row, column, column_i, |time: NaiveTime| Ok(JSONValue::String(time.to_string()))),
        Type::TIMESTAMP => get_basic(row, column, column_i, |time: NaiveDateTime| Ok(JSONValue::String(time.to_string()))),
        Type::TIMESTAMP_ARRAY => get_array(row, column, column_i, |time: NaiveDateTime| Ok(JSONValue::String(time.to_string()))),
        Type::TIMESTAMPTZ => get_basic(row, column, column_i, |time: DateTime<Utc>| Ok(JSONValue::String(time.to_string()))),
        Type::TIMESTAMPTZ_ARRAY => get_array(row, column, column_i, |time: DateTime<Utc>| Ok(JSONValue::String(time.to_string()))),
        Type::TIMETZ => error(column),
        Type::TIMETZ_ARRAY => error(column),
        Type::TRIGGER => error(column),
        Type::TSMULTI_RANGE => error(column),
        Type::TSMULTI_RANGE_ARRAY => error(column),
        Type::TSM_HANDLER => error(column),
        Type::TSQUERY => error(column),
        Type::TSQUERY_ARRAY => error(column),
        Type::TSTZMULTI_RANGE => error(column),
        Type::TSTZMULTI_RANGE_ARRAY => error(column),
        Type::TSTZ_RANGE => error(column),
        Type::TSTZ_RANGE_ARRAY => error(column),
        Type::TS_RANGE => error(column),
        Type::TS_RANGE_ARRAY => error(column),
        Type::TS_VECTOR => get_basic(row, column, column_i, |s: StringCollector| Ok(JSONValue::String(s.0))),
        Type::TS_VECTOR_ARRAY => error(column),
        Type::TXID_SNAPSHOT => error(column),
        Type::TXID_SNAPSHOT_ARRAY => error(column),
        Type::UNKNOWN => error(column),
        Type::UUID => get_basic(row, column, column_i, |uuid: Uuid| Ok(JSONValue::String(uuid.to_string()))),
        Type::UUID_ARRAY => get_array(row, column, column_i, |uuid: Uuid| Ok(JSONValue::String(uuid.to_string()))),
        Type::VARBIT => get_basic(row, column, column_i, |bit: BitVec| {
            Ok(JSONValue::Array(bit.into_iter().map(serde_json::Value::Bool).collect()))
        }),
        Type::VARBIT_ARRAY => get_array(row, column, column_i, |bit: BitVec| {
            Ok(JSONValue::Array(bit.into_iter().map(serde_json::Value::Bool).collect()))
        }),
        Type::VARCHAR => get_basic(row, column, column_i, |var: String| Ok(JSONValue::String(var))),
        Type::VARCHAR_ARRAY => get_array(row, column, column_i, |var: String| Ok(JSONValue::String(var))),
        Type::VOID => error(column),
        Type::XID => error(column),
        Type::XID8 => error(column),
        Type::XID8_ARRAY => error(column),
        Type::XID_ARRAY => error(column),
        Type::XML => error(column),
        Type::XML_ARRAY => error(column),
        _ => error(column),
    }
}

fn get_basic<'a, T: FromSql<'a>>(
    row: &'a Row,
    _column: &Column,
    column_i: usize,
    val_to_json_val: impl Fn(T) -> Result<JSONValue, EpError>,
) -> Result<JSONValue, EpError> {
    let raw_val = row.try_get::<_, Option<T>>(column_i).map_err(EpError::data)?;
    raw_val.map_or(Ok(JSONValue::Null), val_to_json_val)
}
fn get_array<'a, T: FromSql<'a>>(
    row: &'a Row,
    _column: &Column,
    column_i: usize,
    val_to_json_val: impl Fn(T) -> Result<JSONValue, EpError>,
) -> Result<JSONValue, EpError> {
    let raw_val_array = row.try_get::<_, Option<Vec<T>>>(column_i).map_err(EpError::data)?;
    Ok(match raw_val_array {
        Some(val_array) => {
            let mut result = vec![];
            for val in val_array {
                result.push(val_to_json_val(val)?);
            }
            JSONValue::Array(result)
        }
        None => JSONValue::Null,
    })
}

// you can remove this section if not using TS_VECTOR (or other types requiring an intermediary `FromSQL` struct)
struct StringCollector(String);
impl FromSql<'_> for StringCollector {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<StringCollector, Box<dyn std::error::Error + Sync + Send>> {
        let result = std::str::from_utf8(raw)?;
        Ok(StringCollector(result.to_owned()))
    }
    fn accepts(_ty: &Type) -> bool {
        true
    }
}
