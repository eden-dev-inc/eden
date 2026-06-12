use std::collections::HashMap;
use std::str::FromStr;

use crate::command::{self, Cmd};
use chrono::Utc;
use deadpool::managed::Object;
use error::{EpError, ResultEP};
use redis::{FromRedisValue, RedisError, Value, parse_redis_value};
use redis_core::{RedisAsync, RedisConnectionManager, RespResponse};
use telemetry::TelemetryWrapper;

pub(crate) struct InfoSection {
    pub(crate) raw: String,
    pub(crate) map: HashMap<String, String>,
}

pub(crate) async fn fetch_info_section(context: RedisAsync, telemetry: &mut TelemetryWrapper, section: &str) -> ResultEP<InfoSection> {
    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    let mut command = command::cmd("INFO");
    if !section.is_empty() {
        command.arg(section);
    }

    let raw: String = execute_command(&mut connection, command).await?;

    mark_endpoint_response(telemetry);

    Ok(InfoSection { map: parse_info(&raw), raw })
}

pub(crate) fn parse_bool(value: Option<&String>) -> bool {
    parse_bool_opt(value).unwrap_or(false)
}

pub(crate) fn parse_bool_opt(value: Option<&String>) -> Option<bool> {
    value.map(|v| v.to_ascii_lowercase()).and_then(|v| match v.as_str() {
        "yes" | "true" | "1" => Some(true),
        "no" | "false" | "0" => Some(false),
        _ => None,
    })
}

pub(crate) fn parse_default<T>(value: Option<&String>) -> T
where
    T: Default + FromStr,
{
    value.and_then(|v| v.parse::<T>().ok()).unwrap_or_default()
}

pub(crate) fn parse_required<T>(value: Option<&String>, field_name: &str) -> ResultEP<T>
where
    T: FromStr,
{
    match value {
        Some(v) => v.parse::<T>().map_err(|_| EpError::metadata(format!("Invalid {} value: {}", field_name, v))),
        None => Err(EpError::metadata(format!("Missing critical field: {}", field_name))),
    }
}

pub(crate) fn mark_endpoint_response(telemetry: &mut TelemetryWrapper) {
    telemetry.set_endpoint_request_end(Utc::now());
}

pub(crate) async fn execute_command<T: FromRedisValue>(connection: &mut Object<RedisConnectionManager>, cmd: Cmd) -> ResultEP<T> {
    let value = execute_command_raw(connection, cmd).await?;
    from_redis_value::<T>(value)
}

pub(crate) async fn execute_command_raw(connection: &mut Object<RedisConnectionManager>, cmd: Cmd) -> ResultEP<Value> {
    let bytes = cmd.get_packed_command();
    let (response, _latency) = connection.send_command_raw(&bytes).await?;
    resp_response_to_value(response)
}

fn resp_response_to_value(resp: RespResponse) -> ResultEP<Value> {
    let bytes = resp.to_bytes();
    parse_redis_value(&bytes).map_err(|err| EpError::database(err.to_string()))
}

fn from_redis_value<T: FromRedisValue>(value: Value) -> ResultEP<T> {
    T::from_redis_value(&value).map_err(redis_error_to_ep)
}

fn redis_error_to_ep(err: RedisError) -> EpError {
    EpError::parse_redis_error(err)
}

pub(crate) fn parse_u64_with_fallback(map: &HashMap<String, String>, primary: &str, fallbacks: &[&str]) -> ResultEP<u64> {
    if let Some(value) = map.get(primary) {
        return value.parse::<u64>().map_err(|_| EpError::metadata(format!("Invalid {} value: {}", primary, value)));
    }

    for key in fallbacks {
        if let Some(value) = map.get(*key) {
            return value.parse::<u64>().map_err(|_| EpError::metadata(format!("Invalid {} value: {}", key, value)));
        }
    }

    Err(EpError::metadata(format!("Missing critical field: {}", primary)))
}

pub(crate) fn parse_percent(value: Option<&String>) -> Option<f64> {
    value.and_then(|raw| {
        let trimmed = raw.trim_end_matches('%').trim();
        trimmed.parse::<f64>().ok()
    })
}

#[allow(dead_code)]
pub(crate) fn parse_percent_required(value: Option<&String>, field: &str) -> ResultEP<f64> {
    parse_percent(value).ok_or_else(|| EpError::metadata(format!("Invalid {} value: {:?}", field, value)))
}

pub(crate) fn parse_host_and_port(address: &str) -> ResultEP<(String, u16)> {
    let main = address.split('@').next().unwrap_or(address);

    if let Some(end) = main.find(']') {
        // IPv6 formatted as [address]:port
        let host = main[1..end].to_string();
        let port_str = main[end + 1..].trim_start_matches(':');
        let port = port_str.parse::<u16>().map_err(|_| EpError::metadata(format!("Invalid Redis node port in address '{}'", address)))?;
        return Ok((host, port));
    }

    if let Some((host, port_str)) = main.rsplit_once(':') {
        let port = port_str.parse::<u16>().map_err(|_| EpError::metadata(format!("Invalid Redis node port in address '{}'", address)))?;
        return Ok((host.to_string(), port));
    }

    Err(EpError::metadata(format!("Invalid Redis node address '{}'", address)))
}

/// Thin helper around INFO maps to reduce repetitive parsing boilerplate.
pub(crate) struct InfoMap<'a> {
    map: &'a HashMap<String, String>,
}

impl<'a> InfoMap<'a> {
    pub fn new(map: &'a HashMap<String, String>) -> Self {
        Self { map }
    }

    pub fn req<T>(&self, field: &str) -> ResultEP<T>
    where
        T: FromStr,
    {
        parse_required(self.map.get(field), field)
    }

    pub fn opt<T>(&self, field: &str) -> Option<T>
    where
        T: FromStr,
    {
        self.map.get(field).and_then(|v| v.parse::<T>().ok())
    }

    pub fn default<T>(&self, field: &str) -> T
    where
        T: FromStr + Default,
    {
        parse_default(self.map.get(field))
    }

    pub fn bool(&self, field: &str) -> bool {
        parse_bool(self.map.get(field))
    }

    pub fn bool_opt(&self, field: &str) -> Option<bool> {
        parse_bool_opt(self.map.get(field))
    }

    #[allow(dead_code)]
    pub fn percent(&self, field: &str) -> Option<f64> {
        parse_percent(self.map.get(field))
    }

    #[allow(dead_code)]
    pub fn percent_required(&self, field: &str) -> ResultEP<f64> {
        parse_percent_required(self.map.get(field), field)
    }

    pub fn u64_with_fallback(&self, primary: &str, fallbacks: &[&str]) -> ResultEP<u64> {
        parse_u64_with_fallback(self.map, primary, fallbacks)
    }
}

pub(crate) fn value_to_vec(value: &Value) -> Option<Vec<Value>> {
    match value {
        Value::Array(values) => Some(values.clone()),
        _ => None,
    }
}

pub(crate) fn value_to_map(value: &Value) -> Option<HashMap<String, Value>> {
    match value {
        Value::Map(entries) => {
            let mut map = HashMap::with_capacity(entries.len());
            for (key, val) in entries {
                if let Ok(key_str) = String::from_redis_value(key) {
                    map.insert(key_str, val.clone());
                }
            }
            Some(map)
        }
        Value::Array(values) => {
            let mut map = HashMap::new();
            let mut iter = values.iter();
            while let (Some(key), Some(val)) = (iter.next(), iter.next()) {
                if let Ok(key_str) = String::from_redis_value(key) {
                    map.insert(key_str, val.clone());
                }
            }
            Some(map)
        }
        _ => None,
    }
}

pub(crate) fn value_as_u64(value: &Value) -> Option<u64> {
    u64::from_redis_value(value).ok()
}

pub(crate) fn value_as_f64(value: &Value) -> Option<f64> {
    f64::from_redis_value(value).ok()
}

pub(crate) fn value_as_string(value: &Value) -> Option<String> {
    String::from_redis_value(value).ok()
}

fn parse_info(text: &str) -> HashMap<String, String> {
    text.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            line.split_once(':').map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
        })
        .collect()
}

/// Utility macro to populate many fields from an `InfoMap` using `default()`.
macro_rules! assign_defaults {
    ($target:ident, $map:expr, $( $field:ident ),+ $(,)?) => {
        $( $target.$field = $map.default(stringify!($field)); )+
    };
}

pub(crate) use assign_defaults;

#[allow(dead_code)]
pub fn is_cluster_error(err: &EpError) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("cluster") && (msg.contains("not enabled") || msg.contains("disabled"))
}

#[allow(dead_code)]
pub fn is_module_error(err: &EpError) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("unknown command") || msg.contains("no such module")
}
