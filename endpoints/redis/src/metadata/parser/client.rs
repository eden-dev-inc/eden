use crate::command;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_types::metadata::CapabilityChecker;
use error::{EpError, ResultEP};
use function_name::named;
use redis_core::RedisAsync;
use telemetry::TelemetryWrapper;

use super::common::{InfoMap, assign_defaults, execute_command, fetch_info_section, mark_endpoint_response};
use crate::metadata::stc::client::{RedisClientDetail, RedisClientInfo, RedisClientType};

pub async fn load_client_info(
    context: RedisAsync,
    telemetry: &mut TelemetryWrapper,
    _capabilities: &dyn CapabilityChecker,
) -> ResultEP<RedisClientInfo> {
    let section = fetch_info_section(context.clone(), telemetry, "clients").await?;
    let info_map = InfoMap::new(&section.map);

    let connected_clients = info_map.req::<u32>("connected_clients")?;
    let maxclients = info_map.req::<u32>("maxclients")?;

    let mut info = RedisClientInfo::default();
    info.connected_clients = connected_clients;
    info.maxclients = maxclients;
    assign_defaults!(
        info,
        info_map,
        cluster_connections,
        blocked_clients,
        tracking_clients,
        pubsub_clients,
        watching_clients,
        clients_in_timeout_table,
        total_watched_keys,
        total_blocking_keys,
        total_blocking_keys_on_nokey
    );

    info.client_recent_max_input_buffer = info_map.u64_with_fallback("client_recent_max_input_buffer", &["client_recent_input_buffer"])?;
    info.client_recent_max_output_buffer =
        info_map.u64_with_fallback("client_recent_max_output_buffer", &["client_recent_output_buffer"])?;

    info.client_details = load_client_details(context, telemetry).await?;

    validate_client_info_consistency(&mut info)?;

    Ok(info)
}

#[named]
async fn load_client_details(context: RedisAsync, telemetry: &mut TelemetryWrapper) -> ResultEP<Vec<RedisClientDetail>> {
    let ctx = ctx_with_trace!();
    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    let raw: String = {
        let mut cmd = command::cmd("CLIENT");
        cmd.arg("LIST");
        execute_command(&mut connection, cmd).await?
    };

    mark_endpoint_response(telemetry);

    let mut client_details = Vec::new();
    let mut parse_errors = Vec::new();

    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        match RedisClientDetail::parse_from_line(line) {
            Ok(mut client) => {
                enhance_client_detail(&mut client, line);
                client_details.push(client);
            }
            Err(err) => {
                parse_errors.push((line.to_string(), err.clone()));
                log_warn!(
                    ctx.clone(),
                    format!("Failed to parse client line '{line}': {err}"),
                    audience = LogAudience::Internal
                );
            }
        }
    }

    if !parse_errors.is_empty() {
        log_warn!(
            ctx.clone(),
            format!("Failed to parse {} out of {} client lines", parse_errors.len(), raw.lines().count()),
            audience = LogAudience::Internal
        );

        let total_lines = raw.lines().count();
        if parse_errors.len() > total_lines / 2 {
            return Err(EpError::metadata(format!(
                "High client parsing failure rate: {}/{} lines failed",
                parse_errors.len(),
                total_lines
            )));
        }
    }

    Ok(client_details)
}

fn enhance_client_detail(client: &mut RedisClientDetail, line: &str) {
    client.total_buffer_memory = client.qbuf.saturating_add(client.omem);

    if client.client_type == RedisClientType::Normal {
        if client.sub > 0 || client.psub > 0 {
            client.client_type = RedisClientType::PubSub;
        } else if client.qbuf > 1024 * 1024 || client.omem > 1024 * 1024 {
            // retain Normal, but large buffers merit monitoring
        }
    }

    for pair in line.split_whitespace() {
        if let Some((key, value)) = pair.split_once('=') {
            match key {
                "user" => {
                    client.additional_attrs.insert("user".to_string(), value.to_string());
                }
                "redir" => {
                    client.additional_attrs.insert("redir".to_string(), value.to_string());
                }
                "resp" => {
                    client.additional_attrs.insert("resp".to_string(), value.to_string());
                }
                "lib-name" => {
                    client.additional_attrs.insert("lib_name".to_string(), value.to_string());
                }
                "lib-ver" => {
                    client.additional_attrs.insert("lib_version".to_string(), value.to_string());
                }
                _ => {}
            }
        }
    }
}

#[named]
fn validate_client_info_consistency(info: &mut RedisClientInfo) -> ResultEP<()> {
    let ctx = ctx_with_trace!();
    if !info.client_details.is_empty() {
        let actual_clients = info.client_details.len() as u32;
        if info.connected_clients != actual_clients {
            log_warn!(
                ctx.clone(),
                format!(
                    "Client count mismatch: INFO reports {} but CLIENT LIST shows {}",
                    info.connected_clients, actual_clients
                ),
                audience = LogAudience::Internal
            );
            info.connected_clients = actual_clients;
        }
    }

    if info.client_recent_max_input_buffer > 0 && info.client_recent_max_output_buffer > 0 {
        let total_buffer = info.client_recent_max_input_buffer + info.client_recent_max_output_buffer;
        if total_buffer > 100 * 1024 * 1024 {
            log_warn!(
                ctx,
                format!("Large client buffers detected: {} bytes total", total_buffer),
                audience = LogAudience::Internal
            );
        }
    }

    if info.maxclients > 0 && info.connected_clients > info.maxclients {
        return Err(EpError::metadata(format!(
            "Connected clients ({}) exceeds maxclients ({})",
            info.connected_clients, info.maxclients
        )));
    }

    Ok(())
}
