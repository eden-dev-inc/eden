use std::collections::HashMap;

use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_types::metadata::CapabilityChecker;
use error::{EpError, ResultEP};
use function_name::named;
use redis_core::RedisAsync;
use telemetry::TelemetryWrapper;

use super::common::{InfoMap, fetch_info_section, parse_percent};

pub async fn load_replication_info(
    context: RedisAsync,
    telemetry: &mut TelemetryWrapper,
    _capabilities: &dyn CapabilityChecker,
) -> ResultEP<crate::metadata::stc::replication::RedisReplicationInfo> {
    use crate::metadata::stc::replication::{RedisReplicationInfo, RedisRole};

    let section = fetch_info_section(context, telemetry, "replication").await?;
    let map = section.map;
    let info_map = InfoMap::new(&map);

    let role_str = info_map.req::<String>("role")?;
    let role = match role_str.as_str() {
        "master" => RedisRole::Master,
        "slave" => RedisRole::Slave,
        _ => return Err(EpError::metadata(format!("Invalid role: {}", role_str))),
    };

    let mut info = RedisReplicationInfo {
        role,
        connected_slaves: info_map.default("connected_slaves"),
        master_failover_state: map.get("master_failover_state").cloned(),
        master_replid: map.get("master_replid").cloned(),
        master_replid2: map.get("master_replid2").cloned(),
        master_repl_offset: Some(info_map.default("master_repl_offset")),
        second_repl_offset: Some(info_map.default("second_repl_offset")),
        repl_backlog_active: info_map.bool_opt("repl_backlog_active"),
        repl_backlog_size: Some(info_map.default("repl_backlog_size")),
        repl_backlog_first_byte_offset: Some(info_map.default("repl_backlog_first_byte_offset")),
        repl_backlog_histlen: Some(info_map.default("repl_backlog_histlen")),
        ..Default::default()
    };

    if info.role == RedisRole::Slave {
        info.master_host = map.get("master_host").cloned();
        info.master_port = parse_u16(map.get("master_port"));
        info.master_link_status = map.get("master_link_status").cloned();
        info.master_last_io_seconds_ago = Some(info_map.default("master_last_io_seconds_ago"));
        info.master_sync_in_progress = info_map.bool_opt("master_sync_in_progress");
        info.slave_read_repl_offset = Some(info_map.default("slave_read_repl_offset"));
        info.slave_repl_offset = Some(info_map.default("slave_repl_offset"));
        info.slave_priority = Some(info_map.default("slave_priority"));
        info.slave_read_only = info_map.bool_opt("slave_read_only");
        info.replica_announced = info_map.bool_opt("replica_announced");

        info.master_sync_total_bytes = Some(info_map.default("master_sync_total_bytes"));
        info.master_sync_read_bytes = Some(info_map.default("master_sync_read_bytes"));
        info.master_sync_left_bytes = Some(info_map.default("master_sync_left_bytes"));
        info.master_sync_perc = parse_percent(map.get("master_sync_perc"));
        info.master_sync_last_io_seconds_ago = Some(info_map.default("master_sync_last_io_seconds_ago"));

        info.master_link_down_since_seconds = Some(info_map.default("master_link_down_since_seconds"));
    }

    info.min_slaves_good_slaves = Some(info_map.default("min_slaves_good_slaves"));

    if info.role == RedisRole::Master {
        info.slave_replicas = parse_slave_replicas(&map);
    }

    validate_replication_info_consistency(&info)?;

    Ok(info)
}

#[named]
fn parse_slave_replicas(map: &HashMap<String, String>) -> Vec<crate::metadata::stc::replication::RedisSlaveInfo> {
    let ctx = ctx_with_trace!();
    let mut slaves = Vec::new();

    for (key, value) in map {
        if key.starts_with("slave") && !key.contains('_') {
            match crate::metadata::stc::replication::RedisSlaveInfo::parse_from_string(value) {
                Ok(slave) => slaves.push(slave),
                Err(err) => {
                    log_warn!(
                        ctx.clone(),
                        format!("Failed to parse slave info for {}: {}", key, err),
                        audience = LogAudience::Internal
                    );
                }
            }
        }
    }

    slaves
}

fn parse_u16(value: Option<&String>) -> Option<u16> {
    value.and_then(|v| v.parse().ok())
}

fn validate_replication_info_consistency(info: &crate::metadata::stc::replication::RedisReplicationInfo) -> ResultEP<()> {
    use crate::metadata::stc::replication::RedisRole;

    match info.role {
        RedisRole::Master => {
            if info.connected_slaves > 0 && info.master_repl_offset.unwrap_or(0) == 0 {
                return Err(EpError::metadata("Master has connected slaves but replication offset is 0".to_string()));
            }
        }
        RedisRole::Slave => {
            if info.master_replid.is_none() || info.master_replid.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
                return Err(EpError::metadata("Slave role but no master replication ID".to_string()));
            }
        }
    }

    if info.repl_backlog_active.unwrap_or(false) && info.repl_backlog_size.unwrap_or(0) == 0 {
        return Err(EpError::metadata("Replication backlog active but size is 0".to_string()));
    }

    Ok(())
}
