use std::time::{SystemTime, UNIX_EPOCH};

use crate::command;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_info, log_warn};
use endpoint_types::metadata::CapabilityChecker;
use error::{EpError, ResultEP};
use function_name::named;
use redis::{FromRedisValue, Value};
use redis_core::RedisAsync;
use telemetry::TelemetryWrapper;

use super::common::{InfoMap, assign_defaults, execute_command, fetch_info_section, mark_endpoint_response, parse_host_and_port};

use crate::metadata::stc::cluster::{ClusterNodeInfo, ClusterNodeRole, NodeHealthStatus, RedisClusterInfo, RedisSlotRange};

pub async fn load_cluster_info(
    context: RedisAsync,
    telemetry: &mut TelemetryWrapper,
    _capabilities: &dyn CapabilityChecker,
) -> ResultEP<Option<RedisClusterInfo>> {
    let section = fetch_info_section(context.clone(), telemetry, "cluster").await?;
    let map = section.map;
    let info_map = InfoMap::new(&map);

    let cluster_enabled = info_map.bool("cluster_enabled");
    if !cluster_enabled {
        return Ok(None);
    }

    let cluster_state = info_map.req::<String>("cluster_state")?;
    let cluster_slots_assigned = info_map.req::<u32>("cluster_slots_assigned")?;

    let mut info = RedisClusterInfo::default();
    info.cluster_enabled = cluster_enabled;
    info.cluster_state = cluster_state;
    info.cluster_slots_assigned = cluster_slots_assigned;
    assign_defaults!(
        info,
        info_map,
        cluster_slots_ok,
        cluster_slots_pfail,
        cluster_slots_fail,
        cluster_known_nodes,
        cluster_size,
        cluster_current_epoch,
        cluster_my_epoch,
        cluster_stats_messages_ping_sent,
        cluster_stats_messages_pong_sent,
        cluster_stats_messages_meet_sent,
        cluster_stats_messages_fail_sent,
        cluster_stats_messages_publish_sent,
        cluster_stats_messages_auth_req_sent,
        cluster_stats_messages_auth_ack_sent,
        cluster_stats_messages_sent,
        cluster_stats_messages_ping_received,
        cluster_stats_messages_pong_received,
        cluster_stats_messages_meet_received,
        cluster_stats_messages_fail_received,
        cluster_stats_messages_publish_received,
        cluster_stats_messages_auth_req_received,
        cluster_stats_messages_auth_ack_received,
        cluster_stats_messages_received,
        cluster_node_id
    );

    info.cluster_node_slots = load_cluster_slots(context.clone(), telemetry).await?;

    let cluster_nodes = load_cluster_nodes(context.clone(), telemetry).await?;
    info.cluster_nodes = cluster_nodes.clone();

    validate_and_enrich_cluster_info(&mut info, &cluster_nodes)?;

    load_cluster_health_details(context, telemetry, &mut info).await?;

    Ok(Some(info))
}

#[named]
fn validate_and_enrich_cluster_info(info: &mut RedisClusterInfo, cluster_nodes: &[ClusterNodeInfo]) -> ResultEP<()> {
    let ctx = ctx_with_trace!();
    match info.cluster_state.as_str() {
        "ok" => {
            if info.cluster_slots_fail > 0 || info.cluster_slots_pfail > 0 {
                log_warn!(
                    ctx.clone(),
                    format!(
                        "Cluster state is 'ok' but has {} failed and {} partially failed slots",
                        info.cluster_slots_fail, info.cluster_slots_pfail
                    ),
                    audience = LogAudience::Internal
                );
            }
        }
        "fail" => {
            return Err(EpError::metadata(format!(
                "Cluster is in failed state with {} failed slots",
                info.cluster_slots_fail
            )));
        }
        _ => {
            log_warn!(
                ctx.clone(),
                format!("Unknown cluster state: {}", info.cluster_state),
                audience = LogAudience::Internal
            );
        }
    }

    if info.cluster_slots_assigned != 16384 {
        log_info!(
            ctx.clone(),
            format!("Cluster has {}/16384 slots assigned", info.cluster_slots_assigned),
            audience = LogAudience::Internal
        );
    }

    let master_nodes: Vec<_> = cluster_nodes.iter().filter(|node| node.flags.contains(&"master".to_string())).collect();

    if master_nodes.is_empty() {
        return Err(EpError::metadata("Cluster has no master nodes".to_string()));
    }

    let actual_size = master_nodes.len() as u32;
    if info.cluster_size != actual_size {
        log_info!(
            ctx.clone(),
            format!("Cluster size mismatch: INFO reports {} but found {} masters", info.cluster_size, actual_size),
            audience = LogAudience::Internal
        );
        info.cluster_size = actual_size;
    }

    let total_sent = info.cluster_stats_messages_sent;
    let total_received = info.cluster_stats_messages_received;

    if total_sent > 0 && total_received > 0 {
        let ratio = total_received as f64 / total_sent as f64;
        if !(0.5..=2.0).contains(&ratio) {
            log_warn!(
                ctx,
                format!("Unusual cluster message ratio: received/sent = {:.2}", ratio),
                audience = LogAudience::Internal
            );
        }
    }

    Ok(())
}

#[named]
async fn load_cluster_slots(context: RedisAsync, telemetry: &mut TelemetryWrapper) -> ResultEP<Vec<RedisSlotRange>> {
    let ctx = ctx_with_trace!();
    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    let mut slots_cmd = command::cmd("CLUSTER");
    slots_cmd.arg("SLOTS");
    let result = execute_command::<Vec<Vec<Value>>>(&mut connection, slots_cmd).await;

    mark_endpoint_response(telemetry);

    match result {
        Ok(slots_data) => {
            let mut slot_ranges = Vec::new();

            for slot_info in slots_data {
                if slot_info.len() >= 3
                    && let (Ok(start), Ok(end)) = (u64::from_redis_value(&slot_info[0]), u64::from_redis_value(&slot_info[1]))
                {
                    let slot_range = RedisSlotRange { start: start as u16, end: end as u16 };
                    slot_ranges.push(slot_range);
                }
            }

            Ok(slot_ranges)
        }
        Err(err) => {
            log_warn!(ctx, format!("Failed to load cluster slots: {}", err), audience = LogAudience::Internal);
            Ok(Vec::new())
        }
    }
}

#[named]
async fn load_cluster_nodes(context: RedisAsync, telemetry: &mut TelemetryWrapper) -> ResultEP<Vec<ClusterNodeInfo>> {
    let ctx = ctx_with_trace!();
    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    let mut nodes_cmd = command::cmd("CLUSTER");
    nodes_cmd.arg("NODES");
    let raw: String = execute_command(&mut connection, nodes_cmd).await?;

    mark_endpoint_response(telemetry);

    let mut nodes = Vec::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        match parse_cluster_node_line(line) {
            Ok(node) => nodes.push(node),
            Err(err) => {
                log_warn!(
                    ctx.clone(),
                    format!("Failed to parse cluster node line '{}': {}", line, err),
                    audience = LogAudience::Internal
                );
            }
        }
    }

    Ok(nodes)
}

fn parse_cluster_node_line(line: &str) -> ResultEP<ClusterNodeInfo> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 8 {
        return Err(EpError::metadata(format!("Invalid cluster node line format: {}", line)));
    }

    let id = parts[0].to_string();
    let (ip, port) = parse_host_and_port(parts[1])?;

    let flags: Vec<String> = parts[2].split(',').map(|s| s.to_string()).collect();
    let master_id = if parts[3] == "-" { None } else { Some(parts[3].to_string()) };

    let ping_sent = parts[4].parse::<u64>().map_err(|_| EpError::metadata(format!("Invalid ping_sent: {}", parts[4])))?;
    let pong_recv = parts[5].parse::<u64>().map_err(|_| EpError::metadata(format!("Invalid pong_recv: {}", parts[5])))?;
    let config_epoch = parts[6].parse::<u64>().map_err(|_| EpError::metadata(format!("Invalid config_epoch: {}", parts[6])))?;
    let link_state = parts[7].to_string();

    let mut slots = Vec::new();
    for slot_part in parts.iter().skip(8) {
        if let Ok(slot_range) = parse_slot_range(slot_part) {
            slots.push(slot_range);
        }
    }

    let role = if flags.contains(&"master".to_string()) {
        ClusterNodeRole::Master
    } else if flags.contains(&"slave".to_string()) {
        ClusterNodeRole::Replica
    } else {
        ClusterNodeRole::Unknown
    };

    Ok(ClusterNodeInfo {
        id,
        ip,
        port: port as u32,
        flags,
        master_id,
        ping_sent,
        pong_recv,
        config_epoch,
        link_state,
        slots,
        health_status: NodeHealthStatus::Healthy,
        latency_ms: None,
        memory_usage_bytes: None,
        connected_clients: None,
        role,
        last_seen: None,
    })
}

fn parse_slot_range(slot_str: &str) -> ResultEP<RedisSlotRange> {
    if slot_str.contains('-') {
        let parts: Vec<&str> = slot_str.split('-').collect();
        if parts.len() == 2 {
            let start = parts[0].parse::<u16>().map_err(|_| EpError::metadata(format!("Invalid slot start: {}", parts[0])))?;
            let end = parts[1].parse::<u16>().map_err(|_| EpError::metadata(format!("Invalid slot end: {}", parts[1])))?;
            return Ok(RedisSlotRange { start, end });
        }
    } else {
        let slot = slot_str.parse::<u16>().map_err(|_| EpError::metadata(format!("Invalid slot: {}", slot_str)))?;
        return Ok(RedisSlotRange { start: slot, end: slot });
    }

    Err(EpError::metadata(format!("Invalid slot range format: {}", slot_str)))
}

async fn load_cluster_health_details(context: RedisAsync, telemetry: &mut TelemetryWrapper, info: &mut RedisClusterInfo) -> ResultEP<()> {
    {
        let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

        let mut info_cmd = command::cmd("CLUSTER");
        info_cmd.arg("INFO");
        if let Ok(health_info) = execute_command::<String>(&mut connection, info_cmd).await {
            parse_cluster_health_info(&health_info, info);
        }
    }

    for node in &mut info.cluster_nodes {
        node.health_status = determine_node_health(node);
    }

    mark_endpoint_response(telemetry);

    Ok(())
}

fn parse_cluster_health_info(health_info: &str, info: &mut RedisClusterInfo) {
    for line in health_info.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some((key, value)) = line.split_once(':') {
            match key.trim() {
                "cluster_state" => {
                    info.cluster_state = value.trim().to_string();
                }
                "cluster_size" => {
                    if let Ok(size) = value.trim().parse::<u32>() {
                        info.cluster_size = size;
                    }
                }
                "cluster_known_nodes" => {
                    if let Ok(nodes) = value.trim().parse::<u32>() {
                        info.cluster_known_nodes = nodes;
                    }
                }
                "cluster_slots_assigned" => {
                    if let Ok(slots) = value.trim().parse::<u32>() {
                        info.cluster_slots_assigned = slots;
                    }
                }
                "cluster_slots_ok" => {
                    if let Ok(slots) = value.trim().parse::<u32>() {
                        info.cluster_slots_ok = slots;
                    }
                }
                "cluster_slots_pfail" => {
                    if let Ok(slots) = value.trim().parse::<u32>() {
                        info.cluster_slots_pfail = slots;
                    }
                }
                "cluster_slots_fail" => {
                    if let Ok(slots) = value.trim().parse::<u32>() {
                        info.cluster_slots_fail = slots;
                    }
                }
                _ => {}
            }
        }
    }
}

fn determine_node_health(node: &ClusterNodeInfo) -> NodeHealthStatus {
    if node.flags.contains(&"fail".to_string()) {
        return NodeHealthStatus::Failed;
    }

    if node.flags.contains(&"handshake".to_string()) {
        return NodeHealthStatus::Joining;
    }

    if node.flags.contains(&"master".to_string()) && node.slots.is_empty() {
        return NodeHealthStatus::Initializing;
    }

    let current_time_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
    let time_since_last_pong_ms = current_time_ms.saturating_sub(node.pong_recv);

    if time_since_last_pong_ms > 30_000 {
        return NodeHealthStatus::Unreachable;
    }

    if let Some(memory_usage) = node.memory_usage_bytes
        && memory_usage > 2_000_000_000
    {
        return NodeHealthStatus::Overloaded;
    }

    NodeHealthStatus::Healthy
}
