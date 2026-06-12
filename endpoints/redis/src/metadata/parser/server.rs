use endpoint_types::metadata::CapabilityChecker;
use error::ResultEP;
use redis_core::RedisAsync;
use telemetry::TelemetryWrapper;

use super::common::{InfoMap, assign_defaults, fetch_info_section, parse_bool};

pub async fn load_server_info(
    context: RedisAsync,
    telemetry: &mut TelemetryWrapper,
    _capabilities: &dyn CapabilityChecker,
) -> ResultEP<crate::metadata::stc::server::RedisServerInfo> {
    use crate::metadata::stc::server::{ProcessSupervised, RedisMode, RedisServerInfo};

    let section = fetch_info_section(context, telemetry, "server").await?;
    let map = section.map;
    let info_map = InfoMap::new(&map);

    let redis_version = info_map.req::<String>("redis_version")?;
    let uptime_in_seconds = info_map.req::<u64>("uptime_in_seconds")?;

    let mut info = RedisServerInfo::default();
    info.redis_version = redis_version;
    info.redis_git_dirty = parse_bool(map.get("redis_git_dirty"));
    assign_defaults!(
        info,
        info_map,
        redis_git_sha1,
        redis_build_id,
        os,
        multiplexing_api,
        atomicvar_api,
        gcc_version,
        process_id,
        run_id,
        server_time_usec,
        lru_clock
    );

    let redis_mode_str: String = info_map.default("redis_mode");
    info.redis_mode = match redis_mode_str.as_str() {
        "cluster" => RedisMode::Cluster,
        "sentinel" => RedisMode::Sentinel,
        _ => RedisMode::Standalone,
    };

    info.arch_bits = {
        let val = info_map.default("arch_bits");
        if val == 0 { 64 } else { val }
    };

    let supervised_str: String = info_map.default("process_supervised");
    info.process_supervised = match supervised_str.as_str() {
        "upstart" => ProcessSupervised::Upstart,
        "systemd" => ProcessSupervised::Systemd,
        "unknown" => ProcessSupervised::Unknown,
        _ => ProcessSupervised::No,
    };

    info.tcp_port = {
        let val = info_map.default("tcp_port");
        if val == 0 { 6379 } else { val }
    } as u16;
    info.uptime_in_seconds = uptime_in_seconds;
    info.uptime_in_days = info_map.default::<u64>("uptime_in_days") as u32;
    info.hz = {
        let val = info_map.default("hz");
        if val == 0 { 10 } else { val }
    };
    info.configured_hz = {
        let val = info_map.default("configured_hz");
        if val == 0 { 10 } else { val }
    };
    info.executable = Some(info_map.default("executable"));
    info.config_file = Some(info_map.default("config_file"));
    info.io_threads_active = parse_bool(map.get("io_threads_active"));
    info.shutdown_in_milliseconds = Some(info_map.default("shutdown_in_milliseconds"));

    Ok(info)
}
