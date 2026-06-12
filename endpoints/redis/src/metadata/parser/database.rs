use endpoint_types::metadata::CapabilityChecker;
use error::ResultEP;
use redis_core::RedisAsync;
use telemetry::TelemetryWrapper;

use super::common::fetch_info_section;

pub async fn load_database_stats(
    context: RedisAsync,
    telemetry: &mut TelemetryWrapper,
    _capabilities: &dyn CapabilityChecker,
) -> ResultEP<Vec<crate::metadata::stc::database::RedisDatabaseStats>> {
    use crate::metadata::stc::database::RedisDatabaseStats;

    let section = fetch_info_section(context, telemetry, "keyspace").await?;
    let mut stats = Vec::new();

    for line in section.raw.lines() {
        if !line.starts_with("db") {
            continue;
        }
        if let Some((db_name, payload)) = line.split_once(':') {
            let mut keys = 0_u64;
            let mut expires = 0_u64;
            let mut avg_ttl = 0_u64;
            for part in payload.split(',') {
                if let Some((k, v)) = part.split_once('=') {
                    match k.trim() {
                        "keys" => keys = v.trim().parse().unwrap_or_default(),
                        "expires" => expires = v.trim().parse().unwrap_or_default(),
                        "avg_ttl" => {
                            avg_ttl = v.trim().parse().unwrap_or_default();
                        }
                        _ => {}
                    }
                }
            }

            if let Ok(id) = db_name.trim_start_matches("db").parse() {
                stats.push(RedisDatabaseStats { db_id: id, keys, expires, avg_ttl });
            }
        }
    }

    Ok(stats)
}
