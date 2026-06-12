use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_types::metadata::CapabilityChecker;
use error::ResultEP;
use function_name::named;
use redis_core::RedisAsync;
use telemetry::TelemetryWrapper;

use super::common::{InfoMap, assign_defaults, fetch_info_section};

pub async fn load_persistence_info(
    context: RedisAsync,
    telemetry: &mut TelemetryWrapper,
    _capabilities: &dyn CapabilityChecker,
) -> ResultEP<crate::metadata::stc::persistence::RedisPersistenceInfo> {
    use crate::metadata::stc::persistence::RedisPersistenceInfo;

    let section = fetch_info_section(context, telemetry, "persistence").await?;
    let map = section.map;
    let info_map = InfoMap::new(&map);

    let mut info = RedisPersistenceInfo::default();

    info.loading = info_map.bool("loading");
    info.async_loading = info_map.bool("async_loading");
    assign_defaults!(
        info,
        info_map,
        current_cow_peak,
        current_cow_size,
        current_cow_size_age,
        current_fork_perc,
        current_save_keys_processed,
        current_save_keys_total,
        rdb_changes_since_last_save
    );
    info.rdb_bgsave_in_progress = info_map.bool("rdb_bgsave_in_progress");
    assign_defaults!(
        info,
        info_map,
        rdb_last_save_time,
        rdb_last_bgsave_status,
        rdb_last_bgsave_time_sec,
        rdb_current_bgsave_time_sec,
        rdb_last_cow_size,
        rdb_last_load_keys_expired,
        rdb_last_load_keys_loaded,
        rdb_saves
    );

    info.aof_enabled = info_map.bool("aof_enabled");
    info.aof_rewrite_in_progress = info_map.bool("aof_rewrite_in_progress");
    info.aof_rewrite_scheduled = info_map.bool("aof_rewrite_scheduled");
    assign_defaults!(
        info,
        info_map,
        aof_last_rewrite_time_sec,
        aof_current_rewrite_time_sec,
        aof_last_bgrewrite_status,
        aof_last_write_status,
        aof_last_cow_size,
        aof_rewrites
    );

    info.module_fork_in_progress = info_map.bool("module_fork_in_progress");
    assign_defaults!(info, info_map, module_fork_last_cow_size);

    info.aof_current_size = Some(info_map.default("aof_current_size"));
    info.aof_base_size = Some(info_map.default("aof_base_size"));
    info.aof_pending_rewrite = Some(info_map.bool("aof_pending_rewrite"));
    info.aof_buffer_length = Some(info_map.default("aof_buffer_length"));
    info.aof_rewrite_buffer_length = Some(info_map.default("aof_rewrite_buffer_length"));
    info.aof_pending_bio_fsync = Some(info_map.default("aof_pending_bio_fsync"));
    info.aof_delayed_fsync = Some(info_map.default("aof_delayed_fsync"));

    info.loading_start_time = Some(info_map.default("loading_start_time"));
    info.loading_total_bytes = Some(info_map.default("loading_total_bytes"));
    info.loading_rdb_used_mem = Some(info_map.default("loading_rdb_used_mem"));

    validate_persistence_info_consistency(&info)?;

    Ok(info)
}

#[named]
fn validate_persistence_info_consistency(info: &crate::metadata::stc::persistence::RedisPersistenceInfo) -> ResultEP<()> {
    let ctx = ctx_with_trace!();
    if info.loading && info.current_save_keys_total > 0 {
        let progress = if info.current_save_keys_total > 0 {
            info.current_save_keys_processed as f64 / info.current_save_keys_total as f64
        } else {
            0.0
        };

        if progress > 1.0 {
            log_warn!(
                ctx.clone(),
                format!(
                    "Loading progress exceeds 100%: {}/{} keys",
                    info.current_save_keys_processed, info.current_save_keys_total
                ),
                audience = LogAudience::Internal
            );
        }
    }

    if info.rdb_bgsave_in_progress && info.rdb_last_bgsave_status == "ok" {
        log_warn!(ctx.clone(), "Background save in progress but last status is 'ok'", audience = LogAudience::Internal);
    }

    if info.aof_enabled && info.aof_current_size.is_some() && info.aof_base_size.is_some() {
        let current = info.aof_current_size.unwrap_or(0);
        let base = info.aof_base_size.unwrap_or(0);

        if current < base {
            log_warn!(
                ctx.clone(),
                format!("AOF current size ({}) is less than base size ({})", current, base),
                audience = LogAudience::Internal
            );
        }
    }

    if info.current_fork_perc > 100.0 {
        log_warn!(
            ctx,
            format!("Fork progress exceeds 100%: {:.1}%", info.current_fork_perc),
            audience = LogAudience::Internal
        );
    }

    Ok(())
}
