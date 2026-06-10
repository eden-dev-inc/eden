use std::collections::HashMap;

use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_types::metadata::CapabilityChecker;
use error::{EpError, ResultEP};
use function_name::named;
use redis_core::RedisAsync;
use telemetry::TelemetryWrapper;

use super::common::{InfoMap, assign_defaults, fetch_info_section, parse_bool};

pub async fn load_memory_info(
    context: RedisAsync,
    telemetry: &mut TelemetryWrapper,
    _capabilities: &dyn CapabilityChecker,
) -> ResultEP<crate::metadata::stc::memory::RedisMemoryInfo> {
    let section = fetch_info_section(context, telemetry, "memory").await?;
    let map = section.map;
    let info = build_memory_info(&map)?;

    Ok(info)
}

#[named]
fn validate_memory_info_consistency(info: &crate::metadata::stc::memory::RedisMemoryInfo) -> ResultEP<()> {
    let ctx = ctx_with_trace!();
    if info.used_memory_rss < info.used_memory {
        return Err(EpError::metadata(format!(
            "RSS memory ({}) is less than used memory ({})",
            info.used_memory_rss, info.used_memory
        )));
    }

    if info.mem_fragmentation_ratio > 0.0 {
        let expected_ratio = info.used_memory_rss as f64 / info.used_memory as f64;
        let diff = (info.mem_fragmentation_ratio - expected_ratio).abs();
        if diff > 0.1 {
            log_warn!(
                ctx,
                format!(
                    "Fragmentation ratio mismatch: reported {:.2}, calculated {:.2}",
                    info.mem_fragmentation_ratio, expected_ratio
                ),
                audience = LogAudience::Internal
            );
        }
    }

    Ok(())
}

fn build_memory_info(map: &HashMap<String, String>) -> ResultEP<crate::metadata::stc::memory::RedisMemoryInfo> {
    use crate::metadata::stc::memory::RedisMemoryInfo;

    let accessor = InfoMap::new(map);

    let used_memory = accessor.req::<u64>("used_memory")?;
    let used_memory_rss = accessor.req::<u64>("used_memory_rss")?;

    let mut info = RedisMemoryInfo { used_memory, used_memory_rss, ..Default::default() };
    assign_defaults!(
        info,
        accessor,
        used_memory_human,
        used_memory_rss_human,
        used_memory_peak,
        used_memory_peak_human,
        used_memory_peak_perc,
        used_memory_overhead,
        used_memory_startup,
        used_memory_dataset,
        used_memory_dataset_perc,
        total_system_memory,
        total_system_memory_human,
        used_memory_lua,
        used_memory_vm_eval,
        used_memory_lua_human,
        used_memory_scripts_eval,
        number_of_cached_scripts,
        number_of_functions,
        number_of_libraries,
        used_memory_vm_functions,
        used_memory_vm_total,
        used_memory_vm_total_human,
        used_memory_functions,
        used_memory_scripts,
        used_memory_scripts_human,
        maxmemory,
        maxmemory_human,
        maxmemory_policy,
        mem_fragmentation_ratio,
        mem_fragmentation_bytes,
        allocator_frag_ratio,
        allocator_frag_bytes,
        allocator_rss_ratio,
        allocator_rss_bytes,
        rss_overhead_ratio,
        rss_overhead_bytes,
        allocator_allocated,
        allocator_active,
        allocator_resident,
        allocator_muzzy,
        mem_not_counted_for_evict,
        mem_clients_slaves,
        mem_clients_normal,
        mem_cluster_links,
        mem_aof_buffer,
        mem_replication_backlog,
        mem_total_replication_buffers,
        mem_allocator,
        mem_overhead_db_hashtable_rehashing,
        lazyfree_pending_objects,
        lazyfreed_objects
    );
    info.active_defrag_running = parse_bool(map.get("active_defrag_running"));

    validate_memory_info_consistency(&info)?;

    Ok(info)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_memory_info_populates_all_fields() {
        let mut map = HashMap::new();
        map.insert("used_memory".to_string(), "1000".to_string());
        map.insert("used_memory_human".to_string(), "1000B".to_string());
        map.insert("used_memory_rss".to_string(), "1500".to_string());
        map.insert("used_memory_rss_human".to_string(), "1500B".to_string());
        map.insert("used_memory_peak".to_string(), "2000".to_string());
        map.insert("used_memory_peak_human".to_string(), "2000B".to_string());
        map.insert("used_memory_peak_perc".to_string(), "70%".to_string());
        map.insert("used_memory_overhead".to_string(), "300".to_string());
        map.insert("used_memory_startup".to_string(), "400".to_string());
        map.insert("used_memory_dataset".to_string(), "500".to_string());
        map.insert("used_memory_dataset_perc".to_string(), "50%".to_string());
        map.insert("total_system_memory".to_string(), "4000".to_string());
        map.insert("total_system_memory_human".to_string(), "4K".to_string());
        map.insert("used_memory_lua".to_string(), "160".to_string());
        map.insert("used_memory_vm_eval".to_string(), "170".to_string());
        map.insert("used_memory_lua_human".to_string(), "160B".to_string());
        map.insert("used_memory_scripts_eval".to_string(), "180".to_string());
        map.insert("number_of_cached_scripts".to_string(), "5".to_string());
        map.insert("number_of_functions".to_string(), "6".to_string());
        map.insert("number_of_libraries".to_string(), "7".to_string());
        map.insert("used_memory_vm_functions".to_string(), "190".to_string());
        map.insert("used_memory_vm_total".to_string(), "360".to_string());
        map.insert("used_memory_vm_total_human".to_string(), "360B".to_string());
        map.insert("used_memory_functions".to_string(), "200".to_string());
        map.insert("used_memory_scripts".to_string(), "380".to_string());
        map.insert("used_memory_scripts_human".to_string(), "380B".to_string());
        map.insert("maxmemory".to_string(), "3600".to_string());
        map.insert("maxmemory_human".to_string(), "3.5K".to_string());
        map.insert("maxmemory_policy".to_string(), "allkeys-lru".to_string());
        map.insert("mem_fragmentation_ratio".to_string(), "1.5".to_string());
        map.insert("mem_fragmentation_bytes".to_string(), "500".to_string());
        map.insert("allocator_frag_ratio".to_string(), "1.1".to_string());
        map.insert("allocator_frag_bytes".to_string(), "210".to_string());
        map.insert("allocator_rss_ratio".to_string(), "1.2".to_string());
        map.insert("allocator_rss_bytes".to_string(), "-220".to_string());
        map.insert("rss_overhead_ratio".to_string(), "1.3".to_string());
        map.insert("rss_overhead_bytes".to_string(), "230".to_string());
        map.insert("allocator_allocated".to_string(), "240".to_string());
        map.insert("allocator_active".to_string(), "250".to_string());
        map.insert("allocator_resident".to_string(), "260".to_string());
        map.insert("allocator_muzzy".to_string(), "270".to_string());
        map.insert("mem_not_counted_for_evict".to_string(), "280".to_string());
        map.insert("mem_clients_slaves".to_string(), "290".to_string());
        map.insert("mem_clients_normal".to_string(), "300".to_string());
        map.insert("mem_cluster_links".to_string(), "310".to_string());
        map.insert("mem_aof_buffer".to_string(), "320".to_string());
        map.insert("mem_replication_backlog".to_string(), "330".to_string());
        map.insert("mem_total_replication_buffers".to_string(), "340".to_string());
        map.insert("mem_allocator".to_string(), "jemalloc-5.1.0".to_string());
        map.insert("mem_overhead_db_hashtable_rehashing".to_string(), "350".to_string());
        map.insert("active_defrag_running".to_string(), "yes".to_string());
        map.insert("lazyfree_pending_objects".to_string(), "360".to_string());
        map.insert("lazyfreed_objects".to_string(), "370".to_string());

        let info = build_memory_info(&map).expect("should build memory info");

        let expected = crate::metadata::stc::memory::RedisMemoryInfo {
            used_memory: 1000,
            used_memory_human: "1000B".to_string(),
            used_memory_rss: 1500,
            used_memory_rss_human: "1500B".to_string(),
            used_memory_peak: 2000,
            used_memory_peak_human: "2000B".to_string(),
            used_memory_peak_perc: "70%".to_string(),
            used_memory_overhead: 300,
            used_memory_startup: 400,
            used_memory_dataset: 500,
            used_memory_dataset_perc: "50%".to_string(),
            total_system_memory: 4000,
            total_system_memory_human: "4K".to_string(),
            used_memory_lua: 160,
            used_memory_vm_eval: 170,
            used_memory_lua_human: "160B".to_string(),
            used_memory_scripts_eval: 180,
            number_of_cached_scripts: 5,
            number_of_functions: 6,
            number_of_libraries: 7,
            used_memory_vm_functions: 190,
            used_memory_vm_total: 360,
            used_memory_vm_total_human: "360B".to_string(),
            used_memory_functions: 200,
            used_memory_scripts: 380,
            used_memory_scripts_human: "380B".to_string(),
            maxmemory: 3600,
            maxmemory_human: "3.5K".to_string(),
            maxmemory_policy: "allkeys-lru".to_string(),
            mem_fragmentation_ratio: 1.5,
            mem_fragmentation_bytes: 500,
            allocator_frag_ratio: 1.1,
            allocator_frag_bytes: 210,
            allocator_rss_ratio: 1.2,
            allocator_rss_bytes: -220,
            rss_overhead_ratio: 1.3,
            rss_overhead_bytes: 230,
            allocator_allocated: 240,
            allocator_active: 250,
            allocator_resident: 260,
            allocator_muzzy: 270,
            mem_not_counted_for_evict: 280,
            mem_clients_slaves: 290,
            mem_clients_normal: 300,
            mem_cluster_links: 310,
            mem_aof_buffer: 320,
            mem_replication_backlog: 330,
            mem_total_replication_buffers: 340,
            mem_allocator: "jemalloc-5.1.0".to_string(),
            mem_overhead_db_hashtable_rehashing: 350,
            active_defrag_running: true,
            lazyfree_pending_objects: 360,
            lazyfreed_objects: 370,
        };

        assert_eq!(info, expected);
    }

    #[test]
    fn build_memory_info_requires_rss_field() {
        let mut map = HashMap::new();
        map.insert("used_memory".to_string(), "1024".to_string());

        let error = build_memory_info(&map).expect_err("missing rss should error");

        assert!(format!("{error}").contains("Missing critical field: used_memory_rss"), "unexpected error: {error}");
    }
}
