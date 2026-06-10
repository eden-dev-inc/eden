use endpoint_types::metadata::CapabilityChecker;
use error::ResultEP;
use redis_core::RedisAsync;
use telemetry::TelemetryWrapper;

use super::common::{InfoMap, assign_defaults, fetch_info_section};

pub async fn load_cpu_info(
    context: RedisAsync,
    telemetry: &mut TelemetryWrapper,
    _capabilities: &dyn CapabilityChecker,
) -> ResultEP<crate::metadata::stc::cpu::RedisCpuInfo> {
    use crate::metadata::stc::cpu::RedisCpuInfo;

    let section = fetch_info_section(context, telemetry, "cpu").await?;
    let info_map = InfoMap::new(&section.map);

    let mut cpu = RedisCpuInfo::default();
    assign_defaults!(
        cpu,
        info_map,
        used_cpu_sys,
        used_cpu_user,
        used_cpu_sys_children,
        used_cpu_user_children,
        used_cpu_sys_main_thread,
        used_cpu_user_main_thread
    );

    Ok(cpu)
}
