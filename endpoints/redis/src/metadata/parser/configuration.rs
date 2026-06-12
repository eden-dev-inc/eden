use std::collections::HashMap;

use crate::command;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_types::metadata::CapabilityChecker;
use error::{EpError, ResultEP};
use function_name::named;
use redis::{FromRedisValue, Value};
use redis_core::RedisAsync;
use telemetry::TelemetryWrapper;

use super::common::{execute_command_raw, mark_endpoint_response, value_to_map};

#[named]
pub async fn load_configuration_info(
    context: RedisAsync,
    telemetry: &mut TelemetryWrapper,
    _capabilities: &dyn CapabilityChecker,
) -> ResultEP<crate::metadata::stc::config::RedisConfigInfo> {
    use crate::metadata::stc::config::RedisConfigInfo;
    let ctx = ctx_with_trace!();

    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    let mut command = command::cmd("CONFIG");
    command.arg("GET").arg("*");

    let response: Value = execute_command_raw(&mut connection, command).await?;

    mark_endpoint_response(telemetry);

    let mut map = HashMap::new();

    if let Some(entries) = value_to_map(&response) {
        for (key, value) in entries {
            let value_str = match String::from_redis_value(&value) {
                Ok(s) => s,
                Err(_) => format!("{value:?}"),
            };
            map.insert(key, value_str);
        }
    } else {
        return Err(EpError::metadata("Unexpected CONFIG GET response format"));
    }

    let config_info = RedisConfigInfo::new(map);

    if let Some(maxmemory) = config_info.config.get("maxmemory")
        && maxmemory == "0"
    {
        log_warn!(
            ctx.clone(),
            "Redis maxmemory is set to 0 - no memory limit enforced",
            audience = LogAudience::Internal
        );
    }

    if let Some(save_config) = config_info.config.get("save")
        && save_config.is_empty()
    {
        log_warn!(
            ctx.clone(),
            "Redis persistence is disabled - no save configuration",
            audience = LogAudience::Internal
        );
    }

    if let Some(protected_mode) = config_info.config.get("protected-mode")
        && protected_mode != "yes"
    {
        log_warn!(ctx, "Redis protected mode is disabled - potential security risk", audience = LogAudience::Internal);
    }

    Ok(config_info)
}
