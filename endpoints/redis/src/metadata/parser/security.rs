use crate::command;
use eden_logger_internal::{ctx_with_trace, log_debug};
use endpoint_types::metadata::CapabilityChecker;
use error::{EpError, ResultEP};
use function_name::named;
use redis::Value;
use redis_core::RedisAsync;
use telemetry::TelemetryWrapper;

use super::common::{InfoMap, execute_command, fetch_info_section, mark_endpoint_response, value_as_string, value_as_u64, value_to_vec};
use crate::metadata::stc::security::RedisSecurityInfo;

#[named]
pub async fn load_security_info(
    context: RedisAsync,
    telemetry: &mut TelemetryWrapper,
    _capabilities: &dyn CapabilityChecker,
) -> ResultEP<RedisSecurityInfo> {
    let _ctx = ctx_with_trace!();
    let mut info = RedisSecurityInfo::default();

    // First, get basic security info from server section
    let server_section = fetch_info_section(context.clone(), telemetry, "server").await?;
    let server_map = server_section.map;
    let server_info = InfoMap::new(&server_map);

    info.ssl_enabled = server_info.bool("ssl_enabled");
    let has_requirepass = !server_info.default::<String>("requirepass").is_empty();
    let has_acl_users = server_info.opt::<u64>("acl_users").unwrap_or(0) > 0;
    info.auth_required = has_requirepass || has_acl_users;
    info.protected_mode = server_info.bool("protected_mode");

    // Try to get ACL users if ACL is available
    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    // Try to get ACL users
    let mut acl_users_cmd = command::cmd("ACL");
    acl_users_cmd.arg("USERS");
    match execute_command::<Vec<String>>(&mut connection, acl_users_cmd).await {
        Ok(users) => {
            info.acl_users = users;
        }
        Err(_err) => {
            log_debug!(
                _ctx.clone(),
                format!("ACL command not available or failed: {}", _err),
                audience = eden_logger_internal::LogAudience::Internal
            );
            // Try to get user count from INFO if ACL is not available
            if let Some(user_count) = server_map.get("acl_users")
                && let Ok(count) = user_count.parse::<usize>()
            {
                // Create placeholder user names
                for i in 0..count {
                    info.acl_users.push(format!("user_{}", i));
                }
            }
        }
    }

    // Get SSL configuration from CONFIG if available
    let mut tls_cert_cmd = command::cmd("CONFIG");
    tls_cert_cmd.arg("GET").arg("tls-cert-file");
    if let Ok(config_vals) = execute_command::<Vec<String>>(&mut connection, tls_cert_cmd).await
        && config_vals.len() >= 2
    {
        info.ssl_cert_file = Some(config_vals[1].clone());
    }

    let mut tls_key_cmd = command::cmd("CONFIG");
    tls_key_cmd.arg("GET").arg("tls-key-file");
    if let Ok(key_vals) = execute_command::<Vec<String>>(&mut connection, tls_key_cmd).await
        && key_vals.len() >= 2
    {
        info.ssl_key_file = Some(key_vals[1].clone());
    }

    let mut ca_cmd = command::cmd("CONFIG");
    ca_cmd.arg("GET").arg("tls-ca-cert-file");
    if let Ok(ca_vals) = execute_command::<Vec<String>>(&mut connection, ca_cmd).await
        && ca_vals.len() >= 2
    {
        info.ssl_ca_cert_file = Some(ca_vals[1].clone());
    }

    // Load detailed security information
    load_detailed_security_info(context.clone(), telemetry, &mut info).await?;

    Ok(info)
}

/// Load detailed security information beyond basic flags
async fn load_detailed_security_info(context: RedisAsync, telemetry: &mut TelemetryWrapper, info: &mut RedisSecurityInfo) -> ResultEP<()> {
    // Load detailed ACL user information
    load_acl_user_details(context.clone(), info).await?;

    // Load SSL/TLS configuration details
    load_ssl_config_details(context.clone(), info).await?;

    // Load authentication configuration
    load_auth_config_details(context.clone(), info).await?;

    // Load network security settings
    load_network_security_details(context.clone(), info).await?;

    // Load command access control information
    load_command_access_details(context.clone(), info).await?;

    // Load security events and statistics
    load_security_events(context.clone(), info).await?;

    mark_endpoint_response(telemetry);

    Ok(())
}

/// Load detailed ACL user information
async fn load_acl_user_details(context: RedisAsync, info: &mut RedisSecurityInfo) -> ResultEP<()> {
    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    // Get list of all ACL users
    let mut users_cmd = command::cmd("ACL");
    users_cmd.arg("USERS");
    if let Ok(users) = execute_command::<Vec<String>>(&mut connection, users_cmd).await {
        info.acl_users = users.clone();

        for username in users {
            // Get detailed information for each user
            let mut user_cmd = command::cmd("ACL");
            user_cmd.arg("GETUSER").arg(&username);
            if let Ok(user_data) = execute_command::<Vec<Value>>(&mut connection, user_cmd).await {
                let acl_user = parse_acl_user_info(&username, &user_data);

                // Check if this is the default user
                if username == "default" {
                    info.default_acl_user = Some(acl_user.clone());
                }

                info.acl_user_details.push(acl_user);
            }
        }
    }

    // Get ACL categories
    let mut cats_cmd = command::cmd("ACL");
    cats_cmd.arg("CAT");
    if let Ok(categories) = execute_command::<Vec<String>>(&mut connection, cats_cmd).await {
        info.acl_categories = categories;
    }

    Ok(())
}

/// Parse ACL user information from ACL GETUSER output
fn parse_acl_user_info(username: &str, user_data: &[Value]) -> crate::metadata::stc::security::AclUserInfo {
    let mut user_info = crate::metadata::stc::security::AclUserInfo {
        username: username.to_string(),
        is_default: username == "default",
        ..Default::default()
    };

    let mut iter = user_data.iter();
    while let (Some(key), Some(value)) = (iter.next(), iter.next()) {
        let Some(key_name) = value_as_string(key) else {
            continue;
        };

        match key_name.as_str() {
            "flags" => {
                if let Some(flags) = value_to_vec(value) {
                    user_info.flags = flags.iter().filter_map(value_as_string).collect();
                    user_info.is_enabled = !user_info.flags.contains(&"off".to_string());
                }
            }
            "passwords" => {
                if let Some(passwords) = value_to_vec(value) {
                    user_info.passwords = passwords.iter().filter_map(value_as_string).collect();
                }
            }
            "categories" => {
                if let Some(categories) = value_to_vec(value) {
                    user_info.categories = categories.iter().filter_map(value_as_string).collect();
                }
            }
            "commands" => {
                if let Some(commands) = value_to_vec(value) {
                    user_info.commands = commands.iter().filter_map(value_as_string).collect();
                }
            }
            "keys" => {
                if let Some(keys) = value_to_vec(value) {
                    user_info.keys = keys.iter().filter_map(value_as_string).collect();
                }
            }
            "channels" => {
                if let Some(channels) = value_to_vec(value) {
                    user_info.channels = channels.iter().filter_map(value_as_string).collect();
                }
            }
            "selectors" => {
                if let Some(selectors) = value_to_vec(value) {
                    user_info.selectors = selectors.iter().filter_map(value_as_string).collect();
                }
            }
            "created" => {
                if let Some(created) = value_as_u64(value) {
                    user_info.created_at = Some(created);
                }
            }
            "lastuse" | "lastUse" => {
                if let Some(last_login) = value_as_u64(value) {
                    user_info.last_login = Some(last_login);
                }
            }
            _ => {}
        }
    }

    user_info
}

/// Load SSL/TLS configuration details
async fn load_ssl_config_details(context: RedisAsync, info: &mut RedisSecurityInfo) -> ResultEP<()> {
    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    // Get TLS configuration
    let tls_configs = vec![
        ("tls-port", "tls_port"),
        ("tls-cert-file", "tls_cert_file"),
        ("tls-key-file", "tls_key_file"),
        ("tls-ca-cert-file", "tls_ca_cert_file"),
        ("tls-ca-cert-dir", "tls_ca_cert_dir"),
        ("tls-protocols", "tls_protocols"),
        ("tls-ciphers", "tls_ciphers"),
        ("tls-prefer-server-ciphers", "tls_prefer_server_ciphers"),
        ("tls-session-caching", "tls_session_caching"),
        ("tls-session-cache-size", "tls_session_cache_size"),
        ("tls-session-cache-timeout", "tls_session_cache_timeout"),
    ];

    for (config_key, field_name) in tls_configs {
        let mut config_cmd = command::cmd("CONFIG");
        config_cmd.arg("GET").arg(config_key);
        if let Ok(config_data) = execute_command::<Vec<String>>(&mut connection, config_cmd).await
            && config_data.len() >= 2
        {
            let value = &config_data[1];
            match field_name {
                "tls_port" => {
                    if let Ok(port) = value.parse::<u16>() {
                        info.ssl_config.tls_port = port;
                    }
                }
                "tls_cert_file" => info.ssl_config.tls_cert_file = Some(value.clone()),
                "tls_key_file" => info.ssl_config.tls_key_file = Some(value.clone()),
                "tls_ca_cert_file" => info.ssl_config.tls_ca_cert_file = Some(value.clone()),
                "tls_ca_cert_dir" => info.ssl_config.tls_ca_cert_dir = Some(value.clone()),
                "tls_protocols" => {
                    info.ssl_config.tls_protocols = value.split_whitespace().map(|s| s.to_string()).collect();
                }
                "tls_ciphers" => {
                    info.ssl_config.tls_ciphers = value.split_whitespace().map(|s| s.to_string()).collect();
                }
                "tls_prefer_server_ciphers" => {
                    info.ssl_config.tls_prefer_server_ciphers = value == "yes";
                }
                "tls_session_caching" => {
                    info.ssl_config.tls_session_caching = value == "yes";
                }
                "tls_session_cache_size" => {
                    if let Ok(size) = value.parse::<u32>() {
                        info.ssl_config.tls_session_cache_size = Some(size);
                    }
                }
                "tls_session_cache_timeout" => {
                    if let Ok(timeout) = value.parse::<u32>() {
                        info.ssl_config.tls_session_cache_timeout = Some(timeout);
                    }
                }
                _ => {}
            }
        }
    }

    info.ssl_config.ssl_enabled = info.ssl_enabled;

    Ok(())
}

/// Load authentication configuration details
async fn load_auth_config_details(context: RedisAsync, info: &mut RedisSecurityInfo) -> ResultEP<()> {
    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    // Get requirepass configuration
    let mut requirepass_cmd = command::cmd("CONFIG");
    requirepass_cmd.arg("GET").arg("requirepass");
    if let Ok(config_data) = execute_command::<Vec<String>>(&mut connection, requirepass_cmd).await
        && config_data.len() >= 2
        && !config_data[1].is_empty()
    {
        info.auth_config.requirepass = Some("<redacted>".to_string());
        info.auth_required = true;
    }

    // Get default user configuration
    let mut default_user_cmd = command::cmd("ACL");
    default_user_cmd.arg("GETUSER").arg("default");
    if let Ok(default_user_data) = execute_command::<Vec<Value>>(&mut connection, default_user_cmd).await {
        let default_user = parse_acl_user_info("default", &default_user_data);
        info.default_acl_user = Some(default_user);
        info.auth_config.default_user = "default".to_string();
    }

    Ok(())
}

/// Load network security settings
async fn load_network_security_details(context: RedisAsync, info: &mut RedisSecurityInfo) -> ResultEP<()> {
    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    let network_configs = vec![
        ("bind", "bind_addresses"),
        ("port", "port"),
        ("tcp-backlog", "tcp_backlog"),
        ("tcp-keepalive", "tcp_keepalive"),
        ("timeout", "timeout"),
        ("tcp-user-timeout", "tcp_user_timeout"),
        ("maxclients", "maxclients"),
        ("unixsocket", "unixsocket"),
        ("unixsocketperm", "unixsocketperm"),
    ];

    for (config_key, field_name) in network_configs {
        let mut config_cmd = command::cmd("CONFIG");
        config_cmd.arg("GET").arg(config_key);
        if let Ok(config_data) = execute_command::<Vec<String>>(&mut connection, config_cmd).await
            && config_data.len() >= 2
        {
            let value = &config_data[1];
            match field_name {
                "bind_addresses" => {
                    info.network_security.bind_addresses = value.split_whitespace().map(|s| s.to_string()).collect();
                }
                "port" => {
                    if let Ok(port) = value.parse::<u16>() {
                        info.network_security.port = port;
                    }
                }
                "tcp_backlog" => {
                    if let Ok(backlog) = value.parse::<u32>() {
                        info.network_security.tcp_backlog = Some(backlog);
                    }
                }
                "tcp_keepalive" => {
                    if let Ok(keepalive) = value.parse::<u32>() {
                        info.network_security.tcp_keepalive = Some(keepalive);
                    }
                }
                "timeout" => {
                    if let Ok(timeout) = value.parse::<u32>() {
                        info.network_security.timeout = Some(timeout);
                    }
                }
                "tcp_user_timeout" => {
                    if let Ok(timeout) = value.parse::<u32>() {
                        info.network_security.tcp_user_timeout = Some(timeout);
                    }
                }
                "maxclients" => {
                    if let Ok(maxclients) = value.parse::<u32>() {
                        info.network_security.maxclients = maxclients;
                    }
                }
                "unixsocket" => {
                    if !value.is_empty() {
                        info.network_security.unixsocket = Some(value.clone());
                    }
                }
                "unixsocketperm" => {
                    if !value.is_empty() {
                        info.network_security.unixsocketperm = Some(value.clone());
                    }
                }
                _ => {}
            }
        }
    }

    info.network_security.protected_mode = info.protected_mode;

    Ok(())
}

/// Load command access control information
async fn load_command_access_details(context: RedisAsync, info: &mut RedisSecurityInfo) -> ResultEP<()> {
    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    // Get rename-command configurations
    let mut rename_cmd = command::cmd("CONFIG");
    rename_cmd.arg("GET").arg("rename-command");
    if let Ok(config_data) = execute_command::<Vec<String>>(&mut connection, rename_cmd).await
        && config_data.len() >= 2
        && !config_data[1].is_empty()
    {
        // Parse rename-command format
        let commands: Vec<&str> = config_data[1].split_whitespace().collect();
        for i in (0..commands.len()).step_by(2) {
            if i + 1 < commands.len() {
                info.command_access_control.rename_commands.insert(commands[i].to_string(), commands[i + 1].to_string());
            }
        }
    }

    // Check if EVAL is enabled
    let mut eval_cmd = command::cmd("EVAL");
    eval_cmd.arg("return 1").arg("0");
    info.command_access_control.eval_enabled = execute_command::<String>(&mut connection, eval_cmd).await.is_ok();

    Ok(())
}

/// Load security events and statistics
async fn load_security_events(context: RedisAsync, info: &mut RedisSecurityInfo) -> ResultEP<()> {
    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    // Get security-related statistics from INFO
    let mut info_cmd = command::cmd("INFO");
    info_cmd.arg("stats");
    if let Ok(stats_info) = execute_command::<String>(&mut connection, info_cmd).await {
        for line in stats_info.lines() {
            if let Some((key, value)) = line.split_once(':') {
                match key.trim() {
                    "acl_access_denied_auth" => {
                        if let Ok(count) = value.trim().parse::<u64>() {
                            info.security_events.failed_auth_attempts = count;
                        }
                    }
                    "acl_access_denied_cmd" => {
                        if let Ok(count) = value.trim().parse::<u64>() {
                            info.security_events.acl_denied_commands = count;
                        }
                    }
                    "acl_access_denied_key" => {
                        if let Ok(count) = value.trim().parse::<u64>() {
                            info.security_events.acl_denied_keys = count;
                        }
                    }
                    "acl_access_denied_channel" => {
                        if let Ok(count) = value.trim().parse::<u64>() {
                            info.security_events.acl_denied_channels = count;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    info.security_events.last_security_scan =
        Some(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs());

    Ok(())
}
