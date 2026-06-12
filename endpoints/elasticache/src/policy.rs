use endpoint_types::protocol::EpProtocol;
use ep_redis::api::RedisApi;
use ep_redis::protocol::extract_resp_command_uppercase;
use error::EpError;

use ep_redis::protocol::RedisProtocol;

// Restricted data-plane commands for ElastiCache.
// Source: AWS ElastiCache restricted commands + serverless restrictions (union).
const RESTRICTED_COMMANDS: &[&str] = &[
    // Core restricted commands
    "BGREWRITEAOF",
    "BGSAVE",
    "MIGRATE",
    "PSYNC",
    "REPLICAOF",
    "SAVE",
    "SLAVEOF",
    "SHUTDOWN",
    "SYNC",
    // ACL restrictions (serverless)
    "ACL DELUSER",
    "ACL LOAD",
    "ACL LOG",
    "ACL SAVE",
    "ACL SETUSER",
    // CLUSTER restrictions (serverless)
    "CLUSTER ADDSLOT",
    "CLUSTER ADDSLOTS",
    "CLUSTER ADDSLOTSRANGE",
    "CLUSTER BUMPEPOCH",
    "CLUSTER DELSLOT",
    "CLUSTER DELSLOTS",
    "CLUSTER DELSLOTSRANGE",
    "CLUSTER FAILOVER",
    "CLUSTER FLUSHSLOTS",
    "CLUSTER FORGET",
    "CLUSTER LINKS",
    "CLUSTER MEET",
    "CLUSTER SETSLOT",
    // CLIENT restrictions (serverless)
    "CLIENT CACHING",
    "CLIENT GETREDIR",
    "CLIENT ID",
    "CLIENT INFO",
    "CLIENT KILL",
    "CLIENT LIST",
    "CLIENT NO-EVICT",
    "CLIENT PAUSE",
    "CLIENT TRACKING",
];

// Prefix-restricted commands (block all subcommands).
const RESTRICTED_PREFIXES: &[&str] = &[
    // Control-plane prefixes are blocked because control-plane support
    // is not implemented yet; these will be allowed once supported.
    "ELASTICACHE",
    "AWS",
    "CREATE",
    "DESCRIBE",
    "MODIFY",
    "DELETE",
    "LIST",
    "ADD",
    "REMOVE",
    "COPY",
    "PURCHASE",
    "REBOOT",
    "RESET",
    "FAILOVER",
    "REBALANCE",
    "INCREASE",
    "DECREASE",
    // Data-plane restricted prefixes.
    "CONFIG",
    "DEBUG",
];

pub(crate) fn ensure_api_allowed(api: &RedisApi) -> Result<(), EpError> {
    ensure_command_allowed(&api.to_string())
}

pub(crate) fn ensure_raw_bytes_allowed(bytes: &[u8]) -> Result<(), EpError> {
    if let Some(command) = command_from_bytes(bytes) {
        ensure_command_allowed(&command)
    } else {
        Ok(())
    }
}

fn command_from_bytes(bytes: &[u8]) -> Option<String> {
    if let Ok(Some((args, _))) = RedisProtocol::parse_buffer(bytes) {
        return Some(args.command().to_string());
    }

    extract_resp_command_uppercase(bytes)
}

fn ensure_command_allowed(command: &str) -> Result<(), EpError> {
    let normalized = normalize_command(command);

    if is_restricted_command(&normalized) {
        return Err(permission_denied(&normalized));
    }

    Ok(())
}

fn is_restricted_command(command: &str) -> bool {
    if RESTRICTED_COMMANDS.iter().any(|restricted| restricted == &command) {
        return true;
    }

    RESTRICTED_PREFIXES.iter().any(|prefix| matches_prefix(command, prefix))
}

fn matches_prefix(command: &str, prefix: &str) -> bool {
    command.starts_with(prefix)
}

fn normalize_command(command: &str) -> String {
    command.split_whitespace().map(|part| part.to_uppercase()).collect::<Vec<_>>().join(" ")
}

fn permission_denied(command: &str) -> EpError {
    EpError::redis(format!("NOPERM this user has no permissions to run the '{}' command", command))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_ping() {
        ensure_command_allowed("PING").expect("PING should be allowed");
    }

    #[test]
    fn blocks_config_prefix() {
        let err = ensure_command_allowed("config get *").expect_err("CONFIG should be blocked");
        assert!(err.to_string().contains("NOPERM"));
    }

    #[test]
    fn blocks_restricted_command() {
        let err = ensure_command_allowed("BGSAVE").expect_err("BGSAVE should be blocked");
        assert!(err.to_string().contains("NOPERM"));
    }

    #[test]
    fn blocks_control_plane_prefix() {
        let err = ensure_command_allowed("CreateCacheCluster").expect_err("control-plane command should be blocked");
        assert!(err.to_string().contains("NOPERM"));
    }

    #[test]
    fn blocks_raw_bytes_command() {
        let err = ensure_raw_bytes_allowed(b"*2\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n").expect_err("CONFIG should be blocked");
        assert!(err.to_string().contains("NOPERM"));
    }
}
