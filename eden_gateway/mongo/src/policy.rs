use eden_logger_internal::{LogAudience, log_warn};
use serde::Deserialize;

/// Risk classification for MongoDB commands
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandRisk {
    /// Commands that destroy data
    Dangerous,
    /// Commands that may block the server
    Blocking,
    /// Commands that may produce large results or move large amounts of data
    WarnLargeData,
    /// Normal operational commands
    Safe,
}

/// Policy enforcement modes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyEnforcementMode {
    /// Log only, allow all commands
    Observe,
    /// Log warnings, allow all commands
    Warn,
    /// Reject blocked commands with error
    Block,
}

/// Guard configuration for command policies
#[derive(Debug, Clone, Default, Deserialize)]
pub struct CommandGuardConfig {
    /// Policy presets (e.g., "strict", "production")
    #[serde(default)]
    pub presets: Vec<String>,
    /// Explicitly blocked commands
    #[serde(default)]
    pub blocked_commands: Vec<String>,
    /// Services exempt from policy enforcement
    #[serde(default)]
    pub exempt_services: Vec<String>,
}

/// Classify the risk of a MongoDB command.
pub fn classify_risk(command: &str) -> CommandRisk {
    match command.to_lowercase().as_str() {
        // Destructive operations
        "drop"
        | "dropdatabase"
        | "dropindexes"
        | "renamecollection"
        | "dropallrolesfromdatabase"
        | "dropallusersfromdatabase"
        | "droprole"
        | "dropuser" => CommandRisk::Dangerous,

        // Blocking / admin operations
        "reindex" | "compact" | "validate" | "fsync" | "fsynclockwithlock" | "repairdb" => CommandRisk::Blocking,

        // Large data movement
        "aggregate" | "mapreduce" => CommandRisk::WarnLargeData,

        // Everything else is safe
        _ => CommandRisk::Safe,
    }
}

/// Check if an aggregate pipeline contains $out or $merge stages (write stages)
pub fn aggregate_has_write_stage(pipeline_str: &str) -> bool {
    pipeline_str.contains("$out") || pipeline_str.contains("$merge")
}

/// Apply policy to a command. Returns None if allowed, or Some(error_message) if blocked.
pub fn apply_policy(
    command: &str,
    config: &CommandGuardConfig,
    mode: PolicyEnforcementMode,
    ctx: &eden_logger_internal::LogContext,
) -> Option<String> {
    // Check explicit block list
    let command_lower = command.to_lowercase();
    if config.blocked_commands.iter().any(|c| c.to_lowercase() == command_lower) {
        let reason = format!("Command '{}' is explicitly blocked by policy", command);
        match mode {
            PolicyEnforcementMode::Block => return Some(reason),
            PolicyEnforcementMode::Warn => {
                log_warn!(ctx.clone(), reason.clone(), audience = LogAudience::Internal);
            }
            PolicyEnforcementMode::Observe => {}
        }
    }

    // Check presets
    for preset in &config.presets {
        if let Some(reason) = check_preset(preset, command) {
            match mode {
                PolicyEnforcementMode::Block => return Some(reason),
                PolicyEnforcementMode::Warn => {
                    log_warn!(ctx.clone(), reason.clone(), audience = LogAudience::Internal);
                }
                PolicyEnforcementMode::Observe => {}
            }
        }
    }

    // Risk-based check
    let risk = classify_risk(command);
    match (risk, mode) {
        (CommandRisk::Dangerous, PolicyEnforcementMode::Block) => {
            Some(format!("Command '{}' classified as dangerous and is blocked", command))
        }
        (CommandRisk::Dangerous, PolicyEnforcementMode::Warn) => {
            log_warn!(ctx.clone(), format!("Dangerous command '{}' executed", command), audience = LogAudience::Internal);
            None
        }
        _ => None,
    }
}

/// Check command against a named preset policy set
fn check_preset(preset: &str, command: &str) -> Option<String> {
    let command_lower = command.to_lowercase();
    match preset.to_lowercase().as_str() {
        "strict" => {
            let blocked = [
                "drop",
                "dropdatabase",
                "dropindexes",
                "renamecollection",
                "compact",
                "reindex",
                "eval",
                "currentop",
                "killop",
                "fsync",
            ];
            if blocked.contains(&command_lower.as_str()) {
                return Some(format!("Command '{}' blocked by 'strict' preset", command));
            }
        }
        "production" => {
            let blocked = ["drop", "dropdatabase", "eval", "fsync", "repairdb"];
            if blocked.contains(&command_lower.as_str()) {
                return Some(format!("Command '{}' blocked by 'production' preset", command));
            }
        }
        _ => {}
    }
    None
}
