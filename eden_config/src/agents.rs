//! Agent and skill system configuration.
//!
//! Maps to `[agents]` in `eden.toml`.

use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

/// Top-level agent configuration for skills, tool passes and session management.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentsConfig {
    /// Percentage of provider context window allocated to skill content in the system prompt.
    pub skill_prompt_budget_percent: usize,
    /// Explicit skill prompt token budget override. When set, overrides the percentage-based calculation.
    pub skill_prompt_budget_tokens: Option<usize>,
    /// Context window size assumption when the provider/model cannot be detected.
    pub default_context_window_tokens: usize,
    /// Maximum number of dynamic skills that can be loaded simultaneously per conversation.
    pub default_skill_capacity: usize,
    /// Maximum tool call iterations per chat request before forcing a text response.
    pub max_tool_passes: u32,
    /// Maximum messages allowed in a conversation before rejection.
    pub max_conversation_messages: usize,
    /// Session cache TTL in seconds.
    pub session_ttl_secs: u64,
    /// Operational memory retention in days. Memories older than this are eligible for cleanup.
    pub memory_retention_days: u32,
    /// Security settings for tool endpoint registration and execution.
    pub security: AgentsSecurityConfig,
    /// Policy governing which skills Adam may surface to the LLM (builtin-only,
    /// global-curated, or tenant-scoped) and an optional quarantine deny-list.
    pub skill_policy: SkillPolicyConfig,
    /// When false, REST endpoints that create/update/delete or import customer
    /// skills return 403. Defaults to false so launch posture is "builtin-only,
    /// no customer writes"; operators may opt-in once the authoring and
    /// tenant-scoping story is fully wired through.
    pub allow_customer_skill_crud: bool,
}

impl Default for AgentsConfig {
    fn default() -> Self {
        Self {
            skill_prompt_budget_percent: 5,
            skill_prompt_budget_tokens: None,
            default_context_window_tokens: 8_192,
            default_skill_capacity: 5,
            max_tool_passes: 25,
            max_conversation_messages: 200,
            session_ttl_secs: 1_800,
            memory_retention_days: 90,
            security: AgentsSecurityConfig::default(),
            skill_policy: SkillPolicyConfig::default(),
            allow_customer_skill_crud: false,
        }
    }
}

/// Security configuration for agent tool endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentsSecurityConfig {
    /// Hostnames or IPs explicitly allowed for tool endpoint registration,
    /// even if they would otherwise be blocked by private IP checks.
    pub tool_endpoint_allowed_hosts: Vec<String>,
    /// Allow tool endpoints on private/internal IP ranges (RFC1918, link-local).
    /// When true, all private IPs are allowed.
    pub tool_endpoint_allow_private_ips: bool,
    /// Require HTTPS for tool endpoint URLs. HTTP is always allowed for localhost.
    pub tool_endpoint_require_https: bool,
}

impl Default for AgentsSecurityConfig {
    fn default() -> Self {
        Self {
            tool_endpoint_allowed_hosts: Vec::new(),
            tool_endpoint_allow_private_ips: false,
            tool_endpoint_require_https: true,
        }
    }
}

/// Authority under which Adam resolves the set of skills offered to the LLM.
///
/// Launch posture is `BuiltinsOnly`: only the compiled-in catalogue is visible,
/// independent of any rows that exist in `llm_skills`. The two DB-authoritative
/// modes are available for later rollout once per-tenant skill authoring is
/// safe to enable.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SkillPolicyMode {
    /// Only builtin skills are visible to the LLM. DB rows are ignored.
    #[default]
    BuiltinsOnly,
    /// Builtins plus rows from `llm_skills` that have `organization_uuid IS NULL`.
    GlobalCurated,
    /// Builtins plus rows scoped to the caller's `organization_uuid`.
    TenantScoped,
}

/// Skill policy resolver configuration.
///
/// Maps to `[agents.skill_policy]` in `eden.toml`, or via the legacy env vars
/// `ADAM_SKILL_POLICY_MODE` and `ADAM_QUARANTINED_SKILLS` (comma-separated).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SkillPolicyConfig {
    /// Which authority resolves the skill set. See [`SkillPolicyMode`].
    pub mode: SkillPolicyMode,
    /// Skill names (after alias normalization) that must never be served to
    /// the LLM regardless of mode. Surgical incident-response control; empty
    /// is the expected steady state.
    ///
    /// Accepts either a TOML array (`["skill-a", "skill-b"]`) or a single
    /// comma-separated string (e.g. from the `ADAM_QUARANTINED_SKILLS` env
    /// var), so legacy deployment scripts can set one value.
    #[serde(deserialize_with = "deserialize_comma_separated_or_seq")]
    pub quarantined_skills: Vec<String>,
}

/// Accept either a `Vec<String>` or a single comma-separated `String`.
///
/// The nested-env layer turns `ADAM_QUARANTINED_SKILLS=a,b,c` into a String,
/// while `eden.toml` may supply `quarantined_skills = ["a", "b", "c"]`. Both
/// need to end up as `Vec<String>` without truncation or reorder.
fn deserialize_comma_separated_or_seq<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct CommaOrSeq;

    impl<'de> Visitor<'de> for CommaOrSeq {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a list of strings or a comma-separated string")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&value)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut items = Vec::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(item) = seq.next_element::<String>()? {
                let trimmed = item.trim().to_string();
                if !trimmed.is_empty() {
                    items.push(trimmed);
                }
            }
            Ok(items)
        }
    }

    deserializer.deserialize_any(CommaOrSeq)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_policy_defaults_to_builtins_only() {
        let config = AgentsConfig::default();
        assert_eq!(config.skill_policy.mode, SkillPolicyMode::BuiltinsOnly);
        assert!(config.skill_policy.quarantined_skills.is_empty());
        assert!(!config.allow_customer_skill_crud);
    }

    #[test]
    fn quarantined_skills_deserialize_from_array() {
        let toml = r#"
            mode = "tenant_scoped"
            quarantined_skills = ["alpha", "beta"]
        "#;
        let config: SkillPolicyConfig = toml::from_str(toml).expect("toml");
        assert_eq!(config.mode, SkillPolicyMode::TenantScoped);
        assert_eq!(config.quarantined_skills, vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[test]
    fn quarantined_skills_deserialize_from_comma_string() {
        let toml = r#"
            mode = "global_curated"
            quarantined_skills = "alpha, beta, ,gamma"
        "#;
        let config: SkillPolicyConfig = toml::from_str(toml).expect("toml");
        assert_eq!(config.mode, SkillPolicyMode::GlobalCurated);
        assert_eq!(config.quarantined_skills, vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]);
    }
}
