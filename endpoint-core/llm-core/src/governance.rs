use crate::pii::{LlmPiiScanner, PiiScanResult};
use crate::types::{LlmInvocation, PolicyAction};
use error::EpError;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Governance policy applied before an LLM request leaves Eden.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct LlmGovernancePolicy {
    #[serde(default)]
    pub pii_action: PolicyAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_allowlist: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_tokens_monthly: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_rpm: Option<u32>,
}

/// Outcome of evaluating pre-egress governance rules.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct GovernanceDecision {
    pub action_taken: PolicyAction,
    pub pii_result: PiiScanResult,
    pub redacted: bool,
    pub blocked: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_reason: Option<String>,
}

/// Evaluate and apply pre-egress governance rules to an invocation.
///
/// This scans user prompt content and tool-result messages for PII, optionally
/// redacts the prompt in place, and validates any requested model against an
/// allowlist. Blocking outcomes are surfaced as `blocked = true` in the
/// returned decision so callers can emit analytics before returning an error.
pub fn evaluate_pre_egress_policy(
    policy: &LlmGovernancePolicy,
    invocation: &mut LlmInvocation,
    pii_scanner: &LlmPiiScanner,
) -> Result<GovernanceDecision, EpError> {
    let pii_result = pii_scanner.scan_messages(&invocation.conversation);
    let built_in_detected = pii_result.built_in_detected();
    let custom_block = pii_result.custom_matches.iter().any(|matched| matched.action == PolicyAction::Block);
    let custom_redact = pii_result.custom_matches.iter().any(|matched| matched.action == PolicyAction::Redact);

    let mut decision = GovernanceDecision {
        action_taken: PolicyAction::Allow,
        pii_result,
        redacted: false,
        blocked: false,
        block_reason: None,
    };

    if let Some(allowlist) = policy.model_allowlist.as_ref() {
        let Some(requested_model) = invocation.overrides.model.as_deref() else {
            decision.action_taken = PolicyAction::Block;
            decision.blocked = true;
            decision.block_reason = Some("requested model is required when a governance allowlist is configured".to_string());
            return Ok(decision);
        };

        if !allowlist_models_match(allowlist, requested_model) {
            decision.action_taken = PolicyAction::Block;
            decision.blocked = true;
            decision.block_reason = Some("requested model is not allowed by governance policy".to_string());
            return Ok(decision);
        }
    }

    // A custom dictionary `Block` term, or built-in PII under a `Block` policy,
    // rejects the request before it ever leaves Eden.
    if custom_block || (built_in_detected && policy.pii_action == PolicyAction::Block) {
        decision.action_taken = PolicyAction::Block;
        decision.blocked = true;
        decision.block_reason = Some("request blocked by LLM governance policy".to_string());
        return Ok(decision);
    }

    // Redact built-in PII when the base policy says so (that pass also masks any
    // custom `Redact` terms); otherwise mask only custom `Redact` terms so an
    // audit/allow base policy still honors the dictionary.
    let redact_built_in = built_in_detected && policy.pii_action == PolicyAction::Redact;
    if redact_built_in {
        pii_scanner.redact_messages(&mut invocation.conversation, &decision.pii_result);
        decision.redacted = true;
        decision.action_taken = PolicyAction::Redact;
    } else if custom_redact {
        pii_scanner.redact_custom_terms(&mut invocation.conversation);
        decision.redacted = true;
        decision.action_taken = PolicyAction::Redact;
    } else if decision.pii_result.has_matches() {
        // Detected but intentionally not mutated (audit/allow).
        decision.action_taken = if built_in_detected {
            policy.pii_action
        } else {
            PolicyAction::AuditOnly
        };
    }

    Ok(decision)
}

fn allowlist_models_match(allowlist: &[String], requested_model: &str) -> bool {
    let requested_model = normalize_model_for_allowlist(requested_model);
    allowlist.iter().any(|allowed_model| normalize_model_for_allowlist(allowed_model) == requested_model)
}

fn normalize_model_for_allowlist(model: &str) -> String {
    let normalized = model.trim().to_ascii_lowercase();
    normalized.split_once('/').map(|(_, suffix)| suffix.to_string()).unwrap_or(normalized)
}

/// Convenience wrapper that returns an `EpError` when evaluation decides to block.
pub fn apply_pre_egress_policy(
    policy: &LlmGovernancePolicy,
    invocation: &mut LlmInvocation,
    pii_scanner: &LlmPiiScanner,
) -> Result<GovernanceDecision, EpError> {
    let decision = evaluate_pre_egress_policy(policy, invocation, pii_scanner)?;
    if decision.blocked {
        return Err(EpError::request(
            decision.block_reason.clone().unwrap_or_else(|| "request blocked by LLM governance policy".to_string()),
        ));
    }

    Ok(decision)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pii::CustomPiiTerm;
    use crate::types::{LlmMessage, LlmMessageKind, LlmMessageRole, LlmRequestOverrides};

    fn sample_invocation(content: &str, model: &str) -> LlmInvocation {
        LlmInvocation {
            conversation_id: None,
            conversation: vec![LlmMessage {
                role: LlmMessageRole::User,
                content: content.to_string(),
                kind: LlmMessageKind::Text,
            }],
            tools: Vec::new(),
            tool_choice: None,
            system_prompt: Some("system@example.com".to_string()),
            system_prompt_blocks: None,
            turn_context: None,
            overrides: LlmRequestOverrides { model: Some(model.to_string()), ..Default::default() },
            response_format: None,
            parallel_tool_calls: None,
            tool_connections: Vec::new(),
            tool_endpoint_uuids: Vec::new(),
        }
    }

    #[test]
    fn allow_policy_records_findings_without_mutation() {
        let scanner = LlmPiiScanner::new();
        let policy = LlmGovernancePolicy { pii_action: PolicyAction::Allow, ..Default::default() };
        let mut invocation = sample_invocation("email me at user@example.com", "gpt-4o");

        let decision = apply_pre_egress_policy(&policy, &mut invocation, &scanner).expect("allow policy should pass");

        assert!(decision.pii_result.detected);
        assert_eq!(decision.action_taken, PolicyAction::Allow);
        assert_eq!(invocation.conversation[0].content, "email me at user@example.com");
    }

    #[test]
    fn redact_policy_mutates_prompt_content() {
        let scanner = LlmPiiScanner::new();
        let policy = LlmGovernancePolicy { pii_action: PolicyAction::Redact, ..Default::default() };
        let mut invocation = sample_invocation("email me at user@example.com", "gpt-4o");

        let decision = apply_pre_egress_policy(&policy, &mut invocation, &scanner).expect("redact policy should pass");

        assert!(decision.redacted);
        assert_eq!(decision.action_taken, PolicyAction::Redact);
        assert_eq!(invocation.conversation[0].content, "email me at [REDACTED_EMAIL]");
        assert_eq!(invocation.system_prompt.as_deref(), Some("system@example.com"));
    }

    #[test]
    fn block_policy_marks_decision_as_blocked() {
        let scanner = LlmPiiScanner::new();
        let policy = LlmGovernancePolicy { pii_action: PolicyAction::Block, ..Default::default() };
        let mut invocation = sample_invocation("email me at user@example.com", "gpt-4o");

        let decision = evaluate_pre_egress_policy(&policy, &mut invocation, &scanner).expect("evaluation should succeed");
        assert!(decision.blocked);
        assert_eq!(decision.action_taken, PolicyAction::Block);
        assert!(apply_pre_egress_policy(&policy, &mut invocation, &scanner).is_err());
    }

    #[test]
    fn allowlist_rejects_disallowed_models() {
        let scanner = LlmPiiScanner::new();
        let policy = LlmGovernancePolicy {
            pii_action: PolicyAction::AuditOnly,
            model_allowlist: Some(vec!["gpt-4o-mini".to_string()]),
            ..Default::default()
        };
        let mut invocation = sample_invocation("hello", "gpt-4o");

        let decision = evaluate_pre_egress_policy(&policy, &mut invocation, &scanner).expect("evaluation should succeed");
        assert!(decision.blocked);
        assert_eq!(decision.action_taken, PolicyAction::Block);
        assert_eq!(decision.block_reason.as_deref(), Some("requested model is not allowed by governance policy"));
        let err = apply_pre_egress_policy(&policy, &mut invocation, &scanner).expect_err("allowlist should reject model");
        assert!(err.to_string().contains("not allowed"));
    }

    #[test]
    fn allowlist_accepts_provider_prefixed_aliases() {
        let scanner = LlmPiiScanner::new();
        let policy = LlmGovernancePolicy {
            pii_action: PolicyAction::AuditOnly,
            model_allowlist: Some(vec!["openai/gpt-4o-mini".to_string()]),
            ..Default::default()
        };
        let mut invocation = sample_invocation("hello", "GPT-4O-MINI");

        let decision = evaluate_pre_egress_policy(&policy, &mut invocation, &scanner).expect("evaluation should succeed");

        assert!(!decision.blocked);
        assert_eq!(decision.action_taken, PolicyAction::Allow);
    }

    fn pii_term(term: &str, action: PolicyAction) -> CustomPiiTerm {
        CustomPiiTerm { term: term.to_string(), action, label: None }
    }

    #[test]
    fn custom_dictionary_block_term_blocks_request() {
        let scanner = LlmPiiScanner::with_custom_terms(vec![pii_term("Project Titan", PolicyAction::Block)]);
        let policy = LlmGovernancePolicy { pii_action: PolicyAction::Allow, ..Default::default() };
        let mut invocation = sample_invocation("share the Project Titan roadmap", "gpt-4o");

        let decision = evaluate_pre_egress_policy(&policy, &mut invocation, &scanner).expect("evaluation should succeed");

        assert!(decision.blocked);
        assert_eq!(decision.action_taken, PolicyAction::Block);
        assert!(apply_pre_egress_policy(&policy, &mut invocation, &scanner).is_err());
    }

    #[test]
    fn custom_dictionary_redact_term_masks_even_when_base_policy_allows() {
        let scanner = LlmPiiScanner::with_custom_terms(vec![pii_term("widgetco", PolicyAction::Redact)]);
        let policy = LlmGovernancePolicy { pii_action: PolicyAction::Allow, ..Default::default() };
        let mut invocation = sample_invocation("ping widgetco about pricing", "gpt-4o");

        let decision = evaluate_pre_egress_policy(&policy, &mut invocation, &scanner).expect("evaluation should succeed");

        assert!(decision.redacted);
        assert_eq!(decision.action_taken, PolicyAction::Redact);
        assert_eq!(invocation.conversation[0].content, "ping [REDACTED_TERM] about pricing");
    }

    #[test]
    fn audit_base_policy_redacts_only_custom_terms() {
        let scanner = LlmPiiScanner::with_custom_terms(vec![pii_term("widgetco", PolicyAction::Redact)]);
        let policy = LlmGovernancePolicy { pii_action: PolicyAction::AuditOnly, ..Default::default() };
        let mut invocation = sample_invocation("email alice@example.com about widgetco", "gpt-4o");

        let decision = evaluate_pre_egress_policy(&policy, &mut invocation, &scanner).expect("evaluation should succeed");

        assert!(decision.redacted);
        // Built-in email is preserved under an audit base policy; only the
        // custom dictionary term is masked.
        assert_eq!(invocation.conversation[0].content, "email alice@example.com about [REDACTED_TERM]");
    }

    #[test]
    fn redact_base_policy_masks_builtin_and_custom_terms() {
        let scanner = LlmPiiScanner::with_custom_terms(vec![pii_term("widgetco", PolicyAction::Redact)]);
        let policy = LlmGovernancePolicy { pii_action: PolicyAction::Redact, ..Default::default() };
        let mut invocation = sample_invocation("email alice@example.com about widgetco", "gpt-4o");

        let decision = evaluate_pre_egress_policy(&policy, &mut invocation, &scanner).expect("evaluation should succeed");

        assert!(decision.redacted);
        assert_eq!(decision.action_taken, PolicyAction::Redact);
        assert_eq!(invocation.conversation[0].content, "email [REDACTED_EMAIL] about [REDACTED_TERM]");
    }
}
