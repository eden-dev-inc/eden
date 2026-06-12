use crate::types::{LlmMessage, LlmMessageKind, LlmMessageRole, PolicyAction};
use aho_corasick::{AhoCorasick, MatchKind};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use utoipa::ToSchema;

use once_cell::sync::Lazy;

static PII_PATTERNS: Lazy<Vec<(PiiType, Regex)>> = Lazy::new(|| {
    vec![
        (
            PiiType::Email,
            Regex::new(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}").unwrap_or_else(|err| panic!("invalid email pii regex: {err}")),
        ),
        (
            PiiType::Phone,
            Regex::new(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b").unwrap_or_else(|err| panic!("invalid phone pii regex: {err}")),
        ),
        (
            PiiType::Ssn,
            Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").unwrap_or_else(|err| panic!("invalid ssn pii regex: {err}")),
        ),
        (
            PiiType::CreditCard,
            Regex::new(r"\b(?:4(?:[ -]?\d){12}(?:[ -]?\d{3})?|5[1-5](?:[ -]?\d){14}|3[47](?:[ -]?\d){13})\b")
                .unwrap_or_else(|err| panic!("invalid credit card pii regex: {err}")),
        ),
        (
            PiiType::IpAddress,
            Regex::new(r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b")
                .unwrap_or_else(|err| panic!("invalid ip address pii regex: {err}")),
        ),
        (
            PiiType::ApiKey,
            Regex::new(r"\b(?:sk_live_|pk_live_|api_key_|apikey_)[a-zA-Z0-9]{20,}\b")
                .unwrap_or_else(|err| panic!("invalid api key pii regex: {err}")),
        ),
    ]
});

/// PII categories scanned in LLM prompts and completions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PiiType {
    Email,
    Phone,
    Ssn,
    CreditCard,
    ApiKey,
    IpAddress,
}

impl PiiType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Email => "email",
            Self::Phone => "phone",
            Self::Ssn => "ssn",
            Self::CreditCard => "credit_card",
            Self::ApiKey => "api_key",
            Self::IpAddress => "ip_address",
        }
    }

    pub fn placeholder(self) -> &'static str {
        match self {
            Self::Email => "[REDACTED_EMAIL]",
            Self::Phone => "[REDACTED_PHONE]",
            Self::Ssn => "[REDACTED_SSN]",
            Self::CreditCard => "[REDACTED_CREDIT_CARD]",
            Self::ApiKey => "[REDACTED_API_KEY]",
            Self::IpAddress => "[REDACTED_IP_ADDRESS]",
        }
    }
}

impl fmt::Display for PiiType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One concrete PII match in a scanned text segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PiiFinding {
    pub pii_type: PiiType,
    pub start: usize,
    pub end: usize,
}

/// A user-supplied dictionary entry added to the PII scan, with the enforcement
/// level to apply when it is found in a prompt.
///
/// Matching is literal and case-insensitive (ASCII case folding). `Allow` is
/// treated as "off" for the term; `AuditOnly` records the match without
/// mutating content; `Redact` masks each occurrence; `Block` rejects the
/// request before it leaves Eden.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CustomPiiTerm {
    /// Literal phrase to match (case-insensitive).
    pub term: String,
    /// Enforcement level applied when this term matches.
    #[serde(default)]
    pub action: PolicyAction,
    /// Optional label used in the redaction placeholder. `Project` renders as
    /// `[REDACTED_PROJECT]`; defaults to `[REDACTED_TERM]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

impl CustomPiiTerm {
    /// Trimmed, lower-cased term used for matching. Empty terms are inert.
    fn needle(&self) -> String {
        self.term.trim().to_ascii_lowercase()
    }

    /// Redaction placeholder derived from `label` (falling back to `TERM`).
    fn placeholder(&self) -> String {
        let raw = self.label.as_deref().map(str::trim).filter(|label| !label.is_empty()).unwrap_or("TERM");
        let sanitized = raw.to_ascii_uppercase().chars().map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' }).collect::<String>();
        let sanitized = sanitized.trim_matches('_');
        format!("[REDACTED_{}]", if sanitized.is_empty() { "TERM" } else { sanitized })
    }
}

/// Aggregate record of a custom dictionary term that matched during a scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CustomPiiMatch {
    /// The configured term (original casing).
    pub term: String,
    /// Enforcement level configured for the term.
    pub action: PolicyAction,
    /// Number of occurrences found.
    pub count: u32,
}

/// Result of scanning text or message content for PII.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PiiScanResult {
    pub detected: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub types: Vec<PiiType>,
    pub count: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub findings: Vec<PiiFinding>,
    /// Custom dictionary terms that matched, with their configured actions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_matches: Vec<CustomPiiMatch>,
}

impl PiiScanResult {
    pub fn merge(&mut self, other: &Self) {
        if !other.detected {
            return;
        }

        self.detected = true;
        self.count = self.count.saturating_add(other.count);
        self.findings.extend(other.findings.clone());
        self.types.extend(other.types.iter().copied());
        self.types.sort();
        self.types.dedup();
        for matched in &other.custom_matches {
            if let Some(existing) = self.custom_matches.iter_mut().find(|entry| entry.term == matched.term) {
                existing.count = existing.count.saturating_add(matched.count);
            } else {
                self.custom_matches.push(matched.clone());
            }
        }
    }

    /// Built-in regex PII (email, phone, SSN, etc.) was found. Distinct from
    /// custom-dictionary matches, which carry their own per-term action.
    pub fn built_in_detected(&self) -> bool {
        self.count > 0 || !self.findings.is_empty()
    }

    /// Any match at all — built-in PII or a custom dictionary term.
    pub fn has_matches(&self) -> bool {
        self.detected || !self.custom_matches.is_empty()
    }

    /// Built-in PII type names plus `custom:<term>` for each matched dictionary
    /// entry, suitable for analytics/`pii_types`.
    pub fn type_names(&self) -> Vec<String> {
        let mut names = self.types.iter().map(ToString::to_string).collect::<Vec<_>>();
        names.extend(self.custom_matches.iter().map(|matched| format!("custom:{}", matched.term)));
        names
    }
}

/// Per-pattern metadata for a compiled custom dictionary, indexed by the
/// Aho-Corasick pattern id.
#[derive(Debug)]
struct CustomTermMeta {
    /// Original-casing term (trimmed), surfaced in scan results.
    term: String,
    action: PolicyAction,
    /// Precomputed redaction placeholder, e.g. `[REDACTED_PROJECT]`.
    placeholder: String,
}

/// Compiled custom-dictionary matcher: a single Aho-Corasick automaton over all
/// dictionary terms (ASCII case-insensitive). Matching is one O(text + matches)
/// pass regardless of dictionary size, replacing a per-term linear scan.
///
/// Compiling the automaton is the only dictionary-size-dependent cost, so
/// compile it once with [`CustomPiiMatcher::compile`] and keep the returned
/// `Arc` on the long-lived key — the per-request scan is then flat regardless of
/// dictionary size.
#[derive(Debug)]
pub struct CustomPiiMatcher {
    automaton: AhoCorasick,
    meta: Vec<CustomTermMeta>,
    has_redaction: bool,
}

impl CustomPiiMatcher {
    /// Compile a dictionary into a shareable matcher (built once, reused across
    /// requests). Returns `None` when there is nothing to match.
    pub fn compile(terms: &[CustomPiiTerm]) -> Option<Arc<Self>> {
        Self::build(terms).map(Arc::new)
    }

    /// Build the automaton from a dictionary. Empty terms are dropped and
    /// duplicate needles collapse to the last entry (mirrors API normalization).
    /// Returns `None` when there is nothing to match.
    fn build(terms: &[CustomPiiTerm]) -> Option<Self> {
        let mut ordered: Vec<(String, &CustomPiiTerm)> = Vec::new();
        for term in terms {
            let needle = term.needle();
            if needle.is_empty() {
                continue;
            }
            if let Some(slot) = ordered.iter_mut().find(|(existing, _)| *existing == needle) {
                slot.1 = term;
            } else {
                ordered.push((needle, term));
            }
        }
        if ordered.is_empty() {
            return None;
        }

        let patterns = ordered.iter().map(|(needle, _)| needle.as_str()).collect::<Vec<_>>();
        // `Standard` match kind is required for overlapping search, which is how
        // we guarantee every term is seen even when terms nest.
        let automaton = AhoCorasick::builder().ascii_case_insensitive(true).match_kind(MatchKind::Standard).build(&patterns).ok()?;
        let meta = ordered
            .iter()
            .map(|(_, term)| CustomTermMeta {
                term: term.term.trim().to_string(),
                action: term.action,
                placeholder: term.placeholder(),
            })
            .collect::<Vec<_>>();
        let has_redaction = meta.iter().any(|entry| entry.action == PolicyAction::Redact);
        Some(Self { automaton, meta, has_redaction })
    }

    /// Aggregate match counts per term. Uses overlapping search so a nested term
    /// (e.g. a `block` term inside a longer one) is never hidden.
    fn scan(&self, text: &str) -> Vec<CustomPiiMatch> {
        let mut counts = vec![0u32; self.meta.len()];
        for hit in self.automaton.find_overlapping_iter(text) {
            counts[hit.pattern()] = counts[hit.pattern()].saturating_add(1);
        }
        counts
            .iter()
            .enumerate()
            .filter(|&(_, &count)| count > 0)
            .map(|(idx, &count)| CustomPiiMatch {
                term: self.meta[idx].term.clone(),
                action: self.meta[idx].action,
                count,
            })
            .collect()
    }

    /// Byte ranges of `Redact`-action terms, with placeholders, for redaction.
    fn redaction_ranges(&self, text: &str) -> Vec<(usize, usize, String)> {
        if !self.has_redaction {
            return Vec::new();
        }
        self.automaton
            .find_overlapping_iter(text)
            .filter(|hit| self.meta[hit.pattern()].action == PolicyAction::Redact)
            .filter(|hit| text.is_char_boundary(hit.start()) && text.is_char_boundary(hit.end()))
            .map(|hit| (hit.start(), hit.end(), self.meta[hit.pattern()].placeholder.clone()))
            .collect()
    }
}

/// Strictness order for merging a term that appears in more than one
/// dictionary layer: Block beats Redact beats AuditOnly beats Allow.
fn stricter_action(left: PolicyAction, right: PolicyAction) -> PolicyAction {
    fn rank(action: PolicyAction) -> u8 {
        match action {
            PolicyAction::Allow => 0,
            PolicyAction::AuditOnly => 1,
            PolicyAction::Redact => 2,
            PolicyAction::Block => 3,
        }
    }
    if rank(left) >= rank(right) { left } else { right }
}

/// PII scanner for LLM prompt and completion content. Built-in regex patterns
/// are always applied; zero or more custom dictionaries (compiled Aho-Corasick
/// automatons) add literal, case-insensitive terms with their own enforcement
/// actions. Layers — e.g. an org-wide dictionary plus a per-agent one — are
/// scanned together. Matchers are shared via `Arc`, so cloning is cheap.
#[derive(Debug, Clone, Default)]
pub struct LlmPiiScanner {
    custom: Vec<Arc<CustomPiiMatcher>>,
}

impl LlmPiiScanner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a scanner with a single custom dictionary compiled from `terms`.
    /// Prefer the pre-compiled constructors on the hot path to avoid rebuilding
    /// the automaton per request.
    pub fn with_custom_terms(custom_terms: Vec<CustomPiiTerm>) -> Self {
        Self {
            custom: CustomPiiMatcher::compile(&custom_terms).into_iter().collect(),
        }
    }

    /// Build a scanner from a single pre-compiled dictionary (or none).
    pub fn with_compiled_dictionary(dictionary: Option<Arc<CustomPiiMatcher>>) -> Self {
        Self { custom: dictionary.into_iter().collect() }
    }

    /// Build a scanner that applies several pre-compiled dictionaries together
    /// (e.g. an org-wide layer plus a per-agent layer). When the same term
    /// appears in more than one layer the counts merge and the strictest action
    /// wins.
    pub fn with_compiled_dictionaries(dictionaries: Vec<Arc<CustomPiiMatcher>>) -> Self {
        Self { custom: dictionaries }
    }

    /// True when any layer contains a `Redact`-action term.
    fn has_custom_redaction(&self) -> bool {
        self.custom.iter().any(|matcher| matcher.has_redaction)
    }

    /// Case-insensitive aggregate matches across all dictionary layers, merged
    /// per term (strictest action wins on a collision).
    fn scan_custom_terms(&self, text: &str) -> Vec<CustomPiiMatch> {
        let mut combined: Vec<CustomPiiMatch> = Vec::new();
        for matcher in &self.custom {
            for matched in matcher.scan(text) {
                if let Some(existing) = combined.iter_mut().find(|entry| entry.term.eq_ignore_ascii_case(&matched.term)) {
                    existing.count = existing.count.saturating_add(matched.count);
                    existing.action = stricter_action(existing.action, matched.action);
                } else {
                    combined.push(matched);
                }
            }
        }
        combined
    }

    /// Byte ranges of `Redact`-action custom terms across all layers, with placeholders.
    fn custom_redaction_ranges(&self, text: &str) -> Vec<(usize, usize, String)> {
        self.custom.iter().flat_map(|matcher| matcher.redaction_ranges(text)).collect()
    }

    /// Scan a single text segment for PII matches.
    pub fn scan_text(&self, text: &str) -> PiiScanResult {
        let mut findings = Vec::new();

        for (pii_type, regex) in PII_PATTERNS.iter() {
            for capture in regex.find_iter(text) {
                findings.push(PiiFinding {
                    pii_type: *pii_type,
                    start: capture.start(),
                    end: capture.end(),
                });
            }
        }

        findings
            .sort_by(|left, right| left.start.cmp(&right.start).then(right.end.cmp(&left.end)).then(left.pii_type.cmp(&right.pii_type)));

        let mut types = findings.iter().map(|finding| finding.pii_type).collect::<Vec<_>>();
        types.sort();
        types.dedup();

        let custom_matches = self.scan_custom_terms(text);

        PiiScanResult {
            detected: !findings.is_empty() || !custom_matches.is_empty(),
            types,
            count: u32::try_from(findings.len()).unwrap_or(u32::MAX),
            findings,
            custom_matches,
        }
    }

    /// Replace detected PII in a text segment with placeholders. Built-in
    /// findings carry type-specific placeholders; custom `Redact`-action terms
    /// (re-located in `text`) use their dictionary placeholder. The two sets are
    /// merged and applied in a single left-to-right pass.
    pub fn redact_text(&self, text: &str, result: &PiiScanResult) -> String {
        let mut ranges: Vec<(usize, usize, String)> = Vec::new();
        for finding in &result.findings {
            ranges.push((finding.start, finding.end, finding.pii_type.placeholder().to_string()));
        }
        ranges.extend(self.custom_redaction_ranges(text));

        if ranges.is_empty() {
            return text.to_string();
        }
        ranges.sort_by(|left, right| left.0.cmp(&right.0).then(right.1.cmp(&left.1)));

        let mut redacted = String::with_capacity(text.len());
        let mut cursor = 0usize;
        for (start, end, placeholder) in ranges {
            if start < cursor || end > text.len() || !text.is_char_boundary(start) || !text.is_char_boundary(end) {
                continue;
            }
            redacted.push_str(&text[cursor..start]);
            redacted.push_str(&placeholder);
            cursor = end;
        }

        redacted.push_str(&text[cursor..]);
        redacted
    }

    /// Scan user prompt content and tool results in a message list.
    ///
    /// Assistant messages, tool definitions, and system prompts are intentionally excluded.
    pub fn scan_messages(&self, messages: &[LlmMessage]) -> PiiScanResult {
        let mut result = PiiScanResult::default();

        for message in messages {
            if message.role != LlmMessageRole::User {
                continue;
            }

            match &message.kind {
                LlmMessageKind::ToolResult { calls } => {
                    if calls.is_empty() {
                        result.merge(&self.scan_text(&message.content));
                        continue;
                    }

                    let mut any_argument = false;
                    for call in calls {
                        if call.function.arguments.is_empty() {
                            continue;
                        }
                        any_argument = true;
                        result.merge(&self.scan_text(&call.function.arguments));
                    }

                    if !any_argument && !message.content.is_empty() {
                        result.merge(&self.scan_text(&message.content));
                    }
                }
                _ => {
                    if !message.content.is_empty() {
                        result.merge(&self.scan_text(&message.content));
                    }
                }
            }
        }

        result.detected = result.count > 0 || !result.custom_matches.is_empty();
        result
    }

    /// Redact user prompt content and tool results in-place.
    pub fn redact_messages(&self, messages: &mut [LlmMessage], result: &PiiScanResult) {
        if !result.detected {
            return;
        }

        for message in messages {
            if message.role != LlmMessageRole::User {
                continue;
            }

            match &mut message.kind {
                LlmMessageKind::ToolResult { calls } => {
                    for call in calls {
                        if call.function.arguments.is_empty() {
                            continue;
                        }
                        let scan = self.scan_text(&call.function.arguments);
                        if scan.detected {
                            call.function.arguments = self.redact_text(&call.function.arguments, &scan);
                        }
                    }

                    if !message.content.is_empty() {
                        let scan = self.scan_text(&message.content);
                        if scan.detected {
                            message.content = self.redact_text(&message.content, &scan);
                        }
                    }
                }
                _ => {
                    if !message.content.is_empty() {
                        let scan = self.scan_text(&message.content);
                        if scan.detected {
                            message.content = self.redact_text(&message.content, &scan);
                        }
                    }
                }
            }
        }
    }

    /// Redact only custom `Redact`-action dictionary terms in user content,
    /// leaving built-in PII untouched. Used when the agent's base PII policy is
    /// audit/allow but a custom term still requires masking.
    pub fn redact_custom_terms(&self, messages: &mut [LlmMessage]) {
        if !self.has_custom_redaction() {
            return;
        }
        let empty = PiiScanResult::default();
        for message in messages {
            if message.role != LlmMessageRole::User {
                continue;
            }
            match &mut message.kind {
                LlmMessageKind::ToolResult { calls } => {
                    for call in calls {
                        if !call.function.arguments.is_empty() {
                            call.function.arguments = self.redact_text(&call.function.arguments, &empty);
                        }
                    }
                    if !message.content.is_empty() {
                        message.content = self.redact_text(&message.content, &empty);
                    }
                }
                _ => {
                    if !message.content.is_empty() {
                        message.content = self.redact_text(&message.content, &empty);
                    }
                }
            }
        }
    }

    /// Scan completion content emitted by the assistant, including tool-call arguments.
    pub fn scan_completion_message(&self, message: &LlmMessage) -> PiiScanResult {
        let mut result = PiiScanResult::default();

        if !message.content.is_empty() {
            result.merge(&self.scan_text(&message.content));
        }

        match &message.kind {
            LlmMessageKind::ToolUse { calls } | LlmMessageKind::ToolResult { calls } => {
                for call in calls {
                    if !call.function.arguments.is_empty() {
                        result.merge(&self.scan_text(&call.function.arguments));
                    }
                }
            }
            _ => {}
        }

        result.detected = result.count > 0 || !result.custom_matches.is_empty();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{LlmFunctionCall, LlmToolCall};

    #[test]
    fn scans_text_and_collects_types() {
        let scanner = LlmPiiScanner::new();
        let result = scanner.scan_text("alice@example.com called 555-123-4567 from 192.168.1.20");

        assert!(result.detected);
        assert_eq!(result.count, 3);
        assert!(result.types.contains(&PiiType::Email));
        assert!(result.types.contains(&PiiType::Phone));
        assert!(result.types.contains(&PiiType::IpAddress));
    }

    #[test]
    fn redacts_with_type_specific_placeholders() {
        let scanner = LlmPiiScanner::new();
        let text = "Contact alice@example.com and use api_key_abcdefghijklmnopqrstuvwxyz";
        let scan = scanner.scan_text(text);
        let redacted = scanner.redact_text(text, &scan);

        assert_eq!(redacted, "Contact [REDACTED_EMAIL] and use [REDACTED_API_KEY]");
    }

    #[test]
    fn scans_only_user_messages_and_tool_results() {
        let scanner = LlmPiiScanner::new();
        let messages = vec![
            LlmMessage {
                role: LlmMessageRole::Assistant,
                content: "assistant@example.com".to_string(),
                kind: LlmMessageKind::Text,
            },
            LlmMessage {
                role: LlmMessageRole::User,
                content: "user@example.com".to_string(),
                kind: LlmMessageKind::Text,
            },
            LlmMessage {
                role: LlmMessageRole::User,
                content: String::new(),
                kind: LlmMessageKind::ToolResult {
                    calls: vec![LlmToolCall {
                        id: "call_1".to_string(),
                        call_type: "function".to_string(),
                        function: LlmFunctionCall {
                            name: "lookup".to_string(),
                            arguments: "4111 1111 1111 1111".to_string(),
                        },
                    }],
                },
            },
        ];

        let result = scanner.scan_messages(&messages);
        assert_eq!(result.count, 2);
        assert!(result.types.contains(&PiiType::Email));
        assert!(result.types.contains(&PiiType::CreditCard));
    }

    #[test]
    fn redacts_messages_in_place() {
        let scanner = LlmPiiScanner::new();
        let mut messages = vec![LlmMessage {
            role: LlmMessageRole::User,
            content: "Reach me at alice@example.com".to_string(),
            kind: LlmMessageKind::Text,
        }];

        let result = scanner.scan_messages(&messages);
        scanner.redact_messages(&mut messages, &result);

        assert_eq!(messages[0].content, "Reach me at [REDACTED_EMAIL]");
    }

    #[test]
    fn scans_completion_content_and_tool_args() {
        let scanner = LlmPiiScanner::new();
        let message = LlmMessage {
            role: LlmMessageRole::Assistant,
            content: "Call user at 555-123-4567".to_string(),
            kind: LlmMessageKind::ToolUse {
                calls: vec![LlmToolCall {
                    id: "call_1".to_string(),
                    call_type: "function".to_string(),
                    function: LlmFunctionCall {
                        name: "notify".to_string(),
                        arguments: "{\"email\":\"assistant@example.com\"}".to_string(),
                    },
                }],
            },
        };

        let result = scanner.scan_completion_message(&message);
        assert_eq!(result.count, 2);
        assert!(result.types.contains(&PiiType::Phone));
        assert!(result.types.contains(&PiiType::Email));
    }

    fn custom_term(term: &str, action: PolicyAction) -> CustomPiiTerm {
        CustomPiiTerm { term: term.to_string(), action, label: None }
    }

    #[test]
    fn custom_terms_match_case_insensitively_and_record_action() {
        let scanner = LlmPiiScanner::with_custom_terms(vec![custom_term("Project Titan", PolicyAction::Block)]);
        let result = scanner.scan_text("notes about PROJECT titan and project titan");

        assert!(result.detected);
        assert!(!result.built_in_detected());
        assert_eq!(result.custom_matches.len(), 1);
        assert_eq!(result.custom_matches[0].action, PolicyAction::Block);
        assert_eq!(result.custom_matches[0].count, 2);
        assert!(result.type_names().contains(&"custom:Project Titan".to_string()));
    }

    #[test]
    fn redacts_custom_redact_terms_with_label_placeholder() {
        let scanner = LlmPiiScanner::with_custom_terms(vec![CustomPiiTerm {
            term: "acme-secret".to_string(),
            action: PolicyAction::Redact,
            label: Some("project".to_string()),
        }]);
        let text = "the acme-secret ships friday";
        let scan = scanner.scan_text(text);
        let redacted = scanner.redact_text(text, &scan);

        assert_eq!(redacted, "the [REDACTED_PROJECT] ships friday");
    }

    #[test]
    fn redacts_builtin_and_custom_together() {
        let scanner = LlmPiiScanner::with_custom_terms(vec![custom_term("widgetco", PolicyAction::Redact)]);
        let text = "email alice@example.com about widgetco";
        let scan = scanner.scan_text(text);
        let redacted = scanner.redact_text(text, &scan);

        assert_eq!(redacted, "email [REDACTED_EMAIL] about [REDACTED_TERM]");
    }

    #[test]
    fn audit_action_custom_term_is_recorded_but_not_redacted() {
        let scanner = LlmPiiScanner::with_custom_terms(vec![custom_term("internal-codename", PolicyAction::AuditOnly)]);
        let text = "ship internal-codename today";
        let scan = scanner.scan_text(text);

        assert!(scan.detected);
        assert_eq!(scan.custom_matches[0].action, PolicyAction::AuditOnly);
        // AuditOnly never contributes a redaction range.
        assert_eq!(scanner.redact_text(text, &scan), text);
    }

    #[test]
    fn redact_custom_terms_leaves_builtin_pii_intact() {
        let scanner = LlmPiiScanner::with_custom_terms(vec![custom_term("widgetco", PolicyAction::Redact)]);
        let mut messages = vec![LlmMessage {
            role: LlmMessageRole::User,
            content: "email alice@example.com about widgetco".to_string(),
            kind: LlmMessageKind::Text,
        }];

        scanner.redact_custom_terms(&mut messages);

        // Custom term masked; built-in email preserved (base policy was not redact).
        assert_eq!(messages[0].content, "email alice@example.com about [REDACTED_TERM]");
    }

    #[test]
    fn blank_custom_terms_are_dropped() {
        let scanner = LlmPiiScanner::with_custom_terms(vec![custom_term("   ", PolicyAction::Block)]);
        let result = scanner.scan_text("anything at all");

        assert!(!result.detected);
        assert!(result.custom_matches.is_empty());
    }

    #[test]
    fn layered_dictionaries_merge_with_strictest_action() {
        // org layer redacts "widgetco"; agent layer blocks it and adds "acme".
        let org = CustomPiiMatcher::compile(&[custom_term("widgetco", PolicyAction::Redact)]).expect("org matcher");
        let agent = CustomPiiMatcher::compile(&[
            custom_term("widgetco", PolicyAction::Block),
            custom_term("acme", PolicyAction::Redact),
        ])
        .expect("agent matcher");
        let scanner = LlmPiiScanner::with_compiled_dictionaries(vec![org, agent]);

        let result = scanner.scan_text("ping widgetco and acme");

        // "widgetco" matched in both layers: counts merge, strictest action (Block) wins.
        let widget = result.custom_matches.iter().find(|m| m.term.eq_ignore_ascii_case("widgetco")).expect("widgetco match");
        assert_eq!(widget.action, PolicyAction::Block);
        assert_eq!(widget.count, 2);
        assert!(result.custom_matches.iter().any(|m| m.term.eq_ignore_ascii_case("acme") && m.action == PolicyAction::Redact));
    }

    #[test]
    fn layered_dictionaries_redact_across_layers() {
        let org = CustomPiiMatcher::compile(&[custom_term("widgetco", PolicyAction::Redact)]).expect("org matcher");
        let agent = CustomPiiMatcher::compile(&[CustomPiiTerm {
            term: "acme".to_string(),
            action: PolicyAction::Redact,
            label: Some("vendor".to_string()),
        }])
        .expect("agent matcher");
        let scanner = LlmPiiScanner::with_compiled_dictionaries(vec![org, agent]);
        let text = "ship widgetco to acme today";

        let scan = scanner.scan_text(text);
        let redacted = scanner.redact_text(text, &scan);

        assert_eq!(redacted, "ship [REDACTED_TERM] to [REDACTED_VENDOR] today");
    }
}
