use crate::analysis::{LlmPayloadInspector, LlmPiiSummary};
use serde_json::Value;

pub(super) struct LlmPiiInspector;

impl LlmPiiInspector {
    pub(super) fn inspect_json(value: &Value) -> LlmPiiSummary {
        let mut summary = LlmPiiSummary::default();
        Self::inspect_json_into(value, &mut summary);
        summary
    }

    fn inspect_json_into(value: &Value, summary: &mut LlmPiiSummary) {
        match value {
            Value::String(text) => summary.add_summary(LlmPayloadInspector::inspect_text(text)),
            Value::Array(values) => {
                for value in values {
                    Self::inspect_json_into(value, summary);
                }
            }
            Value::Object(map) => {
                for value in map.values() {
                    Self::inspect_json_into(value, summary);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }
}

pub(super) struct LlmPiiRedactor;

impl LlmPiiRedactor {
    pub(super) fn redact_json_strings(value: &mut Value) -> LlmPiiSummary {
        let mut summary = LlmPiiSummary::default();
        Self::redact_json_strings_into(value, &mut summary);
        summary
    }

    fn redact_json_strings_into(value: &mut Value, summary: &mut LlmPiiSummary) {
        match value {
            Value::String(text) => {
                let pii = LlmPayloadInspector::inspect_text(text);
                if pii.contains_pii() {
                    *text = Self::redact_text(text);
                    summary.add_summary(pii);
                }
            }
            Value::Array(values) => {
                for value in values {
                    Self::redact_json_strings_into(value, summary);
                }
            }
            Value::Object(map) => {
                for value in map.values_mut() {
                    Self::redact_json_strings_into(value, summary);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }

    pub(super) fn redact_text(text: &str) -> String {
        let mut ranges = Vec::new();
        ranges.extend(Self::email_ranges(text));
        ranges.extend(Self::sensitive_digit_ranges(text));
        Self::replace_ranges(text, ranges)
    }

    fn email_ranges(text: &str) -> Vec<(usize, usize, &'static str)> {
        let mut ranges = Vec::new();
        let mut token_start = None;

        for (index, ch) in text.char_indices() {
            if Self::email_boundary(ch) {
                if let Some(start) = token_start.take() {
                    Self::push_email_range(text, start, index, &mut ranges);
                }
                continue;
            }

            token_start.get_or_insert(index);
        }

        if let Some(start) = token_start {
            Self::push_email_range(text, start, text.len(), &mut ranges);
        }

        ranges
    }

    fn push_email_range(text: &str, start: usize, end: usize, ranges: &mut Vec<(usize, usize, &'static str)>) {
        let token = &text[start..end];
        let Some((local, domain)) = token.split_once('@') else {
            return;
        };

        if !local.is_empty() && domain.contains('.') && domain.split('.').all(|part| !part.is_empty()) {
            ranges.push((start, end, "[REDACTED_EMAIL]"));
        }
    }

    fn email_boundary(ch: char) -> bool {
        ch.is_whitespace() || matches!(ch, '<' | '>' | '"' | '\'' | ',' | ';' | '(' | ')' | '[' | ']')
    }

    fn sensitive_digit_ranges(text: &str) -> Vec<(usize, usize, &'static str)> {
        let mut ranges = Vec::new();
        let mut start = None;

        for (index, ch) in text.char_indices() {
            if Self::digit_run_char(ch) {
                start.get_or_insert(index);
                continue;
            }

            if let Some(run_start) = start.take() {
                Self::push_digit_range(text, run_start, index, &mut ranges);
            }
        }

        if let Some(run_start) = start {
            Self::push_digit_range(text, run_start, text.len(), &mut ranges);
        }

        ranges
    }

    fn push_digit_range(text: &str, start: usize, end: usize, ranges: &mut Vec<(usize, usize, &'static str)>) {
        let run = &text[start..end];
        let digits = run.chars().filter(|ch| ch.is_ascii_digit()).collect::<String>();
        let has_phone_punctuation = run.chars().any(|ch| matches!(ch, '-' | '.' | '(' | ')' | ' ' | '+'));

        if Self::is_us_ssn(run) {
            ranges.push((start, end, "[REDACTED_SSN]"));
            return;
        }

        if (13..=19).contains(&digits.len()) && Self::passes_luhn(&digits) {
            ranges.push((start, end, "[REDACTED_PAYMENT_CARD]"));
            return;
        }

        if (10..=15).contains(&digits.len()) && has_phone_punctuation && !Self::passes_luhn(&digits) {
            ranges.push((start, end, "[REDACTED_PHONE]"));
        }
    }

    fn digit_run_char(ch: char) -> bool {
        ch.is_ascii_digit() || matches!(ch, '-' | '.' | '(' | ')' | ' ' | '+')
    }

    fn is_us_ssn(text: &str) -> bool {
        text.as_bytes().windows(11).any(|window| {
            window[0].is_ascii_digit()
                && window[1].is_ascii_digit()
                && window[2].is_ascii_digit()
                && window[3] == b'-'
                && window[4].is_ascii_digit()
                && window[5].is_ascii_digit()
                && window[6] == b'-'
                && window[7].is_ascii_digit()
                && window[8].is_ascii_digit()
                && window[9].is_ascii_digit()
                && window[10].is_ascii_digit()
        })
    }

    fn passes_luhn(digits: &str) -> bool {
        let mut sum = 0_u32;
        let mut double = false;
        for byte in digits.bytes().rev() {
            let mut value = u32::from(byte.saturating_sub(b'0'));
            if double {
                value *= 2;
                if value > 9 {
                    value -= 9;
                }
            }
            sum = sum.saturating_add(value);
            double = !double;
        }
        sum > 0 && sum.is_multiple_of(10)
    }

    fn replace_ranges(text: &str, mut ranges: Vec<(usize, usize, &'static str)>) -> String {
        if ranges.is_empty() {
            return text.to_string();
        }

        ranges.sort_by_key(|(start, end, _)| (*start, *end));
        let ranges = Self::dedupe_ranges(ranges);
        let mut redacted = text.to_string();
        for (start, end, replacement) in ranges.into_iter().rev() {
            redacted.replace_range(start..end, replacement);
        }
        redacted
    }

    fn dedupe_ranges(ranges: Vec<(usize, usize, &'static str)>) -> Vec<(usize, usize, &'static str)> {
        let mut deduped = Vec::new();
        let mut last_end = 0;
        for (start, end, replacement) in ranges {
            if start < last_end {
                continue;
            }
            last_end = end;
            deduped.push((start, end, replacement));
        }
        deduped
    }
}
