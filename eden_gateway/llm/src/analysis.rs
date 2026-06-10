use serde_json::Value;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct LlmPiiSummary {
    pub(super) email_count: u32,
    pub(super) phone_count: u32,
    pub(super) us_ssn_count: u32,
    pub(super) payment_card_count: u32,
}

impl LlmPiiSummary {
    pub(super) fn contains_pii(&self) -> bool {
        self.email_count > 0 || self.phone_count > 0 || self.us_ssn_count > 0 || self.payment_card_count > 0
    }

    pub(super) fn add_summary(&mut self, other: Self) {
        self.email_count = self.email_count.saturating_add(other.email_count);
        self.phone_count = self.phone_count.saturating_add(other.phone_count);
        self.us_ssn_count = self.us_ssn_count.saturating_add(other.us_ssn_count);
        self.payment_card_count = self.payment_card_count.saturating_add(other.payment_card_count);
    }

    fn add_text(&mut self, text: &str) {
        self.email_count = self.email_count.saturating_add(PiiPatternScanner::count_email_like(text));
        self.phone_count = self.phone_count.saturating_add(PiiPatternScanner::count_phone_like(text));
        self.us_ssn_count = self.us_ssn_count.saturating_add(PiiPatternScanner::count_us_ssn_like(text));
        self.payment_card_count = self.payment_card_count.saturating_add(PiiPatternScanner::count_payment_card_like(text));
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct LlmPayloadAnalysis {
    pub(super) message_count: u32,
    pub(super) system_message_count: u32,
    pub(super) user_message_count: u32,
    pub(super) assistant_message_count: u32,
    pub(super) tool_message_count: u32,
    pub(super) text_part_count: u32,
    pub(super) image_part_count: u32,
    pub(super) tool_definition_count: u32,
    pub(super) tool_call_count: u32,
    pub(super) prompt_characters: u32,
    pub(super) pii: LlmPiiSummary,
}

impl LlmPayloadAnalysis {
    pub(super) fn contains_pii(&self) -> bool {
        self.pii.contains_pii()
    }

    fn add_prompt_text(&mut self, text: &str) {
        self.text_part_count = self.text_part_count.saturating_add(1);
        self.prompt_characters = self.prompt_characters.saturating_add(text.chars().count().min(u32::MAX as usize) as u32);
        self.pii.add_text(text);
    }
}

pub(super) struct LlmPayloadInspector;

impl LlmPayloadInspector {
    pub(super) fn inspect_text(text: &str) -> LlmPiiSummary {
        let mut summary = LlmPiiSummary::default();
        summary.add_text(text);
        summary
    }

    pub(super) fn inspect_openai_chat_value(value: &Value) -> LlmPayloadAnalysis {
        let mut analysis = LlmPayloadAnalysis::default();

        if let Some(messages) = value.get("messages").and_then(Value::as_array) {
            for message in messages {
                Self::inspect_message(message, &mut analysis);
            }
        }

        analysis.tool_definition_count =
            value.get("tools").and_then(Value::as_array).map(|tools| tools.len().min(u32::MAX as usize) as u32).unwrap_or_default();

        analysis
    }

    fn inspect_message(message: &Value, analysis: &mut LlmPayloadAnalysis) {
        analysis.message_count = analysis.message_count.saturating_add(1);
        match message.get("role").and_then(Value::as_str).unwrap_or_default() {
            "system" | "developer" => analysis.system_message_count = analysis.system_message_count.saturating_add(1),
            "user" => analysis.user_message_count = analysis.user_message_count.saturating_add(1),
            "assistant" => analysis.assistant_message_count = analysis.assistant_message_count.saturating_add(1),
            "tool" => analysis.tool_message_count = analysis.tool_message_count.saturating_add(1),
            _ => {}
        }

        Self::inspect_content(message.get("content"), analysis);

        if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
            analysis.tool_call_count = analysis.tool_call_count.saturating_add(tool_calls.len().min(u32::MAX as usize) as u32);
            for tool_call in tool_calls {
                Self::inspect_tool_call(tool_call, analysis);
            }
        }
    }

    fn inspect_content(content: Option<&Value>, analysis: &mut LlmPayloadAnalysis) {
        match content {
            Some(Value::String(text)) => analysis.add_prompt_text(text),
            Some(Value::Array(parts)) => {
                for part in parts {
                    let part_type = part.get("type").and_then(Value::as_str).unwrap_or_default();
                    match part_type {
                        "text" | "input_text" => {
                            if let Some(text) = part.get("text").and_then(Value::as_str) {
                                analysis.add_prompt_text(text);
                            }
                        }
                        "image_url" | "input_image" => {
                            analysis.image_part_count = analysis.image_part_count.saturating_add(1);
                        }
                        _ => Self::scan_json_strings(part, analysis),
                    }
                }
            }
            Some(value) => Self::scan_json_strings(value, analysis),
            None => {}
        }
    }

    fn inspect_tool_call(tool_call: &Value, analysis: &mut LlmPayloadAnalysis) {
        if let Some(arguments) = tool_call.get("function").and_then(|function| function.get("arguments")) {
            Self::scan_json_strings(arguments, analysis);
        }
    }

    fn scan_json_strings(value: &Value, analysis: &mut LlmPayloadAnalysis) {
        match value {
            Value::String(text) => analysis.add_prompt_text(text),
            Value::Array(values) => {
                for value in values {
                    Self::scan_json_strings(value, analysis);
                }
            }
            Value::Object(map) => {
                for value in map.values() {
                    Self::scan_json_strings(value, analysis);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }
}

struct PiiPatternScanner;

impl PiiPatternScanner {
    fn count_email_like(text: &str) -> u32 {
        text.split(|ch: char| ch.is_whitespace() || matches!(ch, '<' | '>' | '"' | '\'' | ',' | ';' | '(' | ')' | '[' | ']'))
            .filter(|token| {
                let Some((local, domain)) = token.split_once('@') else {
                    return false;
                };
                !local.is_empty() && domain.contains('.') && domain.split('.').all(|part| !part.is_empty())
            })
            .count()
            .min(u32::MAX as usize) as u32
    }

    fn count_phone_like(text: &str) -> u32 {
        Self::digit_runs(text)
            .filter(|run| {
                let digit_count = run.chars().filter(|ch| ch.is_ascii_digit()).count();
                let has_phone_punctuation = run.chars().any(|ch| matches!(ch, '-' | '.' | '(' | ')' | ' ' | '+'));
                (10..=15).contains(&digit_count) && has_phone_punctuation && !Self::is_luhn_candidate(run)
            })
            .count()
            .min(u32::MAX as usize) as u32
    }

    fn count_us_ssn_like(text: &str) -> u32 {
        text.as_bytes()
            .windows(11)
            .filter(|window| {
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
            .count()
            .min(u32::MAX as usize) as u32
    }

    fn count_payment_card_like(text: &str) -> u32 {
        Self::digit_runs(text)
            .filter(|run| {
                let digits = run.chars().filter(|ch| ch.is_ascii_digit()).collect::<String>();
                (13..=19).contains(&digits.len()) && Self::passes_luhn(&digits)
            })
            .count()
            .min(u32::MAX as usize) as u32
    }

    fn digit_runs(text: &str) -> impl Iterator<Item = &str> {
        text.split(|ch: char| !(ch.is_ascii_digit() || matches!(ch, '-' | '.' | '(' | ')' | ' ' | '+')))
            .filter(|part| part.chars().any(|ch| ch.is_ascii_digit()))
    }

    fn is_luhn_candidate(text: &str) -> bool {
        let digits = text.chars().filter(|ch| ch.is_ascii_digit()).collect::<String>();
        (13..=19).contains(&digits.len()) && Self::passes_luhn(&digits)
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
}
