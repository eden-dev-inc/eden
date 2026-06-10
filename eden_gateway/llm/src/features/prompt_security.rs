use super::LlmPromptSecurityRisk;
use serde_json::Value;

pub(super) struct PromptSecurityScanner;

impl PromptSecurityScanner {
    pub(super) fn inspect_json(value: &Value) -> LlmPromptSecurityRisk {
        let mut risk = LlmPromptSecurityRisk::default();
        Self::inspect_json_into(value, &mut risk);
        risk
    }

    fn inspect_json_into(value: &Value, risk: &mut LlmPromptSecurityRisk) {
        match value {
            Value::String(text) => risk.add_text(text),
            Value::Array(values) => {
                for value in values {
                    Self::inspect_json_into(value, risk);
                }
            }
            Value::Object(map) => {
                for value in map.values() {
                    Self::inspect_json_into(value, risk);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }

    pub(super) fn count_any(text: &str, needles: &[&str]) -> u32 {
        needles.iter().map(|needle| text.matches(needle).count().min(u32::MAX as usize) as u32).fold(0_u32, u32::saturating_add)
    }
}
