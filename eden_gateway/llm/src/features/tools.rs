use serde_json::Value;

pub(super) struct LlmToolPolicyInspector;

impl LlmToolPolicyInspector {
    pub(super) fn tool_names(body: &Value) -> Vec<String> {
        let mut names = Vec::new();
        Self::append_tool_definition_names(body, &mut names);
        Self::append_tool_choice_name(body, &mut names);
        Self::append_tool_call_names(body, &mut names);
        names
    }

    fn append_tool_definition_names(body: &Value, names: &mut Vec<String>) {
        let Some(tools) = body.get("tools").and_then(Value::as_array) else {
            return;
        };

        for tool in tools {
            if let Some(name) = tool.get("function").and_then(|function| function.get("name")).and_then(Value::as_str) {
                names.push(name.to_string());
            }
        }
    }

    fn append_tool_choice_name(body: &Value, names: &mut Vec<String>) {
        let Some(name) = body
            .get("tool_choice")
            .and_then(|choice| choice.get("function"))
            .and_then(|function| function.get("name"))
            .and_then(Value::as_str)
        else {
            return;
        };

        names.push(name.to_string());
    }

    fn append_tool_call_names(body: &Value, names: &mut Vec<String>) {
        let Some(messages) = body.get("messages").and_then(Value::as_array) else {
            return;
        };

        for message in messages {
            let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) else {
                continue;
            };

            for tool_call in tool_calls {
                if let Some(name) = tool_call.get("function").and_then(|function| function.get("name")).and_then(Value::as_str) {
                    names.push(name.to_string());
                }
            }
        }
    }
}
