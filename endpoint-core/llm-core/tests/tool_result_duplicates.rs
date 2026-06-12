use llm_core::types::{LlmFunctionCall, LlmMessage, LlmMessageKind, LlmMessageRole, LlmToolCall};
use std::collections::HashSet;

/// Helper to check for duplicate call IDs in ToolResult messages
fn check_tool_result_duplicates(messages: &[LlmMessage]) -> Vec<String> {
    let mut all_tool_result_ids: Vec<String> = Vec::new();

    for message in messages {
        if let LlmMessageKind::ToolResult { calls } = &message.kind {
            for call in calls {
                all_tool_result_ids.push(call.id.clone());
            }
        }
    }

    // Find duplicates
    let mut seen = HashSet::new();
    let mut duplicates = Vec::new();
    for id in &all_tool_result_ids {
        if !seen.insert(id.clone()) {
            duplicates.push(id.clone());
        }
    }
    duplicates
}

/// Check for duplicate call IDs within a single ToolResult message
fn check_single_message_duplicates(message: &LlmMessage) -> Vec<String> {
    if let LlmMessageKind::ToolResult { calls } = &message.kind {
        let mut seen = HashSet::new();
        let mut duplicates = Vec::new();
        for call in calls {
            if !seen.insert(call.id.clone()) {
                duplicates.push(call.id.clone());
            }
        }
        return duplicates;
    }
    Vec::new()
}

/// Test that a properly formatted ToolResult message has no duplicate call IDs
#[test]
fn single_tool_result_no_duplicates() {
    let tool_call_id = "toolu_test_12345";

    let tool_result_msg = LlmMessage {
        role: LlmMessageRole::User,
        content: String::new(),
        kind: LlmMessageKind::ToolResult {
            calls: vec![LlmToolCall {
                id: tool_call_id.to_string(),
                call_type: "function".to_string(),
                function: LlmFunctionCall {
                    name: "test_tool".to_string(),
                    arguments: "Result text".to_string(),
                },
            }],
        },
    };

    let duplicates = check_single_message_duplicates(&tool_result_msg);
    assert!(duplicates.is_empty(), "Expected no duplicates, found: {:?}", duplicates);
}

/// Test that multiple calls in a single ToolResult are handled correctly
#[test]
fn multiple_calls_single_message_no_duplicates() {
    let tool_result_msg = LlmMessage {
        role: LlmMessageRole::User,
        content: String::new(),
        kind: LlmMessageKind::ToolResult {
            calls: vec![
                LlmToolCall {
                    id: "call_1".to_string(),
                    call_type: "function".to_string(),
                    function: LlmFunctionCall {
                        name: "tool_a".to_string(),
                        arguments: "Result 1".to_string(),
                    },
                },
                LlmToolCall {
                    id: "call_2".to_string(),
                    call_type: "function".to_string(),
                    function: LlmFunctionCall {
                        name: "tool_b".to_string(),
                        arguments: "Result 2".to_string(),
                    },
                },
                LlmToolCall {
                    id: "call_3".to_string(),
                    call_type: "function".to_string(),
                    function: LlmFunctionCall {
                        name: "tool_c".to_string(),
                        arguments: "Result 3".to_string(),
                    },
                },
            ],
        },
    };

    let duplicates = check_single_message_duplicates(&tool_result_msg);
    assert!(duplicates.is_empty(), "Expected no duplicates, found: {:?}", duplicates);
}

/// Test that detects if duplicate call IDs are accidentally added
#[test]
fn detect_duplicate_call_ids() {
    // This test verifies our duplicate detection works
    let tool_result_msg = LlmMessage {
        role: LlmMessageRole::User,
        content: String::new(),
        kind: LlmMessageKind::ToolResult {
            calls: vec![
                LlmToolCall {
                    id: "call_1".to_string(),
                    call_type: "function".to_string(),
                    function: LlmFunctionCall {
                        name: "tool_a".to_string(),
                        arguments: "Result 1".to_string(),
                    },
                },
                LlmToolCall {
                    id: "call_1".to_string(), // DUPLICATE!
                    call_type: "function".to_string(),
                    function: LlmFunctionCall {
                        name: "tool_a".to_string(),
                        arguments: "Result 1 duplicate".to_string(),
                    },
                },
            ],
        },
    };

    let duplicates = check_single_message_duplicates(&tool_result_msg);
    assert_eq!(duplicates.len(), 1, "Expected 1 duplicate, found: {:?}", duplicates);
    assert_eq!(duplicates[0], "call_1");
}

/// Test that checks a full conversation for duplicate tool_result IDs across messages
#[test]
fn full_conversation_no_duplicates() {
    let conversation = vec![
        LlmMessage {
            role: LlmMessageRole::User,
            content: "Please call tools".to_string(),
            kind: LlmMessageKind::Text,
        },
        LlmMessage {
            role: LlmMessageRole::Assistant,
            content: String::new(),
            kind: LlmMessageKind::ToolUse {
                calls: vec![
                    LlmToolCall {
                        id: "call_1".to_string(),
                        call_type: "function".to_string(),
                        function: LlmFunctionCall { name: "tool_a".to_string(), arguments: "{}".to_string() },
                    },
                    LlmToolCall {
                        id: "call_2".to_string(),
                        call_type: "function".to_string(),
                        function: LlmFunctionCall { name: "tool_b".to_string(), arguments: "{}".to_string() },
                    },
                ],
            },
        },
        LlmMessage {
            role: LlmMessageRole::User,
            content: String::new(),
            kind: LlmMessageKind::ToolResult {
                calls: vec![
                    LlmToolCall {
                        id: "call_1".to_string(),
                        call_type: "function".to_string(),
                        function: LlmFunctionCall {
                            name: "tool_a".to_string(),
                            arguments: "Result 1".to_string(),
                        },
                    },
                    LlmToolCall {
                        id: "call_2".to_string(),
                        call_type: "function".to_string(),
                        function: LlmFunctionCall {
                            name: "tool_b".to_string(),
                            arguments: "Result 2".to_string(),
                        },
                    },
                ],
            },
        },
        LlmMessage {
            role: LlmMessageRole::Assistant,
            content: "Done!".to_string(),
            kind: LlmMessageKind::Text,
        },
    ];

    let duplicates = check_tool_result_duplicates(&conversation);
    assert!(duplicates.is_empty(), "Expected no duplicates in conversation, found: {:?}", duplicates);
}

/// Test that detects duplicates across multiple ToolResult messages in a conversation
#[test]
fn detect_cross_message_duplicates() {
    // This simulates the bug scenario where the same tool_result_id appears in
    // multiple ToolResult messages
    let conversation = vec![
        LlmMessage {
            role: LlmMessageRole::User,
            content: "First request".to_string(),
            kind: LlmMessageKind::Text,
        },
        LlmMessage {
            role: LlmMessageRole::Assistant,
            content: String::new(),
            kind: LlmMessageKind::ToolUse {
                calls: vec![LlmToolCall {
                    id: "call_1".to_string(),
                    call_type: "function".to_string(),
                    function: LlmFunctionCall { name: "tool_a".to_string(), arguments: "{}".to_string() },
                }],
            },
        },
        // First ToolResult (e.g., from server generating "Tool runtime unavailable")
        LlmMessage {
            role: LlmMessageRole::User,
            content: String::new(),
            kind: LlmMessageKind::ToolResult {
                calls: vec![LlmToolCall {
                    id: "call_1".to_string(),
                    call_type: "function".to_string(),
                    function: LlmFunctionCall {
                        name: "tool_a".to_string(),
                        arguments: "Tool runtime unavailable".to_string(),
                    },
                }],
            },
        },
        // Second ToolResult (from client executing the tool)
        // THIS IS THE DUPLICATE!
        LlmMessage {
            role: LlmMessageRole::User,
            content: String::new(),
            kind: LlmMessageKind::ToolResult {
                calls: vec![LlmToolCall {
                    id: "call_1".to_string(), // SAME ID as above!
                    call_type: "function".to_string(),
                    function: LlmFunctionCall {
                        name: "tool_a".to_string(),
                        arguments: "Actual tool result".to_string(),
                    },
                }],
            },
        },
    ];

    let duplicates = check_tool_result_duplicates(&conversation);
    assert_eq!(duplicates.len(), 1, "Expected 1 duplicate, found: {:?}", duplicates);
    assert_eq!(duplicates[0], "call_1");
}

/// Test the comm::tool_result_message function creates messages correctly
#[test]
fn tool_result_message_helper_no_duplicates() {
    use llm_core::comm::tool_result_message;

    let call = LlmToolCall {
        id: "toolu_test".to_string(),
        call_type: "function".to_string(),
        function: LlmFunctionCall { name: "my_tool".to_string(), arguments: "{}".to_string() },
    };

    let result_msg = tool_result_message(&call, "Success!".to_string());

    // Verify the message structure
    assert_eq!(result_msg.role, LlmMessageRole::User);
    assert_eq!(result_msg.content, "Success!");

    if let LlmMessageKind::ToolResult { calls } = &result_msg.kind {
        assert_eq!(calls.len(), 1, "Should have exactly one call");
        assert_eq!(calls[0].id, "toolu_test");
        assert_eq!(calls[0].function.arguments, "Success!");
    } else {
        panic!("Expected ToolResult kind");
    }

    let duplicates = check_single_message_duplicates(&result_msg);
    assert!(duplicates.is_empty());
}
