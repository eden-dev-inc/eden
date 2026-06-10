use std::sync::Mutex;
use std::sync::{Arc, RwLock};

use futures::StreamExt;
use llm_core::comm::LlmClient;
use llm_core::config::DEFAULT_MAX_TOOL_PASSES;
use llm_core::connection::{AzureMaxTokensField, AzureOpenAiClassicConfig, LlmConnectionDefaults, LlmProvider};
use llm_core::credential::{ResolvedLlmConnection, ResolvedProviderConfig};
use llm_core::types::{LlmFunctionToolDefinition, LlmInvocation, LlmMessage, LlmMessageKind, LlmMessageRole, LlmToolDefinition};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{Duration, timeout};

fn resolved_connection_for_provider_with_model(base_url: &str, provider: LlmProvider, model: &str) -> Arc<RwLock<ResolvedLlmConnection>> {
    Arc::new(RwLock::new(ResolvedLlmConnection {
        provider,
        credential_id: None,
        api_key: Some("test-key".to_string()),
        credential_base_url: None,
        defaults: LlmConnectionDefaults {
            model: model.to_string(),
            base_url_override: Some(base_url.to_string()),
            ..Default::default()
        },
        provider_config: ResolvedProviderConfig::None,
    }))
}

fn resolved_connection_for_provider(base_url: &str, provider: LlmProvider) -> Arc<RwLock<ResolvedLlmConnection>> {
    resolved_connection_for_provider_with_model(base_url, provider, "gpt-4o-mini")
}

fn resolved_connection(base_url: &str) -> Arc<RwLock<ResolvedLlmConnection>> {
    resolved_connection_for_provider(base_url, LlmProvider::OpenAI)
}

async fn spawn_http_response_server(content_type: &'static str, body: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let mut request = vec![0_u8; 8192];
        let _ = socket.read(&mut request).await.expect("read request");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.expect("write response");
    });
    format!("http://{addr}")
}

async fn spawn_http_capture_server(content_type: &'static str, body: String) -> (String, Arc<Mutex<Option<serde_json::Value>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let captured = Arc::new(Mutex::new(None));
    let captured_for_server = Arc::clone(&captured);
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let mut request = vec![0_u8; 8192];
        let read = socket.read(&mut request).await.expect("read request");
        let request_text = String::from_utf8_lossy(&request[..read]);
        if let Some(body_start) = request_text.find("\r\n\r\n") {
            let body_text = &request_text[body_start + 4..];
            let payload = serde_json::from_str(body_text).expect("parse captured request json");
            *captured_for_server.lock().expect("capture lock") = Some(payload);
        }
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.expect("write response");
    });
    (format!("http://{addr}"), captured)
}

#[tokio::test]
async fn chat_stream_returns_deltas_and_usage() {
    let stream_body = concat!(
        "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{}}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5}}\n\n",
        "data: [DONE]\n\n"
    );
    let base = spawn_http_response_server("text/event-stream", stream_body.to_string()).await;
    let client = LlmClient::new(resolved_connection(&base), DEFAULT_MAX_TOOL_PASSES).expect("client init");
    let invocation = LlmInvocation {
        conversation: vec![LlmMessage {
            role: LlmMessageRole::User,
            content: "hi".to_string(),
            kind: LlmMessageKind::Text,
        }],
        ..Default::default()
    };

    let mut stream = client.chat_stream(&invocation).await.expect("stream");
    let mut text = String::new();
    let mut usage = None;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("chunk ok");
        if let Some(delta) = chunk.delta {
            text.push_str(&delta);
        }
        if let Some(u) = chunk.usage {
            usage = Some(u);
        }
    }

    assert_eq!(text, "Hello world");
    let usage = usage.expect("usage present");
    assert_eq!(usage.total_tokens, 5);
    assert_eq!(usage.prompt_tokens, 3);
    assert_eq!(usage.completion_tokens, 2);
}

#[tokio::test]
async fn chat_stream_emits_tool_calls() {
    let stream_body = concat!(
        "data: {\"id\":\"chatcmpl-2\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"id\":\"call_test\",\"type\":\"function\",\"function\":{\"name\":\"echo\",\"arguments\":\"{\\\\\\\"text\\\\\\\":\\\\\\\"ping\\\\\\\"}\"}}]}}]}\n\n",
        "data: [DONE]\n\n"
    );
    let base = spawn_http_response_server("text/event-stream", stream_body.to_string()).await;
    let client = LlmClient::new(resolved_connection(&base), DEFAULT_MAX_TOOL_PASSES).expect("client init");
    let invocation = LlmInvocation {
        conversation: vec![LlmMessage {
            role: LlmMessageRole::User,
            content: "call echo".to_string(),
            kind: LlmMessageKind::Text,
        }],
        tools: vec![LlmToolDefinition {
            r#type: "function".to_string(),
            function: LlmFunctionToolDefinition {
                name: "echo".to_string(),
                description: Some("Echo text".to_string()),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "text": { "type": "string" }
                    }
                }),
                ..Default::default()
            },
        }],
        ..Default::default()
    };

    let mut stream = client.chat_stream(&invocation).await.expect("stream");
    let first = stream.next().await.expect("first chunk").expect("ok chunk");
    assert_eq!(first.tool_calls.len(), 1);
    let call = &first.tool_calls[0];
    assert_eq!(call.id, "call_test");
    assert_eq!(call.function.name, "echo");
    assert!(call.function.arguments.contains("ping"), "arguments should include payload");
}

#[tokio::test]
async fn ollama_chat_parses_native_thinking_tool_calls_and_usage() {
    let base = spawn_http_response_server(
        "application/json",
        json!({
            "message": {
                "role": "assistant",
                "content": "",
                "thinking": "planning",
                "tool_calls": [{
                    "id": "call_test",
                    "type": "function",
                    "function": {
                        "index": 0,
                        "name": "echo",
                        "arguments": { "text": "ping" }
                    }
                }]
            },
            "done": true,
            "prompt_eval_count": 11,
            "eval_count": 5
        })
        .to_string(),
    )
    .await;
    let client =
        LlmClient::new(resolved_connection_for_provider(&base, LlmProvider::Ollama), DEFAULT_MAX_TOOL_PASSES).expect("client init");
    let invocation = LlmInvocation {
        conversation: vec![LlmMessage {
            role: LlmMessageRole::User,
            content: "call echo".to_string(),
            kind: LlmMessageKind::Text,
        }],
        ..Default::default()
    };

    let response = client.chat(&invocation).await.expect("chat");
    assert_eq!(response.thinking.as_deref(), Some("planning"));
    let usage = response.usage.expect("usage");
    assert_eq!(usage.prompt_tokens, 11);
    assert_eq!(usage.completion_tokens, 5);
    assert_eq!(usage.total_tokens, 16);

    match response.message.kind {
        LlmMessageKind::ToolUse { calls } => {
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].id, "call_test");
            assert_eq!(calls[0].function.name, "echo");
            assert!(calls[0].function.arguments.contains("ping"));
        }
        other => panic!("expected tool use response, got {other:?}"),
    }
}

#[tokio::test]
async fn ollama_chat_extracts_inline_thinking_from_content() {
    let base = spawn_http_response_server(
        "application/json",
        json!({
            "message": {
                "role": "assistant",
                "content": "planning reply</think>\n\nHi there!"
            },
            "done": true,
            "prompt_eval_count": 11,
            "eval_count": 3
        })
        .to_string(),
    )
    .await;
    let client =
        LlmClient::new(resolved_connection_for_provider(&base, LlmProvider::Ollama), DEFAULT_MAX_TOOL_PASSES).expect("client init");
    let invocation = LlmInvocation {
        conversation: vec![LlmMessage {
            role: LlmMessageRole::User,
            content: "Hi there".to_string(),
            kind: LlmMessageKind::Text,
        }],
        ..Default::default()
    };

    let response = client.chat(&invocation).await.expect("chat");
    assert_eq!(response.thinking.as_deref(), Some("planning reply"));
    assert_eq!(response.message.content, "Hi there!");
}

#[tokio::test]
async fn ollama_chat_stream_emits_thinking_text_tool_calls_and_usage() {
    let body = concat!(
        "{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"thinking\":\"plan\"},\"done\":false}\n",
        "{\"message\":{\"role\":\"assistant\",\"content\":\"hello\"},\"done\":false}\n",
        "{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"id\":\"call_test\",\"type\":\"function\",\"function\":{\"index\":0,\"name\":\"echo\",\"arguments\":{\"text\":\"ping\"}}}]},\"done\":false}\n",
        "{\"message\":{\"role\":\"assistant\",\"content\":\"\"},\"done\":true,\"prompt_eval_count\":3,\"eval_count\":2}\n"
    );
    let base = spawn_http_response_server("application/x-ndjson", body.to_string()).await;
    let client =
        LlmClient::new(resolved_connection_for_provider(&base, LlmProvider::Ollama), DEFAULT_MAX_TOOL_PASSES).expect("client init");
    let invocation = LlmInvocation {
        conversation: vec![LlmMessage {
            role: LlmMessageRole::User,
            content: "call echo".to_string(),
            kind: LlmMessageKind::Text,
        }],
        ..Default::default()
    };

    let mut stream = client.chat_stream(&invocation).await.expect("stream");

    let first = stream.next().await.expect("first").expect("first ok");
    assert_eq!(first.thinking.as_deref(), Some("plan"));
    assert!(first.delta.is_none());

    let second = stream.next().await.expect("second").expect("second ok");
    assert_eq!(second.delta.as_deref(), Some("hello"));
    assert!(second.usage.is_none());

    let third = stream.next().await.expect("third").expect("third ok");
    assert_eq!(third.tool_calls.len(), 1);
    assert_eq!(third.tool_calls[0].id, "call_test");
    assert_eq!(third.tool_calls[0].function.name, "echo");

    let fourth = stream.next().await.expect("fourth").expect("fourth ok");
    let usage = fourth.usage.expect("usage");
    assert_eq!(usage.prompt_tokens, 3);
    assert_eq!(usage.completion_tokens, 2);
    assert_eq!(usage.total_tokens, 5);
}

#[tokio::test]
async fn ollama_chat_stream_extracts_inline_thinking_before_visible_answer() {
    let body = concat!(
        "{\"message\":{\"role\":\"assistant\",\"content\":\"planning\"},\"done\":false}\n",
        "{\"message\":{\"role\":\"assistant\",\"content\":\" reply\"},\"done\":false}\n",
        "{\"message\":{\"role\":\"assistant\",\"content\":\"</think>\\n\\nHi\"},\"done\":false}\n",
        "{\"message\":{\"role\":\"assistant\",\"content\":\" there!\"},\"done\":false}\n",
        "{\"message\":{\"role\":\"assistant\",\"content\":\"\"},\"done\":true,\"prompt_eval_count\":3,\"eval_count\":2}\n"
    );
    let base = spawn_http_response_server("application/x-ndjson", body.to_string()).await;
    let client = LlmClient::new(
        resolved_connection_for_provider_with_model(&base, LlmProvider::Ollama, "qwen3:30b"),
        DEFAULT_MAX_TOOL_PASSES,
    )
    .expect("client init");
    let invocation = LlmInvocation {
        conversation: vec![LlmMessage {
            role: LlmMessageRole::User,
            content: "Hi there".to_string(),
            kind: LlmMessageKind::Text,
        }],
        ..Default::default()
    };

    let mut stream = client.chat_stream(&invocation).await.expect("stream");
    let mut visible = String::new();
    let mut thinking = String::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("chunk ok");
        if let Some(delta) = chunk.delta {
            visible.push_str(&delta);
        }
        if let Some(delta) = chunk.thinking {
            thinking.push_str(&delta);
        }
    }

    assert_eq!(thinking, "planning reply");
    assert_eq!(visible, "Hi there!");
}

#[tokio::test]
async fn ollama_chat_stream_returns_after_headers_before_body_completes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let _server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let mut request = vec![0_u8; 4096];
        let _ = socket.read(&mut request).await.expect("read request");
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nTransfer-Encoding: chunked\r\n\r\n")
            .await
            .expect("write headers");
        tokio::time::sleep(Duration::from_secs(1)).await;
    });

    let base = format!("http://{addr}/");
    let client =
        LlmClient::new(resolved_connection_for_provider(&base, LlmProvider::Ollama), DEFAULT_MAX_TOOL_PASSES).expect("client init");
    let invocation = LlmInvocation {
        conversation: vec![LlmMessage {
            role: LlmMessageRole::User,
            content: "hi".to_string(),
            kind: LlmMessageKind::Text,
        }],
        ..Default::default()
    };

    let stream = timeout(Duration::from_millis(50), client.chat_stream(&invocation))
        .await
        .expect("chat_stream should return after Ollama responds with headers")
        .expect("stream");

    drop(stream);
}

#[tokio::test]
async fn ollama_chat_disables_thinking_by_default() {
    let (base, captured) = spawn_http_capture_server(
        "application/json",
        json!({
            "message": {
                "role": "assistant",
                "content": "hello"
            },
            "done": true,
            "prompt_eval_count": 1,
            "eval_count": 1
        })
        .to_string(),
    )
    .await;
    let client =
        LlmClient::new(resolved_connection_for_provider(&base, LlmProvider::Ollama), DEFAULT_MAX_TOOL_PASSES).expect("client init");
    let invocation = LlmInvocation {
        conversation: vec![LlmMessage {
            role: LlmMessageRole::User,
            content: "hi".to_string(),
            kind: LlmMessageKind::Text,
        }],
        ..Default::default()
    };

    client.chat(&invocation).await.expect("chat");

    let payload = captured.lock().expect("capture lock").clone().expect("captured payload");
    assert_eq!(payload.get("think").and_then(|value| value.as_bool()), Some(false));
}

/// Capture-server variant that also retains the request line + headers so
/// Azure tests can assert path encoding and the `api-key` auth header in
/// addition to the JSON body shape.
async fn spawn_full_capture_server(
    content_type: &'static str,
    body: String,
) -> (String, Arc<Mutex<Option<(String, String, serde_json::Value)>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let captured = Arc::new(Mutex::new(None));
    let captured_for_server = Arc::clone(&captured);
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut buffer = vec![0_u8; 16384];
        let read = socket.read(&mut buffer).await.expect("read request");
        let text = String::from_utf8_lossy(&buffer[..read]).to_string();
        let mut request_line = String::new();
        let mut headers = String::new();
        let mut payload = serde_json::Value::Null;
        if let Some(idx) = text.find("\r\n") {
            request_line = text[..idx].to_string();
            if let Some(body_start) = text.find("\r\n\r\n") {
                headers = text[idx + 2..body_start].to_string();
                let body_text = &text[body_start + 4..];
                payload = serde_json::from_str(body_text).expect("parse captured body json");
            }
        }
        *captured_for_server.lock().expect("capture lock") = Some((request_line, headers, payload));
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.expect("write response");
    });
    (format!("http://{addr}"), captured)
}

fn azure_resolved(base_url: &str, max_tokens_field: AzureMaxTokensField) -> Arc<RwLock<ResolvedLlmConnection>> {
    Arc::new(RwLock::new(ResolvedLlmConnection {
        provider: LlmProvider::AzureOpenAI,
        credential_id: None,
        api_key: Some("test-azure-key".to_string()),
        credential_base_url: None,
        defaults: LlmConnectionDefaults {
            model: "gpt-4o".into(),
            base_url_override: Some(base_url.to_string()),
            ..Default::default()
        },
        provider_config: ResolvedProviderConfig::AzureClassic(AzureOpenAiClassicConfig {
            deployment_id: "my deploy".into(),
            api_version: "2024-08-01-preview".into(),
            max_tokens_field,
        }),
    }))
}

#[tokio::test]
async fn azure_chat_hits_deployment_path_with_api_key_header_and_omits_model() {
    let response_body = json!({
        "choices": [{
            "message": {"role": "assistant", "content": "ok"}
        }],
        "usage": {"prompt_tokens": 4, "completion_tokens": 1, "total_tokens": 5}
    })
    .to_string();
    let (base, captured) = spawn_full_capture_server("application/json", response_body).await;

    let client = LlmClient::new(azure_resolved(&base, AzureMaxTokensField::Auto), DEFAULT_MAX_TOOL_PASSES).expect("client");
    let invocation = LlmInvocation {
        conversation: vec![LlmMessage {
            role: LlmMessageRole::User,
            content: "ping".into(),
            kind: LlmMessageKind::Text,
        }],
        overrides: llm_core::types::LlmRequestOverrides { max_tokens: Some(64), ..Default::default() },
        ..Default::default()
    };

    let response = client.chat(&invocation).await.expect("chat");
    assert_eq!(response.message.content, "ok");
    let usage = response.usage.expect("usage");
    assert_eq!(usage.total_tokens, 5);

    let (request_line, headers, body) = captured.lock().expect("lock").clone().expect("captured");

    // URL path includes the percent-encoded deployment, and the api-version
    // query parameter is present.
    assert!(
        request_line.contains("/openai/deployments/my%20deploy/chat/completions"),
        "request line should hit the classic deployment path, got: {request_line}"
    );
    assert!(
        request_line.contains("api-version=2024-08-01-preview"),
        "should carry api-version query, got: {request_line}"
    );

    // Auth header is `api-key`, not `Authorization`.
    let headers_lc = headers.to_ascii_lowercase();
    assert!(headers_lc.contains("api-key: test-azure-key"), "expected api-key header, got: {headers}");
    assert!(!headers_lc.contains("authorization:"), "should not send Authorization header, got: {headers}");

    let obj = body.as_object().expect("body object");
    assert!(!obj.contains_key("model"), "Azure classic body must omit `model`, got: {obj:?}");
    assert_eq!(obj["max_completion_tokens"], 64);
    assert_eq!(obj["messages"][0]["role"], "user");
    assert_eq!(obj["messages"][0]["content"], "ping");
}

#[tokio::test]
async fn azure_chat_legacy_max_tokens_field() {
    let response_body =
        json!({"choices": [{"message": {"role": "assistant", "content": "ok"}}], "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}}).to_string();
    let (base, captured) = spawn_full_capture_server("application/json", response_body).await;

    let client = LlmClient::new(azure_resolved(&base, AzureMaxTokensField::MaxTokens), DEFAULT_MAX_TOOL_PASSES).expect("client");
    let invocation = LlmInvocation {
        conversation: vec![LlmMessage {
            role: LlmMessageRole::User,
            content: "hi".into(),
            kind: LlmMessageKind::Text,
        }],
        overrides: llm_core::types::LlmRequestOverrides { max_tokens: Some(128), ..Default::default() },
        ..Default::default()
    };

    client.chat(&invocation).await.expect("chat");

    let (_request_line, _headers, body) = captured.lock().expect("lock").clone().expect("captured");
    let obj = body.as_object().expect("body object");
    assert!(obj.contains_key("max_tokens"));
    assert!(!obj.contains_key("max_completion_tokens"));
    assert_eq!(obj["max_tokens"], 128);
}

#[tokio::test]
async fn azure_chat_stream_parses_text_and_final_usage() {
    let stream_body = concat!(
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hel\"}}]}\n\n",
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"}}]}\n\n",
        "data: {\"choices\":[{\"index\":0,\"delta\":{}}],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2,\"total_tokens\":6}}\n\n",
        "data: [DONE]\n\n"
    );
    let base = spawn_http_response_server("text/event-stream", stream_body.to_string()).await;
    let client = LlmClient::new(azure_resolved(&base, AzureMaxTokensField::Auto), DEFAULT_MAX_TOOL_PASSES).expect("client");
    let invocation = LlmInvocation {
        conversation: vec![LlmMessage {
            role: LlmMessageRole::User,
            content: "hi".into(),
            kind: LlmMessageKind::Text,
        }],
        ..Default::default()
    };

    let mut stream = client.chat_stream(&invocation).await.expect("stream");
    let mut text = String::new();
    let mut usage = None;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("ok chunk");
        if let Some(delta) = chunk.delta {
            text.push_str(&delta);
        }
        if let Some(u) = chunk.usage {
            usage = Some(u);
        }
    }
    assert_eq!(text, "Hello");
    let usage = usage.expect("usage present");
    assert_eq!(usage.total_tokens, 6);
    assert_eq!(usage.prompt_tokens, 4);
    assert_eq!(usage.completion_tokens, 2);
}

#[tokio::test]
async fn ollama_chat_enables_thinking_when_budget_is_set() {
    let (base, captured) = spawn_http_capture_server(
        "application/json",
        json!({
            "message": {
                "role": "assistant",
                "content": "hello"
            },
            "done": true,
            "prompt_eval_count": 1,
            "eval_count": 1
        })
        .to_string(),
    )
    .await;
    let client =
        LlmClient::new(resolved_connection_for_provider(&base, LlmProvider::Ollama), DEFAULT_MAX_TOOL_PASSES).expect("client init");
    let invocation = LlmInvocation {
        conversation: vec![LlmMessage {
            role: LlmMessageRole::User,
            content: "hi".to_string(),
            kind: LlmMessageKind::Text,
        }],
        overrides: llm_core::types::LlmRequestOverrides { thinking_budget: Some(128), ..Default::default() },
        ..Default::default()
    };

    client.chat(&invocation).await.expect("chat");

    let payload = captured.lock().expect("capture lock").clone().expect("captured payload");
    assert_eq!(payload.get("think").and_then(|value| value.as_bool()), Some(true));
}
