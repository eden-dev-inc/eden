#![cfg(external_db)]
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite;

use crate::common::{EDEN_NEW_ORG_TOKEN_VALUE, SUPERADMIN_ID, SUPERADMIN_PWD};
use crate::request::{HttpMethod, auth_login, create_org_with_superadmin, get_base_url, make_method_request};
use crate::util::test_server;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn ws_base_url() -> String {
    let http_base = get_base_url(); // e.g. "http://localhost:PORT/api/v1"
    http_base.replacen("http://", "ws://", 1)
}

async fn create_websocket_source(client: &reqwest::Client, token: &str, name: &str, hmac_secret: Option<&str>) -> Value {
    let mut body = json!({
        "name": name,
        "source_type": "websocket",
    });
    if let Some(secret) = hmac_secret {
        body["hmac_secret"] = json!(secret);
    }

    make_method_request::<Value, Value>(client, token, HttpMethod::Post, &format!("{}/triggers", get_base_url()), Some(&body), Some(201))
        .await
        .expect("failed to create trigger source")
        .expect("expected response body")
}

async fn connect_ws(
    url: &str,
) -> (
    futures::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        tungstenite::Message,
    >,
    futures::stream::SplitStream<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>,
) {
    let (ws_stream, _response) = tokio_tungstenite::connect_async(url).await.expect("failed to connect websocket");
    ws_stream.split()
}

async fn send_text(
    sink: &mut futures::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        tungstenite::Message,
    >,
    msg: &Value,
) {
    sink.send(tungstenite::Message::Text(msg.to_string().into())).await.expect("failed to send ws message");
}

async fn recv_json(
    stream: &mut futures::stream::SplitStream<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>,
) -> Value {
    let timeout = tokio::time::Duration::from_secs(5);
    let msg = tokio::time::timeout(timeout, stream.next())
        .await
        .expect("timed out waiting for ws message")
        .expect("ws stream ended")
        .expect("ws receive error");

    match msg {
        tungstenite::Message::Text(text) => serde_json::from_str(text.as_ref()).expect("invalid JSON from server"),
        other => panic!("expected text frame, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ws_connect_and_ingest_no_rules() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            // Create a websocket trigger source
            let source = create_websocket_source(&client, token, "test-ws-source", None).await;
            let source_id = source["source_id"].as_str().expect("missing source_id");

            // Connect via WebSocket
            let ws_url = format!("{}/triggers/{}/ws", ws_base_url(), source_id);
            let (mut sink, mut stream) = connect_ws(&ws_url).await;

            // Send an event — no rules configured so should be "ignored"
            send_text(
                &mut sink,
                &json!({
                    "event_type": "test.event",
                    "payload": {"key": "value"}
                }),
            )
            .await;

            let ack = recv_json(&mut stream).await;
            assert_eq!(ack["type"], "ack");
            assert!(ack["event_id"].is_string());
            // With no rules, either "ignored" or "received" is valid
            let status = ack["status"].as_str().unwrap_or("");
            assert!(status == "ignored" || status == "received", "expected ignored or received, got: {status}");

            // Clean disconnect
            sink.send(tungstenite::Message::Close(None)).await.ok();
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}

#[tokio::test]
async fn test_ws_connect_rejected_for_non_websocket_source() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            // Create a webhook source (not websocket)
            let body = json!({
                "name": "webhook-source",
                "source_type": "webhook",
            });
            let source: Value =
                make_method_request(&client, token, HttpMethod::Post, &format!("{}/triggers", get_base_url()), Some(&body), Some(201))
                    .await
                    .expect("failed to create source")
                    .expect("expected body");

            let source_id = source["source_id"].as_str().expect("missing source_id");

            // Attempt WS upgrade — should fail
            let ws_url = format!("{}/triggers/{}/ws", ws_base_url(), source_id);
            let result = tokio_tungstenite::connect_async(&ws_url).await;
            assert!(result.is_err(), "WS connect should fail for non-websocket source");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}

#[tokio::test]
async fn test_ws_connect_inactive_source() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            // Create an inactive websocket source
            let body = json!({
                "name": "inactive-ws",
                "source_type": "websocket",
                "is_active": false,
            });
            let source: Value =
                make_method_request(&client, token, HttpMethod::Post, &format!("{}/triggers", get_base_url()), Some(&body), Some(201))
                    .await
                    .expect("failed to create source")
                    .expect("expected body");

            let source_id = source["source_id"].as_str().expect("missing source_id");

            // Attempt WS upgrade — should fail (inactive)
            let ws_url = format!("{}/triggers/{}/ws", ws_base_url(), source_id);
            let result = tokio_tungstenite::connect_async(&ws_url).await;
            assert!(result.is_err(), "WS connect should fail for inactive source");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}

#[tokio::test]
async fn test_ws_invalid_json_does_not_disconnect() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            let source = create_websocket_source(&client, token, "bad-json-test", None).await;
            let source_id = source["source_id"].as_str().expect("missing source_id");

            let ws_url = format!("{}/triggers/{}/ws", ws_base_url(), source_id);
            let (mut sink, mut stream) = connect_ws(&ws_url).await;

            // Send invalid JSON
            sink.send(tungstenite::Message::Text("not valid json".into())).await.expect("failed to send");

            let err_resp = recv_json(&mut stream).await;
            assert_eq!(err_resp["type"], "error");
            assert_eq!(err_resp["code"], "parse_error");

            // Connection should still be alive — send a valid event
            send_text(
                &mut sink,
                &json!({
                    "event_type": "after.error",
                    "payload": {}
                }),
            )
            .await;

            let ack = recv_json(&mut stream).await;
            assert_eq!(ack["type"], "ack");

            sink.send(tungstenite::Message::Close(None)).await.ok();
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}

#[tokio::test]
async fn test_ws_idempotency_key_dedup() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            let source = create_websocket_source(&client, token, "idempotency-test", None).await;
            let source_id = source["source_id"].as_str().expect("missing source_id");

            let ws_url = format!("{}/triggers/{}/ws", ws_base_url(), source_id);
            let (mut sink, mut stream) = connect_ws(&ws_url).await;

            let event = json!({
                "event_type": "alert",
                "payload": {"host": "web-01"},
                "idempotency_key": "unique-key-123"
            });

            // Send same event twice
            send_text(&mut sink, &event).await;
            let ack1 = recv_json(&mut stream).await;
            assert_eq!(ack1["type"], "ack");

            send_text(&mut sink, &event).await;
            let ack2 = recv_json(&mut stream).await;
            assert_eq!(ack2["type"], "ack");

            // Both should return the same event_id (dedup)
            assert_eq!(ack1["event_id"], ack2["event_id"]);

            sink.send(tungstenite::Message::Close(None)).await.ok();
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}

#[tokio::test]
async fn test_ws_bare_event_without_type_envelope() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            let source = create_websocket_source(&client, token, "bare-event-test", None).await;
            let source_id = source["source_id"].as_str().expect("missing source_id");

            let ws_url = format!("{}/triggers/{}/ws", ws_base_url(), source_id);
            let (mut sink, mut stream) = connect_ws(&ws_url).await;

            // Send without {"type": "event", ...} wrapper — should still work
            send_text(
                &mut sink,
                &json!({
                    "event_type": "bare.alert",
                    "payload": {"metric": "memory"}
                }),
            )
            .await;

            let ack = recv_json(&mut stream).await;
            assert_eq!(ack["type"], "ack");
            assert!(ack["event_id"].is_string());

            sink.send(tungstenite::Message::Close(None)).await.ok();
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}

#[tokio::test]
async fn test_ws_hmac_query_param_auth() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            let hmac_secret = "test-secret-key";
            let source = create_websocket_source(&client, token, "hmac-test", Some(hmac_secret)).await;
            let source_id = source["source_id"].as_str().expect("missing source_id");

            // Compute valid HMAC over source_id
            use hmac::{Hmac, Mac};
            use sha2::Sha256;
            type HmacSha256 = Hmac<Sha256>;
            let mut mac = HmacSha256::new_from_slice(hmac_secret.as_bytes()).expect("valid key");
            mac.update(source_id.as_bytes());
            let signature = hex::encode(mac.finalize().into_bytes());
            let hmac_param = format!("sha256={signature}");

            // Connect with valid HMAC — should succeed
            let ws_url = format!("{}/triggers/{}/ws?hmac={}", ws_base_url(), source_id, hmac_param);
            let (mut sink, mut stream) = connect_ws(&ws_url).await;

            // Verify connection works by sending an event
            send_text(
                &mut sink,
                &json!({
                    "event_type": "hmac.test",
                    "payload": {}
                }),
            )
            .await;

            let ack = recv_json(&mut stream).await;
            assert_eq!(ack["type"], "ack");

            sink.send(tungstenite::Message::Close(None)).await.ok();
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}

#[tokio::test]
async fn test_ws_hmac_wrong_signature_rejected() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            let source = create_websocket_source(&client, token, "hmac-reject-test", Some("real-secret")).await;
            let source_id = source["source_id"].as_str().expect("missing source_id");

            // Connect with wrong HMAC
            let ws_url = format!("{}/triggers/{}/ws?hmac=sha256=0000dead", ws_base_url(), source_id);
            let result = tokio_tungstenite::connect_async(&ws_url).await;
            assert!(result.is_err(), "WS connect should fail with wrong HMAC");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}

#[tokio::test]
async fn test_ws_hmac_first_frame_auth() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            let _ = create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD)
                .await
                .unwrap_or_default();
            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed JWT");
            let token = &admin_jwt.token;

            let hmac_secret = "first-frame-secret";
            let source = create_websocket_source(&client, token, "first-frame-auth", Some(hmac_secret)).await;
            let source_id = source["source_id"].as_str().expect("missing source_id");

            // Connect WITHOUT hmac query param — server should accept upgrade
            // but require auth on the first frame
            let ws_url = format!("{}/triggers/{}/ws", ws_base_url(), source_id);
            let (mut sink, mut stream) = connect_ws(&ws_url).await;

            // Compute valid HMAC
            use hmac::{Hmac, Mac};
            use sha2::Sha256;
            type HmacSha256 = Hmac<Sha256>;
            let mut mac = HmacSha256::new_from_slice(hmac_secret.as_bytes()).expect("valid key");
            mac.update(source_id.as_bytes());
            let signature = hex::encode(mac.finalize().into_bytes());

            // Send auth frame
            send_text(
                &mut sink,
                &json!({
                    "type": "auth",
                    "signature": format!("sha256={signature}")
                }),
            )
            .await;

            // Now send an event — should work after auth
            send_text(
                &mut sink,
                &json!({
                    "event_type": "post.auth",
                    "payload": {"ok": true}
                }),
            )
            .await;

            let ack = recv_json(&mut stream).await;
            assert_eq!(ack["type"], "ack");

            sink.send(tungstenite::Message::Close(None)).await.ok();
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    );
}
