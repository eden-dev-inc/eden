use super::context::DatadogTestContext;
use crate::EP;
use crate::api::lib::custom::CustomInput;
use crate::api::lib::dashboards::get_dashboard::GetDashboardInput;
use crate::api::lib::dashboards::list_dashboards::ListDashboardsInput;
use crate::api::lib::events::create_event::CreateEventInput;
use crate::api::lib::events::list_events::ListEventsInput;
use crate::api::lib::infrastructure::get_hosts::GetHostsInput;
use crate::api::lib::logs::search_logs::SearchLogsInput;
use crate::api::lib::metrics::get_metrics::GetMetricsInput;
use crate::api::lib::metrics::submit_metrics::SubmitMetricsInput;
use crate::api::lib::monitors::get_monitor::GetMonitorInput;
use crate::api::lib::monitors::list_monitors::ListMonitorsInput;
use serde_json::json;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, ResponseTemplate};

#[tokio::test]
async fn test_get_metrics() {
    let mut ctx = DatadogTestContext::new().await;

    let response_body = json!({"series": [{"metric": "system.cpu.user", "points": [[1609459200, 0.5]]}]});
    Mock::given(method("GET"))
        .and(path("/api/v1/query"))
        .and(query_param("from", "1609459200"))
        .and(query_param("to", "1609545600"))
        .and(header("dd-api-key", "test-api-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
        .mount(&ctx.mock_server)
        .await;

    let input = GetMetricsInput::new(1609459200, 1609545600, "avg:system.cpu.user{*}".to_string());
    let result = ctx.read_op(input).await;

    assert_eq!(result["series"][0]["metric"], "system.cpu.user");
}

#[tokio::test]
async fn test_submit_metrics() {
    let mut ctx = DatadogTestContext::new().await;

    let response_body = json!({"status": "ok"});
    Mock::given(method("POST"))
        .and(path("/api/v1/series"))
        .and(header("dd-api-key", "test-api-key"))
        .respond_with(ResponseTemplate::new(202).set_body_json(&response_body))
        .mount(&ctx.mock_server)
        .await;

    let body = json!({"series": [{"metric": "test.metric", "points": [[1609459200, 1.0]], "type": "gauge"}]});
    let input = SubmitMetricsInput::new(body);
    let result = ctx.write_op(input).await;

    assert_eq!(result["status"], "ok");
}

#[tokio::test]
async fn test_list_events() {
    let mut ctx = DatadogTestContext::new().await;

    let response_body = json!({"events": [{"id": 1, "title": "Test event"}]});
    Mock::given(method("GET"))
        .and(path("/api/v1/events"))
        .and(query_param("start", "1000"))
        .and(query_param("end", "2000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
        .mount(&ctx.mock_server)
        .await;

    let input = ListEventsInput::new(1000, 2000);
    let result = ctx.read_op(input).await;

    assert_eq!(result["events"][0]["title"], "Test event");
}

#[tokio::test]
async fn test_create_event() {
    let mut ctx = DatadogTestContext::new().await;

    let response_body = json!({"event": {"id": 42, "title": "Deploy completed"}});
    Mock::given(method("POST"))
        .and(path("/api/v1/events"))
        .respond_with(ResponseTemplate::new(202).set_body_json(&response_body))
        .mount(&ctx.mock_server)
        .await;

    let body = json!({"title": "Deploy completed", "text": "v1.2.3 deployed", "priority": "normal"});
    let input = CreateEventInput::new(body);
    let result = ctx.write_op(input).await;

    assert_eq!(result["event"]["id"], 42);
}

#[tokio::test]
async fn test_search_logs() {
    let mut ctx = DatadogTestContext::new().await;

    let response_body = json!({"data": [{"id": "abc", "attributes": {"message": "error occurred"}}]});
    Mock::given(method("POST"))
        .and(path("/api/v2/logs/events/search"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
        .mount(&ctx.mock_server)
        .await;

    let body = json!({"filter": {"query": "service:web status:error", "from": "now-1h", "to": "now"}});
    let input = SearchLogsInput::new(body);
    let result = ctx.read_op(input).await;

    // unwrap_response extracts the "data" field from the response
    assert_eq!(result[0]["attributes"]["message"], "error occurred");
}

#[tokio::test]
async fn test_list_monitors() {
    let mut ctx = DatadogTestContext::new().await;

    let response_body =
        json!([{"id": 1, "name": "CPU Monitor", "type": "metric alert", "query": "avg(last_5m):avg:system.cpu.user{*} > 80"}]);
    Mock::given(method("GET"))
        .and(path("/api/v1/monitor"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
        .mount(&ctx.mock_server)
        .await;

    let input = ListMonitorsInput::new();
    let result = ctx.read_op(input).await;

    assert_eq!(result[0]["name"], "CPU Monitor");
}

#[tokio::test]
async fn test_get_monitor() {
    let mut ctx = DatadogTestContext::new().await;

    let response_body =
        json!({"id": 12345, "name": "CPU Monitor", "type": "metric alert", "query": "avg(last_5m):avg:system.cpu.user{*} > 80"});
    Mock::given(method("GET"))
        .and(path("/api/v1/monitor/12345"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
        .mount(&ctx.mock_server)
        .await;

    let input = GetMonitorInput::new(12345);
    let result = ctx.read_op(input).await;

    assert_eq!(result["id"], 12345);
    assert_eq!(result["name"], "CPU Monitor");
}

#[tokio::test]
async fn test_list_dashboards() {
    let mut ctx = DatadogTestContext::new().await;

    let response_body = json!({"dashboards": [{"id": "abc-123", "title": "System Overview"}]});
    Mock::given(method("GET"))
        .and(path("/api/v1/dashboard"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
        .mount(&ctx.mock_server)
        .await;

    let input = ListDashboardsInput::new();
    let result = ctx.read_op(input).await;

    assert_eq!(result["dashboards"][0]["title"], "System Overview");
}

#[tokio::test]
async fn test_get_dashboard() {
    let mut ctx = DatadogTestContext::new().await;

    let response_body = json!({"id": "abc-123", "title": "System Overview", "layout_type": "ordered", "widgets": []});
    Mock::given(method("GET"))
        .and(path("/api/v1/dashboard/abc-123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
        .mount(&ctx.mock_server)
        .await;

    let input = GetDashboardInput::new("abc-123".to_string());
    let result = ctx.read_op(input).await;

    assert_eq!(result["id"], "abc-123");
    assert_eq!(result["title"], "System Overview");
}

#[tokio::test]
async fn test_get_hosts() {
    let mut ctx = DatadogTestContext::new().await;

    let response_body = json!({"host_list": [{"name": "host1", "up": true}], "total_returned": 1});
    Mock::given(method("GET"))
        .and(path("/api/v1/hosts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
        .mount(&ctx.mock_server)
        .await;

    let input = GetHostsInput::new();
    let result = ctx.read_op(input).await;

    assert_eq!(result["host_list"][0]["name"], "host1");
    assert_eq!(result["total_returned"], 1);
}

#[tokio::test]
async fn test_custom_get() {
    let mut ctx = DatadogTestContext::new().await;

    let response_body = json!({"result": "custom response"});
    Mock::given(method("GET"))
        .and(path("/api/v1/some/custom/endpoint"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
        .mount(&ctx.mock_server)
        .await;

    let input = CustomInput::new("GET".to_string(), "/api/v1/some/custom/endpoint".to_string(), None);
    let result = ctx.write_op(input).await;

    assert_eq!(result["result"], "custom response");
}

#[tokio::test]
async fn test_custom_post() {
    let mut ctx = DatadogTestContext::new().await;

    let response_body = json!({"created": true});
    Mock::given(method("POST"))
        .and(path("/api/v1/custom/resource"))
        .respond_with(ResponseTemplate::new(201).set_body_json(&response_body))
        .mount(&ctx.mock_server)
        .await;

    let input = CustomInput::new("POST".to_string(), "/api/v1/custom/resource".to_string(), Some(json!({"name": "test"})));
    let result = ctx.write_op(input).await;

    assert_eq!(result["created"], true);
}

#[tokio::test]
async fn test_custom_unsupported_method() {
    let mut ctx = DatadogTestContext::new().await;

    let input = CustomInput::new("PATCH".to_string(), "/api/v1/resource".to_string(), None);
    let err = ctx.write_op_err(input).await;

    let err_str = err.to_string();
    assert!(err_str.contains("unsupported HTTP method"), "Expected unsupported method error, got: {err_str}");
}

#[tokio::test]
async fn test_api_error_response() {
    let mut ctx = DatadogTestContext::new().await;

    Mock::given(method("GET"))
        .and(path("/api/v1/monitor"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({"errors": ["Forbidden"]})))
        .mount(&ctx.mock_server)
        .await;

    let input = ListMonitorsInput::new();
    let err = ctx.write_op_err(input).await;

    let err_str = err.to_string();
    assert!(err_str.contains("403") || err_str.contains("Forbidden"), "Expected 403 error, got: {err_str}");
}

#[tokio::test]
async fn test_health_check() {
    let mut ctx = DatadogTestContext::new().await;

    Mock::given(method("GET"))
        .and(path("/api/v1/validate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"valid": true})))
        .mount(&ctx.mock_server)
        .await;

    let result = ctx.ep.health_check(&ctx.endpoint_cache_uuid, &mut ctx.telemetry).await;

    assert!(result.is_ok(), "Health check failed: {:?}", result.err());
}

#[tokio::test]
async fn test_health_check_failure() {
    let mut ctx = DatadogTestContext::new().await;

    Mock::given(method("GET"))
        .and(path("/api/v1/validate"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&ctx.mock_server)
        .await;

    let result = ctx.ep.health_check(&ctx.endpoint_cache_uuid, &mut ctx.telemetry).await;

    assert!(result.is_err(), "Expected health check to fail");
}
