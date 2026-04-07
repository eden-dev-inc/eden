use analytics_server::{run_runtime_validation, ValidationOptions};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::{sleep, Instant};

struct ProcessGuard {
    child: Child,
}

impl ProcessGuard {
    fn spawn(mut command: Command) -> Self {
        let child = command.spawn().expect("process should spawn");
        Self { child }
    }

    fn failure_context(&mut self) -> String {
        match self.child.try_wait() {
            Ok(Some(status)) => {
                let mut stderr_output = String::new();
                if let Some(stderr) = self.child.stderr.as_mut() {
                    let _ = stderr.read_to_string(&mut stderr_output);
                }
                format!("process exited with {status}; stderr: {stderr_output}")
            }
            Ok(None) => "process is still running".to_string(),
            Err(error) => format!("failed to inspect child process: {error}"),
        }
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn test_port(offset: u16) -> u16 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after epoch");
    let entropy = (std::process::id() + now.subsec_nanos()) % 1000;
    43000 + entropy as u16 + offset
}

fn start_server(port: u16) -> ProcessGuard {
    let mut command = Command::new(env!("CARGO_BIN_EXE_analytics-server"));
    command
        .arg("--bind-address")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--redis-enabled")
        .arg("false")
        .arg("--postgres-enabled")
        .arg("false")
        .arg("--allow-no-backend")
        .arg("true")
        .arg("--internal-workload-enabled")
        .arg("false")
        .arg("--organizations")
        .arg("5")
        .arg("--users-per-org")
        .arg("25")
        .arg("--telemetry-enabled")
        .arg("false")
        .env("RUST_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    ProcessGuard::spawn(command)
}

fn start_client(port: u16, target_base_url: &str) -> ProcessGuard {
    let mut command = Command::new(env!("CARGO_BIN_EXE_traffic-client"));
    command
        .arg("--bind-address")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--target-base-url")
        .arg(target_base_url)
        .arg("--client-profile")
        .arg("balanced")
        .arg("--query-workers")
        .arg("1")
        .arg("--event-workers")
        .arg("1")
        .arg("--queries-per-second")
        .arg("2")
        .arg("--events-per-second")
        .arg("1")
        .arg("--organization-fetch-limit")
        .arg("5")
        .arg("--organization-refresh-interval-seconds")
        .arg("2")
        .arg("--request-timeout-ms")
        .arg("1000")
        .arg("--telemetry-enabled")
        .arg("false")
        .env("RUST_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    ProcessGuard::spawn(command)
}

async fn wait_for_json(
    http: &Client,
    url: &str,
    timeout: Duration,
    process: &mut ProcessGuard,
) -> Value {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(response) = http.get(url).send().await {
            if response.status().is_success() {
                if let Ok(body) = response.json::<Value>().await {
                    return body;
                }
            }
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for successful JSON response from {url}; {}",
            process.failure_context()
        );
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn analytics_server_routes_cover_core_runtime_paths() {
    let http = Client::new();
    let server_port = test_port(10);
    let mut server = start_server(server_port);
    let base_url = format!("http://127.0.0.1:{server_port}");

    let health_body = wait_for_json(
        &http,
        &format!("{base_url}/health"),
        Duration::from_secs(10),
        &mut server,
    )
    .await;
    assert_eq!(health_body["status"], "ok");
    assert_eq!(health_body["mode"], "No backend");
    assert_eq!(health_body["redis_enabled"], false);
    assert_eq!(health_body["postgres_enabled"], false);

    let organizations = http
        .get(format!("{base_url}/api/v1/organizations?limit=3"))
        .send()
        .await
        .expect("organization request should succeed");
    assert_eq!(organizations.status(), StatusCode::OK);
    let organizations_body: Value = organizations.json().await.expect("organizations json");
    let organizations_array = organizations_body
        .as_array()
        .expect("organizations should be an array");
    assert_eq!(organizations_array.len(), 3);
    let org_id = organizations_array[0]["id"]
        .as_str()
        .expect("organization id should be present");

    let dashboard = http
        .get(format!(
            "{base_url}/api/v1/organizations/{org_id}/dashboard?hours=24&hourly_points=4&top_pages_limit=5"
        ))
        .send()
        .await
        .expect("dashboard request should succeed");
    assert_eq!(dashboard.status(), StatusCode::OK);
    let dashboard_body: Value = dashboard.json().await.expect("dashboard json");
    assert_eq!(dashboard_body["organization_id"], org_id);
    assert_eq!(
        dashboard_body["top_pages"]
            .as_array()
            .expect("top_pages should be an array")
            .len(),
        5
    );

    let storefront = http
        .get(format!(
            "{base_url}/api/v1/organizations/{org_id}/storefront"
        ))
        .send()
        .await
        .expect("storefront request should succeed");
    assert_eq!(storefront.status(), StatusCode::OK);
    let storefront_body: Value = storefront.json().await.expect("storefront json");
    assert_eq!(storefront_body["organization_id"], org_id);
    assert!(!storefront_body["featured_products"]
        .as_array()
        .expect("featured_products should be an array")
        .is_empty());

    let catalog = http
        .get(format!(
            "{base_url}/api/v1/organizations/{org_id}/catalog?limit=4"
        ))
        .send()
        .await
        .expect("catalog request should succeed");
    assert_eq!(catalog.status(), StatusCode::OK);
    let catalog_body: Value = catalog.json().await.expect("catalog json");
    assert_eq!(catalog_body["organization_id"], org_id);
    assert_eq!(
        catalog_body["products"]
            .as_array()
            .expect("products should be an array")
            .len(),
        4
    );

    let control = http
        .patch(format!("{base_url}/control"))
        .json(&json!({
            "queries_per_second": 321,
            "events_per_second": 45
        }))
        .send()
        .await
        .expect("control patch should return");
    assert_eq!(control.status(), StatusCode::NOT_FOUND);

    let event_ingest = http
        .post(format!("{base_url}/api/v1/organizations/{org_id}/events"))
        .json(&json!({
            "event_type": "page_view",
            "page_url": "https://app.example.com/billing"
        }))
        .send()
        .await
        .expect("event ingest request should return");
    assert_eq!(event_ingest.status(), StatusCode::SERVICE_UNAVAILABLE);

    let cart_create = http
        .post(format!("{base_url}/api/v1/organizations/{org_id}/carts"))
        .json(&json!({
            "quantity": 2
        }))
        .send()
        .await
        .expect("cart create request should return");
    assert_eq!(cart_create.status(), StatusCode::SERVICE_UNAVAILABLE);

    let metrics = http
        .get(format!("{base_url}/metrics"))
        .send()
        .await
        .expect("metrics endpoint should return");
    assert_eq!(metrics.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn traffic_client_and_server_exercise_read_write_runtime_paths() {
    let http = Client::new();
    let server_port = test_port(30);
    let mut server = start_server(server_port);
    let server_base_url = format!("http://127.0.0.1:{server_port}");

    wait_for_json(
        &http,
        &format!("{server_base_url}/health"),
        Duration::from_secs(10),
        &mut server,
    )
    .await;

    let client_port = test_port(31);
    let mut client = start_client(client_port, &server_base_url);
    let client_base_url = format!("http://127.0.0.1:{client_port}");

    let client_health = wait_for_json(
        &http,
        &format!("{client_base_url}/health"),
        Duration::from_secs(10),
        &mut client,
    )
    .await;
    assert_eq!(client_health["status"], "ok");
    assert_eq!(client_health["target_base_url"], server_base_url);
    assert!(client_health["organizations_cached"].as_u64().unwrap_or(0) > 0);

    let updated = http
        .patch(format!("{client_base_url}/config"))
        .json(&json!({
            "queries_per_second": 3,
            "events_per_second": 2,
            "query_distribution": {
                "dashboard": 30,
                "storefront": 30,
                "catalog": 20,
                "cart_detail": 5
            },
            "write_distribution": {
                "event_ingest": 30,
                "cart_create": 30,
                "cart_add_item": 20,
                "cart_checkout": 20
            }
        }))
        .send()
        .await
        .expect("client config patch should succeed");
    assert_eq!(updated.status(), StatusCode::OK);
    let updated_body: Value = updated.json().await.expect("client config json");
    assert_eq!(updated_body["queries_per_second"], 3);
    assert_eq!(updated_body["events_per_second"], 2);

    let invalid_update = http
        .patch(format!("{client_base_url}/config"))
        .json(&json!({
            "query_distribution": {
                "organizations_list": 0,
                "dashboard": 0,
                "analytics_overview_24h": 0,
                "analytics_overview_1h": 0,
                "top_pages": 0,
                "hourly_metrics": 0,
                "storefront": 0,
                "catalog": 0,
                "cart_detail": 0
            }
        }))
        .send()
        .await
        .expect("invalid client config patch should return");
    assert_eq!(invalid_update.status(), StatusCode::BAD_REQUEST);

    let invalid_write_update = http
        .patch(format!("{client_base_url}/config"))
        .json(&json!({
            "write_distribution": {
                "event_ingest": 0,
                "cart_create": 0,
                "cart_add_item": 0,
                "cart_checkout": 0
            }
        }))
        .send()
        .await
        .expect("invalid client write config patch should return");
    assert_eq!(invalid_write_update.status(), StatusCode::BAD_REQUEST);

    let client_metrics = http
        .get(format!("{client_base_url}/metrics"))
        .send()
        .await
        .expect("client metrics endpoint should return");
    assert_eq!(client_metrics.status(), StatusCode::NOT_FOUND);

    let server_metrics = http
        .get(format!("{server_base_url}/metrics"))
        .send()
        .await
        .expect("server metrics endpoint should return");
    assert_eq!(server_metrics.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn runtime_validator_checks_live_stack_over_http() {
    let http = Client::new();
    let server_port = test_port(60);
    let mut server = start_server(server_port);
    let server_base_url = format!("http://127.0.0.1:{server_port}");

    wait_for_json(
        &http,
        &format!("{server_base_url}/health"),
        Duration::from_secs(10),
        &mut server,
    )
    .await;

    let client_port = test_port(61);
    let mut client = start_client(client_port, &server_base_url);
    let client_base_url = format!("http://127.0.0.1:{client_port}");

    wait_for_json(
        &http,
        &format!("{client_base_url}/health"),
        Duration::from_secs(10),
        &mut client,
    )
    .await;

    let report = run_runtime_validation(&ValidationOptions {
        server_base_url,
        client_base_url: Some(client_base_url),
        request_timeout_ms: 2_000,
        require_postgres: false,
        require_redis: false,
        exercise_client_config_patch: true,
    })
    .await
    .expect("runtime validation should succeed");

    assert!(report.client_validated);
    assert!(report.client_config_patch_exercised);
    assert!(!report.write_paths_validated);
    assert_eq!(
        report
            .steps
            .iter()
            .find(|step| step.name == "client config patch")
            .map(|step| step.status.as_str()),
        Some("ok")
    );
}
