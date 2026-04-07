use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationOptions {
    pub server_base_url: String,
    pub client_base_url: Option<String>,
    pub request_timeout_ms: u64,
    pub require_postgres: bool,
    pub require_redis: bool,
    pub exercise_client_config_patch: bool,
}

impl Default for ValidationOptions {
    fn default() -> Self {
        Self {
            server_base_url: "http://127.0.0.1:3000".to_string(),
            client_base_url: None,
            request_timeout_ms: 5_000,
            require_postgres: false,
            require_redis: false,
            exercise_client_config_patch: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerHealth {
    pub mode: String,
    pub redis_enabled: bool,
    pub redis_connected: bool,
    pub postgres_enabled: bool,
    pub postgres_connected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationStep {
    pub name: String,
    pub status: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub server_health: ServerHealth,
    pub client_validated: bool,
    pub client_config_patch_exercised: bool,
    pub write_paths_validated: bool,
    pub steps: Vec<ValidationStep>,
}

impl ValidationReport {
    pub fn success_step_details(&self) -> impl Iterator<Item = (&str, &str)> {
        self.steps
            .iter()
            .filter(|step| step.status == "ok")
            .map(|step| (step.name.as_str(), step.detail.as_str()))
    }
}

fn add_step(steps: &mut Vec<ValidationStep>, name: &str, status: &str, detail: impl Into<String>) {
    steps.push(ValidationStep {
        name: name.to_string(),
        status: status.to_string(),
        detail: detail.into(),
    });
}

fn parse_uuid_field(value: &Value, field: &str) -> Result<Uuid> {
    let raw = value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing string field '{field}'"))?;
    Uuid::parse_str(raw).with_context(|| format!("invalid uuid in '{field}'"))
}

async fn get_json(client: &reqwest::Client, url: &str) -> Result<Value> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url} failed"))?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!("GET {url} returned HTTP {status}: {body}");
    }
    serde_json::from_str(&body).with_context(|| format!("GET {url} returned invalid JSON"))
}

async fn post_json(
    client: &reqwest::Client,
    url: &str,
    payload: &Value,
) -> Result<(StatusCode, Value)> {
    let response = client
        .post(url)
        .json(payload)
        .send()
        .await
        .with_context(|| format!("POST {url} failed"))?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let json =
        serde_json::from_str(&body).with_context(|| format!("POST {url} returned invalid JSON"))?;
    Ok((status, json))
}

async fn patch_json(
    client: &reqwest::Client,
    url: &str,
    payload: &Value,
) -> Result<(StatusCode, Value)> {
    let response = client
        .patch(url)
        .json(payload)
        .send()
        .await
        .with_context(|| format!("PATCH {url} failed"))?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let json = serde_json::from_str(&body)
        .with_context(|| format!("PATCH {url} returned invalid JSON"))?;
    Ok((status, json))
}

async fn validate_server_health(
    client: &reqwest::Client,
    base_url: &str,
    options: &ValidationOptions,
    steps: &mut Vec<ValidationStep>,
) -> Result<ServerHealth> {
    let url = format!("{base_url}/health");
    let health = get_json(client, &url).await?;
    let status = health
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("health response missing status"))?;
    if status != "ok" {
        bail!("server health returned non-ok status: {status}");
    }

    let server_health = ServerHealth {
        mode: health
            .get("mode")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        redis_enabled: health
            .get("redis_enabled")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        redis_connected: health
            .get("redis_connected")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        postgres_enabled: health
            .get("postgres_enabled")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        postgres_connected: health
            .get("postgres_connected")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    };

    if options.require_postgres && !server_health.postgres_enabled {
        bail!("validator requires postgres, but server health says postgres_enabled=false");
    }
    if options.require_postgres && !server_health.postgres_connected {
        bail!("validator requires postgres, but server health says postgres_connected=false");
    }
    if options.require_redis && !server_health.redis_enabled {
        bail!("validator requires redis, but server health says redis_enabled=false");
    }
    if options.require_redis && !server_health.redis_connected {
        bail!("validator requires redis, but server health says redis_connected=false");
    }

    add_step(
        steps,
        "server health",
        "ok",
        format!(
            "mode={}, redis={} ({}), postgres={} ({})",
            server_health.mode,
            server_health.redis_enabled,
            server_health.redis_connected,
            server_health.postgres_enabled,
            server_health.postgres_connected
        ),
    );

    Ok(server_health)
}

async fn validate_client_health(
    client: &reqwest::Client,
    base_url: &str,
    server_base_url: &str,
    steps: &mut Vec<ValidationStep>,
) -> Result<()> {
    let health = get_json(client, &format!("{base_url}/health")).await?;
    let status = health
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("client health missing status"))?;
    if status != "ok" {
        bail!("client health returned non-ok status: {status}");
    }

    let target = health
        .get("target_base_url")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if target != server_base_url {
        bail!("client target_base_url mismatch: expected {server_base_url}, got {target}");
    }

    let organizations_cached = health
        .get("organizations_cached")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if organizations_cached == 0 {
        bail!("client has no cached organizations");
    }

    add_step(
        steps,
        "client health",
        "ok",
        format!("target={target}, organizations_cached={organizations_cached}"),
    );

    let config = get_json(client, &format!("{base_url}/config")).await?;
    let qps = config
        .get("queries_per_second")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("client config missing queries_per_second"))?;
    let eps = config
        .get("events_per_second")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("client config missing events_per_second"))?;
    if config.get("query_distribution").is_none() {
        bail!("client config missing query_distribution");
    }
    if config.get("write_distribution").is_none() {
        bail!("client config missing write_distribution");
    }
    if config.get("event_distribution").is_none() {
        bail!("client config missing event_distribution");
    }

    add_step(
        steps,
        "client config",
        "ok",
        format!("queries_per_second={qps}, events_per_second={eps}"),
    );

    Ok(())
}

async fn validate_read_paths(
    client: &reqwest::Client,
    base_url: &str,
    steps: &mut Vec<ValidationStep>,
) -> Result<Uuid> {
    let organizations =
        get_json(client, &format!("{base_url}/api/v1/organizations?limit=3")).await?;
    let organizations = organizations
        .as_array()
        .ok_or_else(|| anyhow!("organizations response was not an array"))?;
    if organizations.is_empty() {
        bail!("organizations response was empty");
    }
    let org_id = parse_uuid_field(&organizations[0], "id")?;
    add_step(
        steps,
        "organizations",
        "ok",
        format!("selected_org_id={org_id}"),
    );

    let storefront = get_json(
        client,
        &format!("{base_url}/api/v1/organizations/{org_id}/storefront"),
    )
    .await?;
    if storefront.get("organization_id").and_then(Value::as_str)
        != Some(org_id.to_string().as_str())
    {
        bail!("storefront organization_id mismatch");
    }
    let featured_products = storefront
        .get("featured_products")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("storefront missing featured_products"))?;
    if featured_products.is_empty() {
        bail!("storefront returned no featured products");
    }
    add_step(
        steps,
        "storefront",
        "ok",
        format!("featured_products={}", featured_products.len()),
    );

    let catalog = get_json(
        client,
        &format!("{base_url}/api/v1/organizations/{org_id}/catalog?limit=5"),
    )
    .await?;
    let products = catalog
        .get("products")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("catalog missing products"))?;
    if products.is_empty() {
        bail!("catalog returned no products");
    }
    add_step(
        steps,
        "catalog",
        "ok",
        format!("products={}", products.len()),
    );

    let dashboard = get_json(
        client,
        &format!(
            "{base_url}/api/v1/organizations/{org_id}/dashboard?hours=24&hourly_points=4&top_pages_limit=5"
        ),
    )
    .await?;
    let top_pages = dashboard
        .get("top_pages")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("dashboard missing top_pages"))?;
    if top_pages.is_empty() {
        bail!("dashboard returned no top pages");
    }
    if dashboard.get("overview").is_none() {
        bail!("dashboard missing overview");
    }
    add_step(
        steps,
        "dashboard",
        "ok",
        format!("top_pages={}", top_pages.len()),
    );

    let overview = get_json(
        client,
        &format!("{base_url}/api/v1/organizations/{org_id}/analytics/overview?hours=24"),
    )
    .await?;
    let total_events = overview
        .get("total_events")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("overview missing total_events"))?;
    add_step(
        steps,
        "overview",
        "ok",
        format!("total_events={total_events}"),
    );

    Ok(org_id)
}

async fn validate_write_paths(
    client: &reqwest::Client,
    base_url: &str,
    org_id: Uuid,
    steps: &mut Vec<ValidationStep>,
) -> Result<()> {
    let (event_status, event_body) = post_json(
        client,
        &format!("{base_url}/api/v1/organizations/{org_id}/events"),
        &json!({
            "event_type": "page_view",
            "page_url": "https://app.example.com/storefront",
            "referrer": "https://www.google.com/",
            "user_agent": "runtime-validator",
            "properties": {
                "validator": true,
                "channel": "runtime-check"
            }
        }),
    )
    .await?;
    if event_status != StatusCode::OK {
        bail!("event ingest returned HTTP {event_status}: {event_body}");
    }
    if event_body.get("accepted").and_then(Value::as_bool) != Some(true) {
        bail!("event ingest did not return accepted=true");
    }
    add_step(steps, "event ingest", "ok", "accepted");

    let (create_status, create_body) = post_json(
        client,
        &format!("{base_url}/api/v1/organizations/{org_id}/carts"),
        &json!({
            "quantity": 2,
            "metadata": {
                "validator": true,
                "channel": "runtime-check"
            }
        }),
    )
    .await?;
    if create_status != StatusCode::OK {
        bail!("cart create returned HTTP {create_status}: {create_body}");
    }
    let cart_id = parse_uuid_field(&create_body, "cart_id")?;
    let initial_item_count = create_body
        .get("item_count")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("cart create missing item_count"))?;
    add_step(
        steps,
        "cart create",
        "ok",
        format!("cart_id={cart_id}, item_count={initial_item_count}"),
    );

    let cart = get_json(
        client,
        &format!("{base_url}/api/v1/organizations/{org_id}/carts/{cart_id}"),
    )
    .await?;
    if parse_uuid_field(&cart, "id")? != cart_id {
        bail!("cart detail returned wrong cart id");
    }
    add_step(steps, "cart detail", "ok", format!("cart_id={cart_id}"));

    let (add_status, add_body) = post_json(
        client,
        &format!("{base_url}/api/v1/organizations/{org_id}/carts/{cart_id}/items"),
        &json!({ "quantity": 1 }),
    )
    .await?;
    if add_status != StatusCode::OK {
        bail!("cart add-item returned HTTP {add_status}: {add_body}");
    }
    let updated_item_count = add_body
        .get("item_count")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("cart add-item missing item_count"))?;
    if updated_item_count <= initial_item_count {
        bail!(
            "cart item_count did not increase: before={initial_item_count}, after={updated_item_count}"
        );
    }
    add_step(
        steps,
        "cart add-item",
        "ok",
        format!("cart_id={cart_id}, item_count={updated_item_count}"),
    );

    let (checkout_status, checkout_body) = post_json(
        client,
        &format!("{base_url}/api/v1/organizations/{org_id}/carts/{cart_id}/checkout"),
        &json!({ "payment_method": "credit_card" }),
    )
    .await?;
    if checkout_status != StatusCode::OK {
        bail!("cart checkout returned HTTP {checkout_status}: {checkout_body}");
    }
    let order_id = parse_uuid_field(&checkout_body, "order_id")?;
    let payment_id = parse_uuid_field(&checkout_body, "payment_id")?;
    add_step(
        steps,
        "checkout",
        "ok",
        format!("cart_id={cart_id}, order_id={order_id}, payment_id={payment_id}"),
    );
    Ok(())
}

async fn exercise_client_config_patch(
    client: &reqwest::Client,
    client_base_url: &str,
    steps: &mut Vec<ValidationStep>,
) -> Result<()> {
    let (patch_status, patch_body) = patch_json(
        client,
        &format!("{client_base_url}/config"),
        &json!({
            "queries_per_second": 5,
            "events_per_second": 3,
            "query_distribution": {
                "storefront": 30,
                "catalog": 25,
                "dashboard": 15
            },
            "write_distribution": {
                "cart_create": 30,
                "cart_add_item": 30,
                "cart_checkout": 20,
                "event_ingest": 20
            }
        }),
    )
    .await?;
    if patch_status != StatusCode::OK {
        bail!("client config patch returned HTTP {patch_status}: {patch_body}");
    }
    add_step(steps, "client config patch", "ok", "accepted");
    Ok(())
}

pub async fn run_runtime_validation(options: &ValidationOptions) -> Result<ValidationReport> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(options.request_timeout_ms))
        .build()
        .context("failed to build validator http client")?;
    run_runtime_validation_with_client(&client, options).await
}

pub async fn run_runtime_validation_with_client(
    client: &reqwest::Client,
    options: &ValidationOptions,
) -> Result<ValidationReport> {
    let started_at = Utc::now();
    let mut steps = Vec::new();

    let server_health =
        validate_server_health(client, &options.server_base_url, options, &mut steps).await?;
    let org_id = validate_read_paths(client, &options.server_base_url, &mut steps).await?;

    let write_paths_validated =
        if server_health.postgres_enabled && server_health.postgres_connected {
            validate_write_paths(client, &options.server_base_url, org_id, &mut steps).await?;
            true
        } else {
            add_step(
                &mut steps,
                "write validation",
                "skipped",
                format!(
                    "postgres not enabled/connected (mode={})",
                    server_health.mode
                ),
            );
            false
        };

    let mut client_validated = false;
    let mut client_config_patch_exercised = false;
    if let Some(client_base_url) = options.client_base_url.as_deref() {
        validate_client_health(
            client,
            client_base_url,
            &options.server_base_url,
            &mut steps,
        )
        .await?;
        client_validated = true;

        if options.exercise_client_config_patch {
            exercise_client_config_patch(client, client_base_url, &mut steps).await?;
            client_config_patch_exercised = true;
        }
    }

    Ok(ValidationReport {
        started_at,
        completed_at: Utc::now(),
        server_health,
        client_validated,
        client_config_patch_exercised,
        write_paths_validated,
        steps,
    })
}
