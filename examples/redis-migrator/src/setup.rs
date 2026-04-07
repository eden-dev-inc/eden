use anyhow::{anyhow, Context, Result};
use clap::Parser;
use serde::de::DeserializeOwned;
use serde::Deserialize;

const DEFAULT_EDEN_NEW_ORG_SECRET: &str = "neworgsecret";
const DEFAULT_EDEN_ADMIN_PASSWORD: &str = "adam-demo-pass";

fn default_eden_new_org_secret() -> String {
    std::env::var("EDEN_NEW_ORG_TOKEN").unwrap_or_else(|_| DEFAULT_EDEN_NEW_ORG_SECRET.to_string())
}

fn default_eden_admin_password() -> String {
    std::env::var("EDEN_ADMIN_PASS").unwrap_or_else(|_| DEFAULT_EDEN_ADMIN_PASSWORD.to_string())
}

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum MigrationMode {
    BigBang,
    Canary,
    BlueGreen,
}

/// Set up Eden organization, endpoints, interlay, and migration via the Eden API
#[derive(Parser, Debug, Clone)]
pub struct SetupConfig {
    /// Eden API base URL
    #[clap(long, env = "EDEN_API_URL")]
    pub api_url: String,

    /// Organization ID
    #[clap(long, env = "EDEN_ORG_ID", default_value = "adam-demo")]
    pub org_id: String,

    /// Token for creating new organizations
    #[clap(long, env = "EDEN_NEW_ORG_SECRET", default_value_t = default_eden_new_org_secret())]
    pub org_token: String,

    /// Admin username
    #[clap(long, env = "EDEN_ADMIN_USER", default_value = "admin")]
    pub admin_user: String,

    /// Admin password
    #[clap(long, env = "EDEN_ADMIN_PASSWORD", default_value_t = default_eden_admin_password())]
    pub admin_pass: String,

    /// Source (origin) Redis URL
    #[clap(long, env = "REDIS_SOURCE_URL")]
    pub source_url: String,

    /// Destination (target) Redis URL
    #[clap(long, env = "REDIS_DEST_URL")]
    pub dest_url: String,

    /// Interlay listening port
    #[clap(long, env = "INTERLAY_PORT", default_value = "5731")]
    pub interlay_port: u16,

    /// Migration mode
    #[clap(long, env = "MIGRATION_MODE", value_enum, default_value = "big-bang")]
    pub mode: MigrationMode,

    /// Canary read percentage (0.0-1.0, only used with canary mode)
    #[clap(long, env = "CANARY_READ_PCT", default_value = "0.05")]
    pub canary_read_pct: f64,
}

// --- Response types ---

#[derive(Deserialize, Debug)]
struct LoginResponse {
    token: String,
}

#[derive(Deserialize, Debug)]
struct EndpointResponseData {
    #[allow(dead_code)]
    id: String,
    uuid: String,
}

#[derive(Deserialize, Debug)]
struct InterlayResponseData {
    #[allow(dead_code)]
    id: String,
    uuid: String,
}

#[derive(Deserialize, Debug)]
struct MigrationResponseData {
    #[allow(dead_code)]
    id: Option<String>,
    #[allow(dead_code)]
    uuid: Option<String>,
    #[allow(dead_code)]
    status: Option<String>,
    #[allow(dead_code)]
    encrypted_runner_key: Option<String>,
}

// --- URL parsing ---

struct RedisConnInfo {
    host: String,
    port: u16,
    tls: bool,
    password: Option<String>,
}

fn parse_redis_url(url_str: &str) -> Result<RedisConnInfo> {
    let url_str = if url_str.contains("://") {
        url_str.to_string()
    } else {
        format!("redis://{}", url_str)
    };
    let parsed = url::Url::parse(&url_str).context("Invalid Redis URL")?;
    let tls = parsed.scheme() == "rediss";
    let host = parsed.host_str().unwrap_or("localhost").to_string();
    let port = parsed.port().unwrap_or(if tls { 6380 } else { 6379 });
    let password = parsed.password().map(|p| {
        urlencoding::decode(p)
            .unwrap_or_else(|_| p.into())
            .into_owned()
    });
    Ok(RedisConnInfo {
        host,
        port,
        tls,
        password,
    })
}

/// Redact password from a URL for safe logging
fn redact_url(url: &str) -> String {
    if let Ok(mut parsed) = url::Url::parse(url) {
        if parsed.password().is_some() {
            let _ = parsed.set_password(Some("****"));
        }
        parsed.to_string()
    } else {
        url.to_string()
    }
}

fn encode_path_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push_str(&format!("{byte:02X}"));
        }
    }
    encoded
}

async fn grant_endpoint_data_access(
    http: &reqwest::Client,
    api_url: &str,
    org_id: &str,
    token: &str,
    endpoint_id: &str,
    subject: &str,
    perms: &str,
) -> Result<()> {
    let url = format!(
        "{}/api/v1/iam/data/endpoints/{}/subjects/{}",
        api_url,
        encode_path_segment(endpoint_id),
        encode_path_segment(subject)
    );
    let resp = http
        .put(&url)
        .bearer_auth(token)
        .header("X-Org-Id", org_id)
        .json(&serde_json::json!({ "perms": perms }))
        .send()
        .await
        .with_context(|| format!("Failed to grant endpoint data access for '{}'", endpoint_id))?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!(
            "Grant endpoint data access failed for '{}': {} - {}",
            endpoint_id,
            status,
            body
        );
    }

    Ok(())
}

pub async fn run(config: SetupConfig) -> Result<()> {
    let source = parse_redis_url(&config.source_url)?;
    let dest = parse_redis_url(&config.dest_url)?;

    // Print all resolved configuration (env + CLI)
    println!("Eden Setup");
    println!("===========");
    println!();
    println!("Configuration (from .env + CLI flags):");
    println!("  EDEN_API_URL:       {}", config.api_url);
    println!("  EDEN_ORG_ID:        {}", config.org_id);
    println!(
        "  EDEN_NEW_ORG_SECRET: {}",
        if config.org_token == DEFAULT_EDEN_NEW_ORG_SECRET {
            "neworgsecret (default)"
        } else {
            "****"
        }
    );
    println!("  EDEN_ADMIN_USER:    {}", config.admin_user);
    println!(
        "  EDEN_ADMIN_PASSWORD: {}",
        if config.admin_pass == DEFAULT_EDEN_ADMIN_PASSWORD {
            "adam-demo-pass (default)"
        } else {
            "****"
        }
    );
    println!("  REDIS_SOURCE_URL:   {}", redact_url(&config.source_url));
    println!("  REDIS_DEST_URL:     {}", redact_url(&config.dest_url));
    println!("  INTERLAY_PORT:      {}", config.interlay_port);
    println!("  MIGRATION_MODE:     {:?}", config.mode);
    if matches!(config.mode, MigrationMode::Canary) {
        println!("  CANARY_READ_PCT:    {}", config.canary_read_pct);
    }
    println!();
    println!("Resolved endpoints:");
    println!(
        "  Source: {}:{} (TLS: {}, auth: {})",
        source.host,
        source.port,
        source.tls,
        source.password.is_some()
    );
    println!(
        "  Dest:   {}:{} (TLS: {}, auth: {})",
        dest.host,
        dest.port,
        dest.tls,
        dest.password.is_some()
    );
    println!();

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .context("Failed to create HTTP client")?;

    // Pre-flight: check Eden API is reachable
    println!("[0/7] Checking Eden API connectivity...");
    println!("  GET {}", config.api_url);
    match http
        .get(&config.api_url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
    {
        Ok(resp) => println!("  Eden API reachable (status: {})", resp.status()),
        Err(e) => {
            println!("  WARNING: Eden API not reachable: {}", e);
            println!("  Continuing anyway — API calls may fail...");
        }
    }
    println!();

    // Step 1: Create Organization
    println!("[1/7] Creating organization '{}'...", config.org_id);
    let create_org_url = format!("{}/api/v1/new", config.api_url);
    let create_org_body = serde_json::json!({
        "id": config.org_id,
        "description": format!("Organization {}", config.org_id),
        "super_admins": [{
            "username": config.admin_user,
            "password": config.admin_pass,
            "description": null
        }]
    });
    println!("  POST {}", create_org_url);
    println!(
        "  Body: {}",
        serde_json::to_string_pretty(&create_org_body).unwrap_or_default()
    );

    let resp = http
        .post(&create_org_url)
        .header("Authorization", format!("Bearer {}", config.org_token))
        .json(&create_org_body)
        .send()
        .await
        .context("Failed to create organization")?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    println!(
        "  Response: {} {}",
        status.as_u16(),
        status.canonical_reason().unwrap_or("")
    );
    if !body.is_empty() {
        println!("  Body: {}", body);
    }

    if status.as_u16() == 409 {
        println!(
            "  WARNING: Organization '{}' already exists — using existing",
            config.org_id
        );
    } else if !status.is_success() {
        anyhow::bail!("Failed to create organization: {} - {}", status, body);
    } else {
        println!("  Created successfully");
    }
    println!();

    // Step 2: Login
    println!("[2/7] Logging in as '{}'...", config.admin_user);
    let login_url = format!("{}/api/v1/auth/login", config.api_url);
    println!("  POST {}", login_url);
    println!(
        "  Headers: X-Org-Id: {}, Authorization: Basic <{}>",
        config.org_id, config.admin_user
    );

    let resp = http
        .post(&login_url)
        .basic_auth(&config.admin_user, Some(&config.admin_pass))
        .header("X-Org-Id", &config.org_id)
        .send()
        .await
        .context("Failed to login")?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        println!(
            "  Response: {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("")
        );
        println!("  Body: {}", body);
        anyhow::bail!("Login failed: {} - {}", status, body);
    }
    let login_resp = resp
        .json::<LoginResponse>()
        .await
        .context("Failed to parse login response")?;
    let token = login_resp.token;
    println!("  Response: {} OK", status.as_u16());
    println!(
        "  Token: {}...{}",
        &token[..8.min(token.len())],
        &token[token.len().saturating_sub(4)..]
    );
    println!("  Authenticated successfully");
    println!();

    // Step 3: Create Source Endpoint
    let source_endpoint_id = format!("redis_source_{}", source.port);
    println!("[3/7] Creating source endpoint '{}'...", source_endpoint_id);
    let endpoint_url = format!("{}/api/v1/endpoints", config.api_url);
    let source_ep_body = serde_json::json!({
        "endpoint": source_endpoint_id,
        "kind": "redis",
        "config": {
            "read_conn": null,
            "write_conn": {
                "host": source.host,
                "port": source.port,
                "tls": source.tls,
                "password": source.password
            }
        },
        "description": format!("Redis endpoint at {}:{}", source.host, source.port)
    });
    println!("  POST {}", endpoint_url);
    // Log body with password redacted
    let mut source_ep_log = source_ep_body.clone();
    if let Some(config_obj) = source_ep_log.get_mut("config") {
        if let Some(wc) = config_obj.get_mut("write_conn") {
            if wc.get("password").is_some() && !wc["password"].is_null() {
                wc["password"] = serde_json::json!("****");
            }
        }
    }
    println!(
        "  Body: {}",
        serde_json::to_string_pretty(&source_ep_log).unwrap_or_default()
    );

    let resp = http
        .post(&endpoint_url)
        .bearer_auth(&token)
        .header("X-Org-Id", &config.org_id)
        .json(&source_ep_body)
        .send()
        .await
        .context("Failed to create source endpoint")?;

    let status = resp.status();
    let source_uuid = if status.as_u16() == 409 {
        let body = resp.text().await.unwrap_or_default();
        println!("  Response: 409 Conflict");
        if !body.is_empty() {
            println!("  Body: {}", body);
        }
        println!(
            "  WARNING: Source endpoint '{}' already exists — fetching existing",
            source_endpoint_id
        );

        let get_url = format!("{}/api/v1/endpoints/{}", config.api_url, source_endpoint_id);
        println!("  GET {}", get_url);
        let resp = http
            .get(&get_url)
            .bearer_auth(&token)
            .header("X-Org-Id", &config.org_id)
            .send()
            .await
            .context("Failed to fetch existing source endpoint")?;
        let get_status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        let ep: EndpointResponseData = parse_api_data(&body_text)?;
        println!("  Response: {}", get_status.as_u16());
        println!("  Using existing: id={}, uuid={}", ep.id, ep.uuid);
        ep.uuid
    } else if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        println!(
            "  Response: {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("")
        );
        println!("  Body: {}", body);
        anyhow::bail!("Failed to create source endpoint: {} - {}", status, body);
    } else {
        let body_text = resp.text().await.unwrap_or_default();
        println!(
            "  Response: {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("")
        );
        println!("  Body: {}", body_text);
        let ep: EndpointResponseData =
            parse_api_data(&body_text).context("Failed to parse source endpoint response")?;
        println!("  Created: id={}, uuid={}", ep.id, ep.uuid);
        ep.uuid
    };
    grant_endpoint_data_access(
        &http,
        &config.api_url,
        &config.org_id,
        &token,
        &source_endpoint_id,
        &config.admin_user,
        "rwx",
    )
    .await?;
    println!(
        "  Granted '{}' shared runtime access on '{}'",
        config.admin_user, source_endpoint_id
    );
    println!();

    // Step 4: Create Destination Endpoint
    let dest_endpoint_id = format!("redis_dest_{}", dest.port);
    println!(
        "[4/7] Creating destination endpoint '{}'...",
        dest_endpoint_id
    );
    let dest_ep_body = serde_json::json!({
        "endpoint": dest_endpoint_id,
        "kind": "redis",
        "config": {
            "read_conn": null,
            "write_conn": {
                "host": dest.host,
                "port": dest.port,
                "tls": dest.tls,
                "password": dest.password
            }
        },
        "description": format!("Redis endpoint at {}:{}", dest.host, dest.port)
    });
    println!("  POST {}", endpoint_url);
    let mut dest_ep_log = dest_ep_body.clone();
    if let Some(config_obj) = dest_ep_log.get_mut("config") {
        if let Some(wc) = config_obj.get_mut("write_conn") {
            if wc.get("password").is_some() && !wc["password"].is_null() {
                wc["password"] = serde_json::json!("****");
            }
        }
    }
    println!(
        "  Body: {}",
        serde_json::to_string_pretty(&dest_ep_log).unwrap_or_default()
    );

    let resp = http
        .post(&endpoint_url)
        .bearer_auth(&token)
        .header("X-Org-Id", &config.org_id)
        .json(&dest_ep_body)
        .send()
        .await
        .context("Failed to create destination endpoint")?;

    let status = resp.status();
    if status.as_u16() == 409 {
        let body = resp.text().await.unwrap_or_default();
        println!("  Response: 409 Conflict");
        if !body.is_empty() {
            println!("  Body: {}", body);
        }
        println!(
            "  WARNING: Dest endpoint '{}' already exists — fetching existing",
            dest_endpoint_id
        );

        let get_url = format!("{}/api/v1/endpoints/{}", config.api_url, dest_endpoint_id);
        println!("  GET {}", get_url);
        let resp = http
            .get(&get_url)
            .bearer_auth(&token)
            .header("X-Org-Id", &config.org_id)
            .send()
            .await?;
        let get_status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        let ep: EndpointResponseData = parse_api_data(&body_text)?;
        println!("  Response: {}", get_status.as_u16());
        println!("  Using existing: id={}, uuid={}", ep.id, ep.uuid);
    } else if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        println!(
            "  Response: {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("")
        );
        println!("  Body: {}", body);
        anyhow::bail!("Failed to create dest endpoint: {} - {}", status, body);
    } else {
        let body_text = resp.text().await.unwrap_or_default();
        println!(
            "  Response: {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("")
        );
        println!("  Body: {}", body_text);
        let ep: EndpointResponseData =
            parse_api_data(&body_text).context("Failed to parse dest endpoint response")?;
        println!("  Created: id={}, uuid={}", ep.id, ep.uuid);
    }
    grant_endpoint_data_access(
        &http,
        &config.api_url,
        &config.org_id,
        &token,
        &dest_endpoint_id,
        &config.admin_user,
        "rwx",
    )
    .await?;
    println!(
        "  Granted '{}' shared runtime access on '{}'",
        config.admin_user, dest_endpoint_id
    );
    println!();

    // Step 5: Create Interlay
    let interlay_id = format!("redis_interlay_{}_{}", source.port, dest.port);
    println!("[5/7] Creating interlay '{}'...", interlay_id);
    let interlay_url = format!("{}/api/v1/interlays", config.api_url);
    let interlay_body = serde_json::json!({
        "id": interlay_id,
        "endpoint": source_uuid,
        "port": config.interlay_port,
        "settings": {},
        "tls": false
    });
    println!("  POST {}", interlay_url);
    println!(
        "  Body: {}",
        serde_json::to_string_pretty(&interlay_body).unwrap_or_default()
    );

    let resp = http
        .post(&interlay_url)
        .bearer_auth(&token)
        .header("X-Org-Id", &config.org_id)
        .json(&interlay_body)
        .send()
        .await
        .context("Failed to create interlay")?;

    let status = resp.status();
    if status.as_u16() == 409 {
        let body = resp.text().await.unwrap_or_default();
        println!("  Response: 409 Conflict");
        if !body.is_empty() {
            println!("  Body: {}", body);
        }

        if let Some(conflicting_interlay) = conflicting_interlay_id(&body) {
            anyhow::bail!(
                "Interlay port {} is already in use by '{}' — choose a different INTERLAY_PORT",
                config.interlay_port,
                conflicting_interlay
            );
        }

        println!(
            "  WARNING: Interlay '{}' already exists — fetching existing",
            interlay_id
        );

        let get_url = format!("{}/api/v1/interlays/{}", config.api_url, interlay_id);
        println!("  GET {}", get_url);
        let resp = http
            .get(&get_url)
            .bearer_auth(&token)
            .header("X-Org-Id", &config.org_id)
            .send()
            .await?;
        let get_status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        let il: InterlayResponseData = parse_api_data(&body_text)?;
        println!("  Response: {}", get_status.as_u16());
        println!("  Using existing: id={}, uuid={}", il.id, il.uuid);
    } else if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        println!(
            "  Response: {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("")
        );
        println!("  Body: {}", body);
        anyhow::bail!("Failed to create interlay: {} - {}", status, body);
    } else {
        let body_text = resp.text().await.unwrap_or_default();
        println!(
            "  Response: {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("")
        );
        println!("  Body: {}", body_text);
        let il: InterlayResponseData =
            parse_api_data(&body_text).context("Failed to parse interlay response")?;
        println!("  Created: id={}, uuid={}", il.id, il.uuid);
    }
    println!();

    // Step 6: Create Migration
    let mode_suffix = match config.mode {
        MigrationMode::BigBang => "bb",
        MigrationMode::Canary => "canary",
        MigrationMode::BlueGreen => "bg",
    };
    let migration_id = format!(
        "redis_migration_{}_{}_{}",
        source.port, dest.port, mode_suffix
    );
    println!("[6/7] Creating migration '{}'...", migration_id);

    let strategy = match config.mode {
        MigrationMode::BigBang => serde_json::json!({
            "type": "big_bang",
            "durability": true
        }),
        MigrationMode::Canary => serde_json::json!({
            "type": "canary",
            "read_percentage": config.canary_read_pct,
            "write_mode": {
                "mode": "dual_write",
                "policy": "OldAuthoritative"
            }
        }),
        MigrationMode::BlueGreen => serde_json::json!({
            "type": "blue_green",
            "active_is_new": false,
            "write_mode": {
                "mode": "dual_write",
                "policy": "LastWriteWins"
            }
        }),
    };

    let migration_api_url = format!("{}/api/v1/migrations", config.api_url);
    let migration_body = serde_json::json!({
        "id": migration_id,
        "description": format!("Redis {:?} migration", config.mode),
        "strategy": strategy
    });
    println!("  POST {}", migration_api_url);
    println!(
        "  Body: {}",
        serde_json::to_string_pretty(&migration_body).unwrap_or_default()
    );

    let resp = http
        .post(&migration_api_url)
        .bearer_auth(&token)
        .header("X-Org-Id", &config.org_id)
        .json(&migration_body)
        .send()
        .await
        .context("Failed to create migration")?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    println!(
        "  Response: {} {}",
        status.as_u16(),
        status.canonical_reason().unwrap_or("")
    );
    if !body.is_empty() {
        println!("  Body: {}", body);
    }

    if status.is_success() {
        // 200 OK — migration created (or upserted if it already existed)
        if let Ok(resp_data) = serde_json::from_str::<MigrationResponseData>(&body) {
            if resp_data.encrypted_runner_key.is_some() {
                println!("  Created with runner key");
            } else {
                println!("  Created successfully");
            }
        } else {
            println!("  Created successfully");
        }
    } else if is_existing_resource_conflict(status.as_u16(), &body) {
        println!(
            "  WARNING: Migration '{}' already exists — fetching existing",
            migration_id
        );
        let existing = fetch_existing_migration(
            &http,
            &config.api_url,
            &token,
            &config.org_id,
            &migration_id,
        )
        .await
        .context("Failed to fetch existing migration after conflict")?;
        log_existing_migration(&migration_id, &existing);
    } else {
        // Log the error and try to fetch existing migration before failing
        println!("  WARNING: Migration creation failed — checking if migration already exists...");
        match fetch_existing_migration(
            &http,
            &config.api_url,
            &token,
            &config.org_id,
            &migration_id,
        )
        .await
        {
            Ok(existing) => {
                println!(
                    "  Migration '{}' exists — continuing with existing",
                    migration_id
                );
                log_existing_migration(&migration_id, &existing);
            }
            Err(fetch_err) => {
                anyhow::bail!(
                    "Failed to create migration and no existing migration found: {} - {} (lookup failed: {})",
                    status,
                    body,
                    fetch_err
                );
            }
        }
    }
    println!();

    // Step 7: Add Interlay to Migration
    println!("[7/7] Adding interlay to migration...");

    let add_interlay_url = format!(
        "{}/api/v1/migrations/{}/interlay/{}",
        config.api_url, migration_id, interlay_id
    );
    let (migration_strategy, migration_rules) = match config.mode {
        MigrationMode::BigBang => (
            serde_json::json!({
                "type": "big_bang",
                "durability": true
            }),
            serde_json::json!({
                "traffic": {
                    "read": "Replicated",
                    "write": "New"
                },
                "error": "DoNothing",
                "rollback": "Ignore",
                "completion": {
                    "milestone": "Immediate",
                    "require_manual_approval": false
                }
            }),
        ),
        MigrationMode::Canary => (
            serde_json::json!({
                "type": "canary",
                "read_percentage": config.canary_read_pct,
                "write_mode": {
                    "mode": "dual_write",
                    "policy": "OldAuthoritative"
                }
            }),
            serde_json::json!({
                "traffic": {
                    "read": {
                        "Ratio": {
                            "strategy": {
                                "Random": { "ratio": config.canary_read_pct }
                            }
                        }
                    },
                    "write": {
                        "Replicated": {
                            "policy": "OldAuthoritative"
                        }
                    }
                },
                "error": "DoNothing",
                "rollback": "Ignore",
                "completion": {
                    "milestone": {
                        "TotalRequests": 1000000
                    },
                    "require_manual_approval": false
                }
            }),
        ),
        MigrationMode::BlueGreen => (
            serde_json::json!({
                "type": "blue_green",
                "active_is_new": false,
                "write_mode": {
                    "mode": "dual_write",
                    "policy": "LastWriteWins"
                }
            }),
            serde_json::json!({
                "traffic": {
                    "read": "Old",
                    "write": {
                        "Replicated": {
                            "policy": "LastWriteWins"
                        }
                    }
                },
                "error": "DoNothing",
                "rollback": "Ignore",
                "completion": {
                    "milestone": "Immediate",
                    "require_manual_approval": true
                }
            }),
        ),
    };

    let add_interlay_body = serde_json::json!({
        "id": "redis_migration_relay",
        "endpoint": dest_endpoint_id,
        "description": "Migration interlay configuration",
        "migration_strategy": migration_strategy,
        "migration_data": {
            "Scan": {
                "replace": "None"
            }
        },
        "testing_validation": null,
        "migration_rules": migration_rules
    });
    println!("  POST {}", add_interlay_url);
    println!(
        "  Body: {}",
        serde_json::to_string_pretty(&add_interlay_body).unwrap_or_default()
    );

    let resp = http
        .post(&add_interlay_url)
        .bearer_auth(&token)
        .header("X-Org-Id", &config.org_id)
        .json(&add_interlay_body)
        .send()
        .await
        .context("Failed to add interlay to migration")?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    println!(
        "  Response: {} {}",
        status.as_u16(),
        status.canonical_reason().unwrap_or("")
    );
    if !body.is_empty() {
        println!("  Body: {}", body);
    }

    if is_existing_resource_conflict(status.as_u16(), &body) {
        println!("  WARNING: Interlay already has an active migration — fetching current state");

        // Fetch interlay details
        let get_interlay_url = format!("{}/api/v1/interlays/{}", config.api_url, interlay_id);
        println!("  GET {}", get_interlay_url);
        if let Ok(resp) = http
            .get(&get_interlay_url)
            .bearer_auth(&token)
            .header("X-Org-Id", &config.org_id)
            .send()
            .await
        {
            let il_status = resp.status();
            let il_body = resp.text().await.unwrap_or_default();
            println!("  Interlay ({}):", il_status);
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&il_body) {
                let details = parsed.get("data").unwrap_or(&parsed);
                // Check if the running port matches config
                if let Some(running_port) = details.get("port").and_then(|p| p.as_u64()) {
                    if running_port as u16 == config.interlay_port {
                        println!("  Port: {} (matches INTERLAY_PORT)", running_port);
                    } else {
                        println!(
                            "  WARNING: Running interlay port {} does NOT match INTERLAY_PORT={}",
                            running_port, config.interlay_port
                        );
                        println!(
                            "  Update INTERLAY_PORT and REDIS_URL in .env to use port {}",
                            running_port
                        );
                    }
                }
                if let Some(running) = details.get("running").and_then(|r| r.as_bool()) {
                    println!("  Running: {}", running);
                }
                if let Some(migration) = details.get("migration") {
                    if let Some(status) = migration.get("status").and_then(|s| s.as_str()) {
                        println!("  Migration status: {}", status);
                    }
                }
                println!(
                    "{}",
                    serde_json::to_string_pretty(&parsed).unwrap_or(il_body)
                );
            } else {
                println!("  {}", il_body);
            }
        }

        // Fetch migration details
        let get_migration_url = format!("{}/api/v1/migrations/{}", config.api_url, migration_id);
        println!("  GET {}", get_migration_url);
        if let Ok(resp) = http
            .get(&get_migration_url)
            .bearer_auth(&token)
            .header("X-Org-Id", &config.org_id)
            .send()
            .await
        {
            let mig_status = resp.status();
            let mig_body = resp.text().await.unwrap_or_default();
            println!("  Migration ({}):", mig_status);
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&mig_body) {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&parsed).unwrap_or(mig_body)
                );
            } else {
                println!("  {}", mig_body);
            }
        }
    } else if !status.is_success() {
        anyhow::bail!("Failed to add interlay to migration: {} - {}", status, body);
    } else {
        println!("  Added successfully");
    }
    println!();

    // Derive interlay URL from the Eden API host
    let api_parsed = url::Url::parse(&config.api_url).context("Invalid EDEN_API_URL")?;
    let api_host = api_parsed.host_str().unwrap_or("localhost");
    let interlay_conn_url = format!("redis://{}:{}", api_host, config.interlay_port);

    // Health check: verify connectivity (5s timeout per check)
    println!("Health Check (5s timeout per check)");
    println!("====================================");

    // Check interlay connectivity via redis-cli PING
    println!(
        "  Checking interlay at {}:{}...",
        api_host, config.interlay_port
    );
    let ping_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::process::Command::new("redis-cli")
            .args([
                "-h",
                api_host,
                "-p",
                &config.interlay_port.to_string(),
                "PING",
            ])
            .output(),
    )
    .await;

    match ping_result {
        Ok(Ok(output)) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            if output.status.success() && stdout.trim().contains("PONG") {
                println!("  Interlay PING: PONG (healthy)");
            } else {
                println!("  WARNING: Interlay PING failed");
                println!("    stdout: {}", stdout.trim());
                if !stderr.is_empty() {
                    println!("    stderr: {}", stderr.trim());
                }
                println!("    exit: {}", output.status);
                println!("    The interlay may not be running yet. Check Eden logs.");
            }
        }
        Ok(Err(e)) => println!("  WARNING: Could not run redis-cli: {}", e),
        Err(_) => println!(
            "  WARNING: Interlay PING timed out (5s) — port may be blocked or interlay not running"
        ),
    }

    // Check source Redis connectivity
    println!(
        "  Checking source Redis at {}:{}...",
        source.host, source.port
    );
    let mut src_args = vec![
        "-h".to_string(),
        source.host.clone(),
        "-p".to_string(),
        source.port.to_string(),
    ];
    if source.tls {
        src_args.push("--tls".to_string());
    }
    if let Some(ref pw) = source.password {
        src_args.push("-a".to_string());
        src_args.push(pw.clone());
    }
    src_args.push("PING".to_string());

    let src_ping = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::process::Command::new("redis-cli")
            .args(&src_args)
            .output(),
    )
    .await;

    match src_ping {
        Ok(Ok(output)) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            if output.status.success() && stdout.trim().contains("PONG") {
                println!("  Source Redis PING: PONG (healthy)");
            } else {
                println!("  WARNING: Source Redis PING failed");
                println!("    stdout: {}", stdout.trim());
                if !stderr.is_empty() {
                    println!("    stderr: {}", stderr.trim());
                }
                println!("    exit: {}", output.status);
            }
        }
        Ok(Err(e)) => println!("  WARNING: Could not check source Redis: {}", e),
        Err(_) => println!("  WARNING: Source Redis PING timed out (5s)"),
    }

    // Check dest Redis connectivity
    println!("  Checking dest Redis at {}:{}...", dest.host, dest.port);
    let mut dst_args = vec![
        "-h".to_string(),
        dest.host.clone(),
        "-p".to_string(),
        dest.port.to_string(),
    ];
    if dest.tls {
        dst_args.push("--tls".to_string());
    }
    if let Some(ref pw) = dest.password {
        dst_args.push("-a".to_string());
        dst_args.push(pw.clone());
    }
    dst_args.push("PING".to_string());

    let dst_ping = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::process::Command::new("redis-cli")
            .args(&dst_args)
            .output(),
    )
    .await;

    match dst_ping {
        Ok(Ok(output)) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            if output.status.success() && stdout.trim().contains("PONG") {
                println!("  Dest Redis PING: PONG (healthy)");
            } else {
                println!("  WARNING: Dest Redis PING failed");
                println!("    stdout: {}", stdout.trim());
                if !stderr.is_empty() {
                    println!("    stderr: {}", stderr.trim());
                }
                println!("    exit: {}", output.status);
            }
        }
        Ok(Err(e)) => println!("  WARNING: Could not check dest Redis: {}", e),
        Err(_) => println!("  WARNING: Dest Redis PING timed out (5s)"),
    }

    println!();
    println!("Setup Complete!");
    println!("===============");
    println!("Organization:     {}", config.org_id);
    println!("Source Endpoint:   {}", source_endpoint_id);
    println!("Dest Endpoint:    {}", dest_endpoint_id);
    println!("Interlay:         {}", interlay_id);
    println!("Migration:        {} ({:?})", migration_id, config.mode);
    println!();
    println!("Interlay URL:     {}", interlay_conn_url);
    println!(
        "Set REDIS_URL={} in .env to use with populate/client",
        interlay_conn_url
    );

    Ok(())
}

fn parse_api_data<T: DeserializeOwned>(text: &str) -> Result<T> {
    let json = serde_json::from_str::<serde_json::Value>(text)
        .map_err(|e| anyhow!("invalid JSON: {}", e))?;
    let payload = json.get("data").cloned().unwrap_or(json);
    serde_json::from_value(payload).map_err(|e| anyhow!("schema mismatch: {}", e))
}

async fn fetch_existing_migration(
    http: &reqwest::Client,
    api_url: &str,
    token: &str,
    org_id: &str,
    migration_id: &str,
) -> Result<MigrationResponseData> {
    let get_url = format!("{}/api/v1/migrations/{}", api_url, migration_id);
    println!("  GET {}", get_url);
    let resp = http
        .get(&get_url)
        .bearer_auth(token)
        .header("X-Org-Id", org_id)
        .send()
        .await
        .context("Failed to fetch existing migration")?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    println!(
        "  Response: {} {}",
        status.as_u16(),
        status.canonical_reason().unwrap_or("")
    );
    if !body.is_empty() {
        println!("  Body: {}", body);
    }
    if !status.is_success() {
        anyhow::bail!("Existing migration lookup failed: {} - {}", status, body);
    }

    parse_api_data(&body).context("Failed to parse existing migration response")
}

fn log_existing_migration(migration_id: &str, migration: &MigrationResponseData) {
    println!(
        "  Using existing: id={}, status={}",
        migration.id.as_deref().unwrap_or(migration_id),
        migration.status.as_deref().unwrap_or("unknown")
    );
}

fn is_existing_resource_conflict(status_code: u16, body: &str) -> bool {
    status_code == 409 || is_existing_resource_error(body)
}

fn is_existing_resource_error(text: &str) -> bool {
    let normalized = text.to_ascii_lowercase();
    normalized.contains("already exists")
        || normalized.contains("conflict")
        || normalized.contains("already has an active migration")
        || normalized.contains("duplicate")
}

fn conflicting_interlay_id(body: &str) -> Option<&str> {
    let marker = "interlay '";
    let start = body.find(marker)? + marker.len();
    let rest = &body[start..];
    let end = rest.find('\'')?;
    Some(&rest[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_existing_resource_error_variants() {
        assert!(is_existing_resource_conflict(409, ""));
        assert!(is_existing_resource_conflict(
            400,
            "migration already exists for this org"
        ));
        assert!(is_existing_resource_conflict(
            400,
            "interlay already has an active migration"
        ));
        assert!(is_existing_resource_conflict(
            400,
            "duplicate key value violates unique constraint"
        ));
        assert!(!is_existing_resource_conflict(500, "internal server error"));
    }
}
