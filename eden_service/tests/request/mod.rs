#![allow(dead_code)]

use std::{error::Error, fmt::Display, time::Duration};

use eden_core::format::{EdenUuid, EndpointUuid, rbac::ControlPerms};
use eden_service::comm::auth::JwtResponse;
use eden_service::comm::endpoints::post::Response;
use endpoint_core::ep_core::database::schema::{organization::OrganizationInput, user::UserInput};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::common::{ORG_DESCR, ORG_ID};
use crate::util::TestConfig;

/// Get the test organization ID from TestConfig or use the constant
fn get_test_org_id() -> String {
    TestConfig::get_org_id()
}

/// Get the BASE_URL dynamically from TestConfig port or default
pub fn get_base_url() -> String {
    let port = TestConfig::get_port();
    format!("http://localhost:{}/api/v1", port)
}

fn url(path: &str) -> String {
    get_base_url() + path
}

#[derive(PartialEq)]
pub enum EpRequestType {
    Read,
    Write,
    Transaction,
}

impl Display for EpRequestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read => "read".fmt(f),
            Self::Write => "write".fmt(f),
            Self::Transaction => "transaction".fmt(f),
        }
    }
}

/// Makes an HTTP request to the server with retry logic
///
/// Parameters:
/// - `client`: HTTP client to use
/// - `token`: Bearer token for authentication
/// - `url`: URL to request
/// - `body`: Optional request body (POST) vs GET
/// - `no_response`: If true, don't deserialize response body
/// - `expect_status`: If set, verify response status code matches
///
pub async fn make_request<S: Serialize, R: DeserializeOwned>(
    client: &Client,
    token: &str,
    url: &str,
    body: Option<&S>,
    no_response: bool,
    expect_status: Option<u16>,
) -> Result<Option<R>, Box<dyn Error>> {
    log::debug!("Url: {}", url);

    let mut retries_left = 10;
    let mut backoff = Duration::from_millis(100);
    let max_backoff = Duration::from_millis(2000);

    loop {
        let resp = if let Some(body) = body {
            client.post(url).bearer_auth(token).json(body).timeout(Duration::from_secs(30)).send().await?
        } else {
            client.get(url).bearer_auth(token).timeout(Duration::from_secs(30)).send().await?
        };

        let response_status = resp.status();

        // Handle rate limiting with exponential backoff
        if response_status == StatusCode::TOO_MANY_REQUESTS {
            retries_left -= 1;
            if retries_left == 0 {
                return Err("Too many requests after maximum retries".into());
            }

            log::warn!("Rate limited on {}, waiting {:?} (retries left: {})", url, backoff, retries_left);

            tokio::time::sleep(backoff).await;
            backoff = std::cmp::min(Duration::from_millis((backoff.as_millis() as f64 * 1.5) as u64), max_backoff);
            continue;
        }

        let response_body = resp.text().await?;
        log::debug!("Response: {}", response_body);

        // Verify expected status code if specified
        if let Some(expected_status_code) = expect_status {
            let exp = StatusCode::from_u16(expected_status_code)?;
            if response_status != exp {
                return Err(format!("expected status code {}, received {} for {}", exp, response_status, url).into());
            }
        }

        // Return early if we don't need to deserialize the response
        if no_response || expect_status.is_some() {
            return Ok(None);
        }

        // Deserialize and return response body
        match serde_json::from_str::<R>(&response_body) {
            Ok(data) => return Ok(Some(data)),
            Err(e) => {
                return Err(format!("Failed to deserialize response: {}. Response was: {}", e, response_body).into());
            }
        }
    }
}

pub enum HttpMethod {
    Get,
    Post,
    Patch,
    Put,
    Delete,
}

/// Like make_request but supports all HTTP methods.
pub async fn make_method_request<S: Serialize, R: DeserializeOwned>(
    client: &Client,
    token: &str,
    method: HttpMethod,
    url: &str,
    body: Option<&S>,
    expect_status: Option<u16>,
) -> Result<Option<R>, Box<dyn Error>> {
    log::debug!(
        "Url: {} {:?}",
        match &method {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Patch => "PATCH",
            HttpMethod::Put => "PUT",
            HttpMethod::Delete => "DELETE",
        },
        url
    );

    let mut retries_left = 10;
    let mut backoff = Duration::from_millis(100);
    let max_backoff = Duration::from_millis(2000);

    loop {
        let builder = match &method {
            HttpMethod::Get => client.get(url),
            HttpMethod::Post => client.post(url),
            HttpMethod::Patch => client.patch(url),
            HttpMethod::Put => client.put(url),
            HttpMethod::Delete => client.delete(url),
        };
        let builder = builder.bearer_auth(token).timeout(Duration::from_secs(30));
        let resp = if let Some(body) = body {
            builder.json(body).send().await?
        } else {
            builder.send().await?
        };

        let response_status = resp.status();

        if response_status == StatusCode::TOO_MANY_REQUESTS {
            retries_left -= 1;
            if retries_left == 0 {
                return Err("Too many requests after maximum retries".into());
            }
            tokio::time::sleep(backoff).await;
            backoff = std::cmp::min(Duration::from_millis((backoff.as_millis() as f64 * 1.5) as u64), max_backoff);
            continue;
        }

        let response_body = resp.text().await?;
        log::debug!("Response: {}", response_body);

        if let Some(expected_status_code) = expect_status {
            let exp = StatusCode::from_u16(expected_status_code)?;
            if response_status != exp {
                return Err(format!("expected status code {}, received {} for {}", exp, response_status, url).into());
            }
            return Ok(None);
        }

        if !response_status.is_success() {
            return Err(format!("Request failed with status {}: {}", response_status, response_body).into());
        }

        if response_body.is_empty() {
            return Ok(None);
        }

        match serde_json::from_str::<R>(&response_body) {
            Ok(data) => return Ok(Some(data)),
            Err(e) => {
                return Err(format!("Failed to deserialize response: {}. Response was: {}", e, response_body).into());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Raw (no-retry) request helpers for rate-limiting tests
// ---------------------------------------------------------------------------

/// GET /organizations without retrying on 429.
/// Returns `(status_code, body_text, response_headers)`.
pub async fn get_org_raw(client: &Client, token: &str) -> Result<(u16, String, reqwest::header::HeaderMap), Box<dyn Error>> {
    let resp = client.get(url("/organizations")).bearer_auth(token).timeout(Duration::from_secs(30)).send().await?;
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let body = resp.text().await?;
    Ok((status, body, headers))
}

/// PATCH /organizations without retrying on 429.
/// Returns `(status_code, body_text, response_headers)`.
pub async fn patch_org_raw<S: Serialize>(
    client: &Client,
    token: &str,
    body: &S,
) -> Result<(u16, String, reqwest::header::HeaderMap), Box<dyn Error>> {
    let resp = client.patch(url("/organizations")).bearer_auth(token).json(body).timeout(Duration::from_secs(30)).send().await?;
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let body_text = resp.text().await?;
    Ok((status, body_text, headers))
}

/// GET /organizations/rate-limit.  Returns `(status_code, JSON value)`.
pub async fn get_rate_limit_api(client: &Client, token: &str) -> Result<(u16, serde_json::Value), Box<dyn Error>> {
    let resp = client.get(url("/organizations/rate-limit")).bearer_auth(token).timeout(Duration::from_secs(30)).send().await?;
    let status = resp.status().as_u16();
    let body_text = resp.text().await?;
    let val: serde_json::Value = serde_json::from_str(&body_text).unwrap_or(serde_json::Value::Null);
    Ok((status, val))
}

/// POST /api/v1/endpoints to register an OpenAI-compatible LLM endpoint.
///
/// `base_url` should be the server root (e.g. `"http://127.0.0.1:54321"`); the
/// function appends `/v1` automatically so the OpenAI-compatible path is used.
/// Returns the endpoint UUID string on success.
pub async fn create_llm_endpoint(client: &Client, token: &str, name: &str, base_url: &str) -> Result<String, Box<dyn Error>> {
    let compat_url = format!("{}/v1", base_url.trim_end_matches('/'));
    let conn = serde_json::json!({
        "provider": "OpenAI",
        "inline_api_key": "fake",
        "defaults": {
            "model": "gpt-3.5-turbo",
            "max_tokens": 64,
            "base_url_override": compat_url
        }
    });
    let resp = client
        .post(url("/endpoints"))
        .bearer_auth(token)
        .timeout(Duration::from_secs(30))
        .json(&serde_json::json!({
            "endpoint": name,
            "kind": "llm",
            "config": {
                "read_conn": conn,
                "write_conn": conn
            }
        }))
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await?;
    if status != 200 {
        return Err(format!("create_llm_endpoint: HTTP {status}: {body}").into());
    }
    let v: serde_json::Value = serde_json::from_str(&body)?;
    v.pointer("/data/uuid")
        .or_else(|| v.pointer("/uuid"))
        .and_then(|u| u.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| format!("no uuid in endpoint response: {body}").into())
}

/// PUT /api/v1/iam/data/endpoints/{endpoint}/subjects/{subject}
/// Grants the named subject (username) data-plane READ access on the endpoint.
/// Requires the caller to hold `ControlPerms::GRANT` (superadmin always has it).
pub async fn grant_endpoint_data_read(client: &Client, token: &str, endpoint_uuid: &str, subject: &str) -> Result<(), Box<dyn Error>> {
    grant_endpoint_data_perms(client, token, endpoint_uuid, subject, "r").await
}

/// PUT /api/v1/iam/data/endpoints/{endpoint}/subjects/{subject}
/// Grants the named subject (username) data-plane perms (e.g. "r", "rw", "rwx") on the endpoint.
/// Requires the caller to hold `ControlPerms::GRANT` (superadmin always has it).
pub async fn grant_endpoint_data_perms(
    client: &Client,
    token: &str,
    endpoint_uuid: &str,
    subject: &str,
    perms: &str,
) -> Result<(), Box<dyn Error>> {
    let path = format!("/iam/data/endpoints/{}/subjects/{}", endpoint_uuid, subject);
    let resp = client
        .put(url(&path))
        .bearer_auth(token)
        .timeout(Duration::from_secs(30))
        .json(&serde_json::json!({"perms": perms}))
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await?;
    if status != 200 {
        return Err(format!("grant_endpoint_data_perms: HTTP {status}: {body}").into());
    }
    Ok(())
}

pub async fn create_org(client: &Client, possible_relay_new_org_token: Option<&str>) -> Result<String, Box<dyn Error>> {
    let org_input = OrganizationInput::new(ORG_ID.to_owned(), Some(ORG_DESCR.to_owned()), vec![]);
    let response = if let Some(relay_new_org_token) = possible_relay_new_org_token {
        client.post(url("/new")).bearer_auth(relay_new_org_token).json(&org_input).send().await?
    } else {
        client.post(url("/new")).json(&org_input).send().await?
    };

    let status = response.status();
    let body = response.text().await?;

    if !status.is_success() {
        eprintln!("Error creating org: status={}, body={}", status, body);
        return Err(format!("Failed to create org: status={}, body={}", status, body).into());
    }

    Ok(body)
}

pub async fn create_org_with_superadmin_for_id(
    client: &Client,
    possible_relay_new_org_token: Option<&str>,
    org_id: &str,
    org_descr: &str,
    superadmin_username: &str,
    superadmin_password: &str,
) -> Result<String, Box<dyn Error>> {
    let superadmin = UserInput::new(
        superadmin_username.to_string(),
        superadmin_password.to_string(),
        None,
        None,
        None,
        ControlPerms::all(),
    );
    let org_input = OrganizationInput::new(org_id.to_owned(), Some(org_descr.to_owned()), vec![superadmin]);
    let response = if let Some(relay_new_org_token) = possible_relay_new_org_token {
        client
            .post(url("/new"))
            .bearer_auth(relay_new_org_token)
            .json(&org_input)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?
    } else {
        client.post(url("/new")).json(&org_input).send().await?.error_for_status()?.text().await?
    };
    Ok(response)
}

pub async fn create_org_with_superadmin(
    client: &Client,
    possible_relay_new_org_token: Option<&str>,
    superadmin_username: &str,
    superadmin_password: &str,
) -> Result<String, Box<dyn Error>> {
    let org_id = get_test_org_id();
    create_org_with_superadmin_for_id(client, possible_relay_new_org_token, &org_id, ORG_DESCR, superadmin_username, superadmin_password)
        .await
}

pub async fn auth_login(client: &Client, username: &str, password: &str) -> Result<JwtResponse, Box<dyn Error>> {
    let org_id = get_test_org_id();
    auth_login_for_org(client, &org_id, username, password).await
}

pub async fn auth_login_for_org(client: &Client, org_id: &str, username: &str, password: &str) -> Result<JwtResponse, Box<dyn Error>> {
    #[derive(Serialize)]
    struct LoginRequest {
        id: String,
    }

    let login_url = url("/auth/login");
    log::debug!("Auth login URL: {}", login_url);

    let response = client
        .post(&login_url)
        .header("X-Org-Id", org_id)
        .basic_auth(username, Some(password))
        .json(&LoginRequest { id: org_id.to_string() })
        .send()
        .await?;

    let status = response.status();
    let response_text = response.text().await?;

    if !status.is_success() {
        return Err(format!("Auth login failed with status {}: {}", status, response_text).into());
    }

    Ok(serde_json::from_str::<JwtResponse>(&response_text)?)
}

pub async fn create_user(client: &Client, token: &str, new_user: &UserInput) -> Result<(), Box<dyn Error>> {
    let _: Option<()> = make_request(client, token, &url("/iam/humans"), Some(new_user), true, Some(201)).await?;
    Ok(())
}

pub async fn endpoint_connect_pg(client: &Client, token: &str) -> Result<Option<Response>, Box<dyn Error>> {
    let pg_url = TestConfig::get_postgres_conn();

    let connect = serde_json::json!({
        "endpoint": "postgres_test1",
        "kind": "postgres",
        "config": {
            "url": pg_url
        },
        "routing": null,
        "description": "Postgres test connection"
    });
    make_request::<Value, Response>(client, token, &url("/endpoints"), Some(&connect), false, None).await
}

pub async fn endpoint_request(
    client: &Client,
    token: &str,
    endpoint_uuid: EndpointUuid,
    data: Value,
    req_type: EpRequestType,
    no_response: bool,
    expect_status: Option<u16>,
) -> Result<Value, Box<dyn Error>> {
    #[derive(Serialize, Deserialize)]
    struct EndpointRequest {
        request: Value,
    }

    let request_body = EndpointRequest { request: data };

    let resp_data = make_request::<EndpointRequest, Value>(
        client,
        token,
        &url(&format!("/endpoints/{}/{req_type}", endpoint_uuid.uuid())),
        Some(&request_body),
        no_response,
        expect_status,
    )
    .await?;

    Ok(resp_data.unwrap_or(Value::Null))
}
