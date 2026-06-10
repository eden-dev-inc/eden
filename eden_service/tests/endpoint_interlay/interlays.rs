#![cfg(external_db)]
use serde_json::json;

use crate::common::EDEN_NEW_ORG_TOKEN_VALUE;
use crate::util::test_server;

use crate::{
    common::{SUPERADMIN_ID, SUPERADMIN_PWD},
    request::{auth_login, create_org_with_superadmin},
};

use std::net::TcpListener;
use std::sync::Arc;
use testcontainers_modules::testcontainers::core::ContainerPort;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Parse Redis connection string in format "redis://host:port"
fn parse_redis_connection(conn: &str) -> Result<(String, u16), Box<dyn std::error::Error>> {
    // Remove "redis://" prefix
    let conn = conn.strip_prefix("redis://").unwrap_or(conn);

    // Split by colon to get host and port
    let parts: Vec<&str> = conn.split(':').collect();
    if parts.len() != 2 {
        return Err("Invalid Redis connection string format".into());
    }

    let host = parts[0].to_string();
    let port = parts[1].parse::<u16>()?;

    Ok((host, port))
}

/// Build API URL with base path
fn api_url(port: u16, path: &str) -> String {
    format!("http://localhost:{}/api/v1{}", port, path)
}

fn redis_endpoint_payload(endpoint: &str, host: &str, port: u16, description: &str) -> serde_json::Value {
    json!({
        "endpoint": endpoint,
        "kind": "redis",
        "config": {
            "read_conn": null,
            "write_conn": {
                "host": host,
                "port": port,
                "tls": false
            },
            "connection_pool": {
                "min_connections": 0,
                "max_connections": 1
            }
        },
        "description": description
    })
}

fn postgres_endpoint_payload(endpoint: &str, url: &str, description: &str) -> serde_json::Value {
    json!({
        "endpoint": endpoint,
        "kind": "postgres",
        "config": {
            "url": url
        },
        "description": description
    })
}

/// Send a Redis PING through an interlay and return the raw RESP response.
async fn redis_ping_via_interlay(port: u16) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    redis_command_via_interlay(port, b"*1\r\n$4\r\nPING\r\n").await
}

async fn redis_command_via_interlay(port: u16, request: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut stream =
        tokio::time::timeout(std::time::Duration::from_secs(2), tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))).await??;

    tokio::time::timeout(std::time::Duration::from_secs(1), stream.write_all(request)).await??;

    let mut buf = vec![0_u8; 16 * 1024];
    let bytes_read = tokio::time::timeout(std::time::Duration::from_secs(1), stream.read(&mut buf)).await??;

    buf.truncate(bytes_read);
    Ok(buf)
}

struct RedisClusterFixture {
    container: ContainerAsync<GenericImage>,
    backend_ports: Vec<u16>,
}

impl RedisClusterFixture {
    const BUS_PORT_OFFSET: u16 = 10_000;
    const MAX_BACKEND_PORT: u16 = u16::MAX - Self::BUS_PORT_OFFSET;

    async fn start() -> Self {
        let backend_ports = Self::find_available_backend_ports(3);
        let script = Self::container_script(&backend_ports);
        let mut image = GenericImage::new("redis", "7.2.4").with_cmd(["sh", "-c", script.as_str()]);
        for port in &backend_ports {
            image = image.with_mapped_port(*port, ContainerPort::Tcp(*port));
        }

        let container = image.start().await.expect("start redis cluster container");
        Self::wait_until_ready(backend_ports[0]).await;

        Self { container, backend_ports }
    }

    fn backend_ports(&self) -> &[u16] {
        &self.backend_ports
    }

    async fn stop(self) {
        self.container.stop().await.expect("Failed to stop Redis Cluster container");
    }

    fn find_available_backend_ports(count: usize) -> Vec<u16> {
        let mut ports = Vec::with_capacity(count);
        while ports.len() < count {
            let listener = TcpListener::bind("127.0.0.1:0").expect("reserve redis cluster backend port");
            let port = listener.local_addr().expect("read reserved redis cluster backend port").port();
            drop(listener);

            if Self::backend_port_is_available(port, &ports) {
                ports.push(port);
            }
        }
        ports
    }

    fn backend_port_is_available(port: u16, selected_ports: &[u16]) -> bool {
        port <= Self::MAX_BACKEND_PORT
            && selected_ports.iter().all(|selected_port| {
                let selected_bus_port = selected_port.saturating_add(Self::BUS_PORT_OFFSET);
                *selected_port != port && selected_bus_port != port && *selected_port != port.saturating_add(Self::BUS_PORT_OFFSET)
            })
    }

    fn container_script(ports: &[u16]) -> String {
        let server_start = ports
            .iter()
            .map(|port| {
                format!(
                    "mkdir -p /tmp/redis-{port}; \
cat > /tmp/redis-{port}/redis.conf <<'EOF'\n\
port {port}\n\
bind 0.0.0.0\n\
protected-mode no\n\
cluster-enabled yes\n\
cluster-config-file nodes.conf\n\
cluster-node-timeout 5000\n\
cluster-announce-ip 127.0.0.1\n\
cluster-announce-port {port}\n\
cluster-announce-bus-port {}\n\
appendonly no\n\
dir /tmp/redis-{port}\n\
EOF\n\
redis-server /tmp/redis-{port}/redis.conf &",
                    port + Self::BUS_PORT_OFFSET
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let cluster_nodes = ports.iter().map(|port| format!("127.0.0.1:{port}")).collect::<Vec<_>>().join(" ");

        format!(
            "set -e\n\
{server_start}\n\
for port in {ports}; do \
  until redis-cli -p \"$port\" ping >/dev/null 2>&1; do sleep 0.1; done; \
done\n\
yes yes | redis-cli --cluster create {cluster_nodes} --cluster-replicas 0\n\
echo cluster-ready\n\
wait",
            ports = ports.iter().map(u16::to_string).collect::<Vec<_>>().join(" ")
        )
    }

    async fn wait_until_ready(port: u16) {
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(30);
        loop {
            let ready = redis::Client::open(format!("redis://127.0.0.1:{port}"))
                .and_then(|client| {
                    let mut conn = client.get_connection()?;
                    redis::cmd("CLUSTER").arg("INFO").query::<String>(&mut conn)
                })
                .is_ok_and(|info| info.contains("cluster_state:ok"));

            if ready {
                return;
            }

            assert!(start.elapsed() <= timeout, "redis cluster did not become ready within {timeout:?}");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}

/// Make an authenticated GET request to the API
async fn get_authenticated(client: &reqwest::Client, url: String, token: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let response = client.get(url).header("Authorization", format!("Bearer {}", token)).send().await?;

    Ok(response.json::<serde_json::Value>().await?)
}

/// Make an authenticated POST request to the API
async fn post_authenticated(
    client: &reqwest::Client,
    url: String,
    token: &str,
    body: serde_json::Value,
) -> Result<(reqwest::StatusCode, serde_json::Value), Box<dyn std::error::Error>> {
    let response = client
        .post(url)
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;

    let status = response.status();
    let text = response.text().await?;
    let data = serde_json::from_str::<serde_json::Value>(&text).unwrap_or(serde_json::Value::Null);

    Ok((status, data))
}

/// Make an authenticated PATCH request to the API
async fn patch_authenticated(
    client: &reqwest::Client,
    url: String,
    token: &str,
    body: serde_json::Value,
) -> Result<(reqwest::StatusCode, serde_json::Value), Box<dyn std::error::Error>> {
    let response = client
        .patch(url)
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;

    let status = response.status();
    let text = response.text().await?;
    let data = serde_json::from_str::<serde_json::Value>(&text).unwrap_or(serde_json::Value::Null);

    Ok((status, data))
}

/// Make an authenticated DELETE request to the API
async fn delete_authenticated(
    client: &reqwest::Client,
    url: String,
    token: &str,
) -> Result<reqwest::StatusCode, Box<dyn std::error::Error>> {
    let response = client.delete(url).header("Authorization", format!("Bearer {}", token)).send().await?;

    Ok(response.status())
}

/// Get an interlay by ID and return its data
async fn get_interlay(
    client: &reqwest::Client,
    server_port: u16,
    interlay_id: &str,
    token: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    get_authenticated(client, api_url(server_port, &format!("/interlays/{}", interlay_id)), token).await
}

/// Assert that the interlay has the expected running state
fn assert_interlay_running_state(data: &serde_json::Value, expected: bool, context: &str) {
    let actual = data["running"].as_bool().unwrap_or_else(|| panic!("Missing running field in {}. Data: {:?}", context, data));
    assert_eq!(actual, expected, "Interlay running state mismatch in {}", context);
}

/// Test full CRUD roundtrip for interlays:
/// Create -> Read -> Update (start/stop) -> Delete
#[test]
fn test_interlay_crud_roundtrip() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            // Create organization and get admin token
            match create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD).await {
                Ok(resp) => println!("Organization created successfully: {}", resp),
                Err(e) => {
                    eprintln!("Warning: Failed to create organization: {}", e);
                    println!("Continuing anyway, org may already exist");
                }
            }

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed to login as admin");

            let admin_token = &admin_jwt.token;
            let server_port = crate::util::TestConfig::get_port();
            let redis_conn = crate::util::TestConfig::get_redis_conn();

            // Parse the Redis connection string to extract host and port
            let (redis_host, redis_port) = parse_redis_connection(&redis_conn).expect("Failed to parse Redis connection string");

            println!("Redis host: {}, port: {}", redis_host, redis_port);

            // Create a Redis endpoint
            let endpoint_payload = redis_endpoint_payload("redis_endpoint", &redis_host, redis_port, "Redis endpoint description");

            println!("Creating Redis endpoint...");

            let (endpoint_status, endpoint_data) =
                post_authenticated(&client, api_url(server_port, "/endpoints"), admin_token, endpoint_payload)
                    .await
                    .expect("Failed to create endpoint");

            assert!(endpoint_status.is_success(), "Failed to create endpoint. Status: {}", endpoint_status);

            let endpoint_uuid = endpoint_data["uuid"].as_str().expect("Missing endpoint uuid in response").to_string();
            let interlay_port = crate::util::find_available_interlay_port().expect("Failed to find available interlay port");

            // Create an interlay pointing to the endpoint
            let interlay_payload = json!({
                "id": "test_interlay",
                "endpoint": endpoint_uuid,
                "port": interlay_port,
                "tls": null,
                "settings": {},
            });

            let (create_status, interlay_data) =
                post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, interlay_payload)
                    .await
                    .expect("Failed to create interlay");

            assert!(create_status.is_success(), "Failed to create interlay. Status: {}", create_status);

            let interlay_id = interlay_data["id"].as_str().expect("Missing interlay id in response").to_string();

            // Assert that the interlay is running after creation
            assert!(interlay_data["running"].as_bool().expect("Missing running field in response"));
            let ping_response = redis_ping_via_interlay(interlay_port).await.expect("Redis PING should succeed while interlay is running");
            assert_eq!(ping_response, b"+PONG\r\n");

            // Stop the interlay
            let stop_status = client
                .post(api_url(server_port, &format!("/interlays/{}/stop", interlay_id)))
                .header("Authorization", format!("Bearer {}", admin_token))
                .send()
                .await
                .expect("Failed to stop interlay")
                .status();

            assert!(stop_status.is_success(), "Failed to stop interlay");

            // Verify that interlay is stopped
            let stopped_data = get_interlay(&client, server_port, &interlay_id, admin_token).await.expect("Failed to get interlay");
            assert_interlay_running_state(&stopped_data, false, "after stopping");

            // Start the interlay again
            let start_status = client
                .post(api_url(server_port, &format!("/interlays/{}/start", interlay_id)))
                .header("Authorization", format!("Bearer {}", admin_token))
                .send()
                .await
                .expect("Failed to start interlay")
                .status();

            assert!(start_status.is_success(), "Failed to start interlay");

            // Verify that interlay is running
            let running_data = get_interlay(&client, server_port, &interlay_id, admin_token).await.expect("Failed to get interlay");
            assert_interlay_running_state(&running_data, true, "after starting");
            let ping_response = redis_ping_via_interlay(interlay_port).await.expect("Redis PING should succeed after interlay restart");
            assert_eq!(ping_response, b"+PONG\r\n");

            // Delete the interlay
            let delete_status = delete_authenticated(&client, api_url(server_port, &format!("/interlays/{}", interlay_id)), admin_token)
                .await
                .expect("Failed to delete interlay");

            assert!(delete_status.is_success(), "Failed to delete interlay");

            // Verify that interlay is actually deleted
            let get_status = client
                .get(api_url(server_port, &format!("/interlays/{}", interlay_id)))
                .header("Authorization", format!("Bearer {}", admin_token))
                .send()
                .await
                .expect("Failed to get interlay")
                .status();

            assert_eq!(get_status, reqwest::StatusCode::NOT_FOUND);
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

#[test]
fn test_redis_cluster_interlay_virtualizes_topology_against_server() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            match create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD).await {
                Ok(resp) => println!("Organization created successfully: {}", resp),
                Err(e) => {
                    eprintln!("Warning: Failed to create organization: {}", e);
                    println!("Continuing anyway, org may already exist");
                }
            }

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed to login as admin");
            let admin_token = &admin_jwt.token;
            let server_port = crate::util::TestConfig::get_port();
            let cluster = RedisClusterFixture::start().await;

            let endpoint_payload = redis_endpoint_payload(
                "redis_cluster_endpoint",
                "127.0.0.1",
                cluster.backend_ports()[0],
                "Redis Cluster endpoint for virtual topology interlay test",
            );
            let (endpoint_status, endpoint_data) =
                post_authenticated(&client, api_url(server_port, "/endpoints"), admin_token, endpoint_payload)
                    .await
                    .expect("Failed to create Redis Cluster endpoint");
            assert!(
                endpoint_status.is_success(),
                "Failed to create Redis Cluster endpoint. Status: {endpoint_status}, body: {endpoint_data}"
            );
            let endpoint_uuid = endpoint_data["uuid"].as_str().expect("Missing endpoint uuid in response").to_string();

            let listener_ports = [
                crate::util::find_available_interlay_port().expect("cluster listener port 1"),
                crate::util::find_available_interlay_port().expect("cluster listener port 2"),
                crate::util::find_available_interlay_port().expect("cluster listener port 3"),
            ];
            let interlay_payload = json!({
                "id": "redis_cluster_virtual_interlay",
                "endpoint": endpoint_uuid,
                "listeners": [
                    { "id": "cluster-node-1", "bind_port": listener_ports[0], "advertise_port": listener_ports[0] },
                    { "id": "cluster-node-2", "bind_port": listener_ports[1], "advertise_port": listener_ports[1] },
                    { "id": "cluster-node-3", "bind_port": listener_ports[2], "advertise_port": listener_ports[2] }
                ],
                "advertise_host": "127.0.0.1",
                "tls": null,
                "settings": {},
            });
            let (create_status, interlay_data) =
                post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, interlay_payload)
                    .await
                    .expect("Failed to create Redis Cluster interlay");
            assert!(
                create_status.is_success(),
                "Failed to create Redis Cluster interlay. Status: {create_status}, body: {interlay_data}"
            );
            assert!(interlay_data["running"].as_bool().expect("Missing running field in response"));

            let nodes_response = redis_command_via_interlay(listener_ports[0], b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n")
                .await
                .expect("CLUSTER NODES via virtualized interlay");
            let nodes_response = String::from_utf8_lossy(&nodes_response);
            for listener_port in listener_ports {
                assert!(
                    nodes_response.contains(&format!("127.0.0.1:{listener_port}@")),
                    "CLUSTER NODES should advertise listener port {listener_port}, got {nodes_response}"
                );
            }
            for backend_port in cluster.backend_ports() {
                assert!(
                    !nodes_response.contains(&format!("127.0.0.1:{backend_port}@")),
                    "CLUSTER NODES leaked backend port {backend_port}: {nodes_response}"
                );
            }

            let slots_response = redis_command_via_interlay(listener_ports[0], b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n")
                .await
                .expect("CLUSTER SLOTS via virtualized interlay");
            let slots_response = String::from_utf8_lossy(&slots_response);
            for listener_port in listener_ports {
                assert!(
                    slots_response.contains(&format!(":{listener_port}\r\n")),
                    "CLUSTER SLOTS should advertise listener port {listener_port}, got {slots_response}"
                );
            }

            let set_response =
                redis_command_via_interlay(listener_ports[0], b"*3\r\n$3\r\nSET\r\n$16\r\ncluster:test:key\r\n$5\r\nvalue\r\n")
                    .await
                    .expect("SET through virtualized Redis Cluster interlay");
            assert_eq!(set_response, b"+OK\r\n");

            let get_response = redis_command_via_interlay(listener_ports[1], b"*2\r\n$3\r\nGET\r\n$16\r\ncluster:test:key\r\n")
                .await
                .expect("GET through another virtualized Redis Cluster listener");
            assert_eq!(get_response, b"$5\r\nvalue\r\n");

            delete_authenticated(&client, api_url(server_port, "/interlays/redis_cluster_virtual_interlay"), admin_token)
                .await
                .expect("Failed to delete Redis Cluster interlay");
            cluster.stop().await;
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

/// Test that the semaphore limits concurrent connections to interlays
#[test]
#[ignore = "flaky under ci startup timing; covered by other interlay runtime tests"]
fn test_interlay_connection_limit() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            // Create organization and get admin token
            match create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD).await {
                Ok(resp) => println!("Organization created successfully: {}", resp),
                Err(e) => {
                    eprintln!("Warning: Failed to create organization: {}", e);
                    println!("Continuing anyway, org may already exist");
                }
            }

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed to login as admin");

            let admin_token = &admin_jwt.token;
            let server_port = crate::util::TestConfig::get_port();
            let redis_conn = crate::util::TestConfig::get_redis_conn();

            // Parse the Redis connection string to extract host and port
            let (redis_host, redis_port) = parse_redis_connection(&redis_conn).expect("Failed to parse Redis connection string");

            // Create a Redis endpoint
            let endpoint_payload =
                redis_endpoint_payload("redis_endpoint_limit_test", &redis_host, redis_port, "Redis endpoint for connection limit test");

            let (endpoint_status, endpoint_data) =
                post_authenticated(&client, api_url(server_port, "/endpoints"), admin_token, endpoint_payload)
                    .await
                    .expect("Failed to create endpoint");

            assert!(endpoint_status.is_success(), "Failed to create endpoint. Status: {}", endpoint_status);

            let endpoint_uuid = endpoint_data["uuid"].as_str().expect("Missing endpoint uuid in response").to_string();
            let interlay_port = crate::util::find_available_interlay_port().expect("Failed to find available interlay port");

            // Create an interlay with low connection limit
            let interlay_payload = json!({
                "id": "test_interlay_limit",
                "endpoint": endpoint_uuid,
                "port": interlay_port,
                "tls": null,
                "settings": {
                    "max_concurrent_connections": 1
                },
            });

            let (create_status, interlay_data) =
                post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, interlay_payload)
                    .await
                    .expect("Failed to create interlay");

            assert!(create_status.is_success(), "Failed to create interlay. Status: {}", create_status);

            let interlay_id = interlay_data["id"].as_str().expect("Missing interlay id in response").to_string();

            // Assert that the interlay is running after creation
            assert!(interlay_data["running"].as_bool().expect("Missing running field in response"));

            // Now test the connection limit by attempting multiple concurrent connections
            let num_connections = 5;
            let mut handles = vec![];

            for i in 0..num_connections {
                let handle = tokio::spawn(async move {
                    match tokio::net::TcpStream::connect(format!("127.0.0.1:{}", interlay_port)).await {
                        Ok(mut stream) => {
                            // Try to send a simple Redis PING command
                            let ping_cmd = b"*1\r\n$4\r\nPING\r\n";
                            match tokio::time::timeout(tokio::time::Duration::from_secs(1), stream.write_all(ping_cmd)).await {
                                Ok(Ok(_)) => {
                                    // Try to read response
                                    let mut buf = [0; 1024];
                                    match tokio::time::timeout(std::time::Duration::from_millis(500), stream.read(&mut buf)).await {
                                        Ok(Ok(n)) if n > 0 => {
                                            println!("Connection {} succeeded: received {} bytes", i, n);
                                            true // Connection handled successfully
                                        }
                                        _ => {
                                            println!("Connection {} accepted but no response or timed out", i);
                                            false // Connection dropped or no response
                                        }
                                    }
                                }
                                _ => {
                                    println!("Connection {} failed to write", i);
                                    false
                                }
                            }
                        }
                        Err(e) => {
                            println!("Connection {} failed to connect: {}", i, e);
                            false
                        }
                    }
                });
                handles.push(handle);
            }

            // Collect results
            let mut successful_connections = 0;
            for handle in handles {
                if handle.await.expect("expected handle") {
                    successful_connections += 1;
                }
            }

            // With limit of 1, we expect at most 1 successful connection
            // (some may be dropped due to timing, but should be <= limit)
            assert!(
                successful_connections <= 1,
                "Expected at most 1 successful connection, got {}",
                successful_connections
            );

            println!("Connection limit test passed: {} successful connections with limit 1", successful_connections);

            // Clean up: delete the interlay
            let delete_status = delete_authenticated(&client, api_url(server_port, &format!("/interlays/{}", interlay_id)), admin_token)
                .await
                .expect("Failed to delete interlay");

            assert!(delete_status.is_success(), "Failed to delete interlay");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

/// Test full CRUD roundtrip for PostgreSQL interlays:
/// Create -> Read -> Update (start/stop) -> Delete
///
/// This mirrors `test_interlay_crud_roundtrip` but uses a PostgreSQL endpoint
/// instead of Redis, verifying the interlay lifecycle works for Postgres.
#[test]
fn test_interlay_postgres_crud_roundtrip() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            match create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD).await {
                Ok(resp) => println!("Organization created successfully: {}", resp),
                Err(e) => {
                    eprintln!("Warning: Failed to create organization: {}", e);
                    println!("Continuing anyway, org may already exist");
                }
            }

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed to login as admin");
            let admin_token = &admin_jwt.token;
            let server_port = crate::util::TestConfig::get_port();
            let pg_conn = crate::util::TestConfig::get_postgres_conn();

            println!("Postgres connection: {}", pg_conn);

            // Create a PostgreSQL endpoint
            let endpoint_payload = postgres_endpoint_payload("pg_interlay_endpoint", &pg_conn, "PostgreSQL endpoint for interlay test");

            println!("Creating PostgreSQL endpoint...");
            let (endpoint_status, endpoint_data) =
                post_authenticated(&client, api_url(server_port, "/endpoints"), admin_token, endpoint_payload)
                    .await
                    .expect("Failed to create endpoint");

            assert!(endpoint_status.is_success(), "Failed to create endpoint. Status: {}", endpoint_status);
            let endpoint_uuid = endpoint_data["uuid"].as_str().expect("Missing endpoint uuid in response").to_string();

            // Find an available port for the interlay
            let interlay_port = crate::util::find_available_interlay_port().expect("Failed to find available interlay port");

            // Create a PostgreSQL interlay
            let interlay_payload = json!({
                "id": "test_pg_interlay",
                "endpoint": endpoint_uuid,
                "port": interlay_port,
                "tls": null,
                "settings": {},
            });

            let (create_status, interlay_data) =
                post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, interlay_payload)
                    .await
                    .expect("Failed to create interlay");

            assert!(create_status.is_success(), "Failed to create PostgreSQL interlay. Status: {}", create_status);
            let interlay_id = interlay_data["id"].as_str().expect("Missing interlay id in response").to_string();

            // Assert running after creation (POST response includes running field)
            assert!(
                interlay_data["running"].as_bool().expect("Missing running field"),
                "Interlay should be running after creation"
            );

            // Verify interlay is retrievable via GET
            let get_data = get_interlay(&client, server_port, &interlay_id, admin_token).await.expect("Failed to get interlay");
            assert_eq!(
                get_data["uuid"].as_str().expect("Missing uuid"),
                interlay_data["uuid"].as_str().expect("Missing uuid"),
                "GET should return same interlay"
            );

            // Verify the interlay is accepting connections while running
            let tcp_connect = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                tokio::net::TcpStream::connect(format!("127.0.0.1:{}", interlay_port)),
            )
            .await;
            assert!(
                tcp_connect.as_ref().is_ok_and(|r| r.is_ok()),
                "Interlay should accept TCP connections while running"
            );

            // Stop the interlay
            let stop_status = client
                .post(api_url(server_port, &format!("/interlays/{}/stop", interlay_id)))
                .header("Authorization", format!("Bearer {}", admin_token))
                .send()
                .await
                .expect("Failed to stop interlay")
                .status();

            assert!(stop_status.is_success(), "Failed to stop PostgreSQL interlay");

            // Verify stopped: TCP connection should be refused
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let tcp_refused = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", interlay_port)).await;
            assert!(tcp_refused.is_err(), "Interlay should refuse connections after stop");

            // Stopping again should fail (not running)
            let stop_again_status = client
                .post(api_url(server_port, &format!("/interlays/{}/stop", interlay_id)))
                .header("Authorization", format!("Bearer {}", admin_token))
                .send()
                .await
                .expect("Failed to send stop request")
                .status();
            assert!(!stop_again_status.is_success(), "Stopping an already-stopped interlay should fail");

            // Start the interlay again
            let start_status = client
                .post(api_url(server_port, &format!("/interlays/{}/start", interlay_id)))
                .header("Authorization", format!("Bearer {}", admin_token))
                .send()
                .await
                .expect("Failed to start interlay")
                .status();

            assert!(start_status.is_success(), "Failed to start PostgreSQL interlay");

            // Give it a moment to bind the port
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;

            // Verify running again: TCP connection should succeed
            let tcp_reconnect = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                tokio::net::TcpStream::connect(format!("127.0.0.1:{}", interlay_port)),
            )
            .await;
            assert!(
                tcp_reconnect.as_ref().is_ok_and(|r| r.is_ok()),
                "Interlay should accept TCP connections after restart"
            );

            // Delete the interlay
            let delete_status = delete_authenticated(&client, api_url(server_port, &format!("/interlays/{}", interlay_id)), admin_token)
                .await
                .expect("Failed to delete interlay");

            assert!(delete_status.is_success(), "Failed to delete PostgreSQL interlay");

            // Verify deleted via GET → 404
            let get_status = client
                .get(api_url(server_port, &format!("/interlays/{}", interlay_id)))
                .header("Authorization", format!("Bearer {}", admin_token))
                .send()
                .await
                .expect("Failed to get interlay")
                .status();

            assert_eq!(get_status, reqwest::StatusCode::NOT_FOUND);
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

/// Test port validation rejects privileged and unavailable ports.
#[test]
fn test_interlay_port_validation() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            match create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD).await {
                Ok(resp) => println!("Organization created successfully: {}", resp),
                Err(e) => {
                    eprintln!("Warning: Failed to create organization: {}", e);
                }
            }

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed to login as admin");
            let admin_token = &admin_jwt.token;
            let server_port = crate::util::TestConfig::get_port();
            let redis_conn = crate::util::TestConfig::get_redis_conn();
            let (redis_host, redis_port) = parse_redis_connection(&redis_conn).expect("Failed to parse Redis connection string");

            // Create endpoint first
            let endpoint_payload =
                redis_endpoint_payload("redis_ep_port_val", &redis_host, redis_port, "Redis endpoint for port validation test");
            let (ep_status, ep_data) = post_authenticated(&client, api_url(server_port, "/endpoints"), admin_token, endpoint_payload)
                .await
                .expect("Failed to create endpoint");
            assert!(ep_status.is_success(), "Failed to create endpoint");
            let endpoint_uuid = ep_data["uuid"].as_str().expect("Missing endpoint uuid").to_string();
            let valid_port = crate::util::find_available_interlay_port().expect("Failed to find valid interlay port");
            let occupied_listener = TcpListener::bind("127.0.0.1:0").expect("Failed to reserve an occupied port");
            let occupied_port = occupied_listener.local_addr().expect("Failed to inspect occupied port").port();

            // Test: privileged port (< 1024) should be rejected
            let payload = json!({
                "id": "priv_port_interlay",
                "endpoint": endpoint_uuid,
                "port": 80,
                "tls": null,
                "settings": {},
            });
            let (status, _) =
                post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, payload).await.expect("Request failed");
            assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "Privileged port should be rejected");

            // Test: currently occupied port should be rejected
            let payload = json!({
                "id": "occupied_port_interlay",
                "endpoint": endpoint_uuid,
                "port": occupied_port,
                "tls": null,
                "settings": {},
            });
            let (status, _) =
                post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, payload).await.expect("Request failed");
            assert_eq!(status, reqwest::StatusCode::CONFLICT, "Occupied port should be rejected");

            // Test: valid port should succeed
            let payload = json!({
                "id": "valid_port_interlay",
                "endpoint": endpoint_uuid,
                "port": valid_port,
                "tls": null,
                "settings": {},
            });
            let (status, _) =
                post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, payload).await.expect("Request failed");
            assert!(status.is_success(), "Valid port should succeed, got {}", status);

            // Clean up
            delete_authenticated(&client, api_url(server_port, "/interlays/valid_port_interlay"), admin_token)
                .await
                .expect("Failed to delete interlay");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

/// Test that a PostgreSQL interlay can proxy PG wire protocol connections.
///
/// Creates a PostgreSQL endpoint + interlay, connects to the interlay port
/// using the raw PG wire protocol, sends a simple query (`SELECT 1`),
/// and verifies a valid response is received.
#[test]
fn test_interlay_postgres_connection() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            match create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD).await {
                Ok(resp) => println!("Organization created successfully: {}", resp),
                Err(e) => {
                    eprintln!("Warning: Failed to create organization: {}", e);
                    println!("Continuing anyway, org may already exist");
                }
            }

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed to login as admin");
            let admin_token = &admin_jwt.token;
            let server_port = crate::util::TestConfig::get_port();
            let pg_conn = crate::util::TestConfig::get_postgres_conn();

            // Create a PostgreSQL endpoint
            let endpoint_payload = postgres_endpoint_payload("pg_conn_test_endpoint", &pg_conn, "PostgreSQL endpoint for connection test");

            let (endpoint_status, endpoint_data) =
                post_authenticated(&client, api_url(server_port, "/endpoints"), admin_token, endpoint_payload)
                    .await
                    .expect("Failed to create endpoint");

            assert!(endpoint_status.is_success(), "Failed to create endpoint. Status: {}", endpoint_status);
            let endpoint_uuid = endpoint_data["uuid"].as_str().expect("Missing endpoint uuid in response").to_string();

            // Find an available port for the interlay
            let interlay_port = crate::util::find_available_interlay_port().expect("Failed to find available interlay port");

            // Create a PostgreSQL interlay
            let interlay_payload = json!({
                "id": "test_pg_conn_interlay",
                "endpoint": endpoint_uuid,
                "port": interlay_port,
                "tls": null,
                "settings": {},
            });

            let (create_status, interlay_data) =
                post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, interlay_payload)
                    .await
                    .expect("Failed to create interlay");

            assert!(create_status.is_success(), "Failed to create PostgreSQL interlay. Status: {}", create_status);
            let interlay_id = interlay_data["id"].as_str().expect("Missing interlay id in response").to_string();

            // Give the interlay a moment to bind the port
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            // Connect to the interlay using raw PG wire protocol
            let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", interlay_port))
                .await
                .expect("Failed to connect to PostgreSQL interlay");

            // Build PG StartupMessage: length(4) + version(4) + params
            // Version 3.0 = 0x00030000
            let mut startup = Vec::new();
            let params = b"user\0postgres\0database\0postgres\0\0";
            let length = (4 + 4 + params.len()) as i32;
            startup.extend_from_slice(&length.to_be_bytes());
            startup.extend_from_slice(&0x0003_0000i32.to_be_bytes()); // version 3.0
            startup.extend_from_slice(params);

            stream.write_all(&startup).await.expect("Failed to send startup message");

            // Read response — should get AuthenticationOk + parameters + ReadyForQuery
            let mut buf = vec![0u8; 4096];
            let n = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut buf))
                .await
                .expect("Timeout waiting for startup response")
                .expect("Failed to read startup response");

            assert!(n > 0, "Should receive startup response bytes");

            // Parse response: first message should be AuthenticationOk (R message, type=0)
            // Format: 'R'(1) + length(4) + auth_type(4)
            assert_eq!(buf[0], b'R', "First message should be AuthenticationOk (R)");
            let auth_length = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            assert_eq!(auth_length, 8, "AuthenticationOk length should be 8");
            let auth_type = i32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]);
            assert_eq!(auth_type, 0, "Auth type should be 0 (AuthenticationOk)");

            println!("PostgreSQL interlay startup handshake successful ({} bytes)", n);

            // Find ReadyForQuery ('Z') in the response to confirm fully ready
            let mut found_ready = false;
            let mut pos = 0;
            while pos < n {
                let msg_type = buf[pos];
                if pos + 5 > n {
                    break;
                }
                let msg_len = i32::from_be_bytes([buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4]]) as usize;
                if msg_type == b'Z' {
                    found_ready = true;
                    // ReadyForQuery has 1 byte status: 'I' = idle
                    if pos + 5 < n {
                        let status = buf[pos + 5];
                        assert_eq!(status, b'I', "Transaction status should be Idle");
                    }
                    break;
                }
                pos += 1 + msg_len;
            }
            assert!(found_ready, "Should receive ReadyForQuery message");

            // Send a simple query: SELECT 1
            let query = b"SELECT 1\0";
            let query_len = (4 + query.len()) as i32;
            let mut query_msg = vec![b'Q'];
            query_msg.extend_from_slice(&query_len.to_be_bytes());
            query_msg.extend_from_slice(query);

            stream.write_all(&query_msg).await.expect("Failed to send query");

            // Read query response
            let mut response_buf = vec![0u8; 4096];
            let response_n = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut response_buf))
                .await
                .expect("Timeout waiting for query response")
                .expect("Failed to read query response");

            assert!(response_n > 0, "Should receive query response bytes");
            println!("Received {} bytes in query response", response_n);

            // The response should contain: RowDescription (T) + DataRow (D) + CommandComplete (C) + ReadyForQuery (Z)
            // Check for DataRow ('D') message to confirm we got query results
            let mut found_data_row = false;
            let mut found_command_complete = false;
            pos = 0;
            while pos < response_n {
                let msg_type = response_buf[pos];
                if pos + 5 > response_n {
                    break;
                }
                let msg_len = i32::from_be_bytes([
                    response_buf[pos + 1],
                    response_buf[pos + 2],
                    response_buf[pos + 3],
                    response_buf[pos + 4],
                ]) as usize;

                match msg_type {
                    b'D' => found_data_row = true,
                    b'C' => found_command_complete = true,
                    b'E' => {
                        // ErrorResponse — extract the message for debugging
                        let error_bytes = &response_buf[pos + 5..pos + 1 + msg_len];
                        let error_msg = String::from_utf8_lossy(error_bytes);
                        panic!("Received ErrorResponse from interlay: {}", error_msg);
                    }
                    _ => {}
                }
                pos += 1 + msg_len;
            }

            assert!(found_data_row, "Should receive DataRow message with query results");
            assert!(found_command_complete, "Should receive CommandComplete message");

            println!("PostgreSQL interlay successfully proxied SELECT 1 query");

            // Clean up: delete the interlay
            let delete_status = delete_authenticated(&client, api_url(server_port, &format!("/interlays/{}", interlay_id)), admin_token)
                .await
                .expect("Failed to delete interlay");

            assert!(delete_status.is_success(), "Failed to delete PostgreSQL interlay");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

/// Test port collision detection returns 409 Conflict
#[test]
fn test_interlay_port_collision() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            match create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD).await {
                Ok(resp) => println!("Organization created successfully: {}", resp),
                Err(e) => {
                    eprintln!("Warning: Failed to create organization: {}", e);
                }
            }

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed to login as admin");
            let admin_token = &admin_jwt.token;
            let server_port = crate::util::TestConfig::get_port();
            let redis_conn = crate::util::TestConfig::get_redis_conn();
            let (redis_host, redis_port) = parse_redis_connection(&redis_conn).expect("Failed to parse Redis connection string");

            // Create endpoint
            let endpoint_payload =
                redis_endpoint_payload("redis_ep_collision", &redis_host, redis_port, "Redis endpoint for collision test");
            let (ep_status, ep_data) = post_authenticated(&client, api_url(server_port, "/endpoints"), admin_token, endpoint_payload)
                .await
                .expect("Failed to create endpoint");
            assert!(ep_status.is_success());
            let endpoint_uuid = ep_data["uuid"].as_str().expect("Missing endpoint uuid").to_string();
            let interlay_port = crate::util::find_available_interlay_port().expect("Failed to find available interlay port");

            // Create first interlay on a valid interlay port
            let payload1 = json!({
                "id": "interlay_a",
                "endpoint": endpoint_uuid,
                "port": interlay_port,
                "tls": null,
                "settings": {},
            });
            let (status, _) =
                post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, payload1).await.expect("Request failed");
            assert!(status.is_success(), "First interlay should succeed");

            // Create second interlay on the same port — should get 409 Conflict
            let payload2 = json!({
                "id": "interlay_b",
                "endpoint": endpoint_uuid,
                "port": interlay_port,
                "tls": null,
                "settings": {},
            });
            let (status, _) =
                post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, payload2).await.expect("Request failed");
            assert_eq!(status, reqwest::StatusCode::CONFLICT, "Duplicate port should return 409 Conflict");

            // Idempotent re-creation of the same interlay ID should succeed
            let payload_same = json!({
                "id": "interlay_a",
                "endpoint": endpoint_uuid,
                "port": interlay_port,
                "tls": null,
                "settings": {},
            });
            let (status, _) = post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, payload_same)
                .await
                .expect("Request failed");
            assert!(status.is_success(), "Idempotent re-creation should succeed, got {}", status);

            // Clean up
            delete_authenticated(&client, api_url(server_port, "/interlays/interlay_a"), admin_token)
                .await
                .expect("Failed to delete interlay");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

/// Test PATCH endpoint for partial interlay updates
#[test]
fn test_interlay_patch() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            match create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD).await {
                Ok(resp) => println!("Organization created successfully: {}", resp),
                Err(e) => {
                    eprintln!("Warning: Failed to create organization: {}", e);
                }
            }

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed to login as admin");
            let admin_token = &admin_jwt.token;
            let server_port = crate::util::TestConfig::get_port();
            let redis_conn = crate::util::TestConfig::get_redis_conn();
            let (redis_host, redis_port) = parse_redis_connection(&redis_conn).expect("Failed to parse Redis connection string");

            // Create endpoint
            let endpoint_payload = redis_endpoint_payload("redis_ep_patch", &redis_host, redis_port, "Redis endpoint for patch test");
            let (ep_status, ep_data) = post_authenticated(&client, api_url(server_port, "/endpoints"), admin_token, endpoint_payload)
                .await
                .expect("Failed to create endpoint");
            assert!(ep_status.is_success());
            let endpoint_uuid = ep_data["uuid"].as_str().expect("Missing endpoint uuid").to_string();
            let create_port = crate::util::find_available_interlay_port().expect("Failed to find initial interlay port");

            // Create interlay
            let create_payload = json!({
                "id": "patch_interlay",
                "endpoint": endpoint_uuid,
                "port": create_port,
                "tls": null,
                "settings": {},
            });
            let (create_status, create_data) = post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, create_payload)
                .await
                .expect("Failed to create interlay");
            assert!(create_status.is_success(), "Failed to create interlay");
            assert_eq!(create_data["port"].as_u64().expect("missing port"), u64::from(create_port));

            // PATCH: update port
            let updated_port = crate::util::find_available_interlay_port().expect("Failed to find updated interlay port");
            let patch_payload = json!({ "port": updated_port });
            let (patch_status, patch_data) =
                patch_authenticated(&client, api_url(server_port, "/interlays/patch_interlay"), admin_token, patch_payload)
                    .await
                    .expect("Failed to patch interlay");
            assert!(patch_status.is_success(), "PATCH port should succeed, got {}", patch_status);
            assert_eq!(
                patch_data["port"].as_u64().expect("missing port"),
                u64::from(updated_port),
                "Port should be updated"
            );

            // PATCH: update description only (no restart needed)
            let patch_payload = json!({ "description": "Updated description" });
            let (patch_status, _) =
                patch_authenticated(&client, api_url(server_port, "/interlays/patch_interlay"), admin_token, patch_payload)
                    .await
                    .expect("Failed to patch interlay");
            assert!(patch_status.is_success(), "PATCH description should succeed");

            // PATCH: reject reserved port
            let patch_payload = json!({ "port": 5432 });
            let (patch_status, _) =
                patch_authenticated(&client, api_url(server_port, "/interlays/patch_interlay"), admin_token, patch_payload)
                    .await
                    .expect("Failed to patch interlay");
            assert_eq!(patch_status, reqwest::StatusCode::BAD_REQUEST, "PATCH with reserved port should fail");

            // Create a second interlay, then verify patching it onto an occupied
            // port is rejected with 409.
            let create_b_port = crate::util::find_available_interlay_port().expect("Failed to find second interlay port");
            let create_b_payload = json!({
                "id": "patch_interlay_b",
                "endpoint": endpoint_uuid,
                "port": create_b_port,
                "tls": null,
                "settings": {},
            });
            let (create_b_status, _) = post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, create_b_payload)
                .await
                .expect("Failed to create second interlay");
            assert!(create_b_status.is_success(), "Second interlay should succeed, got {}", create_b_status);

            let (patch_collision_status, _) = patch_authenticated(
                &client,
                api_url(server_port, "/interlays/patch_interlay_b"),
                admin_token,
                json!({ "port": updated_port }),
            )
            .await
            .expect("Failed to patch second interlay");
            assert_eq!(
                patch_collision_status,
                reqwest::StatusCode::CONFLICT,
                "PATCH onto an already-used port should return 409 Conflict"
            );

            // Clean up
            delete_authenticated(&client, api_url(server_port, "/interlays/patch_interlay"), admin_token)
                .await
                .expect("Failed to delete interlay");
            delete_authenticated(&client, api_url(server_port, "/interlays/patch_interlay_b"), admin_token)
                .await
                .expect("Failed to delete second interlay");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

/// Test that PATCH with restart-triggering changes properly cleans up old state
/// (old listener closed, new listener active, interlay still marked running).
#[test]
fn test_interlay_patch_state_cleanup() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            match create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD).await {
                Ok(resp) => println!("Organization created successfully: {}", resp),
                Err(e) => {
                    eprintln!("Warning: Failed to create organization: {}", e);
                }
            }

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed to login as admin");
            let admin_token = &admin_jwt.token;
            let server_port = crate::util::TestConfig::get_port();
            let redis_conn = crate::util::TestConfig::get_redis_conn();
            let (redis_host, redis_port) = parse_redis_connection(&redis_conn).expect("Failed to parse Redis connection string");

            // Create endpoint
            let endpoint_payload =
                redis_endpoint_payload("redis_ep_cleanup", &redis_host, redis_port, "Redis endpoint for state cleanup test");
            let (ep_status, ep_data) = post_authenticated(&client, api_url(server_port, "/endpoints"), admin_token, endpoint_payload)
                .await
                .expect("Failed to create endpoint");
            assert!(ep_status.is_success());
            let endpoint_uuid = ep_data["uuid"].as_str().expect("Missing endpoint uuid").to_string();

            let old_port = crate::util::find_available_interlay_port().expect("Failed to find old interlay port");
            let new_port = crate::util::find_available_interlay_port().expect("Failed to find new interlay port");

            // Create interlay on old_port
            let create_payload = json!({
                "id": "cleanup_interlay",
                "endpoint": endpoint_uuid,
                "port": old_port,
                "tls": null,
                "settings": {},
            });
            let (create_status, create_data) = post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, create_payload)
                .await
                .expect("Failed to create interlay");
            assert!(create_status.is_success(), "Failed to create interlay");
            assert_interlay_running_state(&create_data, true, "after creation");

            // Verify the interlay is accepting connections on old_port
            let old_conn =
                tokio::time::timeout(std::time::Duration::from_secs(2), tokio::net::TcpStream::connect(format!("127.0.0.1:{}", old_port)))
                    .await;
            assert!(matches!(old_conn, Ok(Ok(_))), "Interlay should accept connections on old port before PATCH");

            // PATCH: change port (triggers restart)
            let patch_payload = json!({ "port": new_port });
            let (patch_status, patch_data) =
                patch_authenticated(&client, api_url(server_port, "/interlays/cleanup_interlay"), admin_token, patch_payload)
                    .await
                    .expect("Failed to patch interlay");
            assert!(patch_status.is_success(), "PATCH port should succeed, got {}", patch_status);
            assert_eq!(patch_data["port"].as_u64().expect("missing port"), new_port as u64);

            // 1. Interlay must still be reported as running after restart
            assert_interlay_running_state(&patch_data, true, "PATCH response after port change");

            let get_data = get_interlay(&client, server_port, "cleanup_interlay", admin_token).await.expect("Failed to get interlay");
            assert_interlay_running_state(&get_data, true, "GET after port change");

            // 2. Old port listener must be shut down — connection should be refused
            // Give the OS a moment to release the socket
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;

            let old_conn =
                tokio::time::timeout(std::time::Duration::from_secs(1), tokio::net::TcpStream::connect(format!("127.0.0.1:{}", old_port)))
                    .await;
            assert!(
                !matches!(old_conn, Ok(Ok(_))),
                "Old port {} should no longer accept connections after PATCH",
                old_port
            );

            // 3. New port listener must be active
            let new_conn =
                tokio::time::timeout(std::time::Duration::from_secs(2), tokio::net::TcpStream::connect(format!("127.0.0.1:{}", new_port)))
                    .await;
            assert!(
                matches!(new_conn, Ok(Ok(_))),
                "Interlay should accept connections on new port {} after PATCH",
                new_port
            );

            // 4. Old port is freed — another interlay can claim it
            let reuse_payload = json!({
                "id": "reuse_interlay",
                "endpoint": endpoint_uuid,
                "port": old_port,
                "tls": null,
                "settings": {},
            });
            let (reuse_status, _) = post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, reuse_payload)
                .await
                .expect("Failed to create reuse interlay");
            assert!(reuse_status.is_success(), "Should be able to reuse old port {}, got {}", old_port, reuse_status);

            // Clean up
            delete_authenticated(&client, api_url(server_port, "/interlays/cleanup_interlay"), admin_token)
                .await
                .expect("Failed to delete cleanup_interlay");
            delete_authenticated(&client, api_url(server_port, "/interlays/reuse_interlay"), admin_token)
                .await
                .expect("Failed to delete reuse_interlay");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

/// Test that rapid port changes (A → B → A) succeed reliably.
///
/// Before the shutdown-await fix, PATCH would fire a shutdown signal and
/// immediately try to bind the new port. If the new port was the *original*
/// port, the old listener hadn't released the socket yet, causing an
/// intermittent bind failure. With the fix, PATCH awaits the `shutdown_notify`
/// so the old listener is guaranteed to have exited before rebinding.
#[test]
fn test_interlay_patch_same_port_rebind() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            match create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD).await {
                Ok(resp) => println!("Organization created successfully: {}", resp),
                Err(e) => {
                    eprintln!("Warning: Failed to create organization: {}", e);
                }
            }

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed to login as admin");
            let admin_token = &admin_jwt.token;
            let server_port = crate::util::TestConfig::get_port();
            let redis_conn = crate::util::TestConfig::get_redis_conn();
            let (redis_host, redis_port) = parse_redis_connection(&redis_conn).expect("Failed to parse Redis connection string");

            // Create endpoint
            let endpoint_payload =
                redis_endpoint_payload("redis_ep_rebind", &redis_host, redis_port, "Redis endpoint for same-port rebind test");
            let (ep_status, ep_data) = post_authenticated(&client, api_url(server_port, "/endpoints"), admin_token, endpoint_payload)
                .await
                .expect("Failed to create endpoint");
            assert!(ep_status.is_success());
            let endpoint_uuid = ep_data["uuid"].as_str().expect("Missing endpoint uuid").to_string();

            let port_a = crate::util::find_available_interlay_port().expect("Failed to find first rebind port");
            let port_b = crate::util::find_available_interlay_port().expect("Failed to find second rebind port");

            // Create interlay on port_a
            let create_payload = json!({
                "id": "rebind_interlay",
                "endpoint": endpoint_uuid,
                "port": port_a,
                "tls": null,
                "settings": {},
            });
            let (create_status, create_data) = post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, create_payload)
                .await
                .expect("Failed to create interlay");
            assert!(create_status.is_success(), "Failed to create interlay");
            assert_interlay_running_state(&create_data, true, "after creation");

            // PATCH: move to port_b
            let patch_payload = json!({ "port": port_b });
            let (patch_status, patch_data) =
                patch_authenticated(&client, api_url(server_port, "/interlays/rebind_interlay"), admin_token, patch_payload)
                    .await
                    .expect("Failed to patch interlay to port_b");
            assert!(patch_status.is_success(), "PATCH to port_b should succeed, got {}", patch_status);
            assert_eq!(patch_data["port"].as_u64().expect("missing port"), port_b as u64);
            assert_interlay_running_state(&patch_data, true, "after PATCH to port_b");

            // PATCH: move back to port_a immediately.
            // Without the shutdown-await fix this would intermittently fail with
            // a bind error because the old listener on port_a might not have
            // released the socket yet.
            let patch_payload = json!({ "port": port_a });
            let (patch_status, patch_data) =
                patch_authenticated(&client, api_url(server_port, "/interlays/rebind_interlay"), admin_token, patch_payload)
                    .await
                    .expect("Failed to patch interlay back to port_a");
            assert!(
                patch_status.is_success(),
                "PATCH back to original port_a should succeed (shutdown awaited), got {}",
                patch_status
            );
            assert_eq!(patch_data["port"].as_u64().expect("missing port"), port_a as u64);
            assert_interlay_running_state(&patch_data, true, "after PATCH back to port_a");

            // Verify port_a is accepting connections
            let conn =
                tokio::time::timeout(std::time::Duration::from_secs(2), tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port_a)))
                    .await;
            assert!(matches!(conn, Ok(Ok(_))), "Interlay should accept connections on port_a after rebind");

            // Verify port_b is no longer accepting connections
            let old_conn =
                tokio::time::timeout(std::time::Duration::from_secs(1), tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port_b)))
                    .await;
            assert!(!matches!(old_conn, Ok(Ok(_))), "port_b should no longer accept connections after rebind to port_a");

            // Clean up
            delete_authenticated(&client, api_url(server_port, "/interlays/rebind_interlay"), admin_token)
                .await
                .expect("Failed to delete interlay");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

/// Test that a failed PATCH (port occupied externally) triggers rollback and
/// the interlay remains running on its original port.
#[test]
#[ignore = "flaky under ci startup timing; covered by other interlay patch/runtime tests"]
fn test_interlay_patch_rollback_on_init_failure() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            match create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD).await {
                Ok(resp) => println!("Organization created successfully: {}", resp),
                Err(e) => {
                    eprintln!("Warning: Failed to create organization: {}", e);
                }
            }

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed to login as admin");
            let admin_token = &admin_jwt.token;
            let server_port = crate::util::TestConfig::get_port();
            let redis_conn = crate::util::TestConfig::get_redis_conn();
            let (redis_host, redis_port) = parse_redis_connection(&redis_conn).expect("Failed to parse Redis connection string");

            // Create endpoint
            let endpoint_payload = redis_endpoint_payload("redis_ep_rollback", &redis_host, redis_port, "Redis endpoint for rollback test");
            let (ep_status, ep_data) = post_authenticated(&client, api_url(server_port, "/endpoints"), admin_token, endpoint_payload)
                .await
                .expect("Failed to create endpoint");
            assert!(ep_status.is_success());
            let endpoint_uuid = ep_data["uuid"].as_str().expect("Missing endpoint uuid").to_string();

            let original_port = crate::util::find_available_interlay_port().expect("Failed to find original rollback port");
            let occupied_port = crate::util::find_available_interlay_port().expect("Failed to find occupied rollback port");

            // Create interlay on original_port
            let create_payload = json!({
                "id": "rollback_interlay",
                "endpoint": endpoint_uuid,
                "port": original_port,
                "tls": null,
                "settings": {},
            });
            let (create_status, create_data) = post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, create_payload)
                .await
                .expect("Failed to create interlay");
            assert!(create_status.is_success(), "Failed to create interlay");
            assert_interlay_running_state(&create_data, true, "after creation");

            // Externally occupy occupied_port so the PATCH will fail to bind
            let blocker = TcpListener::bind(format!("0.0.0.0:{}", occupied_port)).expect("Failed to bind blocker on occupied_port");

            // PATCH: try to move to occupied_port — should fail
            let patch_payload = json!({ "port": occupied_port });
            let (patch_status, _) =
                patch_authenticated(&client, api_url(server_port, "/interlays/rollback_interlay"), admin_token, patch_payload)
                    .await
                    .expect("Failed to send PATCH request");
            assert!(
                patch_status.is_server_error() || patch_status.is_client_error(),
                "PATCH to occupied port should fail, got {}",
                patch_status
            );

            // Verify the interlay still accepts connections on original_port (rollback re-init worked)
            let conn = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                tokio::net::TcpStream::connect(format!("127.0.0.1:{}", original_port)),
            )
            .await;
            assert!(
                matches!(conn, Ok(Ok(_))),
                "Interlay should still accept connections on original port after failed PATCH"
            );

            // Verify DB was rolled back: a successful PATCH to a free port should show original_port as baseline
            let free_port = crate::util::find_available_interlay_port().expect("Failed to find free rollback port");
            let patch_payload = json!({ "port": free_port });
            let (patch_status, patch_data) =
                patch_authenticated(&client, api_url(server_port, "/interlays/rollback_interlay"), admin_token, patch_payload)
                    .await
                    .expect("Failed to send recovery PATCH");
            assert!(patch_status.is_success(), "PATCH to free port should succeed after rollback, got {}", patch_status);
            assert_eq!(
                patch_data["port"].as_u64().expect("missing port"),
                free_port as u64,
                "Port should now be the new free port"
            );
            assert_interlay_running_state(&patch_data, true, "after recovery PATCH");

            // Release the blocker and clean up
            drop(blocker);
            delete_authenticated(&client, api_url(server_port, "/interlays/rollback_interlay"), admin_token)
                .await
                .expect("Failed to delete interlay");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

/// Test that concurrent PATCHes on the same interlay are serialized by the
/// per-interlay lock. Without the lock, two simultaneous port-change PATCHes
/// would both read the same initial schema, both shut down the listener, and
/// both try to spawn — leaving duplicates or a dead interlay.
///
/// With the lock, the second PATCH waits for the first to finish, reads the
/// updated schema, and applies its change on top. After both complete:
/// - exactly one listener is active (on one of the two target ports)
/// - the interlay is reported as running
/// - both PATCHes succeed (no 500s from conflicting restarts)
#[test]
fn test_concurrent_patch_serialization() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            match create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD).await {
                Ok(resp) => println!("Organization created successfully: {}", resp),
                Err(e) => {
                    eprintln!("Warning: Failed to create organization: {}", e);
                }
            }

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed to login as admin");
            let admin_token = &admin_jwt.token;
            let server_port = crate::util::TestConfig::get_port();
            let redis_conn = crate::util::TestConfig::get_redis_conn();
            let (redis_host, redis_port) = parse_redis_connection(&redis_conn).expect("Failed to parse Redis connection string");

            // Create endpoint
            let endpoint_payload =
                redis_endpoint_payload("redis_ep_concurrent", &redis_host, redis_port, "Redis endpoint for concurrent patch test");
            let (ep_status, ep_data) = post_authenticated(&client, api_url(server_port, "/endpoints"), admin_token, endpoint_payload)
                .await
                .expect("Failed to create endpoint");
            assert!(ep_status.is_success());
            let endpoint_uuid = ep_data["uuid"].as_str().expect("Missing endpoint uuid").to_string();

            let initial_port = crate::util::find_available_interlay_port().expect("Failed to find initial concurrent patch port");
            let port_a = crate::util::find_available_interlay_port().expect("Failed to find concurrent patch port A");
            let port_b = crate::util::find_available_interlay_port().expect("Failed to find concurrent patch port B");

            // Create interlay on initial_port
            let create_payload = json!({
                "id": "concurrent_interlay",
                "endpoint": endpoint_uuid,
                "port": initial_port,
                "tls": null,
                "settings": {},
            });
            let (create_status, create_data) = post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, create_payload)
                .await
                .expect("Failed to create interlay");
            assert!(create_status.is_success(), "Failed to create interlay");
            assert_interlay_running_state(&create_data, true, "after creation");

            // Fire two PATCHes concurrently — each changes to a different port.
            // We use raw reqwest calls inside spawned tasks to satisfy Send bounds.
            let admin_token = Arc::new(admin_token.to_string());
            let url = api_url(server_port, "/interlays/concurrent_interlay");

            let client_a = client.clone();
            let token_a = Arc::clone(&admin_token);
            let url_a = url.clone();
            let patch_a = tokio::spawn(async move {
                client_a
                    .patch(&url_a)
                    .header("Authorization", format!("Bearer {}", token_a))
                    .header("Content-Type", "application/json")
                    .json(&json!({ "port": port_a }))
                    .send()
                    .await
                    .expect("PATCH A send failed")
            });

            let client_b = client.clone();
            let token_b = Arc::clone(&admin_token);
            let url_b = url.clone();
            let patch_b = tokio::spawn(async move {
                client_b
                    .patch(&url_b)
                    .header("Authorization", format!("Bearer {}", token_b))
                    .header("Content-Type", "application/json")
                    .json(&json!({ "port": port_b }))
                    .send()
                    .await
                    .expect("PATCH B send failed")
            });

            let (result_a, result_b) = tokio::join!(patch_a, patch_b);

            let resp_a = result_a.expect("task A panicked");
            let resp_b = result_b.expect("task B panicked");

            // Both PATCHes should succeed (serialized, no 500 from racing restarts)
            let status_a = resp_a.status();
            let status_b = resp_b.status();
            assert!(status_a.is_success(), "PATCH A should succeed, got {}", status_a);
            assert!(status_b.is_success(), "PATCH B should succeed, got {}", status_b);

            // Read the response bodies to find which port each PATCH set
            let body_a: serde_json::Value = resp_a.json().await.expect("PATCH A body");
            let body_b: serde_json::Value = resp_b.json().await.expect("PATCH B body");
            let port_from_a = body_a["port"].as_u64().expect("missing port in PATCH A response");
            let port_from_b = body_b["port"].as_u64().expect("missing port in PATCH B response");

            // The second-to-complete PATCH determines the final port.
            // Both responses should report running.
            assert!(body_a["running"].as_bool().unwrap_or(false), "PATCH A should report running");
            assert!(body_b["running"].as_bool().unwrap_or(false), "PATCH B should report running");

            // Regardless of ordering, both ports must be one of the two targets
            assert!(
                port_from_a == port_a as u64 || port_from_a == port_b as u64,
                "PATCH A port should be {} or {}, got {}",
                port_a,
                port_b,
                port_from_a
            );
            assert!(
                port_from_b == port_a as u64 || port_from_b == port_b as u64,
                "PATCH B port should be {} or {}, got {}",
                port_a,
                port_b,
                port_from_b
            );

            // Give the OS a moment to release sockets from the intermediate port
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;

            // Exactly one of the two target ports should be listening (the last writer)
            let conn_a =
                tokio::time::timeout(std::time::Duration::from_secs(1), tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port_a)))
                    .await;
            let conn_b =
                tokio::time::timeout(std::time::Duration::from_secs(1), tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port_b)))
                    .await;

            let a_open = matches!(conn_a, Ok(Ok(_)));
            let b_open = matches!(conn_b, Ok(Ok(_)));

            // Exactly one port should be listening — not both (no duplicate listeners)
            // and not neither (interlay not dead)
            assert!(a_open || b_open, "At least one target port should accept connections (interlay must be alive)");
            assert!(!(a_open && b_open), "Only one target port should be listening (no duplicate listeners)");

            // The initial port should no longer accept connections
            let old_conn = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                tokio::net::TcpStream::connect(format!("127.0.0.1:{}", initial_port)),
            )
            .await;
            assert!(!matches!(old_conn, Ok(Ok(_))), "Initial port {} should no longer accept connections", initial_port);

            // Clean up
            delete_authenticated(&client, api_url(server_port, "/interlays/concurrent_interlay"), &admin_token)
                .await
                .expect("Failed to delete interlay");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

/// test for patching tls updates:
/// - `"tls": null` and `"tls": false` should explicitly clear TLS (restart path)
/// - omitting `tls` entirely should leave TLS unchanged (metadata-only path)
#[test]
fn test_patch_tls_tristate() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            match create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD).await {
                Ok(resp) => println!("Organization created successfully: {}", resp),
                Err(e) => {
                    eprintln!("Warning: Failed to create organization: {}", e);
                }
            }

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed to login as admin");
            let admin_token = &admin_jwt.token;
            let server_port = crate::util::TestConfig::get_port();
            let redis_conn = crate::util::TestConfig::get_redis_conn();
            let (redis_host, redis_port) = parse_redis_connection(&redis_conn).expect("Failed to parse Redis connection string");

            // Create a Redis endpoint
            let endpoint_payload =
                redis_endpoint_payload("redis_ep_tls_tristate", &redis_host, redis_port, "Redis endpoint for TLS tri-state test");
            let (ep_status, ep_data) = post_authenticated(&client, api_url(server_port, "/endpoints"), admin_token, endpoint_payload)
                .await
                .expect("Failed to create endpoint");
            assert!(ep_status.is_success());
            let endpoint_uuid = ep_data["uuid"].as_str().expect("Missing endpoint uuid").to_string();

            let interlay_port = crate::util::find_available_interlay_port().expect("Failed to find TLS tri-state port");

            // Create interlay without TLS
            let create_payload = json!({
                "id": "tls_tristate_interlay",
                "endpoint": endpoint_uuid,
                "port": interlay_port,
                "tls": null,
                "settings": {},
            });
            let (create_status, create_data) = post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, create_payload)
                .await
                .expect("Failed to create interlay");
            assert!(create_status.is_success(), "Failed to create interlay");
            assert_interlay_running_state(&create_data, true, "after creation");
            assert!(
                create_data.get("tls").is_none() || create_data["tls"].is_null(),
                "TLS should be absent after creation"
            );

            let interlay_url = api_url(server_port, "/interlays/tls_tristate_interlay");

            // 1) PATCH with tls field omitted — metadata-only path, no restart.
            let (patch_status, patch_data) =
                patch_authenticated(&client, interlay_url.clone(), admin_token, json!({})).await.expect("PATCH with tls omitted failed");
            assert!(patch_status.is_success(), "PATCH with tls omitted should succeed, got {}", patch_status);
            assert_interlay_running_state(&patch_data, true, "after PATCH with tls omitted");
            assert!(
                patch_data.get("tls").is_none() || patch_data["tls"].is_null(),
                "TLS should remain absent when field is omitted"
            );

            // 2) PATCH with "tls": null — should take the clear/restart path.
            //    Before the tri-state fix this was silently treated as "omitted".
            let (patch_status, patch_data) = patch_authenticated(&client, interlay_url.clone(), admin_token, json!({ "tls": null }))
                .await
                .expect("PATCH with tls:null failed");
            assert!(patch_status.is_success(), "PATCH with tls:null should succeed, got {}", patch_status);
            assert_interlay_running_state(&patch_data, true, "after PATCH with tls:null");
            assert!(
                patch_data.get("tls").is_none() || patch_data["tls"].is_null(),
                "TLS should be cleared after PATCH with tls:null"
            );

            // 3) PATCH with "tls": false — same semantics as null, should clear.
            let (patch_status, patch_data) = patch_authenticated(&client, interlay_url.clone(), admin_token, json!({ "tls": false }))
                .await
                .expect("PATCH with tls:false failed");
            assert!(patch_status.is_success(), "PATCH with tls:false should succeed, got {}", patch_status);
            assert_interlay_running_state(&patch_data, true, "after PATCH with tls:false");
            assert!(
                patch_data.get("tls").is_none() || patch_data["tls"].is_null(),
                "TLS should be cleared after PATCH with tls:false"
            );

            // Verify the interlay is still functional after all patches
            let conn = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                tokio::net::TcpStream::connect(format!("127.0.0.1:{}", interlay_port)),
            )
            .await;
            assert!(matches!(conn, Ok(Ok(_))), "Interlay should still accept connections after TLS patches");

            // Clean up
            delete_authenticated(&client, interlay_url, admin_token).await.expect("Failed to delete interlay");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}

/// Test that duplicate port assignments are rejected:
/// 1. Creating two interlays on the same port returns 409 Conflict.
/// 2. Patching an interlay to a port already used by another returns 409 Conflict.
#[test]
#[ignore = "covered by test_interlay_patch; redundant and flaky under ci startup timing"]
fn test_duplicate_port_rejected() {
    test_server(
        async || {
            let client = reqwest::Client::default();
            match create_org_with_superadmin(&client, Some(EDEN_NEW_ORG_TOKEN_VALUE), SUPERADMIN_ID, SUPERADMIN_PWD).await {
                Ok(resp) => println!("Organization created successfully: {}", resp),
                Err(e) => {
                    eprintln!("Warning: Failed to create organization: {}", e);
                }
            }

            let admin_jwt = auth_login(&client, SUPERADMIN_ID, SUPERADMIN_PWD).await.expect("Failed to login as admin");
            let admin_token = &admin_jwt.token;
            let server_port = crate::util::TestConfig::get_port();
            let redis_conn = crate::util::TestConfig::get_redis_conn();
            let (redis_host, redis_port) = parse_redis_connection(&redis_conn).expect("Failed to parse Redis connection string");

            // Create a Redis endpoint
            let endpoint_payload =
                redis_endpoint_payload("redis_ep_dup_port", &redis_host, redis_port, "Redis endpoint for duplicate port test");
            let (ep_status, ep_data) = post_authenticated(&client, api_url(server_port, "/endpoints"), admin_token, endpoint_payload)
                .await
                .expect("Failed to create endpoint");
            assert!(ep_status.is_success());
            let endpoint_uuid = ep_data["uuid"].as_str().expect("Missing endpoint uuid").to_string();

            let shared_port = crate::util::find_available_interlay_port().expect("Failed to find shared duplicate-test port");
            let other_port = crate::util::find_available_interlay_port().expect("Failed to find alternate duplicate-test port");

            // --- Case 1: POST two interlays on the same port ---
            let create_a = json!({
                "id": "dup_port_interlay_a",
                "endpoint": endpoint_uuid,
                "port": shared_port,
                "tls": null,
                "settings": {},
            });
            let (status_a, _) = post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, create_a)
                .await
                .expect("Failed to create interlay A");
            assert!(status_a.is_success(), "First interlay should succeed, got {}", status_a);

            let create_b = json!({
                "id": "dup_port_interlay_b",
                "endpoint": endpoint_uuid,
                "port": shared_port,
                "tls": null,
                "settings": {},
            });
            let (status_b, _) = post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, create_b)
                .await
                .expect("Failed to send create interlay B");
            assert_eq!(
                status_b,
                reqwest::StatusCode::CONFLICT,
                "Creating a second interlay on the same port should return 409 Conflict"
            );

            // --- Case 2: PATCH an interlay to collide with another's port ---
            // Create interlay B on a different port first
            let create_b_ok = json!({
                "id": "dup_port_interlay_b",
                "endpoint": endpoint_uuid,
                "port": other_port,
                "tls": null,
                "settings": {},
            });
            let (status_b_ok, _) = post_authenticated(&client, api_url(server_port, "/interlays"), admin_token, create_b_ok)
                .await
                .expect("Failed to create interlay B on different port");
            assert!(status_b_ok.is_success(), "Interlay B on different port should succeed, got {}", status_b_ok);

            // Now PATCH interlay B to use the same port as interlay A
            let (patch_status, _) = patch_authenticated(
                &client,
                api_url(server_port, "/interlays/dup_port_interlay_b"),
                admin_token,
                json!({ "port": shared_port }),
            )
            .await
            .expect("Failed to send PATCH");
            assert_eq!(
                patch_status,
                reqwest::StatusCode::CONFLICT,
                "Patching to an already-used port should return 409 Conflict"
            );

            // Verify interlay B is still running on its original port by doing
            // a no-op PATCH (returns CreateInterlayResponse which includes port).
            let (verify_status, verify_data) =
                patch_authenticated(&client, api_url(server_port, "/interlays/dup_port_interlay_b"), admin_token, json!({}))
                    .await
                    .expect("Failed to send verify PATCH");
            assert!(verify_status.is_success(), "No-op PATCH should succeed, got {}", verify_status);
            assert_eq!(verify_data["port"].as_u64(), Some(other_port as u64), "Interlay B port should be unchanged");
            assert_interlay_running_state(&verify_data, true, "interlay B after rejected PATCH");

            // Clean up
            delete_authenticated(&client, api_url(server_port, "/interlays/dup_port_interlay_a"), admin_token)
                .await
                .expect("Failed to delete interlay A");
            delete_authenticated(&client, api_url(server_port, "/interlays/dup_port_interlay_b"), admin_token)
                .await
                .expect("Failed to delete interlay B");
        },
        Some(EDEN_NEW_ORG_TOKEN_VALUE.to_string()),
    )
}
