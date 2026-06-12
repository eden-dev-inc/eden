#![cfg(feature = "integration")]

use std::time::{Duration, Instant};
use testcontainers_modules::testcontainers::core::IntoContainerPort;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tonic::transport::Channel;
use weaviate_wire::grpc::method::{self, WeaviateGrpcMethod, classify_grpc_method};

struct GrpcTestContext {
    container: ContainerAsync<GenericImage>,
    #[allow(dead_code)]
    rest_url: String,
    grpc_url: String,
}

impl GrpcTestContext {
    async fn stop(self) {
        let _ = self.container.stop().await;
    }
}

async fn wait_for_weaviate_ready(url: &str) {
    let client = reqwest::Client::new();
    let ready_url = format!("{}/v1/.well-known/ready", url);
    let t0 = Instant::now();

    for attempt in 0..60 {
        match client.get(&ready_url).timeout(Duration::from_secs(2)).send().await {
            Ok(resp) if resp.status().is_success() => {
                println!("Weaviate ready: {} ms (attempt {})", t0.elapsed().as_millis(), attempt + 1);
                return;
            }
            _ => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    panic!("Weaviate failed to become ready after {} ms", t0.elapsed().as_millis());
}

async fn setup_with_grpc() -> GrpcTestContext {
    let container = GenericImage::new("semitechnologies/weaviate", "1.28.4")
        .with_env_var("AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED", "true")
        .with_env_var("PERSISTENCE_DATA_PATH", "/var/lib/weaviate")
        .with_mapped_port(0, 8080.tcp())
        .with_mapped_port(0, 50051.tcp())
        .start()
        .await
        .expect("Failed to start Weaviate container");

    let host = container.get_host().await.expect("Failed to get host");
    let rest_port = container.get_host_port_ipv4(8080).await.expect("Failed to get REST port");
    let grpc_port = container.get_host_port_ipv4(50051).await.expect("Failed to get gRPC port");

    let rest_url = format!("http://{}:{}", host, rest_port);
    let grpc_url = format!("http://{}:{}", host, grpc_port);

    wait_for_weaviate_ready(&rest_url).await;

    GrpcTestContext { container, rest_url, grpc_url }
}

#[tokio::test]
async fn grpc_port_accepts_connection() {
    let ctx = setup_with_grpc().await;

    // Verify we can establish a gRPC channel to the Weaviate gRPC port
    let channel = Channel::from_shared(ctx.grpc_url.clone())
        .expect("valid URI")
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(5))
        .connect()
        .await;

    assert!(channel.is_ok(), "should connect to Weaviate gRPC port: {:?}", channel.err());

    ctx.stop().await;
}

#[tokio::test]
async fn grpc_channel_ready() {
    let ctx = setup_with_grpc().await;

    let channel = Channel::from_shared(ctx.grpc_url.clone())
        .expect("valid URI")
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(5))
        .connect()
        .await
        .expect("should connect");

    // Verify the channel is usable by checking readiness
    let mut client = tonic::client::Grpc::new(channel);
    let ready_result = client.ready().await;
    assert!(ready_result.is_ok(), "gRPC channel should be ready: {:?}", ready_result.err());

    ctx.stop().await;
}

#[tokio::test]
async fn wire_classification_covers_all_server_methods() {
    // Verify all known weaviate-wire gRPC method paths classify to known (non-Unknown) variants.
    // This ensures the wire protocol classifier stays in sync with known Weaviate gRPC methods.
    let all_known_paths = [
        (method::paths::SEARCH, WeaviateGrpcMethod::Search),
        (method::paths::AGGREGATE, WeaviateGrpcMethod::Aggregate),
        (method::paths::BATCH_OBJECTS, WeaviateGrpcMethod::BatchObjects),
        (method::paths::BATCH_REFERENCES, WeaviateGrpcMethod::BatchReferences),
        (method::paths::BATCH_DELETE, WeaviateGrpcMethod::BatchDelete),
        (method::paths::BATCH_STREAM, WeaviateGrpcMethod::BatchStream),
        (method::paths::TENANTS_GET, WeaviateGrpcMethod::TenantsGet),
    ];

    for (path, expected) in all_known_paths {
        let classified = classify_grpc_method(path);
        assert_eq!(classified, expected, "path {path} should classify to {expected:?}, got {classified:?}");
        assert!(!matches!(classified, WeaviateGrpcMethod::Unknown(_)), "path {path} should NOT classify as Unknown");
    }
}
