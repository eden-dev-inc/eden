use aws_config::BehaviorVersion;
use aws_sdk_lambda::Client as AwsLambdaClient;
use aws_sdk_lambda::config::{Credentials, Region};
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{FunctionCode, InvocationType, Runtime};
use endpoint_test_utils::telemetry_test_utils::test_telemetry;
use endpoint_types::{EP, EpRequest, RequestConstructor};
use ep_core::settings::EdenSettings;
use ep_function::api::lib::InvokeInputBuilder;
use ep_function::ep::FunctionEp;
use ep_function::request::FunctionRequest;
use format::cache_uuid::EndpointCacheUuid;
use format::{CacheUuid, EndpointUuid, OrganizationCacheUuid, OrganizationUuid};
use function_core::FunctionInvocationType;
use function_core::config::FunctionConfig;
use function_core::connection::{FunctionCredentials, FunctionProvider, FunctionTarget};
use testcontainers_modules::testcontainers::core::{IntoContainerPort, Mount};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

const LAMBDA_ZIP_BASE64: &str = "UEsDBBQAAAAIALFaUVygoXLbSQAAAEoAAAAIABwAaW5kZXgucHlVVAkAAy1BlGktQZRpdXgLAAEE6AMAAAToAwAAS0lNU8hIzEvJSS3SSC1LzSvRUUjOzytJrSjRtOJSAIKi1JLSojyFaqXiksSS0mLn/JRUJSsFIwMDHQWlpPyUSiBHKT9bqZYLAFBLAQIeAxQAAAAIALFaUVygoXLbSQAAAEoAAAAIABgAAAAAAAEAAAC0gQAAAABpbmRleC5weVVUBQADLUGUaXV4CwABBOgDAAAE6AMAAFBLBQYAAAAAAQABAE4AAACLAAAAAAA=";

struct TestContext {
    container: ContainerAsync<GenericImage>,
    endpoint_uuid: EndpointCacheUuid,
    ep: FunctionEp,
    telemetry: telemetry::TelemetryWrapper,
}

impl TestContext {
    async fn stop(self) {
        let _ = self.container.stop().await;
    }

    async fn write(&mut self, request: FunctionRequest) -> Result<serde_json::Value, error::EpError> {
        let request = Box::new(request) as Box<dyn EpRequest>;
        self.ep.write(&self.endpoint_uuid, &*request, EdenSettings::default(), &mut self.telemetry).await
    }
}

fn lambda_zip() -> Vec<u8> {
    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, LAMBDA_ZIP_BASE64).expect("embedded lambda zip must be valid base64")
}

async fn localstack_lambda_client(endpoint_url: &str) -> AwsLambdaClient {
    let shared_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("us-east-1".to_string()))
        .credentials_provider(Credentials::new("test", "test", None, None, "eden-function-test"))
        .load()
        .await;

    let config = aws_sdk_lambda::config::Builder::from(&shared_config).endpoint_url(endpoint_url).build();

    AwsLambdaClient::from_conf(config)
}

async fn wait_for_localstack_lambda(client: &AwsLambdaClient) {
    for _ in 0..40 {
        if client.list_functions().max_items(1).send().await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    panic!("localstack lambda service did not become ready in time");
}

async fn create_function(client: &AwsLambdaClient, function_name: &str) {
    client
        .create_function()
        .function_name(function_name)
        .runtime(Runtime::from("python3.11"))
        .role("arn:aws:iam::000000000000:role/lambda-role")
        .handler("index.handler")
        .code(FunctionCode::builder().zip_file(Blob::new(lambda_zip())).build())
        .send()
        .await
        .expect("failed to create lambda function in localstack");
}

async fn wait_for_function_ready(client: &AwsLambdaClient, function_name: &str) {
    let mut last_error = String::new();
    for _ in 0..80 {
        let result = client.invoke().function_name(function_name).invocation_type(InvocationType::DryRun).send().await;

        if result.is_ok() {
            return;
        }

        if let Err(err) = result {
            last_error = format!("{err:?}");
        }

        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    panic!("localstack lambda function `{function_name}` did not become ready in time: {last_error}");
}

async fn setup() -> TestContext {
    let mut telemetry = test_telemetry();

    let mut container_request = GenericImage::new("localstack/localstack", "3.8")
        .with_env_var("SERVICES", "lambda,iam")
        .with_env_var("AWS_DEFAULT_REGION", "us-east-1")
        .with_mapped_port(0, 4566.tcp());

    if std::path::Path::new("/var/run/docker.sock").exists() {
        container_request = container_request.with_mount(Mount::bind_mount("/var/run/docker.sock", "/var/run/docker.sock"));
    } else {
        container_request = container_request.with_env_var("LAMBDA_EXECUTOR", "local");
    }

    let container = container_request.start().await.expect("failed to start localstack container");

    let host = container.get_host().await.expect("failed to get localstack host");
    let port = container.get_host_port_ipv4(4566).await.expect("failed to get localstack lambda port");
    let endpoint_url = format!("http://{}:{}", host, port);

    let lambda_client = localstack_lambda_client(&endpoint_url).await;
    wait_for_localstack_lambda(&lambda_client).await;

    let function_name = format!(
        "eden-test-function-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos()
    );

    create_function(&lambda_client, &function_name).await;
    wait_for_function_ready(&lambda_client, &function_name).await;

    let target = FunctionTarget {
        provider: FunctionProvider::AwsLambda,
        region: "us-east-1".to_string(),
        endpoint_url: Some(endpoint_url.clone()),
        default_function_name: Some(function_name),
    };

    let credentials = FunctionCredentials {
        access_key_id: Some("test".to_string()),
        secret_access_key: Some("test".to_string()),
        session_token: None,
    };

    let config = Box::new(FunctionConfig {
        target,
        read_credentials: Some(credentials.clone()),
        write_credentials: Some(credentials),
        ..Default::default()
    });

    let endpoint_uuid =
        EndpointCacheUuid::new(Some(OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid())), EndpointUuid::new_uuid());

    let mut ep = FunctionEp::new();
    ep.connect_async(&endpoint_uuid, config, &mut telemetry)
        .await
        .expect("failed to connect function endpoint to localstack lambda");

    TestContext { container, endpoint_uuid, ep, telemetry }
}

#[tokio::test]
async fn invoke_dry_run_returns_normalized_response() {
    let mut ctx = setup().await;

    ctx.ep
        .health_check(&ctx.endpoint_uuid, &mut ctx.telemetry)
        .await
        .expect("function endpoint health check should succeed");

    let invoke = InvokeInputBuilder::default().invocation_type(FunctionInvocationType::DryRun).build().expect("build invoke request");

    let response = ctx.write(FunctionRequest::new(Box::new(invoke))).await.expect("dry run invoke should succeed");

    assert_eq!(response["kind"], "function");

    let status_code = response.get("status_code").and_then(serde_json::Value::as_i64).expect("status_code should be present and numeric");

    assert!(
        status_code == 200 || status_code == 202 || status_code == 204,
        "unexpected dry-run status code: {status_code}"
    );

    assert!(
        response.get("function_error").map(|value| value.is_null()).unwrap_or(true),
        "dry-run response should not contain function error: {response}"
    );

    assert_eq!(response["payload"]["format"], "empty");

    ctx.stop().await;
}

#[tokio::test]
async fn invoke_missing_function_returns_error() {
    let mut ctx = setup().await;

    let invoke = InvokeInputBuilder::default()
        .function_name("missing-function-name".to_string())
        .invocation_type(FunctionInvocationType::DryRun)
        .build()
        .expect("build invoke request");

    let error = ctx.write(FunctionRequest::new(Box::new(invoke))).await.expect_err("invoking a missing lambda should fail");

    let message = error.to_string();
    assert!(message.contains("failed to invoke AWS Lambda function"), "unexpected error: {message}");
    assert!(
        message.contains("missing-function-name"),
        "error should include function name for observability: {message}"
    );

    ctx.stop().await;
}

#[tokio::test]
async fn invoke_without_explicit_function_uses_config_default() {
    let mut ctx = setup().await;

    let invoke = InvokeInputBuilder::default().invocation_type(FunctionInvocationType::DryRun).build().expect("build invoke request");

    let response = ctx.write(FunctionRequest::new(Box::new(invoke))).await.expect("dry-run invoke with default function should succeed");

    let returned_endpoint = response.get("request_id").expect("response should include request_id field for traceability");
    assert!(returned_endpoint.is_string() || returned_endpoint.is_null(), "request_id should be string or null");

    ctx.stop().await;
}
