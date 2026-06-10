#[cfg(feature = "function-aws-integration")]
mod integration {
    use function_core::comm::FunctionClient;
    use function_core::comm::FunctionInvokeRequest;
    use function_core::connection::{FunctionConnection, FunctionProvider};

    // To run:
    // cargo test -p ep-function --features function-aws-integration --test aws_integration -- --ignored
    // Requires AWS credentials + region in environment and a reachable Lambda function.
    #[tokio::test]
    #[ignore = "requires real AWS credentials and Lambda invoke permissions"]
    async fn invoke_smoke() {
        let default_function_name =
            std::env::var("EDEN_FUNCTION_SMOKE_NAME").expect("set EDEN_FUNCTION_SMOKE_NAME to an existing lambda function name");

        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        let client = FunctionClient::new(&FunctionConnection {
            provider: FunctionProvider::AwsLambda,
            region,
            endpoint_url: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            default_function_name: Some(default_function_name),
        })
        .await
        .expect("load AWS SDK config from environment");

        let _ = client
            .invoke(&FunctionInvokeRequest {
                function_name: None,
                payload: None,
                qualifier: None,
                client_context_base64: None,
                invocation_type: Some(function_core::FunctionInvocationType::DryRun),
                log_type: None,
            })
            .await
            .expect("invoke lambda dry-run");
    }
}
