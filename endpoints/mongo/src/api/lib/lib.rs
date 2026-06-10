// pub async fn handle_mongo_operation<F, Fut, C>(
//     name: String,
//     context: C,
//     database_op: F,
//     telemetry_wrapper: &mut TelemetryWrapper,
// ) -> Result<Box<dyn EpOutput>, EpError>
// where
//     F: FnOnce(&C) -> Fut,
//     Fut: Future<Output = Result<Box<dyn EpOutput>, EpError>>,
// {
//     let span_context = telemetry_wrapper
//         .client_tracer(format!("mongo.{}", name))
//         .await;
//
//     let t0 = SystemTime::now();
//
//     let output = database_op(&context).await.inspect_err(|e| {
//         span.set_status(Status::Error {
//             description: Cow::Owned(e.to_string()),
//         })
//     })?; // Now correctly matches the closure signature
//
//     engine_child_duration!(telemetry_wrapper, span, t0);
//
//     Ok(output)
// }
