#![cfg_attr(test, allow(clippy::unwrap_used))]
//
// #[macro_export]
// macro_rules! endoint_process {
//     ($pool:expr, $func:ident($($arg:expr),*), $span:expr, $telemetry:expr) => {{
//
//         let lock = $pool.get().await.map_err(EpError::request)?;
//         let mut conn = lock.lock().await;
//
//         let timer = Instant::now();
//
//         let result = conn.0.$func($($arg),*).await.inspect_err(|e| {
//             $span.set_status(Status::Error {
//                 description: Cow::Owned(e.to_string()),
//             })
//         })?;
//
//         // $telemetry
//         //     .add_sub_process(timer.elapsed().as_micros())
//         //     .await;
//
//         $telemetry.mut_metrics(|metrics| metrics.endpoint_metrics.finish_request(&t0.elapsed(), &[]));
//
//         Ok(result.into_inner())
//     }};
// }

#[macro_export]
macro_rules! engine_process_duration {
    ($telemetry:expr, $t0:expr) => {
        $telemetry.add_process($t0.elapsed().as_micros()).await
    };
}

#[macro_export]
macro_rules! engine_sub_process {
    ($telemetry:expr, $span:expr, $t0:expr) => {
        $telemetry.add_sub_process($t0.elapsed().as_micros()).await
    };
}

#[macro_export]
macro_rules! execute_with_timeout {
    ($span:expr, $telemetry:expr, $settings:expr, $ep:expr, $($ep_fn:tt)*) => {{
        $span.add_simple_event("processing sync operation");

        // let t0 = tokio::time::Instant::now();

        let result = if $settings.max_attempts() > 0 {
            let mut counter = 0;

            loop {
                let attempt_result = {
                    let timeout_duration = $settings.max_timeout();

                    match tokio::time::timeout(
                        tokio::time::Duration::from_millis(timeout_duration),
                        $ep.$($ep_fn)*
                    ).await {
                        Ok(result) => result,
                        Err(_) => {
                            let error = format!("Operation timed out after {} ms", timeout_duration);
                            $span.set_status(FastSpanStatus::Error { message: std::borrow::Cow::Owned(error.clone()),
                            });
                            Err(EpError::timeout(error))
                        }
                    }
                };

                match attempt_result {
                    Ok(result) => break Ok(result),
                    Err(e) => match e {
                        EpError::Timeout(_) => break Err(e),
                        _ if counter >= $settings.max_attempts() - 1 => break Err(e),
                        _ => {
                            counter += 1;
                            // Convert labels() KeyValue to FastSpanAttribute
                            let mut attrs: Vec<eden_core::telemetry::FastSpanAttribute> = $settings.labels()
                                .into_iter()
                                .map(|kv| eden_core::telemetry::FastSpanAttribute::new(kv.key.to_string(), kv.value.to_string()))
                                .collect();
                            attrs.push(eden_core::telemetry::FastSpanAttribute::new("attempts", (counter+1).to_string()));
                            $span.add_event(
                                "retrying operation",
                                attrs
                            );
                            if $settings.retry_delay() > 0 {
                                tokio::time::sleep(Duration::from_millis($settings.retry_delay())).await;
                            }
                            continue;
                        }
                    },
                }
            }
        } else {
            let timeout_duration = $settings.max_timeout();

            match tokio::time::timeout(
                tokio::time::Duration::from_millis(timeout_duration),
                $ep.$($ep_fn)*
            ).await {
                Ok(result) => result,
                Err(_) => {
                    let error = format!("Operation timed out after {} ms", timeout_duration);
                    $span.set_status(FastSpanStatus::Error { message: std::borrow::Cow::Owned(error.clone()),
                    });
                    Err(EpError::timeout(error))
                }
            }
        };

        // $telemetry
        //     .add_child_process(t0.elapsed().as_micros())
        //     .await;

        result
    }}
}
