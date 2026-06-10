//! Redis Cluster virtualized processor loop.

use super::execution::ClusterExecution;
use super::response::ClusterResponseRewriter;
use super::topology::ClusterTopologyLoader;
use super::*;

pub(in crate::processor) struct ClusterProcessor;
impl ClusterProcessor {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::processor) async fn process_virtualized(
        mut receiver: UnboundedReceiver<Bytes>,
        database_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        sender: BytesQueueSender,
        _settings: EdenSettings,
        interlay_cache_uuid: InterlayCacheUuid,
        interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        mut telemetry_wrapper: TelemetryWrapper,
        ctx: LogContext,
        _client_addr: std::net::SocketAddr,
        listener_id: String,
    ) {
        let Some(initial_state) = interlay_endpoints.get(&interlay_cache_uuid).map(|state| state.clone()) else {
            let _ = sender.send(RedisWire::format_resp_error_line("interlay not found"));
            return;
        };

        let interlay_id = interlay_cache_uuid.uuid().to_string();
        let org = initial_state.endpoint_uuid().org();
        let mut signal_rx = initial_state.subscribe_signals();
        let mut cluster_state = match ClusterTopologyLoader::build_connection_state(
            &database_manager,
            &initial_state,
            &listener_id,
            org.as_ref(),
            &mut telemetry_wrapper,
            Some(interlay_id.as_str()),
            Some(&ctx),
        )
        .await
        {
            Ok(state) => state,
            Err(err) => {
                log_error!(
                    ctx.clone(),
                    "Failed to initialize Redis cluster proxy connection",
                    audience = LogAudience::Internal,
                    error = err.to_string(),
                    listener_id = listener_id.clone()
                );
                let _ = sender.send(RedisWire::format_resp_error_line(&err.to_string()));
                return;
            }
        };

        let mut buffer = BytesMut::with_capacity(16 * 1024);

        loop {
            let data = if let Some(ref mut rx) = signal_rx {
                tokio::select! {
                    data = receiver.recv() => data,
                    signal = rx.recv() => {
                        match signal {
                            Ok(InterlaySignal::Shutdown) => return,
                            Ok(InterlaySignal::MirrorUpdate) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                if ClusterExecution::has_active_pin(&cluster_state) {
                                    cluster_state.pending_refresh = true;
                                    continue;
                                }
                                if let Err(err) = ClusterTopologyLoader::refresh_if_needed(
                                    &mut cluster_state,
                                    &database_manager,
                                    &interlay_cache_uuid,
                                    &interlay_endpoints,
                                    &listener_id,
                                    org.as_ref(),
                                    &mut telemetry_wrapper,
                                    false,
                                    Some(&ctx),
                                )
                                .await
                                {
                                    let _ = sender.send(RedisWire::format_resp_error_line(&err.to_string()));
                                    return;
                                }
                                continue;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => receiver.recv().await,
                        }
                    }
                }
            } else {
                receiver.recv().await
            };

            let Some(data) = data else {
                return;
            };

            if let Err(err) = RedisWire::append_bounded(&mut buffer, &data, MAX_REQUEST_BUFFER_BYTES) {
                let _ = sender.send(RedisWire::format_resp_error_line(&err.to_string()));
                return;
            }

            loop {
                if cluster_state.pending_refresh
                    && !ClusterExecution::has_active_pin(&cluster_state)
                    && let Err(err) = ClusterTopologyLoader::refresh_if_needed(
                        &mut cluster_state,
                        &database_manager,
                        &interlay_cache_uuid,
                        &interlay_endpoints,
                        &listener_id,
                        org.as_ref(),
                        &mut telemetry_wrapper,
                        true,
                        Some(&ctx),
                    )
                    .await
                {
                    let _ = sender.send(RedisWire::format_resp_error_line(&err.to_string()));
                    return;
                }

                if let Err(err) = ClusterTopologyLoader::refresh_if_needed(
                    &mut cluster_state,
                    &database_manager,
                    &interlay_cache_uuid,
                    &interlay_endpoints,
                    &listener_id,
                    org.as_ref(),
                    &mut telemetry_wrapper,
                    false,
                    Some(&ctx),
                )
                .await
                {
                    let _ = sender.send(RedisWire::format_resp_error_line(&err.to_string()));
                    return;
                }

                let (parsed, consumed) = match RedisProtocol::parse_buffer(&buffer) {
                    Ok(Some(result)) => result,
                    Ok(None) => break,
                    Err(err) => {
                        let _ = sender.send(RedisWire::format_resp_error_line(&err.to_string()));
                        buffer.clear();
                        return;
                    }
                };

                let command_bytes = buffer.split_to(consumed).freeze();
                if let Some(local_response) = ClusterExecution::local_session_response(parsed.command(), &mut cluster_state) {
                    if sender.send(local_response).is_err() {
                        return;
                    }
                    continue;
                }

                if let Some(blocked_response) = RedisWire::session_state_rejection(parsed.command()) {
                    let _ = sender.send(blocked_response);
                    return;
                }

                let slot = match ClusterExecution::command_slot(&parsed) {
                    Ok(slot) => slot,
                    Err(response) => {
                        if sender.send(response).is_err() {
                            return;
                        }
                        continue;
                    }
                };

                let apply_asking = cluster_state.asking_next;
                let dispatch = match ClusterExecution::execute_dispatched_command(
                    &mut cluster_state,
                    &parsed,
                    command_bytes.clone(),
                    slot,
                    apply_asking,
                )
                .await
                {
                    Ok(dispatch) => dispatch,
                    Err(first_err) => {
                        if let Err(refresh_err) = ClusterTopologyLoader::refresh_if_needed(
                            &mut cluster_state,
                            &database_manager,
                            &interlay_cache_uuid,
                            &interlay_endpoints,
                            &listener_id,
                            org.as_ref(),
                            &mut telemetry_wrapper,
                            true,
                            Some(&ctx),
                        )
                        .await
                        {
                            cluster_state.asking_next = false;
                            let _ = sender.send(RedisWire::format_resp_error_line(&refresh_err.to_string()));
                            return;
                        }

                        match ClusterExecution::execute_dispatched_command(&mut cluster_state, &parsed, command_bytes, slot, apply_asking)
                            .await
                        {
                            Ok(dispatch) => dispatch,
                            Err(_) => {
                                cluster_state.asking_next = false;
                                let _ = sender.send(RedisWire::format_resp_error_line(&first_err.to_string()));
                                return;
                            }
                        }
                    }
                };
                cluster_state.asking_next = false;

                let raw_response = dispatch.response.clone();
                ClusterExecution::apply_session_state(parsed.command(), &raw_response, &mut cluster_state, &dispatch);
                let needs_topology_refresh =
                    matches!(parsed.command(), RedisApi::ClusterNodes | RedisApi::ClusterSlots | RedisApi::ClusterShards)
                        || ClusterResponseRewriter::redirect_line(&raw_response).is_some();

                if needs_topology_refresh
                    && let Err(err) = ClusterTopologyLoader::refresh_if_needed(
                        &mut cluster_state,
                        &database_manager,
                        &interlay_cache_uuid,
                        &interlay_endpoints,
                        &listener_id,
                        org.as_ref(),
                        &mut telemetry_wrapper,
                        true,
                        Some(&ctx),
                    )
                    .await
                {
                    let _ = sender.send(RedisWire::format_resp_error_line(&err.to_string()));
                    return;
                }

                let rewrite_topology = ClusterTopologyLoader::topology_for_endpoint(&cluster_state, &dispatch.topology.endpoint_uuid)
                    .cloned()
                    .unwrap_or_else(|| dispatch.topology.clone());
                let result = match parsed.command() {
                    RedisApi::ClusterNodes => ClusterResponseRewriter::nodes_response(&raw_response, &rewrite_topology),
                    RedisApi::ClusterSlots => ClusterResponseRewriter::slots_response(&raw_response, &rewrite_topology),
                    RedisApi::ClusterShards => ClusterResponseRewriter::shards_response(&raw_response, &rewrite_topology),
                    _ => Ok(ClusterResponseRewriter::redirect_response(raw_response, &rewrite_topology)),
                };

                match result {
                    Ok(response) => {
                        if sender.send(response).is_err() {
                            return;
                        }
                    }
                    Err(err) => {
                        let _ = sender.send(RedisWire::format_resp_error_line(&err.to_string()));
                        return;
                    }
                }
            }
        }
    }
}
