//! Migration-aware routing snapshots shared by Redis processor connections.

use super::*;

pub(crate) struct RoutingRuntime;

impl RoutingRuntime {
    pub(in crate::processor) fn refresh_from_cache_if_changed(
        routing_state: &mut RoutingState,
        routing_state_version: &mut u64,
        interlay_cache_uuid: &InterlayCacheUuid,
        interlay_endpoints: &DashMap<InterlayCacheUuid, InterlayState>,
        org: Option<&eden_core::format::OrganizationCacheUuid>,
        _ctx: &LogContext,
    ) -> Result<bool, eden_core::error::EpError> {
        let Some(state) = interlay_endpoints.get(interlay_cache_uuid) else {
            return Ok(false);
        };

        let next_version = state.version();
        let routing_changed = state.routing() != routing_state.resolver.routing();

        if next_version != *routing_state_version || routing_changed {
            *routing_state = RoutingState::from_interlay_state(&state, org)?;
            *routing_state_version = next_version;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) fn refresh_snapshot_stales_after_cache_mutation(remove_after_refresh: bool, post_refresh_mutations: usize) -> bool {
        use eden_core::format::endpoint::EpKind;
        use ep_core::database::schema::routing::EndpointRouting;
        use ep_core::database::schema::routing::ShardEndpoint;
        use ep_core::database::schema::routing::{HashConfig, ShardingRule};

        let interlay_cache_uuid = InterlayCacheUuid::new(None, InterlayUuid::new_uuid());
        let initial_endpoint_uuid = eden_core::format::EndpointUuid::new_uuid();
        let refreshed_endpoint_uuid = eden_core::format::EndpointUuid::new_uuid();
        let initial_endpoint = EndpointCacheUuid::new(None, initial_endpoint_uuid.clone());
        let refreshed_endpoint = EndpointCacheUuid::new(None, refreshed_endpoint_uuid.clone());

        let initial_state = InterlayState::new(
            initial_endpoint.clone(),
            EpKind::Redis,
            EndpointRouting::direct(initial_endpoint_uuid),
            None,
            None,
            Default::default(),
        );
        let refreshed_state = InterlayState::new(
            refreshed_endpoint.clone(),
            EpKind::Redis,
            EndpointRouting::direct(refreshed_endpoint_uuid),
            None,
            None,
            Default::default(),
        );

        let interlay_endpoints = DashMap::new();
        interlay_endpoints.insert(interlay_cache_uuid.clone(), refreshed_state);

        let mut routing_state = match RoutingState::from_interlay_state(&initial_state, None) {
            Ok(state) => state,
            Err(_) => return false,
        };
        let mut routing_state_version = initial_state.version();
        let ctx = LogContext::default();

        if !Self::refresh_from_cache_if_changed(
            &mut routing_state,
            &mut routing_state_version,
            &interlay_cache_uuid,
            &interlay_endpoints,
            None,
            &ctx,
        )
        .ok()
        .unwrap_or(false)
        {
            return false;
        }

        if remove_after_refresh {
            interlay_endpoints.remove(&interlay_cache_uuid);
            let _ = Self::refresh_from_cache_if_changed(
                &mut routing_state,
                &mut routing_state_version,
                &interlay_cache_uuid,
                &interlay_endpoints,
                None,
                &ctx,
            );
            return false;
        }

        for _ in 0..post_refresh_mutations.max(1) {
            let stale_target = eden_core::format::EndpointUuid::new_uuid();
            let stale_state = InterlayState::new(
                EndpointCacheUuid::new(None, stale_target.clone()),
                EpKind::Redis,
                EndpointRouting::Sharded {
                    shards: vec![
                        ShardEndpoint { endpoint: stale_target.clone(), range: None },
                        ShardEndpoint {
                            endpoint: eden_core::format::EndpointUuid::new_uuid(),
                            range: None,
                        },
                    ],
                    rule: ShardingRule::Modulo { divisor: 2, hash: HashConfig::default() },
                },
                None,
                None,
                Default::default(),
            );
            interlay_endpoints.insert(interlay_cache_uuid.clone(), stale_state);
        }

        let _ = Self::refresh_from_cache_if_changed(
            &mut routing_state,
            &mut routing_state_version,
            &interlay_cache_uuid,
            &interlay_endpoints,
            None,
            &ctx,
        );

        interlay_endpoints
            .get(&interlay_cache_uuid)
            .is_some_and(|live_state| routing_state.resolver.routing() != live_state.routing())
    }
}

/// Current routing state for a connection.
/// Updated when migration state changes.
/// Delegates shard/replica selection to the generic `RoutingResolver` from ep-core.
#[derive(Clone)]
pub(super) struct RoutingState {
    /// Database-agnostic routing resolver for shard/replica endpoint selection.
    pub(super) resolver: RoutingResolver,
}

impl RoutingState {
    pub(super) fn from_interlay_state(
        state: &InterlayState,
        org: Option<&eden_core::format::OrganizationCacheUuid>,
    ) -> Result<Self, eden_core::error::EpError> {
        let resolver = RoutingResolver::new(state.routing(), org)?;

        Ok(Self { resolver })
    }
}
