//! Shared shard-capacity primitives for gateway protocol implementations.
//!
//! Protocol crates own their concrete connection types, but the policy for
//! distributing a process-wide endpoint connection budget across shard
//! runtimes should be the same for Redis, Postgres, Mongo, LLM, and agent
//! gateways.

use ep_core::runtime::{current_shard_id, shard_count_or};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardPoolBudgetConfig {
    total_env: &'static str,
    per_shard_env: &'static str,
    default_per_shard: usize,
}

impl ShardPoolBudgetConfig {
    pub const fn new(total_env: &'static str, per_shard_env: &'static str, default_per_shard: usize) -> Self {
        Self { total_env, per_shard_env, default_per_shard }
    }

    /// Total connection budget across all shard runtimes.
    ///
    /// The total env var is preferred. When unset, the legacy per-shard
    /// value is multiplied by shard count so existing deployments keep their
    /// effective backend connection budget until they opt into total-budget
    /// semantics.
    pub fn total_pool_size(self, shard_count: usize) -> usize {
        std::env::var(self.total_env)
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or_else(|| self.pool_size_per_shard().saturating_mul(shard_count.max(1)))
    }

    pub fn pool_size_per_shard(self) -> usize {
        std::env::var(self.per_shard_env)
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(self.default_per_shard)
    }

    /// Connection budget for the currently-running shard.
    ///
    /// Outside a shard runtime we treat the caller as a single-shard
    /// fallback so non-sharded startup and tests keep the full configured
    /// budget.
    pub fn pool_size_for_current_shard(self) -> usize {
        let (shard_id, shard_count) = match current_shard_id() {
            Some(shard_id) => (shard_id, shard_count_or(1)),
            None => (0, 1),
        };
        pool_size_for_shard(self.total_pool_size(shard_count), shard_count, shard_id)
    }
}

pub fn pool_size_for_shard(total: usize, shard_count: usize, shard_id: usize) -> usize {
    if total == 0 || shard_count == 0 || shard_id >= shard_count {
        return 0;
    }
    let base = total / shard_count;
    let remainder = total % shard_count;
    base + usize::from(shard_id < remainder)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardPoolLoadSnapshot {
    pub target_connections: usize,
    pub open_connections: usize,
    pub available_connections: usize,
    pub inflight_requests: usize,
    pub waiters: usize,
}

impl ShardPoolLoadSnapshot {
    pub fn has_available_connection(self) -> bool {
        self.available_connections > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_budget_assigns_one_connection_per_shard_when_even() {
        let assigned: Vec<usize> = (0..4).map(|shard| pool_size_for_shard(4, 4, shard)).collect();

        assert_eq!(assigned, vec![1, 1, 1, 1]);
    }

    #[test]
    fn pool_budget_leaves_extra_shards_without_connections() {
        let assigned: Vec<usize> = (0..4).map(|shard| pool_size_for_shard(2, 4, shard)).collect();

        assert_eq!(assigned, vec![1, 1, 0, 0]);
    }

    #[test]
    fn pool_budget_distributes_remainder_to_low_shards() {
        let assigned: Vec<usize> = (0..4).map(|shard| pool_size_for_shard(10, 4, shard)).collect();

        assert_eq!(assigned, vec![3, 3, 2, 2]);
    }
}
