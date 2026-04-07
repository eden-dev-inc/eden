// Shared Workers
//
// Contains OrgIdCache and SystemMonitorWorker which are used by both
// Redis and PostgreSQL backends.

use anyhow::Result;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;
use uuid::Uuid;

use crate::postgres::Database;
use crate::runtime_controls::RuntimeControls;
use crate::telemetry::{SystemSnapshot, TelemetryRuntime};

/// Shared cache of organization IDs - initialized synthetically or from DB
pub struct OrgIdCache {
    org_ids: RwLock<Vec<Uuid>>,
    user_ids_by_org: RwLock<std::collections::HashMap<Uuid, Vec<Uuid>>>,
}

impl OrgIdCache {
    pub fn new() -> Self {
        Self {
            org_ids: RwLock::new(Vec::new()),
            user_ids_by_org: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Initialize with synthetic org and user IDs (no DB needed)
    pub async fn initialize_synthetic(&self, num_orgs: u32, users_per_org: u32) {
        let mut org_ids = Vec::with_capacity(num_orgs as usize);
        let mut user_map = std::collections::HashMap::new();

        for _ in 0..num_orgs {
            let org_id = Uuid::new_v4();
            org_ids.push(org_id);

            // Generate synthetic user IDs for this org (cap at 100 for memory)
            let user_ids: Vec<Uuid> = (0..users_per_org.min(100))
                .map(|_| Uuid::new_v4())
                .collect();
            user_map.insert(org_id, user_ids);
        }

        *self.org_ids.write().await = org_ids;
        *self.user_ids_by_org.write().await = user_map;

        info!(
            "Initialized synthetic cache with {} orgs, ~{} users each",
            num_orgs,
            users_per_org.min(100)
        );
    }

    /// Initialize from PostgreSQL database (uses real org/user IDs that exist in DB)
    pub async fn initialize_from_db(
        &self,
        db: &Database,
        num_orgs: u32,
        users_per_org: u32,
    ) -> anyhow::Result<()> {
        let org_ids = db.get_all_organization_ids(num_orgs).await?;
        let mut user_map = std::collections::HashMap::new();

        for &org_id in &org_ids {
            let user_ids = db.get_user_ids_for_org(org_id, users_per_org).await?;
            user_map.insert(org_id, user_ids);
        }

        let org_count = org_ids.len();
        *self.org_ids.write().await = org_ids;
        *self.user_ids_by_org.write().await = user_map;

        info!(
            "Initialized org cache from PostgreSQL with {} orgs, up to {} users each",
            org_count, users_per_org
        );
        Ok(())
    }

    pub async fn get_random_org_id(&self) -> Option<Uuid> {
        let org_ids = self.org_ids.read().await;
        if org_ids.is_empty() {
            return None;
        }
        let mut rng = StdRng::from_entropy();
        Some(org_ids[rng.gen_range(0..org_ids.len())])
    }

    pub async fn get_org_ids(&self) -> Vec<Uuid> {
        self.org_ids.read().await.clone()
    }

    pub async fn get_user_ids(&self, org_id: Uuid) -> Vec<Uuid> {
        let map = self.user_ids_by_org.read().await;
        map.get(&org_id).cloned().unwrap_or_default()
    }
}

impl Default for OrgIdCache {
    fn default() -> Self {
        Self::new()
    }
}

/// SystemMonitorWorker - Updates system metrics (shared by both backends)
pub struct SystemMonitorWorker {
    telemetry: Arc<TelemetryRuntime>,
    org_cache: Arc<OrgIdCache>,
    controls: Arc<RuntimeControls>,
    db: Option<Arc<Database>>,
}

impl SystemMonitorWorker {
    pub fn new(
        telemetry: Arc<TelemetryRuntime>,
        org_cache: Arc<OrgIdCache>,
        controls: Arc<RuntimeControls>,
        db: Option<Arc<Database>>,
    ) -> Self {
        Self {
            telemetry,
            org_cache,
            controls,
            db,
        }
    }

    pub async fn update_system_metrics(&self) -> Result<()> {
        let db_connections = self
            .db
            .as_ref()
            .map(|db| db.pool().size() as i64)
            .unwrap_or(0);
        self.telemetry
            .metrics()
            .set_db_connections_active(db_connections);
        let controls = self.controls.snapshot();

        let org_count = self.org_cache.get_org_ids().await.len() as i64;
        self.telemetry.metrics().set_active_organizations(org_count);
        self.telemetry
            .metrics()
            .set_events_per_second_current(controls.events_per_second as i64);
        self.telemetry.metrics().update_business_kpi(
            "configured_queries_per_second",
            controls.queries_per_second as f64,
        );
        self.telemetry.metrics().update_business_kpi(
            "configured_events_per_second",
            controls.events_per_second as f64,
        );

        let (count, avg_us, min_us, max_us, p50_us, p95_us, p99_us) =
            self.telemetry.metrics().take_latency_snapshot();

        if count > 0 {
            info!(
                "Live latency: {} reqs | avg: {:.1}µs | p50: {:.1}µs | p95: {:.1}µs | p99: {:.1}µs | min: {:.1}µs | max: {:.1}µs",
                count, avg_us, p50_us, p95_us, p99_us, min_us, max_us
            );
        }

        let total_queries = self.telemetry.metrics().queries_executed_total.get();
        let cache_hits = self.telemetry.metrics().cache_hits_total.get();
        let cache_misses = self.telemetry.metrics().cache_misses_total.get();
        let total_cache = cache_hits + cache_misses;
        let cache_hit_ratio = if total_cache > 0 {
            cache_hits as f64 / total_cache as f64
        } else {
            0.0
        };

        self.telemetry
            .metrics()
            .update_business_kpi("cache_hit_ratio", cache_hit_ratio * 100.0);

        self.telemetry.emit_system_snapshot(&SystemSnapshot {
            active_organizations: org_count,
            events_per_second: controls.events_per_second as i64,
            queries_per_second: controls.queries_per_second as i64,
            cache_hit_ratio,
            query_count: total_queries,
            avg_latency_us: avg_us,
            p50_latency_us: p50_us,
            p95_latency_us: p95_us,
            p99_latency_us: p99_us,
        });

        Ok(())
    }
}
