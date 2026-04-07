use crate::eden_client::EdenClient;
use crate::metrics::AppMetrics;
use crate::queries;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;
use tracing::{debug, info, warn};

/// A registered Eden endpoint with its silo name and QPS allocation.
#[derive(Clone, Debug)]
pub struct EndpointDef {
    /// Silo name (used for query dispatch, e.g., "pg_network_security")
    pub silo_name: String,
    /// Eden endpoint ID (registered with Eden API)
    pub endpoint_id: String,
    /// Label for metrics (short, e.g., "pg_netsec")
    pub metrics_label: String,
    /// Target queries per second
    pub qps: u64,
}

/// All registered endpoints for this vertical.
#[derive(Clone)]
pub struct Endpoints {
    /// The vertical name (e.g., "tech", "retail")
    pub vertical: String,
    /// All database silos that should receive query traffic
    pub silos: Vec<EndpointDef>,
    /// Optional external service endpoints (no query traffic)
    pub tavily: Option<String>,
    pub llm: Option<String>,
    pub datadog: Option<String>,
    pub eraser: Option<String>,
}

impl Endpoints {
    /// Build a silo_name -> endpoint_id map for cross-db queries.
    pub fn endpoint_map(&self) -> HashMap<String, String> {
        self.silos
            .iter()
            .map(|e| (e.silo_name.clone(), e.endpoint_id.clone()))
            .collect()
    }
}

/// Continuously queries a single database silo at the target rate.
pub struct SingleDbWorker {
    eden: EdenClient,
    metrics: Arc<AppMetrics>,
    vertical: String,
    silo_name: String,
    metrics_label: String,
    endpoint_id: String,
    qps: u64,
}

impl SingleDbWorker {
    pub fn new(
        eden: EdenClient,
        metrics: Arc<AppMetrics>,
        vertical: &str,
        def: &EndpointDef,
    ) -> Self {
        Self {
            eden,
            metrics,
            vertical: vertical.to_string(),
            silo_name: def.silo_name.clone(),
            metrics_label: def.metrics_label.clone(),
            endpoint_id: def.endpoint_id.clone(),
            qps: def.qps,
        }
    }

    pub async fn run(self) {
        let interval = if self.qps > 0 {
            Duration::from_micros(1_000_000 / self.qps)
        } else {
            Duration::from_secs(5)
        };

        let mut ticker = time::interval(interval);
        let mut query_idx: usize = 0;

        info!(
            "[{}] Worker started — target {} QPS (interval {:?})",
            self.metrics_label, self.qps, interval
        );

        loop {
            ticker.tick().await;

            let query_list = queries::queries_for(&self.vertical, &self.silo_name);
            if query_list.is_empty() {
                continue;
            }

            let (desc, query) = &query_list[query_idx % query_list.len()];
            query_idx = query_idx.wrapping_add(1);

            let eden = self.eden.clone();
            let metrics = self.metrics.clone();
            let label = self.metrics_label.clone();
            let endpoint_id = self.endpoint_id.clone();
            let desc = desc.to_string();
            let query = query.clone();

            tokio::spawn(async move {
                let start = Instant::now();
                let result = eden.query(&endpoint_id, query).await;
                let elapsed = start.elapsed().as_secs_f64();

                metrics
                    .query_duration
                    .with_label_values(&[&label])
                    .observe(elapsed);

                match result {
                    Ok(resp) => {
                        metrics.queries_total.with_label_values(&[&label]).inc();
                        debug!("[{}] {} ({:.1}ms)", label, desc, elapsed * 1000.0);

                        if resp.get("status").and_then(|s| s.as_str()) == Some("error") {
                            metrics.query_errors.with_label_values(&[&label]).inc();
                            warn!(
                                "[{}] Query returned error: {} — {}",
                                label,
                                desc,
                                resp.get("message")
                                    .and_then(|m| m.as_str())
                                    .unwrap_or("unknown")
                            );
                        }
                    }
                    Err(e) => {
                        metrics.query_errors.with_label_values(&[&label]).inc();
                        warn!(
                            "[{}] Query failed: {} — {} ({:.1}ms)",
                            label,
                            desc,
                            e,
                            elapsed * 1000.0
                        );
                    }
                }
            });
        }
    }
}

/// Runs cross-database queries that fan out to multiple endpoint silos.
pub struct CrossDbWorker {
    eden: EdenClient,
    metrics: Arc<AppMetrics>,
    vertical: String,
    endpoint_map: HashMap<String, String>,
    interval_secs: u64,
}

impl CrossDbWorker {
    pub fn new(
        eden: EdenClient,
        metrics: Arc<AppMetrics>,
        endpoints: &Endpoints,
        interval_secs: u64,
    ) -> Self {
        Self {
            eden,
            metrics,
            vertical: endpoints.vertical.clone(),
            endpoint_map: endpoints.endpoint_map(),
            interval_secs,
        }
    }

    pub async fn run(self) {
        let mut ticker = time::interval(Duration::from_secs(self.interval_secs));
        let mut query_idx: usize = 0;

        info!(
            "[cross-db] Worker started — interval {}s, {} silos",
            self.interval_secs,
            self.endpoint_map.len()
        );

        loop {
            ticker.tick().await;

            let cross_queries = queries::cross_db_queries(&self.vertical);
            if cross_queries.is_empty() {
                continue;
            }
            let steps = &cross_queries[query_idx % cross_queries.len()];
            query_idx = query_idx.wrapping_add(1);

            let overall_start = Instant::now();
            let mut all_ok = true;

            for (silo_name, desc, query) in steps {
                let endpoint_id = match self.endpoint_map.get(*silo_name) {
                    Some(id) => id.as_str(),
                    None => {
                        warn!("[cross-db] Unknown silo: {}", silo_name);
                        continue;
                    }
                };

                let start = Instant::now();
                let result = self.eden.query(endpoint_id, query.clone()).await;
                let elapsed = start.elapsed().as_secs_f64();

                self.metrics
                    .query_duration
                    .with_label_values(&[silo_name])
                    .observe(elapsed);

                match result {
                    Ok(_) => {
                        self.metrics
                            .queries_total
                            .with_label_values(&[silo_name])
                            .inc();
                        debug!("  [{}] {} ({:.1}ms)", silo_name, desc, elapsed * 1000.0);
                    }
                    Err(e) => {
                        self.metrics
                            .query_errors
                            .with_label_values(&[silo_name])
                            .inc();
                        warn!(
                            "  [cross-db][{}] Error: {} ({:.1}ms)",
                            silo_name,
                            e,
                            elapsed * 1000.0
                        );
                        all_ok = false;
                    }
                }
            }

            let overall_elapsed = overall_start.elapsed().as_secs_f64();
            self.metrics
                .cross_db_query_duration
                .observe(overall_elapsed);
            self.metrics.cross_db_queries_total.inc();

            if all_ok {
                debug!(
                    "[cross-db] Completed ({:.1}ms total)",
                    overall_elapsed * 1000.0
                );
            } else {
                warn!(
                    "[cross-db] Completed with errors ({:.1}ms total)",
                    overall_elapsed * 1000.0
                );
            }
        }
    }
}

/// Periodically logs summary metrics to the console.
pub struct MetricsReporter {
    metrics: Arc<AppMetrics>,
    silo_labels: Vec<String>,
    interval_secs: u64,
}

impl MetricsReporter {
    pub fn new(metrics: Arc<AppMetrics>, endpoints: &Endpoints, interval_secs: u64) -> Self {
        let silo_labels = endpoints
            .silos
            .iter()
            .map(|e| e.metrics_label.clone())
            .collect();
        Self {
            metrics,
            silo_labels,
            interval_secs,
        }
    }

    pub async fn run(self) {
        let mut ticker = time::interval(Duration::from_secs(self.interval_secs));
        let mut prev_counts: HashMap<String, f64> = HashMap::new();
        let mut prev_time = Instant::now();

        loop {
            ticker.tick().await;
            let now = Instant::now();
            let dt = now.duration_since(prev_time).as_secs_f64();
            prev_time = now;

            let mut summary = String::from("\n╔══ ADAM Query Metrics ══╗\n");

            for label in &self.silo_labels {
                let total = self.metrics.queries_total.with_label_values(&[label]).get();
                let errors = self.metrics.query_errors.with_label_values(&[label]).get();

                let prev = prev_counts.get(label).copied().unwrap_or(0.0);
                let qps = if dt > 0.0 { (total - prev) / dt } else { 0.0 };
                prev_counts.insert(label.clone(), total);

                self.metrics
                    .queries_per_second
                    .with_label_values(&[label])
                    .set(qps);

                summary.push_str(&format!(
                    "║ {:>20} │ {:>8} queries │ {:>5} errors │ {:>6.1} QPS ║\n",
                    label, total as u64, errors as u64, qps
                ));
            }

            let cross = self.metrics.cross_db_queries_total.get();
            summary.push_str(&format!(
                "║ {:>20} │ {:>8} queries │                         ║\n",
                "cross-db", cross as u64
            ));
            summary.push_str("╚═══════════════════════╝");

            info!("{}", summary);
        }
    }
}
