// PostgreSQL Workers
//
// High-throughput PostgreSQL query workers for load testing.
// Runs diverse SQL operations (SELECTs, INSERTs, UPDATEs, DELETEs,
// UPSERTs, JOINs, CTEs, window functions, JSONB ops) at maximum speed.

use anyhow::Result;
use fake::Fake;
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::generators::DataGenerator;
use crate::postgres::Database;
use crate::telemetry::TelemetryRuntime;
use crate::workers::OrgIdCache;

/// PgQuerySimulatorWorker - runs diverse PostgreSQL read queries at high throughput
pub struct PgQuerySimulatorWorker {
    db: Arc<Database>,
    telemetry: Arc<TelemetryRuntime>,
    generator: Arc<DataGenerator>,
    org_cache: Arc<OrgIdCache>,
}

impl PgQuerySimulatorWorker {
    pub fn new(
        db: Arc<Database>,
        telemetry: Arc<TelemetryRuntime>,
        generator: Arc<DataGenerator>,
        org_cache: Arc<OrgIdCache>,
    ) -> Self {
        Self {
            db,
            telemetry,
            generator,
            org_cache,
        }
    }

    /// Start a pool of PostgreSQL query workers
    pub async fn start_worker_pool(&self, num_workers: usize, shutdown: CancellationToken) {
        info!("Starting {} PostgreSQL query workers", num_workers);

        for worker_id in 0..num_workers {
            let db = self.db.clone();
            let telemetry = self.telemetry.clone();
            let generator = self.generator.clone();
            let org_cache = self.org_cache.clone();
            let worker_shutdown = shutdown.clone();

            tokio::spawn(async move {
                let worker = PgQuerySimulatorWorker {
                    db,
                    telemetry,
                    generator,
                    org_cache,
                };
                worker.run_worker(worker_id, worker_shutdown).await;
            });
        }
    }

    async fn run_worker(&self, worker_id: usize, shutdown: CancellationToken) {
        debug!("PostgreSQL query worker {} started", worker_id);

        loop {
            match self.org_cache.get_random_org_id().await {
                Some(org_id) => {
                    if let Err(e) = self.execute_diverse_query(org_id).await {
                        warn!("PG worker {} query error: {}", worker_id, e);
                    }
                }
                None => {
                    debug!("PG worker {} waiting for org cache", worker_id);
                    tokio::select! {
                        _ = sleep(Duration::from_millis(100)) => {}
                        _ = shutdown.cancelled() => break,
                    }
                }
            }

            if shutdown.is_cancelled() {
                break;
            }
        }
    }

    /// Execute a random PostgreSQL query from a diverse set of query types
    ///
    /// All 29 tables are covered by read queries. Distribution:
    ///   - Core analytics (events table): ~40%
    ///   - Analytics domain (sessions, campaigns, experiments, page_views, organizations, goals): ~16%
    ///   - E-commerce (products, orders, carts, reviews, inventory, coupons, goal_completions, product_tags, cart_items): ~19%
    ///   - Finance (subscriptions, invoices, payments, ledger, accounts, invoice_items, refunds, subscription_events): ~17%
    ///   - Cross-domain: ~3%
    ///   - Previously unused methods: ~5%
    async fn execute_diverse_query(&self, org_id: Uuid) -> Result<()> {
        let mut rng = StdRng::from_entropy();
        let query_type = rng.gen_range(0..100);

        let start = Instant::now();
        let result = match query_type {
            // ============================================================
            // Core analytics queries - events table (~40%)
            // ============================================================

            // 10% - Analytics overview (complex aggregate with FILTER)
            0..=9 => {
                let hours = [1, 6, 24, 168][rng.gen_range(0..4)];
                self.db
                    .get_analytics_overview(org_id, hours)
                    .await
                    .map(|_| ())
            }
            // 6% - Top pages (GROUP BY + ORDER BY + LIMIT)
            10..=15 => {
                let limit = rng.gen_range(5..20);
                self.db.get_top_pages(org_id, limit).await.map(|_| ())
            }
            // 4% - Hourly metrics (time-range aggregate with FILTER + JSONB extraction)
            16..=19 => {
                let hour_offset = rng.gen_range(0..24);
                self.db
                    .get_hourly_metrics(org_id, hour_offset)
                    .await
                    .map(|_| ())
            }
            // 4% - Event distribution (multi-column FILTER aggregate)
            20..=23 => self.db.get_event_distribution(org_id).await.map(|_| ()),
            // 3% - User activity (GROUP BY + JSONB extraction + aggregate)
            24..=26 => {
                let user_ids = self.org_cache.get_user_ids(org_id).await;
                if let Some(&user_id) = user_ids.first() {
                    self.db.get_user_activity(user_id).await.map(|_| ())
                } else {
                    Ok(())
                }
            }
            // 3% - Page performance (parameterized WHERE + aggregate)
            27..=29 => {
                let pages = self.generator.get_popular_pages();
                let page = pages[rng.gen_range(0..pages.len())];
                let page_url = format!("https://app.example.com{}", page);
                self.db
                    .get_page_performance(org_id, &page_url)
                    .await
                    .map(|_| ())
            }
            // 3% - Ranked pages (window function: RANK() OVER)
            30..=32 => self.db.get_ranked_pages(org_id).await.map(|_| ()),
            // 3% - Conversion funnel (CTE with CASE + GROUP BY + ORDER BY)
            33..=35 => self.db.get_conversion_funnel(org_id).await.map(|_| ()),
            // 2% - Active users with orgs (INNER JOIN + GROUP BY + ORDER BY)
            36..=37 => {
                let limit = rng.gen_range(5..20);
                self.db
                    .get_active_users_with_orgs(org_id, limit)
                    .await
                    .map(|_| ())
            }
            // 1% - Time-bucketed events (date_trunc + EXTRACT + aggregate)
            38 => {
                let bucket = [5, 15, 30, 60][rng.gen_range(0..4)];
                self.db
                    .get_time_bucketed_events(org_id, bucket)
                    .await
                    .map(|_| ())
            }
            // 1% - Revenue by plan (JSONB extraction + SUM + GROUP BY)
            39 => self.db.get_revenue_by_plan(org_id).await.map(|_| ()),

            // ============================================================
            // Analytics domain (~16%) - sessions, campaigns, experiments, page_views, organizations, goals
            // ============================================================

            // 3% - Session stats (sessions table)
            40..=42 => self.db.get_session_stats(org_id).await.map(|_| ()),
            // 3% - Campaign performance (campaigns table)
            43..=45 => self.db.get_campaign_performance(org_id).await.map(|_| ()),
            // 2% - Experiment results (experiments + assignments)
            46..=47 => self.db.get_experiment_results(org_id).await.map(|_| ()),
            // 2% - Page view metrics (page_views table)
            48..=49 => self.db.get_page_view_metrics(org_id).await.map(|_| ()),
            // 2% - Organization summary (organizations table - NEW)
            50..=51 => self.db.get_organization_summary(org_id).await.map(|_| ()),
            // 2% - Goal performance (goals table - NEW)
            52..=53 => self.db.get_goal_performance(org_id).await.map(|_| ()),
            // 2% - High activity orgs (organizations via subquery)
            54..=55 => self.db.get_high_activity_orgs(10).await.map(|_| ()),

            // ============================================================
            // E-commerce queries (~19%) - products, orders, carts, reviews, inventory, coupons,
            //                             goal_completions, product_tags, cart_items
            // ============================================================

            // 2% - Product catalog (products table)
            56..=57 => {
                let limit = rng.gen_range(10..50);
                self.db.get_product_catalog(org_id, limit).await.map(|_| ())
            }
            // 2% - Order summary (orders table)
            58..=59 => self.db.get_order_summary(org_id).await.map(|_| ()),
            // 2% - Top products by revenue (order_items table)
            60..=61 => {
                let limit = rng.gen_range(5..20);
                self.db
                    .get_top_products_by_revenue(org_id, limit)
                    .await
                    .map(|_| ())
            }
            // 2% - Cart abandonment rate (carts table)
            62..=63 => self.db.get_cart_abandonment_rate(org_id).await.map(|_| ()),
            // 2% - Product reviews summary (reviews table)
            64..=65 => self
                .db
                .get_product_reviews_summary(org_id)
                .await
                .map(|_| ()),
            // 2% - Inventory alerts (inventory table)
            66..=67 => self.db.get_inventory_alerts(org_id).await.map(|_| ()),
            // 2% - Coupon usage (coupons table)
            68..=69 => self.db.get_coupon_usage(org_id).await.map(|_| ()),
            // 2% - Goal completion funnel (goal_completions table - NEW)
            70..=71 => self.db.get_goal_completion_funnel(org_id).await.map(|_| ()),
            // 2% - Product tag distribution (product_tags table - NEW)
            72..=73 => self
                .db
                .get_product_tag_distribution(org_id)
                .await
                .map(|_| ()),
            // 1% - Cart item analysis (cart_items table - NEW)
            74 => self.db.get_cart_item_analysis(org_id).await.map(|_| ()),

            // ============================================================
            // Finance queries (~17%) - subscriptions, invoices, payments, ledger,
            //                          accounts, invoice_items, refunds, subscription_events
            // ============================================================

            // 2% - Subscription metrics (subscriptions table)
            75..=76 => self.db.get_subscription_metrics(org_id).await.map(|_| ()),
            // 2% - MRR by plan (subscription_plans table)
            77..=78 => self.db.get_mrr_by_plan(org_id).await.map(|_| ()),
            // 2% - Invoice aging (invoices table)
            79..=80 => self.db.get_invoice_aging(org_id).await.map(|_| ()),
            // 2% - Payment method distribution (payments table)
            81..=82 => self
                .db
                .get_payment_method_distribution(org_id)
                .await
                .map(|_| ()),
            // 1% - Revenue timeline (payments time-series)
            83 => self.db.get_revenue_timeline(org_id).await.map(|_| ()),
            // 1% - Churn analysis (subscriptions churn)
            84 => self.db.get_churn_analysis(org_id).await.map(|_| ()),
            // 2% - Ledger balance (ledger_entries table)
            85..=86 => {
                let account_codes = ["1000", "1100", "2000", "3000", "4000", "5000"];
                let code = account_codes[rng.gen_range(0..account_codes.len())];
                self.db.get_ledger_balance(org_id, code).await.map(|_| ())
            }
            // 2% - Chart of accounts (accounts table - NEW)
            87..=88 => self.db.get_chart_of_accounts(org_id).await.map(|_| ()),
            // 2% - Invoice line item breakdown (invoice_items table - NEW)
            89..=90 => self
                .db
                .get_invoice_line_item_breakdown(org_id)
                .await
                .map(|_| ()),
            // 1% - Refund summary (refunds table - NEW)
            91 => self.db.get_refund_summary(org_id).await.map(|_| ()),
            // 1% - Subscription event timeline (subscription_events table - NEW)
            92 => self
                .db
                .get_subscription_event_timeline(org_id)
                .await
                .map(|_| ()),

            // ============================================================
            // Cross-domain queries (~3%)
            // ============================================================

            // 2% - User lifetime value (users + orders + subscriptions)
            93..=94 => {
                let limit = rng.gen_range(5..20);
                self.db
                    .get_user_lifetime_value(org_id, limit)
                    .await
                    .map(|_| ())
            }
            // 1% - Product category revenue (categories + products + order_items)
            95 => self
                .db
                .get_product_category_revenue(org_id)
                .await
                .map(|_| ()),

            // ============================================================
            // Previously unused methods (~5%)
            // ============================================================

            // 2% - Referrer stats (events referrer aggregation)
            96..=97 => self.db.get_referrer_stats(org_id).await.map(|_| ()),
            // 2% - Count events by type (events simple aggregate)
            98..=99 => self.db.count_events_by_type(org_id).await.map(|_| ()),
            // unreachable but needed for exhaustive match
            _ => self.db.org_exists(org_id).await.map(|_| ()),
        };

        let latency_ns = start.elapsed().as_nanos() as u64;
        self.telemetry.metrics().record_live_latency_ns(latency_ns);

        match result {
            Ok(()) => {
                self.telemetry
                    .metrics()
                    .record_operation_success("pg_query");
                self.telemetry.metrics().queries_executed_total.inc();
            }
            Err(e) => {
                self.telemetry
                    .metrics()
                    .record_operation_error("pg_query", "execution_error");
                warn!("PG query error for org {}: {}", org_id, e);
            }
        }

        Ok(())
    }
}

/// PgEventWriterWorker - generates INSERT/UPDATE/DELETE/UPSERT traffic against PostgreSQL
pub struct PgEventWriterWorker {
    db: Arc<Database>,
    telemetry: Arc<TelemetryRuntime>,
    generator: Arc<DataGenerator>,
    org_cache: Arc<OrgIdCache>,
}

impl PgEventWriterWorker {
    pub fn new(
        db: Arc<Database>,
        telemetry: Arc<TelemetryRuntime>,
        generator: Arc<DataGenerator>,
        org_cache: Arc<OrgIdCache>,
    ) -> Self {
        Self {
            db,
            telemetry,
            generator,
            org_cache,
        }
    }

    /// Run a batch of diverse write operations across all tables
    ///
    /// Distribution:
    ///   - Event INSERTs: 38%
    ///   - Event UPDATEs: 15%
    ///   - Event UPSERTs: 7%
    ///   - Event DELETEs: 7%
    ///   - User updates: 3%
    ///   - Session INSERTs: 5%
    ///   - Page view INSERTs: 5%
    ///   - Order + order_items INSERTs: 3%
    ///   - Payment INSERTs: 3%
    ///   - Campaign counter UPDATEs: 2%
    ///   - Subscription event INSERTs: 2%
    ///   - Ledger entry INSERTs (debit+credit pair): 2%
    ///   - Remaining: 8% (distributed above rounds)
    pub async fn run_batch(&self, events_per_second: u64) -> Result<()> {
        let start = Instant::now();
        let org_ids = self.org_cache.get_org_ids().await;

        if org_ids.is_empty() {
            return Ok(());
        }

        let mut rng = StdRng::from_entropy();
        let mut insert_count = 0u64;
        let mut update_count = 0u64;
        let mut upsert_count = 0u64;
        let mut delete_count = 0u64;
        let mut new_table_write_count = 0u64;

        let total_ops = events_per_second;
        let pool = self.db.pool();

        // ============================================================
        // Event INSERTs (38%)
        // ============================================================
        let insert_target = (total_ops * 38 / 100) as usize;
        let mut batch_events = Vec::with_capacity(insert_target.min(500));
        for _ in 0..insert_target {
            let org_id = org_ids[rng.gen_range(0..org_ids.len())];
            let user_ids = self.org_cache.get_user_ids(org_id).await;
            let event = self.generator.generate_event(org_id, &user_ids);
            batch_events.push(event);

            // Flush in batches of 500
            if batch_events.len() >= 500 {
                match self.db.insert_events_batch(&batch_events).await {
                    Ok(n) => insert_count += n,
                    Err(e) => error!("PG batch insert error: {}", e),
                }
                batch_events.clear();
            }
        }
        // Flush remaining
        if !batch_events.is_empty() {
            match self.db.insert_events_batch(&batch_events).await {
                Ok(n) => insert_count += n,
                Err(e) => error!("PG batch insert error: {}", e),
            }
        }

        // ============================================================
        // Event UPDATEs (15%): update event properties with JSONB merge
        // ============================================================
        let update_target = (total_ops * 15 / 100) as usize;
        for _ in 0..update_target {
            let org_id = org_ids[rng.gen_range(0..org_ids.len())];
            let event_types = ["page_view", "click", "conversion", "sign_up", "purchase"];
            let event_type = event_types[rng.gen_range(0..event_types.len())];
            let props = json!({
                "updated_at": chrono::Utc::now().to_rfc3339(),
                "batch_id": rng.gen_range(1000..9999)
            });
            match self
                .db
                .update_event_properties(org_id, event_type, props)
                .await
            {
                Ok(n) => update_count += n,
                Err(e) => warn!("PG update error: {}", e),
            }
        }

        // ============================================================
        // Event UPSERTs (7%): insert or update events
        // ============================================================
        let upsert_target = (total_ops * 7 / 100) as usize;
        for _ in 0..upsert_target {
            let org_id = org_ids[rng.gen_range(0..org_ids.len())];
            let user_ids = self.org_cache.get_user_ids(org_id).await;
            let event = self.generator.generate_event(org_id, &user_ids);
            match self.db.upsert_event(&event).await {
                Ok(()) => upsert_count += 1,
                Err(e) => warn!("PG upsert error: {}", e),
            }
        }

        // ============================================================
        // Event DELETEs (7%): clean up old events
        // ============================================================
        let delete_target = (total_ops * 7 / 100) as usize;
        for _ in 0..delete_target.min(5) {
            let org_id = org_ids[rng.gen_range(0..org_ids.len())];
            // Delete events older than 48-168 hours
            let hours = rng.gen_range(48..168);
            match self.db.delete_old_events(org_id, hours).await {
                Ok(n) => delete_count += n,
                Err(e) => warn!("PG delete error: {}", e),
            }
        }

        // ============================================================
        // User name UPDATEs (3%)
        // ============================================================
        let user_update_target = (total_ops * 3 / 100) as usize;
        for _ in 0..user_update_target.min(10) {
            let org_id = org_ids[rng.gen_range(0..org_ids.len())];
            let user_ids = self.org_cache.get_user_ids(org_id).await;
            if let Some(&user_id) = user_ids.first() {
                let new_name: String = fake::faker::name::en::Name().fake();
                let _ = self.db.update_user_name(user_id, &new_name).await;
            }
        }

        // ============================================================
        // Session INSERTs (5%)
        // ============================================================
        let session_target = (total_ops * 5 / 100) as usize;
        for _ in 0..session_target {
            let org_id = org_ids[rng.gen_range(0..org_ids.len())];
            let user_ids = self.org_cache.get_user_ids(org_id).await;
            let user_id = if !user_ids.is_empty() && rng.gen_bool(0.8) {
                Some(user_ids[rng.gen_range(0..user_ids.len())])
            } else {
                None
            };

            let session_id = Uuid::new_v4();
            let now = chrono::Utc::now();
            let duration_secs: i64 = rng.gen_range(10..1800);
            let started_at = now - chrono::Duration::seconds(duration_secs);
            let page_count = rng.gen_range(1..20);
            let is_bounce = page_count == 1;
            let devices = ["desktop", "mobile", "tablet"];
            let device = devices[rng.gen_range(0..devices.len())];
            let browsers = ["Chrome", "Firefox", "Safari", "Edge"];
            let browser = browsers[rng.gen_range(0..browsers.len())];
            let os_list = ["Windows", "macOS", "Linux", "iOS", "Android"];
            let os = os_list[rng.gen_range(0..os_list.len())];
            let countries = ["US", "GB", "DE", "FR", "CA", "AU", "JP"];
            let country = countries[rng.gen_range(0..countries.len())];
            let pages = self.generator.get_popular_pages();
            let landing = format!(
                "https://app.example.com{}",
                pages[rng.gen_range(0..pages.len())]
            );
            let exit = format!(
                "https://app.example.com{}",
                pages[rng.gen_range(0..pages.len())]
            );
            let utm_sources = [
                "google", "twitter", "linkedin", "facebook", "email", "direct",
            ];
            let utm_source = utm_sources[rng.gen_range(0..utm_sources.len())];

            match sqlx::query(
                r#"INSERT INTO sessions
                    (id, organization_id, user_id, device, browser, os, country_code,
                     landing_page, exit_page, page_count, duration_seconds, is_bounce,
                     utm_source, started_at, ended_at, created_at)
                   VALUES ($1, $2, $3, $4::device_type, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)"#,
            )
                .bind(session_id)
                .bind(org_id)
                .bind(user_id)
                .bind(device)
                .bind(browser)
                .bind(os)
                .bind(country)
                .bind(&landing)
                .bind(&exit)
                .bind(page_count)
                .bind(duration_secs as i32)
                .bind(is_bounce)
                .bind(utm_source)
                .bind(started_at)
                .bind(now)
                .bind(now)
                .execute(pool)
                .await
            {
                Ok(_) => new_table_write_count += 1,
                Err(e) => warn!("PG session insert error: {}", e),
            }
        }

        // ============================================================
        // Page view INSERTs (5%)
        // ============================================================
        let pv_target = (total_ops * 5 / 100) as usize;
        for _ in 0..pv_target {
            let org_id = org_ids[rng.gen_range(0..org_ids.len())];
            let user_ids = self.org_cache.get_user_ids(org_id).await;
            let user_id = if !user_ids.is_empty() && rng.gen_bool(0.7) {
                Some(user_ids[rng.gen_range(0..user_ids.len())])
            } else {
                None
            };

            let pv_id = Uuid::new_v4();
            let now = chrono::Utc::now();
            let pages = self.generator.get_popular_pages();
            let page = pages[rng.gen_range(0..pages.len())];
            let page_url = format!("https://app.example.com{}", page);
            let page_title = format!("{} - Analytics Platform", page.trim_start_matches('/'));
            let time_on_page_ms = rng.gen_range(500..60000);
            let dom_load_ms = rng.gen_range(100..3000);
            let fcp_ms = rng.gen_range(200..4000);
            let lcp_ms = rng.gen_range(500..6000);
            let scroll_depth: i16 = rng.gen_range(0..101);

            match sqlx::query(
                r#"INSERT INTO page_views
                    (id, organization_id, user_id, page_url, page_title,
                     time_on_page_ms, dom_load_ms, first_contentful_paint_ms,
                     largest_contentful_paint_ms, scroll_depth_pct, created_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"#,
            )
            .bind(pv_id)
            .bind(org_id)
            .bind(user_id)
            .bind(&page_url)
            .bind(&page_title)
            .bind(time_on_page_ms)
            .bind(dom_load_ms)
            .bind(fcp_ms)
            .bind(lcp_ms)
            .bind(scroll_depth)
            .bind(now)
            .execute(pool)
            .await
            {
                Ok(_) => new_table_write_count += 1,
                Err(e) => warn!("PG page_view insert error: {}", e),
            }
        }

        // ============================================================
        // Order + order_items INSERTs (3%)
        // ============================================================
        let order_target = (total_ops * 3 / 100) as usize;
        for _ in 0..order_target.min(10) {
            let org_id = org_ids[rng.gen_range(0..org_ids.len())];
            let user_ids = self.org_cache.get_user_ids(org_id).await;
            if user_ids.is_empty() {
                continue;
            }
            let user_id = user_ids[rng.gen_range(0..user_ids.len())];

            let order_id = Uuid::new_v4();
            let now = chrono::Utc::now();
            let order_number = format!("ORD-{}", &Uuid::new_v4().to_string()[..8]);
            let statuses = ["pending", "confirmed", "processing", "shipped"];
            let status = statuses[rng.gen_range(0..statuses.len())];
            let item_count = rng.gen_range(1..4);
            let subtotal_cents: i64 = rng.gen_range(1000..50000);
            let tax_cents: i64 = subtotal_cents * 8 / 100;
            let shipping_cents: i64 = if subtotal_cents > 5000 { 0 } else { 999 };
            let total_cents = subtotal_cents + tax_cents + shipping_cents;
            let shipping_address = json!({
                "street": "123 Main St",
                "city": "San Francisco",
                "state": "CA",
                "zip": "94105",
                "country": "US"
            });

            match sqlx::query(
                r#"INSERT INTO orders
                    (id, organization_id, user_id, order_number, status,
                     subtotal_cents, tax_cents, shipping_cents, total_cents,
                     shipping_address, placed_at, created_at, updated_at)
                   VALUES ($1, $2, $3, $4, $5::order_status, $6, $7, $8, $9, $10, $11, $12, $13)"#,
            )
            .bind(order_id)
            .bind(org_id)
            .bind(user_id)
            .bind(&order_number)
            .bind(status)
            .bind(subtotal_cents)
            .bind(tax_cents)
            .bind(shipping_cents)
            .bind(total_cents)
            .bind(&shipping_address)
            .bind(now)
            .bind(now)
            .bind(now)
            .execute(pool)
            .await
            {
                Ok(_) => {
                    new_table_write_count += 1;

                    // Insert 1-3 order items for this order
                    // We use fake product data since we may not have real product IDs
                    for item_idx in 0..item_count {
                        let item_id = Uuid::new_v4();
                        let product_names = [
                            "Widget Pro",
                            "Gadget Plus",
                            "Thingamajig",
                            "Doohickey",
                            "Gizmo X",
                        ];
                        let product_name = product_names[rng.gen_range(0..product_names.len())];
                        let sku = format!("SKU-{:04}", rng.gen_range(1000..9999));
                        let quantity = rng.gen_range(1..5);
                        let unit_price_cents: i64 = rng.gen_range(500..15000);
                        let line_total = unit_price_cents * quantity as i64;

                        // order_items requires a valid product_id FK; skip if we can't guarantee one
                        // Instead, insert directly without product_id reference for load testing
                        if let Err(e) = sqlx::query(
                            r#"INSERT INTO order_items
                                (id, order_id, product_id, product_name, sku, quantity,
                                 unit_price_cents, line_total_cents)
                               VALUES ($1, $2, (SELECT id FROM products WHERE organization_id = $3 LIMIT 1),
                                       $4, $5, $6, $7, $8)"#,
                        )
                            .bind(item_id)
                            .bind(order_id)
                            .bind(org_id)
                            .bind(product_name)
                            .bind(&sku)
                            .bind(quantity)
                            .bind(unit_price_cents)
                            .bind(line_total)
                            .execute(pool)
                            .await
                        {
                            warn!("PG order_item insert error (item {}): {}", item_idx, e);
                        } else {
                            new_table_write_count += 1;
                        }
                    }
                }
                Err(e) => warn!("PG order insert error: {}", e),
            }
        }

        // ============================================================
        // Payment INSERTs (3%)
        // ============================================================
        let payment_target = (total_ops * 3 / 100) as usize;
        for _ in 0..payment_target.min(10) {
            let org_id = org_ids[rng.gen_range(0..org_ids.len())];
            let user_ids = self.org_cache.get_user_ids(org_id).await;
            if user_ids.is_empty() {
                continue;
            }
            let user_id = user_ids[rng.gen_range(0..user_ids.len())];

            let payment_id = Uuid::new_v4();
            let now = chrono::Utc::now();
            let amount_cents: i64 = rng.gen_range(500..100000);
            let methods = [
                "credit_card",
                "debit_card",
                "bank_transfer",
                "paypal",
                "stripe",
            ];
            let method = methods[rng.gen_range(0..methods.len())];
            let statuses = [
                "pending",
                "processing",
                "completed",
                "completed",
                "completed",
            ];
            let status = statuses[rng.gen_range(0..statuses.len())];
            let gateway_tx_id = format!("txn_{}", Uuid::new_v4());
            let idempotency_key = format!("idem_{}", Uuid::new_v4());
            let gateway_response = json!({
                "processor": "stripe",
                "auth_code": format!("{:06}", rng.gen_range(100000..999999u32)),
                "risk_score": rng.gen_range(0..100)
            });

            match sqlx::query(
                r#"INSERT INTO payments
                    (id, organization_id, user_id, amount_cents, method, status,
                     gateway_transaction_id, gateway_response, idempotency_key,
                     processed_at, created_at, updated_at)
                   VALUES ($1, $2, $3, $4, $5::payment_method, $6::payment_status,
                           $7, $8, $9, $10, $11, $12)"#,
            )
            .bind(payment_id)
            .bind(org_id)
            .bind(user_id)
            .bind(amount_cents)
            .bind(method)
            .bind(status)
            .bind(&gateway_tx_id)
            .bind(&gateway_response)
            .bind(&idempotency_key)
            .bind(now)
            .bind(now)
            .bind(now)
            .execute(pool)
            .await
            {
                Ok(_) => new_table_write_count += 1,
                Err(e) => warn!("PG payment insert error: {}", e),
            }
        }

        // ============================================================
        // Campaign counter UPDATEs (2%)
        // ============================================================
        let campaign_target = (total_ops * 2 / 100) as usize;
        for _ in 0..campaign_target.min(10) {
            let org_id = org_ids[rng.gen_range(0..org_ids.len())];
            let counter_type = rng.gen_range(0..3);
            let sql = match counter_type {
                0 => "UPDATE campaigns SET click_count = click_count + 1, updated_at = NOW() WHERE id = (SELECT id FROM campaigns WHERE organization_id = $1 AND status = 'active' LIMIT 1)",
                1 => "UPDATE campaigns SET impression_count = impression_count + 1, updated_at = NOW() WHERE id = (SELECT id FROM campaigns WHERE organization_id = $1 AND status = 'active' LIMIT 1)",
                _ => "UPDATE campaigns SET conversion_count = conversion_count + 1, updated_at = NOW() WHERE id = (SELECT id FROM campaigns WHERE organization_id = $1 AND status = 'active' LIMIT 1)",
            };

            match sqlx::query(sql).bind(org_id).execute(pool).await {
                Ok(res) => {
                    if res.rows_affected() > 0 {
                        new_table_write_count += res.rows_affected();
                    }
                }
                Err(e) => warn!("PG campaign update error: {}", e),
            }
        }

        // ============================================================
        // Subscription event INSERTs (2%)
        // ============================================================
        let sub_event_target = (total_ops * 2 / 100) as usize;
        for _ in 0..sub_event_target.min(10) {
            let org_id = org_ids[rng.gen_range(0..org_ids.len())];
            let sub_event_id = Uuid::new_v4();
            let now = chrono::Utc::now();
            let event_types = [
                "created",
                "activated",
                "renewed",
                "upgraded",
                "downgraded",
                "cancelled",
                "reactivated",
            ];
            let event_type = event_types[rng.gen_range(0..event_types.len())];
            let mrr_delta: i64 = match event_type {
                "created" | "activated" | "reactivated" => rng.gen_range(1000..10000),
                "upgraded" => rng.gen_range(500..5000),
                "downgraded" => -rng.gen_range(500..5000),
                "cancelled" => -rng.gen_range(1000..10000),
                _ => 0,
            };
            let metadata = json!({
                "source": "system",
                "batch_writer": true,
                "timestamp": now.to_rfc3339()
            });

            // Need a valid subscription_id; use subquery to pick one
            match sqlx::query(
                r#"INSERT INTO subscription_events
                    (id, organization_id, subscription_id, event_type, mrr_delta_cents, metadata, occurred_at)
                   VALUES ($1, $2,
                           (SELECT id FROM subscriptions WHERE organization_id = $2 ORDER BY RANDOM() LIMIT 1),
                           $3, $4, $5, $6)"#,
            )
                .bind(sub_event_id)
                .bind(org_id)
                .bind(event_type)
                .bind(mrr_delta)
                .bind(&metadata)
                .bind(now)
                .execute(pool)
                .await
            {
                Ok(_) => new_table_write_count += 1,
                Err(e) => warn!("PG subscription_event insert error: {}", e),
            }
        }

        // ============================================================
        // Ledger entry INSERTs (2%) - always insert debit+credit pair
        // ============================================================
        let ledger_target = (total_ops * 2 / 100) as usize;
        for _ in 0..ledger_target.min(5) {
            let org_id = org_ids[rng.gen_range(0..org_ids.len())];
            let transaction_id = Uuid::new_v4();
            let now = chrono::Utc::now();
            let amount_cents: i64 = rng.gen_range(100..50000);

            // Common account pairs: revenue/receivable, expense/payable, etc.
            let account_pairs = [
                ("1100", "Accounts Receivable", "4000", "Revenue"),
                ("5000", "Cost of Goods Sold", "2000", "Accounts Payable"),
                ("1000", "Cash", "1100", "Accounts Receivable"),
                ("6000", "Operating Expenses", "1000", "Cash"),
            ];
            let (debit_code, debit_name, credit_code, credit_name) =
                account_pairs[rng.gen_range(0..account_pairs.len())];

            let ref_types = ["order", "payment", "subscription", "refund"];
            let ref_type = ref_types[rng.gen_range(0..ref_types.len())];
            let ref_id = Uuid::new_v4();
            let description = format!(
                "{} transaction for {}",
                ref_type,
                amount_cents as f64 / 100.0
            );
            let metadata = json!({
                "source": "batch_writer",
                "auto_generated": true
            });

            // Insert debit entry
            let debit_id = Uuid::new_v4();
            match sqlx::query(
                r#"INSERT INTO ledger_entries
                    (id, organization_id, transaction_id, entry_type, account_code, account_name,
                     amount_cents, description, reference_type, reference_id, metadata, posted_at, created_at)
                   VALUES ($1, $2, $3, 'debit'::ledger_entry_type, $4, $5, $6, $7, $8, $9, $10, $11, $12)"#,
            )
                .bind(debit_id)
                .bind(org_id)
                .bind(transaction_id)
                .bind(debit_code)
                .bind(debit_name)
                .bind(amount_cents)
                .bind(&description)
                .bind(ref_type)
                .bind(ref_id)
                .bind(&metadata)
                .bind(now)
                .bind(now)
                .execute(pool)
                .await
            {
                Ok(_) => new_table_write_count += 1,
                Err(e) => {
                    warn!("PG ledger debit insert error: {}", e);
                    continue; // Skip credit if debit failed
                }
            }

            // Insert matching credit entry
            let credit_id = Uuid::new_v4();
            match sqlx::query(
                r#"INSERT INTO ledger_entries
                    (id, organization_id, transaction_id, entry_type, account_code, account_name,
                     amount_cents, description, reference_type, reference_id, metadata, posted_at, created_at)
                   VALUES ($1, $2, $3, 'credit'::ledger_entry_type, $4, $5, $6, $7, $8, $9, $10, $11, $12)"#,
            )
                .bind(credit_id)
                .bind(org_id)
                .bind(transaction_id)
                .bind(credit_code)
                .bind(credit_name)
                .bind(amount_cents)
                .bind(&description)
                .bind(ref_type)
                .bind(ref_id)
                .bind(&metadata)
                .bind(now)
                .bind(now)
                .execute(pool)
                .await
            {
                Ok(_) => new_table_write_count += 1,
                Err(e) => warn!("PG ledger credit insert error: {}", e),
            }
        }

        let duration = start.elapsed().as_secs_f64();
        self.telemetry
            .metrics()
            .record_operation_success("pg_write_batch");

        debug!(
            "PG write batch: {} inserts, {} updates, {} upserts, {} deletes, {} new-table writes in {:.2}ms",
            insert_count, update_count, upsert_count, delete_count, new_table_write_count, duration * 1000.0
        );

        Ok(())
    }
}
