use rand::Rng;
use serde_json::{Value, json};

use super::{
    Query, ch_query, mongo_aggregate, pg_query, redis_get, redis_hgetall, redis_smembers,
    redis_zrevrange, weaviate_graphql,
};

/// Database-specific query definitions for the ADAM e-commerce demo.
/// Covers both T-ECD dataset tables and synthetic e-commerce data.
/// All domains share user_id, product_id, order_id, and brand_id.
/// Each returns a (description, query_body) tuple for the Eden API.

/// Route queries by endpoint name for the retail vertical.
pub fn queries_for(endpoint_name: &str) -> Vec<Query> {
    match endpoint_name {
        "postgres" | "adam_postgres" => pg_queries(),
        "mongodb" | "adam_mongodb" => mongo_queries(),
        "redis" | "adam_redis" => redis_queries(),
        "clickhouse" | "adam_clickhouse" => clickhouse_queries(),
        "weaviate" | "adam_weaviate" => weaviate_queries(),
        _ => pg_queries(), // fallback
    }
}

/// Cross-database queries as (endpoint_name, description, query_body) triples.
pub fn cross_db_queries() -> Vec<Vec<(&'static str, &'static str, Value)>> {
    let raw = raw_cross_db_queries();
    raw.into_iter()
        .map(|q| {
            q.steps
                .into_iter()
                .map(|s| (s.database, s.description, s.query))
                .collect()
        })
        .collect()
}

// ─── PostgreSQL Queries (users, orders, invoices, payments, marketplace) ───

pub fn pg_queries() -> Vec<(&'static str, Value)> {
    vec![
        // ── T-ECD marketplace queries ──
        (
            "User activity by action type",
            pg_query(
                "SELECT action_type, \
                      COUNT(*) as total_events, \
                      COUNT(DISTINCT user_id) as unique_users \
                      FROM marketplace_events \
                      GROUP BY action_type \
                      ORDER BY total_events DESC",
            ),
        ),
        (
            "Marketplace events by region",
            pg_query(
                "SELECT u.region, \
                      COUNT(*) as events, \
                      COUNT(DISTINCT e.user_id) as users \
                      FROM marketplace_events e \
                      JOIN users u ON e.user_id = u.user_id \
                      GROUP BY u.region \
                      ORDER BY events DESC \
                      LIMIT 20",
            ),
        ),
        (
            "Conversion funnel by subdomain",
            pg_query(
                "SELECT subdomain, action_type, \
                      COUNT(*) as event_count, \
                      COUNT(DISTINCT user_id) as unique_users \
                      FROM marketplace_events \
                      WHERE subdomain != '' \
                      GROUP BY subdomain, action_type \
                      ORDER BY subdomain, event_count DESC",
            ),
        ),
        (
            "Most active users",
            pg_query(
                "SELECT u.user_id, u.region, u.socdem_cluster, \
                      COUNT(*) as total_events \
                      FROM marketplace_events e \
                      JOIN users u ON e.user_id = u.user_id \
                      GROUP BY u.user_id, u.region, u.socdem_cluster \
                      ORDER BY total_events DESC \
                      LIMIT 20",
            ),
        ),
        (
            "Daily event trend",
            pg_query(
                "SELECT event_day, \
                      COUNT(*) as events, \
                      COUNT(DISTINCT user_id) as active_users \
                      FROM marketplace_events \
                      GROUP BY event_day \
                      ORDER BY event_day",
            ),
        ),
        (
            "Top item categories by interactions",
            pg_query(
                "SELECT i.category, i.subcategory, \
                      COUNT(*) as interactions, \
                      COUNT(DISTINCT e.user_id) as unique_users, \
                      ROUND(AVG(i.price)::numeric, 2) as avg_price \
                      FROM marketplace_events e \
                      JOIN marketplace_items i ON e.item_id = i.item_id \
                      WHERE i.category IS NOT NULL \
                      GROUP BY i.category, i.subcategory \
                      ORDER BY interactions DESC \
                      LIMIT 25",
            ),
        ),
        (
            "OS platform breakdown",
            pg_query(
                "SELECT os, \
                      COUNT(*) as events, \
                      COUNT(DISTINCT user_id) as users \
                      FROM marketplace_events \
                      WHERE os != '' \
                      GROUP BY os \
                      ORDER BY events DESC",
            ),
        ),
        // ── Synthetic e-commerce queries ──
        (
            "Order volume by status",
            pg_query(
                "SELECT status, \
                      COUNT(*) as total_orders, \
                      ROUND(SUM(total)::numeric, 2) as total_revenue, \
                      ROUND(AVG(total)::numeric, 2) as avg_order_value \
                      FROM orders \
                      GROUP BY status \
                      ORDER BY total_orders DESC",
            ),
        ),
        (
            "Top spending customers",
            pg_query(
                "SELECT u.user_id, u.first_name, u.last_name, u.loyalty_tier, \
                      COUNT(o.order_id) as order_count, \
                      ROUND(SUM(o.total)::numeric, 2) as total_spent \
                      FROM orders o \
                      JOIN users u ON o.user_id = u.user_id \
                      GROUP BY u.user_id, u.first_name, u.last_name, u.loyalty_tier \
                      ORDER BY total_spent DESC \
                      LIMIT 25",
            ),
        ),
        (
            "Revenue by country",
            pg_query(
                "SELECT country, region, \
                      COUNT(*) as orders, \
                      ROUND(SUM(total)::numeric, 2) as revenue, \
                      COUNT(DISTINCT user_id) as unique_buyers \
                      FROM orders \
                      GROUP BY country, region \
                      ORDER BY revenue DESC \
                      LIMIT 20",
            ),
        ),
        (
            "Invoice status summary",
            pg_query(
                "SELECT i.status, \
                      COUNT(*) as invoice_count, \
                      ROUND(SUM(i.total)::numeric, 2) as total_amount, \
                      ROUND(AVG(i.total)::numeric, 2) as avg_amount \
                      FROM invoices i \
                      GROUP BY i.status \
                      ORDER BY total_amount DESC",
            ),
        ),
        (
            "Payment method distribution",
            pg_query(
                "SELECT p.method, p.status, \
                      COUNT(*) as payment_count, \
                      ROUND(SUM(p.amount)::numeric, 2) as total_amount \
                      FROM payments p \
                      GROUP BY p.method, p.status \
                      ORDER BY total_amount DESC \
                      LIMIT 20",
            ),
        ),
        (
            "Best selling products by revenue",
            pg_query(
                "SELECT oi.product_id, \
                      SUM(oi.quantity) as units_sold, \
                      ROUND(SUM(oi.line_total)::numeric, 2) as revenue, \
                      COUNT(DISTINCT o.user_id) as unique_buyers \
                      FROM order_items oi \
                      JOIN orders o ON oi.order_id = o.order_id \
                      WHERE o.status NOT IN ('cancelled', 'refunded') \
                      GROUP BY oi.product_id \
                      ORDER BY revenue DESC \
                      LIMIT 25",
            ),
        ),
        (
            "Orders with coupon usage",
            pg_query(
                "SELECT c.code, c.coupon_type, c.discount_value, \
                      COUNT(o.order_id) as times_used, \
                      ROUND(SUM(o.total)::numeric, 2) as total_order_value \
                      FROM orders o \
                      JOIN coupons c ON o.coupon_id = c.coupon_id \
                      GROUP BY c.code, c.coupon_type, c.discount_value \
                      ORDER BY times_used DESC \
                      LIMIT 20",
            ),
        ),
        (
            "Loyalty tier analysis",
            pg_query(
                "SELECT u.loyalty_tier, \
                      COUNT(DISTINCT u.user_id) as users, \
                      COUNT(o.order_id) as total_orders, \
                      ROUND(AVG(o.total)::numeric, 2) as avg_order_value, \
                      ROUND(SUM(o.total)::numeric, 2) as total_revenue \
                      FROM users u \
                      LEFT JOIN orders o ON u.user_id = o.user_id \
                      WHERE u.loyalty_tier IS NOT NULL \
                      GROUP BY u.loyalty_tier \
                      ORDER BY total_revenue DESC",
            ),
        ),
    ]
}

// ─── MongoDB Queries (retail_items, retail_events) ──

pub fn mongo_queries() -> Vec<(&'static str, Value)> {
    vec![
        // ── T-ECD retail queries ──
        (
            "Retail action type distribution",
            mongo_aggregate(
                "ecommerce",
                "retail_events",
                json!([
                    {"$group": {
                        "_id": "$action_type",
                        "count": {"$sum": 1}
                    }},
                    {"$sort": {"count": -1}}
                ]),
            ),
        ),
        (
            "Top retail items by interaction count",
            mongo_aggregate(
                "ecommerce",
                "retail_events",
                json!([
                    {"$group": {
                        "_id": "$item_id",
                        "interaction_count": {"$sum": 1}
                    }},
                    {"$sort": {"interaction_count": -1}},
                    {"$limit": 20}
                ]),
            ),
        ),
        (
            "Retail items by category",
            mongo_aggregate(
                "ecommerce",
                "retail_items",
                json!([
                    {"$match": {"category": {"$ne": null}}},
                    {"$group": {
                        "_id": "$category",
                        "count": {"$sum": 1},
                        "avg_price": {"$avg": "$price"}
                    }},
                    {"$sort": {"count": -1}},
                    {"$limit": 20}
                ]),
            ),
        ),
        (
            "Most active retail users",
            mongo_aggregate(
                "ecommerce",
                "retail_events",
                json!([
                    {"$group": {
                        "_id": "$user_id",
                        "event_count": {"$sum": 1},
                        "items_viewed": {"$addToSet": "$item_id"}
                    }},
                    {"$addFields": {"unique_items": {"$size": "$items_viewed"}}},
                    {"$project": {"items_viewed": 0}},
                    {"$sort": {"event_count": -1}},
                    {"$limit": 20}
                ]),
            ),
        ),
    ]
}

// ─── Redis Queries (sessions, inventory, leaderboards, carts, offers) ──

pub fn redis_queries() -> Vec<(&'static str, Value)> {
    let mut rng = rand::thread_rng();
    let offer_item_key = format!("offer:item:offer_{}", rng.gen_range(1..1001));
    let session_key = format!("session:{}", rng.gen_range(1..50001));
    let inventory_key = format!("inventory:{}", rng.gen_range(1..250001));
    let price_key = format!("price:{}", rng.gen_range(1..250001));
    let user_orders_key = format!("user:orders:{}", rng.gen_range(1..100001));
    let user_recent_key = format!("user:recent:{}", rng.gen_range(1..50001));
    let cart_active_key = format!("cart:active:{}", rng.gen_range(1..50001));
    let cart_abandoned_key = format!("cart:abandoned:{}", rng.gen_range(1..10001));
    vec![
        // ── T-ECD offer queries ──
        (
            "Top offer items by engagement",
            redis_zrevrange("leaderboard:offer_items", 0, 19),
        ),
        (
            "Most active offer users",
            redis_zrevrange("leaderboard:user_offer_activity", 0, 19),
        ),
        (
            "Get total offer events",
            redis_get("stats:total_offer_events"),
        ),
        ("Get offer item details", redis_hgetall(&offer_item_key)),
        // ── Synthetic e-commerce queries ──
        ("Get user session", redis_hgetall(&session_key)),
        ("Get product inventory", redis_hgetall(&inventory_key)),
        ("Get product price", redis_get(&price_key)),
        (
            "Top spending customers",
            redis_zrevrange("leaderboard:top_spenders", 0, 19),
        ),
        (
            "Top selling products",
            redis_zrevrange("leaderboard:top_products", 0, 19),
        ),
        (
            "Top brands by revenue",
            redis_zrevrange("leaderboard:top_brands", 0, 9),
        ),
        (
            "Top categories by sales",
            redis_zrevrange("leaderboard:top_categories", 0, 9),
        ),
        ("Get user order history", redis_smembers(&user_orders_key)),
        ("Out of stock alerts", redis_smembers("alerts:out_of_stock")),
        (
            "Low stock alerts",
            // SCARD not directly supported, using SMEMBERS as closest equivalent
            redis_smembers("alerts:low_stock"),
        ),
        ("Get total orders stat", redis_get("stats:total_orders")),
        ("Get total revenue stat", redis_get("stats:total_revenue")),
        ("Get active cart", redis_hgetall(&cart_active_key)),
        (
            "Recently viewed products",
            // Using GET as fallback for LRANGE
            redis_get(&user_recent_key),
        ),
        // ── Abandoned cart tracking ──
        (
            "Get abandoned cart details",
            redis_hgetall(&cart_abandoned_key),
        ),
        (
            "Highest value abandoned carts",
            redis_zrevrange("abandoned_carts:by_value", 0, 19),
        ),
        (
            "Most recent abandoned carts",
            redis_zrevrange("abandoned_carts:by_time", 0, 19),
        ),
        (
            "Abandonment reasons breakdown",
            redis_hgetall("stats:abandonment_reasons"),
        ),
        (
            "Total abandoned carts count",
            redis_get("stats:abandoned_carts"),
        ),
    ]
}

// ─── ClickHouse Queries (marketplace + clickstream + purchase analytics) ──

pub fn clickhouse_queries() -> Vec<(&'static str, Value)> {
    vec![
        // ── T-ECD marketplace OLAP ──
        (
            "Event volume by action type",
            ch_query(
                "SELECT action_type, \
                      count() as total, \
                      uniq(user_id) as unique_users \
                      FROM analytics.marketplace_events \
                      GROUP BY action_type \
                      ORDER BY total DESC",
            ),
        ),
        (
            "Daily event trend (marketplace)",
            ch_query(
                "SELECT event_day, \
                      count() as events, \
                      uniq(user_id) as active_users \
                      FROM analytics.marketplace_events \
                      GROUP BY event_day \
                      ORDER BY event_day",
            ),
        ),
        (
            "Daily action summary (pre-aggregated)",
            ch_query(
                "SELECT event_day, action_type, subdomain, \
                      event_count, unique_users \
                      FROM analytics.daily_action_summary \
                      ORDER BY event_day, event_count DESC \
                      LIMIT 50",
            ),
        ),
        // ── Synthetic clickstream analytics ──
        (
            "Clickstream event funnel",
            ch_query(
                "SELECT event_type, \
                      count() as events, \
                      uniq(user_id) as unique_users, \
                      uniq(session_id) as unique_sessions \
                      FROM analytics.clickstream_events \
                      GROUP BY event_type \
                      ORDER BY events DESC",
            ),
        ),
        (
            "Traffic by device and browser",
            ch_query(
                "SELECT device_type, browser, \
                      count() as events, \
                      uniq(user_id) as users \
                      FROM analytics.clickstream_events \
                      GROUP BY device_type, browser \
                      ORDER BY events DESC \
                      LIMIT 20",
            ),
        ),
        (
            "Hourly traffic pattern",
            ch_query(
                "SELECT event_hour, \
                      count() as events, \
                      uniq(user_id) as active_users, \
                      uniq(session_id) as sessions \
                      FROM analytics.clickstream_events \
                      GROUP BY event_hour \
                      ORDER BY event_hour",
            ),
        ),
        (
            "Top referral sources",
            ch_query(
                "SELECT referrer, \
                      count() as events, \
                      uniq(user_id) as users, \
                      countIf(event_type = 'purchase') as purchases \
                      FROM analytics.clickstream_events \
                      GROUP BY referrer \
                      ORDER BY events DESC",
            ),
        ),
        (
            "Conversion rate by country",
            ch_query(
                "SELECT country, \
                      count() as total_events, \
                      countIf(event_type = 'purchase') as purchases, \
                      round(countIf(event_type = 'purchase') * 100.0 / count(), 2) as conversion_rate \
                      FROM analytics.clickstream_events \
                      GROUP BY country \
                      ORDER BY total_events DESC \
                      LIMIT 20",
            ),
        ),
        // ── Purchase event analytics ──
        (
            "Revenue by category",
            ch_query(
                "SELECT category, \
                      count() as orders, \
                      sum(total) as revenue, \
                      round(avg(total), 2) as avg_order_value, \
                      uniq(user_id) as unique_buyers \
                      FROM analytics.purchase_events \
                      GROUP BY category \
                      ORDER BY revenue DESC",
            ),
        ),
        (
            "Revenue trend by day",
            ch_query(
                "SELECT event_day, \
                      count() as orders, \
                      round(sum(total), 2) as revenue, \
                      uniq(user_id) as buyers \
                      FROM analytics.purchase_events \
                      GROUP BY event_day \
                      ORDER BY event_day",
            ),
        ),
        (
            "Payment method analysis",
            ch_query(
                "SELECT payment_method, \
                      count() as transactions, \
                      round(sum(total), 2) as revenue, \
                      round(avg(total), 2) as avg_transaction \
                      FROM analytics.purchase_events \
                      GROUP BY payment_method \
                      ORDER BY revenue DESC",
            ),
        ),
        (
            "Daily revenue summary (pre-aggregated)",
            ch_query(
                "SELECT event_day, category, country, \
                      order_count, revenue, unique_buyers, units_sold \
                      FROM analytics.revenue_daily \
                      ORDER BY event_day DESC, revenue DESC \
                      LIMIT 50",
            ),
        ),
        (
            "Conversion funnel by device",
            ch_query(
                "SELECT step, device_type, \
                      sum(event_count) as events, \
                      sum(unique_users) as users \
                      FROM analytics.funnel_events \
                      GROUP BY step, device_type \
                      ORDER BY events DESC",
            ),
        ),
    ]
}

// ─── Weaviate Vector Queries (Review) ────────────

pub fn weaviate_queries() -> Vec<(&'static str, Value)> {
    vec![
        // ── T-ECD review embeddings ──
        (
            "Find similar reviews: positive experience",
            weaviate_graphql(
                "{ Get { Review(nearText: {concepts: [\"excellent outstanding amazing best perfect\"]}, limit: 10) { user_id brand_id rating event_day } } }",
            ),
        ),
        (
            "Find similar reviews: negative experience",
            weaviate_graphql(
                "{ Get { Review(nearText: {concepts: [\"terrible awful worst disappointed poor\"]}, limit: 10) { user_id brand_id rating event_day } } }",
            ),
        ),
        (
            "Find similar reviews: good value",
            weaviate_graphql(
                "{ Get { Review(nearText: {concepts: [\"good satisfied happy quality recommended\"]}, limit: 10) { user_id brand_id rating event_day } } }",
            ),
        ),
    ]
}

// ─── Cross-Database Queries ───────────────────────────────────
// These queries span multiple databases using shared user_id.

pub struct CrossDbQuery {
    pub description: &'static str,
    pub steps: Vec<QueryStep>,
}

pub struct QueryStep {
    pub database: &'static str,
    pub description: &'static str,
    pub query: Value,
}

fn raw_cross_db_queries() -> Vec<CrossDbQuery> {
    vec![
        CrossDbQuery {
            description: "E-commerce dashboard: orders + inventory + analytics",
            steps: vec![
                QueryStep {
                    database: "postgres",
                    description: "Order volume and revenue by status",
                    query: pg_query(
                        "SELECT status, COUNT(*) as orders, \
                              ROUND(SUM(total)::numeric, 2) as revenue \
                              FROM orders GROUP BY status \
                              ORDER BY revenue DESC",
                    ),
                },
                QueryStep {
                    database: "redis",
                    description: "Real-time stats and leaderboards",
                    query: redis_get("stats:total_revenue"),
                },
                QueryStep {
                    database: "clickhouse",
                    description: "Revenue trend by category",
                    query: ch_query(
                        "SELECT category, \
                              round(sum(revenue), 2) as total_revenue, \
                              sum(order_count) as orders \
                              FROM analytics.revenue_daily \
                              GROUP BY category \
                              ORDER BY total_revenue DESC \
                              LIMIT 10",
                    ),
                },
            ],
        },
        CrossDbQuery {
            description: "User journey: browse -> cart -> purchase -> review",
            steps: vec![
                QueryStep {
                    database: "clickhouse",
                    description: "Clickstream funnel events",
                    query: ch_query(
                        "SELECT event_type, \
                              count() as events, \
                              uniq(user_id) as users \
                              FROM analytics.clickstream_events \
                              WHERE event_type IN ('page_view','product_view','add_to_cart','begin_checkout','purchase') \
                              GROUP BY event_type \
                              ORDER BY events DESC",
                    ),
                },
                QueryStep {
                    database: "mongodb",
                    description: "Retail event breakdown by action type",
                    query: mongo_aggregate(
                        "ecommerce",
                        "retail_events",
                        json!([
                            {"$group": {
                                "_id": "$action_type",
                                "count": {"$sum": 1},
                                "unique_users": {"$addToSet": "$user_id"}
                            }},
                            {"$project": {
                                "action_type": "$_id",
                                "count": 1,
                                "unique_users": {"$size": "$unique_users"}
                            }},
                            {"$sort": {"count": -1}}
                        ]),
                    ),
                },
                QueryStep {
                    database: "postgres",
                    description: "Recent orders and payment methods",
                    query: pg_query(
                        "SELECT p.method, COUNT(*) as payments, \
                              ROUND(SUM(p.amount)::numeric, 2) as total \
                              FROM payments p \
                              GROUP BY p.method \
                              ORDER BY total DESC LIMIT 10",
                    ),
                },
                QueryStep {
                    database: "weaviate",
                    description: "Post-purchase review sentiment",
                    query: weaviate_graphql(
                        "{ Get { Review(nearText: {concepts: [\"satisfied happy good purchase quality\"]}, limit: 10) { review_id user_id text rating } } }",
                    ),
                },
            ],
        },
        CrossDbQuery {
            description: "Product intelligence: catalog + inventory + reviews",
            steps: vec![
                QueryStep {
                    database: "postgres",
                    description: "Marketplace items summary by brand",
                    query: pg_query(
                        "SELECT b.name as brand, COUNT(*) as items, \
                              ROUND(AVG(mi.price)::numeric, 2) as avg_price \
                              FROM marketplace_items mi \
                              JOIN brands b ON mi.brand_id = b.brand_id \
                              GROUP BY b.name \
                              ORDER BY items DESC LIMIT 10",
                    ),
                },
                QueryStep {
                    database: "redis",
                    description: "Low stock and out-of-stock alerts",
                    query: redis_smembers("alerts:out_of_stock"),
                },
                QueryStep {
                    database: "mongodb",
                    description: "Retail items by category",
                    query: mongo_aggregate(
                        "ecommerce",
                        "retail_items",
                        json!([
                            {"$group": {
                                "_id": "$category",
                                "items": {"$sum": 1},
                                "avg_price": {"$avg": "$price"}
                            }},
                            {"$sort": {"items": -1}}
                        ]),
                    ),
                },
                QueryStep {
                    database: "weaviate",
                    description: "Find reviews mentioning top-rated products",
                    query: weaviate_graphql(
                        "{ Get { Review(nearText: {concepts: [\"bestseller popular trending top rated\"]}, limit: 10) { review_id user_id text rating } } }",
                    ),
                },
            ],
        },
        CrossDbQuery {
            description: "Order fulfillment: orders + invoices + analytics",
            steps: vec![
                QueryStep {
                    database: "postgres",
                    description: "Orders pending fulfillment with invoice status",
                    query: pg_query(
                        "SELECT o.status as order_status, \
                              i.status as invoice_status, \
                              COUNT(*) as total, \
                              ROUND(SUM(o.total)::numeric, 2) as revenue \
                              FROM orders o \
                              LEFT JOIN invoices i ON o.order_id = i.order_id \
                              GROUP BY o.status, i.status \
                              ORDER BY total DESC LIMIT 15",
                    ),
                },
                QueryStep {
                    database: "clickhouse",
                    description: "Purchase event volume over time",
                    query: ch_query(
                        "SELECT toDate(event_time) as day, \
                              count() as purchases, \
                              uniq(user_id) as buyers \
                              FROM analytics.purchase_events \
                              GROUP BY day \
                              ORDER BY day DESC \
                              LIMIT 14",
                    ),
                },
                QueryStep {
                    database: "redis",
                    description: "Total orders count",
                    query: redis_get("stats:total_orders"),
                },
            ],
        },
        CrossDbQuery {
            description: "Cross-domain user activity: marketplace + retail + offers",
            steps: vec![
                QueryStep {
                    database: "postgres",
                    description: "Marketplace activity by loyalty tier",
                    query: pg_query(
                        "SELECT u.loyalty_tier, COUNT(*) as events, \
                              COUNT(DISTINCT e.user_id) as users \
                              FROM marketplace_events e \
                              JOIN users u ON e.user_id = u.user_id \
                              WHERE u.loyalty_tier IS NOT NULL \
                              GROUP BY u.loyalty_tier \
                              ORDER BY events DESC",
                    ),
                },
                QueryStep {
                    database: "mongodb",
                    description: "Retail activity summary",
                    query: mongo_aggregate(
                        "ecommerce",
                        "retail_events",
                        json!([
                            {"$group": {
                                "_id": "$action_type",
                                "count": {"$sum": 1}
                            }},
                            {"$sort": {"count": -1}}
                        ]),
                    ),
                },
                QueryStep {
                    database: "redis",
                    description: "Offer engagement stats",
                    query: redis_get("stats:total_offer_events"),
                },
                QueryStep {
                    database: "clickhouse",
                    description: "Clickstream by referrer",
                    query: ch_query(
                        "SELECT referrer, count() as events, \
                              uniq(user_id) as users \
                              FROM analytics.clickstream_events \
                              GROUP BY referrer \
                              ORDER BY events DESC",
                    ),
                },
            ],
        },
    ]
}
