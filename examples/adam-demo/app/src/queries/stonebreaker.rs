use serde_json::{Value, json};

use super::{
    Query, ch_query, mongo_aggregate, pg_query, redis_zrevrange, retail, weaviate_graphql,
};

pub fn queries_for(endpoint_name: &str) -> Vec<Query> {
    match endpoint_name {
        "weaviate" => {
            let mut queries = retail::weaviate_queries();
            queries.extend(stonebreaker_document_queries());
            queries
        }
        _ => retail::queries_for(endpoint_name),
    }
}

pub fn cross_db_queries() -> Vec<Vec<(&'static str, &'static str, Value)>> {
    vec![
        vec![
            (
                "postgres",
                "SB-001 evidence: marketplace engagement by brand",
                pg_query(
                    "SELECT mi.brand_id, COUNT(*) AS marketplace_events, \
                     COUNT(DISTINCT e.user_id) AS active_users, \
                     ROUND(AVG(mi.price)::numeric, 2) AS avg_marketplace_price \
                     FROM marketplace_events e \
                     JOIN marketplace_items mi ON e.item_id = mi.item_id \
                     WHERE mi.brand_id IS NOT NULL \
                     GROUP BY mi.brand_id \
                     ORDER BY marketplace_events DESC \
                     LIMIT 25",
                ),
            ),
            (
                "mongodb",
                "SB-001 evidence: retail catalog price and assortment by brand",
                mongo_aggregate(
                    "ecommerce",
                    "retail_items",
                    json!([
                        {"$match": {"brand_id": {"$ne": null}}},
                        {"$group": {
                            "_id": "$brand_id",
                            "catalog_items": {"$sum": 1},
                            "avg_price": {"$avg": "$price"},
                            "max_price": {"$max": "$price"}
                        }},
                        {"$sort": {"catalog_items": -1}},
                        {"$limit": 25}
                    ]),
                ),
            ),
        ],
        vec![
            (
                "postgres",
                "SB-002 evidence: top spenders from orders",
                pg_query(
                    "SELECT user_id, COUNT(*) AS order_count, \
                     ROUND(SUM(total)::numeric, 2) AS total_spent \
                     FROM orders \
                     GROUP BY user_id \
                     ORDER BY total_spent DESC \
                     LIMIT 25",
                ),
            ),
            (
                "redis",
                "SB-002 evidence: top spender leaderboard from Redis",
                redis_zrevrange("leaderboard:top_spenders", 0, 24),
            ),
        ],
        vec![
            (
                "postgres",
                "SB-003 evidence: marketplace engagement by brand",
                pg_query(
                    "SELECT mi.brand_id, COUNT(*) AS marketplace_events, \
                     COUNT(DISTINCT e.user_id) AS active_users \
                     FROM marketplace_events e \
                     JOIN marketplace_items mi ON e.item_id = mi.item_id \
                     WHERE mi.brand_id IS NOT NULL \
                     GROUP BY mi.brand_id \
                     ORDER BY marketplace_events DESC \
                     LIMIT 25",
                ),
            ),
            (
                "weaviate",
                "SB-003 evidence: negative review clusters by brand",
                weaviate_graphql(
                    "{ Get { Review(nearText: {concepts: [\"terrible awful worst disappointed poor\"]}, limit: 25) { brand_id rating event_day user_id } } }",
                ),
            ),
        ],
        vec![
            (
                "mongodb",
                "SB-004 evidence: retail catalog coverage by brand",
                mongo_aggregate(
                    "ecommerce",
                    "retail_items",
                    json!([
                        {"$match": {"brand_id": {"$ne": null}}},
                        {"$group": {
                            "_id": "$brand_id",
                            "catalog_items": {"$sum": 1},
                            "avg_price": {"$avg": "$price"}
                        }},
                        {"$sort": {"catalog_items": -1}},
                        {"$limit": 25}
                    ]),
                ),
            ),
            (
                "redis",
                "SB-004 evidence: top brand leaderboard from Redis",
                redis_zrevrange("leaderboard:top_brands", 0, 24),
            ),
        ],
        vec![
            (
                "mongodb",
                "SB-005 evidence: retail catalog coverage by brand",
                mongo_aggregate(
                    "ecommerce",
                    "retail_items",
                    json!([
                        {"$match": {"brand_id": {"$ne": null}}},
                        {"$group": {
                            "_id": "$brand_id",
                            "catalog_items": {"$sum": 1},
                            "avg_price": {"$avg": "$price"},
                            "max_price": {"$max": "$price"}
                        }},
                        {"$sort": {"catalog_items": -1}},
                        {"$limit": 25}
                    ]),
                ),
            ),
            (
                "clickhouse",
                "SB-005 evidence: purchase revenue by brand",
                ch_query(
                    "SELECT brand_id, countDistinct(order_id) AS orders, \
                     sum(quantity) AS units_sold, round(sum(total), 2) AS revenue \
                     FROM analytics.purchase_events \
                     GROUP BY brand_id \
                     ORDER BY revenue DESC \
                     LIMIT 25",
                ),
            ),
        ],
        vec![
            (
                "redis",
                "SB-006 evidence: top product leaderboard from Redis",
                redis_zrevrange("leaderboard:top_products", 0, 24),
            ),
            (
                "clickhouse",
                "SB-006 evidence: purchase volume by product",
                ch_query(
                    "SELECT product_id, brand_id, sum(quantity) AS units_sold, \
                     round(sum(total), 2) AS revenue \
                     FROM analytics.purchase_events \
                     GROUP BY product_id, brand_id \
                     ORDER BY units_sold DESC \
                     LIMIT 25",
                ),
            ),
        ],
        vec![
            (
                "weaviate",
                "SB-007 evidence: negative review clusters by brand",
                weaviate_graphql(
                    "{ Get { Review(nearText: {concepts: [\"terrible awful worst disappointed poor\"]}, limit: 25) { brand_id rating event_day user_id } } }",
                ),
            ),
            (
                "clickhouse",
                "SB-007 evidence: purchase revenue by brand",
                ch_query(
                    "SELECT brand_id, countDistinct(order_id) AS orders, \
                     sum(quantity) AS units_sold, round(sum(total), 2) AS revenue \
                     FROM analytics.purchase_events \
                     GROUP BY brand_id \
                     ORDER BY revenue DESC \
                     LIMIT 25",
                ),
            ),
        ],
        vec![
            (
                "weaviate",
                "SB-008 evidence: negative review clusters by brand",
                weaviate_graphql(
                    "{ Get { Review(nearText: {concepts: [\"terrible awful worst disappointed poor\"]}, limit: 25) { brand_id rating event_day user_id } } }",
                ),
            ),
            (
                "mongodb",
                "SB-008 evidence: retail catalog coverage by brand",
                mongo_aggregate(
                    "ecommerce",
                    "retail_items",
                    json!([
                        {"$match": {"brand_id": {"$ne": null}}},
                        {"$group": {
                            "_id": "$brand_id",
                            "catalog_items": {"$sum": 1},
                            "avg_price": {"$avg": "$price"}
                        }},
                        {"$sort": {"catalog_items": -1}},
                        {"$limit": 25}
                    ]),
                ),
            ),
        ],
        vec![
            (
                "postgres",
                "SB-009 evidence: marketplace engagement by brand",
                pg_query(
                    "SELECT mi.brand_id, COUNT(*) AS marketplace_events, \
                     COUNT(DISTINCT e.user_id) AS active_users \
                     FROM marketplace_events e \
                     JOIN marketplace_items mi ON e.item_id = mi.item_id \
                     WHERE mi.brand_id IS NOT NULL \
                     GROUP BY mi.brand_id \
                     ORDER BY marketplace_events DESC \
                     LIMIT 25",
                ),
            ),
            (
                "clickhouse",
                "SB-009 evidence: purchase revenue by brand",
                ch_query(
                    "SELECT brand_id, countDistinct(order_id) AS orders, \
                     sum(quantity) AS units_sold, round(sum(total), 2) AS revenue \
                     FROM analytics.purchase_events \
                     GROUP BY brand_id \
                     ORDER BY revenue DESC \
                     LIMIT 25",
                ),
            ),
        ],
    ]
}

fn stonebreaker_document_queries() -> Vec<Query> {
    vec![
        (
            "Stonebreaker docs: liquidity and cash flow",
            weaviate_graphql(
                "{ Get { BenchmarkDocument(nearText: {concepts: [\"liquidity cash flow operating activities capital resources\"]}, limit: 10) { company_name company_symbol report_year page_number question context_snippet } } }",
            ),
        ),
        (
            "Stonebreaker docs: revenue growth and margins",
            weaviate_graphql(
                "{ Get { BenchmarkDocument(nearText: {concepts: [\"revenue growth gross profit margin segment performance\"]}, limit: 10) { company_name company_symbol report_year page_number question context_snippet } } }",
            ),
        ),
        (
            "Stonebreaker docs: debt and financing risk",
            weaviate_graphql(
                "{ Get { BenchmarkDocument(nearText: {concepts: [\"debt financing revolving credit facility repayment risk\"]}, limit: 10) { company_name company_symbol report_year page_number question context_snippet } } }",
            ),
        ),
    ]
}
