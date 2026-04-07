use serde_json::{Value, json};

use super::{
    Query, ch_query, mongo_aggregate, mongo_find, pg_query, redis_get, redis_hgetall,
    redis_smembers, redis_zrevrange, weaviate_graphql,
};

/// Route queries by endpoint name for the tech vertical.
pub fn queries_for(endpoint_name: &str) -> Vec<Query> {
    match endpoint_name {
        "pg_network_security" => pg_network_security_queries(),
        "pg_saas_billing" => pg_saas_billing_queries(),
        "ch_user_events" => clickhouse_event_queries(),
        "mongo_cve" => mongo_cve_queries(),
        "redis_sessions" => redis_session_queries(),
        "weaviate_logs" => weaviate_log_queries(),
        _ => pg_network_security_queries(),
    }
}

pub fn cross_db_queries() -> Vec<Vec<(&'static str, &'static str, Value)>> {
    vec![
        // Cross-silo: Incident response — trace attack IPs to affected orgs
        // Correlation key: srcip (network) → ip_orgs:{ip} (redis) → org_id → billing (pg)
        vec![
            (
                "pg_network_security",
                "Top attack source IPs targeting our network",
                pg_query(
                    "SELECT srcip, COUNT(*) as attacks, \
                     COUNT(DISTINCT dstip) as targets, \
                     array_agg(DISTINCT attack_cat) as attack_types \
                     FROM network_flows WHERE label = 1 \
                     GROUP BY srcip ORDER BY attacks DESC LIMIT 10",
                ),
            ),
            (
                "redis_sessions",
                "Orgs with sessions from attack IP range (10.x.x.x)",
                redis_smembers("ip_orgs:10.1.1.1"),
            ),
            (
                "pg_saas_billing",
                "Potentially compromised organizations",
                pg_query(
                    "SELECT o.org_id, o.org_name, o.industry, o.size_tier, o.arr, \
                     u.email, u.role, u.last_login \
                     FROM organizations o JOIN users u ON o.org_id = u.org_id \
                     WHERE u.status = 'active' \
                     ORDER BY o.arr DESC LIMIT 20",
                ),
            ),
        ],
        // Cross-silo: Billing + API usage + platform health
        // Correlation key: org_id (billing) → org_sessions:{org_id} (redis) → api_usage (pg)
        vec![
            (
                "pg_saas_billing",
                "Orgs by ARR with API usage",
                pg_query(
                    "SELECT o.org_id, o.org_name, o.size_tier, \
                     ROUND(o.arr::numeric, 2) as arr, \
                     SUM(u.total_requests) as api_calls_30d, \
                     ROUND(AVG(u.avg_latency_ms)::numeric, 1) as avg_latency \
                     FROM organizations o \
                     JOIN api_usage_daily u ON o.org_id = u.org_id \
                     GROUP BY o.org_id, o.org_name, o.size_tier, o.arr \
                     ORDER BY arr DESC LIMIT 20",
                ),
            ),
            (
                "redis_sessions",
                "Active session count for top org",
                redis_get("org_sessions:1"),
            ),
            (
                "ch_user_events",
                "Product engagement by org (linked via org_id)",
                ch_query(
                    "SELECT org_id, count() as events, \
                     uniq(user_id) as unique_users, \
                     uniq(product_id) as products, \
                     countIf(event_type = 'purchase') as purchases, \
                     round(sumIf(price, event_type = 'purchase'), 2) as revenue \
                     FROM analytics.user_events \
                     GROUP BY org_id ORDER BY revenue DESC LIMIT 20",
                ),
            ),
        ],
        // Cross-silo: Vulnerability triage — CVE severity vs active attacks vs affected services
        // Correlation key: attack_cat (network) → CWE category (CVE) — both describe attack types
        vec![
            (
                "pg_network_security",
                "Active attacks by category with alert severity",
                pg_query(
                    "SELECT a.attack_cat, a.severity, COUNT(*) as alerts, \
                     COUNT(DISTINCT f.srcip) as unique_sources \
                     FROM security_alerts a \
                     JOIN network_flows f ON f.attack_cat = a.attack_cat \
                     WHERE a.status = 'open' AND f.label = 1 \
                     GROUP BY a.attack_cat, a.severity ORDER BY alerts DESC LIMIT 15",
                ),
            ),
            (
                "mongo_cve",
                "Critical CVEs matching active attack patterns",
                mongo_aggregate(
                    "issues",
                    "cves",
                    json!([
                        {"$match": {"severity": {"$in": ["CRITICAL", "HIGH"]}}},
                        {"$group": {"_id": "$cwe_id", "count": {"$sum": 1},
                                    "max_cvss": {"$max": "$cvss_v3"},
                                    "sample_cve": {"$first": "$cve_id"}}},
                        {"$sort": {"count": -1}},
                        {"$limit": 10}
                    ]),
                ),
            ),
            (
                "weaviate_logs",
                "Semantic search: vulnerabilities matching current attacks",
                weaviate_graphql(
                    "{ Get { Vulnerability(nearText: {concepts: [\"remote code execution network intrusion exploit\"]}, limit: 5) { cve_id severity description } } }",
                ),
            ),
        ],
    ]
}

// ─── Postgres #1: Network Security (UNSW-NB15) ─────────────────

fn pg_network_security_queries() -> Vec<Query> {
    vec![
        (
            "Attack category distribution",
            pg_query(
                "SELECT attack_cat, COUNT(*) as flows, \
                 ROUND(AVG(sbytes + dbytes)::numeric, 0) as avg_bytes \
                 FROM network_flows GROUP BY attack_cat ORDER BY flows DESC",
            ),
        ),
        (
            "Top source IPs by attack volume",
            pg_query(
                "SELECT srcip, COUNT(*) as attacks, \
                 COUNT(DISTINCT dstip) as targets \
                 FROM network_flows WHERE label = 1 \
                 GROUP BY srcip ORDER BY attacks DESC LIMIT 20",
            ),
        ),
        (
            "Service breakdown for attacks",
            pg_query(
                "SELECT service, attack_cat, COUNT(*) as flows \
                 FROM network_flows WHERE label = 1 AND service != '' \
                 GROUP BY service, attack_cat ORDER BY flows DESC LIMIT 20",
            ),
        ),
        (
            "Protocol distribution",
            pg_query(
                "SELECT proto, label, COUNT(*) as flows, \
                 ROUND(AVG(dur)::numeric, 4) as avg_duration_sec \
                 FROM network_flows GROUP BY proto, label ORDER BY flows DESC LIMIT 20",
            ),
        ),
        (
            "High-bandwidth flows",
            pg_query(
                "SELECT srcip, dstip, proto, service, sbytes, dbytes, \
                 dur, attack_cat FROM network_flows \
                 WHERE sbytes + dbytes > 100000 \
                 ORDER BY sbytes + dbytes DESC LIMIT 20",
            ),
        ),
        (
            "Connection state analysis",
            pg_query(
                "SELECT state, label, COUNT(*) as flows, \
                 ROUND(AVG(sttl)::numeric, 1) as avg_src_ttl \
                 FROM network_flows GROUP BY state, label ORDER BY flows DESC LIMIT 20",
            ),
        ),
        (
            "Alert severity breakdown",
            pg_query(
                "SELECT severity, status, COUNT(*) as alerts \
                 FROM security_alerts GROUP BY severity, status ORDER BY alerts DESC",
            ),
        ),
        (
            "Top targeted destination ports",
            pg_query(
                "SELECT dsport, proto, COUNT(*) as attacks, \
                 COUNT(DISTINCT srcip) as unique_sources \
                 FROM network_flows WHERE label = 1 \
                 GROUP BY dsport, proto ORDER BY attacks DESC LIMIT 20",
            ),
        ),
        (
            "Reconnaissance vs exploit flows",
            pg_query(
                "SELECT attack_cat, \
                 ROUND(AVG(spkts)::numeric, 1) as avg_src_pkts, \
                 ROUND(AVG(dpkts)::numeric, 1) as avg_dst_pkts, \
                 ROUND(AVG(sload)::numeric, 2) as avg_src_load \
                 FROM network_flows WHERE label = 1 \
                 GROUP BY attack_cat ORDER BY avg_src_pkts DESC",
            ),
        ),
        (
            "Jitter anomalies",
            pg_query(
                "SELECT attack_cat, \
                 ROUND(AVG(sjit)::numeric, 4) as avg_src_jitter, \
                 ROUND(AVG(djit)::numeric, 4) as avg_dst_jitter, \
                 ROUND(AVG(tcprtt)::numeric, 4) as avg_rtt \
                 FROM network_flows WHERE label = 1 \
                 GROUP BY attack_cat ORDER BY avg_rtt DESC",
            ),
        ),
    ]
}

// ─── Postgres #2: SaaS Billing ──────────────────────────────────

fn pg_saas_billing_queries() -> Vec<Query> {
    vec![
        (
            "Revenue by industry",
            pg_query(
                "SELECT industry, COUNT(*) as orgs, \
                 ROUND(SUM(mrr)::numeric, 2) as total_mrr, \
                 ROUND(AVG(health_score)::numeric, 1) as avg_health \
                 FROM organizations GROUP BY industry ORDER BY total_mrr DESC",
            ),
        ),
        (
            "Subscription status breakdown",
            pg_query(
                "SELECT s.status, p.plan_name, COUNT(*) as subs \
                 FROM subscriptions s JOIN plans p ON s.plan_id = p.plan_id \
                 GROUP BY s.status, p.plan_name ORDER BY subs DESC",
            ),
        ),
        (
            "Invoice collection status",
            pg_query(
                "SELECT status, COUNT(*) as invoices, \
                 ROUND(SUM(total)::numeric, 2) as total_amount, \
                 ROUND(AVG(total)::numeric, 2) as avg_invoice \
                 FROM invoices GROUP BY status ORDER BY total_amount DESC",
            ),
        ),
        (
            "Payment method success rates",
            pg_query(
                "SELECT method, status, COUNT(*) as payments, \
                 ROUND(SUM(amount)::numeric, 2) as total \
                 FROM payments GROUP BY method, status ORDER BY total DESC",
            ),
        ),
        (
            "Top organizations by ARR",
            pg_query(
                "SELECT org_name, industry, size_tier, \
                 ROUND(arr::numeric, 2) as arr, health_score \
                 FROM organizations ORDER BY arr DESC LIMIT 20",
            ),
        ),
        (
            "API usage top consumers",
            pg_query(
                "SELECT o.org_name, o.size_tier, \
                 SUM(u.total_requests) as total_requests, \
                 ROUND(AVG(u.avg_latency_ms)::numeric, 1) as avg_latency \
                 FROM api_usage_daily u JOIN organizations o ON u.org_id = o.org_id \
                 GROUP BY o.org_name, o.size_tier ORDER BY total_requests DESC LIMIT 20",
            ),
        ),
        (
            "Churn risk: low health + high ARR",
            pg_query(
                "SELECT org_name, industry, arr, health_score \
                 FROM organizations WHERE health_score < 30 AND arr > 1000 \
                 ORDER BY arr DESC LIMIT 20",
            ),
        ),
        (
            "User role distribution",
            pg_query(
                "SELECT role, status, COUNT(*) as users \
                 FROM users GROUP BY role, status ORDER BY users DESC",
            ),
        ),
        (
            "API key activity",
            pg_query(
                "SELECT k.status, COUNT(*) as keys, \
                 COUNT(DISTINCT k.org_id) as orgs \
                 FROM api_keys k GROUP BY k.status ORDER BY keys DESC",
            ),
        ),
        (
            "Monthly billing trend",
            pg_query(
                "SELECT DATE_TRUNC('month', created_at) as month, \
                 COUNT(*) as invoices, ROUND(SUM(total)::numeric, 2) as revenue \
                 FROM invoices WHERE status = 'paid' \
                 GROUP BY month ORDER BY month DESC LIMIT 12",
            ),
        ),
    ]
}

// ─── ClickHouse: User Behavior Events ───────────────────────────

fn clickhouse_event_queries() -> Vec<Query> {
    vec![
        (
            "Event type distribution",
            ch_query(
                "SELECT event_type, count() as events, \
                 uniq(user_id) as unique_users \
                 FROM analytics.user_events GROUP BY event_type ORDER BY events DESC",
            ),
        ),
        (
            "Top categories by revenue",
            ch_query(
                "SELECT category_code, count() as purchases, \
                 round(sum(price), 2) as revenue, uniq(user_id) as buyers \
                 FROM analytics.user_events WHERE event_type = 'purchase' \
                 GROUP BY category_code ORDER BY revenue DESC LIMIT 20",
            ),
        ),
        (
            "Hourly event volume",
            ch_query(
                "SELECT toHour(event_time) as hour, event_type, count() as events \
                 FROM analytics.user_events GROUP BY hour, event_type ORDER BY hour, events DESC",
            ),
        ),
        (
            "Top brands by engagement",
            ch_query(
                "SELECT brand, \
                 countIf(event_type = 'view') as views, \
                 countIf(event_type = 'cart') as carts, \
                 countIf(event_type = 'purchase') as purchases \
                 FROM analytics.user_events WHERE brand != '' \
                 GROUP BY brand ORDER BY views DESC LIMIT 20",
            ),
        ),
        (
            "Conversion funnel",
            ch_query(
                "SELECT event_day, \
                 uniqIf(user_id, event_type = 'view') as viewers, \
                 uniqIf(user_id, event_type = 'cart') as carters, \
                 uniqIf(user_id, event_type = 'purchase') as buyers \
                 FROM analytics.user_events GROUP BY event_day ORDER BY event_day DESC LIMIT 30",
            ),
        ),
        (
            "Session depth analysis",
            ch_query(
                "SELECT user_session, count() as events, \
                 uniq(product_id) as products_seen, \
                 max(price) as max_price \
                 FROM analytics.user_events GROUP BY user_session \
                 ORDER BY events DESC LIMIT 20",
            ),
        ),
        (
            "Price distribution of purchases",
            ch_query(
                "SELECT round(price, -1) as price_bucket, count() as purchases \
                 FROM analytics.user_events WHERE event_type = 'purchase' \
                 GROUP BY price_bucket ORDER BY price_bucket LIMIT 30",
            ),
        ),
        (
            "Cart abandonment rate by category",
            ch_query(
                "SELECT category_code, \
                 countIf(event_type = 'cart') as cart_adds, \
                 countIf(event_type = 'purchase') as purchases, \
                 round(countIf(event_type = 'purchase') / countIf(event_type = 'cart'), 3) as conversion \
                 FROM analytics.user_events WHERE category_code != '' \
                 GROUP BY category_code HAVING cart_adds > 100 \
                 ORDER BY conversion DESC LIMIT 20",
            ),
        ),
    ]
}

// ─── MongoDB: CVE Vulnerabilities ───────────────────────────────

fn mongo_cve_queries() -> Vec<Query> {
    vec![
        (
            "CVEs by severity",
            mongo_aggregate(
                "issues",
                "cves",
                json!([
                    {"$group": {"_id": "$severity", "count": {"$sum": 1},
                                "avg_cvss": {"$avg": "$cvss_v3"}}},
                    {"$sort": {"count": -1}}
                ]),
            ),
        ),
        (
            "CVEs by CWE category",
            mongo_aggregate(
                "issues",
                "cves",
                json!([
                    {"$group": {"_id": "$cwe_id", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 20}
                ]),
            ),
        ),
        (
            "Critical CVEs with highest CVSS",
            mongo_find("issues", "cves", json!({"severity": "CRITICAL"})),
        ),
        (
            "CVE trend by year",
            mongo_aggregate(
                "issues",
                "cves",
                json!([
                    {"$group": {"_id": "$year", "count": {"$sum": 1},
                                "critical": {"$sum": {"$cond": [{"$eq": ["$severity", "CRITICAL"]}, 1, 0]}},
                                "high": {"$sum": {"$cond": [{"$eq": ["$severity", "HIGH"]}, 1, 0]}}}},
                    {"$sort": {"_id": -1}},
                    {"$limit": 25}
                ]),
            ),
        ),
        (
            "Top CWEs for critical vulnerabilities",
            mongo_aggregate(
                "issues",
                "cves",
                json!([
                    {"$match": {"severity": "CRITICAL"}},
                    {"$group": {"_id": "$cwe_id", "count": {"$sum": 1},
                                "avg_cvss": {"$avg": "$cvss_v3"}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 15}
                ]),
            ),
        ),
    ]
}

// ─── Redis: Sessions & Platform ─────────────────────────────────

fn redis_session_queries() -> Vec<Query> {
    vec![
        ("Total active sessions", redis_get("stats:active_sessions")),
        ("Total organizations", redis_get("stats:total_orgs")),
        (
            "Total API calls today",
            redis_get("stats:total_api_calls_today"),
        ),
        (
            "Feature flag: AI assistant",
            redis_get("feature:ai_assistant"),
        ),
        ("Feature flag: API v2", redis_get("feature:api_v2")),
        (
            "Top orgs by MRR",
            redis_zrevrange("leaderboard:org_mrr", 0, 9),
        ),
        ("Sample session lookup", redis_hgetall("session:1")),
        ("Rate limit check", redis_get("rate_limit:1")),
    ]
}

// ─── Weaviate: Vulnerability Search ─────────────────────────────

fn weaviate_log_queries() -> Vec<Query> {
    vec![
        (
            "Search: SQL injection vulnerabilities",
            weaviate_graphql(
                "{ Get { Vulnerability(nearText: {concepts: [\"SQL injection database query manipulation\"]}, limit: 10) { cve_id severity description } } }",
            ),
        ),
        (
            "Search: remote code execution",
            weaviate_graphql(
                "{ Get { Vulnerability(nearText: {concepts: [\"remote code execution arbitrary command RCE\"]}, limit: 10) { cve_id severity description } } }",
            ),
        ),
        (
            "Search: buffer overflow",
            weaviate_graphql(
                "{ Get { Vulnerability(nearText: {concepts: [\"buffer overflow memory corruption heap stack\"]}, limit: 10) { cve_id severity description } } }",
            ),
        ),
        (
            "Search: authentication bypass",
            weaviate_graphql(
                "{ Get { Vulnerability(nearText: {concepts: [\"authentication bypass unauthorized access privilege escalation\"]}, limit: 10) { cve_id severity description } } }",
            ),
        ),
        (
            "Search: cross-site scripting",
            weaviate_graphql(
                "{ Get { Vulnerability(nearText: {concepts: [\"cross-site scripting XSS HTML injection\"]}, limit: 10) { cve_id severity description } } }",
            ),
        ),
    ]
}
