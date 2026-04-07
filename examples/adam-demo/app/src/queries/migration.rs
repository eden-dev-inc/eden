use super::Query;
use serde_json::Value;

pub fn queries_for(endpoint_name: &str) -> Vec<Query> {
    match endpoint_name {
        "redis_source" | "redis_dest" | "redis_interlay" => redis_finance_queries(),
        _ => redis_finance_queries(),
    }
}

pub fn cross_db_queries() -> Vec<Vec<(&'static str, &'static str, Value)>> {
    // No cross-DB queries — single Redis vertical
    // But we can do multi-step workflows within Redis
    vec![
        // Workflow: Check fraud score -> look up customer -> check recent transactions
        vec![
            (
                "redis_source",
                "Top fraud risk accounts",
                super::redis_zrevrange("fraud:scores", 0, 9),
            ),
            (
                "redis_source",
                "Customer profile for high-risk account",
                super::redis_hgetall("customer:1"),
            ),
            (
                "redis_source",
                "Pending fraud alerts",
                super::redis_get("stats:pending_alerts"),
            ),
        ],
        // Workflow: Market data -> trading volume leaders -> system status
        vec![
            (
                "redis_source",
                "AAPL market data",
                super::redis_hgetall("market:AAPL"),
            ),
            (
                "redis_source",
                "Top merchants by volume",
                super::redis_zrevrange("leaderboard:merchant_volume", 0, 4),
            ),
            (
                "redis_source",
                "System status",
                super::redis_get("stats:system_status"),
            ),
        ],
    ]
}

fn redis_finance_queries() -> Vec<Query> {
    vec![
        // STRING queries — account balances & stats
        (
            "Account balance lookup",
            super::redis_get("balance:acct:42"),
        ),
        ("Total accounts", super::redis_get("stats:total_accounts")),
        (
            "Active sessions count",
            super::redis_get("stats:total_sessions"),
        ),
        (
            "Pending alerts count",
            super::redis_get("stats:pending_alerts"),
        ),
        (
            "Today's transaction volume",
            super::redis_get("stats:total_transactions_today"),
        ),
        (
            "Current fraud rate",
            super::redis_get("stats:fraud_rate_pct"),
        ),
        (
            "Average transaction amount",
            super::redis_get("stats:avg_transaction_amount"),
        ),
        (
            "System operational status",
            super::redis_get("stats:system_status"),
        ),
        (
            "Rate limit check for account",
            super::redis_get("ratelimit:acct:100"),
        ),
        // HASH queries — sessions, market data, customer profiles
        ("Session token lookup", super::redis_hgetall("session:1")),
        ("Session token #500", super::redis_hgetall("session:500")),
        ("AAPL market data", super::redis_hgetall("market:AAPL")),
        ("GOOGL market data", super::redis_hgetall("market:GOOGL")),
        ("TSLA market data", super::redis_hgetall("market:TSLA")),
        ("JPM market data", super::redis_hgetall("market:JPM")),
        ("NVDA market data", super::redis_hgetall("market:NVDA")),
        ("Customer profile #1", super::redis_hgetall("customer:1")),
        ("Customer profile #42", super::redis_hgetall("customer:42")),
        (
            "Customer profile #1000",
            super::redis_hgetall("customer:1000"),
        ),
        // SORTED SET queries — fraud scores & leaderboards
        (
            "Top 10 fraud risk accounts",
            super::redis_zrevrange("fraud:scores", 0, 9),
        ),
        (
            "Top 20 fraud risk accounts",
            super::redis_zrevrange("fraud:scores", 0, 19),
        ),
        (
            "Top 5 merchants by volume",
            super::redis_zrevrange("leaderboard:merchant_volume", 0, 4),
        ),
        (
            "Top 10 merchants by volume",
            super::redis_zrevrange("leaderboard:merchant_volume", 0, 9),
        ),
        (
            "Merchants by fraud rate",
            super::redis_zrevrange("leaderboard:merchant_fraud_rate", 0, 9),
        ),
        (
            "Top 10 account balances",
            super::redis_zrevrange("leaderboard:top_balances", 0, 9),
        ),
        (
            "Top 50 account balances",
            super::redis_zrevrange("leaderboard:top_balances", 0, 49),
        ),
    ]
}
