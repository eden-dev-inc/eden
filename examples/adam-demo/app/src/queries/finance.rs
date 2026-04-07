use serde_json::{Value, json};

use super::{
    Query, ch_query, mongo_aggregate, pg_query, redis_get, redis_hgetall, redis_zrevrange,
    weaviate_graphql,
};

pub fn queries_for(endpoint_name: &str) -> Vec<Query> {
    match endpoint_name {
        "pg_core_banking" => vec![
            (
                "Transaction volume by type",
                pg_query(
                    "SELECT type, COUNT(*) as txns, ROUND(SUM(amount)::numeric, 2) as total \
                     FROM transactions GROUP BY type ORDER BY txns DESC",
                ),
            ),
            (
                "Fraud detection summary",
                pg_query(
                    "SELECT is_fraud, COUNT(*) as txns, ROUND(SUM(amount)::numeric, 2) as total \
                     FROM transactions GROUP BY is_fraud ORDER BY txns DESC",
                ),
            ),
            (
                "Average transaction amount by type",
                pg_query(
                    "SELECT type, ROUND(AVG(amount)::numeric, 2) as avg_amount, \
                     ROUND(STDDEV(amount)::numeric, 2) as stddev_amount, COUNT(*) as txns \
                     FROM transactions GROUP BY type ORDER BY avg_amount DESC",
                ),
            ),
            (
                "Flagged fraud vs actual fraud accuracy",
                pg_query(
                    "SELECT is_flagged_fraud, is_fraud, COUNT(*) as txns, \
                     ROUND(SUM(amount)::numeric, 2) as total_amount \
                     FROM transactions GROUP BY is_flagged_fraud, is_fraud ORDER BY txns DESC",
                ),
            ),
            (
                "Top 20 largest transactions",
                pg_query(
                    "SELECT txn_id, type, amount, name_orig, name_dest, is_fraud \
                     FROM transactions ORDER BY amount DESC LIMIT 20",
                ),
            ),
            (
                "Balance change anomalies (sender balance mismatch)",
                pg_query(
                    "SELECT type, COUNT(*) as anomalies, ROUND(AVG(amount)::numeric, 2) as avg_amount \
                     FROM transactions \
                     WHERE ABS(oldbalance_org - newbalance_org) != amount AND amount > 0 \
                     GROUP BY type ORDER BY anomalies DESC",
                ),
            ),
            (
                "High-value transfers over 500K",
                pg_query(
                    "SELECT txn_id, type, amount, name_orig, name_dest, \
                     oldbalance_org, newbalance_org, is_fraud \
                     FROM transactions WHERE amount > 500000 ORDER BY amount DESC LIMIT 30",
                ),
            ),
            (
                "Fraud rate by transaction step (time period)",
                pg_query(
                    "SELECT step, COUNT(*) as total_txns, \
                     SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_txns, \
                     ROUND(100.0 * SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 2) as fraud_pct \
                     FROM transactions GROUP BY step ORDER BY step LIMIT 50",
                ),
            ),
        ],
        "pg_credit_scoring" => vec![
            (
                "Transactions by category",
                pg_query(
                    "SELECT category, COUNT(*) as txns, ROUND(AVG(amt)::numeric, 2) as avg_amount \
                     FROM credit_transactions GROUP BY category ORDER BY txns DESC LIMIT 20",
                ),
            ),
            (
                "Fraud rate by merchant",
                pg_query(
                    "SELECT merchant, COUNT(*) as txns, \
                     SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as frauds \
                     FROM credit_transactions GROUP BY merchant ORDER BY frauds DESC LIMIT 20",
                ),
            ),
            (
                "Geographic fraud hotspots by state",
                pg_query(
                    "SELECT state, COUNT(*) as total_txns, \
                     SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count, \
                     ROUND(100.0 * SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 2) as fraud_pct, \
                     ROUND(SUM(CASE WHEN is_fraud = 1 THEN amt ELSE 0 END)::numeric, 2) as fraud_amount \
                     FROM credit_transactions GROUP BY state ORDER BY fraud_count DESC LIMIT 20",
                ),
            ),
            (
                "Time-of-day fraud patterns (hourly)",
                pg_query(
                    "SELECT EXTRACT(HOUR FROM trans_date_time::timestamp) as hour_of_day, \
                     COUNT(*) as total_txns, \
                     SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_txns, \
                     ROUND(100.0 * SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 2) as fraud_pct \
                     FROM credit_transactions GROUP BY hour_of_day ORDER BY hour_of_day",
                ),
            ),
            (
                "Top spending categories by gender",
                pg_query(
                    "SELECT gender, category, COUNT(*) as txns, \
                     ROUND(SUM(amt)::numeric, 2) as total_spent, \
                     ROUND(AVG(amt)::numeric, 2) as avg_txn \
                     FROM credit_transactions GROUP BY gender, category ORDER BY total_spent DESC LIMIT 30",
                ),
            ),
            (
                "Age cohort fraud analysis",
                pg_query(
                    "SELECT CASE \
                     WHEN EXTRACT(YEAR FROM AGE(NOW(), dob::date)) < 30 THEN 'Under 30' \
                     WHEN EXTRACT(YEAR FROM AGE(NOW(), dob::date)) < 45 THEN '30-44' \
                     WHEN EXTRACT(YEAR FROM AGE(NOW(), dob::date)) < 60 THEN '45-59' \
                     ELSE '60+' END as age_group, \
                     COUNT(*) as total_txns, \
                     SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_txns, \
                     ROUND(AVG(amt)::numeric, 2) as avg_amount \
                     FROM credit_transactions GROUP BY age_group ORDER BY age_group",
                ),
            ),
            (
                "Merchant distance from cardholder (potential card-not-present fraud)",
                pg_query(
                    "SELECT category, is_fraud, \
                     ROUND(AVG(SQRT(POWER(lat - merch_lat, 2) + POWER(long - merch_long, 2)))::numeric, 4) as avg_distance, \
                     COUNT(*) as txns \
                     FROM credit_transactions GROUP BY category, is_fraud \
                     HAVING COUNT(*) > 10 ORDER BY avg_distance DESC LIMIT 20",
                ),
            ),
            (
                "High-value transactions above $500",
                pg_query(
                    "SELECT trans_num, merchant, category, amt, first_name, last_name, \
                     city, state, is_fraud \
                     FROM credit_transactions WHERE amt > 500 ORDER BY amt DESC LIMIT 30",
                ),
            ),
        ],
        "ch_trading" => vec![
            (
                "Daily trading volume",
                ch_query(
                    "SELECT toDate(trade_time) as trade_date, count() as bars, sum(volume) as total_volume \
                     FROM analytics.stock_bars GROUP BY trade_date ORDER BY trade_date DESC LIMIT 30",
                ),
            ),
            (
                "Intraday volatility by symbol (high-low spread)",
                ch_query(
                    "SELECT symbol, toDate(trade_time) as trade_date, \
                     round(max(high) - min(low), 4) as daily_range, \
                     round(avg(high - low), 4) as avg_bar_spread, \
                     count() as bars \
                     FROM analytics.stock_bars \
                     GROUP BY symbol, trade_date ORDER BY daily_range DESC LIMIT 30",
                ),
            ),
            (
                "Top movers by daily percentage change",
                ch_query(
                    "SELECT symbol, trade_date, \
                     round((close - open) / open * 100, 2) as pct_change, \
                     total_volume \
                     FROM analytics.daily_ohlcv \
                     WHERE open > 0 \
                     ORDER BY abs((close - open) / open) DESC LIMIT 30",
                ),
            ),
            (
                "Volume-weighted average price (VWAP) by symbol",
                ch_query(
                    "SELECT symbol, toDate(trade_time) as trade_date, \
                     round(sum(close * volume) / sum(volume), 4) as vwap, \
                     sum(volume) as total_volume \
                     FROM analytics.stock_bars \
                     WHERE volume > 0 \
                     GROUP BY symbol, trade_date ORDER BY trade_date DESC, total_volume DESC LIMIT 30",
                ),
            ),
            (
                "Most actively traded symbols",
                ch_query(
                    "SELECT symbol, sum(total_volume) as cumulative_volume, \
                     count() as trading_days, \
                     round(avg(total_volume), 0) as avg_daily_volume \
                     FROM analytics.daily_ohlcv \
                     GROUP BY symbol ORDER BY cumulative_volume DESC LIMIT 20",
                ),
            ),
            (
                "Hourly trading activity heatmap",
                ch_query(
                    "SELECT toHour(trade_time) as hour, \
                     count() as bar_count, sum(volume) as total_volume, \
                     round(avg(close - open), 4) as avg_price_move \
                     FROM analytics.stock_bars \
                     GROUP BY hour ORDER BY hour",
                ),
            ),
            (
                "Daily OHLCV summary with spread analysis",
                ch_query(
                    "SELECT symbol, trade_date, open, high, low, close, total_volume, \
                     round(high - low, 4) as daily_spread, \
                     round(avg_spread, 4) as avg_bar_spread, bar_count \
                     FROM analytics.daily_ohlcv ORDER BY trade_date DESC LIMIT 30",
                ),
            ),
            (
                "Symbols with highest average spread (most volatile)",
                ch_query(
                    "SELECT symbol, \
                     round(avg(avg_spread), 4) as mean_spread, \
                     round(max(high - low), 4) as max_daily_range, \
                     sum(total_volume) as total_vol \
                     FROM analytics.daily_ohlcv \
                     GROUP BY symbol ORDER BY mean_spread DESC LIMIT 20",
                ),
            ),
        ],
        "mongo_compliance" => vec![
            (
                "SEC filings by year",
                mongo_aggregate(
                    "compliance",
                    "sec_filings",
                    json!([
                        {"$group": {"_id": "$filing_year", "count": {"$sum": 1}}},
                        {"$sort": {"_id": -1}}, {"$limit": 20}
                    ]),
                ),
            ),
            (
                "Top companies by filing count",
                mongo_aggregate(
                    "compliance",
                    "sec_filings",
                    json!([
                        {"$group": {"_id": "$company_name", "filings": {"$sum": 1}, "avg_word_count": {"$avg": "$word_count"}}},
                        {"$sort": {"filings": -1}}, {"$limit": 20}
                    ]),
                ),
            ),
            (
                "Filing size analysis (word count distribution)",
                mongo_aggregate(
                    "compliance",
                    "sec_filings",
                    json!([
                        {"$bucket": {
                            "groupBy": "$word_count",
                            "boundaries": [0, 1000, 5000, 10000, 50000, 100000, 500000],
                            "default": "500000+",
                            "output": {"count": {"$sum": 1}, "avg_chars": {"$avg": "$character_count"}}
                        }}
                    ]),
                ),
            ),
            (
                "Search filings mentioning risk factors",
                mongo_aggregate(
                    "compliance",
                    "sec_filings",
                    json!([
                        {"$match": {"$text": {"$search": "risk factor material adverse"}}},
                        {"$project": {"company_name": 1, "filing_year": 1, "word_count": 1, "score": {"$meta": "textScore"}}},
                        {"$sort": {"score": -1}}, {"$limit": 20}
                    ]),
                ),
            ),
            (
                "Average filing length trend by year",
                mongo_aggregate(
                    "compliance",
                    "sec_filings",
                    json!([
                        {"$group": {"_id": "$filing_year",
                            "avg_word_count": {"$avg": "$word_count"},
                            "avg_char_count": {"$avg": "$character_count"},
                            "total_filings": {"$sum": 1}}},
                        {"$sort": {"_id": 1}}
                    ]),
                ),
            ),
            (
                "Longest SEC filings (most detailed disclosures)",
                mongo_aggregate(
                    "compliance",
                    "sec_filings",
                    json!([
                        {"$sort": {"word_count": -1}},
                        {"$limit": 15},
                        {"$project": {"company_name": 1, "filing_year": 1, "word_count": 1, "character_count": 1}}
                    ]),
                ),
            ),
            (
                "Filings per company per year (activity matrix)",
                mongo_aggregate(
                    "compliance",
                    "sec_filings",
                    json!([
                        {"$group": {"_id": {"company": "$company_name", "year": "$filing_year"}, "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}}, {"$limit": 30}
                    ]),
                ),
            ),
        ],
        "redis_fraud" => vec![
            ("Total transactions", redis_get("stats:total_transactions")),
            ("Fraud score lookup", redis_get("fraud_score:1")),
            ("Customer profile lookup", redis_hgetall("customer:5")),
            ("Account balance check", redis_get("balance:10")),
            (
                "Reverse lookup: bank account to customer",
                redis_get("account_customer:C100"),
            ),
            (
                "Total credit card transactions",
                redis_get("stats:total_credit_txns"),
            ),
            ("Overall fraud rate", redis_get("stats:fraud_rate")),
            (
                "Total registered customers",
                redis_get("stats:total_customers"),
            ),
            (
                "Top 10 transactors leaderboard",
                redis_zrevrange("leaderboard:top_transactors", 0, 9),
            ),
            (
                "Credit card to customer mapping",
                redis_get("cc_customer:4017277031352640"),
            ),
        ],
        "weaviate_risk" => vec![
            (
                "Search: money laundering risk",
                weaviate_graphql(
                    "{ Get { ComplianceDocument(nearText: {concepts: [\"money laundering suspicious activity\"]}, limit: 10) { company_name filing_year text_snippet } } }",
                ),
            ),
            (
                "Search: executive compensation and governance",
                weaviate_graphql(
                    "{ Get { ComplianceDocument(nearText: {concepts: [\"executive compensation corporate governance board of directors\"]}, limit: 10) { company_name filing_year text_snippet } } }",
                ),
            ),
            (
                "Search: cybersecurity and data breach risk",
                weaviate_graphql(
                    "{ Get { ComplianceDocument(nearText: {concepts: [\"cybersecurity data breach information security vulnerability\"]}, limit: 10) { company_name filing_year text_snippet } } }",
                ),
            ),
            (
                "Search: environmental and climate risk disclosures",
                weaviate_graphql(
                    "{ Get { ComplianceDocument(nearText: {concepts: [\"climate change environmental risk sustainability carbon emissions\"]}, limit: 10) { company_name filing_year text_snippet } } }",
                ),
            ),
            (
                "Search: merger and acquisition activity",
                weaviate_graphql(
                    "{ Get { ComplianceDocument(nearText: {concepts: [\"merger acquisition takeover business combination\"]}, limit: 10) { company_name filing_year text_snippet } } }",
                ),
            ),
            (
                "Search: debt obligations and liquidity risk",
                weaviate_graphql(
                    "{ Get { ComplianceDocument(nearText: {concepts: [\"debt obligation liquidity credit facility loan default\"]}, limit: 10) { company_name filing_year text_snippet } } }",
                ),
            ),
            (
                "Search: intellectual property and patent disputes",
                weaviate_graphql(
                    "{ Get { ComplianceDocument(nearText: {concepts: [\"intellectual property patent infringement trade secret litigation\"]}, limit: 10) { company_name filing_year text_snippet } } }",
                ),
            ),
        ],
        _ => vec![],
    }
}

pub fn cross_db_queries() -> Vec<Vec<(&'static str, &'static str, Value)>> {
    vec![
        // Cross-silo: Fraud pattern analysis across banking + credit + real-time scores
        // Correlation: fraud patterns by type (core) vs by merchant/category (credit) vs live scores (redis)
        vec![
            (
                "pg_core_banking",
                "Fraud by transaction type and amount range",
                pg_query(
                    "SELECT type, is_fraud, COUNT(*) as txns, \
                     ROUND(AVG(amount)::numeric, 2) as avg_amount, \
                     ROUND(MAX(amount)::numeric, 2) as max_amount \
                     FROM transactions WHERE is_fraud = 1 \
                     GROUP BY type, is_fraud ORDER BY txns DESC",
                ),
            ),
            (
                "pg_credit_scoring",
                "Credit card fraud hotspots by state and category",
                pg_query(
                    "SELECT state, category, COUNT(*) as txns, \
                     SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as frauds, \
                     ROUND(SUM(CASE WHEN is_fraud = 1 THEN amt ELSE 0 END)::numeric, 2) as fraud_amount \
                     FROM credit_transactions \
                     GROUP BY state, category \
                     HAVING SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) > 0 \
                     ORDER BY frauds DESC LIMIT 20",
                ),
            ),
            (
                "redis_fraud",
                "Real-time fraud rate and top transactor scores",
                redis_get("stats:fraud_rate"),
            ),
            (
                "redis_fraud",
                "Customer lookup by bank account (reverse index)",
                redis_get("account_customer:C42"),
            ),
        ],
        // Cross-silo: Market activity vs compliance risk
        // Correlation: trading volume trends (CH) vs SEC filing activity (Mongo) vs compliance search (Weaviate)
        vec![
            (
                "ch_trading",
                "Trading volume and volatility by symbol",
                ch_query(
                    "SELECT symbol, count() as bars, \
                     round(avg(close), 2) as avg_price, \
                     round(max(high) - min(low), 2) as price_range, \
                     sum(volume) as total_volume \
                     FROM analytics.stock_bars \
                     GROUP BY symbol ORDER BY total_volume DESC LIMIT 20",
                ),
            ),
            (
                "mongo_compliance",
                "SEC filing volume by year and company",
                mongo_aggregate(
                    "compliance",
                    "sec_filings",
                    json!([
                        {"$group": {"_id": {"year": "$filing_year", "company": "$company_name"},
                                    "filings": {"$sum": 1}}},
                        {"$sort": {"filings": -1}},
                        {"$limit": 20}
                    ]),
                ),
            ),
            (
                "weaviate_risk",
                "Search: insider trading and market manipulation",
                weaviate_graphql(
                    "{ Get { ComplianceDocument(nearText: {concepts: [\"insider trading market manipulation securities fraud\"]}, limit: 10) { company_name filing_year text_snippet } } }",
                ),
            ),
        ],
        // Cross-silo: Account risk profile across all systems
        // Correlation: high-value accounts (core) → customer linkage (redis) → credit scoring
        vec![
            (
                "pg_core_banking",
                "Highest-value fraud transactions with account details",
                pg_query(
                    "SELECT name_orig, name_dest, type, amount, \
                     oldbalance_org, newbalance_org \
                     FROM transactions WHERE is_fraud = 1 \
                     ORDER BY amount DESC LIMIT 20",
                ),
            ),
            (
                "redis_fraud",
                "Customer linkage: bank account + credit card + fraud score",
                redis_hgetall("customer:1"),
            ),
            (
                "redis_fraud",
                "Top transactors leaderboard",
                redis_zrevrange("leaderboard:top_transactors", 0, 9),
            ),
            (
                "pg_credit_scoring",
                "High-risk merchants with most fraud",
                pg_query(
                    "SELECT merchant, COUNT(*) as total_txns, \
                     SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count, \
                     ROUND(100.0 * SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / COUNT(*)::numeric, 2) as fraud_pct \
                     FROM credit_transactions \
                     GROUP BY merchant HAVING COUNT(*) > 100 \
                     ORDER BY fraud_pct DESC LIMIT 20",
                ),
            ),
        ],
    ]
}
