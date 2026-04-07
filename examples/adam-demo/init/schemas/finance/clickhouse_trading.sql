-- Finance vertical: Trading Desk silo
-- Source: Traders-Lab/TroveLedger (multi-symbol OHLCV bars)

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.stock_bars (
    symbol         String,
    trade_time     DateTime,
    open           Float64,
    high           Float64,
    low            Float64,
    close          Float64,
    volume         UInt64
) ENGINE = MergeTree()
ORDER BY (symbol, trade_time)
PARTITION BY toYYYYMM(trade_time);

-- Daily aggregates
CREATE TABLE IF NOT EXISTS analytics.daily_ohlcv (
    symbol         String,
    trade_date     Date,
    open           Float64,
    high           Float64,
    low            Float64,
    close          Float64,
    total_volume   UInt64,
    bar_count      UInt32,
    avg_spread     Float64
) ENGINE = SummingMergeTree()
ORDER BY (symbol, trade_date);
