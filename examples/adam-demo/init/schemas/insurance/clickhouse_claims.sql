-- Insurance vertical: Claims Analytics silo
-- Source: nateraw/us-accidents (2.85M US traffic accidents)

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.accidents (
    accident_id    String,
    severity       UInt8,              -- 1 to 4
    start_time     DateTime,
    end_time       Nullable(DateTime),
    start_lat      Float64,
    start_lng      Float64,
    distance_mi    Float32,
    description    String,
    street         String,
    city           String,
    county         String,
    state          String,
    zipcode        String,
    country        String,
    timezone       String,
    temperature_f  Nullable(Float32),
    humidity_pct   Nullable(Float32),
    pressure_in    Nullable(Float32),
    visibility_mi  Nullable(Float32),
    wind_direction String,
    wind_speed_mph Nullable(Float32),
    precipitation_in Nullable(Float32),
    weather_condition String,
    amenity        UInt8,
    bump           UInt8,
    crossing       UInt8,
    give_way       UInt8,
    junction       UInt8,
    no_exit        UInt8,
    railway        UInt8,
    roundabout     UInt8,
    station        UInt8,
    stop           UInt8,
    traffic_calming UInt8,
    traffic_signal UInt8,
    turning_loop   UInt8,
    sunrise_sunset String,
    event_day      UInt32
) ENGINE = MergeTree()
ORDER BY (state, event_day, severity)
PARTITION BY state;

-- Daily severity summary
CREATE TABLE IF NOT EXISTS analytics.daily_severity (
    event_day      UInt32,
    state          String,
    severity       UInt8,
    incident_count UInt64,
    avg_distance   Float32,
    avg_temperature Nullable(Float32)
) ENGINE = SummingMergeTree()
ORDER BY (event_day, state, severity);
