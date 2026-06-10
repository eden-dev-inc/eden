use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_table_inheritance_basic() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE ti_vehicle (
            id SERIAL PRIMARY KEY,
            make TEXT NOT NULL,
            model TEXT NOT NULL,
            year INT4 NOT NULL
        );
         CREATE TABLE ti_car (
            doors INT4 DEFAULT 4,
            fuel_type TEXT DEFAULT 'gas'
         ) INHERITS (ti_vehicle);
         CREATE TABLE ti_truck (
            payload_kg INT4,
            axles INT4 DEFAULT 2
         ) INHERITS (ti_vehicle)",
    )
    .await;

    ctx.execute("INSERT INTO ti_car (make, model, year, doors) VALUES ('Toyota', 'Camry', 2024, 4)", &[]).await;
    ctx.execute("INSERT INTO ti_truck (make, model, year, payload_kg) VALUES ('Ford', 'F150', 2023, 1000)", &[]).await;

    // Querying parent table returns all children
    let rows = ctx.query("SELECT make, model FROM ti_vehicle ORDER BY make", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["make"], "Ford");
    assert_eq!(arr[1]["make"], "Toyota");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_table_inheritance_only() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE ti_base (id SERIAL PRIMARY KEY, val TEXT);
         CREATE TABLE ti_derived (extra INT4) INHERITS (ti_base)",
    )
    .await;

    ctx.execute("INSERT INTO ti_base (val) VALUES ('base_only')", &[]).await;
    ctx.execute("INSERT INTO ti_derived (val, extra) VALUES ('from_derived', 42)", &[]).await;

    // ONLY keyword restricts to just the parent table
    let row = ctx.query_one("SELECT val FROM ONLY ti_base", &[]).await;
    assert_eq!(row["val"], "base_only");

    // Without ONLY, includes derived
    let rows = ctx.query("SELECT val FROM ti_base ORDER BY val", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_declarative_partitioning_range() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE ti_logs (
            id SERIAL,
            log_date DATE NOT NULL,
            message TEXT
        ) PARTITION BY RANGE (log_date);

         CREATE TABLE ti_logs_2024q1 PARTITION OF ti_logs
            FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
         CREATE TABLE ti_logs_2024q2 PARTITION OF ti_logs
            FOR VALUES FROM ('2024-04-01') TO ('2024-07-01')",
    )
    .await;

    ctx.execute(
        "INSERT INTO ti_logs (log_date, message) VALUES
         ('2024-02-15', 'Q1 entry'),
         ('2024-05-20', 'Q2 entry')",
        &[],
    )
    .await;

    // Query parent table gets all partitions
    let rows = ctx.query("SELECT message FROM ti_logs ORDER BY log_date", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["message"], "Q1 entry");
    assert_eq!(arr[1]["message"], "Q2 entry");

    // Query specific partition directly
    let row = ctx.query_one("SELECT message FROM ti_logs_2024q1", &[]).await;
    assert_eq!(row["message"], "Q1 entry");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_declarative_partitioning_list() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE ti_regions (
            id SERIAL,
            region TEXT NOT NULL,
            data TEXT
        ) PARTITION BY LIST (region);

         CREATE TABLE ti_regions_us PARTITION OF ti_regions FOR VALUES IN ('us-east', 'us-west');
         CREATE TABLE ti_regions_eu PARTITION OF ti_regions FOR VALUES IN ('eu-west', 'eu-central')",
    )
    .await;

    ctx.execute(
        "INSERT INTO ti_regions (region, data) VALUES
         ('us-east', 'NYC server'),
         ('eu-west', 'London server'),
         ('us-west', 'LA server')",
        &[],
    )
    .await;

    let rows = ctx.query("SELECT region, data FROM ti_regions ORDER BY region", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);

    // Specific partition
    let rows = ctx.query("SELECT data FROM ti_regions_us ORDER BY region", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_partition_pruning() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE ti_prune (
            id SERIAL,
            status TEXT NOT NULL,
            val INT4
        ) PARTITION BY LIST (status);

         CREATE TABLE ti_prune_active PARTITION OF ti_prune FOR VALUES IN ('active');
         CREATE TABLE ti_prune_archived PARTITION OF ti_prune FOR VALUES IN ('archived')",
    )
    .await;

    ctx.execute(
        "INSERT INTO ti_prune (status, val) VALUES
         ('active', 1), ('active', 2), ('archived', 3), ('archived', 4)",
        &[],
    )
    .await;

    // Query with partition key in WHERE should only scan relevant partition
    let rows = ctx.query("SELECT val FROM ti_prune WHERE status = 'active' ORDER BY val", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["val"], 1);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_partition_insert_fails_outside_range() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE ti_strict (
            id SERIAL,
            val INT4 NOT NULL
        ) PARTITION BY RANGE (val);

         CREATE TABLE ti_strict_low PARTITION OF ti_strict FOR VALUES FROM (0) TO (100);
         CREATE TABLE ti_strict_high PARTITION OF ti_strict FOR VALUES FROM (100) TO (200)",
    )
    .await;

    // Valid inserts
    ctx.execute("INSERT INTO ti_strict (val) VALUES (50), (150)", &[]).await;

    // Insert outside all partitions should fail
    let err = ctx.execute_err("INSERT INTO ti_strict (val) VALUES (300)", &[]).await;
    let err_str = format!("{:?}", err);
    assert!(err_str.contains("error") || err_str.contains("Error") || err_str.contains("partition"));

    ctx.stop().await;
}
