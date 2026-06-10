use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_empty_string_vs_null() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ec_empty (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute(
        "INSERT INTO ec_empty (val) VALUES ($1), ($2), ($3)",
        &[
            SqlParam::Text(String::new()),
            SqlParam::Null,
            SqlParam::Text("notempty".to_string()),
        ],
    )
    .await;

    // Empty string is NOT NULL
    let rows = ctx.query("SELECT id, val FROM ec_empty WHERE val IS NOT NULL ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2); // empty string + "notempty"

    // Empty string matches ''
    let row = ctx.query_one("SELECT id FROM ec_empty WHERE val = ''", &[]).await;
    assert_eq!(row["id"], 1);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_unicode_strings() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ec_unicode (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute(
        "INSERT INTO ec_unicode (val) VALUES ($1), ($2), ($3), ($4)",
        &[
            SqlParam::Text("\u{1F600}\u{1F4A9}\u{1F680}".to_string()),      // emoji
            SqlParam::Text("\u{4F60}\u{597D}\u{4E16}\u{754C}".to_string()), // Chinese
            SqlParam::Text("\u{0410}\u{0411}\u{0412}".to_string()),         // Cyrillic
            SqlParam::Text("caf\u{00E9}".to_string()),                      // accented
        ],
    )
    .await;

    let rows = ctx.query("SELECT val FROM ec_unicode ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 4);
    assert_eq!(arr[0]["val"], "\u{1F600}\u{1F4A9}\u{1F680}");
    assert_eq!(arr[3]["val"], "caf\u{00E9}");

    // LENGTH counts characters not bytes
    let row = ctx.query_one("SELECT LENGTH(val) AS len FROM ec_unicode WHERE id = 1", &[]).await;
    assert_eq!(row["len"], 3); // 3 emoji characters

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_very_long_string() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ec_long (id SERIAL PRIMARY KEY, val TEXT)").await;

    let long_string = "x".repeat(100_000);
    ctx.execute("INSERT INTO ec_long (val) VALUES ($1)", &[SqlParam::Text(long_string.clone())]).await;

    let row = ctx.query_one("SELECT LENGTH(val) AS len FROM ec_long WHERE id = 1", &[]).await;
    assert_eq!(row["len"], 100_000);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_integer_boundaries() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE ec_bounds (
            id SERIAL PRIMARY KEY,
            i2 INT2,
            i4 INT4,
            i8 INT8
        )",
    )
    .await;
    ctx.execute(
        "INSERT INTO ec_bounds (i2, i4, i8) VALUES ($1, $2, $3), ($4, $5, $6)",
        &[
            SqlParam::Int2(i16::MAX),
            SqlParam::Int4(i32::MAX),
            SqlParam::Int8(i64::MAX),
            SqlParam::Int2(i16::MIN),
            SqlParam::Int4(i32::MIN),
            SqlParam::Int8(i64::MIN),
        ],
    )
    .await;

    let rows = ctx.query("SELECT i2, i4, i8 FROM ec_bounds ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["i2"], i16::MAX as i64);
    assert_eq!(arr[0]["i4"], i32::MAX as i64);
    assert_eq!(arr[1]["i2"], i16::MIN as i64);
    assert_eq!(arr[1]["i4"], i32::MIN as i64);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_float_special_values() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ec_float (id SERIAL PRIMARY KEY, val FLOAT8)").await;
    ctx.execute(
        "INSERT INTO ec_float (val) VALUES ('NaN'::FLOAT8), ('Infinity'::FLOAT8), ('-Infinity'::FLOAT8), (0.0)",
        &[],
    )
    .await;

    let rows = ctx.query("SELECT val FROM ec_float ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 4);
    // NaN and Infinity are represented as null in JSON (serde_json can't represent them)
    assert!(arr[0]["val"].is_null()); // NaN
    assert!(arr[1]["val"].is_null()); // Infinity
    assert!(arr[2]["val"].is_null()); // -Infinity
    let zero = arr[3]["val"].as_f64().unwrap();
    assert!((zero - 0.0).abs() < 0.001);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_null_handling_in_expressions() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                (NULL + 5) AS null_add,
                (NULL = NULL) AS null_eq,
                (NULL IS NULL) AS is_null,
                (NULL IS DISTINCT FROM NULL) AS distinct_null,
                (1 IS DISTINCT FROM NULL) AS distinct_from_null",
            &[],
        )
        .await;
    assert!(row["null_add"].is_null());
    assert!(row["null_eq"].is_null()); // NULL = NULL is NULL, not true
    assert_eq!(row["is_null"], true);
    assert_eq!(row["distinct_null"], false); // NULL IS NOT DISTINCT FROM NULL
    assert_eq!(row["distinct_from_null"], true);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_large_number_of_rows() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ec_large (id SERIAL PRIMARY KEY, val INT4)").await;
    // Insert 1000 rows via generate_series
    ctx.execute("INSERT INTO ec_large (val) SELECT g FROM generate_series(1, 1000) AS g", &[]).await;

    let row = ctx.query_one("SELECT COUNT(*)::INT4 AS cnt, SUM(val)::INT4 AS total FROM ec_large", &[]).await;
    assert_eq!(row["cnt"], 1000);
    assert_eq!(row["total"], 500500); // sum 1..1000

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_multiple_null_params() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ec_nullp (id SERIAL PRIMARY KEY, a TEXT, b INT4, c BOOLEAN)").await;
    ctx.execute(
        "INSERT INTO ec_nullp (a, b, c) VALUES ($1, $2, $3)",
        &[SqlParam::Null, SqlParam::Null, SqlParam::Null],
    )
    .await;

    let row = ctx.query_one("SELECT a, b, c FROM ec_nullp WHERE id = 1", &[]).await;
    assert!(row["a"].is_null());
    assert!(row["b"].is_null());
    assert!(row["c"].is_null());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_special_characters_in_strings() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ec_special (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute(
        "INSERT INTO ec_special (val) VALUES ($1), ($2), ($3), ($4)",
        &[
            SqlParam::Text("it's a test".to_string()),  // single quote
            SqlParam::Text("line1\nline2".to_string()), // newline
            SqlParam::Text("tab\there".to_string()),    // tab
            SqlParam::Text("back\\slash".to_string()),  // backslash
        ],
    )
    .await;

    let rows = ctx.query("SELECT val FROM ec_special ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["val"], "it's a test");
    assert_eq!(arr[1]["val"], "line1\nline2");
    assert_eq!(arr[2]["val"], "tab\there");
    assert_eq!(arr[3]["val"], "back\\slash");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_boolean_expressions() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                TRUE AND TRUE AS tt,
                TRUE AND FALSE AS tf,
                TRUE OR FALSE AS t_or_f,
                NOT TRUE AS not_t,
                TRUE AND NULL AS t_and_null,
                FALSE OR NULL AS f_or_null",
            &[],
        )
        .await;
    assert_eq!(row["tt"], true);
    assert_eq!(row["tf"], false);
    assert_eq!(row["t_or_f"], true);
    assert_eq!(row["not_t"], false);
    assert!(row["t_and_null"].is_null()); // TRUE AND NULL = NULL
    assert!(row["f_or_null"].is_null()); // FALSE OR NULL = NULL

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_zero_rows_aggregates() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ec_empty_agg (id SERIAL PRIMARY KEY, val INT4)").await;

    // Aggregates on empty table
    let row = ctx
        .query_one(
            "SELECT
                COUNT(*) AS cnt,
                SUM(val) AS total,
                AVG(val::FLOAT8) AS average,
                MIN(val) AS minimum,
                MAX(val) AS maximum
             FROM ec_empty_agg",
            &[],
        )
        .await;
    assert_eq!(row["cnt"], 0);
    assert!(row["total"].is_null()); // SUM of empty set is NULL
    assert!(row["average"].is_null());
    assert!(row["minimum"].is_null());
    assert!(row["maximum"].is_null());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_many_columns() {
    let mut ctx = PostgresTestContext::new().await;

    // Create a table with many columns
    let cols: Vec<String> = (1..=20).map(|i| format!("col{} INT4", i)).collect();
    let ddl = format!("CREATE TABLE ec_wide (id SERIAL PRIMARY KEY, {})", cols.join(", "));
    ctx.batch_execute(&ddl).await;

    let vals: Vec<String> = (1..=20).map(|i| i.to_string()).collect();
    let insert = format!(
        "INSERT INTO ec_wide ({}) VALUES ({})",
        (1..=20).map(|i| format!("col{}", i)).collect::<Vec<_>>().join(", "),
        vals.join(", "),
    );
    ctx.execute(&insert, &[]).await;

    let row = ctx.query_one("SELECT * FROM ec_wide WHERE id = 1", &[]).await;
    assert_eq!(row["col1"], 1);
    assert_eq!(row["col10"], 10);
    assert_eq!(row["col20"], 20);

    ctx.stop().await;
}
