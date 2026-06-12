use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_copy_in_csv() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE cp_csv (id INT, name TEXT, value INT)").await;

    let result = ctx.copy_in("COPY cp_csv FROM STDIN WITH (FORMAT csv)", "1,alice,100\n2,bob,200\n").await;
    // copy_in returns {"type": "copy_in", "rows": N}
    assert_eq!(result["rows"], 2);

    let rows = ctx.query("SELECT * FROM cp_csv ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["name"], "alice");
    assert_eq!(arr[1]["name"], "bob");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_copy_in_tab_delimited() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE cp_tab (id INT, name TEXT)").await;

    let result = ctx.copy_in("COPY cp_tab FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')", "1\talice\n2\tbob\n").await;
    assert_eq!(result["rows"], 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_copy_in_multiple_rows() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE cp_multi (id INT, val TEXT)").await;

    let mut data = String::new();
    for i in 1..=10 {
        data.push_str(&format!("{},row{}\n", i, i));
    }
    let result = ctx.copy_in("COPY cp_multi FROM STDIN WITH (FORMAT csv)", &data).await;
    assert_eq!(result["rows"], 10);

    let row = ctx.query_one("SELECT COUNT(*)::INT4 AS cnt FROM cp_multi", &[]).await;
    assert_eq!(row["cnt"], 10);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_copy_out_csv() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE cp_out_csv (id INT, name TEXT)").await;
    ctx.execute("INSERT INTO cp_out_csv (id, name) VALUES (1, 'alice'), (2, 'bob')", &[]).await;

    let result = ctx.copy_out("COPY cp_out_csv TO STDOUT WITH (FORMAT csv)").await;
    // copy_out returns {"type": "copy_out", "value": "..."}
    let csv_data = result["value"].as_str().unwrap();
    assert!(csv_data.contains("alice"));
    assert!(csv_data.contains("bob"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_copy_out_tab_delimited() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE cp_out_tab (id INT, name TEXT)").await;
    ctx.execute("INSERT INTO cp_out_tab (id, name) VALUES (1, 'x'), (2, 'y')", &[]).await;

    let result = ctx.copy_out("COPY cp_out_tab TO STDOUT").await;
    // Default format is text with tab delimiter
    let data = result["value"].as_str().unwrap();
    assert!(data.contains('\t'));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_copy_in_then_out_roundtrip() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE cp_round (id INT, label TEXT)").await;

    // Copy in
    ctx.copy_in("COPY cp_round FROM STDIN WITH (FORMAT csv)", "1,hello\n2,world\n3,test\n").await;

    // Copy out
    let result = ctx.copy_out("COPY cp_round TO STDOUT WITH (FORMAT csv)").await;
    let csv = result["value"].as_str().unwrap();

    // Verify round-trip data integrity
    assert!(csv.contains("hello"));
    assert!(csv.contains("world"));
    assert!(csv.contains("test"));
    let lines: Vec<&str> = csv.trim().split('\n').collect();
    assert_eq!(lines.len(), 3);

    ctx.stop().await;
}
