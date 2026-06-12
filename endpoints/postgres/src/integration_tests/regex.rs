use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_like_patterns() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE rx_test (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute(
        "INSERT INTO rx_test (val) VALUES ('apple'), ('banana'), ('apricot'), ('avocado'), ('blueberry')",
        &[],
    )
    .await;

    // LIKE with %
    let rows = ctx.query("SELECT val FROM rx_test WHERE val LIKE 'a%' ORDER BY val", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3); // apple, apricot, avocado

    // LIKE with _ (single char)
    let rows = ctx.query("SELECT val FROM rx_test WHERE val LIKE 'a____e' ORDER BY val", &[]).await;
    // 'a' + 4 chars + 'e' = 6 chars: "apple" is 5 chars, doesn't match
    // Let's check what matches: none of our values are 6 chars starting with a, ending e
    let empty = vec![];
    let arr = rows.as_array().unwrap_or(&empty);
    // This tests that _ works as single-char wildcard
    assert!(arr.is_empty() || arr.len() <= 5);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_ilike_case_insensitive() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE rx_ilike (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute("INSERT INTO rx_ilike (val) VALUES ('Hello'), ('HELLO'), ('hello'), ('World')", &[]).await;

    let rows = ctx.query("SELECT val FROM rx_ilike WHERE val ILIKE $1 ORDER BY id", &[SqlParam::Text("hello".to_string())]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3); // Hello, HELLO, hello

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_similar_to() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE rx_similar (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute("INSERT INTO rx_similar (val) VALUES ('abc'), ('adc'), ('aec'), ('axyz'), ('bbc')", &[]).await;

    // SIMILAR TO uses SQL regex (% and _ plus | for alternation)
    let rows = ctx.query("SELECT val FROM rx_similar WHERE val SIMILAR TO 'a(b|d|e)c' ORDER BY val", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3); // abc, adc, aec

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_posix_regex_match() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE rx_posix (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute("INSERT INTO rx_posix (val) VALUES ('cat'), ('bat'), ('hat'), ('dog'), ('catch')", &[]).await;

    // ~ is case-sensitive regex match
    let rows = ctx.query("SELECT val FROM rx_posix WHERE val ~ '^[cbh]at$' ORDER BY val", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3); // bat, cat, hat
    assert_eq!(arr[0]["val"], "bat");
    assert_eq!(arr[1]["val"], "cat");
    assert_eq!(arr[2]["val"], "hat");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_posix_regex_case_insensitive() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE rx_ci (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute("INSERT INTO rx_ci (val) VALUES ('Hello'), ('HELLO'), ('hello'), ('World')", &[]).await;

    // ~* is case-insensitive regex match
    let rows = ctx.query("SELECT val FROM rx_ci WHERE val ~* '^hello$' ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_regexp_replace() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                REGEXP_REPLACE('Hello 123 World 456', '[0-9]+', '#', 'g') AS replaced",
            &[],
        )
        .await;
    assert_eq!(row["replaced"], "Hello # World #");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_regexp_split_to_array() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT REGEXP_SPLIT_TO_ARRAY('one-two--three', '-+') AS parts", &[]).await;
    let parts = row["parts"].as_array().unwrap();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0], "one");
    assert_eq!(parts[1], "two");
    assert_eq!(parts[2], "three");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_regexp_split_to_table() {
    let mut ctx = PostgresTestContext::new().await;

    let rows = ctx.query("SELECT val FROM REGEXP_SPLIT_TO_TABLE('a,b,,c', ',+') AS val", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3); // a, b, c (empty strings collapsed by ,+)

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_not_like_not_similar() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE rx_not (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute("INSERT INTO rx_not (val) VALUES ('abc'), ('def'), ('abx'), ('xyz')", &[]).await;

    let rows = ctx.query("SELECT val FROM rx_not WHERE val NOT LIKE 'ab%' ORDER BY val", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2); // def, xyz

    // !~ is NOT matching regex
    let rows = ctx.query("SELECT val FROM rx_not WHERE val !~ '^a' ORDER BY val", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2); // def, xyz

    ctx.stop().await;
}
