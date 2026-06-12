use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_lpad_rpad() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                LPAD('42', 5, '0') AS lpadded,
                RPAD('hi', 6, '.') AS rpadded",
            &[],
        )
        .await;
    assert_eq!(row["lpadded"], "00042");
    assert_eq!(row["rpadded"], "hi....");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_repeat() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT REPEAT('ab', 3) AS repeated", &[]).await;
    assert_eq!(row["repeated"], "ababab");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_reverse() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT REVERSE('hello') AS reversed", &[]).await;
    assert_eq!(row["reversed"], "olleh");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_left_right() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT LEFT('abcdef', 3) AS l, RIGHT('abcdef', 3) AS r", &[]).await;
    assert_eq!(row["l"], "abc");
    assert_eq!(row["r"], "def");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_initcap() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT INITCAP('hello world foo') AS result", &[]).await;
    assert_eq!(row["result"], "Hello World Foo");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_translate() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT TRANSLATE('12345', '135', 'ace') AS result", &[]).await;
    // 1→a, 3→c, 5→e → "a2c4e"
    assert_eq!(row["result"], "a2c4e");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_md5() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT MD5('hello') AS hash", &[]).await;
    assert_eq!(row["hash"], "5d41402abc4b2a76b9719d911017c592");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_encode_decode() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                ENCODE('hello'::BYTEA, 'hex') AS hex_encoded,
                ENCODE('hello'::BYTEA, 'base64') AS b64_encoded",
            &[],
        )
        .await;
    assert_eq!(row["hex_encoded"], "68656c6c6f");
    assert_eq!(row["b64_encoded"], "aGVsbG8=");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_split_part() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                SPLIT_PART('a.b.c.d', '.', 2) AS second,
                SPLIT_PART('2024-06-15', '-', 1) AS year_part",
            &[],
        )
        .await;
    assert_eq!(row["second"], "b");
    assert_eq!(row["year_part"], "2024");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_chr_ascii() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT CHR(65) AS ch, ASCII('A') AS code", &[]).await;
    assert_eq!(row["ch"], "A");
    assert_eq!(row["code"], 65);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_overlay() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT OVERLAY('Txxxxas' PLACING 'hom' FROM 2 FOR 4) AS result", &[]).await;
    assert_eq!(row["result"], "Thomas");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_btrim_ltrim_rtrim() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                BTRIM('xxhelloxx', 'x') AS btrimmed,
                LTRIM('xxhello', 'x') AS ltrimmed,
                RTRIM('helloxx', 'x') AS rtrimmed",
            &[],
        )
        .await;
    assert_eq!(row["btrimmed"], "hello");
    assert_eq!(row["ltrimmed"], "hello");
    assert_eq!(row["rtrimmed"], "hello");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_format_function() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT FORMAT('Hello, %s! You are %s.', $1::TEXT, $2::TEXT) AS formatted",
            &[SqlParam::Text("world".to_string()), SqlParam::Text("great".to_string())],
        )
        .await;
    assert_eq!(row["formatted"], "Hello, world! You are great.");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_string_to_array() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT string_to_array('a,b,c,d', ',') AS arr", &[]).await;
    let arr = row["arr"].as_array().unwrap();
    assert_eq!(arr.len(), 4);
    assert_eq!(arr[0], "a");
    assert_eq!(arr[3], "d");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_to_string() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT array_to_string(ARRAY['a','b','c'], '-') AS joined", &[]).await;
    assert_eq!(row["joined"], "a-b-c");

    ctx.stop().await;
}
