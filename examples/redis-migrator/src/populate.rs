use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

const POPULATE_STATS_FILE_ENV: &str = "REDIS_POPULATE_STATS_FILE";

/// Populate a Redis database with configurable data using redis-cli --pipe for maximum throughput
#[derive(Parser, Debug, Clone)]
pub struct PopulateConfig {
    /// Redis URL to populate through (typically the interlay, e.g. redis://eden-host:5731)
    #[clap(
        long,
        short = 'u',
        env = "REDIS_URL",
        default_value = "redis://localhost:6379"
    )]
    pub url: String,

    /// Total megabytes of data to generate
    #[clap(long = "mb", short = 'm', env = "MEGABYTES", default_value = "1000")]
    pub megabytes: u64,

    /// Individual key/value size in bytes
    #[clap(long = "size", short = 's', env = "KEY_SIZE", default_value = "1024")]
    pub key_size: u64,

    /// Use STRING data type (SET/GET)
    #[clap(long, group = "datatype")]
    pub string: bool,

    /// Use JSON data type (JSON.SET/JSON.GET) - requires RedisJSON module
    #[clap(long, group = "datatype")]
    pub json: bool,

    /// Use HASH data type (HSET/HGET)
    #[clap(long, group = "datatype")]
    pub hash: bool,

    /// Use LIST data type (LPUSH/LRANGE)
    #[clap(long, group = "datatype")]
    pub list: bool,

    /// Use SET data type (SADD/SMEMBERS)
    #[clap(long, group = "datatype")]
    pub set: bool,

    /// Use SORTED SET data type (ZADD/ZRANGE)
    #[clap(long = "zset", group = "datatype")]
    pub sorted_set: bool,

    /// Use a random mix of all data types (STRING, HASH, LIST, SET, ZSET)
    #[clap(long, group = "datatype")]
    pub mixed: bool,

    /// Key prefix for all generated keys
    #[clap(long, short = 'p', env = "KEY_PREFIX", default_value = "pop")]
    pub prefix: String,

    /// Number of keys to buffer before flushing to the redis-cli pipe
    #[clap(long, short = 'b', env = "BATCH_SIZE", default_value = "10000")]
    pub batch_size: usize,

    /// TTL in seconds for keys (0 = no expiry)
    #[clap(long, short = 't', env = "TTL", default_value = "0")]
    pub ttl: u64,

    /// Number of elements per key (for list, set, sorted-set, hash types)
    #[clap(long, short = 'e', env = "ELEMENTS_PER_KEY", default_value = "10")]
    pub elements_per_key: usize,

    /// Clear existing keys with the same prefix before populating
    #[clap(long)]
    pub clear: bool,

    /// After populating, automatically run the client with this read/write split (write percentage 0-100)
    #[clap(long = "then-client", short = 'w')]
    pub then_client_write_pct: Option<u8>,

    /// Duration in seconds for the automatic client run (0 = until interrupted)
    #[clap(long, short = 'd', default_value = "60")]
    pub client_duration: u64,

    /// Number of concurrent workers for the automatic client run
    #[clap(long, default_value = "50")]
    pub client_concurrency: usize,

    /// Number of parallel redis-cli --pipe processes
    #[clap(long, env = "PIPES", default_value = "1")]
    pub pipes: usize,
}

pub struct PopulateResult {
    pub num_keys: u64,
    pub url: String,
    pub prefix: String,
    pub key_size: u64,
    pub then_client_write_pct: Option<u8>,
    pub client_duration: u64,
    pub client_concurrency: usize,
}

#[derive(Serialize)]
struct PopulateStatsFile {
    status: String,
    url: String,
    key_prefix: String,
    data_type: String,
    megabytes: u64,
    key_size: u64,
    batch_size: usize,
    ttl: u64,
    elements_per_key: usize,
    clear: bool,
    pipes: usize,
    then_client_write_pct: Option<u8>,
    client_duration_secs: u64,
    client_concurrency: usize,
    total_bytes: u64,
    bytes_written: u64,
    target_keys: u64,
    written_keys: u64,
    elapsed_secs: f64,
    mb_per_sec: f64,
    keys_per_sec: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    String,
    Json,
    Hash,
    List,
    Set,
    SortedSet,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::String => write!(f, "STRING"),
            DataType::Json => write!(f, "JSON"),
            DataType::Hash => write!(f, "HASH"),
            DataType::List => write!(f, "LIST"),
            DataType::Set => write!(f, "SET"),
            DataType::SortedSet => write!(f, "SORTED SET"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SampleDocument {
    id: String,
    timestamp: u64,
    data: String,
    metadata: DocumentMetadata,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DocumentMetadata {
    source: String,
    version: u32,
    tags: Vec<String>,
}

struct RedisConnInfo {
    host: String,
    port: u16,
    tls: bool,
    password: Option<String>,
}

/// Parse a redis:// or rediss:// URL into connection components for redis-cli
fn parse_redis_url(url: &str) -> Result<RedisConnInfo> {
    let url = if url.contains("://") {
        url.to_string()
    } else {
        format!("redis://{}", url)
    };
    let parsed = url::Url::parse(&url).context("Invalid Redis URL")?;
    let tls = parsed.scheme() == "rediss";
    let host = parsed.host_str().unwrap_or("localhost").to_string();
    let port = parsed.port().unwrap_or(if tls { 6380 } else { 6379 });
    let password = if !parsed.password().unwrap_or("").is_empty() {
        Some(
            percent_encoding::percent_decode_str(parsed.password().unwrap())
                .decode_utf8()
                .context("Invalid password encoding")?
                .to_string(),
        )
    } else {
        None
    };
    Ok(RedisConnInfo {
        host,
        port,
        tls,
        password,
    })
}

fn populate_data_type_label(config: &PopulateConfig) -> String {
    if config.mixed {
        "MIXED".to_string()
    } else if config.json {
        DataType::Json.to_string()
    } else if config.hash {
        DataType::Hash.to_string()
    } else if config.list {
        DataType::List.to_string()
    } else if config.set {
        DataType::Set.to_string()
    } else if config.sorted_set {
        DataType::SortedSet.to_string()
    } else {
        DataType::String.to_string()
    }
}

fn write_populate_stats_file(
    path: Option<&str>,
    config: &PopulateConfig,
    status: &str,
    total_bytes: u64,
    target_keys: u64,
    bytes_written: u64,
    elapsed_secs: f64,
) {
    let Some(path) = path else {
        return;
    };

    let data_type = populate_data_type_label(config);
    let mb_per_sec = if elapsed_secs > 0.0 {
        (bytes_written as f64 / (1024.0 * 1024.0)) / elapsed_secs
    } else {
        0.0
    };
    let written_keys = if config.key_size > 0 {
        bytes_written / config.key_size
    } else {
        0
    };
    let keys_per_sec = if elapsed_secs > 0.0 {
        written_keys as f64 / elapsed_secs
    } else {
        0.0
    };

    let snapshot = PopulateStatsFile {
        status: status.to_string(),
        url: config.url.clone(),
        key_prefix: config.prefix.clone(),
        data_type,
        megabytes: config.megabytes,
        key_size: config.key_size,
        batch_size: config.batch_size,
        ttl: config.ttl,
        elements_per_key: config.elements_per_key,
        clear: config.clear,
        pipes: config.pipes,
        then_client_write_pct: config.then_client_write_pct,
        client_duration_secs: config.client_duration,
        client_concurrency: config.client_concurrency,
        total_bytes,
        bytes_written,
        target_keys,
        written_keys,
        elapsed_secs,
        mb_per_sec,
        keys_per_sec,
    };

    if let Ok(json) = serde_json::to_string_pretty(&snapshot) {
        let _ = fs::write(path, json);
    }
}

// --- RESP protocol encoding ---

fn resp_bulk_string(s: &[u8], buf: &mut Vec<u8>) {
    buf.extend_from_slice(format!("${}\r\n", s.len()).as_bytes());
    buf.extend_from_slice(s);
    buf.extend_from_slice(b"\r\n");
}

fn resp_array_header(count: usize, buf: &mut Vec<u8>) {
    buf.extend_from_slice(format!("*{}\r\n", count).as_bytes());
}

// --- Random data generation ---

fn generate_random_bytes(size: usize, rng: &mut StdRng) -> Vec<u8> {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    (0..size)
        .map(|_| CHARSET[rng.gen_range(0..CHARSET.len())])
        .collect()
}

fn generate_sample_document(target_size: usize, rng: &mut StdRng) -> SampleDocument {
    let base_overhead = 150;
    let data_size = target_size.saturating_sub(base_overhead);
    let data: String = generate_random_bytes(data_size, rng)
        .iter()
        .map(|&b| b as char)
        .collect();

    SampleDocument {
        id: Uuid::new_v4().to_string(),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        data,
        metadata: DocumentMetadata {
            source: "redis-migrator".to_string(),
            version: 1,
            tags: vec!["generated".to_string(), "test".to_string()],
        },
    }
}

// --- RESP command generators per data type ---

fn encode_string_key(key: &[u8], value_size: usize, ttl: u64, rng: &mut StdRng, buf: &mut Vec<u8>) {
    let value = generate_random_bytes(value_size, rng);
    if ttl > 0 {
        let ttl_str = ttl.to_string();
        resp_array_header(4, buf);
        resp_bulk_string(b"SETEX", buf);
        resp_bulk_string(key, buf);
        resp_bulk_string(ttl_str.as_bytes(), buf);
        resp_bulk_string(&value, buf);
    } else {
        resp_array_header(3, buf);
        resp_bulk_string(b"SET", buf);
        resp_bulk_string(key, buf);
        resp_bulk_string(&value, buf);
    }
}

fn encode_json_key(key: &[u8], value_size: usize, ttl: u64, rng: &mut StdRng, buf: &mut Vec<u8>) {
    let doc = generate_sample_document(value_size, rng);
    let json_str = serde_json::to_string(&doc).unwrap();

    resp_array_header(4, buf);
    resp_bulk_string(b"JSON.SET", buf);
    resp_bulk_string(key, buf);
    resp_bulk_string(b"$", buf);
    resp_bulk_string(json_str.as_bytes(), buf);

    if ttl > 0 {
        let ttl_str = ttl.to_string();
        resp_array_header(3, buf);
        resp_bulk_string(b"EXPIRE", buf);
        resp_bulk_string(key, buf);
        resp_bulk_string(ttl_str.as_bytes(), buf);
    }
}

fn encode_hash_key(
    key: &[u8],
    value_size: usize,
    elements_per_key: usize,
    ttl: u64,
    rng: &mut StdRng,
    buf: &mut Vec<u8>,
) {
    let field_value_size = value_size / elements_per_key.max(1);

    // Single HSET with all field-value pairs
    resp_array_header(2 + elements_per_key * 2, buf);
    resp_bulk_string(b"HSET", buf);
    resp_bulk_string(key, buf);
    for i in 0..elements_per_key {
        let field = format!("field_{}", i);
        let value = generate_random_bytes(field_value_size, rng);
        resp_bulk_string(field.as_bytes(), buf);
        resp_bulk_string(&value, buf);
    }

    if ttl > 0 {
        let ttl_str = ttl.to_string();
        resp_array_header(3, buf);
        resp_bulk_string(b"EXPIRE", buf);
        resp_bulk_string(key, buf);
        resp_bulk_string(ttl_str.as_bytes(), buf);
    }
}

fn encode_list_key(
    key: &[u8],
    value_size: usize,
    elements_per_key: usize,
    ttl: u64,
    rng: &mut StdRng,
    buf: &mut Vec<u8>,
) {
    let element_size = value_size / elements_per_key.max(1);

    // Single RPUSH with all elements
    resp_array_header(2 + elements_per_key, buf);
    resp_bulk_string(b"RPUSH", buf);
    resp_bulk_string(key, buf);
    for _ in 0..elements_per_key {
        let value = generate_random_bytes(element_size, rng);
        resp_bulk_string(&value, buf);
    }

    if ttl > 0 {
        let ttl_str = ttl.to_string();
        resp_array_header(3, buf);
        resp_bulk_string(b"EXPIRE", buf);
        resp_bulk_string(key, buf);
        resp_bulk_string(ttl_str.as_bytes(), buf);
    }
}

fn encode_set_key(
    key: &[u8],
    value_size: usize,
    elements_per_key: usize,
    ttl: u64,
    rng: &mut StdRng,
    buf: &mut Vec<u8>,
) {
    let element_size = value_size / elements_per_key.max(1);

    // Single SADD with all members
    resp_array_header(2 + elements_per_key, buf);
    resp_bulk_string(b"SADD", buf);
    resp_bulk_string(key, buf);
    for _ in 0..elements_per_key {
        let value = generate_random_bytes(element_size, rng);
        resp_bulk_string(&value, buf);
    }

    if ttl > 0 {
        let ttl_str = ttl.to_string();
        resp_array_header(3, buf);
        resp_bulk_string(b"EXPIRE", buf);
        resp_bulk_string(key, buf);
        resp_bulk_string(ttl_str.as_bytes(), buf);
    }
}

fn encode_sorted_set_key(
    key: &[u8],
    value_size: usize,
    elements_per_key: usize,
    ttl: u64,
    rng: &mut StdRng,
    buf: &mut Vec<u8>,
) {
    let element_size = value_size / elements_per_key.max(1);

    // Single ZADD with all score-member pairs
    resp_array_header(2 + elements_per_key * 2, buf);
    resp_bulk_string(b"ZADD", buf);
    resp_bulk_string(key, buf);
    for _ in 0..elements_per_key {
        let score: f64 = rng.gen_range(0.0..1000000.0);
        let score_str = format!("{}", score);
        let value = generate_random_bytes(element_size, rng);
        resp_bulk_string(score_str.as_bytes(), buf);
        resp_bulk_string(&value, buf);
    }

    if ttl > 0 {
        let ttl_str = ttl.to_string();
        resp_array_header(3, buf);
        resp_bulk_string(b"EXPIRE", buf);
        resp_bulk_string(key, buf);
        resp_bulk_string(ttl_str.as_bytes(), buf);
    }
}

const MIXED_TYPES: [DataType; 5] = [
    DataType::String,
    DataType::Hash,
    DataType::List,
    DataType::Set,
    DataType::SortedSet,
];

fn encode_key(
    key: &[u8],
    data_type: DataType,
    value_size: usize,
    elements_per_key: usize,
    ttl: u64,
    rng: &mut StdRng,
    buf: &mut Vec<u8>,
) {
    match data_type {
        DataType::String => encode_string_key(key, value_size, ttl, rng, buf),
        DataType::Json => encode_json_key(key, value_size, ttl, rng, buf),
        DataType::Hash => encode_hash_key(key, value_size, elements_per_key, ttl, rng, buf),
        DataType::List => encode_list_key(key, value_size, elements_per_key, ttl, rng, buf),
        DataType::Set => encode_set_key(key, value_size, elements_per_key, ttl, rng, buf),
        DataType::SortedSet => {
            encode_sorted_set_key(key, value_size, elements_per_key, ttl, rng, buf)
        }
    }
}

async fn clear_keys_with_prefix(
    conn: &mut redis::aio::MultiplexedConnection,
    prefix: &str,
) -> Result<u64> {
    let pattern = format!("{}:*", prefix);
    let mut cursor: u64 = 0;
    let mut total_deleted = 0u64;

    loop {
        let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(1000)
            .query_async(conn)
            .await?;

        if !keys.is_empty() {
            let deleted: u64 = redis::cmd("DEL").arg(&keys).query_async(conn).await?;
            total_deleted += deleted;
        }

        cursor = new_cursor;
        if cursor == 0 {
            break;
        }
    }

    Ok(total_deleted)
}

pub async fn run(config: PopulateConfig) -> Result<PopulateResult> {
    let data_type = if config.json {
        DataType::Json
    } else if config.hash {
        DataType::Hash
    } else if config.list {
        DataType::List
    } else if config.set {
        DataType::Set
    } else if config.sorted_set {
        DataType::SortedSet
    } else if config.mixed {
        DataType::String // placeholder, not used directly
    } else {
        DataType::String
    };
    let is_mixed = config.mixed;

    let total_bytes = config.megabytes * 1024 * 1024;
    let num_keys = (total_bytes / config.key_size).max(1);
    let stats_path = std::env::var(POPULATE_STATS_FILE_ENV).ok();

    let conn = parse_redis_url(&config.url)?;
    write_populate_stats_file(
        stats_path.as_deref(),
        &config,
        "starting",
        total_bytes,
        num_keys,
        0,
        0.0,
    );

    println!("Redis Migrator (redis-cli --pipe)");
    println!("===================================");
    println!("Redis:           {}", config.url);
    println!(
        "Data Type:       {}",
        if is_mixed {
            "MIXED".to_string()
        } else {
            data_type.to_string()
        }
    );
    println!("Total Data:      {} MB", config.megabytes);
    println!("Key Size:        {} bytes", config.key_size);
    println!("Keys to Create:  {}", num_keys);
    println!("Batch Size:      {}", config.batch_size);
    println!("Parallel Pipes:  {}", config.pipes);
    println!("Key Prefix:      {}", config.prefix);
    if config.ttl > 0 {
        println!("TTL:             {} seconds", config.ttl);
    }
    if is_mixed
        || matches!(
            data_type,
            DataType::Hash | DataType::List | DataType::Set | DataType::SortedSet
        )
    {
        println!("Elements/Key:    {}", config.elements_per_key);
    }
    println!();

    // Clear keys if requested (needs a Redis connection)
    if config.clear {
        write_populate_stats_file(
            stats_path.as_deref(),
            &config,
            "clearing",
            total_bytes,
            num_keys,
            0,
            0.0,
        );
        let client =
            redis::Client::open(config.url.as_str()).context("Failed to create Redis client")?;
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .context("Failed to connect to Redis")?;
        println!("Clearing existing keys with prefix '{}'...", config.prefix);
        let deleted = clear_keys_with_prefix(&mut conn, &config.prefix).await?;
        println!("Deleted {} existing keys", deleted);
    }

    // Build common redis-cli connection args
    let port_str = conn.port.to_string();
    let mut base_args = vec![
        "-h".to_string(),
        conn.host.clone(),
        "-p".to_string(),
        port_str.clone(),
    ];
    if conn.tls {
        base_args.push("--tls".to_string());
    }
    if let Some(ref pw) = conn.password {
        base_args.push("-a".to_string());
        base_args.push(pw.clone());
    }

    // Step 1: Verify redis-cli is available
    println!("[1/4] Checking redis-cli...");
    write_populate_stats_file(
        stats_path.as_deref(),
        &config,
        "checking",
        total_bytes,
        num_keys,
        0,
        0.0,
    );
    let check = tokio::process::Command::new("redis-cli")
        .arg("--version")
        .output()
        .await
        .context("redis-cli not found. Install redis-tools or ensure redis-cli is in PATH.")?;
    if !check.status.success() {
        anyhow::bail!("redis-cli --version check failed");
    }
    let version = std::str::from_utf8(&check.stdout)
        .unwrap_or("redis-cli")
        .trim()
        .to_string();
    println!("  {}", version);

    // Step 2: Test connectivity with PING
    println!("[2/4] Testing connectivity...");
    write_populate_stats_file(
        stats_path.as_deref(),
        &config,
        "connecting",
        total_bytes,
        num_keys,
        0,
        0.0,
    );
    let mut ping_args = base_args.clone();
    ping_args.push("PING".to_string());
    println!(
        "  Running: redis-cli {}",
        ping_args
            .iter()
            .map(|a| if a.contains(' ') || a.contains('=') {
                format!("'{}'", a)
            } else {
                a.clone()
            })
            .collect::<Vec<_>>()
            .join(" ")
    );

    let ping_output = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio::process::Command::new("redis-cli")
            .args(&ping_args)
            .output(),
    )
    .await
    .context("redis-cli PING timed out after 10s — host may be unreachable or port blocked")?
    .context("Failed to run redis-cli PING")?;

    let ping_stdout = String::from_utf8_lossy(&ping_output.stdout);
    let ping_stderr = String::from_utf8_lossy(&ping_output.stderr);
    println!("  stdout: {}", ping_stdout.trim());
    if !ping_stderr.is_empty() {
        println!("  stderr: {}", ping_stderr.trim());
    }
    println!("  exit code: {}", ping_output.status);

    if !ping_output.status.success() || !ping_stdout.trim().contains("PONG") {
        anyhow::bail!(
            "redis-cli PING failed (exit {}). stdout='{}' stderr='{}'",
            ping_output.status,
            ping_stdout.trim(),
            ping_stderr.trim()
        );
    }
    println!("  Connected successfully");

    // Step 3: Start redis-cli --pipe processes
    let num_pipes = config.pipes.max(1);
    println!(
        "[3/4] Starting {} redis-cli --pipe process(es)...",
        num_pipes
    );
    let mut pipe_args = base_args.clone();
    pipe_args.push("--pipe-timeout".to_string());
    pipe_args.push("0".to_string());
    pipe_args.push("--pipe".to_string());
    println!(
        "  Running: redis-cli {}",
        pipe_args
            .iter()
            .map(|a| if a.contains(' ') || a.contains('=') {
                format!("'{}'", a)
            } else {
                a.clone()
            })
            .collect::<Vec<_>>()
            .join(" ")
    );

    let start_time = Instant::now();
    let pb = Arc::new(ProgressBar::new(total_bytes));
    let progress_done = Arc::new(AtomicBool::new(false));
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) [{binary_bytes_per_sec}] ETA: {eta}")
            .unwrap()
            .progress_chars("#>-"),
    );

    // Step 4: Spawn parallel pipes and stream data
    println!("[4/4] Streaming data across {} pipe(s)...", num_pipes);
    write_populate_stats_file(
        stats_path.as_deref(),
        &config,
        "running",
        total_bytes,
        num_keys,
        0,
        0.0,
    );
    let progress_task = if let Some(path) = stats_path.clone() {
        let pb = pb.clone();
        let config = config.clone();
        let done = progress_done.clone();
        Some(tokio::spawn(async move {
            while !done.load(Ordering::Relaxed) {
                let elapsed_secs = start_time.elapsed().as_secs_f64();
                write_populate_stats_file(
                    Some(&path),
                    &config,
                    "running",
                    total_bytes,
                    num_keys,
                    pb.position(),
                    elapsed_secs,
                );
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }))
    } else {
        None
    };
    let keys_per_pipe = (num_keys + num_pipes as u64 - 1) / num_pipes as u64;
    let mut pipe_handles = Vec::new();

    for pipe_idx in 0..num_pipes {
        let pipe_start = pipe_idx as u64 * keys_per_pipe;
        let pipe_end = (pipe_start + keys_per_pipe).min(num_keys);
        if pipe_start >= num_keys {
            break;
        }

        let pipe_args = pipe_args.clone();
        let prefix = config.prefix.clone();
        let key_size = config.key_size;
        let batch_size = config.batch_size as u64;
        let elements_per_key = config.elements_per_key;
        let ttl = config.ttl;
        let pb = pb.clone();

        let handle = tokio::spawn(async move {
            let mut child = tokio::process::Command::new("redis-cli")
                .args(&pipe_args)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .context("Failed to spawn redis-cli --pipe")?;

            let mut stdin = child.stdin.take().unwrap();
            let stderr_handle = child.stderr.take().unwrap();

            let stderr_task = tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut buf = String::new();
                let mut reader = tokio::io::BufReader::new(stderr_handle);
                let _ = reader.read_to_string(&mut buf).await;
                buf
            });

            // Brief pause for first pipe only to verify connectivity
            if pipe_idx == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                if let Some(status) = child
                    .try_wait()
                    .context("Failed to check redis-cli status")?
                {
                    let stderr_output = stderr_task.await.unwrap_or_default();
                    anyhow::bail!(
                        "redis-cli --pipe exited immediately (status: {}). stderr: {}",
                        status,
                        stderr_output.trim()
                    );
                }
            }

            let mut rng = StdRng::from_entropy();
            let mut key_idx = pipe_start;

            while key_idx < pipe_end {
                let batch_end = (key_idx + batch_size).min(pipe_end);
                let estimated_capacity = batch_size as usize * (key_size as usize + 64);
                let mut buf = Vec::with_capacity(estimated_capacity);

                for i in key_idx..batch_end {
                    let key = format!("{}:{}", prefix, i);
                    let dt = if is_mixed {
                        MIXED_TYPES[rng.gen_range(0..MIXED_TYPES.len())]
                    } else {
                        data_type
                    };

                    encode_key(
                        key.as_bytes(),
                        dt,
                        key_size as usize,
                        elements_per_key,
                        ttl,
                        &mut rng,
                        &mut buf,
                    );
                }

                if let Err(e) = stdin.write_all(&buf).await {
                    drop(stdin);
                    let stderr_output = stderr_task.await.unwrap_or_default();
                    let exit_status = child.wait().await.ok();
                    anyhow::bail!(
                        "Pipe {} write failed at key {} (error: {}). exit: {:?}, stderr: {}",
                        pipe_idx,
                        key_idx,
                        e,
                        exit_status,
                        stderr_output.trim()
                    );
                }

                let keys_in_batch = batch_end - key_idx;
                pb.inc(keys_in_batch * key_size);
                key_idx = batch_end;
            }

            drop(stdin);

            let output = child
                .wait_with_output()
                .await
                .context("Failed to wait for redis-cli")?;

            let stderr_output = stderr_task.await.unwrap_or_default();
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();

            if !output.status.success() {
                anyhow::bail!(
                    "Pipe {} failed (status: {}). stdout: {}, stderr: {}",
                    pipe_idx,
                    output.status,
                    stdout.trim(),
                    stderr_output.trim()
                );
            }

            Ok::<(String, String), anyhow::Error>((stdout, stderr_output))
        });

        pipe_handles.push(handle);
    }

    // Wait for all pipes to complete
    let mut any_failed = false;
    for (i, handle) in pipe_handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok((stdout, stderr))) => {
                if !stdout.trim().is_empty() {
                    println!("  Pipe {} stdout: {}", i, stdout.trim());
                }
                if !stderr.trim().is_empty() {
                    eprintln!("  Pipe {} stderr: {}", i, stderr.trim());
                }
            }
            Ok(Err(e)) => {
                eprintln!("  Pipe {} error: {}", i, e);
                any_failed = true;
            }
            Err(e) => {
                eprintln!("  Pipe {} panicked: {}", i, e);
                any_failed = true;
            }
        }
    }

    progress_done.store(true, Ordering::Relaxed);
    if let Some(task) = progress_task {
        let _ = task.await;
    }
    pb.finish_with_message("Done!");

    if any_failed {
        write_populate_stats_file(
            stats_path.as_deref(),
            &config,
            "failed",
            total_bytes,
            num_keys,
            pb.position(),
            start_time.elapsed().as_secs_f64(),
        );
        anyhow::bail!("One or more redis-cli --pipe processes failed");
    }

    let elapsed = start_time.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let actual_mb = (num_keys * config.key_size) as f64 / (1024.0 * 1024.0);
    let mb_per_sec = if elapsed_secs > 0.0 {
        actual_mb / elapsed_secs
    } else {
        0.0
    };
    let keys_per_sec = if elapsed_secs > 0.0 {
        num_keys as f64 / elapsed_secs
    } else {
        0.0
    };

    println!();
    println!("Population Complete");
    println!("===================");
    println!("Keys Created:    {}", num_keys);
    println!("Data Written:    {:.2} MB", actual_mb);
    println!("Time Elapsed:    {:.2}s", elapsed_secs);
    println!(
        "Throughput:      {:.2} MB/s ({:.0} keys/s)",
        mb_per_sec, keys_per_sec
    );

    // Query DB size
    let client =
        redis::Client::open(config.url.as_str()).context("Failed to create Redis client")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("Failed to connect to Redis for DBSIZE")?;
    let dbsize: u64 = redis::cmd("DBSIZE").query_async(&mut conn).await?;
    println!("Total DB Keys:   {}", dbsize);
    write_populate_stats_file(
        stats_path.as_deref(),
        &config,
        "completed",
        total_bytes,
        num_keys,
        total_bytes,
        elapsed_secs,
    );

    Ok(PopulateResult {
        num_keys,
        url: config.url.clone(),
        prefix: config.prefix.clone(),
        key_size: config.key_size,
        then_client_write_pct: config.then_client_write_pct,
        client_duration: config.client_duration,
        client_concurrency: config.client_concurrency,
    })
}
