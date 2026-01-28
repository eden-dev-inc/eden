use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use uuid::Uuid;

/// Redis Populator - Populate a Redis database with configurable data
#[derive(Parser, Debug, Clone)]
#[clap(name = "redis-populator", version = "0.1.0")]
pub struct Config {
    /// Redis host to connect to
    #[clap(long, short = 'H', env = "REDIS_HOST", default_value = "localhost")]
    pub host: String,

    /// Redis port to connect to
    #[clap(long, short = 'P', env = "REDIS_PORT", default_value = "6379")]
    pub port: u16,

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

    /// Key prefix for all generated keys
    #[clap(long, short = 'p', env = "KEY_PREFIX", default_value = "pop")]
    pub prefix: String,

    /// Number of concurrent connections
    #[clap(long, short = 'c', env = "CONCURRENCY", default_value = "50")]
    pub concurrency: usize,

    /// Batch size for pipelining
    #[clap(long, short = 'b', env = "BATCH_SIZE", default_value = "100")]
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
}

#[derive(Debug, Clone, Copy, PartialEq)]
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

/// Sample JSON document for JSON type
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

fn generate_random_string(size: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = StdRng::from_entropy();
    (0..size)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

fn generate_sample_document(target_size: usize) -> SampleDocument {
    let base_overhead = 150; // Approximate JSON overhead
    let data_size = target_size.saturating_sub(base_overhead);

    SampleDocument {
        id: Uuid::new_v4().to_string(),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        data: generate_random_string(data_size),
        metadata: DocumentMetadata {
            source: "redis-populator".to_string(),
            version: 1,
            tags: vec!["generated".to_string(), "test".to_string()],
        },
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
            let deleted: u64 = redis::cmd("DEL")
                .arg(&keys)
                .query_async(conn)
                .await?;
            total_deleted += deleted;
        }

        cursor = new_cursor;
        if cursor == 0 {
            break;
        }
    }

    Ok(total_deleted)
}

async fn populate_strings(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
    value_size: usize,
    ttl: u64,
) -> Result<()> {
    let mut pipe = redis::pipe();

    for key in keys {
        let value = generate_random_string(value_size);
        if ttl > 0 {
            pipe.cmd("SETEX").arg(key).arg(ttl).arg(&value);
        } else {
            pipe.cmd("SET").arg(key).arg(&value);
        }
    }

    pipe.query_async::<()>(conn).await?;
    Ok(())
}

async fn populate_json(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
    value_size: usize,
    ttl: u64,
) -> Result<()> {
    let mut pipe = redis::pipe();

    for key in keys {
        let doc = generate_sample_document(value_size);
        let json_str = serde_json::to_string(&doc)?;
        pipe.cmd("JSON.SET").arg(key).arg("$").arg(&json_str);
        if ttl > 0 {
            pipe.cmd("EXPIRE").arg(key).arg(ttl);
        }
    }

    pipe.query_async::<()>(conn).await?;
    Ok(())
}

async fn populate_hashes(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
    value_size: usize,
    elements_per_key: usize,
    ttl: u64,
) -> Result<()> {
    let field_value_size = value_size / elements_per_key.max(1);
    let mut pipe = redis::pipe();

    for key in keys {
        for i in 0..elements_per_key {
            let field = format!("field_{}", i);
            let value = generate_random_string(field_value_size);
            pipe.cmd("HSET").arg(key).arg(&field).arg(&value);
        }
        if ttl > 0 {
            pipe.cmd("EXPIRE").arg(key).arg(ttl);
        }
    }

    pipe.query_async::<()>(conn).await?;
    Ok(())
}

async fn populate_lists(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
    value_size: usize,
    elements_per_key: usize,
    ttl: u64,
) -> Result<()> {
    let element_size = value_size / elements_per_key.max(1);
    let mut pipe = redis::pipe();

    for key in keys {
        for _ in 0..elements_per_key {
            let value = generate_random_string(element_size);
            pipe.cmd("RPUSH").arg(key).arg(&value);
        }
        if ttl > 0 {
            pipe.cmd("EXPIRE").arg(key).arg(ttl);
        }
    }

    pipe.query_async::<()>(conn).await?;
    Ok(())
}

async fn populate_sets(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
    value_size: usize,
    elements_per_key: usize,
    ttl: u64,
) -> Result<()> {
    let element_size = value_size / elements_per_key.max(1);
    let mut pipe = redis::pipe();

    for key in keys {
        for _ in 0..elements_per_key {
            let value = generate_random_string(element_size);
            pipe.cmd("SADD").arg(key).arg(&value);
        }
        if ttl > 0 {
            pipe.cmd("EXPIRE").arg(key).arg(ttl);
        }
    }

    pipe.query_async::<()>(conn).await?;
    Ok(())
}

async fn populate_sorted_sets(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
    value_size: usize,
    elements_per_key: usize,
    ttl: u64,
) -> Result<()> {
    let element_size = value_size / elements_per_key.max(1);
    let mut pipe = redis::pipe();
    let mut rng = StdRng::from_entropy();

    for key in keys {
        for _ in 0..elements_per_key {
            let score: f64 = rng.gen_range(0.0..1000000.0);
            let value = generate_random_string(element_size);
            pipe.cmd("ZADD").arg(key).arg(score).arg(&value);
        }
        if ttl > 0 {
            pipe.cmd("EXPIRE").arg(key).arg(ttl);
        }
    }

    pipe.query_async::<()>(conn).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    // Determine data type (default to string if none specified)
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
    } else {
        DataType::String
    };

    // Calculate number of keys needed
    let total_bytes = config.megabytes * 1024 * 1024;
    let num_keys = (total_bytes / config.key_size).max(1);

    // Construct Redis URL from host and port
    let redis_url = format!("redis://{}:{}", config.host, config.port);

    println!("Redis Populator");
    println!("================");
    println!("Redis:           {}:{}", config.host, config.port);
    println!("Data Type:       {}", data_type);
    println!("Total Data:      {} MB", config.megabytes);
    println!("Key Size:        {} bytes", config.key_size);
    println!("Keys to Create:  {}", num_keys);
    println!("Concurrency:     {}", config.concurrency);
    println!("Batch Size:      {}", config.batch_size);
    println!("Key Prefix:      {}", config.prefix);
    if config.ttl > 0 {
        println!("TTL:             {} seconds", config.ttl);
    }
    if matches!(data_type, DataType::Hash | DataType::List | DataType::Set | DataType::SortedSet) {
        println!("Elements/Key:    {}", config.elements_per_key);
    }
    println!();

    // Connect to Redis
    let client = redis::Client::open(redis_url.as_str())
        .context("Failed to create Redis client")?;

    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("Failed to connect to Redis")?;

    // Test connection
    let pong: String = redis::cmd("PING").query_async(&mut conn).await?;
    if pong != "PONG" {
        anyhow::bail!("Unexpected PING response: {}", pong);
    }
    println!("Connected to Redis successfully");

    // Clear existing keys if requested
    if config.clear {
        println!("Clearing existing keys with prefix '{}'...", config.prefix);
        let deleted = clear_keys_with_prefix(&mut conn, &config.prefix).await?;
        println!("Deleted {} existing keys", deleted);
    }

    // Start timing
    let start_time = Instant::now();

    // Create progress bar (track bytes for MB/s display)
    let total_bytes = config.megabytes * 1024 * 1024;
    let pb = ProgressBar::new(total_bytes);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) [{binary_bytes_per_sec}] ETA: {eta}")
            .unwrap()
            .progress_chars("#>-"),
    );

    // Create connection pool
    let semaphore = Arc::new(Semaphore::new(config.concurrency));
    let keys_created = Arc::new(AtomicU64::new(0));
    let config = Arc::new(config);

    // Generate all key names
    let all_keys: Vec<String> = (0..num_keys)
        .map(|i| format!("{}:{}", config.prefix, i))
        .collect();

    // Process in batches
    let mut handles = Vec::new();

    for batch in all_keys.chunks(config.batch_size) {
        let permit = semaphore.clone().acquire_owned().await?;
        let batch_keys: Vec<String> = batch.to_vec();
        let keys_created = keys_created.clone();
        let pb = pb.clone();
        let config = config.clone();
        let client = client.clone();

        let handle = tokio::spawn(async move {
            let mut conn = match client.get_multiplexed_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Connection error: {}", e);
                    drop(permit);
                    return;
                }
            };

            let result = match data_type {
                DataType::String => {
                    populate_strings(&mut conn, &batch_keys, config.key_size as usize, config.ttl)
                        .await
                }
                DataType::Json => {
                    populate_json(&mut conn, &batch_keys, config.key_size as usize, config.ttl)
                        .await
                }
                DataType::Hash => {
                    populate_hashes(
                        &mut conn,
                        &batch_keys,
                        config.key_size as usize,
                        config.elements_per_key,
                        config.ttl,
                    )
                    .await
                }
                DataType::List => {
                    populate_lists(
                        &mut conn,
                        &batch_keys,
                        config.key_size as usize,
                        config.elements_per_key,
                        config.ttl,
                    )
                    .await
                }
                DataType::Set => {
                    populate_sets(
                        &mut conn,
                        &batch_keys,
                        config.key_size as usize,
                        config.elements_per_key,
                        config.ttl,
                    )
                    .await
                }
                DataType::SortedSet => {
                    populate_sorted_sets(
                        &mut conn,
                        &batch_keys,
                        config.key_size as usize,
                        config.elements_per_key,
                        config.ttl,
                    )
                    .await
                }
            };

            if let Err(e) = result {
                eprintln!("Error populating batch: {}", e);
            } else {
                let count = batch_keys.len() as u64;
                keys_created.fetch_add(count, Ordering::Relaxed);
                pb.inc(count * config.key_size);
            }

            drop(permit);
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        let _ = handle.await;
    }

    pb.finish_with_message("Done!");

    let elapsed = start_time.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let total_created = keys_created.load(Ordering::Relaxed);
    let actual_mb = (total_created * config.key_size) as f64 / (1024.0 * 1024.0);
    let mb_per_sec = if elapsed_secs > 0.0 { actual_mb / elapsed_secs } else { 0.0 };
    let keys_per_sec = if elapsed_secs > 0.0 { total_created as f64 / elapsed_secs } else { 0.0 };

    println!();
    println!("Population Complete");
    println!("===================");
    println!("Keys Created:    {}", total_created);
    println!("Data Written:    {:.2} MB", actual_mb);
    println!("Time Elapsed:    {:.2}s", elapsed_secs);
    println!("Throughput:      {:.2} MB/s ({:.0} keys/s)", mb_per_sec, keys_per_sec);

    // Verify with DBSIZE
    let dbsize: u64 = redis::cmd("DBSIZE").query_async(&mut conn).await?;
    println!("Total DB Keys:   {}", dbsize);

    Ok(())
}
