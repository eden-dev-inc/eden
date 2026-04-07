use anyhow::{Context, Result};
use clap::Parser;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

const WORKLOAD_STATS_FILE_ENV: &str = "REDIS_WORKLOAD_STATS_FILE";

/// Run random read/write queries against Redis at a configurable ratio
#[derive(Parser, Debug, Clone)]
pub struct ClientConfig {
    /// Redis URL to query (typically the interlay, e.g. redis://eden-host:5731)
    #[clap(
        long,
        short = 'u',
        env = "REDIS_URL",
        default_value = "redis://localhost:6379"
    )]
    pub url: String,

    /// Key prefix to operate on (must match populated keys)
    #[clap(long, short = 'p', env = "KEY_PREFIX", default_value = "pop")]
    pub prefix: String,

    /// Number of keys to operate on (should match populated key count)
    #[clap(long, short = 'n', env = "NUM_KEYS", default_value = "1000")]
    pub num_keys: u64,

    /// Write percentage (0-100). Reads make up the remainder.
    #[clap(long, short = 'w', env = "WRITE_PCT", default_value = "20")]
    pub write_pct: u8,

    /// Value size in bytes for write operations
    #[clap(long, short = 's', env = "VALUE_SIZE", default_value = "1024")]
    pub value_size: usize,

    /// Number of concurrent workers
    #[clap(long, short = 'c', env = "CONCURRENCY", default_value = "50")]
    pub concurrency: usize,

    /// Duration to run in seconds (0 = run until interrupted)
    #[clap(long, short = 'd', env = "DURATION", default_value = "60")]
    pub duration: u64,

    /// Interval in seconds between stats reports
    #[clap(long, env = "REPORT_INTERVAL", default_value = "5")]
    pub report_interval: u64,
}

struct Stats {
    reads: AtomicU64,
    writes: AtomicU64,
    read_errors: AtomicU64,
    write_errors: AtomicU64,
    read_latency_us: AtomicU64,
    write_latency_us: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            read_errors: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            read_latency_us: AtomicU64::new(0),
            write_latency_us: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            reads: self.reads.load(Ordering::Relaxed),
            writes: self.writes.load(Ordering::Relaxed),
            read_errors: self.read_errors.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
            read_latency_us: self.read_latency_us.load(Ordering::Relaxed),
            write_latency_us: self.write_latency_us.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy)]
struct StatsSnapshot {
    reads: u64,
    writes: u64,
    read_errors: u64,
    write_errors: u64,
    read_latency_us: u64,
    write_latency_us: u64,
}

#[derive(Serialize)]
struct WorkloadStatsFile {
    status: String,
    key_prefix: String,
    num_keys: u64,
    write_pct: u8,
    value_size: usize,
    concurrency: usize,
    duration_secs: u64,
    elapsed_secs: f64,
    total_reads: u64,
    total_writes: u64,
    total_errors: u64,
    ops_per_sec: f64,
    reads_per_sec: f64,
    writes_per_sec: f64,
    avg_read_latency_us: f64,
    avg_write_latency_us: f64,
}

impl StatsSnapshot {
    fn delta(&self, prev: &StatsSnapshot) -> StatsSnapshot {
        StatsSnapshot {
            reads: self.reads - prev.reads,
            writes: self.writes - prev.writes,
            read_errors: self.read_errors - prev.read_errors,
            write_errors: self.write_errors - prev.write_errors,
            read_latency_us: self.read_latency_us - prev.read_latency_us,
            write_latency_us: self.write_latency_us - prev.write_latency_us,
        }
    }
}

fn generate_random_value(size: usize, rng: &mut StdRng) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    rng.fill(&mut buf[..]);
    buf
}

async fn worker(
    client: redis::Client,
    config: Arc<ClientConfig>,
    stats: Arc<Stats>,
    stop: Arc<AtomicBool>,
    _permit: tokio::sync::OwnedSemaphorePermit,
) {
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Worker connection error: {}", e);
            return;
        }
    };

    let mut rng = StdRng::from_entropy();
    let write_threshold = config.write_pct as u32;

    while !stop.load(Ordering::Relaxed) {
        let key_idx = rng.gen_range(0..config.num_keys);
        let key = format!("{}:{}", config.prefix, key_idx);
        let is_write = rng.gen_range(0..100u32) < write_threshold;

        // Detect key type for type-aware operations
        let key_type: String = match redis::cmd("TYPE").arg(&key).query_async(&mut conn).await {
            Ok(t) => t,
            Err(_) => "none".to_string(),
        };

        let (result_ok, elapsed, was_write) = if is_write {
            let start = Instant::now();
            let result = match key_type.as_str() {
                "hash" => {
                    let value = generate_random_value(config.value_size, &mut rng);
                    redis::cmd("HSET")
                        .arg(&key)
                        .arg("field_0")
                        .arg(&value)
                        .query_async::<()>(&mut conn)
                        .await
                }
                "list" => {
                    let value = generate_random_value(config.value_size, &mut rng);
                    redis::cmd("RPUSH")
                        .arg(&key)
                        .arg(&value)
                        .query_async::<()>(&mut conn)
                        .await
                }
                "set" => {
                    let value = generate_random_value(config.value_size, &mut rng);
                    redis::cmd("SADD")
                        .arg(&key)
                        .arg(&value)
                        .query_async::<()>(&mut conn)
                        .await
                }
                "zset" => {
                    let score: f64 = rng.gen_range(0.0..1000000.0);
                    let value = generate_random_value(config.value_size, &mut rng);
                    redis::cmd("ZADD")
                        .arg(&key)
                        .arg(score)
                        .arg(&value)
                        .query_async::<()>(&mut conn)
                        .await
                }
                _ => {
                    // string or none — use SET
                    let value = generate_random_value(config.value_size, &mut rng);
                    redis::cmd("SET")
                        .arg(&key)
                        .arg(&value)
                        .query_async::<()>(&mut conn)
                        .await
                }
            };
            (result.is_ok(), start.elapsed(), true)
        } else {
            let start = Instant::now();
            let result: redis::RedisResult<redis::Value> = match key_type.as_str() {
                "hash" => redis::cmd("HGETALL").arg(&key).query_async(&mut conn).await,
                "list" => {
                    redis::cmd("LRANGE")
                        .arg(&key)
                        .arg(0)
                        .arg(-1)
                        .query_async(&mut conn)
                        .await
                }
                "set" => {
                    redis::cmd("SMEMBERS")
                        .arg(&key)
                        .query_async(&mut conn)
                        .await
                }
                "zset" => {
                    redis::cmd("ZRANGE")
                        .arg(&key)
                        .arg(0)
                        .arg(-1)
                        .query_async(&mut conn)
                        .await
                }
                _ => {
                    // string or none — use GET
                    redis::cmd("GET").arg(&key).query_async(&mut conn).await
                }
            };
            (result.is_ok(), start.elapsed(), false)
        };

        if was_write {
            if result_ok {
                stats.writes.fetch_add(1, Ordering::Relaxed);
                stats
                    .write_latency_us
                    .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
            } else {
                stats.write_errors.fetch_add(1, Ordering::Relaxed);
            }
        } else if result_ok {
            stats.reads.fetch_add(1, Ordering::Relaxed);
            stats
                .read_latency_us
                .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
        } else {
            stats.read_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn print_interval_stats(delta: &StatsSnapshot, elapsed_secs: f64) {
    let total_ops = delta.reads + delta.writes;
    let ops_per_sec = total_ops as f64 / elapsed_secs;
    let reads_per_sec = delta.reads as f64 / elapsed_secs;
    let writes_per_sec = delta.writes as f64 / elapsed_secs;
    let avg_read_lat = if delta.reads > 0 {
        delta.read_latency_us as f64 / delta.reads as f64
    } else {
        0.0
    };
    let avg_write_lat = if delta.writes > 0 {
        delta.write_latency_us as f64 / delta.writes as f64
    } else {
        0.0
    };

    println!(
        "  ops/s: {:.0}  |  reads/s: {:.0}  writes/s: {:.0}  |  avg latency  read: {:.0}µs  write: {:.0}µs  |  errors: {}",
        ops_per_sec, reads_per_sec, writes_per_sec, avg_read_lat, avg_write_lat,
        delta.read_errors + delta.write_errors,
    );
}

fn write_workload_stats_file(
    path: Option<&str>,
    status: &str,
    config: &ClientConfig,
    snapshot: &StatsSnapshot,
    interval: Option<(&StatsSnapshot, f64)>,
    elapsed_secs: f64,
) {
    let Some(path) = path else {
        return;
    };

    let (ops_per_sec, reads_per_sec, writes_per_sec, avg_read_lat, avg_write_lat) =
        if let Some((delta, interval_secs)) = interval {
            let total_ops = delta.reads + delta.writes;
            let ops_per_sec = if interval_secs > 0.0 {
                total_ops as f64 / interval_secs
            } else {
                0.0
            };
            let reads_per_sec = if interval_secs > 0.0 {
                delta.reads as f64 / interval_secs
            } else {
                0.0
            };
            let writes_per_sec = if interval_secs > 0.0 {
                delta.writes as f64 / interval_secs
            } else {
                0.0
            };
            let avg_read_lat = if delta.reads > 0 {
                delta.read_latency_us as f64 / delta.reads as f64
            } else {
                0.0
            };
            let avg_write_lat = if delta.writes > 0 {
                delta.write_latency_us as f64 / delta.writes as f64
            } else {
                0.0
            };
            (
                ops_per_sec,
                reads_per_sec,
                writes_per_sec,
                avg_read_lat,
                avg_write_lat,
            )
        } else {
            let total_ops = snapshot.reads + snapshot.writes;
            let ops_per_sec = if elapsed_secs > 0.0 {
                total_ops as f64 / elapsed_secs
            } else {
                0.0
            };
            let reads_per_sec = if elapsed_secs > 0.0 {
                snapshot.reads as f64 / elapsed_secs
            } else {
                0.0
            };
            let writes_per_sec = if elapsed_secs > 0.0 {
                snapshot.writes as f64 / elapsed_secs
            } else {
                0.0
            };
            let avg_read_lat = if snapshot.reads > 0 {
                snapshot.read_latency_us as f64 / snapshot.reads as f64
            } else {
                0.0
            };
            let avg_write_lat = if snapshot.writes > 0 {
                snapshot.write_latency_us as f64 / snapshot.writes as f64
            } else {
                0.0
            };
            (
                ops_per_sec,
                reads_per_sec,
                writes_per_sec,
                avg_read_lat,
                avg_write_lat,
            )
        };

    let payload = WorkloadStatsFile {
        status: status.to_string(),
        key_prefix: config.prefix.clone(),
        num_keys: config.num_keys,
        write_pct: config.write_pct,
        value_size: config.value_size,
        concurrency: config.concurrency,
        duration_secs: config.duration,
        elapsed_secs,
        total_reads: snapshot.reads,
        total_writes: snapshot.writes,
        total_errors: snapshot.read_errors + snapshot.write_errors,
        ops_per_sec,
        reads_per_sec,
        writes_per_sec,
        avg_read_latency_us: avg_read_lat,
        avg_write_latency_us: avg_write_lat,
    };

    if let Ok(bytes) = serde_json::to_vec(&payload) {
        let _ = std::fs::write(path, bytes);
    }
}

pub async fn run(config: ClientConfig) -> Result<()> {
    if config.write_pct > 100 {
        anyhow::bail!("Write percentage must be 0-100");
    }

    println!("Redis Client");
    println!("=============");
    println!("Redis:           {}", config.url);
    println!("Key Prefix:      {}", config.prefix);
    println!("Key Space:       {} keys", config.num_keys);
    println!(
        "Read/Write:      {}% reads / {}% writes",
        100 - config.write_pct,
        config.write_pct
    );
    println!("Value Size:      {} bytes", config.value_size);
    println!("Concurrency:     {}", config.concurrency);
    if config.duration > 0 {
        println!("Duration:        {}s", config.duration);
    } else {
        println!("Duration:        until interrupted (Ctrl+C)");
    }
    println!();

    let client =
        redis::Client::open(config.url.as_str()).context("Failed to create Redis client")?;

    // Test connection
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("Failed to connect to Redis")?;
    let pong: String = redis::cmd("PING").query_async(&mut conn).await?;
    if pong != "PONG" {
        anyhow::bail!("Unexpected PING response: {}", pong);
    }
    drop(conn);
    println!("Connected to Redis successfully");
    println!();

    let stats = Arc::new(Stats::new());
    let stop = Arc::new(AtomicBool::new(false));
    let workload_stats_file = std::env::var(WORKLOAD_STATS_FILE_ENV).ok();
    write_workload_stats_file(
        workload_stats_file.as_deref(),
        "running",
        &config,
        &stats.snapshot(),
        None,
        0.0,
    );
    let config = Arc::new(config);
    let semaphore = Arc::new(Semaphore::new(config.concurrency));

    // Spawn workers
    let mut handles = Vec::new();
    for _ in 0..config.concurrency {
        let permit = semaphore.clone().acquire_owned().await?;
        let client = client.clone();
        let config = config.clone();
        let stats = stats.clone();
        let stop = stop.clone();

        handles.push(tokio::spawn(worker(client, config, stats, stop, permit)));
    }

    // Set up Ctrl+C handler
    let stop_signal = stop.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        stop_signal.store(true, Ordering::Relaxed);
    });

    // Reporting loop
    let start_time = Instant::now();
    let report_interval = Duration::from_secs(config.report_interval);
    let mut prev_snapshot = stats.snapshot();
    let mut prev_time = Instant::now();

    loop {
        tokio::time::sleep(report_interval).await;

        if stop.load(Ordering::Relaxed) {
            break;
        }

        if config.duration > 0 && start_time.elapsed().as_secs() >= config.duration {
            stop.store(true, Ordering::Relaxed);
            break;
        }

        let now = Instant::now();
        let snapshot = stats.snapshot();
        let delta = snapshot.delta(&prev_snapshot);
        let interval_secs = now.duration_since(prev_time).as_secs_f64();

        print_interval_stats(&delta, interval_secs);
        write_workload_stats_file(
            workload_stats_file.as_deref(),
            "running",
            config.as_ref(),
            &snapshot,
            Some((&delta, interval_secs)),
            start_time.elapsed().as_secs_f64(),
        );

        prev_snapshot = snapshot;
        prev_time = now;
    }

    // Wait for workers to finish
    for handle in handles {
        let _ = handle.await;
    }

    // Final summary
    let elapsed = start_time.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let final_stats = stats.snapshot();
    let total_ops = final_stats.reads + final_stats.writes;
    let total_errors = final_stats.read_errors + final_stats.write_errors;

    println!();
    println!("Run Complete");
    println!("=============");
    println!("Duration:        {:.2}s", elapsed_secs);
    println!("Total Ops:       {}", total_ops);
    println!(
        "  Reads:         {} ({:.0}/s)",
        final_stats.reads,
        final_stats.reads as f64 / elapsed_secs
    );
    println!(
        "  Writes:        {} ({:.0}/s)",
        final_stats.writes,
        final_stats.writes as f64 / elapsed_secs
    );
    println!(
        "Throughput:      {:.0} ops/s",
        total_ops as f64 / elapsed_secs
    );
    if final_stats.reads > 0 {
        println!(
            "Avg Read Lat:    {:.0}µs",
            final_stats.read_latency_us as f64 / final_stats.reads as f64
        );
    }
    if final_stats.writes > 0 {
        println!(
            "Avg Write Lat:   {:.0}µs",
            final_stats.write_latency_us as f64 / final_stats.writes as f64
        );
    }
    if total_errors > 0 {
        println!("Errors:          {}", total_errors);
    }

    write_workload_stats_file(
        workload_stats_file.as_deref(),
        "completed",
        config.as_ref(),
        &final_stats,
        None,
        elapsed_secs,
    );

    Ok(())
}
