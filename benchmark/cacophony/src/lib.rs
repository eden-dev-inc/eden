pub mod arrival;
pub mod backend;
pub mod connection;
pub mod recorder;
pub mod scenario;

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use serde::Serialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use arrival::{CommandSpec, generate_arrivals, planned_set_counts_for_arrivals};
use connection::{ConnectionPool, OutboundCommand, connect_one};
use recorder::{CommandOutcome, CompletedCommand, PhaseSummary, Recorder};
use scenario::{KeyspaceConfig, Phase, Scenario, parse_duration};

/// Maximum number of commands that can wait in the shared dispatch queue
/// before excess arrivals are shed.
const MAX_QUEUE_DEPTH: usize = 10_000;

#[derive(Serialize)]
pub struct ScenarioResult {
    pub scenario: String,
    pub target: String,
    pub loadgen_shards: usize,
    pub phases: Vec<PhaseResult>,
}

#[derive(Serialize)]
pub struct PhaseResult {
    pub name: String,
    pub duration_secs: f64,
    pub elapsed_secs: f64,
    pub target_rate: f64,
    pub connections: u32,
    pub pipeline_depth: u32,
    pub request_encoding: &'static str,
    pub precomputed_request_bytes: u64,
    pub offered: u64,
    pub shed: u64,
    pub retired_workers: u64,
    #[serde(flatten)]
    pub summary: PhaseSummary,
}

pub async fn run_scenario(scenario: &Scenario, target: &str) -> Result<ScenarioResult, io::Error> {
    run_scenario_with_shards(scenario, target, 1).await
}

pub async fn run_scenario_with_shards(scenario: &Scenario, target: &str, loadgen_shards: usize) -> Result<ScenarioResult, io::Error> {
    assert!(loadgen_shards > 0, "loadgen_shards must be > 0");

    let keyspace = scenario.keyspace.clone().unwrap_or_default();
    let targets = parse_targets(target)?;
    let mut phases = Vec::new();

    for (i, phase) in scenario.phases.iter().enumerate() {
        let duration = parse_duration(&phase.duration);
        let rate = phase.target_rate();
        eprintln!(
            "[{}/{}] phase '{}': {} connections, pipeline={}, {:.0} req/s, {:.0}s, shards={}",
            i + 1,
            scenario.phases.len(),
            phase.name,
            phase.connections,
            phase.pipeline_depth(),
            rate,
            duration.as_secs_f64(),
            loadgen_shards,
        );

        let result = run_phase(phase, &targets, &keyspace, loadgen_shards).await?;

        eprintln!(
            "  done: offered={} completed={} errors={} (redis={} conn={}) shed={} integrity_failures={} race_suspects={} elapsed={:.3}s",
            result.offered,
            result.summary.completed,
            result.summary.errors,
            result.summary.redis_errors,
            result.summary.connection_errors,
            result.shed,
            result.summary.integrity_failures,
            result.summary.integrity_race_suspects,
            result.elapsed_secs,
        );
        eprintln!(
            "  service p50={}μs p99={}μs | sojourn p50={}μs p99={}μs | queue p50={}μs p99={}μs",
            result.summary.service_latency_us.p50,
            result.summary.service_latency_us.p99,
            result.summary.sojourn_latency_us.p50,
            result.summary.sojourn_latency_us.p99,
            result.summary.queue_delay_us.p50,
            result.summary.queue_delay_us.p99,
        );

        phases.push(result);
    }

    Ok(ScenarioResult {
        scenario: scenario.meta.name.clone(),
        target: target.to_string(),
        loadgen_shards,
        phases,
    })
}

fn parse_targets(target: &str) -> Result<Vec<String>, io::Error> {
    let targets = target.split(',').map(str::trim).filter(|target| !target.is_empty()).map(ToOwned::to_owned).collect::<Vec<_>>();

    if targets.is_empty() {
        Err(io::Error::new(io::ErrorKind::InvalidInput, "--target must include at least one host:port"))
    } else {
        Ok(targets)
    }
}

/// A command ready for dispatch, with its scheduled time already resolved.
struct ReadyCommand {
    spec: CommandSpec,
    planned_set_count: u16,
    scheduled_time: Instant,
}

#[derive(Default)]
struct PhaseCounters {
    elapsed_secs: f64,
    connections: u32,
    precomputed_request_bytes: u64,
    offered: u64,
    shed: u64,
    retired_workers: u64,
}

impl PhaseCounters {
    fn merge(&mut self, other: Self) {
        self.elapsed_secs = self.elapsed_secs.max(other.elapsed_secs);
        self.connections += other.connections;
        self.precomputed_request_bytes += other.precomputed_request_bytes;
        self.offered += other.offered;
        self.shed += other.shed;
        self.retired_workers += other.retired_workers;
    }
}

async fn run_phase(phase: &Phase, targets: &[String], keyspace: &KeyspaceConfig, loadgen_shards: usize) -> Result<PhaseResult, io::Error> {
    let pipeline_depth = phase.pipeline_depth();
    let duration = parse_duration(&phase.duration);

    // Recorder channel — all workers send outcomes here.
    let (recorder_tx, recorder_rx) = mpsc::unbounded_channel();
    let recorder = Recorder::new();
    let recorder_handle = tokio::spawn(recorder.run(recorder_rx));

    let mut shard_handles = Vec::with_capacity(loadgen_shards);
    for shard_index in 0..loadgen_shards {
        let shard_phase = if loadgen_shards == 1 {
            phase.clone()
        } else {
            phase.shard_for_loadgen(shard_index, loadgen_shards)
        };
        let shard_keyspace = if loadgen_shards == 1 {
            keyspace.clone()
        } else {
            keyspace.shard_for_loadgen(shard_index)
        };
        let target = targets[shard_index % targets.len()].clone();
        let progress_label = if loadgen_shards == 1 {
            String::new()
        } else {
            format!("shard {}/{} target={} ", shard_index + 1, loadgen_shards, target)
        };

        let recorder_tx = recorder_tx.clone();
        shard_handles.push(tokio::spawn(async move {
            drive_phase_shard(&shard_phase, &target, &shard_keyspace, recorder_tx, progress_label).await
        }));
    }
    drop(recorder_tx);

    let mut counters = PhaseCounters::default();
    for handle in shard_handles {
        let shard_counters = handle.await.map_err(|e| io::Error::other(format!("loadgen shard task failed: {e}")))??;
        counters.merge(shard_counters);
    }

    let summary = recorder_handle.await.map_err(|e| io::Error::other(format!("recorder task failed: {e}")))?;

    Ok(PhaseResult {
        name: phase.name.clone(),
        duration_secs: duration.as_secs_f64(),
        elapsed_secs: counters.elapsed_secs,
        target_rate: phase.target_rate(),
        connections: counters.connections,
        pipeline_depth,
        request_encoding: "precomputed-resp",
        precomputed_request_bytes: counters.precomputed_request_bytes,
        offered: counters.offered,
        shed: counters.shed,
        retired_workers: counters.retired_workers,
        summary,
    })
}

async fn drive_phase_shard(
    phase: &Phase,
    target: &str,
    keyspace: &KeyspaceConfig,
    recorder_tx: mpsc::UnboundedSender<CompletedCommand>,
    progress_label: String,
) -> Result<PhaseCounters, io::Error> {
    let pipeline_depth = phase.pipeline_depth();
    let num_connections = phase.connections;

    assert!(num_connections > 0, "connections must be > 0");
    assert!(pipeline_depth > 0, "pipeline_depth must be > 0");

    // Generate a single global arrival schedule.
    eprintln!("  {progress_label}generating arrivals...");
    let arrivals = generate_arrivals(phase, keyspace);
    let planned_set_counts = planned_set_counts_for_arrivals(&arrivals);
    let total_arrivals = arrivals.len() as u64;
    let precomputed_request_bytes: u64 = arrivals.iter().map(|arrival| arrival.spec.encoded.len() as u64).sum();
    eprintln!("  {progress_label}{total_arrivals} total arrivals generated ({precomputed_request_bytes} precomputed RESP bytes)");

    // Connect.
    eprintln!("  {progress_label}connecting {num_connections} connections to {target}...");
    let pool = ConnectionPool::connect(target, num_connections, pipeline_depth, recorder_tx.clone())
        .await
        .map_err(|e| io::Error::new(e.kind(), format!("failed to connect to {target}: {e}")))?;
    eprintln!("  {progress_label}connected, dispatching...");

    // Shared command channel: scheduler pushes, workers pull.
    // Bounded to MAX_QUEUE_DEPTH to provide backpressure.
    let (cmd_tx, cmd_rx) = async_channel::bounded::<ReadyCommand>(MAX_QUEUE_DEPTH);

    // Shared counters.
    let dispatched = Arc::new(AtomicU64::new(0));
    let shed = Arc::new(AtomicU64::new(0));
    let retired_workers = Arc::new(AtomicU64::new(0));

    // Anchor instants.
    let tokio_start = tokio::time::Instant::now();
    let std_start = Instant::now();

    // --- Scheduler task ---
    // Sleeps until each arrival's absolute time, then pushes to the shared channel.
    // If the channel is full (all workers backed up), the command is shed.
    let sched_shed = shed.clone();
    let scheduler_handle: JoinHandle<()> = tokio::spawn(async move {
        for (arrival, planned_set_count) in arrivals.into_iter().zip(planned_set_counts) {
            // Sleep until this arrival's scheduled time.
            tokio::time::sleep_until(tokio_start + arrival.offset).await;

            let scheduled_time = std_start + arrival.offset;
            let cmd = ReadyCommand { spec: arrival.spec, planned_set_count, scheduled_time };

            // Non-blocking push. If the queue is full, shed.
            match cmd_tx.try_send(cmd) {
                Ok(()) => {}
                Err(_) => {
                    sched_shed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        // Close the channel — workers will drain remaining items then exit.
        cmd_tx.close();
    });

    // --- Worker tasks ---
    // Each worker owns one connection. It waits for a pipeline slot (capacity-driven),
    // then pulls the next command from the shared channel (work-conserving).
    // If connection A is slow, connection B picks up the slack.
    let mut worker_handles: Vec<JoinHandle<()>> = Vec::with_capacity(num_connections as usize);

    let target_str: Arc<str> = Arc::from(target);

    for conn in &pool.connections {
        let mut slots = conn.slots.clone();
        let mut writer_tx = conn.cmd_tx.clone();
        let conn_id = conn.id;
        let cmd_rx = cmd_rx.clone();
        let recorder_tx = recorder_tx.clone();
        let dispatched = dispatched.clone();
        let retired_workers = retired_workers.clone();
        let target_str = target_str.clone();

        let handle = tokio::spawn(async move {
            loop {
                // Wait for pipeline capacity (capacity-driven wakeup).
                let permit = match slots.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break, // semaphore closed
                };

                // Pull the next ready command (work-conserving across pool).
                let ready = match cmd_rx.recv().await {
                    Ok(cmd) => cmd,
                    Err(_) => {
                        // Channel closed and drained — no more work.
                        drop(permit);
                        break;
                    }
                };

                // Dispatch.
                let cmd = OutboundCommand {
                    command_type: ready.spec.command_type,
                    key: ready.spec.key,
                    planned_set_count: ready.planned_set_count,
                    value: ready.spec.value,
                    encoded: ready.spec.encoded,
                    scheduled_time: ready.scheduled_time,
                    permit,
                };

                if let Err(returned) = writer_tx.send(cmd).map_err(|e| e.0) {
                    // Writer is dead. Record this command as an error.
                    let now = Instant::now();
                    let _ = recorder_tx.send(CompletedCommand {
                        scheduled_time: returned.scheduled_time,
                        send_time: now,
                        recv_time: now,
                        connection_id: conn_id,
                        command_type: returned.command_type,
                        key: returned.key,
                        planned_set_count: returned.planned_set_count,
                        set_value: returned.value,
                        request_wire_bytes: 0,
                        response_wire_bytes: 0,
                        response_payload_bytes: 0,
                        outcome: CommandOutcome::ConnectionError(format!("connection {conn_id} writer dead")),
                    });

                    // Reconnect instead of retiring.
                    eprintln!("  worker {conn_id}: connection dead, reconnecting...");
                    match connect_one(&target_str, conn_id, pipeline_depth, recorder_tx.clone()).await {
                        Ok((new_tx, new_slots)) => {
                            writer_tx = new_tx;
                            slots = new_slots;
                            eprintln!("  worker {conn_id}: reconnected");
                        }
                        Err(e) => {
                            retired_workers.fetch_add(1, Ordering::Relaxed);
                            eprintln!("  worker {conn_id}: reconnect failed ({e}), retiring");
                            break;
                        }
                    }
                    continue;
                }

                dispatched.fetch_add(1, Ordering::Relaxed);
            }
        });

        worker_handles.push(handle);
    }

    // Drop our recorder_tx so the recorder can finalize after workers finish.
    drop(recorder_tx);

    // Progress reporting.
    let progress_dispatched = dispatched.clone();
    let progress_shed = shed.clone();
    let progress_retired = retired_workers.clone();
    let progress_total = total_arrivals;
    let progress_conns = num_connections;
    let progress_label_for_task = progress_label.clone();
    let progress_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        interval.tick().await; // skip first immediate tick
        loop {
            interval.tick().await;
            let d = progress_dispatched.load(Ordering::Relaxed);
            let s = progress_shed.load(Ordering::Relaxed);
            let r = progress_retired.load(Ordering::Relaxed);
            let elapsed = std_start.elapsed().as_secs_f64();
            let rate = d as f64 / elapsed;
            let live = progress_conns as u64 - r;
            eprintln!(
                "  {progress_label_for_task}[{elapsed:.1}s] dispatched={d}/{progress_total} shed={s} ({rate:.0} req/s) workers={live}/{progress_conns}"
            );
            if d + s >= progress_total {
                break;
            }
        }
    });

    // Wait for scheduler to finish pushing all arrivals.
    let _ = scheduler_handle.await;

    // Wait for all workers to drain the channel and finish.
    for handle in worker_handles {
        let _ = handle.await;
    }
    progress_handle.abort();

    // Wait for in-flight responses.
    eprintln!("  {progress_label}all arrivals scheduled, waiting for in-flight responses...");
    pool.shutdown().await;
    let elapsed_secs = std_start.elapsed().as_secs_f64();

    let total_shed = shed.load(Ordering::Relaxed);
    let total_retired = retired_workers.load(Ordering::Relaxed);

    Ok(PhaseCounters {
        elapsed_secs,
        connections: num_connections,
        precomputed_request_bytes,
        offered: total_arrivals,
        shed: total_shed,
        retired_workers: total_retired,
    })
}

#[cfg(test)]
mod tests {
    use super::parse_targets;

    #[test]
    fn parse_targets_accepts_comma_separated_targets() {
        assert_eq!(
            parse_targets("127.0.0.1:6379, 127.0.0.1:6380").expect("targets"),
            vec!["127.0.0.1:6379", "127.0.0.1:6380"]
        );
    }

    #[test]
    fn parse_targets_rejects_empty_target_list() {
        assert!(parse_targets(" , ").is_err());
    }
}
