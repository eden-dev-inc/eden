//! Runtime-affinity helpers for the proxy hot path.
//!
//! Eden's proxy listener runs on a multi-threaded tokio runtime, but
//! per-connection work is dispatched to **shard runtimes** — single-thread
//! `current_thread` runtimes + `LocalSet`s, one per CPU. Tasks spawned from
//! inside connection-handling code should stay on the shard's thread so that
//! every wakeup is intra-thread (no futex syscall, no cross-core cache
//! invalidation). Tasks spawned from outside a shard (admin handlers,
//! migration loops, replication tasks) should keep going to the global
//! multi-threaded runtime so the global scheduler can balance them.
//!
//! `mark_shard_thread` is called once at the start of each shard
//! runtime's block_on closure, flipping a thread-local to true.
//! `spawn_on_current_runtime` then routes every spawn through
//! `tokio::task::spawn_local` on shard threads and `tokio::spawn`
//! everywhere else — call sites don't need to know which runtime
//! they're on. Hot-path code (parser tasks, processor sub-tasks,
//! multiplexer workers spawned at request time) uses this helper.
//!
//! Lives in `ep-core` so both `redis-core` (which holds the multiplexer
//! dispatch entry point) and `eden-gateway-core` (where the higher-level
//! per-connection helpers run) can read the same thread-local marker.

use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};

thread_local! {
    static IS_SHARD_RUNTIME: Cell<bool> = const { Cell::new(false) };
    /// Numeric id of the shard this thread serves. Only meaningful when
    /// `IS_SHARD_RUNTIME` is true; `usize::MAX` is the sentinel for
    /// "unset" so we can disambiguate before-mark callers.
    static CURRENT_SHARD_ID: Cell<usize> = const { Cell::new(usize::MAX) };
}

/// Process-wide count of shard runtimes. Set once at startup by
/// `ShardRouter::start` via `set_shard_count`; read by per-shard
/// resource budgeters that want to divide a global limit (e.g.
/// `multiplexed_connections`) across shards rather than multiply it.
/// Zero before `set_shard_count` is called; callers should treat
/// `shard_count() == 0` as "no sharding configured" and fall back to
/// the unsharded budget.
static SHARD_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Set the process-wide shard count. Called once at startup, after
/// the shard runtimes have been created. Idempotent only insofar as
/// callers should pass the same value — overwriting at runtime would
/// invalidate already-budgeted per-shard resources.
pub fn set_shard_count(n: usize) {
    SHARD_COUNT.store(n, Ordering::Relaxed);
}

/// Process-wide shard count. `0` means unconfigured (no
/// `set_shard_count` call yet). Use `shard_count_or(default)` for the
/// common "treat unset as 1" pattern.
pub fn shard_count() -> usize {
    SHARD_COUNT.load(Ordering::Relaxed)
}

/// `shard_count()` falling back to `default` when unconfigured. Most
/// per-shard budget calculations want `shard_count_or(1)` so an
/// unsharded process gets the full budget instead of dividing by zero.
pub fn shard_count_or(default: usize) -> usize {
    let n = shard_count();
    if n == 0 { default } else { n }
}

/// Mark the current OS thread as belonging to shard `shard_id`. Called
/// once when each shard thread enters its `block_on` closure. After
/// this returns, `is_shard_runtime()` returns true and
/// `current_shard_id()` returns `Some(shard_id)` on this thread for
/// the remainder of its life.
pub fn mark_shard_thread(shard_id: usize) {
    IS_SHARD_RUNTIME.with(|cell| cell.set(true));
    CURRENT_SHARD_ID.with(|cell| cell.set(shard_id));
}

/// True if the calling code is running on a shard runtime thread (set
/// by `mark_shard_thread`). False on actix workers, the multi-threaded
/// proxy_runtime, and any other thread that hasn't been marked.
pub fn is_shard_runtime() -> bool {
    IS_SHARD_RUNTIME.with(Cell::get)
}

/// The shard id of the calling thread, if it has been marked. Returns
/// `None` from non-shard threads. Used by metric emitters to label
/// per-shard counters / gauges without plumbing the id through every
/// call site.
pub fn current_shard_id() -> Option<usize> {
    if !is_shard_runtime() {
        return None;
    }
    let id = CURRENT_SHARD_ID.with(Cell::get);
    if id == usize::MAX { None } else { Some(id) }
}

/// Spawn a future on whichever runtime the caller is currently running
/// on:
///
/// - On a shard thread: `tokio::task::spawn_local` — the future stays
///   on this shard's `LocalSet`, no cross-thread wakeups.
/// - Anywhere else: `tokio::spawn` — the global multi-threaded runtime
///   balances it across cores.
///
/// Both branches return the same `JoinHandle<F::Output>` type so callers
/// can `.await` or `.abort_handle()` uniformly. Future is bounded
/// `Send + 'static` (the strictest of the two spawners) so the same
/// signature works on either side.
///
/// **Use this for per-connection helper tasks** spawned from inside
/// connection-handling code (parsers, processor sub-tasks, etc.) so
/// they inherit the connection's affinity.
///
/// **Do NOT use this for background tasks** that should outlive any
/// single connection (migration data movement, periodic GC, replication,
/// cancel forwarding) — those must always go to the global runtime, so
/// they should call `tokio::spawn` directly. Pinning a long-lived
/// background task to one shard's CPU breaks load balancing and ties
/// the task's progress to whatever client work that shard is doing.
pub fn spawn_on_current_runtime<F>(fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    if is_shard_runtime() {
        if let Some(shard_id) = current_shard_id() {
            record_local_task_spawned(shard_id);
        }
        tokio::task::spawn_local(fut)
    } else {
        tokio::spawn(fut)
    }
}

/// Increment `proxy.shard.local_tasks_spawned_total{org_uuid="_system",shard_id}`.
/// Pulled out as a small helper so the metric path is the same in
/// integration tests where the global metrics handle isn't installed.
fn record_local_task_spawned(shard_id: usize) {
    if let Some(metrics) = telemetry::global_metrics() {
        let id_str = shard_id.to_string();
        metrics
            .proxy()
            .record_shard_local_task_spawned(&[("org_uuid", telemetry::labels::SYSTEM_ORG_UUID), ("shard_id", id_str.as_str())]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unmarked_thread_reports_global() {
        // Each test gets its own thread (cargo test default) — no
        // mark_shard_thread call here, so the flag stays false.
        assert!(!is_shard_runtime());
        assert!(current_shard_id().is_none());
    }

    #[test]
    fn mark_shard_thread_flips_flag_for_this_thread() {
        assert!(!is_shard_runtime());
        mark_shard_thread(7);
        assert!(is_shard_runtime());
        assert_eq!(current_shard_id(), Some(7));
    }
}
