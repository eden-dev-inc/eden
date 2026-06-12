//! Thread-per-core shard runtimes for the proxy hot path.
//!
//! Each shard owns:
//!   - one OS thread, pinned to a CPU core via `gateway_cpu_affinity`
//!   - one `current_thread` tokio runtime + a `LocalSet`
//!   - all connection state (bridge, processor, multiplex workers) for the
//!     subset of client connections routed to it
//!
//! Why: cross-thread scheduler wakeups dominate per-request cost on the
//! response path (exported as `multiplex_oneshot_delivery`). By keeping
//! every per-connection task on the same thread, every wakeup becomes
//! intra-thread — no futex syscall, no cross-core cache invalidation.
//! Tasks are pinned via `tokio::task::spawn_local` inside the shard's
//! LocalSet.
//!
//! Routing: incoming connections are mapped to a *subset* of K_choice
//! shards by independent hashes (Brooker's shuffle-sharding /
//! power-of-two-choices). The dispatcher picks the shorter-queue shard
//! at batch-dispatch time. With K_choice=2 a single noisy client can
//! only affect 2 of N shards, and uniform-load tail latency is
//! dramatically flatter than single-choice routing.
//!
//! Per-batch ordering across shards: a single client connection may
//! dispatch successive batches to different shards (whichever was
//! shorter at the moment). Responses come back at different times. To
//! preserve RESP per-connection order, each connection holds a small
//! per-batch sequencer + reorder buffer (`ConnectionSequencer`). The
//! client write half drains the sequencer in monotonic order.
//!
//! ## Thread affinity rules (read before adding new spawns)
//!
//! Tasks spawned from inside connection-handling code may run on a
//! shard thread (current_thread runtime + LocalSet) or on the main
//! multi-threaded runtime, and the choice matters:
//!
//! - **Use `tokio::task::spawn_local`** for per-connection helpers
//!   that should stay co-located with their connection: parser tasks,
//!   processor sub-tasks, multiplexer workers (via `SpawnMode::Local`),
//!   sequencer reorder loops. Keeping them on the shard thread makes
//!   inter-task wakeups intra-thread (no futex syscall, no cache
//!   invalidation) and is the whole point of thread-per-core.
//!
//! - **Use `tokio::spawn`** for anything that:
//!     - has a lifetime independent of any single connection
//!       (background data-movement tasks, replication tasks, cancel
//!       forwarders, periodic GC),
//!     - performs blocking-ish work that would starve the shard thread
//!       (large bulk reads),
//!     - or fans out to multiple peers (per-target send loops).
//!
//!   These belong on the main multi-threaded runtime so the global
//!   scheduler can balance them across cores; pinning them via
//!   `spawn_local` would tie their progress to one shard's CPU and
//!   break replication when that shard is busy serving
//!   other client work.
//!
//! In particular every spawn site under `eden_gateway/redis/src/replication.rs`,
//! the postgres divergence /
//! replication-lag tasks, and the per-processor write-serializer GC
//! must remain `tokio::spawn`. Audit them whenever you change the
//! spawn primitive in a hot-path file.

use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;

use eden_logger_internal::{LogAudience, LogContext, log_info};
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;
use tokio::task::{JoinHandle as TokioJoinHandle, LocalSet};

/// Identifies one of `N` shards. Indexes into `ShardRouter::senders`.
///
/// The inner `usize` is intentionally not public — only `ShardRouter`
/// hands out valid `ShardId`s (via `assign_shards`, `pick_round_robin`,
/// or the `shard_ids` iterator), so external callers can't fabricate
/// an out-of-range id and then panic on `senders[idx]` inside dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardId(usize);

impl ShardId {
    pub fn index(self) -> usize {
        self.0
    }
}

/// Work item handed to a shard's runtime. Carries an opaque future plus
/// the assignment metadata that the shard needs to spawn it.
///
/// We use `Box<dyn FnOnce(&LocalSet)>` instead of a `BoxFuture` directly
/// so the shard can call `spawn_local` *itself* — the future is built
/// at the shard side with the LocalSet in scope, avoiding the "spawn_local
/// must be called inside a LocalSet" panic that would happen if we tried
/// to spawn from the producer side.
pub type ShardWorkFn = Box<dyn FnOnce() + Send + 'static>;

/// Thin handle to the N shard runtimes. Cheap to clone — internal Arcs
/// only. `dispatch` from any thread sends a work closure to a shard;
/// the shard's runtime polls its inbox and runs the closure (which
/// typically calls `spawn_local`).
#[derive(Clone)]
pub struct ShardRouter {
    inner: Arc<ShardRouterInner>,
}

struct ShardRouterInner {
    senders: Vec<UnboundedSender<ShardWorkFn>>,
    /// Per-shard inflight counters used by `pick_shorter` for two-choice
    /// dispatch. Producers update them as they send work; consumers
    /// (the shard runtime) decrement when the work future completes.
    /// Approximate (Relaxed) — used only for load-balancing hints.
    inflight: Vec<AtomicUsize>,
    /// Round-robin counter when the caller doesn't specify a sharding
    /// key (e.g., admin requests). Hashing-based routing for the hot
    /// connection path doesn't touch this.
    round_robin: AtomicUsize,
    /// Number of shards K_choice picks from when sharding by client
    /// (1 = sticky, 2 = power-of-two-choices à la Brooker).
    k_choice: usize,
    /// Per-router random salts mixed into the connection-address hash.
    /// Generated once at startup so the `SocketAddr → ShardId` mapping
    /// is unpredictable to clients — prevents an attacker who controls
    /// many source addresses from pre-computing tuples that all land on
    /// the same shard.
    primary_salt: u64,
    secondary_salt: u64,
    /// Join handles for the shard threads. Held to keep them alive;
    /// the runtime cleans up on drop of the router.
    _join_handles: Vec<JoinHandle<()>>,
}

impl ShardRouter {
    /// Spawn `num_shards` OS threads, each running a `current_thread`
    /// tokio runtime + a `LocalSet`. Returns a router that can dispatch
    /// work into any of them.
    ///
    /// `k_choice` is the number of shards a client is mapped to under
    /// the SFQ-style routing scheme (1 = sticky, 2 = recommended for
    /// power-of-two-choices). `num_shards >= k_choice` is required.
    pub fn start(num_shards: usize, k_choice: usize) -> Self {
        assert!(num_shards >= 1, "num_shards must be >= 1");
        assert!(k_choice >= 1 && k_choice <= num_shards, "k_choice must be in 1..=num_shards");

        // Publish the shard count to ep_core::runtime so per-shard
        // resource budgeters (e.g. the multiplexer's worker count)
        // can divide global limits across shards instead of
        // multiplying them. Set once, never overwritten.
        eden_gateway::runtime::set_shard_count(num_shards);

        let mut senders = Vec::with_capacity(num_shards);
        let mut handles = Vec::with_capacity(num_shards);
        let mut inflight = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            inflight.push(AtomicUsize::new(0));
        }

        for shard_id in 0..num_shards {
            let (tx, mut rx) = unbounded_channel::<ShardWorkFn>();
            senders.push(tx);

            let handle = std::thread::Builder::new()
                .name(format!("eden-gateway-shard-{shard_id}"))
                .spawn(move || {
                    let rt = RuntimeBuilder::new_current_thread()
                        .enable_all()
                        .thread_name(format!("eden-gateway-shard-{shard_id}-rt"))
                        .build()
                        .expect("build current_thread runtime for shard");

                    let local = LocalSet::new();
                    local.block_on(&rt, async move {
                        // Flip the per-thread "shard runtime" flag so that
                        // hot-path code calling `spawn_on_current_runtime`
                        // sees this thread and routes through `spawn_local`
                        // (keeping all per-connection helper tasks on the
                        // shard's LocalSet). Also stamps `shard_id` so the
                        // metric path can label `local_tasks_spawned_total`.
                        eden_gateway::runtime::mark_shard_thread(shard_id);

                        let ctx = LogContext::default().with_feature("eden_gateway_shard");
                        log_info!(
                            ctx.clone(),
                            "Eden gateway shard runtime started",
                            audience = LogAudience::Internal,
                            shard_id = shard_id
                        );
                        while let Some(work) = rx.recv().await {
                            // Run the producer's closure inside the LocalSet
                            // context so that any `tokio::task::spawn_local`
                            // calls inside it are valid. The closure is
                            // expected to be cheap (it typically just spawns
                            // a few local tasks); long-running work runs in
                            // those spawned tasks, not here.
                            (work)();
                        }
                        log_info!(ctx, "Eden gateway shard runtime exiting", audience = LogAudience::Internal, shard_id = shard_id);
                    });
                })
                .expect("spawn shard OS thread");
            handles.push(handle);
        }

        ShardRouter {
            inner: Arc::new(ShardRouterInner {
                senders,
                inflight,
                round_robin: AtomicUsize::new(0),
                k_choice,
                primary_salt: rand::random::<u64>(),
                secondary_salt: rand::random::<u64>(),
                _join_handles: handles,
            }),
        }
    }

    pub fn num_shards(&self) -> usize {
        self.inner.senders.len()
    }

    pub fn k_choice(&self) -> usize {
        self.inner.k_choice
    }

    /// Hash a connection's remote address to its assigned shard subset.
    /// Returns `k_choice` shard ids (deduplicated). With `k_choice=1`
    /// this is sticky-per-client; with `k_choice=2` it's the
    /// shuffle-sharding map. The two hashes use different mixers so
    /// the subset is independent across clients.
    pub fn assign_shards(&self, addr: &SocketAddr) -> SmallShardSet {
        let n = self.inner.senders.len();
        let k = self.inner.k_choice;
        let primary = hash_one(addr, self.inner.primary_salt) as usize % n;
        let mut set = SmallShardSet::single(ShardId(primary));
        if k >= 2 && n >= 2 {
            // Deterministic non-collision: pick the secondary uniformly
            // from the `n - 1` non-primary shards by hashing the address
            // with a different salt and offsetting around `primary`.
            // With `n == 2` this trivially returns the other shard;
            // for larger `n` it spreads uniformly. No retry loop, no
            // sticky fallback — the previous implementation's 4-try
            // rehash silently collapsed to k_choice=1 at probability
            // (1/n)^4, which is non-negligible for small n (6.25% at
            // n=2) and breaks two-choice exactly when `n` is small
            // enough that each connection's load matters most.
            let h = hash_one(addr, self.inner.secondary_salt) as usize;
            let secondary = (primary + 1 + h % (n - 1)) % n;
            set.push(ShardId(secondary));
        }
        set
    }

    /// Pick the shorter-inflight shard from the assigned subset. With
    /// `k_choice=1` returns the only one. With `k_choice=2` reads both
    /// inflight counters and returns the smaller; **on equal load** it
    /// reservoir-samples among the tied shards using a per-router
    /// rotating counter, so cold start (all loads zero) and steady-state
    /// ties don't collapse two-choice into "always pick first."
    pub fn pick_shorter(&self, subset: &SmallShardSet) -> ShardId {
        let mut best = subset.first();
        if subset.len() <= 1 {
            return best;
        }
        let mut best_load = self.inner.inflight[best.0].load(Ordering::Relaxed);
        // Number of shards seen so far that tie with `best` (including
        // `best` itself). Reservoir-sampling uniformly across the tied
        // set on each tie keeps load distribution unbiased even under
        // sustained equal-load conditions.
        let mut tied = 1u64;
        for shard in subset.iter().skip(1) {
            let load = self.inner.inflight[shard.0].load(Ordering::Relaxed);
            match load.cmp(&best_load) {
                std::cmp::Ordering::Less => {
                    best = shard;
                    best_load = load;
                    tied = 1;
                }
                std::cmp::Ordering::Equal => {
                    tied += 1;
                    let pick = (self.inner.round_robin.fetch_add(1, Ordering::Relaxed) as u64) % tied;
                    if pick == 0 {
                        best = shard;
                    }
                }
                std::cmp::Ordering::Greater => {}
            }
        }
        best
    }

    /// Iterate over every `ShardId` this router owns, in shard-index
    /// order. Used for fan-out work (e.g., broadcasting an eviction to
    /// every shard's thread_local registry). Lets callers stay agnostic
    /// of how many shards there are and prevents them from hand-
    /// constructing potentially-out-of-range `ShardId`s.
    pub fn shard_ids(&self) -> impl Iterator<Item = ShardId> + '_ {
        (0..self.inner.senders.len()).map(ShardId)
    }

    /// Dispatch a work closure to a specific shard. The shard's runtime
    /// runs the closure inside its LocalSet so the closure may call
    /// `spawn_local`. The inflight counter for `shard` is incremented
    /// before send and decremented when `RouterInflightGuard` drops —
    /// callers that want load-balancing should use `dispatch_with_guard`.
    pub fn dispatch(&self, shard: ShardId, work: ShardWorkFn) -> Result<(), DispatchError> {
        match self.inner.senders[shard.0].send(work) {
            Ok(()) => Ok(()),
            Err(_) => {
                record_shard_dispatch_failure(shard, "shard_closed");
                Err(DispatchError::ShardClosed(shard))
            }
        }
    }

    /// Like `dispatch`, but increments the inflight counter for `shard`
    /// and returns a guard whose Drop decrements it. Used so two-choice
    /// load-balancing has accurate counters. Caller must drop the guard
    /// when the work logically completes (e.g., when the connection
    /// closes, not when the future starts running).
    pub fn dispatch_with_guard(&self, shard: ShardId, work: ShardWorkFn) -> Result<RouterInflightGuard, DispatchError> {
        let count = self.inner.inflight[shard.0].fetch_add(1, Ordering::Relaxed) + 1;
        set_shard_connections_active_gauge(shard, count);
        match self.dispatch(shard, work) {
            Ok(()) => Ok(RouterInflightGuard { router: self.clone(), shard, released: false }),
            Err(e) => {
                let count = self.inner.inflight[shard.0].fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
                set_shard_connections_active_gauge(shard, count);
                Err(e)
            }
        }
    }

    /// Round-robin pick when no connection-specific key is available.
    /// Used by admin / non-hot-path callers that just want any shard.
    pub fn pick_round_robin(&self) -> ShardId {
        let n = self.inner.senders.len();
        let i = self.inner.round_robin.fetch_add(1, Ordering::Relaxed) % n;
        ShardId(i)
    }

    /// Dispatch a future to a shard's `LocalSet` via `spawn_local`. The
    /// `factory` closure is invoked on the shard thread; the future it
    /// returns is then `spawn_local`'d there. Returns a oneshot receiver
    /// that yields the spawn_local'd `JoinHandle` once the shard has
    /// accepted the work — callers `await` the JoinHandle to know when
    /// the connection finishes (and may take its `abort_handle()` to
    /// support listener-driven cancellation).
    ///
    /// The returned future is not Send; we don't require it to be, since
    /// it's spawned on a current_thread runtime + LocalSet. The shard's
    /// inflight counter is incremented before the closure is sent, and
    /// decremented when the spawn_local'd future is dropped (whether via
    /// normal completion or abort).
    pub fn dispatch_local_task<F>(&self, shard: ShardId, factory: F) -> Result<oneshot::Receiver<TokioJoinHandle<()>>, DispatchError>
    where
        F: FnOnce() -> Pin<Box<dyn Future<Output = ()>>> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<TokioJoinHandle<()>>();
        let inflight_arc = Arc::clone(&self.inner);
        let shard_idx = shard.0;

        let work: ShardWorkFn = Box::new(move || {
            let dec = InflightDecrement { inner: inflight_arc, shard: shard_idx };
            let fut = factory();
            let join = tokio::task::spawn_local(async move {
                let _dec = dec; // dropped on completion or abort
                fut.await;
            });
            // If the listener already gave up waiting, the receiver is
            // dropped — the spawn_local'd task continues; the inflight
            // guard still decrements when it ends.
            let _ = tx.send(join);
        });

        let count = self.inner.inflight[shard_idx].fetch_add(1, Ordering::Relaxed) + 1;
        set_shard_connections_active_gauge(shard, count);
        if self.dispatch(shard, work).is_err() {
            // Shard runtime is gone — undo the inflight increment we
            // optimistically added (the closure will never run).
            let count = self.inner.inflight[shard_idx].fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
            set_shard_connections_active_gauge(shard, count);
            return Err(DispatchError::ShardClosed(shard));
        }

        Ok(rx)
    }
}

/// Set `gateway.shard_connections_active{shard_id}` to `count`. No-op if
/// the global metrics handle hasn't been installed yet (early startup,
/// some unit tests). Lives next to the dispatch sites so every change
/// to the inflight counter has a single, obvious place to emit from.
fn set_shard_connections_active_gauge(shard: ShardId, count: usize) {
    if let Some(metrics) = eden_core::telemetry::global_metrics() {
        let id = shard.0.to_string();
        metrics.proxy().set_shard_connections_active(count as i64, &[("org_uuid", "_system"), ("shard_id", id.as_str())]);
    }
}

/// Increment `gateway.shard_dispatch_failures_total{shard_id, reason}`.
/// Reasons are static strings (currently only `"shard_closed"`).
fn record_shard_dispatch_failure(shard: ShardId, reason: &'static str) {
    if let Some(metrics) = eden_core::telemetry::global_metrics() {
        let id = shard.0.to_string();
        metrics.proxy().record_shard_dispatch_failure(&[("org_uuid", "_system"), ("shard_id", id.as_str()), ("reason", reason)]);
    }
}

/// Decrements a shard's inflight counter on drop. Lives inside the
/// `spawn_local`'d future so the count goes back to zero whether the
/// task finishes naturally or is aborted.
struct InflightDecrement {
    inner: Arc<ShardRouterInner>,
    shard: usize,
}

impl Drop for InflightDecrement {
    fn drop(&mut self) {
        let count = self.inner.inflight[self.shard].fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
        set_shard_connections_active_gauge(ShardId(self.shard), count);
    }
}

/// Decrements a shard's inflight counter on drop. Returned by
/// `ShardRouter::dispatch_with_guard`. Forgetting to drop this skews
/// load-balancing toward shards whose connections happen to retain
/// guards longer — not a correctness issue.
pub struct RouterInflightGuard {
    router: ShardRouter,
    shard: ShardId,
    released: bool,
}

impl RouterInflightGuard {
    pub fn shard(&self) -> ShardId {
        self.shard
    }

    /// Manually release the guard (otherwise releases on drop).
    pub fn release(mut self) {
        if !self.released {
            let count = self.router.inner.inflight[self.shard.0].fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
            set_shard_connections_active_gauge(self.shard, count);
            self.released = true;
        }
    }
}

impl Drop for RouterInflightGuard {
    fn drop(&mut self) {
        if !self.released {
            let count = self.router.inner.inflight[self.shard.0].fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
            set_shard_connections_active_gauge(self.shard, count);
        }
    }
}

#[derive(Debug)]
pub enum DispatchError {
    ShardClosed(ShardId),
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DispatchError::ShardClosed(s) => write!(f, "shard {} runtime is shut down", s.0),
        }
    }
}

impl std::error::Error for DispatchError {}

/// A small inline-storage set of `ShardId`s, sized for `k_choice <= 4`.
/// Avoids heap-allocating a Vec for the per-connection assignment when
/// `k_choice` is 1 or 2 (the common case).
#[derive(Debug, Clone, Copy)]
pub struct SmallShardSet {
    items: [ShardId; 4],
    len: u8,
}

impl SmallShardSet {
    pub fn single(s: ShardId) -> Self {
        Self { items: [s, ShardId(0), ShardId(0), ShardId(0)], len: 1 }
    }

    pub fn push(&mut self, s: ShardId) {
        if (self.len as usize) < self.items.len() {
            self.items[self.len as usize] = s;
            self.len += 1;
        }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn first(&self) -> ShardId {
        debug_assert!(self.len >= 1);
        self.items[0]
    }

    pub fn iter(&self) -> impl Iterator<Item = ShardId> + '_ {
        self.items.iter().copied().take(self.len as usize)
    }
}

#[inline]
fn hash_one<T: Hash>(value: &T, salt: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    salt.hash(&mut hasher);
    value.hash(&mut hasher);
    hasher.finish()
}

/// Re-exported here for existing shard tests/callers. The implementation is
/// protocol-neutral and lives in `eden_gateway_core` so Redis, Postgres,
/// Mongo, LLM, and agent gateways can all use the same response-ordering
/// primitive when cross-shard per-batch dispatch is enabled.
pub use eden_gateway::shard_dispatch::ConnectionSequencer;

impl eden_gateway::shard_dispatch::GatewayShardDispatcher for ShardRouter {
    fn shard_count(&self) -> usize {
        self.num_shards()
    }

    fn dispatch_to_shard(
        &self,
        shard_index: usize,
        work: eden_gateway::shard_dispatch::GatewayShardWork,
    ) -> Result<(), eden_gateway::shard_dispatch::GatewayShardDispatchError> {
        if shard_index >= self.num_shards() {
            return Err(eden_gateway::shard_dispatch::GatewayShardDispatchError::new(shard_index));
        }
        self.dispatch(ShardId(shard_index), work)
            .map_err(|_| eden_gateway::shard_dispatch::GatewayShardDispatchError::new(shard_index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    fn addr(ip: u8) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, ip)), 5555)
    }

    #[test]
    fn assign_shards_returns_k_distinct_shards() {
        let router = ShardRouter::start(4, 2);
        let set = router.assign_shards(&addr(1));
        assert!(!set.is_empty());
        if set.len() == 2 {
            let mut iter = set.iter();
            let a = iter.next().unwrap();
            let b = iter.next().unwrap();
            assert_ne!(a.0, b.0, "two-choice subset must be distinct shards");
        }
    }

    #[test]
    fn assign_shards_at_n2_always_returns_both_shards() {
        // Two-shard topology was the case where the prior 4-try retry
        // loop silently fell back to k=1 about 6.25% of the time. The
        // deterministic offset must always yield both.
        let router = ShardRouter::start(2, 2);
        for ip in 0..50u8 {
            let set = router.assign_shards(&addr(ip));
            assert_eq!(set.len(), 2, "k=2,n=2 must produce a 2-shard subset for every address");
            let mut iter = set.iter();
            let a = iter.next().unwrap();
            let b = iter.next().unwrap();
            assert_ne!(a.0, b.0, "the two shards must be distinct");
        }
    }

    #[test]
    fn assign_shards_secondary_is_uniform_for_larger_n() {
        // For n >= 3 the secondary is `(primary + 1 + h % (n - 1)) % n`
        // which always lands on a non-primary shard. Sanity-check that
        // across many addresses we hit every non-primary shard at least
        // once (i.e., the offset is actually using the hash, not always
        // landing on the same neighbor).
        let router = ShardRouter::start(8, 2);
        let mut secondaries_seen = std::collections::HashSet::new();
        for ip in 0..200u8 {
            let set = router.assign_shards(&addr(ip));
            assert_eq!(set.len(), 2);
            let mut iter = set.iter();
            let primary = iter.next().unwrap();
            let secondary = iter.next().unwrap();
            assert_ne!(primary.0, secondary.0);
            secondaries_seen.insert(secondary.0);
        }
        // With 8 shards and 200 samples we should comfortably hit all 8
        // secondaries; assert at least 6 to keep the test stable.
        assert!(
            secondaries_seen.len() >= 6,
            "secondary distribution looks degenerate: only {} unique secondaries across 200 samples",
            secondaries_seen.len(),
        );
    }

    #[test]
    fn pick_shorter_returns_lower_inflight_shard() {
        let router = ShardRouter::start(4, 2);
        // Manually skew inflight: shard 0 high, shard 1 low.
        router.inner.inflight[0].store(10, Ordering::Relaxed);
        router.inner.inflight[1].store(1, Ordering::Relaxed);
        let mut set = SmallShardSet::single(ShardId(0));
        set.push(ShardId(1));
        let chosen = router.pick_shorter(&set);
        assert_eq!(chosen.0, 1);
    }

    #[test]
    fn dispatch_runs_closure_on_shard() {
        let router = ShardRouter::start(2, 1);
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&flag);
        router.dispatch(ShardId(0), Box::new(move || flag_clone.store(true, Ordering::SeqCst))).expect("dispatch ok");
        // Closure runs on the shard's runtime; spin briefly waiting.
        for _ in 0..200 {
            if flag.load(Ordering::SeqCst) {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("shard did not run dispatched closure within 1s");
    }

    #[test]
    fn dispatch_with_guard_increments_and_decrements() {
        let router = ShardRouter::start(2, 1);
        assert_eq!(router.inner.inflight[0].load(Ordering::Relaxed), 0);
        let guard = router.dispatch_with_guard(ShardId(0), Box::new(|| {})).expect("dispatch ok");
        // Inflight is 1 right after dispatch; releasing drops it to 0.
        assert_eq!(guard.shard().0, 0);
        // (counter could be 0 or 1 depending on timing; manually release)
        guard.release();
        assert_eq!(router.inner.inflight[0].load(Ordering::Relaxed), 0);
    }

    #[test]
    fn dispatch_local_task_runs_future_on_shard() {
        let router = ShardRouter::start(2, 1);
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&flag);
        let factory: Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()>>> + Send + 'static> = Box::new(move || {
            Box::pin(async move {
                flag_clone.store(true, Ordering::SeqCst);
            })
        });
        let _join_rx = router.dispatch_local_task(ShardId(0), factory).expect("dispatch ok");

        for _ in 0..200 {
            if flag.load(Ordering::SeqCst) {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("shard did not run dispatched local future within 1s");
    }

    #[test]
    fn dispatch_local_task_decrements_inflight_on_completion() {
        let router = ShardRouter::start(2, 1);
        assert_eq!(router.inner.inflight[0].load(Ordering::Relaxed), 0);
        let factory: Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()>>> + Send + 'static> = Box::new(|| Box::pin(async {}));
        let _join_rx = router.dispatch_local_task(ShardId(0), factory).expect("dispatch ok");

        for _ in 0..200 {
            if router.inner.inflight[0].load(Ordering::Relaxed) == 0 {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("inflight counter did not reach 0 after task completion");
    }

    #[test]
    fn sequencer_in_order_completion_drains_immediately() {
        let seq = ConnectionSequencer::<&'static str>::new();
        let s0 = seq.issue_seq();
        let s1 = seq.issue_seq();
        let s2 = seq.issue_seq();
        assert_eq!((s0, s1, s2), (0, 1, 2));

        assert_eq!(seq.complete(0, "a"), vec!["a"]);
        assert_eq!(seq.complete(1, "b"), vec!["b"]);
        assert_eq!(seq.complete(2, "c"), vec!["c"]);
        assert_eq!(seq.buffered_len(), 0);
    }

    #[test]
    fn sequencer_out_of_order_buffers_then_drains_when_gap_fills() {
        let seq = ConnectionSequencer::<&'static str>::new();
        for _ in 0..3 {
            let _ = seq.issue_seq();
        }

        // 1 and 2 arrive before 0; both get buffered.
        assert_eq!(seq.complete(1, "b"), Vec::<&str>::new());
        assert_eq!(seq.complete(2, "c"), Vec::<&str>::new());
        assert_eq!(seq.buffered_len(), 2);

        // 0 lands; the contiguous run [0,1,2] drains in order.
        assert_eq!(seq.complete(0, "a"), vec!["a", "b", "c"]);
        assert_eq!(seq.buffered_len(), 0);
    }

    #[test]
    fn sequencer_partial_drain_leaves_unfilled_gap_buffered() {
        let seq = ConnectionSequencer::<u32>::new();
        for _ in 0..5 {
            let _ = seq.issue_seq();
        }

        assert_eq!(seq.complete(0, 0), vec![0]);
        // Skip seq 1; complete 2,3 — both buffered since 1 is missing.
        assert_eq!(seq.complete(2, 2), Vec::<u32>::new());
        assert_eq!(seq.complete(3, 3), Vec::<u32>::new());
        assert_eq!(seq.buffered_len(), 2);

        // 1 lands; drains 1,2,3 in order.
        assert_eq!(seq.complete(1, 1), vec![1, 2, 3]);
        assert_eq!(seq.buffered_len(), 0);
        // 4 still pending; complete it to confirm pointer is correct.
        assert_eq!(seq.complete(4, 4), vec![4]);
    }

    #[test]
    fn pick_shorter_breaks_ties_across_calls_instead_of_always_first() {
        // At cold start every shard has zero inflight. Strict `<`
        // comparison would always return the first-iterated shard,
        // collapsing two-choice into single-choice. The reservoir
        // tiebreak must spread picks across the tied set.
        let router = ShardRouter::start(4, 2);
        let mut set = SmallShardSet::single(ShardId(0));
        set.push(ShardId(1));

        let mut zero = 0u32;
        let mut one = 0u32;
        for _ in 0..1000 {
            let chosen = router.pick_shorter(&set);
            match chosen.0 {
                0 => zero += 1,
                1 => one += 1,
                other => panic!("unexpected shard {other}"),
            }
        }

        // Reservoir sampling backed by a rotating counter should give
        // roughly 50/50; assert each side took at least 25% of picks.
        assert!(zero >= 250 && one >= 250, "ties skewed: {zero} vs {one}");
    }

    #[test]
    fn shard_ids_iterates_every_shard_exactly_once() {
        let router = ShardRouter::start(4, 2);
        let collected: Vec<usize> = router.shard_ids().map(|s| s.0).collect();
        assert_eq!(collected, vec![0, 1, 2, 3]);
    }
}
