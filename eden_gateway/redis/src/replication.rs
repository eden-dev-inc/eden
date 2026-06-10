#![allow(dead_code)]

use crate::aof::AofStreamer;
use crate::psync::RedisPsyncHandler;
use bytes::Bytes;
use dashmap::DashMap;
use eden_core::format::InterlayUuid;
use eden_core::format::cache_uuid::{EndpointCacheUuid, OrganizationCacheUuid};
use once_cell::sync::Lazy;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

pub struct ReplicationManager {
    psync_handler: Arc<RedisPsyncHandler>,
    aof_streamers: Arc<RwLock<Vec<AofStreamer>>>,
    streaming_mode: AtomicBool,
    fanout_tasks_spawned: Arc<AtomicU64>,
    fanout_tasks_completed: Arc<AtomicU64>,
}

impl ReplicationManager {
    pub fn new() -> Self {
        Self {
            psync_handler: Arc::new(RedisPsyncHandler::new()),
            aof_streamers: Arc::new(RwLock::new(Vec::new())),
            streaming_mode: AtomicBool::new(false),
            fanout_tasks_spawned: Arc::new(AtomicU64::new(0)),
            fanout_tasks_completed: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn set_streaming_mode(&self, enabled: bool) {
        self.streaming_mode.store(enabled, Ordering::Relaxed);
    }

    pub fn is_streaming(&self) -> bool {
        self.streaming_mode.load(Ordering::Relaxed)
    }

    /// Stream write command to all replicas.
    /// Accepts Bytes to avoid allocation when the caller already has Bytes.
    pub fn stream_write_command(&self, command: Bytes) {
        let streamers = self.aof_streamers.clone();
        self.fanout_tasks_spawned.fetch_add(1, Ordering::Relaxed);
        let fanout_tasks_completed = self.fanout_tasks_completed.clone();

        // Fire and forget - completely non-blocking
        tokio::spawn(async move {
            let mut has_dead = false;
            let readers = streamers.read().await;
            for streamer in readers.iter() {
                if streamer.stream_command(command.clone()).is_err() {
                    has_dead = true;
                }
            }
            drop(readers);
            if has_dead {
                let mut writers = streamers.write().await;
                writers.retain(|streamer| !streamer.is_closed());
            }
            fanout_tasks_completed.fetch_add(1, Ordering::Relaxed);
        });
    }

    pub async fn handle_full_sync(&self) -> bytes::Bytes {
        self.psync_handler.handle_psync(None, -1)
    }

    pub async fn handle_partial_sync(&self, repl_id: Option<String>, offset: i64) -> bytes::Bytes {
        self.psync_handler.handle_psync(repl_id, offset)
    }

    pub async fn add_replication_target(&self, target_addr: SocketAddr) {
        let (tx, rx) = unbounded_channel();
        let streamer = AofStreamer::new(tx);

        self.aof_streamers.write().await.push(streamer);

        // Spawn connection handler for this target
        tokio::spawn(replication_connection_handler(target_addr, rx));
    }

    pub async fn remove_failed_targets(&self) {
        let mut streamers = self.aof_streamers.write().await;
        streamers.retain(|streamer| !streamer.is_closed());
    }

    pub async fn streamer_count(&self) -> usize {
        self.aof_streamers.read().await.len()
    }

    pub fn fanout_tasks_spawned(&self) -> u64 {
        self.fanout_tasks_spawned.load(Ordering::Relaxed)
    }

    pub fn fanout_tasks_in_flight(&self) -> u64 {
        self.fanout_tasks_spawned.load(Ordering::Relaxed).saturating_sub(self.fanout_tasks_completed.load(Ordering::Relaxed))
    }
}

impl Default for ReplicationManager {
    fn default() -> Self {
        Self::new()
    }
}

// Global registry of replication managers
pub(crate) static REPLICATION_MANAGERS: Lazy<DashMap<InterlayUuid, Arc<ReplicationManager>>> = Lazy::new(DashMap::new);

pub fn get_or_create_manager(
    interlay_uuid: InterlayUuid,
    _cache_key: &EndpointCacheUuid,
    _org_uuid: OrganizationCacheUuid,
) -> Arc<ReplicationManager> {
    REPLICATION_MANAGERS.entry(interlay_uuid).or_insert_with(|| Arc::new(ReplicationManager::new())).clone()
}

pub fn manager_count() -> usize {
    REPLICATION_MANAGERS.len()
}

pub fn manager_for(interlay_uuid: &InterlayUuid) -> Option<Arc<ReplicationManager>> {
    REPLICATION_MANAGERS.get(interlay_uuid).map(|entry| entry.clone())
}

#[cfg(test)]
pub fn clear_managers_for_tests() {
    REPLICATION_MANAGERS.clear();
}

pub async fn replication_connection_handler(
    target_addr: SocketAddr,
    command_receiver: UnboundedReceiver<Bytes>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let stream = TcpStream::connect(target_addr).await?;
    replication_connection_handler_on_stream(stream, command_receiver).await
}

pub(crate) async fn replication_connection_handler_on_stream<S>(
    stream: S,
    mut command_receiver: UnboundedReceiver<Bytes>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut read_half, mut write_half) = tokio::io::split(stream);

    let mut drain_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 8 * 1024];
        loop {
            match read_half.read(&mut buf).await {
                Ok(0) => break,
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    // Handshake sequence
    write_half.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
    write_half.write_all(b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6379\r\n").await?;
    write_half.write_all(b"*5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n").await?;
    write_half.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await?;

    // Stream commands without blocking
    loop {
        tokio::select! {
            drain_result = &mut drain_task => {
                let _ = drain_result;
                break;
            }
            maybe_command = command_receiver.recv() => {
                let Some(command) = maybe_command else {
                    break;
                };
                if write_half.write_all(&command).await.is_err() {
                    break;
                }
            }
        }
    }

    if !drain_task.is_finished() {
        drain_task.abort();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use eden_core::format::CacheUuid;
    use eden_core::format::cache_uuid::{EndpointCacheUuid, OrganizationCacheUuid};
    use eden_core::format::{EndpointUuid, InterlayUuid, OrganizationUuid};
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, duplex};
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::time::{Duration, sleep, timeout};

    static REPLICATION_MANAGER_TEST_LOCK: Lazy<tokio::sync::Mutex<()>> = Lazy::new(|| tokio::sync::Mutex::new(()));

    #[derive(Clone, Copy)]
    enum LoadProfile {
        Consistent,
        Variable,
        Malicious,
    }

    impl LoadProfile {
        fn label(self) -> &'static str {
            match self {
                Self::Consistent => "consistent",
                Self::Variable => "variable",
                Self::Malicious => "malicious",
            }
        }
    }

    fn replication_profiles() -> [LoadProfile; 3] {
        [LoadProfile::Consistent, LoadProfile::Variable, LoadProfile::Malicious]
    }

    fn replication_commands(profile: LoadProfile) -> Vec<Bytes> {
        match profile {
            LoadProfile::Consistent => vec![Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"); 8],
            LoadProfile::Variable => vec![
                Bytes::from_static(b"*3\r\n$3\r\nSET\r\n$5\r\nalpha\r\n$3\r\none\r\n"),
                Bytes::from_static(b"*2\r\n$3\r\nGET\r\n$5\r\nalpha\r\n"),
                Bytes::from(vec![b'v'; 8 * 1024]),
                Bytes::from_static(b"*4\r\n$4\r\nHSET\r\n$4\r\nhash\r\n$5\r\nfield\r\n$5\r\nvalue\r\n"),
                Bytes::from(vec![0x11; 2 * 1024]),
            ],
            LoadProfile::Malicious => vec![
                Bytes::from(vec![0xff; 32 * 1024]),
                Bytes::from_static(b"*999\r\n$3\r\nSET\r\n"),
                Bytes::from(vec![b'!'; 64 * 1024]),
                Bytes::from(vec![0u8; 4 * 1024]),
            ],
        }
    }

    fn replication_command_burst(profile: LoadProfile) -> Vec<Bytes> {
        let repeat_count = match profile {
            LoadProfile::Consistent => 128,
            LoadProfile::Variable => 32,
            LoadProfile::Malicious => 4,
        };

        let commands = replication_commands(profile);
        let mut burst = Vec::with_capacity(commands.len() * repeat_count);
        for _ in 0..repeat_count {
            burst.extend(commands.iter().cloned());
        }
        burst
    }

    fn blocking_replication_response(profile: LoadProfile) -> Vec<u8> {
        match profile {
            LoadProfile::Consistent => vec![b'R'; 2 * 1024],
            LoadProfile::Variable => vec![b'R'; 8 * 1024],
            LoadProfile::Malicious => vec![b'R'; 64 * 1024],
        }
    }

    async fn read_handshake_until_psync<S: AsyncRead + Unpin>(stream: &mut S) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 512];

        loop {
            let read = timeout(Duration::from_millis(100), stream.read(&mut chunk))
                .await
                .expect("replication handshake should make progress")
                .expect("read handshake bytes");
            assert!(read > 0, "replication handler closed before sending PSYNC");
            buf.extend_from_slice(&chunk[..read]);

            if buf.windows(5).any(|window| window == b"PSYNC") {
                return buf;
            }
        }
    }

    #[tokio::test]
    async fn stream_write_command_fans_out() {
        let manager = ReplicationManager::new();
        let (tx1, mut rx1) = unbounded_channel();
        let (tx2, mut rx2) = unbounded_channel();

        manager.aof_streamers.write().await.push(AofStreamer::new(tx1));
        manager.aof_streamers.write().await.push(AofStreamer::new(tx2));

        manager.set_streaming_mode(true).await;
        manager.stream_write_command(Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"));

        let forwarded1 = rx1.recv().await.expect("streamer 1 receives data");
        let forwarded2 = rx2.recv().await.expect("streamer 2 receives data");
        assert_eq!(&forwarded1[..], b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(&forwarded2[..], b"*1\r\n$4\r\nPING\r\n");
    }

    #[tokio::test]
    async fn streaming_mode_toggle_updates_flag() {
        let manager = ReplicationManager::new();
        assert!(!manager.is_streaming());

        manager.set_streaming_mode(true).await;
        assert!(manager.is_streaming());

        manager.set_streaming_mode(false).await;
        assert!(!manager.is_streaming());
    }

    #[tokio::test]
    async fn dead_streamers_remain_registered_before_prune_fix() {
        let _guard = REPLICATION_MANAGER_TEST_LOCK.lock().await;
        clear_managers_for_tests();

        let manager = ReplicationManager::new();
        let (tx, rx) = unbounded_channel();
        drop(rx);

        manager.aof_streamers.write().await.push(AofStreamer::new(tx));
        manager.stream_write_command(Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"));
        sleep(Duration::from_millis(20)).await;

        assert_eq!(manager.streamer_count().await, 0, "dead channels should be pruned after failed send");

        manager.remove_failed_targets().await;
        assert_eq!(manager.streamer_count().await, 0, "remove_failed_targets should keep dead channels pruned");
    }

    #[tokio::test]
    async fn healthy_streamers_survive_characterization_probe() {
        let _guard = REPLICATION_MANAGER_TEST_LOCK.lock().await;
        clear_managers_for_tests();

        let manager = ReplicationManager::new();
        let (tx1, mut rx1) = unbounded_channel();
        let (tx2, mut rx2) = unbounded_channel();

        manager.aof_streamers.write().await.push(AofStreamer::new(tx1));
        manager.aof_streamers.write().await.push(AofStreamer::new(tx2));
        manager.stream_write_command(Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"));
        sleep(Duration::from_millis(20)).await;

        assert_eq!(manager.streamer_count().await, 2);
        assert!(rx1.recv().await.is_some());
        assert!(rx2.recv().await.is_some());

        manager.remove_failed_targets().await;
        assert_eq!(manager.streamer_count().await, 2, "healthy channels should remain visible during characterization");
    }

    #[tokio::test]
    async fn disconnected_replica_target_is_pruned_after_send_failure() {
        let _guard = REPLICATION_MANAGER_TEST_LOCK.lock().await;
        clear_managers_for_tests();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let accept_task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept replica");
            drop(socket);
        });

        let interlay_uuid = InterlayUuid::new_uuid();
        let endpoint_cache_uuid =
            EndpointCacheUuid::new(Some(OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid())), EndpointUuid::new_uuid());
        let manager = get_or_create_manager(
            interlay_uuid.clone(),
            &endpoint_cache_uuid,
            OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid()),
        );
        manager.add_replication_target(addr).await;
        accept_task.await.expect("join accept task");

        sleep(Duration::from_millis(50)).await;
        for profile in replication_profiles() {
            for command in replication_commands(profile) {
                manager.stream_write_command(command);
            }
        }
        sleep(Duration::from_millis(50)).await;

        assert_eq!(manager.streamer_count().await, 0, "failed replica streamers should be pruned");
    }

    #[tokio::test]
    async fn manager_state_persists_after_streaming_enabled() {
        let _guard = REPLICATION_MANAGER_TEST_LOCK.lock().await;
        clear_managers_for_tests();

        let interlay_uuid = InterlayUuid::new_uuid();
        let endpoint_cache_uuid =
            EndpointCacheUuid::new(Some(OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid())), EndpointUuid::new_uuid());
        let manager = get_or_create_manager(
            interlay_uuid.clone(),
            &endpoint_cache_uuid,
            OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid()),
        );

        manager.set_streaming_mode(true).await;
        for profile in replication_profiles() {
            for command in replication_commands(profile) {
                manager.stream_write_command(command);
            }
        }
        sleep(Duration::from_millis(20)).await;

        assert!(manager.is_streaming());
        assert!(manager_for(&interlay_uuid).is_some(), "manager remains in global registry until explicit cleanup");
        assert!(manager.fanout_tasks_spawned() >= 1);
    }

    #[tokio::test]
    async fn unread_replication_responses_stall_follow_up_writes() {
        for profile in replication_profiles() {
            let (client, mut server) = duplex(1024);
            let (tx, rx) = unbounded_channel();
            let mut handler = tokio::spawn(replication_connection_handler_on_stream(client, rx));

            let handshake = read_handshake_until_psync(&mut server).await;
            assert!(
                handshake.windows(5).any(|window| window == b"PSYNC"),
                "handshake should reach PSYNC before the peer starts sending replication data"
            );

            let server_write_stalled =
                timeout(Duration::from_millis(40), server.write_all(&blocking_replication_response(profile))).await.is_err();
            assert!(
                !server_write_stalled,
                "the replica peer should be able to write its {} full-resync payload because the handler drains responses",
                profile.label()
            );

            let server_reader = tokio::spawn(async move {
                let mut buf = [0u8; 8 * 1024];
                loop {
                    match timeout(Duration::from_millis(40), server.read(&mut buf)).await {
                        Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                        Ok(Ok(_)) => {}
                    }
                }
            });

            for command in replication_command_burst(profile) {
                tx.send(command).expect("queue follow-up replication command");
            }
            drop(tx);

            let _ = timeout(Duration::from_millis(250), &mut handler)
                .await
                .expect("replication handler should finish once queued commands drain")
                .expect("replication handler should exit cleanly");
            server_reader.await.expect("replica reader task should join cleanly");
        }
    }

    #[tokio::test]
    async fn replication_handler_completes_when_peer_keeps_reading_commands() {
        for profile in replication_profiles() {
            let (client, mut server) = duplex(8 * 1024);
            let (tx, rx) = unbounded_channel();
            let handler = tokio::spawn(replication_connection_handler_on_stream(client, rx));

            let server_task = tokio::spawn(async move {
                let handshake = read_handshake_until_psync(&mut server).await;
                assert!(handshake.windows(5).any(|window| window == b"PSYNC"));

                let mut buf = [0u8; 8 * 1024];
                loop {
                    match timeout(Duration::from_millis(40), server.read(&mut buf)).await {
                        Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                        Ok(Ok(_)) => {}
                    }
                }
            });

            for command in replication_command_burst(profile) {
                tx.send(command).expect("queue replication command");
            }
            drop(tx);

            let _ = timeout(Duration::from_millis(120), handler)
                .await
                .expect("handler should finish when the replica keeps draining commands")
                .expect("replication handler should exit cleanly");
            server_task.await.expect("join draining replica task");
        }
    }

    #[tokio::test]
    #[ignore = "manual fan-out stress harness for prune behavior under disconnect churn"]
    async fn manual_replication_fanout_stress_harness() {
        let _guard = REPLICATION_MANAGER_TEST_LOCK.lock().await;
        clear_managers_for_tests();

        let endpoint_cache_uuid =
            EndpointCacheUuid::new(Some(OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid())), EndpointUuid::new_uuid());
        let manager = get_or_create_manager(
            InterlayUuid::new_uuid(),
            &endpoint_cache_uuid,
            OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid()),
        );

        let mut accept_tasks = Vec::new();
        for _ in 0..10 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
            let addr = listener.local_addr().expect("listener addr");
            accept_tasks.push(tokio::spawn(async move {
                let (socket, _) = listener.accept().await.expect("accept replica");
                drop(socket);
            }));
            manager.add_replication_target(addr).await;
        }

        for task in accept_tasks {
            task.await.expect("join accept task");
        }

        sleep(Duration::from_millis(100)).await;
        let before = manager.streamer_count().await;
        for profile in replication_profiles() {
            let commands = replication_commands(profile);
            for _ in 0..20 {
                for command in &commands {
                    manager.stream_write_command(command.clone());
                }
            }
        }
        sleep(Duration::from_millis(100)).await;

        assert_eq!(before, 10);
        assert_eq!(manager.streamer_count().await, 0, "dead replica streamers should be pruned after disconnect churn");
        assert!(
            manager.fanout_tasks_spawned() >= (replication_profiles().len() * 20) as u64,
            "fanout tasks should accumulate under mixed load"
        );
    }
}
