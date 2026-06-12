use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use socket2::SockRef;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;

use crate::arrival::CommandType;
use crate::recorder::{CommandOutcome, CompletedCommand};

// ---------------------------------------------------------------------------
// RESP codec (minimal response reader, covers GET/SET responses)
// ---------------------------------------------------------------------------

pub enum RespValue {
    SimpleString(String),
    Error(String),
    BulkString(Vec<u8>),
    Nil,
}

struct RespFrame {
    value: RespValue,
    wire_bytes: u64,
    payload_bytes: u64,
}

/// Read one RESP value from a buffered reader.
async fn read_resp<R: tokio::io::AsyncRead + Unpin>(reader: &mut BufReader<R>) -> io::Result<RespFrame> {
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed"));
    }
    let line = line.trim_end();

    let first = line.as_bytes().first().copied().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "empty RESP line"))?;
    let rest = &line[1..];

    match first {
        // RESP2 + RESP3 simple string
        b'+' => Ok(RespFrame {
            value: RespValue::SimpleString(rest.to_string()),
            wire_bytes: n as u64,
            payload_bytes: 0,
        }),
        // RESP2 + RESP3 error
        b'-' => Ok(RespFrame {
            value: RespValue::Error(rest.to_string()),
            wire_bytes: n as u64,
            payload_bytes: 0,
        }),
        // RESP2 + RESP3 integer
        b':' => Ok(RespFrame {
            value: RespValue::SimpleString(rest.to_string()),
            wire_bytes: n as u64,
            payload_bytes: 0,
        }),
        // RESP2 bulk string
        b'$' => {
            let len: i64 = rest.parse().map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bad bulk length: {e}")))?;
            if len < 0 {
                return Ok(RespFrame {
                    value: RespValue::Nil,
                    wire_bytes: n as u64,
                    payload_bytes: 0,
                });
            }
            let len = len as usize;
            let mut buf = vec![0u8; len + 2]; // data + \r\n
            reader.read_exact(&mut buf).await?;
            buf.truncate(len);
            Ok(RespFrame {
                value: RespValue::BulkString(buf),
                wire_bytes: (n + len + 2) as u64,
                payload_bytes: len as u64,
            })
        }
        // RESP3 null: _\r\n
        b'_' => Ok(RespFrame {
            value: RespValue::Nil,
            wire_bytes: n as u64,
            payload_bytes: 0,
        }),
        // RESP3 blob string (same framing as RESP2 bulk string)
        b'=' => {
            let len: usize = rest.parse().map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bad verbatim length: {e}")))?;
            let mut buf = vec![0u8; len + 2];
            reader.read_exact(&mut buf).await?;
            buf.truncate(len);
            Ok(RespFrame {
                value: RespValue::BulkString(buf),
                wire_bytes: (n + len + 2) as u64,
                payload_bytes: len as u64,
            })
        }
        // RESP3 blob error
        b'!' => {
            let len: usize = rest.parse().map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bad blob error length: {e}")))?;
            let mut buf = vec![0u8; len + 2];
            reader.read_exact(&mut buf).await?;
            buf.truncate(len);
            Ok(RespFrame {
                value: RespValue::Error(String::from_utf8_lossy(&buf).to_string()),
                wire_bytes: (n + len + 2) as u64,
                payload_bytes: 0,
            })
        }
        // RESP3 double
        b',' => Ok(RespFrame {
            value: RespValue::SimpleString(rest.to_string()),
            wire_bytes: n as u64,
            payload_bytes: 0,
        }),
        // RESP3 boolean
        b'#' => Ok(RespFrame {
            value: RespValue::SimpleString(rest.to_string()),
            wire_bytes: n as u64,
            payload_bytes: 0,
        }),
        // RESP3 big number
        b'(' => Ok(RespFrame {
            value: RespValue::SimpleString(rest.to_string()),
            wire_bytes: n as u64,
            payload_bytes: 0,
        }),
        other => Err(io::Error::new(io::ErrorKind::InvalidData, format!("unexpected RESP type byte: {}", other as char))),
    }
}

// ---------------------------------------------------------------------------
// Per-connection types
// ---------------------------------------------------------------------------

/// Command dispatched to a connection's writer task.
pub struct OutboundCommand {
    pub command_type: CommandType,
    pub key: String,
    pub planned_set_count: u16,
    pub value: Option<Vec<u8>>,
    pub encoded: Vec<u8>,
    pub scheduled_time: Instant,
    pub permit: OwnedSemaphorePermit,
}

/// Metadata forwarded from writer to reader for response matching.
struct InFlight {
    command_type: CommandType,
    key: String,
    planned_set_count: u16,
    set_value: Option<Vec<u8>>,
    request_wire_bytes: u64,
    scheduled_time: Instant,
    send_time: Instant,
    connection_id: u64,
    _permit: OwnedSemaphorePermit,
}

// ---------------------------------------------------------------------------
// Connection handle (held by the pool / dispatcher)
// ---------------------------------------------------------------------------

pub struct ConnectionHandle {
    pub cmd_tx: mpsc::UnboundedSender<OutboundCommand>,
    pub slots: Arc<Semaphore>,
    pub id: u64,
    writer_handle: JoinHandle<()>,
    reader_handle: JoinHandle<()>,
}

impl ConnectionHandle {
    /// Try to acquire a pipeline slot (non-blocking).
    pub fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.slots.clone().try_acquire_owned().ok()
    }

    /// Send a command to the writer task. The permit must already be acquired.
    /// Returns the command back as Err if the writer channel is closed (connection dead).
    pub fn send(&self, cmd: OutboundCommand) -> Result<(), OutboundCommand> {
        self.cmd_tx.send(cmd).map_err(|e| e.0)
    }
}

// ---------------------------------------------------------------------------
// Connection pool
// ---------------------------------------------------------------------------

pub struct ConnectionPool {
    pub connections: Vec<ConnectionHandle>,
}

impl ConnectionPool {
    /// Connect to the target and spawn writer/reader tasks for each connection.
    pub async fn connect(
        target: &str,
        count: u32,
        pipeline_depth: u32,
        recorder_tx: mpsc::UnboundedSender<CompletedCommand>,
    ) -> io::Result<Self> {
        let mut connections = Vec::with_capacity(count as usize);

        for id in 0..count {
            let stream = TcpStream::connect(target).await?;
            stream.set_nodelay(true)?;
            // Detect half-open connections: kernel sends probes after 5s
            // idle, retries every 1s, gives up after 3 failures (~8s).
            let sock = SockRef::from(&stream);
            let keepalive = socket2::TcpKeepalive::new().with_time(Duration::from_secs(5)).with_interval(Duration::from_secs(1));
            let _ = sock.set_tcp_keepalive(&keepalive);
            let (read_half, write_half) = stream.into_split();

            let slots = Arc::new(Semaphore::new(pipeline_depth as usize));
            let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<OutboundCommand>();
            let (meta_tx, meta_rx) = mpsc::unbounded_channel::<InFlight>();

            let conn_id = id as u64;
            let writer_recorder = recorder_tx.clone();
            let reader_recorder = recorder_tx.clone();

            let writer_handle = tokio::spawn(writer_task(conn_id, cmd_rx, meta_tx, writer_recorder, write_half));
            let reader_handle = tokio::spawn(reader_task(conn_id, meta_rx, read_half, reader_recorder));

            connections.push(ConnectionHandle { cmd_tx, slots, id: conn_id, writer_handle, reader_handle });
        }

        Ok(Self { connections })
    }

    /// Wait for all in-flight commands to complete.
    pub async fn shutdown(self) {
        for conn in self.connections {
            drop(conn.cmd_tx);
            let _ = conn.writer_handle.await;
            let _ = conn.reader_handle.await;
        }
    }
}

/// Create a single connection with writer/reader tasks. Used by workers
/// to reconnect after a connection dies.
pub async fn connect_one(
    target: &str,
    conn_id: u64,
    pipeline_depth: u32,
    recorder_tx: mpsc::UnboundedSender<CompletedCommand>,
) -> io::Result<(mpsc::UnboundedSender<OutboundCommand>, Arc<Semaphore>)> {
    let stream = TcpStream::connect(target).await?;
    stream.set_nodelay(true)?;
    let sock = SockRef::from(&stream);
    let keepalive = socket2::TcpKeepalive::new().with_time(Duration::from_secs(5)).with_interval(Duration::from_secs(1));
    let _ = sock.set_tcp_keepalive(&keepalive);
    let (read_half, write_half) = stream.into_split();

    let slots = Arc::new(Semaphore::new(pipeline_depth as usize));
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<OutboundCommand>();
    let (meta_tx, meta_rx) = mpsc::unbounded_channel::<InFlight>();

    tokio::spawn(writer_task(conn_id, cmd_rx, meta_tx, recorder_tx.clone(), write_half));
    tokio::spawn(reader_task(conn_id, meta_rx, read_half, recorder_tx));

    Ok((cmd_tx, slots))
}

// ---------------------------------------------------------------------------
// Writer task
// ---------------------------------------------------------------------------

async fn writer_task(
    conn_id: u64,
    mut cmd_rx: mpsc::UnboundedReceiver<OutboundCommand>,
    meta_tx: mpsc::UnboundedSender<InFlight>,
    recorder_tx: mpsc::UnboundedSender<CompletedCommand>,
    mut writer: tokio::net::tcp::OwnedWriteHalf,
) {
    while let Some(cmd) = cmd_rx.recv().await {
        let send_time = Instant::now();
        let request_wire_bytes = cmd.encoded.len() as u64;

        if let Err(e) = writer.write_all(&cmd.encoded).await {
            let err_msg = format!("connection {conn_id}: write error: {e}");
            eprintln!("{err_msg}");
            record_error(&recorder_tx, cmd, send_time, conn_id, &err_msg);

            // Drain remaining buffered commands as errors.
            while let Ok(buffered) = cmd_rx.try_recv() {
                let now = Instant::now();
                record_error(&recorder_tx, buffered, now, conn_id, &err_msg);
            }
            break;
        }

        let _ = meta_tx.send(InFlight {
            command_type: cmd.command_type,
            key: cmd.key,
            planned_set_count: cmd.planned_set_count,
            set_value: cmd.value,
            request_wire_bytes,
            scheduled_time: cmd.scheduled_time,
            send_time,
            connection_id: conn_id,
            _permit: cmd.permit,
        });
    }
}

/// Record a command as a ConnectionError directly from the writer task.
fn record_error(
    recorder_tx: &mpsc::UnboundedSender<CompletedCommand>,
    cmd: OutboundCommand,
    send_time: Instant,
    conn_id: u64,
    error_msg: &str,
) {
    let now = Instant::now();
    let _ = recorder_tx.send(CompletedCommand {
        scheduled_time: cmd.scheduled_time,
        send_time,
        recv_time: now,
        connection_id: conn_id,
        command_type: cmd.command_type,
        key: cmd.key,
        planned_set_count: cmd.planned_set_count,
        set_value: cmd.value,
        request_wire_bytes: 0,
        response_wire_bytes: 0,
        response_payload_bytes: 0,
        outcome: CommandOutcome::ConnectionError(error_msg.to_string()),
    });
    // Permit dropped here with cmd.permit, freeing the slot.
}

// ---------------------------------------------------------------------------
// Reader task
// ---------------------------------------------------------------------------

async fn reader_task(
    conn_id: u64,
    mut meta_rx: mpsc::UnboundedReceiver<InFlight>,
    read_half: tokio::net::tcp::OwnedReadHalf,
    recorder_tx: mpsc::UnboundedSender<CompletedCommand>,
) {
    let mut reader = BufReader::new(read_half);

    loop {
        let meta = match meta_rx.recv().await {
            Some(m) => m,
            None => break,
        };

        let recv_time;
        let outcome;
        let response_wire_bytes;
        let response_payload_bytes;

        // 60s safety timeout: purely a deadlock breaker for half-open
        // connections where the kernel hasn't delivered EOF yet.
        // Normal operation never hits this — EOF/broken pipe arrive
        // promptly, and TCP keepalive detects silent deaths in ~8s.
        match tokio::time::timeout(Duration::from_secs(30), read_resp(&mut reader)).await {
            Err(_elapsed) => {
                let now = Instant::now();
                let wait_secs = now.duration_since(meta.send_time).as_secs_f64();
                let age_secs = now.duration_since(meta.scheduled_time).as_secs_f64();
                let pending = meta_rx.len();
                let err_msg = format!(
                    "connection {conn_id}: read timeout (10s) cmd={:?} key={} waited={wait_secs:.1}s age={age_secs:.1}s pending_behind={}",
                    meta.command_type, meta.key, pending,
                );
                eprintln!("  {err_msg}");
                let _ = recorder_tx.send(CompletedCommand {
                    scheduled_time: meta.scheduled_time,
                    send_time: meta.send_time,
                    recv_time: now,
                    connection_id: meta.connection_id,
                    command_type: meta.command_type,
                    key: meta.key,
                    planned_set_count: meta.planned_set_count,
                    set_value: meta.set_value,
                    request_wire_bytes: meta.request_wire_bytes,
                    response_wire_bytes: 0,
                    response_payload_bytes: 0,
                    outcome: CommandOutcome::ConnectionError(err_msg.clone()),
                });
                let mut drained = 0u64;
                while let Ok(remaining) = meta_rx.try_recv() {
                    drained += 1;
                    let _ = recorder_tx.send(CompletedCommand {
                        scheduled_time: remaining.scheduled_time,
                        send_time: remaining.send_time,
                        recv_time: now,
                        connection_id: remaining.connection_id,
                        command_type: remaining.command_type,
                        key: remaining.key,
                        planned_set_count: remaining.planned_set_count,
                        set_value: remaining.set_value,
                        request_wire_bytes: remaining.request_wire_bytes,
                        response_wire_bytes: 0,
                        response_payload_bytes: 0,
                        outcome: CommandOutcome::ConnectionError(err_msg.clone()),
                    });
                }
                if drained > 0 {
                    eprintln!("  connection {conn_id}: drained {drained} additional pending commands");
                }
                break;
            }
            Ok(Ok(frame)) => {
                recv_time = Instant::now();
                response_wire_bytes = frame.wire_bytes;
                response_payload_bytes = frame.payload_bytes;
                outcome = match (&meta.command_type, frame.value) {
                    (CommandType::Set, RespValue::SimpleString(ref s)) if s == "OK" => CommandOutcome::SetOk,
                    (CommandType::Set, RespValue::SimpleString(s)) => {
                        CommandOutcome::RedisError(format!("SET returned unexpected simple string: {s:?}"))
                    }
                    (CommandType::Get, RespValue::BulkString(data)) => CommandOutcome::GetHit(data),
                    (CommandType::Get, RespValue::Nil) => CommandOutcome::GetMiss,
                    (_, RespValue::Error(e)) => CommandOutcome::RedisError(e),
                    (cmd, ref unexpected) => {
                        let desc = match unexpected {
                            RespValue::SimpleString(s) => format!("SimpleString({:?})", &s[..s.len().min(80)]),
                            RespValue::Error(s) => format!("Error({:?})", &s[..s.len().min(80)]),
                            RespValue::BulkString(b) => {
                                format!("BulkString(len={}, {:?})", b.len(), String::from_utf8_lossy(&b[..b.len().min(40)]))
                            }
                            RespValue::Nil => "Nil".to_string(),
                        };
                        CommandOutcome::RedisError(format!("unexpected response type for {cmd:?}: got {desc}"))
                    }
                };
            }
            Ok(Err(e)) => {
                // Connection error (EOF, broken pipe, parse error) —
                // drain all remaining in-flight commands as errors and
                // exit immediately.
                let now = Instant::now();
                let err_msg = format!("connection {conn_id}: {e}");
                let _ = recorder_tx.send(CompletedCommand {
                    scheduled_time: meta.scheduled_time,
                    send_time: meta.send_time,
                    recv_time: now,
                    connection_id: meta.connection_id,
                    command_type: meta.command_type,
                    key: meta.key,
                    planned_set_count: meta.planned_set_count,
                    set_value: meta.set_value,
                    request_wire_bytes: meta.request_wire_bytes,
                    response_wire_bytes: 0,
                    response_payload_bytes: 0,
                    outcome: CommandOutcome::ConnectionError(err_msg.clone()),
                });
                while let Ok(remaining) = meta_rx.try_recv() {
                    let _ = recorder_tx.send(CompletedCommand {
                        scheduled_time: remaining.scheduled_time,
                        send_time: remaining.send_time,
                        recv_time: now,
                        connection_id: remaining.connection_id,
                        command_type: remaining.command_type,
                        key: remaining.key,
                        planned_set_count: remaining.planned_set_count,
                        set_value: remaining.set_value,
                        request_wire_bytes: remaining.request_wire_bytes,
                        response_wire_bytes: 0,
                        response_payload_bytes: 0,
                        outcome: CommandOutcome::ConnectionError(err_msg.clone()),
                    });
                }
                break;
            }
        }

        let _ = recorder_tx.send(CompletedCommand {
            scheduled_time: meta.scheduled_time,
            send_time: meta.send_time,
            recv_time,
            connection_id: meta.connection_id,
            command_type: meta.command_type,
            key: meta.key,
            planned_set_count: meta.planned_set_count,
            set_value: meta.set_value,
            request_wire_bytes: meta.request_wire_bytes,
            response_wire_bytes,
            response_payload_bytes,
            outcome,
        });

        // Permit dropped here with meta._permit, freeing the pipeline slot.
    }
}
