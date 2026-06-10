use crate::protocol::{Handshake, detect_protocol, handshake_for};
use crate::replay::ReplayQueue;
use std::io::{self, Read, Write};
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Accept backend connections and serve them with the shared replay queue.
pub fn run_backend_pool(listener: TcpListener, stop: Arc<AtomicBool>, queue: Arc<Mutex<ReplayQueue>>, verbose: bool) {
    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }

        listener.set_nonblocking(true).ok();
        match listener.accept() {
            Ok((conn, peer)) => {
                listener.set_nonblocking(false).ok();
                eprintln!("backend connected: {peer}");

                let stop2 = Arc::clone(&stop);
                let queue2 = Arc::clone(&queue);
                thread::spawn(move || {
                    serve_backend_conn(conn, stop2, queue2, verbose);
                });
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                listener.set_nonblocking(false).ok();
                thread::sleep(Duration::from_millis(50));
            }
            Err(e) => {
                eprintln!("  backend pool: accept error: {e}");
                break;
            }
        }
    }
}

/// Serve one backend connection using the shared replay queue.
///
/// Decision logic for each received message:
/// 1. If it matches the next expected PCAP incoming -> respond with PCAP outgoing
/// 2. If it's a handshake command -> respond with mock
/// 3. If PCAP not ready and not handshake -> respond +OK, warn
/// 4. If PCAP ready but no match -> warn, wait; on timeout -> respond with
///    PCAP outgoing anyway and continue
fn serve_backend_conn(mut conn: TcpStream, stop: Arc<AtomicBool>, queue: Arc<Mutex<ReplayQueue>>, verbose: bool) {
    let poll = Duration::from_millis(100);
    conn.set_read_timeout(Some(poll)).ok();
    conn.set_write_timeout(Some(Duration::from_secs(5))).ok();

    let mut buf = vec![0u8; 65536];
    let mut pending = 0usize;
    let mut mismatch_since: Option<Instant> = None;
    let mismatch_timeout = Duration::from_secs(5);

    // Sticky protocol handler: once detected, reuse for this connection.
    let mut handler: Option<Box<dyn Handshake>> = None;

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }

        // --- Process buffered data ---
        'process: loop {
            if pending == 0 {
                break;
            }

            // Step 1: Try raw PCAP match against front of queue.
            {
                let mut q = queue.lock().expect("queue lock");
                if q.pcap_ready
                    && let Some(entry) = q.entries.front()
                {
                    let exp = &entry.incoming;
                    let check_len = pending.min(exp.len());

                    if buf[..check_len] == exp[..check_len] {
                        if pending >= exp.len() {
                            let outgoing = entry.outgoing.clone();
                            let consumed = exp.len();
                            q.entries.pop_front();
                            drop(q);

                            mismatch_since = None;
                            if verbose {
                                eprintln!("  backend: PCAP match ({consumed} bytes in, {} bytes out)", outgoing.len());
                            }
                            if conn.write_all(&outgoing).is_err() || conn.flush().is_err() {
                                return;
                            }
                            buf.copy_within(consumed..pending, 0);
                            pending -= consumed;
                            continue 'process;
                        } else {
                            break 'process;
                        }
                    }
                }
            }

            // Step 2: Parse individual commands using the sticky handler.
            let h = handler.get_or_insert_with(|| {
                detect_protocol(&buf[..pending]).map(|p| handshake_for(&p)).unwrap_or_else(|| Box::new(crate::protocol::RedisHandshake))
            });

            match h.parse_command(&buf[..pending]) {
                Some((args, consumed)) => {
                    let arg_refs: Vec<&[u8]> = args.to_vec();
                    let verb: Vec<u8> = arg_refs[0].to_ascii_uppercase();

                    if h.is_handshake_verb(&verb) {
                        let (response, should_close) = h.mock_response(&arg_refs);
                        if verbose {
                            let parts: Vec<String> = arg_refs.iter().map(|a| String::from_utf8_lossy(a).into_owned()).collect();
                            eprintln!("  backend handshake: {parts:?}");
                        }
                        if conn.write_all(&response).is_err() || conn.flush().is_err() {
                            return;
                        }
                        buf.copy_within(consumed..pending, 0);
                        pending -= consumed;
                        mismatch_since = None;
                        if should_close {
                            return;
                        }
                        continue 'process;
                    }

                    let pcap_ready = {
                        let q = queue.lock().expect("queue lock");
                        q.pcap_ready
                    };

                    if !pcap_ready {
                        let parts: Vec<String> = arg_refs.iter().map(|a| String::from_utf8_lossy(a).into_owned()).collect();
                        eprintln!("  backend: WARNING unexpected command (no PCAP): {parts:?}");
                        if conn.write_all(b"+OK\r\n").is_err() || conn.flush().is_err() {
                            return;
                        }
                        buf.copy_within(consumed..pending, 0);
                        pending -= consumed;
                        continue 'process;
                    }

                    if mismatch_since.is_none() {
                        let parts: Vec<String> = arg_refs.iter().map(|a| String::from_utf8_lossy(a).into_owned()).collect();
                        eprintln!("  backend: WARNING non-matching command during replay: {parts:?}, waiting...");
                        mismatch_since = Some(Instant::now());
                    }
                    break 'process;
                }
                None => break 'process,
            }
        }

        // --- Check mismatch timeout ---
        if let Some(since) = mismatch_since
            && since.elapsed() >= mismatch_timeout
        {
            let mut q = queue.lock().expect("queue lock");
            if let Some(entry) = q.entries.pop_front() {
                let outgoing = entry.outgoing;
                drop(q);
                eprintln!("  backend: WARNING match timeout, responding with PCAP outgoing ({} bytes)", outgoing.len());
                if conn.write_all(&outgoing).is_err() || conn.flush().is_err() {
                    return;
                }
                pending = 0;
                mismatch_since = None;
                continue;
            }
        }

        // --- Read more data ---
        match conn.read(&mut buf[pending..]) {
            Ok(0) => {
                eprintln!("  backend: connection closed");
                break;
            }
            Ok(n) => {
                pending += n;
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {}
            Err(e) => {
                eprintln!("  backend: read error: {e}");
                break;
            }
        }
    }
}
