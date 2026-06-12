use crate::pcap::Exchange;
use crate::util::{first_diff, hexdump_region, read_exact_timeout};
use std::collections::VecDeque;
use std::io::Write;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Shared replay queue: backend handler threads check incoming data against
/// the front entry. When PCAP data matches, they respond with the recorded
/// outgoing bytes instead of a mock response.
pub struct ReplayQueue {
    pub entries: VecDeque<ReplayEntry>,
    pub pcap_ready: bool,
}

pub struct ReplayEntry {
    pub incoming: Vec<u8>,
    pub outgoing: Vec<u8>,
}

/// Connect to eden_server.
pub fn connect_to_eden(eden_addr: &str, verbose: bool) -> Result<TcpStream, Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(5);
    eprintln!("connecting to eden at {eden_addr}...");
    let eden_conn = TcpStream::connect(eden_addr)?;
    eden_conn.set_read_timeout(Some(timeout))?;
    eden_conn.set_write_timeout(Some(timeout))?;
    eprintln!("connected to eden");
    if verbose {
        eprintln!("  eden connection established");
    }
    Ok(eden_conn)
}

/// End-to-end replay: send PCAP incoming to Eden, backend handlers auto-respond
/// with PCAP outgoing, read Eden's response and verify.
pub fn replay_end_to_end(
    eden_conn: &mut TcpStream,
    exchanges: &[Exchange],
    queue: &Arc<Mutex<ReplayQueue>>,
    verbose: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(10);

    let mut passed = 0usize;
    let mut failed = 0usize;

    for (idx, ex) in exchanges.iter().enumerate() {
        if ex.incoming.is_empty() && ex.outgoing.is_empty() {
            println!("exchange {idx}: SKIP (empty)");
            continue;
        }

        if !ex.incoming.is_empty() {
            if verbose {
                eprintln!("  exchange {idx}: sending {} incoming bytes to eden...", ex.incoming.len());
            }
            eden_conn.write_all(&ex.incoming)?;
            eden_conn.flush()?;
        }

        if !ex.outgoing.is_empty() {
            if verbose {
                eprintln!("  exchange {idx}: reading {} bytes from eden...", ex.outgoing.len());
            }
            match read_exact_timeout(eden_conn, ex.outgoing.len(), timeout) {
                Ok(received) => {
                    if received == ex.outgoing {
                        println!("exchange {idx}: {} bytes >> OK | {} bytes << OK", ex.incoming.len(), ex.outgoing.len());
                        passed += 1;
                    } else {
                        let offset = first_diff(&ex.outgoing, &received);
                        println!(
                            "exchange {idx}: {} bytes >> OK | {} bytes << MISMATCH at offset {offset}",
                            ex.incoming.len(),
                            ex.outgoing.len()
                        );
                        eprintln!("  expected:");
                        hexdump_region(&ex.outgoing, offset);
                        eprintln!("  got:");
                        hexdump_region(&received, offset);
                        failed += 1;
                    }
                }
                Err(e) => {
                    println!("exchange {idx}: {} bytes >> sent | {} bytes << ERROR: {e}", ex.incoming.len(), ex.outgoing.len());
                    failed += 1;
                }
            }
        } else {
            println!("exchange {idx}: {} bytes >> sent | 0 bytes << SKIP", ex.incoming.len());
            passed += 1;
        }
    }

    eprintln!();
    eprintln!("verification: {}/{} passed, {} failed", passed, exchanges.len(), failed);

    let remaining = {
        let q = queue.lock().expect("queue lock");
        q.entries.len()
    };
    if remaining > 0 {
        eprintln!("  WARNING: {remaining} replay queue entries were not consumed by backend");
    }

    if failed > 0 {
        eprintln!("keeping connections open...");
    } else {
        eprintln!("all exchanges verified OK");
    }

    Ok(())
}
