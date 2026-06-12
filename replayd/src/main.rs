use clap::Parser;
use std::collections::VecDeque;
use std::net::TcpListener;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread;

use replayd::backend::run_backend_pool;
use replayd::pcap::{DbServer, parse_pcap};
use replayd::replay::{ReplayEntry, ReplayQueue, connect_to_eden, replay_end_to_end};

/// Replay PCAP traffic through a proxy to verify transparency.
#[derive(Parser)]
#[command(name = "replayd")]
struct Cli {
    /// DB server address to identify traffic direction (e.g. 6379, 10.0.0.5:6379)
    #[arg(long)]
    db_server: String,

    /// Port to listen for PCAP stream
    #[arg(long, default_value = "8888")]
    listen_port: String,

    /// Proxy address to replay through (e.g. localhost:6366)
    #[arg(long)]
    eden_server: String,

    /// Port to listen for proxy backend connections (e.g. 8001)
    #[arg(long)]
    backend_listen: String,

    /// Print packet hexdumps and detailed replay info
    #[arg(short, long)]
    verbose: bool,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let listen_port = cli.listen_port;
    let eden_server = cli.eden_server;
    let backend_listen = cli.backend_listen;
    let verbose = cli.verbose;
    let db_server = DbServer::parse(&cli.db_server)?;

    // Start backend pool on the backend port.
    let replay_queue = Arc::new(Mutex::new(ReplayQueue { entries: VecDeque::new(), pcap_ready: false }));

    let backend_listener = TcpListener::bind(format!("0.0.0.0:{backend_listen}"))?;
    eprintln!("backend listening on :{backend_listen}");

    let stop_flag = Arc::new(AtomicBool::new(false));
    let listener_clone = backend_listener.try_clone()?;
    let queue = Arc::clone(&replay_queue);
    let v = verbose;
    thread::spawn(move || {
        run_backend_pool(listener_clone, stop_flag, queue, v);
    });

    // Keep backend_listener alive so the socket stays bound.
    std::mem::forget(backend_listener);

    // Bind PCAP listener and loop: accept PCAP, parse, replay, repeat.
    let pcap_addr = format!("0.0.0.0:{listen_port}");
    let pcap_listener = TcpListener::bind(&pcap_addr)?;
    eprintln!("db_server: {db_server}");
    eprintln!("listening for pcap on {pcap_addr}");

    loop {
        let (stream, peer) = pcap_listener.accept()?;
        eprintln!("pcap connected: {peer}");

        let exchanges = match parse_pcap(stream, &db_server, verbose) {
            Ok(ex) => ex,
            Err(e) => {
                eprintln!("pcap error: {e}");
                continue;
            }
        };

        // Populate the shared queue with PCAP exchanges.
        {
            let mut q = replay_queue.lock().expect("queue lock");
            q.entries.clear();
            for ex in &exchanges {
                q.entries.push_back(ReplayEntry { incoming: ex.incoming.clone(), outgoing: ex.outgoing.clone() });
            }
            q.pcap_ready = true;
            eprintln!("replay queue loaded: {} exchanges", q.entries.len());
        }

        // Connect to Eden and replay.
        match connect_to_eden(&eden_server, verbose) {
            Ok(mut eden_conn) => {
                if let Err(e) = replay_end_to_end(&mut eden_conn, &exchanges, &replay_queue, verbose) {
                    eprintln!("replay error: {e}");
                }
            }
            Err(e) => {
                eprintln!("eden connect error: {e}");
            }
        }

        // Reset queue for next run.
        {
            let mut q = replay_queue.lock().expect("queue lock");
            q.entries.clear();
            q.pcap_ready = false;
        }

        eprintln!("listening for pcap on {pcap_addr}");
    }
}
