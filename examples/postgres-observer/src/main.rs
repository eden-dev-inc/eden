//! PostgreSQL Monitor (TUI)
//!
//! A terminal dashboard for monitoring PostgreSQL databases with migration support.
//!
//! # Usage
//!     cargo run -- <source_url> <dest_url> [api_endpoint] [eden_source_url] [eden_dest_url]
//!
//! # Arguments
//!     source_url       Source PostgreSQL URL (e.g., postgresql://user:pass@host:5432/db)
//!     dest_url         Destination PostgreSQL URL
//!     api_endpoint     Eden API endpoint (default: http://localhost:8000)
//!     eden_source_url  Eden's source PostgreSQL URL (when different from TUI connection)
//!     eden_dest_url    Eden's dest PostgreSQL URL (when different from TUI connection)
//!
//! # Examples
//!     cargo run -- postgresql://postgres:postgres@localhost:5432/src postgresql://postgres:postgres@localhost:5433/dst
//!     cargo run -- postgresql://postgres:postgres@localhost:5432/src postgresql://postgres:postgres@localhost:5433/dst http://localhost:8000
//!
//! # Controls
//!     q / Ctrl+C         Quit
//!     c                  Complete running migration
//!     b                  Rollback completed/failed migration
//!     v                  Toggle TPS chart
//!     Tab                Toggle migration mode (BigBang / Canary / BlueGreen)
//!     s                  Start migration setup (connect to Eden API)
//!     m                  Trigger migration
//!     r                  Refresh migration status (retry if completed)
//!     +/=                Increase canary traffic by 5% (canary mode only)
//!     -                  Decrease canary traffic by 5% (canary mode only)
//!     t                  Toggle environment (blue-green mode only)

mod api_types;
pub mod app;
mod db;
mod eden_api;
mod events;
mod migration;
mod tasks;
mod ui;

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};
use std::io;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::app::{App, Config};
use crate::db::{check_connection, parse_pg_url};
use crate::events::ApiEvent;
use crate::ui::draw_ui;

const DEFAULT_API_BASE: &str = "http://localhost:8000";

fn parse_args() -> Option<Config> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.len() < 2 {
        return None;
    }

    let source_url = args[0].clone();
    let dest_url = args[1].clone();
    let api_base = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| DEFAULT_API_BASE.to_string());

    let eden_source_url = args.get(3).cloned().unwrap_or_else(|| source_url.clone());
    let eden_dest_url = args.get(4).cloned().unwrap_or_else(|| dest_url.clone());

    Some(Config {
        source_url,
        dest_url,
        eden_source_url,
        eden_dest_url,
        api_base,
    })
}

fn init_logging() -> Result<(), Box<dyn std::error::Error>> {
    let log_file = "postgres-observer.log";
    if std::path::Path::new(log_file).exists() {
        std::fs::remove_file(log_file)?;
    }

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{} [{}] {}: {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.target(),
                message
            ))
        })
        .level(log::LevelFilter::Debug)
        .chain(fern::log_file(log_file)?)
        .apply()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging()?;
    let config = match parse_args() {
        Some(c) => c,
        None => {
            eprintln!(
                "Usage: cargo run -- <source_url> <dest_url> [api_endpoint] [eden_source_url] [eden_dest_url]"
            );
            eprintln!();
            eprintln!("Arguments:");
            eprintln!(
                "  source_url       Source PostgreSQL URL (e.g., postgresql://user:pass@host:5432/db)"
            );
            eprintln!("  dest_url         Destination PostgreSQL URL");
            eprintln!(
                "  api_endpoint     Eden API endpoint (default: {})",
                DEFAULT_API_BASE
            );
            eprintln!(
                "  eden_source_url  Eden's source PostgreSQL URL (when different from TUI connection)"
            );
            eprintln!(
                "  eden_dest_url    Eden's dest PostgreSQL URL (when different from TUI connection)"
            );
            eprintln!();
            eprintln!("Examples:");
            eprintln!(
                "  cargo run -- postgresql://postgres:postgres@localhost:5432/src postgresql://postgres:postgres@localhost:5433/dst"
            );
            eprintln!(
                "  cargo run -- postgresql://postgres:postgres@localhost:5432/src postgresql://postgres:postgres@localhost:5433/dst http://localhost:8000 postgresql://postgres:postgres@172.24.2.218:5432/src postgresql://postgres:postgres@172.24.2.218:5433/dst"
            );
            std::process::exit(1);
        }
    };

    // Health check: verify PostgreSQL connections BEFORE entering TUI
    log::info!("Checking PostgreSQL connections...");
    if let Err(e) = check_connection("source", &config.source_url) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
    if let Err(e) = check_connection("dest", &config.dest_url) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
    log::info!("All connections verified. Starting TUI...");

    // Parse URL parts for display
    let source_parts = parse_pg_url(&config.source_url);
    let dest_parts = parse_pg_url(&config.dest_url);
    let eden_source_parts = parse_pg_url(&config.eden_source_url);
    let eden_dest_parts = parse_pg_url(&config.eden_dest_url);

    // Create tokio runtime for async API calls
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;

    // Create channel for API events
    let (tx, rx) = mpsc::channel::<ApiEvent>(100);

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(
        config,
        source_parts,
        dest_parts,
        eden_source_parts,
        eden_dest_parts,
        tx,
        rx,
        runtime.handle().clone(),
    );

    let tick_rate = Duration::from_secs(1);

    loop {
        terminal.draw(|f| draw_ui(f, &app))?;

        // Check for API events (non-blocking)
        app.process_api_events();

        let timeout = tick_rate.saturating_sub(app.last_update.elapsed());

        if crossterm::event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') => app.should_quit = true,
                        KeyCode::Char('c') => app.handle_complete_key(),
                        KeyCode::Char('b') => app.handle_rollback_key(),
                        KeyCode::Char('p') => {
                            if app.migration_state.can_pause() {
                                app.handle_pause_key();
                            } else if app.migration_state.can_resume() {
                                app.handle_resume_key();
                            }
                        }
                        KeyCode::Char('t') => app.handle_toggle_environment(),
                        KeyCode::Char('v') => app.show_tps = !app.show_tps,
                        KeyCode::Char('d') => {
                            app.show_debug = !app.show_debug;
                            if app.show_debug {
                                app.debug_scroll = 0; // reset to bottom on toggle
                            }
                        }
                        KeyCode::Up if app.show_debug => app.debug_scroll_up(1),
                        KeyCode::Down if app.show_debug => app.debug_scroll_down(1),
                        KeyCode::PageUp if app.show_debug => app.debug_scroll_up(10),
                        KeyCode::PageDown if app.show_debug => app.debug_scroll_down(10),
                        KeyCode::Esc => app.should_quit = true,
                        KeyCode::Tab => app.handle_toggle_mode(),
                        KeyCode::Char('s') => app.handle_setup_key(),
                        KeyCode::Char('m') => app.handle_migrate_key(),
                        KeyCode::Char('r') => app.handle_refresh_key(),
                        KeyCode::Char('+') | KeyCode::Char('=') => app.handle_traffic_increase(),
                        KeyCode::Char('-') => app.handle_traffic_decrease(),
                        _ => {}
                    }
                }
            }
        }

        if app.last_update.elapsed() >= tick_rate {
            app.update();
        }

        if app.should_quit {
            break;
        }
    }

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    println!("\n--- Summary ---");
    println!("Runtime: {}s", app.runtime().as_secs());
    for stats in &app.db_stats {
        println!(":{} rows={} tps={}", stats.port, stats.rows, stats.tps);
    }

    // Migration summary
    println!("\n--- Migration ---");
    match &app.migration_state.status {
        migration::MigrationStatus::NotSetup => println!("Migration not configured"),
        migration::MigrationStatus::Pending => println!("Migration pending"),
        migration::MigrationStatus::Testing => println!("Migration testing"),
        migration::MigrationStatus::Ready => println!("Migration ready"),
        migration::MigrationStatus::Running => println!("Migration running"),
        migration::MigrationStatus::PartialFailure => println!("Migration partial failure"),
        migration::MigrationStatus::Failed => println!("Migration failed"),
        migration::MigrationStatus::Paused => println!("Migration paused"),
        migration::MigrationStatus::Completed => println!("Migration completed successfully"),
        migration::MigrationStatus::RollingBack => println!("Migration rolling back"),
        migration::MigrationStatus::RolledBack => println!("Migration rolled back"),
    }
    if let Some(ref id) = app.migration_state.migration_id {
        println!("Migration ID: {}", id);
    }

    Ok(())
}
