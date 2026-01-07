//! Redis Monitor (TUI)
//!
//! A terminal dashboard for monitoring Redis databases.
//!
//! # Usage
//!     cargo run -- <port1> <port2> [port3]
//!
//! # Controls
//!     q / Ctrl+C         Quit
//!     c                  Force coverage check now
//!     v                  Toggle ops/sec chart

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    symbols,
    text::{Line, Span},
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph, Row, Table},
};
use redis::Client;
use std::collections::HashSet;
use std::env;
use std::io;
use std::time::{Duration, Instant};

const HISTORY_SIZE: usize = 120;

struct Config {
    ports: Vec<String>,
}

#[derive(Clone)]
struct DbStats {
    port: String,
    keys: i64,
    keys_delta: i64,
    ops_per_sec: i64,
    connected_clients: i64,
    unique_keys: Option<usize>,
    keys_history: Vec<(f64, f64)>,
    ops_history: Vec<(f64, f64)>,
    coverage: Option<f64>,
    status: DbStatus,
}

#[derive(Clone, PartialEq)]
enum DbStatus {
    Connected,
    Error,
}

impl DbStats {
    fn new(port: String) -> Self {
        Self {
            port,
            keys: 0,
            keys_delta: 0,
            ops_per_sec: 0,
            connected_clients: 0,
            unique_keys: None,
            keys_history: Vec::with_capacity(HISTORY_SIZE),
            ops_history: Vec::with_capacity(HISTORY_SIZE),
            coverage: None,
            status: DbStatus::Connected,
        }
    }

    fn push_history(&mut self, tick: u64) {
        let x = tick as f64;

        if self.keys_history.len() >= HISTORY_SIZE {
            self.keys_history.remove(0);
        }
        if self.ops_history.len() >= HISTORY_SIZE {
            self.ops_history.remove(0);
        }

        self.keys_history.push((x, self.keys.max(0) as f64));
        self.ops_history.push((x, self.ops_per_sec.max(0) as f64));
    }
}

struct App {
    clients: Vec<(String, Client)>,
    db_stats: Vec<DbStats>,
    config: Config,
    start_time: Instant,
    last_update: Instant,
    total_ticks: u64,
    coverage_countdown: u64,
    should_quit: bool,
    force_coverage: bool,
    show_ops: bool,
}

impl App {
    fn new(config: Config) -> Self {
        let clients: Vec<(String, Client)> = config
            .ports
            .iter()
            .filter_map(|port| {
                let url = format!("redis://127.0.0.1:{}", port);
                Client::open(url.as_str()).ok().map(|c| (port.clone(), c))
            })
            .collect();

        let db_stats = clients
            .iter()
            .map(|(port, _)| DbStats::new(port.clone()))
            .collect();

        Self {
            clients,
            db_stats,
            config,
            start_time: Instant::now(),
            last_update: Instant::now(),
            total_ticks: 0,
            coverage_countdown: 0, // Run immediately on first tick
            should_quit: false,
            force_coverage: false,
            show_ops: false,
        }
    }

    fn update(&mut self) {
        self.total_ticks += 1;

        for (i, (_, client)) in self.clients.iter().enumerate() {
            let stats = &mut self.db_stats[i];
            let old_keys = stats.keys;

            match client.get_connection() {
                Ok(mut conn) => {
                    stats.status = DbStatus::Connected;

                    if let Ok(count) = redis::cmd("DBSIZE").query::<i64>(&mut conn) {
                        stats.keys = count;
                        stats.keys_delta = count - old_keys;
                    }

                    if let Ok(info) = redis::cmd("INFO").arg("stats").query::<String>(&mut conn) {
                        stats.ops_per_sec =
                            parse_info_field(&info, "instantaneous_ops_per_sec").unwrap_or(0);
                    }

                    if let Ok(info) = redis::cmd("INFO").arg("clients").query::<String>(&mut conn) {
                        stats.connected_clients =
                            parse_info_field(&info, "connected_clients").unwrap_or(0);
                    }
                }
                Err(_) => {
                    stats.status = DbStatus::Error;
                }
            }

            stats.push_history(self.total_ticks);
        }

        // Coverage check every 15 seconds
        if self.coverage_countdown > 0 {
            self.coverage_countdown -= 1;
        }

        if self.force_coverage || self.coverage_countdown == 0 {
            self.run_coverage_check();
            self.coverage_countdown = 15;
            self.force_coverage = false;
        }

        self.last_update = Instant::now();
    }

    fn run_coverage_check(&mut self) {
        if self.clients.len() < 2 {
            return;
        }

        // Collect all key sets
        let key_sets: Vec<HashSet<String>> = self
            .clients
            .iter()
            .filter_map(|(_, client)| get_all_keys(client))
            .collect();

        if key_sets.len() != self.clients.len() {
            return; // Failed to get keys from all instances
        }

        // Union of all keys across all databases
        let all_keys: HashSet<&String> = key_sets.iter().flat_map(|s| s.iter()).collect();
        let total_unique = all_keys.len();

        // For each instance:
        // - unique = keys only in this instance (not in others)
        // - coverage = my_keys / total_unique
        for (i, stats) in self.db_stats.iter_mut().enumerate() {
            let my_keys = &key_sets[i];

            // Keys unique to this instance (not in any other)
            let my_unique = my_keys
                .iter()
                .filter(|k| {
                    key_sets
                        .iter()
                        .enumerate()
                        .all(|(j, other)| j == i || !other.contains(*k))
                })
                .count();

            stats.unique_keys = Some(my_unique);

            if total_unique > 0 {
                stats.coverage = Some((my_keys.len() as f64 / total_unique as f64) * 100.0);
            } else {
                stats.coverage = Some(100.0);
            }
        }
    }

    fn runtime(&self) -> Duration {
        self.start_time.elapsed()
    }
}

fn get_all_keys(client: &Client) -> Option<HashSet<String>> {
    let mut conn = client.get_connection().ok()?;
    let mut keys = HashSet::new();
    let mut cursor: u64 = 0;

    loop {
        let (new_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("COUNT")
            .arg(1000)
            .query(&mut conn)
            .ok()?;

        keys.extend(batch);
        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    Some(keys)
}

fn parse_info_field(info: &str, field: &str) -> Option<i64> {
    info.lines()
        .find(|line| line.starts_with(field))
        .and_then(|line| line.split(':').nth(1))
        .and_then(|val| val.trim().parse().ok())
}

fn parse_args() -> Config {
    let ports: Vec<String> = env::args().skip(1).collect();
    Config { ports }
}

fn coverage_color(pct: f64) -> Color {
    if pct >= 99.0 {
        Color::Green
    } else if pct >= 90.0 {
        Color::Yellow
    } else {
        Color::Red
    }
}

fn format_delta(delta: i64) -> (String, Color) {
    if delta > 0 {
        (format!("+{}", delta), Color::Green)
    } else if delta < 0 {
        (format!("{}", delta), Color::Red)
    } else {
        ("—".to_string(), Color::DarkGray)
    }
}

fn draw_db_table(f: &mut Frame, area: Rect, app: &App) {
    let header = Row::new(vec![
        "port", "keys", "Δ", "unique", "ops/s", "conn", "coverage",
    ])
    .style(Style::default().fg(Color::DarkGray))
    .bottom_margin(1);

    let rows: Vec<Row> = app
        .db_stats
        .iter()
        .map(|stats| {
            let status_color = if stats.status == DbStatus::Connected {
                Color::Cyan
            } else {
                Color::Red
            };

            let (delta_str, delta_color) = format_delta(stats.keys_delta);

            let unique_span = match stats.unique_keys {
                Some(n) => Span::styled(format!("{}", n), Style::default().fg(Color::White)),
                None => Span::styled("—", Style::default().fg(Color::DarkGray)),
            };

            let coverage_span = match stats.coverage {
                Some(pct) => Span::styled(
                    format!("{:.1}%", pct),
                    Style::default().fg(coverage_color(pct)),
                ),
                None => Span::styled("—", Style::default().fg(Color::DarkGray)),
            };

            Row::new(vec![
                Span::styled(
                    format!(":{}", stats.port),
                    Style::default().fg(status_color),
                ),
                Span::styled(format!("{}", stats.keys), Style::default().fg(Color::White)),
                Span::styled(delta_str, Style::default().fg(delta_color)),
                unique_span,
                Span::styled(
                    format!("{}", stats.ops_per_sec),
                    Style::default().fg(Color::Yellow),
                ),
                Span::styled(
                    format!("{}", stats.connected_clients),
                    Style::default().fg(Color::Magenta),
                ),
                coverage_span,
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(6),
            Constraint::Length(10),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .title(" Instances ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );

    f.render_widget(table, area);
}

fn draw_keys_chart(f: &mut Frame, area: Rect, app: &App) {
    let colors = [Color::Cyan, Color::Yellow, Color::Green];

    // Calculate shared bounds - Y always starts at 0
    let max_val = app
        .db_stats
        .iter()
        .flat_map(|s| s.keys_history.iter().map(|(_, y)| *y))
        .fold(1.0_f64, f64::max);

    let y_max = max_val * 1.05;

    let x_min = app.total_ticks.saturating_sub(HISTORY_SIZE as u64) as f64;
    let x_max = app.total_ticks as f64;

    let datasets: Vec<Dataset> = app
        .db_stats
        .iter()
        .enumerate()
        .map(|(i, stats)| {
            Dataset::default()
                .name(format!(":{}", stats.port))
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(colors[i % colors.len()]))
                .data(&stats.keys_history)
        })
        .collect();

    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .title(" Keys (overlaid) ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        )
        .x_axis(Axis::default().bounds([x_min, x_max]).labels(vec![
            Span::styled(
                format!("-{}s", HISTORY_SIZE),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled("now", Style::default().fg(Color::DarkGray)),
        ]))
        .y_axis(Axis::default().bounds([0.0, y_max]).labels(vec![
            Span::styled("0", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}", max_val as i64),
                Style::default().fg(Color::DarkGray),
            ),
        ]));

    f.render_widget(chart, area);
}

fn draw_ops_chart(f: &mut Frame, area: Rect, app: &App) {
    let colors = [Color::Cyan, Color::Yellow, Color::Green];

    let all_values: Vec<f64> = app
        .db_stats
        .iter()
        .flat_map(|s| s.ops_history.iter().map(|(_, y)| *y))
        .collect();

    let max_val = all_values.iter().cloned().fold(1.0_f64, f64::max);

    let x_min = app.total_ticks.saturating_sub(HISTORY_SIZE as u64) as f64;
    let x_max = app.total_ticks as f64;

    let datasets: Vec<Dataset> = app
        .db_stats
        .iter()
        .enumerate()
        .map(|(i, stats)| {
            Dataset::default()
                .name(format!(":{}", stats.port))
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(colors[i % colors.len()]))
                .data(&stats.ops_history)
        })
        .collect();

    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .title(" Ops/sec ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        )
        .x_axis(Axis::default().bounds([x_min, x_max]).labels(vec![
            Span::styled(
                format!("-{}s", HISTORY_SIZE),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled("now", Style::default().fg(Color::DarkGray)),
        ]))
        .y_axis(Axis::default().bounds([0.0, max_val * 1.1]).labels(vec![
            Span::styled("0", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}", max_val as i64),
                Style::default().fg(Color::DarkGray),
            ),
        ]));

    f.render_widget(chart, area);
}

fn draw_ui(f: &mut Frame, app: &App) {
    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Title bar
            Constraint::Length(6), // Stats table
            Constraint::Min(10),   // Charts
            Constraint::Length(1), // Status bar
        ])
        .split(f.area());

    // Title bar
    let runtime = app.runtime();
    let title = Line::from(vec![
        Span::styled(" redis-monitor ", Style::default().fg(Color::Cyan).bold()),
        Span::styled(
            format!(
                "{}:{:02}:{:02}",
                runtime.as_secs() / 3600,
                (runtime.as_secs() % 3600) / 60,
                runtime.as_secs() % 60
            ),
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    f.render_widget(Paragraph::new(title), main_chunks[0]);

    // Stats table
    draw_db_table(f, main_chunks[1], app);

    // Charts - overlaid view
    let chart_constraints = if app.show_ops {
        vec![Constraint::Percentage(50), Constraint::Percentage(50)]
    } else {
        vec![Constraint::Percentage(100)]
    };

    let chart_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(chart_constraints)
        .split(main_chunks[2]);

    draw_keys_chart(f, chart_chunks[0], app);

    if app.show_ops && chart_chunks.len() > 1 {
        draw_ops_chart(f, chart_chunks[1], app);
    }

    // Status bar
    let status = Line::from(vec![
        Span::styled(" q", Style::default().fg(Color::White)),
        Span::styled(" quit  ", Style::default().fg(Color::DarkGray)),
        Span::styled("c", Style::default().fg(Color::White)),
        Span::styled(
            format!(" coverage ({}s)  ", app.coverage_countdown),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled("v", Style::default().fg(Color::White)),
        Span::styled(
            if app.show_ops {
                " hide ops"
            } else {
                " show ops"
            },
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    f.render_widget(Paragraph::new(status), main_chunks[3]);
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = parse_args();

    if config.ports.is_empty() {
        eprintln!("Usage: cargo run -- <port1> <port2> [port3]");
        std::process::exit(1);
    }

    if config.ports.len() > 3 {
        eprintln!("Warning: This tool is optimized for 2-3 instances");
    }

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(config);
    let tick_rate = Duration::from_secs(1);

    loop {
        terminal.draw(|f| draw_ui(f, &app))?;

        let timeout = tick_rate.saturating_sub(app.last_update.elapsed());

        if crossterm::event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') => app.should_quit = true,
                        KeyCode::Char('c') => app.force_coverage = true,
                        KeyCode::Char('v') => app.show_ops = !app.show_ops,
                        KeyCode::Esc => app.should_quit = true,
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
        let unique_str = stats
            .unique_keys
            .map(|n| format!("{}", n))
            .unwrap_or_else(|| "—".to_string());
        let coverage_str = stats
            .coverage
            .map(|p| format!("{:.1}%", p))
            .unwrap_or_else(|| "—".to_string());
        println!(
            ":{} keys={} unique={} coverage={}",
            stats.port, stats.keys, unique_str, coverage_str
        );
    }

    Ok(())
}
