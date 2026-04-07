//! TUI rendering functions for the postgres-observer dashboard.

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    symbols,
    text::{Line, Span},
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph, Row, Table},
};

use crate::app::App;
use crate::db::{DbStatus, HISTORY_SIZE};
use crate::migration::{ApiCallStatus, MigrationMode, MigrationStatus, SetupStep};

fn format_delta(delta: i64) -> (String, Color) {
    if delta > 0 {
        (format!("+{}", delta), Color::Green)
    } else if delta < 0 {
        (format!("{}", delta), Color::Red)
    } else {
        ("\u{2014}".to_string(), Color::DarkGray) // em-dash
    }
}

fn draw_db_table(f: &mut Frame, area: Rect, app: &App) {
    let eden_urls_differ = app.config.eden_source_url != app.config.source_url
        || app.config.eden_dest_url != app.config.dest_url;

    let title_suffix = if eden_urls_differ {
        format!(
            " (TUI: {}+{} | Eden: {}+{}) ",
            app.source_parts.port,
            app.dest_parts.port,
            app.eden_source_parts.port,
            app.eden_dest_parts.port
        )
    } else {
        " Instances ".to_string()
    };

    let header = Row::new(vec![
        "port", "active", "rows", "\u{0394}", "tps", "conn", "tables",
    ])
    .style(Style::default().fg(Color::DarkGray))
    .bottom_margin(1);

    let rows: Vec<Row> = app
        .db_stats
        .iter()
        .enumerate()
        .map(|(i, stats)| {
            let status_color = if stats.status == DbStatus::Connected {
                Color::Cyan
            } else {
                Color::Red
            };

            let (delta_str, delta_color) = format_delta(stats.rows_delta);

            // Determine if this endpoint is active in BlueGreen mode
            let is_active = if app.migration_state.mode == MigrationMode::BlueGreen
                && app.migration_state.status == MigrationStatus::Running
            {
                (i == 0 && !app.migration_state.active_is_new)
                    || (i == 1 && app.migration_state.active_is_new)
            } else {
                false
            };

            let active_span = if is_active {
                Span::styled("\u{25CF}", Style::default().fg(Color::Green).bold())
            } else {
                Span::styled("\u{25CB}", Style::default().fg(Color::DarkGray))
            };

            Row::new(vec![
                Span::styled(
                    format!(":{}", stats.port),
                    Style::default().fg(status_color),
                ),
                active_span,
                Span::styled(format!("{}", stats.rows), Style::default().fg(Color::White)),
                Span::styled(delta_str, Style::default().fg(delta_color)),
                Span::styled(format!("{}", stats.tps), Style::default().fg(Color::Yellow)),
                Span::styled(
                    format!("{}", stats.connected_clients),
                    Style::default().fg(Color::Magenta),
                ),
                Span::styled(
                    format!("{}", stats.table_count),
                    Style::default().fg(Color::Green),
                ),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(7),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(6),
            Constraint::Length(7),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .title(title_suffix)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );

    f.render_widget(table, area);
}

fn draw_rows_chart(f: &mut Frame, area: Rect, app: &App) {
    let colors = [Color::Cyan, Color::Yellow, Color::Green];

    let max_val = app
        .db_stats
        .iter()
        .flat_map(|s| s.rows_history.iter().map(|(_, y)| *y))
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
                .data(&stats.rows_history)
        })
        .collect();

    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .title(" Rows (overlaid) ")
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

fn draw_tps_chart(f: &mut Frame, area: Rect, app: &App) {
    let colors = [Color::Cyan, Color::Yellow, Color::Green];

    let all_values: Vec<f64> = app
        .db_stats
        .iter()
        .flat_map(|s| s.tps_history.iter().map(|(_, y)| *y))
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
                .data(&stats.tps_history)
        })
        .collect();

    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .title(" TPS ")
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

fn style_debug_line(msg: &str) -> Line<'_> {
    // Request lines: >>> METHOD URL
    if msg.starts_with(">>> ") {
        let rest = &msg[4..];
        return Line::from(vec![
            Span::styled("  >>> ", Style::default().fg(Color::Cyan).bold()),
            Span::styled(rest, Style::default().fg(Color::Cyan)),
        ]);
    }

    // Response lines: <<< STATUS URL or <<< Body: ...
    if msg.starts_with("<<< ") {
        let rest = &msg[4..];
        // Check for error status codes (4xx, 5xx)
        let is_error = rest.starts_with("4") || rest.starts_with("5");
        let is_body = rest.starts_with("Body:");
        let color = if is_error {
            Color::Red
        } else if is_body {
            Color::DarkGray
        } else {
            Color::Green
        };
        return Line::from(vec![
            Span::styled("  <<< ", Style::default().fg(color).bold()),
            Span::styled(rest, Style::default().fg(color)),
        ]);
    }

    // Error / failure lines
    if msg.contains("FAIL")
        || msg.contains("Error")
        || msg.contains("error")
        || msg.contains("failed")
    {
        return Line::from(Span::styled(
            format!("  {}", msg),
            Style::default().fg(Color::Red),
        ));
    }

    // Success lines
    if msg.contains("OK") || msg.contains("complete") || msg.contains("Complete") {
        return Line::from(Span::styled(
            format!("  {}", msg),
            Style::default().fg(Color::Green),
        ));
    }

    // Skipped
    if msg.contains("skipped") || msg.contains("Skipped") {
        return Line::from(Span::styled(
            format!("  {}", msg),
            Style::default().fg(Color::Cyan),
        ));
    }

    // Started / initiated
    if msg.contains("started") || msg.contains("Started") || msg.contains("initiated") {
        return Line::from(Span::styled(
            format!("  {}", msg),
            Style::default().fg(Color::Yellow),
        ));
    }

    // Default
    Line::from(Span::styled(
        format!("  {}", msg),
        Style::default().fg(Color::White),
    ))
}

fn draw_debug_panel(f: &mut Frame, area: Rect, app: &App) {
    let state = &app.migration_state;

    let status_color = match state.status {
        MigrationStatus::Running => Color::Yellow,
        MigrationStatus::Completed => Color::Green,
        MigrationStatus::Failed | MigrationStatus::PartialFailure => Color::Red,
        MigrationStatus::RollingBack => Color::Cyan,
        MigrationStatus::RolledBack => Color::Blue,
        _ => Color::White,
    };

    // Header: state line + scroll indicator
    let scroll_indicator = if app.debug_scroll > 0 {
        format!(" [+{}] ", app.debug_scroll)
    } else {
        String::new()
    };

    let state_line = Line::from(vec![
        Span::styled("State: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:?}", state.status),
            Style::default().fg(status_color).bold(),
        ),
        Span::styled(" | Mode: ", Style::default().fg(Color::DarkGray)),
        Span::styled(state.mode.name(), Style::default().fg(Color::Cyan)),
        Span::styled(" | Setup: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            if state.is_ready() {
                "Ready"
            } else {
                "Not Ready"
            },
            Style::default().fg(if state.is_ready() {
                Color::Green
            } else {
                Color::Yellow
            }),
        ),
        Span::styled(" | Interlay: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            if state.interlay_id.is_some() {
                "Yes"
            } else {
                "No"
            },
            Style::default().fg(if state.interlay_id.is_some() {
                Color::Green
            } else {
                Color::Red
            }),
        ),
        Span::styled(" | Rollback: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            if state.can_rollback() {
                "Available"
            } else {
                "N/A"
            },
            Style::default().fg(if state.can_rollback() {
                Color::Magenta
            } else {
                Color::DarkGray
            }),
        ),
    ]);

    // 2 header lines (state + blank), borders take 2 lines
    let visible_lines = area.height.saturating_sub(4) as usize;
    let total = app.debug_log.len();

    // Scroll from the bottom: scroll=0 means show the most recent lines
    let end = total.saturating_sub(app.debug_scroll);
    let start = end.saturating_sub(visible_lines);

    let log_lines: Vec<Line> = app.debug_log[start..end]
        .iter()
        .map(|msg| style_debug_line(msg))
        .collect();

    let mut all_lines = vec![state_line, Line::from("")];
    all_lines.extend(log_lines);

    let title = format!(
        " Debug ({}/{}) {} \u{2191}\u{2193}PgUp/PgDn ",
        end, total, scroll_indicator
    );
    let paragraph = Paragraph::new(all_lines).block(
        Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    );

    f.render_widget(paragraph, area);
}

fn draw_api_panel(f: &mut Frame, area: Rect, app: &App) {
    let state = &app.migration_state;

    let mut lines = vec![];

    // Mode selector
    let mode_color = match state.mode {
        MigrationMode::BigBang => Color::Cyan,
        MigrationMode::Canary => Color::Yellow,
        MigrationMode::BlueGreen => Color::Blue,
    };
    let mode_can_change = state.setup_step == SetupStep::NotStarted;
    lines.push(Line::from(vec![
        Span::styled("Mode: ", Style::default().fg(Color::White)),
        Span::styled(state.mode.name(), Style::default().fg(mode_color).bold()),
        if mode_can_change {
            Span::styled(" (Tab)", Style::default().fg(Color::DarkGray))
        } else {
            Span::styled("", Style::default())
        },
    ]));

    // Canary percentage
    if state.mode == MigrationMode::Canary {
        let pct = state.canary.read_percentage * 100.0;
        let pct_color = if pct >= 75.0 {
            Color::Green
        } else if pct >= 25.0 {
            Color::Yellow
        } else {
            Color::Cyan
        };
        lines.push(Line::from(vec![
            Span::styled("Traffic: ", Style::default().fg(Color::White)),
            Span::styled(
                format!("{:.0}%", pct),
                Style::default().fg(pct_color).bold(),
            ),
            Span::styled(" to new", Style::default().fg(Color::DarkGray)),
            if state.can_update_traffic() {
                Span::styled(" (+/-)", Style::default().fg(Color::DarkGray))
            } else {
                Span::styled("", Style::default())
            },
        ]));
    }

    // BlueGreen active environment
    if state.mode == MigrationMode::BlueGreen && state.status == MigrationStatus::Running {
        let (active_env, env_color) = if state.active_is_new {
            ("New (Green)", Color::Green)
        } else {
            ("Old (Blue)", Color::Blue)
        };
        lines.push(Line::from(vec![
            Span::styled("Active: ", Style::default().fg(Color::White)),
            Span::styled(active_env, Style::default().fg(env_color).bold()),
            if state.can_toggle_environment() {
                Span::styled(" (t)", Style::default().fg(Color::DarkGray))
            } else {
                Span::styled("", Style::default())
            },
        ]));
    }
    lines.push(Line::from(""));

    // Header
    lines.push(Line::from(Span::styled(
        "API Calls",
        Style::default().fg(Color::White).bold(),
    )));

    // Setup hint
    if state.setup_step == SetupStep::NotStarted {
        lines.push(Line::from(Span::styled(
            "Press 's' to start setup",
            Style::default().fg(Color::Yellow),
        )));
        lines.push(Line::from(""));
    }

    // API call list
    for call in &state.api_calls {
        let (icon, color) = match &call.status {
            ApiCallStatus::Pending => ("\u{25CB}", Color::DarkGray),
            ApiCallStatus::InProgress => ("\u{25D0}", Color::Yellow),
            ApiCallStatus::Success => ("\u{25CF}", Color::Green),
            ApiCallStatus::Failed(_) => ("\u{2717}", Color::Red),
            ApiCallStatus::Skipped => ("\u{2013}", Color::Cyan),
        };

        let status_text = match &call.status {
            ApiCallStatus::Failed(msg) => format!(" {}", msg),
            _ => String::new(),
        };

        lines.push(Line::from(vec![
            Span::styled(format!("{} ", icon), Style::default().fg(color)),
            Span::styled(&call.name, Style::default().fg(color)),
            Span::styled(status_text, Style::default().fg(Color::Red)),
        ]));
    }

    // Migration status
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("Status: ", Style::default().fg(Color::White)),
        match &state.status {
            MigrationStatus::NotSetup => {
                Span::styled("Not configured", Style::default().fg(Color::DarkGray))
            }
            MigrationStatus::Pending => {
                Span::styled("Pending", Style::default().fg(Color::DarkGray))
            }
            MigrationStatus::Testing => {
                Span::styled("Testing...", Style::default().fg(Color::Yellow))
            }
            MigrationStatus::Ready => {
                Span::styled("Ready to migrate", Style::default().fg(Color::Cyan))
            }
            MigrationStatus::Running => {
                Span::styled("Running...", Style::default().fg(Color::Yellow))
            }
            MigrationStatus::PartialFailure => {
                Span::styled("Partial failure", Style::default().fg(Color::Red))
            }
            MigrationStatus::Failed => Span::styled("Failed", Style::default().fg(Color::Red)),
            MigrationStatus::Paused => Span::styled("Paused", Style::default().fg(Color::Yellow)),
            MigrationStatus::Completed => {
                Span::styled("Completed", Style::default().fg(Color::Green))
            }
            MigrationStatus::RollingBack => {
                Span::styled("Rolling back...", Style::default().fg(Color::Yellow))
            }
            MigrationStatus::RolledBack => {
                Span::styled("Rolled back", Style::default().fg(Color::Magenta))
            }
        },
    ]));

    // Migration ID
    if let Some(ref id) = state.migration_id {
        lines.push(Line::from(vec![
            Span::styled("ID: ", Style::default().fg(Color::DarkGray)),
            Span::styled(id.clone(), Style::default().fg(Color::DarkGray)),
        ]));
    }

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .title(" Migration Setup ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Magenta)),
    );

    f.render_widget(paragraph, area);
}

pub fn draw_ui(f: &mut Frame, app: &App) {
    // Main vertical split for debug panel
    let main_area = if app.show_debug {
        let vertical_split = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(15), Constraint::Percentage(40)])
            .split(f.area());
        draw_debug_panel(f, vertical_split[1], app);
        vertical_split[0]
    } else {
        f.area()
    };

    // Main horizontal split: left panel (API status) | right panel (everything else)
    let horizontal_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(32), Constraint::Min(50)])
        .split(main_area);

    // Left panel
    draw_api_panel(f, horizontal_chunks[0], app);

    // Right panel layout
    let right_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Title bar
            Constraint::Length(6), // Stats table
            Constraint::Min(8),    // Charts
            Constraint::Length(1), // Status bar
        ])
        .split(horizontal_chunks[1]);

    // Title bar
    let runtime = app.runtime();
    let title = Line::from(vec![
        Span::styled(
            " postgres-monitor ",
            Style::default().fg(Color::Cyan).bold(),
        ),
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
    f.render_widget(Paragraph::new(title), right_chunks[0]);

    // Stats table
    draw_db_table(f, right_chunks[1], app);

    // Charts
    let chart_constraints = if app.show_tps {
        vec![Constraint::Percentage(50), Constraint::Percentage(50)]
    } else {
        vec![Constraint::Percentage(100)]
    };

    let chart_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(chart_constraints)
        .split(right_chunks[2]);

    draw_rows_chart(f, chart_chunks[0], app);

    if app.show_tps && chart_chunks.len() > 1 {
        draw_tps_chart(f, chart_chunks[1], app);
    }

    // Status bar
    let mut status_spans = vec![
        Span::styled(" q", Style::default().fg(Color::White)),
        Span::styled(" quit  ", Style::default().fg(Color::DarkGray)),
        Span::styled("Tab", Style::default().fg(Color::White)),
        Span::styled(" mode  ", Style::default().fg(Color::DarkGray)),
        Span::styled("s", Style::default().fg(Color::White)),
        Span::styled(" setup  ", Style::default().fg(Color::DarkGray)),
        Span::styled("m", Style::default().fg(Color::White)),
        Span::styled(" migrate  ", Style::default().fg(Color::DarkGray)),
    ];

    if app.migration_state.mode == MigrationMode::Canary && app.migration_state.can_update_traffic()
    {
        status_spans.push(Span::styled("+/-", Style::default().fg(Color::Yellow)));
        status_spans.push(Span::styled(
            " traffic  ",
            Style::default().fg(Color::DarkGray),
        ));
    }

    if app.migration_state.can_toggle_environment() {
        status_spans.push(Span::styled("t", Style::default().fg(Color::Cyan)));
        status_spans.push(Span::styled(
            " toggle  ",
            Style::default().fg(Color::DarkGray),
        ));
    }

    if app.migration_state.can_complete() {
        status_spans.push(Span::styled("c", Style::default().fg(Color::Green)));
        status_spans.push(Span::styled(
            " complete  ",
            Style::default().fg(Color::DarkGray),
        ));
    }

    if app.migration_state.can_pause() {
        status_spans.push(Span::styled("p", Style::default().fg(Color::Yellow)));
        status_spans.push(Span::styled(
            " pause  ",
            Style::default().fg(Color::DarkGray),
        ));
    }

    if app.migration_state.can_resume() {
        status_spans.push(Span::styled("p", Style::default().fg(Color::Cyan)));
        status_spans.push(Span::styled(
            " resume  ",
            Style::default().fg(Color::DarkGray),
        ));
    }

    if app.migration_state.can_rollback() {
        status_spans.push(Span::styled("b", Style::default().fg(Color::Magenta)));
        status_spans.push(Span::styled(
            " rollback  ",
            Style::default().fg(Color::DarkGray),
        ));
    }

    let can_retry = app.migration_state.status == MigrationStatus::Completed;
    if can_retry {
        status_spans.push(Span::styled("r", Style::default().fg(Color::Yellow)));
        status_spans.push(Span::styled(
            " retry  ",
            Style::default().fg(Color::DarkGray),
        ));
    } else {
        status_spans.push(Span::styled("r", Style::default().fg(Color::White)));
        status_spans.push(Span::styled(
            " refresh  ",
            Style::default().fg(Color::DarkGray),
        ));
    }

    status_spans.extend(vec![
        Span::styled("d", Style::default().fg(Color::White)),
        Span::styled(" debug", Style::default().fg(Color::DarkGray)),
    ]);

    let status = Line::from(status_spans);
    f.render_widget(Paragraph::new(status), right_chunks[3]);
}
