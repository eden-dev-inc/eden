//! Application state and event handling.

use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use postgres::Client;

use crate::db::{self, DbStats, PgUrlParts};
use crate::events::ApiEvent;
use crate::migration::{ApiCallStatus, MigrationState, MigrationStatus, SetupStep};
use crate::tasks;

pub struct Config {
    pub source_url: String,
    pub dest_url: String,
    pub eden_source_url: String,
    pub eden_dest_url: String,
    pub api_base: String,
}

pub struct App {
    pub config: Config,
    pub source_parts: PgUrlParts,
    pub dest_parts: PgUrlParts,
    pub eden_source_parts: PgUrlParts,
    pub eden_dest_parts: PgUrlParts,
    pub db_stats: Vec<DbStats>,
    pub source_client: Option<Client>,
    pub dest_client: Option<Client>,
    pub start_time: Instant,
    pub last_update: Instant,
    pub total_ticks: u64,
    pub should_quit: bool,
    pub show_tps: bool,
    pub show_debug: bool,
    pub debug_log: Vec<String>,
    pub debug_scroll: usize,
    pub migration_state: MigrationState,
    pub api_event_tx: mpsc::Sender<ApiEvent>,
    pub api_event_rx: mpsc::Receiver<ApiEvent>,
    pub runtime: tokio::runtime::Handle,
}

impl App {
    pub fn new(
        config: Config,
        source_parts: PgUrlParts,
        dest_parts: PgUrlParts,
        eden_source_parts: PgUrlParts,
        eden_dest_parts: PgUrlParts,
        api_event_tx: mpsc::Sender<ApiEvent>,
        api_event_rx: mpsc::Receiver<ApiEvent>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let db_stats = vec![
            DbStats::new(source_parts.port.clone()),
            DbStats::new(dest_parts.port.clone()),
        ];

        let api_base = config.api_base.clone();

        Self {
            config,
            source_parts,
            dest_parts,
            eden_source_parts,
            eden_dest_parts,
            db_stats,
            source_client: None,
            dest_client: None,
            start_time: Instant::now(),
            last_update: Instant::now(),
            total_ticks: 0,
            should_quit: false,
            show_tps: true,
            show_debug: false,
            debug_log: Vec::new(),
            debug_scroll: 0,
            migration_state: MigrationState::new(api_base),
            api_event_tx,
            api_event_rx,
            runtime,
        }
    }

    fn log_debug(&mut self, msg: String) {
        log::debug!("{}", msg);
        let was_at_bottom = self.debug_scroll == 0;
        if self.debug_log.len() >= 500 {
            self.debug_log.remove(0);
        }
        self.debug_log.push(msg);
        if was_at_bottom {
            self.debug_scroll = 0;
        }
    }

    pub fn debug_scroll_up(&mut self, amount: usize) {
        let max_scroll = self.debug_log.len().saturating_sub(1);
        self.debug_scroll = (self.debug_scroll + amount).min(max_scroll);
    }

    pub fn debug_scroll_down(&mut self, amount: usize) {
        self.debug_scroll = self.debug_scroll.saturating_sub(amount);
    }

    pub fn process_api_events(&mut self) {
        while let Ok(event) = self.api_event_rx.try_recv() {
            match event {
                ApiEvent::SetupProgress(step) => {
                    self.migration_state.setup_step = step;
                }
                ApiEvent::ApiCallUpdate { index, ref status } => {
                    if !matches!(status, ApiCallStatus::InProgress | ApiCallStatus::Pending) {
                        let name = self
                            .migration_state
                            .api_calls
                            .get(index)
                            .map(|c| c.name.clone())
                            .unwrap_or_else(|| format!("Call {}", index));
                        match status {
                            ApiCallStatus::Success => {
                                self.log_debug(format!("{}: OK", name));
                            }
                            ApiCallStatus::Failed(e) => {
                                self.log_debug(format!("{}: FAIL - {}", name, e));
                            }
                            ApiCallStatus::Skipped => {
                                self.log_debug(format!("{}: skipped", name));
                            }
                            _ => {}
                        }
                    }
                    self.migration_state.update_api_call(index, status.clone());
                }
                ApiEvent::SetupComplete {
                    auth_token,
                    source_endpoint_id,
                    dest_endpoint_id,
                    interlay_id,
                    migration_id,
                } => {
                    self.log_debug("Setup complete".to_string());
                    self.migration_state.auth_token = Some(auth_token);
                    self.migration_state.source_endpoint_id = Some(source_endpoint_id);
                    self.migration_state.dest_endpoint_id = Some(dest_endpoint_id);
                    self.migration_state.interlay_id = Some(interlay_id);
                    self.migration_state.migration_id = Some(migration_id);
                    self.migration_state.setup_step = SetupStep::Ready;
                    self.migration_state.last_error = None;
                }
                ApiEvent::SetupFailed(err) => {
                    self.log_debug(format!("Setup FAILED: {}", err));
                    self.migration_state.setup_step = SetupStep::Failed(err.clone());
                    self.migration_state.last_error = Some(err);
                }
                ApiEvent::MigrationTriggered => {
                    self.log_debug("Migration started".to_string());
                    self.migration_state.status = MigrationStatus::Running;
                    self.migration_state.last_error = None;
                }
                ApiEvent::MigrationStatusUpdate { ref status, force } => {
                    let current = &self.migration_state.status;

                    let should_skip = if force {
                        false
                    } else {
                        let current_is_protected = matches!(
                            current,
                            MigrationStatus::Completed
                                | MigrationStatus::Failed
                                | MigrationStatus::RolledBack
                                | MigrationStatus::RollingBack
                                | MigrationStatus::Paused
                        );
                        let new_is_non_terminal = matches!(
                            status,
                            MigrationStatus::Pending
                                | MigrationStatus::Testing
                                | MigrationStatus::Ready
                                | MigrationStatus::Running
                        );
                        let is_pre_running = matches!(
                            status,
                            MigrationStatus::Pending
                                | MigrationStatus::Testing
                                | MigrationStatus::Ready
                        );
                        let running_downgrade =
                            *current == MigrationStatus::Running && is_pre_running;

                        (current_is_protected && new_is_non_terminal) || running_downgrade
                    };

                    if should_skip {
                        self.log_debug(format!(
                            "Ignoring stale status {:?} (current: {:?})",
                            status, current
                        ));
                    } else {
                        match status {
                            MigrationStatus::Completed => {
                                self.log_debug("Migration completed".to_string())
                            }
                            MigrationStatus::Failed => {
                                self.log_debug("Migration failed".to_string())
                            }
                            MigrationStatus::PartialFailure => {
                                self.log_debug("Migration partial failure".to_string())
                            }
                            MigrationStatus::RolledBack => {
                                self.log_debug("Migration rolled back".to_string())
                            }
                            MigrationStatus::RollingBack => {
                                self.log_debug("Migration rolling back".to_string())
                            }
                            _ => {}
                        }
                        self.migration_state.status = status.clone();
                    }
                }
                ApiEvent::MigrationError(err) => {
                    self.log_debug(format!("Error: {}", err));
                    self.migration_state.last_error = Some(err);
                }
                ApiEvent::TrafficUpdated {
                    old_percentage,
                    new_percentage,
                } => {
                    self.log_debug(format!(
                        "Traffic: {:.0}% \u{2192} {:.0}%",
                        old_percentage * 100.0,
                        new_percentage * 100.0
                    ));
                    self.migration_state.canary.read_percentage = new_percentage;
                }
                ApiEvent::TrafficUpdateFailed(err) => {
                    self.log_debug(format!("Traffic update failed: {}", err));
                    self.migration_state.last_error = Some(err);
                }
                ApiEvent::MigrationCompleted => {
                    self.log_debug("Migration manually completed".to_string());
                    self.migration_state.status = MigrationStatus::Completed;
                    self.migration_state.last_error = None;
                }
                ApiEvent::MigrationCompleteFailed(err) => {
                    self.log_debug(format!("Complete failed: {}", err));
                    self.migration_state.last_error = Some(err);
                }
                ApiEvent::MigrationRolledBack => {
                    self.log_debug("Migration rollback initiated".to_string());
                    self.migration_state.status = MigrationStatus::RollingBack;
                    self.migration_state.last_error = None;
                }
                ApiEvent::MigrationRollbackFailed(err) => {
                    self.log_debug(format!("Rollback failed: {}", err));
                    self.migration_state.last_error = Some(err);
                }
                ApiEvent::EnvironmentToggled {
                    previous_active,
                    new_active,
                } => {
                    self.log_debug(format!(
                        "Environment toggled: {} \u{2192} {}",
                        previous_active, new_active
                    ));
                    self.migration_state.active_is_new = new_active.to_lowercase().contains("new")
                        || new_active.to_lowercase().contains("green");
                    self.migration_state.last_error = None;
                }
                ApiEvent::EnvironmentToggleFailed(err) => {
                    self.log_debug(format!("Environment toggle failed: {}", err));
                    self.migration_state.last_error = Some(err);
                }
                ApiEvent::MigrationPaused => {
                    self.log_debug("Migration paused".to_string());
                    self.migration_state.status = MigrationStatus::Paused;
                    self.migration_state.last_error = None;
                }
                ApiEvent::MigrationPauseFailed(err) => {
                    self.log_debug(format!("Pause failed: {}", err));
                    self.migration_state.last_error = Some(err);
                }
                ApiEvent::MigrationResumed => {
                    self.log_debug("Migration resumed".to_string());
                    self.migration_state.status = MigrationStatus::Running;
                    self.migration_state.last_error = None;
                }
                ApiEvent::MigrationResumeFailed(err) => {
                    self.log_debug(format!("Resume failed: {}", err));
                    self.migration_state.last_error = Some(err);
                }
                ApiEvent::DebugLog(msg) => {
                    self.log_debug(msg);
                }
            }
        }
    }

    pub fn update(&mut self) {
        self.total_ticks += 1;

        // Update source stats (reusing persistent connection)
        db::update_stats(
            &mut self.db_stats[0],
            &mut self.source_client,
            &self.config.source_url,
        );
        self.db_stats[0].push_history(self.total_ticks);

        // Update dest stats (reusing persistent connection)
        db::update_stats(
            &mut self.db_stats[1],
            &mut self.dest_client,
            &self.config.dest_url,
        );
        self.db_stats[1].push_history(self.total_ticks);

        self.last_update = Instant::now();
    }

    pub fn runtime(&self) -> Duration {
        self.start_time.elapsed()
    }

    // ==========================================
    // Key handlers
    // ==========================================

    pub fn handle_migrate_key(&mut self) {
        if self.migration_state.can_migrate() {
            let tx = self.api_event_tx.clone();
            let token = self.migration_state.auth_token.clone().unwrap();
            let org_id = self.migration_state.org_id.clone();
            let migration_id = self.migration_state.migration_id.clone().unwrap();
            let api_base = self.migration_state.api_base.clone();

            self.runtime.spawn(tasks::trigger_migration_task(
                tx,
                token,
                org_id,
                migration_id,
                api_base,
            ));
        }
    }

    pub fn handle_refresh_key(&mut self) {
        if self.migration_state.is_ready() {
            let tx = self.api_event_tx.clone();
            let token = self.migration_state.auth_token.clone().unwrap();
            let org_id = self.migration_state.org_id.clone();
            let migration_id = self.migration_state.migration_id.clone().unwrap();
            let api_base = self.migration_state.api_base.clone();

            self.runtime.spawn(tasks::refresh_migration_task(
                tx,
                token,
                org_id,
                migration_id,
                api_base,
            ));
        }
    }

    pub fn handle_setup_key(&mut self) {
        if self.migration_state.setup_step == SetupStep::NotStarted {
            let tx = self.api_event_tx.clone();
            let source_url = self.config.eden_source_url.clone();
            let dest_url = self.config.eden_dest_url.clone();
            let source_port = self.eden_source_parts.port.clone();
            let dest_port = self.eden_dest_parts.port.clone();
            let org_id = self.migration_state.org_id.clone();
            let api_base = self.migration_state.api_base.clone();
            let mode = self.migration_state.mode;
            let canary_state = self.migration_state.canary.clone();

            self.log_debug(format!(
                "Eden endpoints: {} \u{2192} {}",
                self.eden_source_parts.full_url, self.eden_dest_parts.full_url
            ));

            self.runtime.spawn(tasks::run_migration_setup(
                tx,
                source_url,
                dest_url,
                source_port,
                dest_port,
                org_id,
                api_base,
                mode,
                canary_state,
            ));
        }
    }

    pub fn handle_toggle_mode(&mut self) {
        if self.migration_state.setup_step == SetupStep::NotStarted {
            self.migration_state.mode = self.migration_state.mode.toggle();
            self.log_debug(format!("Mode: {}", self.migration_state.mode.name()));
        }
    }

    pub fn handle_complete_key(&mut self) {
        if self.migration_state.can_complete() {
            let tx = self.api_event_tx.clone();
            let token = self.migration_state.auth_token.clone().unwrap();
            let org_id = self.migration_state.org_id.clone();
            let migration_id = self.migration_state.migration_id.clone().unwrap();
            let api_base = self.migration_state.api_base.clone();

            self.runtime.spawn(tasks::complete_migration_task(
                tx,
                token,
                org_id,
                migration_id,
                api_base,
            ));
        }
    }

    pub fn handle_toggle_environment(&mut self) {
        if self.migration_state.can_toggle_environment() {
            let tx = self.api_event_tx.clone();
            let token = self.migration_state.auth_token.clone().unwrap();
            let org_id = self.migration_state.org_id.clone();
            let migration_id = self.migration_state.migration_id.clone().unwrap();
            let api_base = self.migration_state.api_base.clone();
            let current_active_is_new = self.migration_state.active_is_new;

            self.runtime.spawn(tasks::toggle_environment_task(
                tx,
                token,
                org_id,
                migration_id,
                api_base,
                current_active_is_new,
            ));
        }
    }

    pub fn handle_rollback_key(&mut self) {
        if self.migration_state.can_rollback() {
            let tx = self.api_event_tx.clone();
            let token = self.migration_state.auth_token.clone().unwrap();
            let org_id = self.migration_state.org_id.clone();
            let migration_id = self.migration_state.migration_id.clone().unwrap();
            let interlay_id = self.migration_state.interlay_id.clone().unwrap();
            let api_base = self.migration_state.api_base.clone();

            self.runtime.spawn(tasks::rollback_migration_task(
                tx,
                token,
                org_id,
                migration_id,
                interlay_id,
                api_base,
            ));
        }
    }

    pub fn handle_pause_key(&mut self) {
        if self.migration_state.can_pause() {
            let tx = self.api_event_tx.clone();
            let token = self.migration_state.auth_token.clone().unwrap();
            let org_id = self.migration_state.org_id.clone();
            let migration_id = self.migration_state.migration_id.clone().unwrap();
            let api_base = self.migration_state.api_base.clone();

            self.runtime.spawn(tasks::pause_migration_task(
                tx,
                token,
                org_id,
                migration_id,
                api_base,
            ));
        }
    }

    pub fn handle_resume_key(&mut self) {
        if self.migration_state.can_resume() {
            let tx = self.api_event_tx.clone();
            let token = self.migration_state.auth_token.clone().unwrap();
            let org_id = self.migration_state.org_id.clone();
            let migration_id = self.migration_state.migration_id.clone().unwrap();
            let api_base = self.migration_state.api_base.clone();

            self.runtime.spawn(tasks::resume_migration_task(
                tx,
                token,
                org_id,
                migration_id,
                api_base,
            ));
        }
    }

    pub fn handle_traffic_increase(&mut self) {
        if self.migration_state.can_update_traffic() {
            let new_percentage = (self.migration_state.canary.read_percentage + 0.05).min(1.0);
            self.update_canary_traffic(new_percentage);
        }
    }

    pub fn handle_traffic_decrease(&mut self) {
        if self.migration_state.can_update_traffic() {
            let new_percentage = (self.migration_state.canary.read_percentage - 0.05).max(0.0);
            self.update_canary_traffic(new_percentage);
        }
    }

    fn update_canary_traffic(&mut self, new_percentage: f64) {
        let tx = self.api_event_tx.clone();
        let token = self.migration_state.auth_token.clone().unwrap();
        let org_id = self.migration_state.org_id.clone();
        let migration_id = self.migration_state.migration_id.clone().unwrap();
        let api_base = self.migration_state.api_base.clone();

        self.runtime.spawn(tasks::update_traffic_task(
            tx,
            token,
            org_id,
            migration_id,
            api_base,
            new_percentage,
        ));
    }
}
