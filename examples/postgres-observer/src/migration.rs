//! Migration state machine, modes, and canary state.

// ============================================
// Migration Mode Selection
// ============================================

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum MigrationMode {
    #[default]
    BigBang,
    Canary,
    BlueGreen,
}

impl MigrationMode {
    pub fn toggle(&self) -> Self {
        match self {
            Self::BigBang => Self::Canary,
            Self::Canary => Self::BlueGreen,
            Self::BlueGreen => Self::BigBang,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::BigBang => "BigBang",
            Self::Canary => "Canary",
            Self::BlueGreen => "BlueGreen",
        }
    }
}

/// Canary-specific state for traffic management
#[derive(Debug, Clone)]
pub struct CanaryState {
    /// Current read percentage routed to new system (0.0 to 1.0)
    pub read_percentage: f64,
    /// Write consistency policy
    pub write_policy: &'static str,
}

impl Default for CanaryState {
    fn default() -> Self {
        Self {
            read_percentage: 0.05, // Start with 5%
            write_policy: "OldAuthoritative",
        }
    }
}

// ============================================
// Migration State Machine
// ============================================

#[derive(Debug, Clone, PartialEq)]
pub enum SetupStep {
    NotStarted,
    CreatingOrganization,
    LoggingIn,
    CreatingSourceEndpoint,
    CreatingDestEndpoint,
    CreatingInterlay,
    CreatingMigration,
    AddingInterlay,
    Ready,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum MigrationStatus {
    NotSetup,
    Pending,
    Testing,
    Ready,
    Running,
    PartialFailure,
    Failed,
    Paused,
    Completed,
    RollingBack,
    RolledBack,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ApiCallStatus {
    Pending,
    InProgress,
    Success,
    Failed(String),
    Skipped,
}

#[derive(Debug, Clone)]
pub struct ApiCall {
    pub name: String,
    pub status: ApiCallStatus,
}

impl ApiCall {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            status: ApiCallStatus::Pending,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MigrationState {
    pub setup_step: SetupStep,
    pub auth_token: Option<String>,
    pub org_id: String,
    pub api_base: String,
    pub source_endpoint_id: Option<String>,
    pub dest_endpoint_id: Option<String>,
    pub interlay_id: Option<String>,
    pub migration_id: Option<String>,
    pub status: MigrationStatus,
    pub last_error: Option<String>,
    pub api_calls: Vec<ApiCall>,
    /// Selected migration mode
    pub mode: MigrationMode,
    /// Canary-specific state (only relevant when mode is Canary)
    pub canary: CanaryState,
    /// Blue-green state: true if new (green) environment is active, false if old (blue) is active
    pub active_is_new: bool,
}

impl MigrationState {
    pub fn new(api_base: String) -> Self {
        Self {
            setup_step: SetupStep::NotStarted,
            auth_token: None,
            org_id: "adam-demo".to_string(),
            api_base,
            source_endpoint_id: None,
            dest_endpoint_id: None,
            interlay_id: None,
            migration_id: None,
            status: MigrationStatus::NotSetup,
            last_error: None,
            api_calls: vec![
                ApiCall::new("Create Organization"),
                ApiCall::new("Login"),
                ApiCall::new("Create Source Endpoint"),
                ApiCall::new("Create Dest Endpoint"),
                ApiCall::new("Create Interlay"),
                ApiCall::new("Create Migration"),
                ApiCall::new("Add Interlay to Migration"),
            ],
            mode: MigrationMode::default(),
            canary: CanaryState::default(),
            active_is_new: false,
        }
    }

    pub fn update_api_call(&mut self, index: usize, status: ApiCallStatus) {
        if index < self.api_calls.len() {
            self.api_calls[index].status = status;
        }
    }

    pub fn is_ready(&self) -> bool {
        self.setup_step == SetupStep::Ready
    }

    pub fn can_migrate(&self) -> bool {
        self.is_ready()
            && matches!(
                self.status,
                MigrationStatus::Pending | MigrationStatus::Testing | MigrationStatus::Ready
            )
    }

    pub fn can_update_traffic(&self) -> bool {
        self.is_ready()
            && self.mode == MigrationMode::Canary
            && self.status == MigrationStatus::Running
    }

    pub fn can_toggle_environment(&self) -> bool {
        self.is_ready()
            && self.mode == MigrationMode::BlueGreen
            && self.status == MigrationStatus::Running
    }

    pub fn can_complete(&self) -> bool {
        self.is_ready() && self.status == MigrationStatus::Running
    }

    pub fn can_rollback(&self) -> bool {
        self.is_ready()
            && self.interlay_id.is_some()
            && matches!(
                self.status,
                MigrationStatus::Completed
                    | MigrationStatus::Failed
                    | MigrationStatus::PartialFailure
                    | MigrationStatus::Paused
            )
    }

    pub fn can_pause(&self) -> bool {
        self.is_ready() && self.status == MigrationStatus::Running
    }

    pub fn can_resume(&self) -> bool {
        self.is_ready() && self.status == MigrationStatus::Paused
    }
}

pub fn parse_migration_status(status: Option<&str>) -> MigrationStatus {
    match status {
        Some("Pending") | None => MigrationStatus::Pending,
        Some("Testing") => MigrationStatus::Testing,
        Some("Ready") => MigrationStatus::Ready,
        Some("Running") => MigrationStatus::Running,
        Some("PartialFailure") => MigrationStatus::PartialFailure,
        Some("Failed") => MigrationStatus::Failed,
        Some("Paused") => MigrationStatus::Paused,
        Some("Completed") => MigrationStatus::Completed,
        Some("RollingBack") => MigrationStatus::RollingBack,
        Some("RolledBack") => MigrationStatus::RolledBack,
        Some(_) => MigrationStatus::Pending,
    }
}

// ============================================
// Tests
// ============================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_state(
        setup_step: SetupStep,
        status: MigrationStatus,
        interlay_id: Option<&str>,
        mode: MigrationMode,
    ) -> MigrationState {
        MigrationState {
            setup_step,
            auth_token: Some("test_token".to_string()),
            org_id: "adam-demo".to_string(),
            api_base: "http://localhost:8000".to_string(),
            source_endpoint_id: Some("src_123".to_string()),
            dest_endpoint_id: Some("dst_456".to_string()),
            interlay_id: interlay_id.map(|s| s.to_string()),
            migration_id: Some("mig_789".to_string()),
            status,
            last_error: None,
            api_calls: vec![],
            mode,
            canary: CanaryState::default(),
            active_is_new: false,
        }
    }

    // parse_migration_status tests

    #[test]
    fn test_parse_migration_status_pending() {
        assert_eq!(
            parse_migration_status(Some("Pending")),
            MigrationStatus::Pending
        );
    }

    #[test]
    fn test_parse_migration_status_none_defaults_to_pending() {
        assert_eq!(parse_migration_status(None), MigrationStatus::Pending);
    }

    #[test]
    fn test_parse_migration_status_testing() {
        assert_eq!(
            parse_migration_status(Some("Testing")),
            MigrationStatus::Testing
        );
    }

    #[test]
    fn test_parse_migration_status_ready() {
        assert_eq!(
            parse_migration_status(Some("Ready")),
            MigrationStatus::Ready
        );
    }

    #[test]
    fn test_parse_migration_status_running() {
        assert_eq!(
            parse_migration_status(Some("Running")),
            MigrationStatus::Running
        );
    }

    #[test]
    fn test_parse_migration_status_partial_failure() {
        assert_eq!(
            parse_migration_status(Some("PartialFailure")),
            MigrationStatus::PartialFailure
        );
    }

    #[test]
    fn test_parse_migration_status_failed() {
        assert_eq!(
            parse_migration_status(Some("Failed")),
            MigrationStatus::Failed
        );
    }

    #[test]
    fn test_parse_migration_status_paused() {
        assert_eq!(
            parse_migration_status(Some("Paused")),
            MigrationStatus::Paused
        );
    }

    #[test]
    fn test_parse_migration_status_completed() {
        assert_eq!(
            parse_migration_status(Some("Completed")),
            MigrationStatus::Completed
        );
    }

    #[test]
    fn test_parse_migration_status_rolling_back() {
        assert_eq!(
            parse_migration_status(Some("RollingBack")),
            MigrationStatus::RollingBack
        );
    }

    #[test]
    fn test_parse_migration_status_rolled_back() {
        assert_eq!(
            parse_migration_status(Some("RolledBack")),
            MigrationStatus::RolledBack
        );
    }

    #[test]
    fn test_parse_migration_status_unknown_defaults_to_pending() {
        assert_eq!(
            parse_migration_status(Some("UnknownStatus")),
            MigrationStatus::Pending
        );
    }

    // is_ready tests

    #[test]
    fn test_is_ready_when_setup_ready() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Pending,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.is_ready());
    }

    #[test]
    fn test_is_not_ready_when_not_started() {
        let state = create_test_state(
            SetupStep::NotStarted,
            MigrationStatus::NotSetup,
            None,
            MigrationMode::BigBang,
        );
        assert!(!state.is_ready());
    }

    #[test]
    fn test_is_not_ready_when_creating_organization() {
        let state = create_test_state(
            SetupStep::CreatingOrganization,
            MigrationStatus::NotSetup,
            None,
            MigrationMode::BigBang,
        );
        assert!(!state.is_ready());
    }

    #[test]
    fn test_is_not_ready_when_failed() {
        let state = create_test_state(
            SetupStep::Failed("error".to_string()),
            MigrationStatus::NotSetup,
            None,
            MigrationMode::BigBang,
        );
        assert!(!state.is_ready());
    }

    // can_migrate tests

    #[test]
    fn test_can_migrate_when_ready_and_pending() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Pending,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_migrate());
    }

    #[test]
    fn test_can_migrate_when_ready_and_testing() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Testing,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_migrate());
    }

    #[test]
    fn test_can_migrate_when_ready_and_status_ready() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Ready,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_migrate());
    }

    #[test]
    fn test_cannot_migrate_when_running() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Running,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_migrate());
    }

    #[test]
    fn test_cannot_migrate_when_not_ready() {
        let state = create_test_state(
            SetupStep::NotStarted,
            MigrationStatus::Pending,
            None,
            MigrationMode::BigBang,
        );
        assert!(!state.can_migrate());
    }

    // can_complete tests

    #[test]
    fn test_can_complete_when_ready_and_running() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Running,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_complete());
    }

    #[test]
    fn test_cannot_complete_when_pending() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Pending,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_complete());
    }

    #[test]
    fn test_cannot_complete_when_not_ready() {
        let state = create_test_state(
            SetupStep::NotStarted,
            MigrationStatus::Running,
            None,
            MigrationMode::BigBang,
        );
        assert!(!state.can_complete());
    }

    #[test]
    fn test_cannot_complete_when_already_completed() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Completed,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_complete());
    }

    // can_rollback tests

    #[test]
    fn test_can_rollback_when_completed() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Completed,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_rollback());
    }

    #[test]
    fn test_can_rollback_when_failed() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Failed,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_rollback());
    }

    #[test]
    fn test_can_rollback_when_partial_failure() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::PartialFailure,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_rollback());
    }

    #[test]
    fn test_cannot_rollback_when_pending() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Pending,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_rollback());
    }

    #[test]
    fn test_cannot_rollback_when_testing() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Testing,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_rollback());
    }

    #[test]
    fn test_cannot_rollback_when_not_ready() {
        let state = create_test_state(
            SetupStep::NotStarted,
            MigrationStatus::Running,
            None,
            MigrationMode::BigBang,
        );
        assert!(!state.can_rollback());
    }

    #[test]
    fn test_cannot_rollback_without_interlay_id() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Running,
            None,
            MigrationMode::BigBang,
        );
        assert!(!state.can_rollback());
    }

    #[test]
    fn test_cannot_rollback_when_rolling_back() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::RollingBack,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_rollback());
    }

    #[test]
    fn test_cannot_rollback_when_rolled_back() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::RolledBack,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_rollback());
    }

    // can_update_traffic tests

    #[test]
    fn test_can_update_traffic_in_canary_mode_when_running() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Running,
            Some("interlay_1"),
            MigrationMode::Canary,
        );
        assert!(state.can_update_traffic());
    }

    #[test]
    fn test_cannot_update_traffic_in_bigbang_mode() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Running,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_update_traffic());
    }

    #[test]
    fn test_cannot_update_traffic_when_not_running() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Pending,
            Some("interlay_1"),
            MigrationMode::Canary,
        );
        assert!(!state.can_update_traffic());
    }

    #[test]
    fn test_cannot_update_traffic_when_not_ready() {
        let state = create_test_state(
            SetupStep::NotStarted,
            MigrationStatus::Running,
            None,
            MigrationMode::Canary,
        );
        assert!(!state.can_update_traffic());
    }

    // MigrationMode toggle tests

    #[test]
    fn test_toggle_bigbang_to_canary() {
        assert_eq!(MigrationMode::BigBang.toggle(), MigrationMode::Canary);
    }

    #[test]
    fn test_toggle_canary_to_bluegreen() {
        assert_eq!(MigrationMode::Canary.toggle(), MigrationMode::BlueGreen);
    }

    #[test]
    fn test_toggle_bluegreen_to_bigbang() {
        assert_eq!(MigrationMode::BlueGreen.toggle(), MigrationMode::BigBang);
    }

    // MigrationMode name tests

    #[test]
    fn test_bigbang_name() {
        assert_eq!(MigrationMode::BigBang.name(), "BigBang");
    }

    #[test]
    fn test_canary_name() {
        assert_eq!(MigrationMode::Canary.name(), "Canary");
    }

    #[test]
    fn test_bluegreen_name() {
        assert_eq!(MigrationMode::BlueGreen.name(), "BlueGreen");
    }

    // ApiCall tests

    #[test]
    fn test_api_call_new() {
        let call = ApiCall::new("Test API Call");
        assert_eq!(call.name, "Test API Call");
        assert_eq!(call.status, ApiCallStatus::Pending);
    }

    // MigrationState update_api_call tests

    #[test]
    fn test_update_api_call_valid_index() {
        let mut state = MigrationState::new("http://localhost:8000".to_string());
        state.update_api_call(0, ApiCallStatus::InProgress);
        assert_eq!(state.api_calls[0].status, ApiCallStatus::InProgress);
    }

    #[test]
    fn test_update_api_call_success() {
        let mut state = MigrationState::new("http://localhost:8000".to_string());
        state.update_api_call(1, ApiCallStatus::Success);
        assert_eq!(state.api_calls[1].status, ApiCallStatus::Success);
    }

    #[test]
    fn test_update_api_call_failed() {
        let mut state = MigrationState::new("http://localhost:8000".to_string());
        state.update_api_call(2, ApiCallStatus::Failed("error message".to_string()));
        assert_eq!(
            state.api_calls[2].status,
            ApiCallStatus::Failed("error message".to_string())
        );
    }

    #[test]
    fn test_update_api_call_invalid_index_does_nothing() {
        let mut state = MigrationState::new("http://localhost:8000".to_string());
        let original_len = state.api_calls.len();
        state.update_api_call(100, ApiCallStatus::Success);
        assert_eq!(state.api_calls.len(), original_len);
    }

    // CanaryState tests

    #[test]
    fn test_canary_state_default() {
        let canary = CanaryState::default();
        assert_eq!(canary.read_percentage, 0.05);
        assert_eq!(canary.write_policy, "OldAuthoritative");
    }

    // can_pause / can_resume tests

    #[test]
    fn test_can_pause_when_running() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Running,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_pause());
    }

    #[test]
    fn test_cannot_pause_when_paused() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Paused,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_pause());
    }

    #[test]
    fn test_can_resume_when_paused() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Paused,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_resume());
    }

    #[test]
    fn test_cannot_resume_when_running() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Running,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_resume());
    }

    #[test]
    fn test_can_rollback_when_paused() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Paused,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(state.can_rollback());
    }

    // can_toggle_environment tests

    #[test]
    fn test_can_toggle_environment_bluegreen_running() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Running,
            Some("interlay_1"),
            MigrationMode::BlueGreen,
        );
        assert!(state.can_toggle_environment());
    }

    #[test]
    fn test_cannot_toggle_environment_bigbang() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Running,
            Some("interlay_1"),
            MigrationMode::BigBang,
        );
        assert!(!state.can_toggle_environment());
    }

    #[test]
    fn test_cannot_toggle_environment_not_running() {
        let state = create_test_state(
            SetupStep::Ready,
            MigrationStatus::Pending,
            Some("interlay_1"),
            MigrationMode::BlueGreen,
        );
        assert!(!state.can_toggle_environment());
    }
}
