//! API response types for Eden API communication.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct LoginResponse {
    pub token: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EndpointResponseData {
    pub id: String,
    pub uuid: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InterlayResponseData {
    pub id: String,
    #[allow(dead_code)]
    pub uuid: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MigrationResponseData {
    pub id: String,
    #[allow(dead_code)]
    pub uuid: String,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpdateTrafficResponse {
    #[allow(dead_code)]
    pub migration_id: String,
    pub old_percentage: f64,
    pub new_percentage: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompleteMigrationResponse {
    #[allow(dead_code)]
    pub migration_id: String,
    #[allow(dead_code)]
    pub status: String,
    #[allow(dead_code)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RollbackInterlayResponse {
    #[allow(dead_code)]
    pub migration_id: String,
    pub interlay_id: String,
    pub status: String,
    #[allow(dead_code)]
    pub rolled_back_at: String,
    #[allow(dead_code)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PauseMigrationResponse {
    #[allow(dead_code)]
    pub migration_id: String,
    #[allow(dead_code)]
    pub status: String,
    #[allow(dead_code)]
    pub paused_at: String,
    #[allow(dead_code)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResumeMigrationResponse {
    #[allow(dead_code)]
    pub migration_id: String,
    #[allow(dead_code)]
    pub status: String,
    #[allow(dead_code)]
    pub resumed_at: String,
    #[allow(dead_code)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ToggleEnvironmentResponse {
    #[allow(dead_code)]
    pub migration_id: String,
    #[allow(dead_code)]
    pub previous_active: String,
    #[allow(dead_code)]
    pub new_active: String,
    #[allow(dead_code)]
    pub write_mode: String,
    #[allow(dead_code)]
    pub updated_at: String,
    #[allow(dead_code)]
    pub updated_by: String,
}
