#![cfg(external_db)]

mod common;
mod request;
mod util;

#[path = "api_admin_auth/analytics.rs"]
mod analytics;
#[path = "api_admin_auth/apis.rs"]
mod apis;
#[path = "api_admin_auth/auth_extended.rs"]
mod auth_extended;
#[path = "api_admin_auth/backups.rs"]
mod backups;
#[path = "api_admin_auth/rate_limiting.rs"]
mod rate_limiting;
#[path = "api_admin_auth/robots.rs"]
mod robots;
