// Data Models and Types
//
// Enhanced models with granular structures for time-series, user-level,
// and page-level analytics to support 10K+ cache keys.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// Organization represents a tenant company in the analytics platform
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Organization {
    pub id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

/// User represents an individual within an organization
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, FromRow)]
pub struct User {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub email: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

/// EventType enum represents different types of user activities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventType {
    PageView,
    Click,
    Conversion,
    SignUp,
    Purchase,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::PageView => "page_view",
            EventType::Click => "click",
            EventType::Conversion => "conversion",
            EventType::SignUp => "sign_up",
            EventType::Purchase => "purchase",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "page_view" => Some(EventType::PageView),
            "click" => Some(EventType::Click),
            "conversion" => Some(EventType::Conversion),
            "sign_up" => Some(EventType::SignUp),
            "purchase" => Some(EventType::Purchase),
            _ => None,
        }
    }
}

/// Event represents a single user activity record
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Event {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub user_id: Option<Uuid>,
    pub event_type: String,
    pub page_url: Option<String>,
    pub referrer: Option<String>,
    pub user_agent: Option<String>,
    pub ip_address: Option<String>,
    pub properties: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// AnalyticsOverview provides high-level metrics for dashboard display
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsOverview {
    pub organization_id: Uuid,
    pub total_events: i64,
    pub unique_users: i64,
    pub page_views: i64,
    pub conversions: i64,
    pub conversion_rate: f64,
    pub time_period: String,
}

/// TopPage represents popular pages or screens by traffic volume
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopPage {
    pub url: String,
    pub views: i64,
    pub unique_visitors: i64,
}

/// FunnelStep represents a step in a conversion funnel analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunnelStep {
    pub step_name: String,
    pub users: i64,
    pub conversion_rate: f64,
}

/// HourlyMetrics tracks time-series analytics per hour
/// Used for granular time-based caching (creates many cache keys)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlyMetrics {
    pub organization_id: Uuid,
    pub hour: DateTime<Utc>,
    pub events: i64,
    pub unique_users: i64,
    pub page_views: i64,
    pub clicks: i64,
    pub conversions: i64,
    pub signups: i64,
    pub purchases: i64,
    pub revenue: f64,
}

/// DailyMetrics aggregates metrics per day
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyMetrics {
    pub organization_id: Uuid,
    pub date: DateTime<Utc>,
    pub events: i64,
    pub unique_users: i64,
    pub page_views: i64,
    pub conversions: i64,
    pub conversion_rate: f64,
    pub revenue: f64,
}

/// UserActivity summarizes a specific user's behavior
/// Enables per-user caching for user profile queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserActivity {
    pub user_id: Uuid,
    pub organization_id: Uuid,
    pub total_events: i64,
    pub last_seen: DateTime<Utc>,
    pub page_views: i64,
    pub clicks: i64,
    pub conversions: i64,
    pub lifetime_value: f64,
}

/// PagePerformance tracks individual page/URL metrics
/// Creates cache entries per page per organization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PagePerformance {
    pub organization_id: Uuid,
    pub page_url: String,
    pub views: i64,
    pub unique_visitors: i64,
    pub avg_time_on_page: f64,
    pub bounce_rate: f64,
    pub conversions: i64,
}

/// EventTypeDistribution shows breakdown of event types
/// Cached per org for quick dashboard loading
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTypeDistribution {
    pub organization_id: Uuid,
    pub page_views: i64,
    pub clicks: i64,
    pub conversions: i64,
    pub signups: i64,
    pub purchases: i64,
    pub total: i64,
}

/// ReferrerStats tracks traffic source performance
/// Cached per org and per time period
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferrerStats {
    pub referrer: String,
    pub visits: i64,
    pub unique_visitors: i64,
    pub conversions: i64,
    pub conversion_rate: f64,
}

/// CohortAnalysis tracks user behavior by signup cohort
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CohortAnalysis {
    pub organization_id: Uuid,
    pub cohort_period: String,
    pub users: i64,
    pub retention_rate: f64,
    pub avg_events_per_user: f64,
}

/// RealtimeCounter for live dashboard updates
/// Incremented atomically in Redis using INCR
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealtimeCounter {
    pub organization_id: Uuid,
    pub current_active_users: i64,
    pub events_last_minute: i64,
    pub events_last_hour: i64,
}

/// DeviceBrowserStats tracks user agent analytics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceBrowserStats {
    pub organization_id: Uuid,
    pub device_type: String,
    pub browser: String,
    pub count: i64,
    pub percentage: f64,
}

/// GeographicDistribution tracks user location analytics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeographicDistribution {
    pub organization_id: Uuid,
    pub country_code: String,
    pub city: String,
    pub users: i64,
    pub events: i64,
}