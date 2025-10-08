// Data Models and Types
//
// This module defines all the data structures used throughout the application.
// These models represent a realistic analytics platform with multi-tenant
// organizations, users, events, and aggregated analytics data.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// Organization represents a tenant company in the analytics platform
/// Each organization has its own isolated data and analytics
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Organization {
    pub id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

/// User represents an individual within an organization
/// Users generate events that drive the analytics
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, FromRow)]
pub struct User {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub email: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

/// EventType enum represents different types of user activities
/// Distribution: 60% PageView, 28% Click, 10% Conversion, 1.5% SignUp, 0.5% Purchase
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventType {
    /// User navigating to a page or screen
    PageView,
    /// User clicking buttons, links, or UI elements
    Click,
    /// User completing a desired action (goal achievement)
    Conversion,
    /// New user registration or account creation
    SignUp,
    /// User making a purchase or payment
    Purchase,
}

impl EventType {
    /// Convert enum to database string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::PageView => "page_view",
            EventType::Click => "click",
            EventType::Conversion => "conversion",
            EventType::SignUp => "sign_up",
            EventType::Purchase => "purchase",
        }
    }

    /// Parse database string back to enum
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
/// Contains contextual information like page URLs, referrers, and custom properties
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Event {
    pub id: Uuid,
    pub organization_id: Uuid,
    /// Optional user ID (anonymous events have None)
    pub user_id: Option<Uuid>,
    pub event_type: String,
    /// Page or screen where event occurred
    pub page_url: Option<String>,
    /// Traffic source or previous page
    pub referrer: Option<String>,
    /// Browser user agent string
    pub user_agent: Option<String>,
    /// Client IP address for geolocation
    pub ip_address: Option<String>,
    /// Event-specific metadata as JSON
    pub properties: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// AnalyticsOverview provides high-level metrics for dashboard display
/// This is frequently cached in Redis due to expensive aggregation queries
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
/// Used for content performance analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopPage {
    pub url: String,
    pub views: i64,
    pub unique_visitors: i64,
}

/// FunnelStep represents a step in a conversion funnel analysis
/// Shows user drop-off rates through multi-step processes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunnelStep {
    pub step_name: String,
    pub users: i64,
    pub conversion_rate: f64,
}