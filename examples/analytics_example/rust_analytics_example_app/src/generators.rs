// Data Generation and Fake Data
//
// Enhanced generator with methods for creating diverse cache keys
// and realistic test data across time-series, user, and page dimensions.

use std::collections::HashSet;
use chrono::{DateTime, Duration, Utc};
use fake::{
    faker::{
        company::en::CompanyName,
        internet::en::UserAgent,
        name::en::Name,
    },
    Fake,
};
use rand::{rngs::StdRng, SeedableRng, Rng};
use serde_json::json;
use uuid::Uuid;

use crate::models::{Event, EventType, Organization, User};

/// DataGenerator creates realistic fake data and cache keys
pub struct DataGenerator;

impl DataGenerator {
    pub fn new() -> Self {
        Self
    }

    /// Generate a fake organization with realistic company name
    pub fn generate_organization(&self) -> Organization {
        Organization {
            id: Uuid::new_v4(),
            name: CompanyName().fake(),
            created_at: Utc::now(),
        }
    }

    /// Generate a collection of fake users belonging to the specified organization
    pub fn generate_users(&self, organization_id: Uuid, count: usize) -> Vec<User> {
        let mut users = Vec::with_capacity(count);
        let mut used_emails = HashSet::new();

        for i in 0..count {
            let user = loop {
                let full_name: String = Name().fake();

                let base_name = full_name
                    .split_whitespace()
                    .map(|s| s.chars().filter(|c| c.is_alphabetic()).take(3).collect::<String>())
                    .collect::<Vec<_>>()
                    .join("")
                    .to_lowercase();

                let unique_id = Uuid::new_v4().to_string().chars().take(8).collect::<String>();
                let email = format!("{}{}{}@example.com", base_name, unique_id, i);

                if !used_emails.contains(&email) {
                    used_emails.insert(email.clone());
                    break User {
                        id: Uuid::new_v4(),
                        organization_id,
                        email,
                        name: full_name,
                        created_at: Utc::now(),
                    };
                }
            };

            users.push(user);
        }

        users
    }

    /// Generate a realistic event with proper type distribution and metadata
    pub fn generate_event(&self, organization_id: Uuid, user_ids: &[Uuid]) -> Event {
        let mut rng = StdRng::from_entropy();

        let event_type = self.random_event_type();

        let user_id = if !user_ids.is_empty() && rng.gen_bool(0.8) {
            Some(user_ids[rng.gen_range(0..user_ids.len())])
        } else {
            None
        };

        let (page_url, properties) = match event_type {
            EventType::PageView => {
                let pages = [
                    "/dashboard", "/analytics", "/reports", "/settings",
                    "/users", "/billing", "/integrations", "/help",
                    "/docs", "/profile", "/team", "/api"
                ];
                let page = pages[rng.gen_range(0..pages.len())];
                (
                    Some(format!("https://app.example.com{}", page)),
                    json!({
                        "page_title": format!("{} - Analytics Platform", page.trim_start_matches('/')),
                        "load_time_ms": rng.gen_range(100..2000)
                    })
                )
            }
            EventType::Click => (
                Some("https://app.example.com/dashboard".to_string()),
                json!({
                    "element": "button",
                    "text": "Export Data",
                    "position_x": rng.gen_range(0..1920),
                    "position_y": rng.gen_range(0..1080)
                })
            ),
            EventType::Conversion => (
                Some("https://app.example.com/billing".to_string()),
                json!({
                    "plan": if rng.gen_bool(0.3) { "premium" } else { "basic" },
                    "amount": rng.gen_range(2900..19900),
                    "currency": "USD"
                })
            ),
            EventType::SignUp => (
                Some("https://app.example.com/signup".to_string()),
                json!({
                    "referrer_source": self.random_referrer(),
                    "trial_days": 14
                })
            ),
            EventType::Purchase => (
                Some("https://app.example.com/billing".to_string()),
                json!({
                    "item_count": rng.gen_range(1..5),
                    "total_amount": rng.gen_range(1000..50000),
                    "currency": "USD"
                })
            ),
        };

        Event {
            id: Uuid::new_v4(),
            organization_id,
            user_id,
            event_type: event_type.as_str().to_string(),
            page_url,
            referrer: Some(self.random_referrer()),
            user_agent: Some(UserAgent().fake()),
            ip_address: Some(self.random_ip()),
            properties,
            created_at: Utc::now(),
        }
    }

    fn random_event_type(&self) -> EventType {
        let mut rng = StdRng::from_entropy();
        let rand: f64 = rng.gen();

        if rand < 0.60 {
            EventType::PageView
        } else if rand < 0.88 {
            EventType::Click
        } else if rand < 0.98 {
            EventType::Conversion
        } else if rand < 0.995 {
            EventType::SignUp
        } else {
            EventType::Purchase
        }
    }

    fn random_referrer(&self) -> String {
        let mut rng = StdRng::from_entropy();
        let referrers = [
            "https://google.com/search",
            "https://twitter.com",
            "https://linkedin.com",
            "https://facebook.com",
            "https://github.com",
            "direct",
            "email_campaign",
            "organic_search"
        ];
        referrers[rng.gen_range(0..referrers.len())].to_string()
    }

    fn random_ip(&self) -> String {
        let mut rng = StdRng::from_entropy();
        format!(
            "{}.{}.{}.{}",
            rng.gen_range(1..255),
            rng.gen_range(0..255),
            rng.gen_range(0..255),
            rng.gen_range(1..255)
        )
    }

    // ========== ENHANCED CACHE KEY GENERATORS ==========

    /// Generate overview cache key
    pub fn cache_key_overview(&self, org_id: Uuid, hours: u32) -> String {
        format!("analytics:{}:overview:{}h", org_id, hours)
    }

    /// Generate hourly metrics cache key
    pub fn cache_key_hourly(&self, org_id: Uuid, hour: DateTime<Utc>) -> String {
        format!("analytics:{}:hourly:{}", org_id, hour.format("%Y%m%d%H"))
    }

    /// Generate daily metrics cache key
    pub fn cache_key_daily(&self, org_id: Uuid, date: DateTime<Utc>) -> String {
        format!("analytics:{}:daily:{}", org_id, date.format("%Y%m%d"))
    }

    /// Generate user activity cache key
    pub fn cache_key_user_activity(&self, user_id: Uuid) -> String {
        format!("analytics:user:{}:activity", user_id)
    }

    /// Generate page performance cache key
    pub fn cache_key_page(&self, org_id: Uuid, page_url: &str) -> String {
        let sanitized = page_url.replace(['/', ':', '?', '&'], "_");
        format!("analytics:{}:page:{}", org_id, sanitized)
    }

    /// Generate top pages cache key
    pub fn cache_key_top_pages(&self, org_id: Uuid, hours: u32) -> String {
        format!("analytics:{}:top_pages:{}h", org_id, hours)
    }

    /// Generate event type distribution cache key
    pub fn cache_key_event_distribution(&self, org_id: Uuid, period: &str) -> String {
        format!("analytics:{}:events:dist:{}", org_id, period)
    }

    /// Generate referrer stats cache key
    pub fn cache_key_referrers(&self, org_id: Uuid, period: &str) -> String {
        format!("analytics:{}:referrers:{}", org_id, period)
    }

    /// Generate cohort analysis cache key
    pub fn cache_key_cohort(&self, org_id: Uuid, cohort_period: &str) -> String {
        format!("analytics:{}:cohort:{}", org_id, cohort_period)
    }

    /// Generate realtime counter cache key
    pub fn cache_key_realtime(&self, org_id: Uuid) -> String {
        format!("analytics:{}:realtime", org_id)
    }

    /// Generate realtime events counter (for INCR operations)
    pub fn cache_key_realtime_counter(&self, org_id: Uuid, bucket: &str) -> String {
        format!("analytics:{}:realtime:{}:count", org_id, bucket)
    }

    /// Generate device/browser stats cache key
    pub fn cache_key_device_stats(&self, org_id: Uuid, period: &str) -> String {
        format!("analytics:{}:devices:{}", org_id, period)
    }

    /// Generate geographic distribution cache key
    pub fn cache_key_geo(&self, org_id: Uuid, period: &str) -> String {
        format!("analytics:{}:geo:{}", org_id, period)
    }

    /// Generate funnel analysis cache key
    pub fn cache_key_funnel(&self, org_id: Uuid, funnel_id: &str) -> String {
        format!("analytics:{}:funnel:{}", org_id, funnel_id)
    }

    /// Generate user list cache key (paginated)
    pub fn cache_key_user_list(&self, org_id: Uuid, page: u32) -> String {
        format!("analytics:{}:users:page:{}", org_id, page)
    }

    /// Generate session cache key for active sessions
    pub fn cache_key_session(&self, org_id: Uuid, session_id: &str) -> String {
        format!("analytics:{}:session:{}", org_id, session_id)
    }

    /// Generate aggregate counter key for fast increments
    pub fn cache_key_counter(&self, org_id: Uuid, metric: &str) -> String {
        format!("analytics:{}:counter:{}", org_id, metric)
    }

    /// Generate time-series bucket key
    pub fn cache_key_timeseries_bucket(&self, org_id: Uuid, metric: &str, timestamp: DateTime<Utc>) -> String {
        format!("analytics:{}:ts:{}:{}", org_id, metric, timestamp.format("%Y%m%d%H%M"))
    }

    /// Generate rolling window aggregation key (last N minutes)
    pub fn cache_key_rolling_window(&self, org_id: Uuid, metric: &str, minutes: u32) -> String {
        format!("analytics:{}:rolling:{}:{}m", org_id, metric, minutes)
    }

    /// Get list of all time buckets for the last N hours
    pub fn get_hourly_time_buckets(&self, hours: u32) -> Vec<DateTime<Utc>> {
        let now = Utc::now();
        (0..hours)
            .map(|i| now - Duration::hours(i as i64))
            .collect()
    }

    /// Get list of popular pages for prewarming
    pub fn get_popular_pages(&self) -> Vec<&'static str> {
        vec![
            "/dashboard",
            "/analytics",
            "/reports",
            "/settings",
            "/users",
            "/billing",
            "/integrations",
            "/help",
            "/docs",
            "/profile",
            "/team",
            "/api"
        ]
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;
    use super::*;

    #[test]
    fn test_cache_key_generation() {
        let generator = DataGenerator::new();
        let org_id = Uuid::new_v4();

        println!("Overview: {}", generator.cache_key_overview(org_id, 24));
        println!("Hourly: {}", generator.cache_key_hourly(org_id, Utc::now()));
        println!("User: {}", generator.cache_key_user_activity(Uuid::new_v4()));
        println!("Page: {}", generator.cache_key_page(org_id, "/dashboard"));
    }
}