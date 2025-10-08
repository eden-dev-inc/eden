// Data Generation and Fake Data
//
// This module generates realistic test data for organizations, users, and events.
// It uses the `fake` crate to create believable names, emails, and other data
// while maintaining realistic distributions and patterns.

use std::collections::{HashMap, HashSet};
use chrono::{DateTime, Utc};
use fake::{
    faker::{
        company::en::CompanyName,
        internet::en::{UserAgent},
        name::en::Name,
    },
    Fake,
};
use rand::{rngs::StdRng, SeedableRng, Rng};
use serde_json::json;
use uuid::Uuid;

use crate::models::{Event, EventType, Organization, User};

/// DataGenerator creates realistic fake data for the analytics simulation
/// Uses weighted distributions to simulate real user behavior patterns
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
            let mut user = loop {
                let full_name: String = Name().fake();
                let mut rng = StdRng::from_entropy();

                // Create guaranteed unique email using UUID and counter
                let base_name = full_name
                    .split_whitespace()
                    .map(|s| s.chars().filter(|c| c.is_alphabetic()).take(3).collect::<String>())
                    .collect::<Vec<_>>()
                    .join("")
                    .to_lowercase();

                // Guarantee uniqueness with UUID fragment and counter
                let unique_id = Uuid::new_v4().to_string().chars().take(8).collect::<String>();
                let email = format!("{}{}{}@example.com", base_name, unique_id, i);

                // Double-check uniqueness (shouldn't be needed, but safety first)
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
    /// Event types follow realistic analytics patterns:
    /// - 60% page views (most common user activity)
    /// - 25% clicks (user interactions)
    /// - 10% conversions (goal completions)
    /// - 3% signups (new user registrations)
    /// - 2% purchases (revenue events)
    pub fn generate_event(&self, organization_id: Uuid, user_ids: &[Uuid]) -> Event {
        let mut rng = StdRng::from_entropy();

        let event_type = self.random_event_type();

        // 80% of events have an associated user ID (logged in users)
        // 20% are anonymous events
        let user_id = if !user_ids.is_empty() && rng.gen_bool(0.8) {
            Some(user_ids[rng.gen_range(0..user_ids.len())])
        } else {
            None
        };

        // Generate event-specific data and properties
        let (page_url, properties) = match event_type {
            EventType::PageView => {
                let pages = [
                    "/dashboard", "/analytics", "/reports", "/settings",
                    "/users", "/billing", "/integrations", "/help"
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
                    "amount": rng.gen_range(2900..19900), // cents
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

    /// Select a random event type using realistic distribution weights
    fn random_event_type(&self) -> EventType {
        let mut rng = StdRng::from_entropy();
        let rand: f64 = rng.gen();

        // Cumulative distribution matching real analytics platforms
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

    /// Generate realistic traffic referrer sources
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

    /// Generate realistic IPv4 addresses for geolocation simulation
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

    /// Generate consistent cache keys for Redis operations
    /// Format: prefix:organization_id:suffix
    pub fn generate_cache_key(&self, prefix: &str, org_id: Uuid, suffix: Option<&str>) -> String {
        match suffix {
            Some(s) => format!("{}:{}:{}", prefix, org_id, s),
            None => format!("{}:{}", prefix, org_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;
    use super::*;

    #[test]
    fn generate_users() {
        let generator = DataGenerator::new();
        let users = generator.generate_users(Uuid::new_v4(), 100);

        println!("{:#?}", users);
    }
}