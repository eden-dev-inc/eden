// Data Generation and Fake Data
//
// Enhanced generator with methods for creating diverse cache keys
// and realistic test data across time-series, user, and page dimensions.

use chrono::{DateTime, Duration, Utc};
use fake::{
    faker::{company::en::CompanyName, internet::en::UserAgent, name::en::Name},
    Fake,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde_json::json;
use std::collections::HashSet;
use uuid::Uuid;

use crate::models::{
    Campaign, Event, EventType, Experiment, Goal, Invoice, Order, Organization, PageViewRecord,
    Payment, Product, ProductCategory, Review, Session, Subscription, SubscriptionPlan, User,
};

/// DataGenerator creates realistic fake data and cache keys
pub struct DataGenerator;

impl Default for DataGenerator {
    fn default() -> Self {
        Self::new()
    }
}

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
                    .map(|s| {
                        s.chars()
                            .filter(|c| c.is_alphabetic())
                            .take(3)
                            .collect::<String>()
                    })
                    .collect::<Vec<_>>()
                    .join("")
                    .to_lowercase();

                let unique_id = Uuid::new_v4()
                    .to_string()
                    .chars()
                    .take(8)
                    .collect::<String>();
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
                    "/api",
                ];
                let page = pages[rng.gen_range(0..pages.len())];
                (
                    Some(format!("https://app.example.com{}", page)),
                    json!({
                        "page_title": format!("{} - Analytics Platform", page.trim_start_matches('/')),
                        "load_time_ms": rng.gen_range(100..2000)
                    }),
                )
            }
            EventType::Click => (
                Some("https://app.example.com/dashboard".to_string()),
                json!({
                    "element": "button",
                    "text": "Export Data",
                    "position_x": rng.gen_range(0..1920),
                    "position_y": rng.gen_range(0..1080)
                }),
            ),
            EventType::Conversion => (
                Some("https://app.example.com/billing".to_string()),
                json!({
                    "plan": if rng.gen_bool(0.3) { "premium" } else { "basic" },
                    "amount": rng.gen_range(2900..19900),
                    "currency": "USD"
                }),
            ),
            EventType::SignUp => (
                Some("https://app.example.com/signup".to_string()),
                json!({
                    "referrer_source": self.random_referrer(),
                    "trial_days": 14
                }),
            ),
            EventType::Purchase => (
                Some("https://app.example.com/billing".to_string()),
                json!({
                    "item_count": rng.gen_range(1..5),
                    "total_amount": rng.gen_range(1000..50000),
                    "currency": "USD"
                }),
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
            "organic_search",
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

    // ========== ANALYTICS DOMAIN GENERATORS ==========

    /// Generate a realistic browsing session with device, geo, and UTM data
    pub fn generate_session(&self, organization_id: Uuid, user_id: Option<Uuid>) -> Session {
        let mut rng = StdRng::from_entropy();

        let devices = ["desktop", "mobile", "tablet", "bot", "unknown"];
        let browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera", "Brave"];
        let operating_systems = [
            "Windows 11",
            "macOS 14",
            "Ubuntu 22.04",
            "iOS 17",
            "Android 14",
        ];
        let resolutions = [
            "1920x1080",
            "2560x1440",
            "1366x768",
            "1440x900",
            "390x844",
            "412x915",
        ];
        let countries = ["US", "GB", "DE", "FR", "CA", "AU", "JP", "BR", "IN", "NL"];
        let cities = [
            "New York",
            "London",
            "Berlin",
            "Paris",
            "Toronto",
            "Sydney",
            "Tokyo",
            "Sao Paulo",
            "Mumbai",
            "Amsterdam",
        ];
        let landing_pages = [
            "/",
            "/pricing",
            "/features",
            "/blog",
            "/docs",
            "/signup",
            "/demo",
            "/about",
            "/contact",
        ];
        let utm_sources = [
            "google",
            "facebook",
            "twitter",
            "linkedin",
            "newsletter",
            "partner",
        ];
        let utm_mediums = ["cpc", "organic", "social", "email", "referral"];
        let utm_campaigns = [
            "spring_sale",
            "product_launch",
            "brand_awareness",
            "retargeting",
            "onboarding_q1",
            "webinar_promo",
        ];

        let device = devices[rng.gen_range(0..devices.len())].to_string();
        let page_count: i32 = rng.gen_range(1..20);
        let duration_seconds = if page_count > 1 {
            Some(rng.gen_range(10..1800))
        } else {
            Some(rng.gen_range(0..15))
        };
        let is_bounce = page_count == 1 && duration_seconds.unwrap_or(0) < 10;

        let started_at = Utc::now() - Duration::seconds(rng.gen_range(0..86400));
        let ended_at = duration_seconds.map(|d| started_at + Duration::seconds(d as i64));

        let landing = landing_pages[rng.gen_range(0..landing_pages.len())];
        let exit = landing_pages[rng.gen_range(0..landing_pages.len())];

        let has_utm = rng.gen_bool(0.4);

        Session {
            id: Uuid::new_v4(),
            organization_id,
            user_id,
            session_token: Uuid::new_v4().to_string(),
            device,
            browser: Some(browsers[rng.gen_range(0..browsers.len())].to_string()),
            os: Some(operating_systems[rng.gen_range(0..operating_systems.len())].to_string()),
            screen_resolution: Some(resolutions[rng.gen_range(0..resolutions.len())].to_string()),
            country_code: Some(countries[rng.gen_range(0..countries.len())].to_string()),
            city: Some(cities[rng.gen_range(0..cities.len())].to_string()),
            ip_address: Some(self.random_ip()),
            landing_page: Some(format!("https://app.example.com{}", landing)),
            exit_page: Some(format!("https://app.example.com{}", exit)),
            page_count,
            duration_seconds,
            is_bounce,
            utm_source: if has_utm {
                Some(utm_sources[rng.gen_range(0..utm_sources.len())].to_string())
            } else {
                None
            },
            utm_medium: if has_utm {
                Some(utm_mediums[rng.gen_range(0..utm_mediums.len())].to_string())
            } else {
                None
            },
            utm_campaign: if has_utm {
                Some(utm_campaigns[rng.gen_range(0..utm_campaigns.len())].to_string())
            } else {
                None
            },
            utm_content: if has_utm && rng.gen_bool(0.3) {
                Some(format!("variant_{}", rng.gen_range(1..5)))
            } else {
                None
            },
            started_at,
            ended_at,
            created_at: started_at,
        }
    }

    /// Generate a marketing campaign with budget, channel, and performance metrics
    pub fn generate_campaign(&self, organization_id: Uuid) -> Campaign {
        let mut rng = StdRng::from_entropy();

        let campaign_names = [
            "Spring Product Launch",
            "Summer Sale Blast",
            "Holiday Promo",
            "Brand Awareness Q1",
            "Customer Re-engagement",
            "Webinar Promotion",
            "Partner Referral Drive",
            "Back to School",
            "Year-End Clearance",
            "New Feature Announcement",
        ];
        let statuses = ["draft", "active", "paused", "completed", "archived"];
        let channels = [
            "email",
            "social",
            "search",
            "display",
            "affiliate",
            "content",
        ];
        let tag_pool = [
            "seasonal",
            "high-priority",
            "evergreen",
            "retargeting",
            "b2b",
            "b2c",
            "acquisition",
            "retention",
        ];

        let budget_cents: i64 = rng.gen_range(500_000..50_000_000);
        let spent_ratio: f64 = rng.gen_range(0.0..1.0);
        let spent_cents = (budget_cents as f64 * spent_ratio) as i64;

        let impression_count: i32 = rng.gen_range(1000..500_000);
        let click_count: i32 = (impression_count as f64 * rng.gen_range(0.01..0.08)) as i32;
        let conversion_count: i32 = (click_count as f64 * rng.gen_range(0.02..0.15)) as i32;

        let num_tags = rng.gen_range(1..4);
        let mut tags = Vec::new();
        for _ in 0..num_tags {
            let tag = tag_pool[rng.gen_range(0..tag_pool.len())].to_string();
            if !tags.contains(&tag) {
                tags.push(tag);
            }
        }

        let starts_at = Utc::now() - Duration::days(rng.gen_range(1..90));
        let ends_at = starts_at + Duration::days(rng.gen_range(7..90));

        Campaign {
            id: Uuid::new_v4(),
            organization_id,
            name: campaign_names[rng.gen_range(0..campaign_names.len())].to_string(),
            status: statuses[rng.gen_range(0..statuses.len())].to_string(),
            channel: channels[rng.gen_range(0..channels.len())].to_string(),
            budget_cents,
            spent_cents,
            target_audience: json!({
                "age_range": [rng.gen_range(18..30), rng.gen_range(35..65)],
                "geos": ["US", "CA", "GB"],
                "interests": ["technology", "business", "marketing"],
                "lookalike_pct": rng.gen_range(1..10)
            }),
            tags,
            click_count,
            impression_count,
            conversion_count,
            starts_at: Some(starts_at),
            ends_at: Some(ends_at),
            created_at: starts_at - Duration::days(rng.gen_range(1..7)),
            updated_at: Utc::now(),
        }
    }

    /// Generate an A/B experiment with hypothesis, variants, and metrics
    pub fn generate_experiment(&self, organization_id: Uuid) -> Experiment {
        let mut rng = StdRng::from_entropy();

        let experiment_names = [
            "CTA Button Color Test",
            "Pricing Page Layout",
            "Checkout Flow Simplification",
            "Onboarding Wizard Steps",
            "Email Subject Line Test",
            "Hero Image Variation",
            "Navigation Menu Redesign",
            "Social Proof Badge Placement",
        ];
        let statuses = ["draft", "running", "paused", "concluded"];
        let metrics = [
            "conversion_rate",
            "click_through_rate",
            "revenue_per_visitor",
            "time_on_page",
            "bounce_rate",
            "signup_rate",
        ];
        let hypotheses = [
            "Changing the CTA button color to green will increase conversions by 15%",
            "Simplifying the checkout flow will reduce cart abandonment by 20%",
            "Adding social proof badges will increase trust and conversion rate",
            "A shorter onboarding wizard will improve activation rates",
            "Personalized subject lines will improve email open rates by 10%",
        ];

        let status = statuses[rng.gen_range(0..statuses.len())].to_string();
        let variant_names = ["control", "variant_a", "variant_b", "variant_c"];
        let num_variants = rng.gen_range(2..=4);
        let variants: Vec<serde_json::Value> = variant_names[..num_variants]
            .iter()
            .map(|name| {
                json!({
                    "name": name,
                    "weight": 1.0 / num_variants as f64,
                    "description": format!("{} variant", name),
                })
            })
            .collect();

        let started_at = if status != "draft" {
            Some(Utc::now() - Duration::days(rng.gen_range(1..30)))
        } else {
            None
        };
        let concluded_at = if status == "concluded" {
            Some(Utc::now() - Duration::days(rng.gen_range(0..5)))
        } else {
            None
        };

        let winning_variant = if status == "concluded" {
            Some(variant_names[rng.gen_range(0..num_variants)].to_string())
        } else {
            None
        };

        let results = if status == "concluded" {
            json!({
                "control": { "visitors": rng.gen_range(500..5000), "conversions": rng.gen_range(25..500), "rate": rng.gen_range(3.0..15.0_f64) },
                "variant_a": { "visitors": rng.gen_range(500..5000), "conversions": rng.gen_range(30..550), "rate": rng.gen_range(4.0..18.0_f64) },
                "p_value": rng.gen_range(0.001..0.1_f64),
                "significant": true
            })
        } else {
            json!({})
        };

        Experiment {
            id: Uuid::new_v4(),
            organization_id,
            name: experiment_names[rng.gen_range(0..experiment_names.len())].to_string(),
            description: Some("Testing impact on user engagement and conversion".to_string()),
            status,
            hypothesis: Some(hypotheses[rng.gen_range(0..hypotheses.len())].to_string()),
            variants: json!(variants),
            metric_name: metrics[rng.gen_range(0..metrics.len())].to_string(),
            baseline_rate: Some(rng.gen_range(2.0..20.0)),
            sample_size_target: Some(rng.gen_range(1000..50000)),
            confidence_level: if rng.gen_bool(0.8) { 0.95 } else { 0.99 },
            winning_variant,
            results,
            started_at,
            concluded_at,
            created_at: Utc::now() - Duration::days(rng.gen_range(5..60)),
            updated_at: Utc::now(),
        }
    }

    /// Generate a page view record with web vitals performance metrics
    pub fn generate_page_view_record(
        &self,
        organization_id: Uuid,
        session_id: Option<Uuid>,
        user_id: Option<Uuid>,
    ) -> PageViewRecord {
        let mut rng = StdRng::from_entropy();

        let pages = [
            ("/dashboard", "Dashboard - Analytics Platform"),
            ("/analytics", "Analytics - Overview"),
            ("/reports", "Reports - Monthly Summary"),
            ("/settings", "Account Settings"),
            ("/billing", "Billing & Subscription"),
            ("/docs", "Documentation"),
            ("/pricing", "Pricing Plans"),
            ("/blog/post-1", "10 Tips for Better Analytics"),
            ("/features", "Product Features"),
            ("/integrations", "Integrations Directory"),
        ];
        let referrers = [
            "https://google.com/search?q=analytics",
            "https://twitter.com/link",
            "https://linkedin.com/feed",
            "https://news.ycombinator.com",
            "direct",
        ];

        let (path, title) = pages[rng.gen_range(0..pages.len())];

        PageViewRecord {
            id: Uuid::new_v4(),
            organization_id,
            session_id,
            user_id,
            event_id: if rng.gen_bool(0.5) {
                Some(Uuid::new_v4())
            } else {
                None
            },
            page_url: format!("https://app.example.com{}", path),
            page_title: Some(title.to_string()),
            referrer_url: if rng.gen_bool(0.7) {
                Some(referrers[rng.gen_range(0..referrers.len())].to_string())
            } else {
                None
            },
            time_on_page_ms: Some(rng.gen_range(500..120_000)),
            dom_load_ms: Some(rng.gen_range(100..3000)),
            first_paint_ms: Some(rng.gen_range(50..1500)),
            first_contentful_paint_ms: Some(rng.gen_range(100..2500)),
            largest_contentful_paint_ms: Some(rng.gen_range(200..4000)),
            cumulative_layout_shift: Some(rng.gen_range(0.0..0.5_f64)),
            viewport_width: Some(rng.gen_range(320..2560)),
            viewport_height: Some(rng.gen_range(568..1440)),
            scroll_depth_pct: Some(rng.gen_range(0..100) as i16),
            created_at: Utc::now() - Duration::seconds(rng.gen_range(0..86400)),
        }
    }

    /// Generate a conversion goal with target value and tracking configuration
    pub fn generate_goal(&self, organization_id: Uuid) -> Goal {
        let mut rng = StdRng::from_entropy();

        let goal_configs = [
            ("Sign Up Completion", "event", "/signup/complete", 1.0),
            ("Purchase Made", "revenue", "/checkout/success", 50.0),
            ("Free Trial Started", "event", "/trial/start", 10.0),
            ("Page Visit: Pricing", "page_view", "/pricing", 0.5),
            ("Session Duration > 5min", "duration", "300", 2.0),
            ("View 5+ Pages", "pages_per_session", "5", 1.5),
            ("Demo Requested", "event", "/demo/request", 25.0),
            ("Newsletter Signup", "event", "/newsletter/subscribe", 3.0),
        ];

        let (name, goal_type, pattern, base_value) =
            goal_configs[rng.gen_range(0..goal_configs.len())];

        let completions_count: i32 = rng.gen_range(0..5000);
        let total_value = completions_count as f64 * base_value * rng.gen_range(0.8..1.2);

        Goal {
            id: Uuid::new_v4(),
            organization_id,
            name: name.to_string(),
            goal_type: goal_type.to_string(),
            target_value: Some(base_value * rng.gen_range(0.5..2.0)),
            match_pattern: Some(pattern.to_string()),
            is_active: rng.gen_bool(0.85),
            completions_count,
            total_value,
            created_at: Utc::now() - Duration::days(rng.gen_range(10..180)),
            updated_at: Utc::now() - Duration::days(rng.gen_range(0..10)),
        }
    }

    // ========== E-COMMERCE DOMAIN GENERATORS ==========

    /// Generate a product category with optional parent for hierarchical structure
    #[allow(dead_code)]
    pub fn generate_product_category(
        &self,
        organization_id: Uuid,
        parent_id: Option<Uuid>,
    ) -> ProductCategory {
        let mut rng = StdRng::from_entropy();

        let categories = [
            "Electronics",
            "Clothing",
            "Home & Garden",
            "Books",
            "Sports",
            "Toys & Games",
            "Health & Beauty",
            "Automotive",
            "Food & Drink",
            "Office Supplies",
            "Pet Supplies",
            "Software",
        ];
        let subcategories = [
            "Laptops",
            "Smartphones",
            "T-Shirts",
            "Dresses",
            "Furniture",
            "Cookware",
            "Fiction",
            "Non-Fiction",
            "Running",
            "Yoga",
            "Board Games",
            "Vitamins",
            "Tires",
            "Coffee",
            "Pens",
        ];

        let name = if parent_id.is_some() {
            subcategories[rng.gen_range(0..subcategories.len())].to_string()
        } else {
            categories[rng.gen_range(0..categories.len())].to_string()
        };

        let slug = name.to_lowercase().replace(' ', "-").replace('&', "and");
        let depth = if parent_id.is_some() { 1 } else { 0 };

        let path = if parent_id.is_some() {
            vec!["root".to_string(), slug.clone()]
        } else {
            vec![slug.clone()]
        };

        ProductCategory {
            id: Uuid::new_v4(),
            organization_id,
            name,
            slug,
            parent_id,
            depth,
            path,
            is_active: rng.gen_bool(0.9),
            created_at: Utc::now() - Duration::days(rng.gen_range(30..365)),
        }
    }

    /// Generate a product with pricing, tags, attributes, and images
    pub fn generate_product(&self, organization_id: Uuid, category_id: Option<Uuid>) -> Product {
        let mut rng = StdRng::from_entropy();

        let product_names = [
            "Premium Wireless Headphones",
            "Ergonomic Office Chair",
            "Organic Cotton T-Shirt",
            "Stainless Steel Water Bottle",
            "Smart LED Desk Lamp",
            "Leather Messenger Bag",
            "Mechanical Keyboard Pro",
            "Noise Cancelling Earbuds",
            "Bamboo Cutting Board Set",
            "Portable Bluetooth Speaker",
            "Merino Wool Sweater",
            "Glass Food Storage Set",
            "USB-C Hub Adapter",
            "Running Shoes Ultra",
            "Ceramic Coffee Mug",
        ];
        let tag_pool = [
            "bestseller",
            "new-arrival",
            "eco-friendly",
            "limited-edition",
            "sale",
            "premium",
            "handmade",
            "organic",
            "vegan",
            "imported",
        ];
        let colors = ["Black", "White", "Navy", "Red", "Green", "Gray", "Natural"];
        let sizes = ["XS", "S", "M", "L", "XL", "One Size"];
        let materials = [
            "Cotton",
            "Polyester",
            "Leather",
            "Metal",
            "Wood",
            "Bamboo",
            "Plastic",
        ];

        let name = product_names[rng.gen_range(0..product_names.len())].to_string();
        let sku_prefix = name
            .chars()
            .filter(|c| c.is_uppercase())
            .collect::<String>();
        let sku = format!("{}-{}", sku_prefix, rng.gen_range(1000..9999));

        let price_cents: i64 = rng.gen_range(499..49999);
        let compare_at_price_cents = if rng.gen_bool(0.3) {
            Some(price_cents + rng.gen_range(500..5000))
        } else {
            None
        };
        let cost_cents = Some((price_cents as f64 * rng.gen_range(0.3..0.6)) as i64);

        let num_tags = rng.gen_range(1..4);
        let mut tags = Vec::new();
        for _ in 0..num_tags {
            let tag = tag_pool[rng.gen_range(0..tag_pool.len())].to_string();
            if !tags.contains(&tag) {
                tags.push(tag);
            }
        }

        let is_digital = rng.gen_bool(0.15);

        Product {
            id: Uuid::new_v4(),
            organization_id,
            category_id,
            sku,
            name: name.clone(),
            description: Some(format!(
                "High-quality {} for everyday use. Made with premium materials.",
                name.to_lowercase()
            )),
            price_cents,
            compare_at_price_cents,
            cost_cents,
            currency: "USD".to_string(),
            tags,
            attributes: json!({
                "color": colors[rng.gen_range(0..colors.len())],
                "size": sizes[rng.gen_range(0..sizes.len())],
                "material": materials[rng.gen_range(0..materials.len())],
                "warranty_months": rng.gen_range(0..36)
            }),
            images: vec![
                format!(
                    "https://cdn.example.com/products/{}.jpg",
                    rng.gen_range(1000..9999)
                ),
                format!(
                    "https://cdn.example.com/products/{}.jpg",
                    rng.gen_range(1000..9999)
                ),
            ],
            is_active: rng.gen_bool(0.9),
            is_digital,
            weight_grams: if is_digital {
                None
            } else {
                Some(rng.gen_range(50..5000))
            },
            rating_avg: (rng.gen_range(2.5..5.0_f64) * 10.0).round() / 10.0,
            rating_count: rng.gen_range(0..500),
            created_at: Utc::now() - Duration::days(rng.gen_range(5..365)),
            updated_at: Utc::now() - Duration::days(rng.gen_range(0..5)),
        }
    }

    /// Generate an e-commerce order with addresses and full financial breakdown
    pub fn generate_order(&self, organization_id: Uuid, user_id: Uuid) -> Order {
        let mut rng = StdRng::from_entropy();

        let statuses = [
            "pending",
            "confirmed",
            "processing",
            "shipped",
            "delivered",
            "cancelled",
            "refunded",
        ];
        let status = statuses[rng.gen_range(0..statuses.len())].to_string();

        let item_count = rng.gen_range(1..6);
        let subtotal_cents: i64 = rng.gen_range(1999..150_000);
        let discount_cents: i64 = if rng.gen_bool(0.3) {
            rng.gen_range(500..5000)
        } else {
            0
        };
        let tax_rate = rng.gen_range(0.05..0.12);
        let tax_cents = ((subtotal_cents - discount_cents) as f64 * tax_rate) as i64;
        let shipping_cents: i64 = if rng.gen_bool(0.2) {
            0
        } else {
            rng.gen_range(499..1999)
        };
        let total_cents = subtotal_cents - discount_cents + tax_cents + shipping_cents;

        let order_number = format!(
            "ORD-{}-{}",
            Utc::now().format("%Y%m"),
            rng.gen_range(100000..999999)
        );

        let placed_at = Utc::now() - Duration::days(rng.gen_range(0..90));

        let shipped_at = if ["shipped", "delivered"].contains(&status.as_str()) {
            Some(placed_at + Duration::days(rng.gen_range(1..5)))
        } else {
            None
        };
        let delivered_at = if status == "delivered" {
            shipped_at.map(|s| s + Duration::days(rng.gen_range(1..7)))
        } else {
            None
        };
        let cancelled_at = if status == "cancelled" {
            Some(placed_at + Duration::hours(rng.gen_range(1..48)))
        } else {
            None
        };

        let streets = [
            "123 Main St",
            "456 Oak Ave",
            "789 Pine Blvd",
            "321 Maple Dr",
            "654 Elm Way",
        ];
        let cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"];
        let states = ["NY", "CA", "IL", "TX", "AZ"];
        let zips = ["10001", "90001", "60601", "77001", "85001"];
        let addr_idx = rng.gen_range(0..streets.len());

        let shipping_address = json!({
            "line1": streets[addr_idx],
            "city": cities[addr_idx],
            "state": states[addr_idx],
            "zip": zips[addr_idx],
            "country": "US"
        });

        let billing_address = if rng.gen_bool(0.7) {
            shipping_address.clone()
        } else {
            let idx2 = rng.gen_range(0..streets.len());
            json!({
                "line1": streets[idx2],
                "city": cities[idx2],
                "state": states[idx2],
                "zip": zips[idx2],
                "country": "US"
            })
        };

        Order {
            id: Uuid::new_v4(),
            organization_id,
            user_id,
            cart_id: if rng.gen_bool(0.6) {
                Some(Uuid::new_v4())
            } else {
                None
            },
            order_number,
            status,
            subtotal_cents,
            discount_cents,
            tax_cents,
            shipping_cents,
            total_cents,
            currency: "USD".to_string(),
            coupon_id: if discount_cents > 0 {
                Some(Uuid::new_v4())
            } else {
                None
            },
            shipping_address: Some(shipping_address),
            billing_address: Some(billing_address),
            notes: if rng.gen_bool(0.2) {
                Some("Please leave at front door".to_string())
            } else {
                None
            },
            metadata: json!({
                "source": "web",
                "item_count": item_count,
                "ip_address": self.random_ip()
            }),
            placed_at,
            shipped_at,
            delivered_at,
            cancelled_at,
            created_at: placed_at,
            updated_at: Utc::now(),
        }
    }

    /// Generate a product review with rating, title, and body
    pub fn generate_review(
        &self,
        organization_id: Uuid,
        product_id: Uuid,
        user_id: Uuid,
    ) -> Review {
        let mut rng = StdRng::from_entropy();

        let positive_titles = [
            "Absolutely love it!",
            "Great quality product",
            "Exceeded expectations",
            "Best purchase this year",
            "Highly recommend",
            "Worth every penny",
        ];
        let neutral_titles = [
            "Decent product",
            "Good but not great",
            "Does the job",
            "Average quality",
            "Okay for the price",
        ];
        let negative_titles = [
            "Disappointed",
            "Not as described",
            "Poor quality",
            "Would not buy again",
            "Broke after a week",
        ];

        let positive_bodies = [
            "This product is amazing! The build quality is outstanding and it works perfectly. I've been using it daily for a month now and couldn't be happier with my purchase.",
            "Arrived quickly and well-packaged. The material feels premium and it functions exactly as advertised. Would definitely buy from this brand again.",
            "I did a lot of research before buying this and I'm glad I chose it. It's durable, well-designed, and great value for the money.",
        ];
        let negative_bodies = [
            "Unfortunately this product did not meet my expectations. The quality feels cheap and it stopped working properly after just two weeks of normal use.",
            "Not worth the price. There are much better alternatives available. The product arrived with a small defect and customer support was slow to respond.",
        ];

        let rating: i16 = {
            let r: f64 = rng.gen();
            if r < 0.10 {
                rng.gen_range(1..=2)
            } else if r < 0.25 {
                3
            } else if r < 0.55 {
                4
            } else {
                5
            }
        };

        let title = match rating {
            1..=2 => negative_titles[rng.gen_range(0..negative_titles.len())],
            3 => neutral_titles[rng.gen_range(0..neutral_titles.len())],
            _ => positive_titles[rng.gen_range(0..positive_titles.len())],
        };

        let body = match rating {
            1..=2 => negative_bodies[rng.gen_range(0..negative_bodies.len())],
            _ => positive_bodies[rng.gen_range(0..positive_bodies.len())],
        };

        Review {
            id: Uuid::new_v4(),
            organization_id,
            product_id,
            user_id,
            order_id: if rng.gen_bool(0.7) {
                Some(Uuid::new_v4())
            } else {
                None
            },
            rating,
            title: Some(title.to_string()),
            body: Some(body.to_string()),
            is_verified_purchase: rng.gen_bool(0.7),
            helpful_count: rng.gen_range(0..50),
            reported: rng.gen_bool(0.03),
            metadata: json!({
                "platform": if rng.gen_bool(0.7) { "web" } else { "mobile" },
                "photos_attached": rng.gen_range(0..4)
            }),
            created_at: Utc::now() - Duration::days(rng.gen_range(1..180)),
            updated_at: Utc::now() - Duration::days(rng.gen_range(0..30)),
        }
    }

    // ========== FINANCE DOMAIN GENERATORS ==========

    /// Generate a subscription plan with features, limits, and pricing
    pub fn generate_subscription_plan(&self, organization_id: Uuid) -> SubscriptionPlan {
        let mut rng = StdRng::from_entropy();

        let plans = [
            ("Starter", "starter", 0, "Basic features for individuals"),
            ("Basic", "basic", 999, "Essential features for small teams"),
            (
                "Professional",
                "professional",
                2999,
                "Advanced features for growing teams",
            ),
            ("Business", "business", 7999, "Full-featured for businesses"),
            (
                "Enterprise",
                "enterprise",
                19999,
                "Custom solutions for large organizations",
            ),
        ];
        let intervals = ["month", "year"];

        let (name, slug, base_price, description) = plans[rng.gen_range(0..plans.len())];
        let interval = intervals[rng.gen_range(0..intervals.len())].to_string();
        let interval_count = 1;
        let price_cents = if interval == "year" {
            (base_price as f64 * 10.0) as i64
        } else {
            base_price
        };

        let trial_days = if base_price > 0 {
            rng.gen_range(0..=14)
        } else {
            0
        };

        SubscriptionPlan {
            id: Uuid::new_v4(),
            organization_id,
            name: name.to_string(),
            slug: slug.to_string(),
            description: Some(description.to_string()),
            price_cents,
            currency: "USD".to_string(),
            interval,
            interval_count,
            trial_days,
            features: json!({
                "analytics": base_price >= 999,
                "custom_reports": base_price >= 2999,
                "api_access": base_price >= 2999,
                "white_label": base_price >= 7999,
                "dedicated_support": base_price >= 19999,
                "sso": base_price >= 7999,
                "audit_log": base_price >= 2999,
                "data_export": true
            }),
            limits: json!({
                "users": if base_price == 0 { 1 } else if base_price < 2999 { 5 } else if base_price < 7999 { 25 } else { -1 },
                "events_per_month": if base_price == 0 { 1000 } else if base_price < 2999 { 50000 } else if base_price < 7999 { 500000 } else { -1 },
                "projects": if base_price == 0 { 1 } else if base_price < 2999 { 5 } else if base_price < 7999 { 50 } else { -1 },
                "retention_days": if base_price < 2999 { 30 } else if base_price < 7999 { 90 } else { 365 }
            }),
            is_active: rng.gen_bool(0.9),
            sort_order: rng.gen_range(0..10),
            created_at: Utc::now() - Duration::days(rng.gen_range(30..365)),
            updated_at: Utc::now() - Duration::days(rng.gen_range(0..30)),
        }
    }

    /// Generate a subscription linking a user to a plan
    pub fn generate_subscription(
        &self,
        organization_id: Uuid,
        user_id: Uuid,
        plan_id: Uuid,
    ) -> Subscription {
        let mut rng = StdRng::from_entropy();

        let statuses = [
            "trialing",
            "active",
            "past_due",
            "cancelled",
            "expired",
            "paused",
        ];
        let status = statuses[rng.gen_range(0..statuses.len())].to_string();

        let period_start = Utc::now() - Duration::days(rng.gen_range(0..30));
        let period_end = period_start + Duration::days(30);

        let trial_end = if status == "trialing" {
            Some(Utc::now() + Duration::days(rng.gen_range(1..14)))
        } else {
            None
        };

        let cancel_reasons = [
            "Too expensive",
            "Switching to competitor",
            "No longer needed",
            "Missing features",
            "Poor support experience",
        ];
        let cancelled_at = if status == "cancelled" {
            Some(Utc::now() - Duration::days(rng.gen_range(0..30)))
        } else {
            None
        };
        let cancel_reason = if status == "cancelled" {
            Some(cancel_reasons[rng.gen_range(0..cancel_reasons.len())].to_string())
        } else {
            None
        };

        let mrr_cents: i64 = rng.gen_range(999..19999);

        Subscription {
            id: Uuid::new_v4(),
            organization_id,
            user_id,
            plan_id,
            status,
            current_period_start: period_start,
            current_period_end: period_end,
            trial_end,
            cancelled_at,
            cancel_reason,
            metadata: json!({
                "payment_method": "credit_card",
                "auto_renew": true,
                "billing_email": format!("billing-{}@example.com", rng.gen_range(100..999))
            }),
            mrr_cents,
            created_at: Utc::now() - Duration::days(rng.gen_range(30..365)),
            updated_at: Utc::now(),
        }
    }

    /// Generate an invoice with line items, tax, and due date
    pub fn generate_invoice(&self, organization_id: Uuid, user_id: Uuid) -> Invoice {
        let mut rng = StdRng::from_entropy();

        let statuses = ["draft", "sent", "paid", "overdue", "void", "disputed"];
        let status = statuses[rng.gen_range(0..statuses.len())].to_string();

        let invoice_number = format!(
            "INV-{}-{}",
            Utc::now().format("%Y%m"),
            rng.gen_range(10000..99999)
        );

        let line_items_count: i32 = rng.gen_range(1..6);
        let subtotal_cents: i64 = rng.gen_range(999..50000);
        let tax_rate = rng.gen_range(0.05..0.12);
        let tax_cents = (subtotal_cents as f64 * tax_rate) as i64;
        let discount_cents: i64 = if rng.gen_bool(0.2) {
            rng.gen_range(100..2000)
        } else {
            0
        };
        let total_cents = subtotal_cents + tax_cents - discount_cents;

        let issued_at = Utc::now() - Duration::days(rng.gen_range(0..60));
        let due_date = (issued_at + Duration::days(30)).date_naive();

        let paid_at = if status == "paid" {
            Some(issued_at + Duration::days(rng.gen_range(1..28)))
        } else {
            None
        };

        let metadata = json!({
            "billing_period": "monthly",
            "auto_charged": rng.gen_bool(0.7),
            "reminder_sent": status == "overdue"
        });

        Invoice {
            id: Uuid::new_v4(),
            organization_id,
            user_id,
            subscription_id: if rng.gen_bool(0.6) {
                Some(Uuid::new_v4())
            } else {
                None
            },
            order_id: if rng.gen_bool(0.3) {
                Some(Uuid::new_v4())
            } else {
                None
            },
            invoice_number,
            status,
            subtotal_cents,
            tax_cents,
            discount_cents,
            total_cents,
            currency: "USD".to_string(),
            due_date,
            paid_at,
            notes: if rng.gen_bool(0.2) {
                Some("Net 30 payment terms".to_string())
            } else {
                None
            },
            line_items_count,
            metadata,
            issued_at,
            created_at: issued_at,
            updated_at: Utc::now(),
        }
    }

    /// Generate a payment transaction with gateway details
    pub fn generate_payment(&self, organization_id: Uuid, user_id: Uuid) -> Payment {
        let mut rng = StdRng::from_entropy();

        let statuses = [
            "pending",
            "processing",
            "completed",
            "failed",
            "refunded",
            "disputed",
        ];
        let methods = [
            "credit_card",
            "debit_card",
            "bank_transfer",
            "paypal",
            "stripe",
            "crypto",
            "invoice",
        ];

        let status = statuses[rng.gen_range(0..statuses.len())].to_string();
        let method = methods[rng.gen_range(0..methods.len())].to_string();
        let amount_cents: i64 = rng.gen_range(499..100_000);

        let gateway_transaction_id = if status != "pending" {
            Some(format!(
                "txn_{}",
                Uuid::new_v4().to_string().replace('-', "")
            ))
        } else {
            None
        };

        let failure_reasons = [
            "Insufficient funds",
            "Card declined",
            "Network timeout",
            "Invalid card number",
            "Expired card",
            "3DS authentication failed",
        ];

        let failure_reason = if status == "failed" {
            Some(failure_reasons[rng.gen_range(0..failure_reasons.len())].to_string())
        } else {
            None
        };

        let processed_at = if ["completed", "failed", "refunded"].contains(&status.as_str()) {
            Some(Utc::now() - Duration::seconds(rng.gen_range(60..86400)))
        } else {
            None
        };

        let card_brands = ["visa", "mastercard", "amex", "discover"];
        let gateway_response = if status == "completed" {
            json!({
                "gateway": "stripe",
                "charge_id": format!("ch_{}", Uuid::new_v4().to_string().replace('-', "")[..24].to_string()),
                "card_brand": card_brands[rng.gen_range(0..card_brands.len())],
                "card_last4": format!("{:04}", rng.gen_range(1000..9999)),
                "risk_score": rng.gen_range(0..100),
                "receipt_url": format!("https://pay.stripe.com/receipts/{}", rng.gen_range(100000..999999))
            })
        } else if status == "failed" {
            json!({
                "gateway": "stripe",
                "error_code": "card_declined",
                "decline_code": "insufficient_funds",
                "message": failure_reason.as_deref().unwrap_or("Unknown error")
            })
        } else {
            json!({})
        };

        Payment {
            id: Uuid::new_v4(),
            organization_id,
            user_id,
            invoice_id: if rng.gen_bool(0.6) {
                Some(Uuid::new_v4())
            } else {
                None
            },
            order_id: if rng.gen_bool(0.4) {
                Some(Uuid::new_v4())
            } else {
                None
            },
            amount_cents,
            currency: "USD".to_string(),
            status,
            method,
            gateway_transaction_id,
            gateway_response,
            failure_reason,
            idempotency_key: Some(Uuid::new_v4().to_string()),
            processed_at,
            created_at: Utc::now() - Duration::seconds(rng.gen_range(60..172800)),
            updated_at: Utc::now(),
        }
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub fn cache_key_user_list(&self, org_id: Uuid, page: u32) -> String {
        format!("analytics:{}:users:page:{}", org_id, page)
    }

    /// Generate session cache key for active sessions
    pub fn cache_key_session(&self, org_id: Uuid, session_id: &str) -> String {
        format!("analytics:{}:session:{}", org_id, session_id)
    }

    /// Generate aggregate counter key for fast increments
    #[allow(dead_code)]
    pub fn cache_key_counter(&self, org_id: Uuid, metric: &str) -> String {
        format!("analytics:{}:counter:{}", org_id, metric)
    }

    /// Generate time-series bucket key
    #[allow(dead_code)]
    pub fn cache_key_timeseries_bucket(
        &self,
        org_id: Uuid,
        metric: &str,
        timestamp: DateTime<Utc>,
    ) -> String {
        format!(
            "analytics:{}:ts:{}:{}",
            org_id,
            metric,
            timestamp.format("%Y%m%d%H%M")
        )
    }

    /// Generate rolling window aggregation key (last N minutes)
    pub fn cache_key_rolling_window(&self, org_id: Uuid, metric: &str, minutes: u32) -> String {
        format!("analytics:{}:rolling:{}:{}m", org_id, metric, minutes)
    }

    /// Generate marketing snapshot cache key
    pub fn cache_key_marketing(&self, org_id: Uuid) -> String {
        format!("analytics:{}:marketing:summary", org_id)
    }

    /// Generate commerce snapshot cache key
    pub fn cache_key_commerce(&self, org_id: Uuid) -> String {
        format!("analytics:{}:commerce:summary", org_id)
    }

    /// Generate storefront cache key
    pub fn cache_key_storefront(&self, org_id: Uuid) -> String {
        format!("analytics:{}:commerce:storefront", org_id)
    }

    /// Generate product catalog cache key
    pub fn cache_key_catalog(&self, org_id: Uuid) -> String {
        format!("analytics:{}:commerce:catalog", org_id)
    }

    /// Generate cart snapshot cache key
    pub fn cache_key_cart(&self, org_id: Uuid, cart_id: Uuid) -> String {
        format!("analytics:{}:commerce:cart:{}", org_id, cart_id)
    }

    /// Get list of all time buckets for the last N hours
    #[allow(dead_code)]
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
            "/api",
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_cache_key_generation() {
        let generator = DataGenerator::new();
        let org_id = Uuid::new_v4();

        println!("Overview: {}", generator.cache_key_overview(org_id, 24));
        println!("Hourly: {}", generator.cache_key_hourly(org_id, Utc::now()));
        println!(
            "User: {}",
            generator.cache_key_user_activity(Uuid::new_v4())
        );
        println!("Page: {}", generator.cache_key_page(org_id, "/dashboard"));
    }
}
