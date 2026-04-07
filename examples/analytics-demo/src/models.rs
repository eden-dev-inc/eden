#![allow(dead_code)]

// Data Models and Types
//
// Enhanced models with granular structures for time-series, user-level,
// and page-level analytics to support 10K+ cache keys.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::str::FromStr;
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
}

impl FromStr for EventType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "page_view" => Ok(EventType::PageView),
            "click" => Ok(EventType::Click),
            "conversion" => Ok(EventType::Conversion),
            "sign_up" => Ok(EventType::SignUp),
            "purchase" => Ok(EventType::Purchase),
            _ => Err(()),
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

/// ReferrerBreakdown captures traffic sources for a reporting period
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferrerBreakdown {
    pub organization_id: Uuid,
    pub period: String,
    pub sources: Vec<ReferrerStats>,
}

/// CohortBreakdown groups retention metrics by signup cohort
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CohortBreakdown {
    pub organization_id: Uuid,
    pub cohorts: Vec<CohortAnalysis>,
}

/// DeviceBreakdown summarizes browser and device trends
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceBreakdown {
    pub organization_id: Uuid,
    pub period: String,
    pub stats: Vec<DeviceBrowserStats>,
}

/// GeoBreakdown summarizes geographic performance by city and country
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoBreakdown {
    pub organization_id: Uuid,
    pub period: String,
    pub regions: Vec<GeographicDistribution>,
}

/// FunnelAnalysis captures performance across a multi-step journey
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunnelAnalysis {
    pub organization_id: Uuid,
    pub funnel_id: String,
    pub steps: Vec<FunnelStep>,
}

/// SessionSnapshot contains active-session and page-view context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub organization_id: Uuid,
    pub active_users: i64,
    pub sessions: Vec<Session>,
    pub recent_page_views: Vec<PageViewRecord>,
}

/// MarketingSnapshot groups marketing and experimentation signals
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketingSnapshot {
    pub organization_id: Uuid,
    pub active_campaigns: i64,
    pub running_experiments: i64,
    pub campaigns: Vec<Campaign>,
    pub experiments: Vec<Experiment>,
    pub goals: Vec<Goal>,
}

/// CommerceSnapshot groups revenue and subscription signals
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommerceSnapshot {
    pub organization_id: Uuid,
    pub revenue_cents: i64,
    pub payment_failure_rate: f64,
    pub active_subscriptions: i64,
    pub plans: Vec<SubscriptionPlan>,
    pub products: Vec<Product>,
    pub orders: Vec<Order>,
    pub reviews: Vec<Review>,
    pub subscriptions: Vec<Subscription>,
    pub invoices: Vec<Invoice>,
    pub payments: Vec<Payment>,
}

/// CatalogProduct represents a storefront-ready product card with inventory state
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct CatalogProduct {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub sku: String,
    pub name: String,
    pub description: Option<String>,
    pub price_cents: i64,
    pub compare_at_price_cents: Option<i64>,
    pub currency: String,
    pub tags: Vec<String>,
    pub images: Vec<String>,
    pub rating_avg: f64,
    pub rating_count: i32,
    pub quantity_available: i32,
    pub is_low_stock: bool,
}

/// CatalogResponse represents a cached storefront catalog view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogResponse {
    pub organization_id: Uuid,
    pub generated_at: DateTime<Utc>,
    pub products: Vec<CatalogProduct>,
}

/// StorefrontProductRevenue summarizes best-selling products
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorefrontProductRevenue {
    pub product_name: String,
    pub revenue_cents: i64,
}

/// StorefrontOrderSummary captures order totals by status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorefrontOrderSummary {
    pub status: String,
    pub order_count: i64,
    pub total_cents: i64,
}

/// StorefrontInventoryAlert surfaces inventory pressure for the demo
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorefrontInventoryAlert {
    pub product_name: String,
    pub quantity_available: i32,
    pub reorder_point: i32,
}

/// StorefrontResponse represents the main commerce landing page
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorefrontResponse {
    pub organization_id: Uuid,
    pub generated_at: DateTime<Utc>,
    pub featured_products: Vec<CatalogProduct>,
    pub top_products_by_revenue: Vec<StorefrontProductRevenue>,
    pub order_summary: Vec<StorefrontOrderSummary>,
    pub low_stock_alerts: Vec<StorefrontInventoryAlert>,
    pub cart_abandonment_rate: f64,
    pub carts_abandoned: i64,
    pub carts_total: i64,
}

/// CartLineItemDetail represents a line item with product metadata for cart reads
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct CartLineItemDetail {
    pub id: Uuid,
    pub product_id: Uuid,
    pub product_name: String,
    pub sku: String,
    pub quantity: i32,
    pub unit_price_cents: i64,
    pub line_total_cents: i64,
    pub tags: Vec<String>,
}

/// CartSnapshot captures the current cart state for an organization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CartSnapshot {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub user_id: Option<Uuid>,
    pub status: String,
    pub subtotal_cents: i64,
    pub discount_cents: i64,
    pub total_cents: i64,
    pub item_count: i32,
    pub updated_at: DateTime<Utc>,
    pub abandoned_at: Option<DateTime<Utc>>,
    pub items: Vec<CartLineItemDetail>,
}

/// CheckoutReceipt captures the result of a completed checkout
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckoutReceipt {
    pub organization_id: Uuid,
    pub cart_id: Uuid,
    pub order_id: Uuid,
    pub payment_id: Uuid,
    pub user_id: Uuid,
    pub total_cents: i64,
    pub currency: String,
    pub created_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// New Enums for PostgreSQL custom types
// ---------------------------------------------------------------------------

/// DeviceType classifies the device used in a session
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "device_type", rename_all = "snake_case")]
pub enum DeviceType {
    Desktop,
    Mobile,
    Tablet,
    Bot,
    Unknown,
}

/// CampaignStatus tracks the lifecycle of a marketing campaign
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "campaign_status", rename_all = "snake_case")]
pub enum CampaignStatus {
    Draft,
    Active,
    Paused,
    Completed,
    Archived,
}

/// ExperimentStatus tracks the lifecycle of an A/B experiment
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "experiment_status", rename_all = "snake_case")]
pub enum ExperimentStatus {
    Draft,
    Running,
    Paused,
    Concluded,
}

/// GoalType classifies what kind of conversion goal is being tracked
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "goal_type", rename_all = "snake_case")]
pub enum GoalType {
    PageView,
    Event,
    Duration,
    PagesPerSession,
    Revenue,
}

/// OrderStatus tracks the lifecycle of an e-commerce order
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "order_status", rename_all = "snake_case")]
pub enum OrderStatus {
    Pending,
    Confirmed,
    Processing,
    Shipped,
    Delivered,
    Cancelled,
    Refunded,
}

/// CartStatus tracks the state of a shopping cart
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "cart_status", rename_all = "snake_case")]
pub enum CartStatus {
    Active,
    Converted,
    Abandoned,
    Expired,
}

/// DiscountType classifies the type of discount applied by a coupon
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "discount_type", rename_all = "snake_case")]
pub enum DiscountType {
    Percentage,
    FixedAmount,
    BuyXGetY,
    FreeShipping,
}

/// PaymentStatus tracks the state of a payment transaction
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "payment_status", rename_all = "snake_case")]
pub enum PaymentStatus {
    Pending,
    Processing,
    Completed,
    Failed,
    Refunded,
    Disputed,
}

/// PaymentMethod classifies how a payment was made
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "payment_method", rename_all = "snake_case")]
pub enum PaymentMethod {
    CreditCard,
    DebitCard,
    BankTransfer,
    Paypal,
    Stripe,
    Crypto,
    Invoice,
}

/// SubscriptionStatus tracks the lifecycle of a recurring subscription
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "subscription_status", rename_all = "snake_case")]
pub enum SubscriptionStatus {
    Trialing,
    Active,
    PastDue,
    Cancelled,
    Expired,
    Paused,
}

/// InvoiceStatus tracks the state of an invoice
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "invoice_status", rename_all = "snake_case")]
pub enum InvoiceStatus {
    Draft,
    Sent,
    Paid,
    Overdue,
    Void,
    Disputed,
}

/// LedgerEntryType classifies a double-entry bookkeeping entry
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "ledger_entry_type", rename_all = "snake_case")]
pub enum LedgerEntryType {
    Debit,
    Credit,
}

/// RefundStatus tracks the lifecycle of a refund request
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "refund_status", rename_all = "snake_case")]
pub enum RefundStatus {
    Requested,
    Processing,
    Approved,
    Completed,
    Rejected,
}

// ---------------------------------------------------------------------------
// Analytics Domain Structs
// ---------------------------------------------------------------------------

/// Session represents a user browsing session with device and UTM data
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Session {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub user_id: Option<Uuid>,
    pub session_token: String,
    pub device: String,
    pub browser: Option<String>,
    pub os: Option<String>,
    pub screen_resolution: Option<String>,
    pub country_code: Option<String>,
    pub city: Option<String>,
    pub ip_address: Option<String>,
    pub landing_page: Option<String>,
    pub exit_page: Option<String>,
    pub page_count: i32,
    pub duration_seconds: Option<i32>,
    pub is_bounce: bool,
    pub utm_source: Option<String>,
    pub utm_medium: Option<String>,
    pub utm_campaign: Option<String>,
    pub utm_content: Option<String>,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Campaign represents a marketing campaign with budget and performance tracking
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Campaign {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub name: String,
    pub status: String,
    pub channel: String,
    pub budget_cents: i64,
    pub spent_cents: i64,
    pub target_audience: serde_json::Value,
    pub tags: Vec<String>,
    pub click_count: i32,
    pub impression_count: i32,
    pub conversion_count: i32,
    pub starts_at: Option<DateTime<Utc>>,
    pub ends_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Experiment represents an A/B test with hypothesis, variants, and results
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Experiment {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub status: String,
    pub hypothesis: Option<String>,
    pub variants: serde_json::Value,
    pub metric_name: String,
    pub baseline_rate: Option<f64>,
    pub sample_size_target: Option<i32>,
    pub confidence_level: f64,
    pub winning_variant: Option<String>,
    pub results: serde_json::Value,
    pub started_at: Option<DateTime<Utc>>,
    pub concluded_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// ExperimentAssignment links a user to a specific variant in an experiment
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ExperimentAssignment {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub experiment_id: Uuid,
    pub user_id: Uuid,
    pub variant: String,
    pub converted: bool,
    pub conversion_value: Option<f64>,
    pub assigned_at: DateTime<Utc>,
    pub converted_at: Option<DateTime<Utc>>,
}

/// PageView records a single page visit with web-vitals performance metrics
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct PageViewRecord {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub session_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub event_id: Option<Uuid>,
    pub page_url: String,
    pub page_title: Option<String>,
    pub referrer_url: Option<String>,
    pub time_on_page_ms: Option<i32>,
    pub dom_load_ms: Option<i32>,
    pub first_paint_ms: Option<i32>,
    pub first_contentful_paint_ms: Option<i32>,
    pub largest_contentful_paint_ms: Option<i32>,
    pub cumulative_layout_shift: Option<f64>,
    pub viewport_width: Option<i32>,
    pub viewport_height: Option<i32>,
    pub scroll_depth_pct: Option<i16>,
    pub created_at: DateTime<Utc>,
}

/// Goal defines a conversion goal with a target value and completion tracking
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Goal {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub name: String,
    pub goal_type: String,
    pub target_value: Option<f64>,
    pub match_pattern: Option<String>,
    pub is_active: bool,
    pub completions_count: i32,
    pub total_value: f64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// GoalCompletion records a single completion event for a goal
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct GoalCompletion {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub goal_id: Uuid,
    pub user_id: Option<Uuid>,
    pub session_id: Option<Uuid>,
    pub value: Option<f64>,
    pub properties: serde_json::Value,
    pub completed_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// E-commerce Domain Structs
// ---------------------------------------------------------------------------

/// ProductCategory organizes products into a hierarchical tree
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ProductCategory {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub name: String,
    pub slug: String,
    pub parent_id: Option<Uuid>,
    pub depth: i32,
    pub path: Vec<String>,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
}

/// Product represents a purchasable item in the catalog
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Product {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub category_id: Option<Uuid>,
    pub sku: String,
    pub name: String,
    pub description: Option<String>,
    pub price_cents: i64,
    pub compare_at_price_cents: Option<i64>,
    pub cost_cents: Option<i64>,
    pub currency: String,
    pub tags: Vec<String>,
    pub attributes: serde_json::Value,
    pub images: Vec<String>,
    pub is_active: bool,
    pub is_digital: bool,
    pub weight_grams: Option<i32>,
    pub rating_avg: f64,
    pub rating_count: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Inventory tracks stock levels for a product at a specific warehouse
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Inventory {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub product_id: Uuid,
    pub warehouse_code: String,
    pub quantity_on_hand: i32,
    pub quantity_reserved: i32,
    pub quantity_available: i32,
    pub reorder_point: i32,
    pub reorder_quantity: i32,
    pub is_low_stock: bool,
    pub last_restocked_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Coupon represents a discount code that can be applied to carts and orders
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Coupon {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub code: String,
    pub discount_type: String,
    pub discount_value: f64,
    pub min_order_cents: i64,
    pub max_discount_cents: Option<i64>,
    pub max_uses: Option<i32>,
    pub current_uses: i32,
    pub is_active: bool,
    pub starts_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Cart represents a shopping cart with pricing and status tracking
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Cart {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub user_id: Option<Uuid>,
    pub session_id: Option<Uuid>,
    pub status: String,
    pub coupon_id: Option<Uuid>,
    pub subtotal_cents: i64,
    pub discount_cents: i64,
    pub total_cents: i64,
    pub item_count: i32,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub abandoned_at: Option<DateTime<Utc>>,
}

/// CartItem represents a single line item within a shopping cart
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct CartItem {
    pub id: Uuid,
    pub cart_id: Uuid,
    pub product_id: Uuid,
    pub quantity: i32,
    pub unit_price_cents: i64,
    pub line_total_cents: i64,
    pub added_at: DateTime<Utc>,
}

/// Order represents a placed e-commerce order with full financial breakdown
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Order {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub user_id: Uuid,
    pub cart_id: Option<Uuid>,
    pub order_number: String,
    pub status: String,
    pub subtotal_cents: i64,
    pub discount_cents: i64,
    pub tax_cents: i64,
    pub shipping_cents: i64,
    pub total_cents: i64,
    pub currency: String,
    pub coupon_id: Option<Uuid>,
    pub shipping_address: Option<serde_json::Value>,
    pub billing_address: Option<serde_json::Value>,
    pub notes: Option<String>,
    pub metadata: serde_json::Value,
    pub placed_at: DateTime<Utc>,
    pub shipped_at: Option<DateTime<Utc>>,
    pub delivered_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// OrderItem represents a single line item within an order
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct OrderItem {
    pub id: Uuid,
    pub order_id: Uuid,
    pub product_id: Uuid,
    pub product_name: String,
    pub sku: String,
    pub quantity: i32,
    pub unit_price_cents: i64,
    pub discount_cents: i64,
    pub line_total_cents: i64,
    pub metadata: serde_json::Value,
}

/// Review represents a user review and rating for a product
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Review {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub product_id: Uuid,
    pub user_id: Uuid,
    pub order_id: Option<Uuid>,
    pub rating: i16,
    pub title: Option<String>,
    pub body: Option<String>,
    pub is_verified_purchase: bool,
    pub helpful_count: i32,
    pub reported: bool,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Finance Domain Structs
// ---------------------------------------------------------------------------

/// SubscriptionPlan defines a recurring billing plan with features and limits
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct SubscriptionPlan {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub name: String,
    pub slug: String,
    pub description: Option<String>,
    pub price_cents: i64,
    pub currency: String,
    pub interval: String,
    pub interval_count: i32,
    pub trial_days: i32,
    pub features: serde_json::Value,
    pub limits: serde_json::Value,
    pub is_active: bool,
    pub sort_order: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Subscription represents an active recurring billing relationship
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Subscription {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub user_id: Uuid,
    pub plan_id: Uuid,
    pub status: String,
    pub current_period_start: DateTime<Utc>,
    pub current_period_end: DateTime<Utc>,
    pub trial_end: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub cancel_reason: Option<String>,
    pub metadata: serde_json::Value,
    pub mrr_cents: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// SubscriptionEvent records lifecycle events for a subscription (upgrades, downgrades, cancellations)
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct SubscriptionEvent {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub subscription_id: Uuid,
    pub event_type: String,
    pub from_plan_id: Option<Uuid>,
    pub to_plan_id: Option<Uuid>,
    pub mrr_delta_cents: i64,
    pub metadata: serde_json::Value,
    pub occurred_at: DateTime<Utc>,
}

/// Account represents a chart-of-accounts entry for double-entry bookkeeping
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Account {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub code: String,
    pub name: String,
    pub account_type: String,
    pub parent_code: Option<String>,
    pub is_active: bool,
    pub normal_balance: String,
    pub created_at: DateTime<Utc>,
}

/// Invoice represents a billing document sent to a customer
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Invoice {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub user_id: Uuid,
    pub subscription_id: Option<Uuid>,
    pub order_id: Option<Uuid>,
    pub invoice_number: String,
    pub status: String,
    pub subtotal_cents: i64,
    pub tax_cents: i64,
    pub discount_cents: i64,
    pub total_cents: i64,
    pub currency: String,
    pub due_date: chrono::NaiveDate,
    pub paid_at: Option<DateTime<Utc>>,
    pub notes: Option<String>,
    pub line_items_count: i32,
    pub metadata: serde_json::Value,
    pub issued_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// InvoiceItem represents a single line item on an invoice
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct InvoiceItem {
    pub id: Uuid,
    pub invoice_id: Uuid,
    pub description: String,
    pub quantity: f64,
    pub unit_price_cents: i64,
    pub amount_cents: i64,
    pub product_id: Option<Uuid>,
    pub subscription_plan_id: Option<Uuid>,
    pub period_start: Option<chrono::NaiveDate>,
    pub period_end: Option<chrono::NaiveDate>,
    pub metadata: serde_json::Value,
}

/// Payment records a financial transaction against an invoice or order
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Payment {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub user_id: Uuid,
    pub invoice_id: Option<Uuid>,
    pub order_id: Option<Uuid>,
    pub amount_cents: i64,
    pub currency: String,
    pub status: String,
    pub method: String,
    pub gateway_transaction_id: Option<String>,
    pub gateway_response: serde_json::Value,
    pub failure_reason: Option<String>,
    pub idempotency_key: Option<String>,
    pub processed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Refund records a partial or full reversal of a payment
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Refund {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub payment_id: Uuid,
    pub order_id: Option<Uuid>,
    pub amount_cents: i64,
    pub reason: String,
    pub status: String,
    pub refunded_by: Option<Uuid>,
    pub gateway_refund_id: Option<String>,
    pub notes: Option<String>,
    pub requested_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// LedgerEntry records a single debit or credit in the general ledger
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct LedgerEntry {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub transaction_id: Uuid,
    pub entry_type: String,
    pub account_code: String,
    pub account_name: String,
    pub amount_cents: i64,
    pub currency: String,
    pub description: Option<String>,
    pub reference_type: Option<String>,
    pub reference_id: Option<Uuid>,
    pub metadata: serde_json::Value,
    pub posted_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}
