// PostgreSQL Database Layer
//
// Database struct with connection pooling, schema setup, seeding,
// and all query methods for analytics workloads.

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use rand::Rng;
use serde_json::{json, Value};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::time::Duration as StdDuration;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    config::Config,
    generators::DataGenerator,
    models::{
        AnalyticsOverview, CartLineItemDetail, CartSnapshot, CatalogProduct, CheckoutReceipt,
        Event, EventTypeDistribution, HourlyMetrics, Organization, PagePerformance, TopPage,
        UserActivity,
    },
};

/// Database provides all PostgreSQL operations with connection pooling
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Get a reference to the underlying connection pool for direct SQL queries
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Create a new database connection with optimized pool settings for 10K+ QPS
    pub async fn new(database_url: &str, pool_size: u32) -> Result<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(pool_size)
            .min_connections(pool_size / 2)
            .acquire_timeout(StdDuration::from_secs(5))
            .idle_timeout(StdDuration::from_secs(600))
            .max_lifetime(StdDuration::from_secs(1800))
            .connect(database_url)
            .await?;

        Ok(Self { pool })
    }

    /// Setup database schema with proper indexing for analytics workloads
    pub async fn setup_schema(&self) -> Result<()> {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS organizations (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            name VARCHAR NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            email VARCHAR UNIQUE NOT NULL,
            name VARCHAR NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS events (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            user_id UUID REFERENCES users(id),
            event_type VARCHAR NOT NULL,
            page_url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            ip_address VARCHAR,
            properties JSONB DEFAULT '{}',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        // Enhanced indexes for high-performance queries
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_org_created ON events(organization_id, created_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_type_created ON events(event_type, created_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_user_created ON events(user_id, created_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_page_url ON events(page_url) WHERE page_url IS NOT NULL;")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_users_org ON users(organization_id);")
            .execute(&self.pool)
            .await?;

        // ============================================================
        // ENUM types (using DO $$ block to handle pre-existing types)
        // ============================================================

        sqlx::query("DO $$ BEGIN CREATE TYPE device_type AS ENUM ('desktop', 'mobile', 'tablet', 'bot', 'unknown'); EXCEPTION WHEN duplicate_object THEN null; END $$;")
            .execute(&self.pool)
            .await?;

        sqlx::query("DO $$ BEGIN CREATE TYPE campaign_status AS ENUM ('draft', 'active', 'paused', 'completed', 'archived'); EXCEPTION WHEN duplicate_object THEN null; END $$;")
            .execute(&self.pool)
            .await?;

        sqlx::query("DO $$ BEGIN CREATE TYPE experiment_status AS ENUM ('draft', 'running', 'paused', 'concluded'); EXCEPTION WHEN duplicate_object THEN null; END $$;")
            .execute(&self.pool)
            .await?;

        sqlx::query("DO $$ BEGIN CREATE TYPE goal_type AS ENUM ('page_view', 'event', 'duration', 'pages_per_session', 'revenue'); EXCEPTION WHEN duplicate_object THEN null; END $$;")
            .execute(&self.pool)
            .await?;

        sqlx::query("DO $$ BEGIN CREATE TYPE order_status AS ENUM ('pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded'); EXCEPTION WHEN duplicate_object THEN null; END $$;")
            .execute(&self.pool)
            .await?;

        sqlx::query("DO $$ BEGIN CREATE TYPE cart_status AS ENUM ('active', 'converted', 'abandoned', 'expired'); EXCEPTION WHEN duplicate_object THEN null; END $$;")
            .execute(&self.pool)
            .await?;

        sqlx::query("DO $$ BEGIN CREATE TYPE discount_type AS ENUM ('percentage', 'fixed_amount', 'buy_x_get_y', 'free_shipping'); EXCEPTION WHEN duplicate_object THEN null; END $$;")
            .execute(&self.pool)
            .await?;

        sqlx::query("DO $$ BEGIN CREATE TYPE payment_status AS ENUM ('pending', 'processing', 'completed', 'failed', 'refunded', 'disputed'); EXCEPTION WHEN duplicate_object THEN null; END $$;")
            .execute(&self.pool)
            .await?;

        sqlx::query("DO $$ BEGIN CREATE TYPE payment_method AS ENUM ('credit_card', 'debit_card', 'bank_transfer', 'paypal', 'stripe', 'crypto', 'invoice'); EXCEPTION WHEN duplicate_object THEN null; END $$;")
            .execute(&self.pool)
            .await?;

        sqlx::query("DO $$ BEGIN CREATE TYPE subscription_status AS ENUM ('trialing', 'active', 'past_due', 'cancelled', 'expired', 'paused'); EXCEPTION WHEN duplicate_object THEN null; END $$;")
            .execute(&self.pool)
            .await?;

        sqlx::query("DO $$ BEGIN CREATE TYPE invoice_status AS ENUM ('draft', 'sent', 'paid', 'overdue', 'void', 'disputed'); EXCEPTION WHEN duplicate_object THEN null; END $$;")
            .execute(&self.pool)
            .await?;

        sqlx::query("DO $$ BEGIN CREATE TYPE ledger_entry_type AS ENUM ('debit', 'credit'); EXCEPTION WHEN duplicate_object THEN null; END $$;")
            .execute(&self.pool)
            .await?;

        sqlx::query("DO $$ BEGIN CREATE TYPE refund_status AS ENUM ('requested', 'processing', 'approved', 'completed', 'rejected'); EXCEPTION WHEN duplicate_object THEN null; END $$;")
            .execute(&self.pool)
            .await?;

        // ============================================================
        // Analytics domain tables
        // ============================================================

        // 1. sessions
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS sessions (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            user_id UUID REFERENCES users(id),
            session_token VARCHAR(128),
            device device_type DEFAULT 'unknown',
            browser VARCHAR(64),
            os VARCHAR(64),
            screen_resolution VARCHAR(16),
            country_code CHAR(2),
            city VARCHAR(128),
            ip_address INET,
            landing_page VARCHAR(512),
            exit_page VARCHAR(512),
            page_count INT DEFAULT 0,
            duration_seconds INT,
            is_bounce BOOLEAN DEFAULT false,
            utm_source VARCHAR(128),
            utm_medium VARCHAR(128),
            utm_campaign VARCHAR(128),
            utm_content VARCHAR(128),
            started_at TIMESTAMPTZ,
            ended_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_sessions_org_started ON sessions(organization_id, started_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_sessions_user_started ON sessions(user_id, started_at DESC) WHERE user_id IS NOT NULL;")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_sessions_org_device ON sessions(organization_id, device);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_sessions_org_country ON sessions(organization_id, country_code) WHERE country_code IS NOT NULL;")
            .execute(&self.pool)
            .await?;

        // 2. campaigns
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS campaigns (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            name VARCHAR(256) NOT NULL,
            status campaign_status DEFAULT 'draft',
            channel VARCHAR(64),
            budget_cents BIGINT DEFAULT 0,
            spent_cents BIGINT DEFAULT 0,
            target_audience JSONB DEFAULT '{}',
            tags TEXT[] DEFAULT '{}',
            click_count INT DEFAULT 0,
            impression_count INT DEFAULT 0,
            conversion_count INT DEFAULT 0,
            starts_at TIMESTAMPTZ,
            ends_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_campaigns_org_status ON campaigns(organization_id, status);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_campaigns_org_channel ON campaigns(organization_id, channel);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_campaigns_tags ON campaigns USING GIN(tags);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_campaigns_target_audience ON campaigns USING GIN(target_audience);")
            .execute(&self.pool)
            .await?;

        // 3. experiments
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS experiments (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            name VARCHAR(256) NOT NULL,
            description TEXT,
            status experiment_status DEFAULT 'draft',
            hypothesis TEXT,
            variants JSONB,
            metric_name VARCHAR(128),
            baseline_rate NUMERIC(8,4),
            sample_size_target INT,
            confidence_level NUMERIC(4,3) DEFAULT 0.95,
            winning_variant VARCHAR(64),
            results JSONB DEFAULT '{}',
            started_at TIMESTAMPTZ,
            concluded_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_experiments_org_status ON experiments(organization_id, status);")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_experiments_results ON experiments USING GIN(results);",
        )
        .execute(&self.pool)
        .await?;

        // 4. experiment_assignments
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS experiment_assignments (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            experiment_id UUID NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            user_id UUID NOT NULL REFERENCES users(id),
            variant VARCHAR(64) NOT NULL,
            converted BOOLEAN DEFAULT false,
            conversion_value NUMERIC(12,2),
            assigned_at TIMESTAMPTZ DEFAULT NOW(),
            converted_at TIMESTAMPTZ,
            UNIQUE(experiment_id, user_id)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_exp_assign_exp_variant ON experiment_assignments(experiment_id, variant);")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_exp_assign_user ON experiment_assignments(user_id);",
        )
        .execute(&self.pool)
        .await?;

        // 5. page_views
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS page_views (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            session_id UUID REFERENCES sessions(id),
            user_id UUID REFERENCES users(id),
            event_id UUID REFERENCES events(id),
            page_url VARCHAR(512) NOT NULL,
            page_title VARCHAR(256),
            referrer_url VARCHAR(512),
            time_on_page_ms INT,
            dom_load_ms INT,
            first_paint_ms INT,
            first_contentful_paint_ms INT,
            largest_contentful_paint_ms INT,
            cumulative_layout_shift NUMERIC(6,4),
            viewport_width INT,
            viewport_height INT,
            scroll_depth_pct SMALLINT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_page_views_org_created ON page_views(organization_id, created_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_page_views_session_created ON page_views(session_id, created_at) WHERE session_id IS NOT NULL;")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_page_views_org_url_created ON page_views(organization_id, page_url, created_at DESC);")
            .execute(&self.pool)
            .await?;

        // 6. goals
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS goals (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            name VARCHAR(256) NOT NULL,
            goal_type goal_type NOT NULL,
            target_value NUMERIC(12,2),
            match_pattern VARCHAR(512),
            is_active BOOLEAN DEFAULT true,
            completions_count INT DEFAULT 0,
            total_value NUMERIC(14,2) DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_goals_org_active ON goals(organization_id) WHERE is_active = true;")
            .execute(&self.pool)
            .await?;

        // 7. goal_completions
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS goal_completions (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            goal_id UUID NOT NULL REFERENCES goals(id) ON DELETE CASCADE,
            user_id UUID NOT NULL REFERENCES users(id),
            session_id UUID REFERENCES sessions(id),
            value NUMERIC(12,2),
            properties JSONB DEFAULT '{}',
            completed_at TIMESTAMPTZ DEFAULT NOW()
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_goal_completions_goal_completed ON goal_completions(goal_id, completed_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_goal_completions_org_completed ON goal_completions(organization_id, completed_at DESC);")
            .execute(&self.pool)
            .await?;

        // ============================================================
        // E-commerce domain tables
        // ============================================================

        // 8. product_categories
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS product_categories (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            name VARCHAR(128) NOT NULL,
            slug VARCHAR(128) NOT NULL,
            parent_id UUID REFERENCES product_categories(id),
            depth INT DEFAULT 0,
            path TEXT[] DEFAULT '{}',
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(organization_id, slug)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_product_categories_org ON product_categories(organization_id);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_product_categories_parent ON product_categories(parent_id) WHERE parent_id IS NOT NULL;")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_product_categories_path ON product_categories USING GIN(path);")
            .execute(&self.pool)
            .await?;

        // 9. products
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS products (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            category_id UUID REFERENCES product_categories(id),
            sku VARCHAR(64) NOT NULL,
            name VARCHAR(256) NOT NULL,
            description TEXT,
            price_cents BIGINT NOT NULL,
            compare_at_price_cents BIGINT,
            cost_cents BIGINT,
            currency CHAR(3) DEFAULT 'USD',
            tags TEXT[] DEFAULT '{}',
            attributes JSONB DEFAULT '{}',
            images TEXT[] DEFAULT '{}',
            is_active BOOLEAN DEFAULT true,
            is_digital BOOLEAN DEFAULT false,
            weight_grams INT,
            rating_avg NUMERIC(3,2) DEFAULT 0,
            rating_count INT DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(organization_id, sku),
            CHECK(price_cents > 0)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_products_org_category ON products(organization_id, category_id);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_products_org_active_created ON products(organization_id, is_active, created_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_products_tags ON products USING GIN(tags);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_products_attributes ON products USING GIN(attributes jsonb_path_ops);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_products_org_price ON products(organization_id, price_cents) WHERE is_active;")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_products_org_rating ON products(organization_id, rating_avg DESC) WHERE rating_count >= 5;")
            .execute(&self.pool)
            .await?;

        // 10. product_tags
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS product_tags (
            product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            tag VARCHAR(64) NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY(product_id, tag)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_product_tags_tag ON product_tags(tag);")
            .execute(&self.pool)
            .await?;

        // 11. inventory
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS inventory (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            warehouse_code VARCHAR(32) DEFAULT 'default',
            quantity_on_hand INT DEFAULT 0,
            quantity_reserved INT DEFAULT 0,
            quantity_available INT DEFAULT 0,
            reorder_point INT DEFAULT 10,
            reorder_quantity INT DEFAULT 50,
            is_low_stock BOOLEAN DEFAULT false,
            last_restocked_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(product_id, warehouse_code),
            CHECK(quantity_on_hand >= 0)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_inventory_org ON inventory(organization_id);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_inventory_product ON inventory(product_id);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_inventory_org_low_stock ON inventory(organization_id) WHERE is_low_stock;")
            .execute(&self.pool)
            .await?;

        // 12. coupons
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS coupons (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            code VARCHAR(32) NOT NULL,
            discount_type discount_type NOT NULL,
            discount_value NUMERIC(10,2) NOT NULL,
            min_order_cents BIGINT DEFAULT 0,
            max_discount_cents BIGINT,
            max_uses INT,
            current_uses INT DEFAULT 0,
            applicable_product_ids UUID[] DEFAULT '{}',
            applicable_category_ids UUID[] DEFAULT '{}',
            is_active BOOLEAN DEFAULT true,
            starts_at TIMESTAMPTZ,
            expires_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(organization_id, code)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_coupons_org_code ON coupons(organization_id, code);",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_coupons_product_ids ON coupons USING GIN(applicable_product_ids);")
            .execute(&self.pool)
            .await?;

        // 13. carts
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS carts (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            user_id UUID REFERENCES users(id),
            session_id UUID REFERENCES sessions(id),
            status cart_status DEFAULT 'active',
            coupon_id UUID REFERENCES coupons(id),
            subtotal_cents BIGINT DEFAULT 0,
            discount_cents BIGINT DEFAULT 0,
            total_cents BIGINT DEFAULT 0,
            item_count INT DEFAULT 0,
            metadata JSONB DEFAULT '{}',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            abandoned_at TIMESTAMPTZ
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_carts_org_status ON carts(organization_id, status);",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_carts_user ON carts(user_id) WHERE user_id IS NOT NULL;")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_carts_session ON carts(session_id) WHERE session_id IS NOT NULL;")
            .execute(&self.pool)
            .await?;

        // 14. cart_items
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS cart_items (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            cart_id UUID NOT NULL REFERENCES carts(id) ON DELETE CASCADE,
            product_id UUID NOT NULL REFERENCES products(id),
            quantity INT DEFAULT 1,
            unit_price_cents BIGINT NOT NULL,
            line_total_cents BIGINT DEFAULT 0,
            added_at TIMESTAMPTZ DEFAULT NOW(),
            CHECK(quantity > 0)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_cart_items_cart ON cart_items(cart_id);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_cart_items_product ON cart_items(product_id);")
            .execute(&self.pool)
            .await?;

        // 15. orders
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS orders (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            user_id UUID NOT NULL REFERENCES users(id),
            cart_id UUID REFERENCES carts(id),
            order_number VARCHAR(32) NOT NULL,
            status order_status DEFAULT 'pending',
            subtotal_cents BIGINT NOT NULL,
            discount_cents BIGINT DEFAULT 0,
            tax_cents BIGINT DEFAULT 0,
            shipping_cents BIGINT DEFAULT 0,
            total_cents BIGINT NOT NULL,
            currency CHAR(3) DEFAULT 'USD',
            coupon_id UUID REFERENCES coupons(id),
            shipping_address JSONB,
            billing_address JSONB,
            notes TEXT,
            metadata JSONB DEFAULT '{}',
            placed_at TIMESTAMPTZ,
            shipped_at TIMESTAMPTZ,
            delivered_at TIMESTAMPTZ,
            cancelled_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(organization_id, order_number),
            CHECK(total_cents >= 0)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_orders_org_placed ON orders(organization_id, placed_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_orders_user_placed ON orders(user_id, placed_at DESC);",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_orders_org_status ON orders(organization_id, status);",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_orders_coupon ON orders(coupon_id) WHERE coupon_id IS NOT NULL;")
            .execute(&self.pool)
            .await?;

        // 16. order_items
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS order_items (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
            product_id UUID NOT NULL REFERENCES products(id),
            product_name VARCHAR(256) NOT NULL,
            sku VARCHAR(64),
            quantity INT NOT NULL,
            unit_price_cents BIGINT NOT NULL,
            discount_cents BIGINT DEFAULT 0,
            line_total_cents BIGINT DEFAULT 0,
            metadata JSONB DEFAULT '{}',
            CHECK(quantity > 0)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id);",
        )
        .execute(&self.pool)
        .await?;

        // 17. reviews
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS reviews (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            user_id UUID NOT NULL REFERENCES users(id),
            order_id UUID REFERENCES orders(id),
            rating SMALLINT NOT NULL,
            title VARCHAR(256),
            body TEXT,
            is_verified_purchase BOOLEAN DEFAULT false,
            helpful_count INT DEFAULT 0,
            reported BOOLEAN DEFAULT false,
            metadata JSONB DEFAULT '{}',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            CHECK(rating BETWEEN 1 AND 5),
            UNIQUE(product_id, user_id)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_reviews_product_rating ON reviews(product_id, rating DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_reviews_user ON reviews(user_id);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_reviews_org_created ON reviews(organization_id, created_at DESC);")
            .execute(&self.pool)
            .await?;

        // ============================================================
        // Finance domain tables
        // ============================================================

        // 18. subscription_plans
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS subscription_plans (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            name VARCHAR(128) NOT NULL,
            slug VARCHAR(64) NOT NULL,
            description TEXT,
            price_cents BIGINT NOT NULL,
            currency CHAR(3) DEFAULT 'USD',
            interval VARCHAR(16) DEFAULT 'monthly',
            interval_count INT DEFAULT 1,
            trial_days INT DEFAULT 0,
            features JSONB DEFAULT '[]',
            limits JSONB DEFAULT '{}',
            is_active BOOLEAN DEFAULT true,
            sort_order INT DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(organization_id, slug),
            CHECK(price_cents >= 0)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_subscription_plans_org_sort ON subscription_plans(organization_id, sort_order) WHERE is_active;")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_subscription_plans_features ON subscription_plans USING GIN(features);")
            .execute(&self.pool)
            .await?;

        // 19. subscriptions
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS subscriptions (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            user_id UUID NOT NULL REFERENCES users(id),
            plan_id UUID NOT NULL REFERENCES subscription_plans(id),
            status subscription_status DEFAULT 'trialing',
            current_period_start TIMESTAMPTZ,
            current_period_end TIMESTAMPTZ,
            trial_end TIMESTAMPTZ,
            cancelled_at TIMESTAMPTZ,
            cancel_reason TEXT,
            metadata JSONB DEFAULT '{}',
            mrr_cents BIGINT DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_subscriptions_org_status ON subscriptions(organization_id, status);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_subscriptions_user ON subscriptions(user_id);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_subscriptions_plan ON subscriptions(plan_id);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_subscriptions_org_mrr ON subscriptions(organization_id, mrr_cents DESC) WHERE status IN ('active', 'trialing');")
            .execute(&self.pool)
            .await?;

        // 20. subscription_events
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS subscription_events (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            subscription_id UUID NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
            event_type VARCHAR(32) NOT NULL,
            from_plan_id UUID REFERENCES subscription_plans(id),
            to_plan_id UUID REFERENCES subscription_plans(id),
            mrr_delta_cents BIGINT DEFAULT 0,
            metadata JSONB DEFAULT '{}',
            occurred_at TIMESTAMPTZ DEFAULT NOW()
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_sub_events_sub_occurred ON subscription_events(subscription_id, occurred_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_sub_events_org_type_occurred ON subscription_events(organization_id, event_type, occurred_at DESC);")
            .execute(&self.pool)
            .await?;

        // 21. accounts
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS accounts (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            code VARCHAR(16) NOT NULL,
            name VARCHAR(128) NOT NULL,
            account_type VARCHAR(32) NOT NULL,
            parent_code VARCHAR(16),
            is_active BOOLEAN DEFAULT true,
            normal_balance ledger_entry_type NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(organization_id, code)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_accounts_org_type ON accounts(organization_id, account_type);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_accounts_org_parent ON accounts(organization_id, parent_code) WHERE parent_code IS NOT NULL;")
            .execute(&self.pool)
            .await?;

        // 22. invoices
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS invoices (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            user_id UUID NOT NULL REFERENCES users(id),
            subscription_id UUID REFERENCES subscriptions(id),
            order_id UUID REFERENCES orders(id),
            invoice_number VARCHAR(32) NOT NULL,
            status invoice_status DEFAULT 'draft',
            subtotal_cents BIGINT NOT NULL,
            tax_cents BIGINT DEFAULT 0,
            discount_cents BIGINT DEFAULT 0,
            total_cents BIGINT NOT NULL,
            currency CHAR(3) DEFAULT 'USD',
            due_date DATE,
            paid_at TIMESTAMPTZ,
            notes TEXT,
            line_items_count INT DEFAULT 0,
            metadata JSONB DEFAULT '{}',
            issued_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(organization_id, invoice_number),
            CHECK(total_cents >= 0)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_invoices_org_status ON invoices(organization_id, status);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_invoices_user ON invoices(user_id);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_invoices_subscription ON invoices(subscription_id) WHERE subscription_id IS NOT NULL;")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_invoices_order ON invoices(order_id) WHERE order_id IS NOT NULL;")
            .execute(&self.pool)
            .await?;

        // 23. invoice_items
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS invoice_items (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            invoice_id UUID NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
            description VARCHAR(256) NOT NULL,
            quantity NUMERIC(10,2) DEFAULT 1,
            unit_price_cents BIGINT NOT NULL,
            amount_cents BIGINT DEFAULT 0,
            product_id UUID REFERENCES products(id),
            subscription_plan_id UUID REFERENCES subscription_plans(id),
            period_start DATE,
            period_end DATE,
            metadata JSONB DEFAULT '{}',
            CHECK(quantity > 0)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_invoice_items_invoice ON invoice_items(invoice_id);",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_invoice_items_product ON invoice_items(product_id) WHERE product_id IS NOT NULL;")
            .execute(&self.pool)
            .await?;

        // 24. payments
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS payments (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            user_id UUID NOT NULL REFERENCES users(id),
            invoice_id UUID REFERENCES invoices(id),
            order_id UUID REFERENCES orders(id),
            amount_cents BIGINT NOT NULL,
            currency CHAR(3) DEFAULT 'USD',
            status payment_status DEFAULT 'pending',
            method payment_method NOT NULL,
            gateway_transaction_id VARCHAR(128),
            gateway_response JSONB DEFAULT '{}',
            failure_reason TEXT,
            idempotency_key VARCHAR(128) UNIQUE,
            processed_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            CHECK(amount_cents > 0)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_payments_org_status_created ON payments(organization_id, status, created_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_payments_user_created ON payments(user_id, created_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_payments_invoice ON payments(invoice_id) WHERE invoice_id IS NOT NULL;")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(order_id) WHERE order_id IS NOT NULL;")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_payments_org_method ON payments(organization_id, method);")
            .execute(&self.pool)
            .await?;

        // 25. refunds
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS refunds (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            payment_id UUID NOT NULL REFERENCES payments(id),
            order_id UUID REFERENCES orders(id),
            amount_cents BIGINT NOT NULL,
            reason TEXT,
            status refund_status DEFAULT 'requested',
            refunded_by UUID REFERENCES users(id),
            gateway_refund_id VARCHAR(128),
            notes TEXT,
            requested_at TIMESTAMPTZ DEFAULT NOW(),
            processed_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            CHECK(amount_cents > 0)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_refunds_org_requested ON refunds(organization_id, requested_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_refunds_payment ON refunds(payment_id);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_refunds_order ON refunds(order_id) WHERE order_id IS NOT NULL;")
            .execute(&self.pool)
            .await?;

        // 26. ledger_entries (table 26 but part of the 25 new tables including sessions through ledger_entries)
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS ledger_entries (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            transaction_id UUID NOT NULL,
            entry_type ledger_entry_type NOT NULL,
            account_code VARCHAR(16) NOT NULL,
            account_name VARCHAR(128) NOT NULL,
            amount_cents BIGINT NOT NULL,
            currency CHAR(3) DEFAULT 'USD',
            description TEXT,
            reference_type VARCHAR(32),
            reference_id UUID,
            metadata JSONB DEFAULT '{}',
            posted_at TIMESTAMPTZ DEFAULT NOW(),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            CHECK(amount_cents > 0)
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_ledger_entries_org_posted ON ledger_entries(organization_id, posted_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_ledger_entries_transaction ON ledger_entries(transaction_id);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_ledger_entries_org_account_posted ON ledger_entries(organization_id, account_code, posted_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_ledger_entries_ref ON ledger_entries(reference_type, reference_id);")
            .execute(&self.pool)
            .await?;

        // ============================================================
        // Materialized views
        // ============================================================

        // mv_daily_revenue_summary - may fail if orders table is empty, that is OK
        if let Err(e) = sqlx::query(
            r#"
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_revenue_summary AS
        SELECT
            o.id AS organization_id,
            DATE(ord.placed_at) AS revenue_date,
            COUNT(DISTINCT ord.id) AS order_count,
            COUNT(DISTINCT ord.user_id) AS unique_buyers,
            SUM(ord.total_cents) AS gross_revenue_cents,
            SUM(ord.discount_cents) AS total_discount_cents,
            SUM(ord.tax_cents) AS total_tax_cents,
            AVG(ord.total_cents) AS avg_order_value_cents
        FROM organizations o
        JOIN orders ord ON ord.organization_id = o.id
        WHERE ord.status NOT IN ('cancelled')
        GROUP BY o.id, DATE(ord.placed_at);
        "#,
        )
        .execute(&self.pool)
        .await
        {
            warn!(
                "Could not create mv_daily_revenue_summary (may already exist or tables empty): {}",
                e
            );
        }

        if let Err(e) = sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_revenue_org_date ON mv_daily_revenue_summary(organization_id, revenue_date);")
            .execute(&self.pool)
            .await
        {
            warn!("Could not create index on mv_daily_revenue_summary: {}", e);
        }

        // mv_campaign_performance - may fail if campaigns/sessions tables are empty, that is OK
        if let Err(e) = sqlx::query(r#"
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_campaign_performance AS
        SELECT
            c.id AS campaign_id,
            c.organization_id,
            c.name AS campaign_name,
            c.channel,
            c.budget_cents,
            c.spent_cents,
            c.click_count,
            c.impression_count,
            c.conversion_count,
            COUNT(DISTINCT s.id) AS session_count,
            COUNT(DISTINCT s.user_id) AS unique_visitors
        FROM campaigns c
        LEFT JOIN sessions s ON s.organization_id = c.organization_id
            AND s.utm_campaign = c.name
        GROUP BY c.id, c.organization_id, c.name, c.channel, c.budget_cents, c.spent_cents, c.click_count, c.impression_count, c.conversion_count;
        "#)
            .execute(&self.pool)
            .await
        {
            warn!("Could not create mv_campaign_performance (may already exist or tables empty): {}", e);
        }

        if let Err(e) = sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_campaign_performance_id ON mv_campaign_performance(campaign_id);")
            .execute(&self.pool)
            .await
        {
            warn!("Could not create index on mv_campaign_performance: {}", e);
        }

        info!("Database schema setup complete");
        Ok(())
    }

    /// Seed the database with initial organizations and users
    pub async fn seed_initial_data(
        &self,
        generator: &DataGenerator,
        config: &Config,
    ) -> Result<()> {
        info!("Seeding initial data...");

        // Phase 1: Ensure organizations exist (load existing or create new)
        let existing_org_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM organizations")
            .fetch_one(&self.pool)
            .await?;

        let orgs: Vec<Organization> = if existing_org_count > 0 {
            info!(
                "Loading {} existing organizations from database",
                existing_org_count
            );
            sqlx::query_as(
                "SELECT id, name, created_at FROM organizations ORDER BY created_at LIMIT $1",
            )
            .bind(config.organizations as i64)
            .fetch_all(&self.pool)
            .await?
        } else {
            info!("Creating {} organizations...", config.organizations);
            let mut new_orgs = Vec::new();
            for _ in 0..config.organizations {
                let org = generator.generate_organization();
                sqlx::query("INSERT INTO organizations (id, name, created_at) VALUES ($1, $2, $3)")
                    .bind(org.id)
                    .bind(&org.name)
                    .bind(org.created_at)
                    .execute(&self.pool)
                    .await?;
                new_orgs.push(org);
            }
            new_orgs
        };

        for (org_index, org) in orgs.into_iter().enumerate() {
            // Phase 2: Ensure users exist for this org
            let existing_user_count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE organization_id = $1")
                    .bind(org.id)
                    .fetch_one(&self.pool)
                    .await?;

            let user_ids: Vec<Uuid> = if existing_user_count > 0 {
                info!(
                    "Loading {} existing users for organization {} ({})",
                    existing_user_count,
                    org_index + 1,
                    org.name
                );
                sqlx::query_scalar("SELECT id FROM users WHERE organization_id = $1 LIMIT $2")
                    .bind(org.id)
                    .bind(config.users_per_org as i64)
                    .fetch_all(&self.pool)
                    .await?
            } else {
                let users = generator.generate_users(org.id, config.users_per_org as usize);
                info!(
                    "Inserting {} users for organization {} ({})",
                    users.len(),
                    org_index + 1,
                    org.name
                );
                for user in users.iter() {
                    if let Err(e) = sqlx::query("INSERT INTO users (id, organization_id, email, name, created_at) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (email) DO NOTHING")
                        .bind(user.id)
                        .bind(user.organization_id)
                        .bind(&user.email)
                        .bind(&user.name)
                        .bind(user.created_at)
                        .execute(&self.pool)
                        .await
                    {
                        error!("Failed to insert user {}: {}", user.email, e);
                    }
                }
                users.iter().map(|u| u.id).collect()
            };

            // Phase 3: Check if domain data already exists for this org
            let has_domain_data: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM sessions WHERE organization_id = $1")
                    .bind(org.id)
                    .fetch_one(&self.pool)
                    .await?;

            if has_domain_data > 0 {
                info!(
                    "Organization {} ({}) already has domain data, skipping",
                    org_index + 1,
                    org.name
                );
                continue;
            }

            info!(
                "Seeding domain tables for organization {} ({})",
                org_index + 1,
                org.name
            );
            let now = Utc::now();
            let mut rng = rand::thread_rng();

            // ----------------------------------------------------------
            // 1. Sessions (5-10 per org)
            // ----------------------------------------------------------
            let session_count = rng.gen_range(5..=10);
            let mut session_ids: Vec<Uuid> = Vec::new();
            let devices = ["desktop", "mobile", "tablet", "bot", "unknown"];
            let browsers = ["Chrome", "Firefox", "Safari", "Edge"];
            let countries = ["US", "GB", "DE", "FR", "JP", "AU", "CA"];

            for s_idx in 0..session_count {
                let session_id = Uuid::new_v4();
                session_ids.push(session_id);
                let user_id = if !user_ids.is_empty() && rng.gen_bool(0.8) {
                    Some(user_ids[rng.gen_range(0..user_ids.len())])
                } else {
                    None
                };
                let device = devices[rng.gen_range(0..devices.len())];
                let browser = browsers[rng.gen_range(0..browsers.len())];
                let country = countries[rng.gen_range(0..countries.len())];
                let started = now - Duration::hours(rng.gen_range(1..72));
                let duration_secs: i32 = rng.gen_range(10..1800);
                let page_count: i32 = rng.gen_range(1..15);
                let is_bounce = page_count == 1;

                if let Err(e) = sqlx::query(
                    r#"INSERT INTO sessions (id, organization_id, user_id, session_token, device, browser, os, country_code, landing_page, page_count, duration_seconds, is_bounce, utm_source, utm_medium, started_at, ended_at, created_at)
                       VALUES ($1, $2, $3, $4, $5::device_type, $6, 'Linux', $7, '/dashboard', $8, $9, $10, 'google', 'cpc', $11, $12, $13)"#)
                    .bind(session_id)
                    .bind(org.id)
                    .bind(user_id)
                    .bind(format!("sess_{}_{}", org_index, s_idx))
                    .bind(device)
                    .bind(browser)
                    .bind(country)
                    .bind(page_count)
                    .bind(duration_secs)
                    .bind(is_bounce)
                    .bind(started)
                    .bind(started + Duration::seconds(duration_secs as i64))
                    .bind(now)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert session for org {}: {}", org.name, e);
                }
            }

            // ----------------------------------------------------------
            // 2. Campaigns (2-3 per org)
            // ----------------------------------------------------------
            let campaign_count = rng.gen_range(2..=3);
            let campaign_names = [
                "Summer Sale 2024",
                "Black Friday Blast",
                "Spring Launch",
                "Holiday Special",
            ];
            let channels = ["email", "social", "search", "display"];

            for c_idx in 0..campaign_count {
                let campaign_id = Uuid::new_v4();
                let name = campaign_names[c_idx % campaign_names.len()];
                let channel = channels[c_idx % channels.len()];
                let clicks: i32 = rng.gen_range(100..5000);
                let impressions: i32 = rng.gen_range(5000..100000);
                let conversions: i32 = rng.gen_range(10..clicks);

                if let Err(e) = sqlx::query(
                    r#"INSERT INTO campaigns (id, organization_id, name, status, channel, budget_cents, spent_cents, target_audience, tags, click_count, impression_count, conversion_count, starts_at, ends_at, created_at, updated_at)
                       VALUES ($1, $2, $3, 'active'::campaign_status, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14)"#)
                    .bind(campaign_id)
                    .bind(org.id)
                    .bind(name)
                    .bind(channel)
                    .bind(rng.gen_range(100000_i64..500000))
                    .bind(rng.gen_range(50000_i64..200000))
                    .bind(json!({"age_range": "25-54", "interests": ["tech", "shopping"]}))
                    .bind(vec!["promo".to_string(), channel.to_string()])
                    .bind(clicks)
                    .bind(impressions)
                    .bind(conversions)
                    .bind(now - Duration::days(30))
                    .bind(now + Duration::days(30))
                    .bind(now)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert campaign for org {}: {}", org.name, e);
                }
            }

            // ----------------------------------------------------------
            // 3. Experiments (1-2 per org)
            // ----------------------------------------------------------
            let experiment_count = rng.gen_range(1..=2);
            let mut experiment_ids: Vec<Uuid> = Vec::new();

            for ex_idx in 0..experiment_count {
                let experiment_id = Uuid::new_v4();
                experiment_ids.push(experiment_id);
                let exp_name = format!("Experiment {} - Org {}", ex_idx + 1, org_index + 1);
                let status = if ex_idx == 0 { "running" } else { "concluded" };

                if let Err(e) = sqlx::query(
                    r#"INSERT INTO experiments (id, organization_id, name, description, status, hypothesis, variants, metric_name, baseline_rate, sample_size_target, confidence_level, started_at, created_at, updated_at)
                       VALUES ($1, $2, $3, 'Testing conversion optimization', $4::experiment_status, 'New variant increases conversion by 10%', $5, 'conversion_rate', 0.05, 1000, 0.95, $6, $7, $7)"#)
                    .bind(experiment_id)
                    .bind(org.id)
                    .bind(&exp_name)
                    .bind(status)
                    .bind(json!({"control": {"name": "Control", "weight": 50}, "variant_a": {"name": "Variant A", "weight": 50}}))
                    .bind(now - Duration::days(14))
                    .bind(now)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert experiment for org {}: {}", org.name, e);
                }

                // Insert experiment assignments for a few users
                let assign_count = std::cmp::min(3, user_ids.len());
                for (a_idx, user_id) in user_ids.iter().take(assign_count).enumerate() {
                    let variant = if a_idx % 2 == 0 {
                        "control"
                    } else {
                        "variant_a"
                    };
                    let converted = rng.gen_bool(0.3);
                    if let Err(e) = sqlx::query(
                        r#"INSERT INTO experiment_assignments (id, organization_id, experiment_id, user_id, variant, converted, conversion_value, assigned_at, converted_at)
                           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                           ON CONFLICT (experiment_id, user_id) DO NOTHING"#)
                        .bind(Uuid::new_v4())
                        .bind(org.id)
                        .bind(experiment_id)
                        .bind(*user_id)
                        .bind(variant)
                        .bind(converted)
                        .bind(if converted { Some(rng.gen_range(10.0..500.0)) } else { None::<f64> })
                        .bind(now - Duration::days(rng.gen_range(1..14)))
                        .bind(if converted { Some(now - Duration::days(rng.gen_range(0..7))) } else { None::<DateTime<Utc>> })
                        .execute(&self.pool)
                        .await
                    {
                        error!("Failed to insert experiment assignment for org {}: {}", org.name, e);
                    }
                }
            }

            // ----------------------------------------------------------
            // 4. Goals (2-3 per org)
            // ----------------------------------------------------------
            let goal_count = rng.gen_range(2..=3);
            let mut goal_ids: Vec<Uuid> = Vec::new();
            let goal_defs = [
                ("Sign-up Goal", "event", "/signup"),
                ("Revenue Goal", "revenue", "/checkout"),
                ("Engagement Goal", "pages_per_session", "/dashboard"),
            ];

            for g_idx in 0..goal_count {
                let goal_id = Uuid::new_v4();
                goal_ids.push(goal_id);
                let (gname, gtype, gpattern) = goal_defs[g_idx % goal_defs.len()];
                if let Err(e) = sqlx::query(
                    r#"INSERT INTO goals (id, organization_id, name, goal_type, target_value, match_pattern, is_active, completions_count, total_value, created_at, updated_at)
                       VALUES ($1, $2, $3, $4::goal_type, $5, $6, true, $7, $8, $9, $9)"#)
                    .bind(goal_id)
                    .bind(org.id)
                    .bind(gname)
                    .bind(gtype)
                    .bind(rng.gen_range(10.0..1000.0))
                    .bind(gpattern)
                    .bind(rng.gen_range(5..200_i32))
                    .bind(rng.gen_range(100.0..50000.0))
                    .bind(now)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert goal for org {}: {}", org.name, e);
                }
            }

            // ----------------------------------------------------------
            // 4b. Goal Completions (2-5 per goal)
            // ----------------------------------------------------------
            for &goal_id in &goal_ids {
                let completion_count = rng.gen_range(2..=5);
                for _ in 0..completion_count {
                    let uid = user_ids[rng.gen_range(0..user_ids.len())];
                    let sid = if !session_ids.is_empty() {
                        Some(session_ids[rng.gen_range(0..session_ids.len())])
                    } else {
                        None
                    };
                    if let Err(e) = sqlx::query(
                        r#"INSERT INTO goal_completions (id, organization_id, goal_id, user_id, session_id, value, properties, completed_at)
                           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#)
                        .bind(Uuid::new_v4())
                        .bind(org.id)
                        .bind(goal_id)
                        .bind(uid)
                        .bind(sid)
                        .bind(rng.gen_range(1.0..500.0))
                        .bind(json!({"source": "seed"}))
                        .bind(now - Duration::hours(rng.gen_range(1..72)))
                        .execute(&self.pool)
                        .await
                    {
                        error!("Failed to insert goal completion for org {}: {}", org.name, e);
                    }
                }
            }

            // ----------------------------------------------------------
            // 5. Product categories (3-5 per org, some with parent_id)
            // ----------------------------------------------------------
            let cat_defs = [
                ("Electronics", "electronics"),
                ("Clothing", "clothing"),
                ("Home & Garden", "home-garden"),
                ("Books", "books"),
                ("Sports", "sports"),
            ];
            let cat_count = rng.gen_range(3..=5);
            let mut category_ids: Vec<Uuid> = Vec::new();

            for cat_idx in 0..cat_count {
                let cat_id = Uuid::new_v4();
                category_ids.push(cat_id);
                let (cname, cslug) = cat_defs[cat_idx % cat_defs.len()];
                // Make slug unique per org
                let unique_slug = format!("{}-{}", cslug, org_index);
                let parent_id: Option<Uuid> = if cat_idx >= 2 && !category_ids.is_empty() {
                    Some(category_ids[0]) // child of first category
                } else {
                    None
                };
                let depth: i32 = if parent_id.is_some() { 1 } else { 0 };

                if let Err(e) = sqlx::query(
                    r#"INSERT INTO product_categories (id, organization_id, name, slug, parent_id, depth, path, is_active, created_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, true, $8)"#)
                    .bind(cat_id)
                    .bind(org.id)
                    .bind(cname)
                    .bind(&unique_slug)
                    .bind(parent_id)
                    .bind(depth)
                    .bind(vec![cname.to_string()])
                    .bind(now)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert product category for org {}: {}", org.name, e);
                }
            }

            // ----------------------------------------------------------
            // 6. Products (10-20 per org, linked to categories)
            // ----------------------------------------------------------
            let product_names = [
                "Widget Pro",
                "Gadget Plus",
                "Smart Sensor",
                "Power Bank XL",
                "USB Hub 7-Port",
                "LED Monitor 27\"",
                "Wireless Mouse",
                "Keyboard Mech",
                "Webcam HD",
                "Speaker Mini",
                "Tablet Stand",
                "Phone Case Premium",
                "Laptop Sleeve",
                "Cable Organizer",
                "Desk Lamp",
                "Headphones NC",
                "Charger Fast",
                "Screen Protector",
                "Mouse Pad XL",
                "Docking Station",
            ];
            let product_count = rng.gen_range(10..=20);
            let mut product_ids: Vec<Uuid> = Vec::new();
            let mut product_prices: Vec<i64> = Vec::new();
            let mut product_skus: Vec<String> = Vec::new();

            for p_idx in 0..product_count {
                let product_id = Uuid::new_v4();
                product_ids.push(product_id);
                let price_cents: i64 = rng.gen_range(999..99999);
                product_prices.push(price_cents);
                let sku = format!(
                    "SKU-{}-{}-{}",
                    org_index,
                    p_idx,
                    Uuid::new_v4()
                        .to_string()
                        .chars()
                        .take(6)
                        .collect::<String>()
                );
                product_skus.push(sku.clone());
                let pname = product_names[p_idx % product_names.len()];
                let cat_id = if !category_ids.is_empty() {
                    Some(category_ids[rng.gen_range(0..category_ids.len())])
                } else {
                    None
                };
                let rating_avg: f64 = rng.gen_range(2.0..5.0);
                let rating_count: i32 = rng.gen_range(0..200);

                if let Err(e) = sqlx::query(
                    r#"INSERT INTO products (id, organization_id, category_id, sku, name, description, price_cents, cost_cents, currency, tags, attributes, images, is_active, is_digital, weight_grams, rating_avg, rating_count, created_at, updated_at)
                       VALUES ($1, $2, $3, $4, $5, 'High-quality product description', $6, $7, 'USD', $8, $9, $10, true, false, $11, $12, $13, $14, $14)"#)
                    .bind(product_id)
                    .bind(org.id)
                    .bind(cat_id)
                    .bind(&sku)
                    .bind(pname)
                    .bind(price_cents)
                    .bind(price_cents / 2)
                    .bind(vec!["featured".to_string(), "new".to_string()])
                    .bind(json!({"color": "black", "material": "aluminum"}))
                    .bind(vec!["https://cdn.example.com/img1.jpg".to_string()])
                    .bind(rng.gen_range(100..5000_i32))
                    .bind(rating_avg)
                    .bind(rating_count)
                    .bind(now)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert product for org {}: {}", org.name, e);
                }
            }

            // ----------------------------------------------------------
            // 6b. Product Tags (2-3 per product)
            // ----------------------------------------------------------
            let tag_pool = [
                "new-arrival",
                "bestseller",
                "sale",
                "featured",
                "clearance",
                "limited-edition",
                "eco-friendly",
                "premium",
            ];
            for &prod_id in &product_ids {
                let tag_count = rng.gen_range(2..=3);
                for t_idx in 0..tag_count {
                    let tag = tag_pool[(t_idx + rng.gen_range(0..tag_pool.len())) % tag_pool.len()];
                    if let Err(e) = sqlx::query(
                        r#"INSERT INTO product_tags (product_id, tag, created_at)
                           VALUES ($1, $2, $3)
                           ON CONFLICT (product_id, tag) DO NOTHING"#,
                    )
                    .bind(prod_id)
                    .bind(tag)
                    .bind(now)
                    .execute(&self.pool)
                    .await
                    {
                        error!("Failed to insert product tag for org {}: {}", org.name, e);
                    }
                }
            }

            // ----------------------------------------------------------
            // 7. Inventory (one per product)
            // ----------------------------------------------------------
            for &prod_id in product_ids.iter() {
                let qty_on_hand: i32 = rng.gen_range(0..500);
                let qty_reserved: i32 = if qty_on_hand > 0 {
                    rng.gen_range(0..std::cmp::min(qty_on_hand, 50))
                } else {
                    0
                };
                let qty_available = qty_on_hand - qty_reserved;
                let reorder_point: i32 = rng.gen_range(5..30);
                let is_low_stock = qty_available <= reorder_point;

                if let Err(e) = sqlx::query(
                    r#"INSERT INTO inventory (id, organization_id, product_id, warehouse_code, quantity_on_hand, quantity_reserved, quantity_available, reorder_point, reorder_quantity, is_low_stock, last_restocked_at, created_at, updated_at)
                       VALUES ($1, $2, $3, 'default', $4, $5, $6, $7, 50, $8, $9, $10, $10)"#)
                    .bind(Uuid::new_v4())
                    .bind(org.id)
                    .bind(prod_id)
                    .bind(qty_on_hand)
                    .bind(qty_reserved)
                    .bind(qty_available)
                    .bind(reorder_point)
                    .bind(is_low_stock)
                    .bind(now - Duration::days(rng.gen_range(1..30)))
                    .bind(now)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert inventory for org {}: {}", org.name, e);
                }
            }

            // ----------------------------------------------------------
            // 8. Coupons (2-3 per org)
            // ----------------------------------------------------------
            let coupon_count = rng.gen_range(2..=3);
            let coupon_defs = [
                ("SAVE10", "percentage", 10.0),
                ("FLAT500", "fixed_amount", 500.0),
                ("FREESHIP", "free_shipping", 0.0),
            ];
            let mut coupon_ids: Vec<Uuid> = Vec::new();

            for cp_idx in 0..coupon_count {
                let coupon_id = Uuid::new_v4();
                coupon_ids.push(coupon_id);
                let (code, dtype, dval) = coupon_defs[cp_idx % coupon_defs.len()];
                let unique_code = format!("{}-{}", code, org_index);
                let max_uses: i32 = rng.gen_range(50..500);
                let current_uses: i32 = rng.gen_range(0..max_uses);

                if let Err(e) = sqlx::query(
                    r#"INSERT INTO coupons (id, organization_id, code, discount_type, discount_value, min_order_cents, max_discount_cents, max_uses, current_uses, is_active, starts_at, expires_at, created_at)
                       VALUES ($1, $2, $3, $4::discount_type, $5, 1000, 10000, $6, $7, true, $8, $9, $10)"#)
                    .bind(coupon_id)
                    .bind(org.id)
                    .bind(&unique_code)
                    .bind(dtype)
                    .bind(dval)
                    .bind(max_uses)
                    .bind(current_uses)
                    .bind(now - Duration::days(60))
                    .bind(now + Duration::days(60))
                    .bind(now)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert coupon for org {}: {}", org.name, e);
                }
            }

            // ----------------------------------------------------------
            // 9. Subscription plans (3 per org: free, pro, enterprise)
            // ----------------------------------------------------------
            let plan_defs: Vec<(&str, &str, i64, i32)> = vec![
                ("Free", "free", 0, 0),
                ("Pro", "pro", 2900, 14),
                ("Enterprise", "enterprise", 9900, 30),
            ];
            let mut plan_ids: Vec<Uuid> = Vec::new();
            let mut plan_prices: Vec<i64> = Vec::new();

            for (sort_idx, (pname, pslug, price, trial_days)) in plan_defs.iter().enumerate() {
                let plan_id = Uuid::new_v4();
                plan_ids.push(plan_id);
                plan_prices.push(*price);
                let unique_slug = format!("{}-{}", pslug, org_index);

                if let Err(e) = sqlx::query(
                    r#"INSERT INTO subscription_plans (id, organization_id, name, slug, description, price_cents, currency, interval, interval_count, trial_days, features, limits, is_active, sort_order, created_at, updated_at)
                       VALUES ($1, $2, $3, $4, $5, $6, 'USD', 'monthly', 1, $7, $8, $9, true, $10, $11, $11)"#)
                    .bind(plan_id)
                    .bind(org.id)
                    .bind(pname)
                    .bind(&unique_slug)
                    .bind(format!("{} plan for {}", pname, org.name))
                    .bind(price)
                    .bind(trial_days)
                    .bind(json!(["analytics", "reports", if *price > 0 { "api_access" } else { "basic" }]))
                    .bind(json!({"users": if *price == 0 { 5 } else if *price < 5000 { 50 } else { 500 }, "events_per_month": if *price == 0 { 10000 } else { 1000000 }}))
                    .bind(sort_idx as i32)
                    .bind(now)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert subscription plan for org {}: {}", org.name, e);
                }
            }

            // ----------------------------------------------------------
            // 10. Subscriptions (some users get subscriptions)
            // ----------------------------------------------------------
            let sub_user_count = std::cmp::min(rng.gen_range(2..=5), user_ids.len());
            let mut subscription_ids: Vec<Uuid> = Vec::new();
            let mut subscribed_user_ids: Vec<Uuid> = Vec::new();
            let sub_statuses = ["active", "active", "active", "cancelled", "trialing"];
            let cancel_reasons = [
                "too_expensive",
                "missing_features",
                "switched_competitor",
                "no_longer_needed",
            ];

            for su_idx in 0..sub_user_count {
                let sub_id = Uuid::new_v4();
                subscription_ids.push(sub_id);
                let uid = user_ids[su_idx];
                subscribed_user_ids.push(uid);
                let plan_idx = rng.gen_range(0..plan_ids.len());
                let plan_id = plan_ids[plan_idx];
                let mrr = plan_prices[plan_idx];
                let status = sub_statuses[su_idx % sub_statuses.len()];
                let period_start = now - Duration::days(rng.gen_range(1..30));
                let period_end = period_start + Duration::days(30);
                let cancelled_at: Option<DateTime<Utc>> = if status == "cancelled" {
                    Some(now - Duration::days(rng.gen_range(0..10)))
                } else {
                    None
                };
                let cancel_reason: Option<&str> = if status == "cancelled" {
                    Some(cancel_reasons[rng.gen_range(0..cancel_reasons.len())])
                } else {
                    None
                };

                if let Err(e) = sqlx::query(
                    r#"INSERT INTO subscriptions (id, organization_id, user_id, plan_id, status, current_period_start, current_period_end, cancelled_at, cancel_reason, metadata, mrr_cents, created_at, updated_at)
                       VALUES ($1, $2, $3, $4, $5::subscription_status, $6, $7, $8, $9, $10, $11, $12, $12)"#)
                    .bind(sub_id)
                    .bind(org.id)
                    .bind(uid)
                    .bind(plan_id)
                    .bind(status)
                    .bind(period_start)
                    .bind(period_end)
                    .bind(cancelled_at)
                    .bind(cancel_reason)
                    .bind(json!({}))
                    .bind(mrr)
                    .bind(now)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert subscription for org {}: {}", org.name, e);
                }
            }

            // ----------------------------------------------------------
            // 10b. Subscription Events (1-3 per subscription)
            // ----------------------------------------------------------
            let sub_event_types = ["created", "activated", "renewed", "upgraded", "downgraded"];
            for (se_idx, &sub_id) in subscription_ids.iter().enumerate() {
                let event_count = rng.gen_range(1..=3);
                for ev_i in 0..event_count {
                    let event_type = sub_event_types[(se_idx + ev_i) % sub_event_types.len()];
                    let mrr_delta: i64 = match event_type {
                        "created" | "activated" => rng.gen_range(1000..10000),
                        "renewed" => 0,
                        "upgraded" => rng.gen_range(500..5000),
                        "downgraded" => -rng.gen_range(500..5000),
                        _ => 0,
                    };
                    let from_plan = if event_type == "upgraded" || event_type == "downgraded" {
                        Some(plan_ids[rng.gen_range(0..plan_ids.len())])
                    } else {
                        None
                    };
                    let to_plan = if event_type == "upgraded" || event_type == "downgraded" {
                        Some(plan_ids[rng.gen_range(0..plan_ids.len())])
                    } else {
                        None
                    };
                    if let Err(e) = sqlx::query(
                        r#"INSERT INTO subscription_events (id, organization_id, subscription_id, event_type, from_plan_id, to_plan_id, mrr_delta_cents, metadata, occurred_at)
                           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#)
                        .bind(Uuid::new_v4())
                        .bind(org.id)
                        .bind(sub_id)
                        .bind(event_type)
                        .bind(from_plan)
                        .bind(to_plan)
                        .bind(mrr_delta)
                        .bind(json!({"source": "seed", "batch": se_idx}))
                        .bind(now - Duration::days(rng.gen_range(1..30)))
                        .execute(&self.pool)
                        .await
                    {
                        error!("Failed to insert subscription event for org {}: {}", org.name, e);
                    }
                }
            }

            // ----------------------------------------------------------
            // 11. Invoices (for subscribed users)
            // ----------------------------------------------------------
            let mut invoice_ids: Vec<Uuid> = Vec::new();
            let mut invoice_user_ids: Vec<Uuid> = Vec::new();
            let invoice_statuses = ["paid", "sent", "overdue", "draft"];

            for (inv_idx, &sub_id) in subscription_ids.iter().enumerate() {
                let invoice_id = Uuid::new_v4();
                invoice_ids.push(invoice_id);
                let uid = subscribed_user_ids[inv_idx];
                invoice_user_ids.push(uid);
                let total_cents: i64 = rng.gen_range(1000..15000);
                let tax_cents: i64 = total_cents / 10;
                let subtotal_cents = total_cents - tax_cents;
                let inv_status = invoice_statuses[inv_idx % invoice_statuses.len()];
                let due_date = (now + Duration::days(30)).date_naive();
                let paid_at: Option<DateTime<Utc>> = if inv_status == "paid" {
                    Some(now - Duration::days(rng.gen_range(1..10)))
                } else {
                    None
                };

                if let Err(e) = sqlx::query(
                    r#"INSERT INTO invoices (id, organization_id, user_id, subscription_id, invoice_number, status, subtotal_cents, tax_cents, discount_cents, total_cents, currency, due_date, paid_at, line_items_count, metadata, issued_at, created_at, updated_at)
                       VALUES ($1, $2, $3, $4, $5, $6::invoice_status, $7, $8, 0, $9, 'USD', $10, $11, 1, '{}', $12, $13, $13)"#)
                    .bind(invoice_id)
                    .bind(org.id)
                    .bind(uid)
                    .bind(sub_id)
                    .bind(format!("INV-{}-{}-{}", org_index, inv_idx, Uuid::new_v4().to_string().chars().take(4).collect::<String>()))
                    .bind(inv_status)
                    .bind(subtotal_cents)
                    .bind(tax_cents)
                    .bind(total_cents)
                    .bind(due_date)
                    .bind(paid_at)
                    .bind(now - Duration::days(5))
                    .bind(now)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert invoice for org {}: {}", org.name, e);
                }
            }

            // ----------------------------------------------------------
            // 11b. Invoice Items (1-2 per invoice)
            // ----------------------------------------------------------
            for (ii_idx, &inv_id) in invoice_ids.iter().enumerate() {
                let item_count = rng.gen_range(1..=2);
                for ii_i in 0..item_count {
                    let plan_id = plan_ids[ii_idx % plan_ids.len()];
                    let unit_price: i64 = plan_prices[ii_idx % plan_prices.len()];
                    let quantity: f64 = 1.0;
                    let amount = (unit_price as f64 * quantity) as i64;
                    let period_start = (now - Duration::days(30)).date_naive();
                    let period_end = now.date_naive();
                    let desc = if ii_i == 0 {
                        "Monthly subscription fee".to_string()
                    } else {
                        "Overage charges".to_string()
                    };
                    if let Err(e) = sqlx::query(
                        r#"INSERT INTO invoice_items (id, invoice_id, description, quantity, unit_price_cents, amount_cents, subscription_plan_id, period_start, period_end, metadata)
                           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"#)
                        .bind(Uuid::new_v4())
                        .bind(inv_id)
                        .bind(&desc)
                        .bind(quantity)
                        .bind(unit_price)
                        .bind(amount)
                        .bind(plan_id)
                        .bind(period_start)
                        .bind(period_end)
                        .bind(json!({"line": ii_i + 1}))
                        .execute(&self.pool)
                        .await
                    {
                        error!("Failed to insert invoice item for org {}: {}", org.name, e);
                    }
                }
            }

            // ----------------------------------------------------------
            // 12. Payments (for invoices)
            // ----------------------------------------------------------
            let payment_methods = ["credit_card", "stripe", "paypal", "bank_transfer"];
            let mut payment_ids: Vec<Uuid> = Vec::new();
            let mut payment_amounts: Vec<i64> = Vec::new();

            for (pay_idx, &inv_id) in invoice_ids.iter().enumerate() {
                let payment_id = Uuid::new_v4();
                payment_ids.push(payment_id);
                let amount_cents: i64 = rng.gen_range(1000..15000);
                payment_amounts.push(amount_cents);
                let method = payment_methods[pay_idx % payment_methods.len()];
                let uid = invoice_user_ids[pay_idx];
                let pay_status = if pay_idx % 3 <= 1 {
                    "completed"
                } else {
                    "pending"
                };
                let processed_at: Option<DateTime<Utc>> = if pay_status == "completed" {
                    Some(now - Duration::hours(rng.gen_range(1..48)))
                } else {
                    None
                };

                if let Err(e) = sqlx::query(
                    r#"INSERT INTO payments (id, organization_id, user_id, invoice_id, amount_cents, currency, status, method, gateway_transaction_id, gateway_response, processed_at, created_at, updated_at)
                       VALUES ($1, $2, $3, $4, $5, 'USD', $6::payment_status, $7::payment_method, $8, $9, $10, $11, $11)"#)
                    .bind(payment_id)
                    .bind(org.id)
                    .bind(uid)
                    .bind(inv_id)
                    .bind(amount_cents)
                    .bind(pay_status)
                    .bind(method)
                    .bind(format!("txn_{}", Uuid::new_v4()))
                    .bind(json!({"status": "ok", "gateway": method}))
                    .bind(processed_at)
                    .bind(now)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert payment for org {}: {}", org.name, e);
                }
            }

            // ----------------------------------------------------------
            // 12b. Refunds (1-2 for completed payments)
            // ----------------------------------------------------------
            let refund_reasons = [
                "defective_product",
                "not_as_described",
                "changed_mind",
                "duplicate_charge",
                "service_issue",
            ];
            let refund_statuses = ["requested", "processing", "approved", "completed"];
            for (rf_idx, &pay_id) in payment_ids.iter().enumerate() {
                // Only refund ~50% of payments
                if rng.gen_bool(0.5) {
                    continue;
                }
                let refund_amount = if !payment_amounts.is_empty() {
                    let amt = payment_amounts[rf_idx % payment_amounts.len()];
                    rng.gen_range(1..=amt).max(1)
                } else {
                    rng.gen_range(500..5000_i64)
                };
                let rf_status = refund_statuses[rng.gen_range(0..refund_statuses.len())];
                let processed_at: Option<DateTime<Utc>> =
                    if rf_status == "completed" || rf_status == "approved" {
                        Some(now - Duration::hours(rng.gen_range(1..24)))
                    } else {
                        None
                    };
                let uid = if !user_ids.is_empty() {
                    Some(user_ids[rng.gen_range(0..user_ids.len())])
                } else {
                    None
                };
                if let Err(e) = sqlx::query(
                    r#"INSERT INTO refunds (id, organization_id, payment_id, amount_cents, reason, status, refunded_by, gateway_refund_id, notes, requested_at, processed_at, created_at)
                       VALUES ($1, $2, $3, $4, $5, $6::refund_status, $7, $8, $9, $10, $11, $10)"#)
                    .bind(Uuid::new_v4())
                    .bind(org.id)
                    .bind(pay_id)
                    .bind(refund_amount)
                    .bind(refund_reasons[rng.gen_range(0..refund_reasons.len())])
                    .bind(rf_status)
                    .bind(uid)
                    .bind(format!("rfnd_{}", Uuid::new_v4()))
                    .bind("Seed refund")
                    .bind(now - Duration::hours(rng.gen_range(2..48)))
                    .bind(processed_at)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert refund for org {}: {}", org.name, e);
                }
            }

            // ----------------------------------------------------------
            // 13. Accounts (chart of accounts: 5-8 standard accounts)
            // ----------------------------------------------------------
            let account_defs = [
                ("1000", "Cash", "asset", "debit", None),
                ("1100", "Accounts Receivable", "asset", "debit", None),
                ("2000", "Accounts Payable", "liability", "credit", None),
                ("3000", "Retained Earnings", "equity", "credit", None),
                ("4000", "Revenue", "revenue", "credit", None),
                ("5000", "Cost of Goods Sold", "expense", "debit", None),
                (
                    "5100",
                    "Marketing Expense",
                    "expense",
                    "debit",
                    Some("5000"),
                ),
                (
                    "5200",
                    "Operating Expense",
                    "expense",
                    "debit",
                    Some("5000"),
                ),
            ];

            for (code, aname, atype, normal_balance, parent_code) in &account_defs {
                if let Err(e) = sqlx::query(
                    r#"INSERT INTO accounts (id, organization_id, code, name, account_type, parent_code, is_active, normal_balance, created_at)
                       VALUES ($1, $2, $3, $4, $5, $6, true, $7::ledger_entry_type, $8)
                       ON CONFLICT (organization_id, code) DO NOTHING"#)
                    .bind(Uuid::new_v4())
                    .bind(org.id)
                    .bind(code)
                    .bind(aname)
                    .bind(atype)
                    .bind(parent_code)
                    .bind(normal_balance)
                    .bind(now)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert account {} for org {}: {}", code, org.name, e);
                }
            }

            // ----------------------------------------------------------
            // 14. A few orders with order_items
            // ----------------------------------------------------------
            let order_count = std::cmp::min(rng.gen_range(3..=6), user_ids.len());
            let order_statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"];

            for o_idx in 0..order_count {
                let order_id = Uuid::new_v4();
                let uid = user_ids[o_idx % user_ids.len()];
                let status = order_statuses[o_idx % order_statuses.len()];
                let item_count = rng.gen_range(1..=3_usize);

                // Calculate order totals from items
                let mut subtotal: i64 = 0;
                let mut order_item_data: Vec<(Uuid, i64, i32, String, String)> = Vec::new();

                for _ in 0..item_count {
                    let p_idx = rng.gen_range(0..product_ids.len());
                    let qty: i32 = rng.gen_range(1..=3);
                    let unit_price = product_prices[p_idx];
                    let line_total = unit_price * qty as i64;
                    subtotal += line_total;
                    order_item_data.push((
                        product_ids[p_idx],
                        unit_price,
                        qty,
                        product_names[p_idx % product_names.len()].to_string(),
                        product_skus[p_idx].clone(),
                    ));
                }

                let discount: i64 = if rng.gen_bool(0.3) { subtotal / 10 } else { 0 };
                let tax: i64 = subtotal / 10;
                let shipping: i64 = if subtotal > 5000 { 0 } else { 599 };
                let total = subtotal - discount + tax + shipping;
                let placed_at = now - Duration::days(rng.gen_range(1..30));
                let shipped_at: Option<DateTime<Utc>> =
                    if status == "shipped" || status == "delivered" {
                        Some(placed_at + Duration::days(2))
                    } else {
                        None
                    };
                let delivered_at: Option<DateTime<Utc>> = if status == "delivered" {
                    Some(placed_at + Duration::days(5))
                } else {
                    None
                };
                let cancelled_at: Option<DateTime<Utc>> = if status == "cancelled" {
                    Some(placed_at + Duration::days(1))
                } else {
                    None
                };

                if let Err(e) = sqlx::query(
                    r#"INSERT INTO orders (id, organization_id, user_id, order_number, status, subtotal_cents, discount_cents, tax_cents, shipping_cents, total_cents, currency, shipping_address, billing_address, metadata, placed_at, shipped_at, delivered_at, cancelled_at, created_at, updated_at)
                       VALUES ($1, $2, $3, $4, $5::order_status, $6, $7, $8, $9, $10, 'USD', $11, $12, '{}', $13, $14, $15, $16, $17, $17)"#)
                    .bind(order_id)
                    .bind(org.id)
                    .bind(uid)
                    .bind(format!("ORD-{}-{}", org_index, o_idx))
                    .bind(status)
                    .bind(subtotal)
                    .bind(discount)
                    .bind(tax)
                    .bind(shipping)
                    .bind(total)
                    .bind(json!({"street": "123 Main St", "city": "San Francisco", "state": "CA", "zip": "94102"}))
                    .bind(json!({"street": "123 Main St", "city": "San Francisco", "state": "CA", "zip": "94102"}))
                    .bind(placed_at)
                    .bind(shipped_at)
                    .bind(delivered_at)
                    .bind(cancelled_at)
                    .bind(now)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert order for org {}: {}", org.name, e);
                    continue;
                }

                // Insert order items
                for (prod_id, unit_price, qty, prod_name, prod_sku) in &order_item_data {
                    let line_total = unit_price * (*qty as i64);
                    if let Err(e) = sqlx::query(
                        r#"INSERT INTO order_items (id, order_id, product_id, product_name, sku, quantity, unit_price_cents, discount_cents, line_total_cents, metadata)
                           VALUES ($1, $2, $3, $4, $5, $6, $7, 0, $8, '{}')"#)
                        .bind(Uuid::new_v4())
                        .bind(order_id)
                        .bind(prod_id)
                        .bind(prod_name)
                        .bind(prod_sku)
                        .bind(qty)
                        .bind(unit_price)
                        .bind(line_total)
                        .execute(&self.pool)
                        .await
                    {
                        error!("Failed to insert order item for org {}: {}", org.name, e);
                    }
                }

                // Also create a payment for delivered/confirmed orders
                if status == "delivered" || status == "confirmed" {
                    if let Err(e) = sqlx::query(
                        r#"INSERT INTO payments (id, organization_id, user_id, order_id, amount_cents, currency, status, method, gateway_transaction_id, gateway_response, processed_at, created_at, updated_at)
                           VALUES ($1, $2, $3, $4, $5, 'USD', 'completed'::payment_status, 'credit_card'::payment_method, $6, $7, $8, $9, $9)"#)
                        .bind(Uuid::new_v4())
                        .bind(org.id)
                        .bind(uid)
                        .bind(order_id)
                        .bind(total)
                        .bind(format!("txn_ord_{}", Uuid::new_v4()))
                        .bind(json!({"status": "ok", "type": "order_payment"}))
                        .bind(placed_at + Duration::hours(1))
                        .bind(now)
                        .execute(&self.pool)
                        .await
                    {
                        error!("Failed to insert order payment for org {}: {}", org.name, e);
                    }
                }
            }

            // ----------------------------------------------------------
            // 15. A few reviews
            // ----------------------------------------------------------
            let review_count = std::cmp::min(
                rng.gen_range(3..=6),
                std::cmp::min(product_ids.len(), user_ids.len()),
            );
            let review_titles = [
                "Great product!",
                "Good value",
                "Okay, could be better",
                "Excellent quality",
                "Not worth it",
                "Amazing!",
            ];
            let review_bodies = [
                "Really enjoyed using this product. Highly recommend.",
                "Decent quality for the price. Would buy again.",
                "It works but nothing special. Average experience.",
                "Outstanding build quality and great customer service.",
                "Did not meet my expectations. Returning it.",
                "Best purchase I've made this year!",
            ];

            for r_idx in 0..review_count {
                let rating: i16 = rng.gen_range(1..=5);
                if let Err(e) = sqlx::query(
                    r#"INSERT INTO reviews (id, organization_id, product_id, user_id, rating, title, body, is_verified_purchase, helpful_count, reported, metadata, created_at, updated_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false, '{}', $10, $10)
                       ON CONFLICT (product_id, user_id) DO NOTHING"#)
                    .bind(Uuid::new_v4())
                    .bind(org.id)
                    .bind(product_ids[r_idx % product_ids.len()])
                    .bind(user_ids[r_idx % user_ids.len()])
                    .bind(rating)
                    .bind(review_titles[r_idx % review_titles.len()])
                    .bind(review_bodies[r_idx % review_bodies.len()])
                    .bind(rng.gen_bool(0.6))
                    .bind(rng.gen_range(0..50_i32))
                    .bind(now - Duration::days(rng.gen_range(1..60)))
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert review for org {}: {}", org.name, e);
                }
            }

            // ----------------------------------------------------------
            // Seed a few carts (some abandoned, some converted)
            // ----------------------------------------------------------
            let cart_count = std::cmp::min(rng.gen_range(3..=5), user_ids.len());
            let cart_statuses_seed = ["active", "converted", "abandoned", "abandoned", "converted"];

            for ct_idx in 0..cart_count {
                let cart_id = Uuid::new_v4();
                let uid = user_ids[ct_idx % user_ids.len()];
                let cstatus = cart_statuses_seed[ct_idx % cart_statuses_seed.len()];
                let sid = if !session_ids.is_empty() {
                    Some(session_ids[ct_idx % session_ids.len()])
                } else {
                    None
                };
                let cart_total: i64 = rng.gen_range(2000..50000);
                let abandoned_at_val: Option<DateTime<Utc>> = if cstatus == "abandoned" {
                    Some(now - Duration::hours(rng.gen_range(2..48)))
                } else {
                    None
                };

                if let Err(e) = sqlx::query(
                    r#"INSERT INTO carts (id, organization_id, user_id, session_id, status, subtotal_cents, discount_cents, total_cents, item_count, metadata, created_at, updated_at, abandoned_at)
                       VALUES ($1, $2, $3, $4, $5::cart_status, $6, 0, $6, $7, '{}', $8, $8, $9)"#)
                    .bind(cart_id)
                    .bind(org.id)
                    .bind(uid)
                    .bind(sid)
                    .bind(cstatus)
                    .bind(cart_total)
                    .bind(rng.gen_range(1..5_i32))
                    .bind(now - Duration::hours(rng.gen_range(1..72)))
                    .bind(abandoned_at_val)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert cart for org {}: {}", org.name, e);
                    continue;
                }

                // Cart Items (1-3 per cart)
                let ci_count = rng.gen_range(1..=3_usize);
                for _ci_i in 0..ci_count {
                    if product_ids.is_empty() {
                        break;
                    }
                    let p_idx = rng.gen_range(0..product_ids.len());
                    let qty: i32 = rng.gen_range(1..=3);
                    let unit_price = product_prices[p_idx];
                    let line_total = unit_price * qty as i64;
                    if let Err(e) = sqlx::query(
                        r#"INSERT INTO cart_items (id, cart_id, product_id, quantity, unit_price_cents, line_total_cents, added_at)
                           VALUES ($1, $2, $3, $4, $5, $6, $7)"#)
                        .bind(Uuid::new_v4())
                        .bind(cart_id)
                        .bind(product_ids[p_idx])
                        .bind(qty)
                        .bind(unit_price)
                        .bind(line_total)
                        .bind(now - Duration::hours(rng.gen_range(1..72)))
                        .execute(&self.pool)
                        .await
                    {
                        error!("Failed to insert cart item for org {}: {}", org.name, e);
                    }
                }
            }

            // ----------------------------------------------------------
            // Seed a few page_views (for page view metrics queries)
            // ----------------------------------------------------------
            let pv_pages = [
                "/dashboard",
                "/analytics",
                "/settings",
                "/billing",
                "/reports",
            ];
            let pv_count = rng.gen_range(5..=10);

            for pv_idx in 0..pv_count {
                let page_url = pv_pages[pv_idx % pv_pages.len()];
                let sid = if !session_ids.is_empty() {
                    Some(session_ids[pv_idx % session_ids.len()])
                } else {
                    None
                };
                let uid = if !user_ids.is_empty() {
                    Some(user_ids[pv_idx % user_ids.len()])
                } else {
                    None
                };

                if let Err(e) = sqlx::query(
                    r#"INSERT INTO page_views (id, organization_id, session_id, user_id, page_url, page_title, time_on_page_ms, dom_load_ms, first_paint_ms, first_contentful_paint_ms, largest_contentful_paint_ms, cumulative_layout_shift, viewport_width, viewport_height, scroll_depth_pct, created_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 1920, 1080, $13, $14)"#)
                    .bind(Uuid::new_v4())
                    .bind(org.id)
                    .bind(sid)
                    .bind(uid)
                    .bind(format!("https://app.example.com{}", page_url))
                    .bind(format!("{} Page", page_url.trim_start_matches('/')))
                    .bind(rng.gen_range(500..30000_i32))
                    .bind(rng.gen_range(100..2000_i32))
                    .bind(rng.gen_range(50..500_i32))
                    .bind(rng.gen_range(100..800_i32))
                    .bind(rng.gen_range(200..4000_i32))
                    .bind(rng.gen_range(0.0..0.5_f64))
                    .bind(rng.gen_range(10..100_i16))
                    .bind(now - Duration::minutes(rng.gen_range(1..1440)))
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert page_view for org {}: {}", org.name, e);
                }
            }

            // ----------------------------------------------------------
            // Seed a few ledger entries (for double-entry bookkeeping queries)
            // ----------------------------------------------------------
            let ledger_txn_count = rng.gen_range(2..=4);
            for _le_idx in 0..ledger_txn_count {
                let txn_id = Uuid::new_v4();
                let amount: i64 = rng.gen_range(1000..50000);

                // Debit entry
                if let Err(e) = sqlx::query(
                    r#"INSERT INTO ledger_entries (id, organization_id, transaction_id, entry_type, account_code, account_name, amount_cents, currency, description, reference_type, metadata, posted_at, created_at)
                       VALUES ($1, $2, $3, 'debit'::ledger_entry_type, '1000', 'Cash', $4, 'USD', 'Payment received', 'payment', '{}', $5, $5)"#)
                    .bind(Uuid::new_v4())
                    .bind(org.id)
                    .bind(txn_id)
                    .bind(amount)
                    .bind(now - Duration::days(rng.gen_range(1..30)))
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert ledger debit entry for org {}: {}", org.name, e);
                }

                // Credit entry
                if let Err(e) = sqlx::query(
                    r#"INSERT INTO ledger_entries (id, organization_id, transaction_id, entry_type, account_code, account_name, amount_cents, currency, description, reference_type, metadata, posted_at, created_at)
                       VALUES ($1, $2, $3, 'credit'::ledger_entry_type, '4000', 'Revenue', $4, 'USD', 'Revenue recognized', 'payment', '{}', $5, $5)"#)
                    .bind(Uuid::new_v4())
                    .bind(org.id)
                    .bind(txn_id)
                    .bind(amount)
                    .bind(now - Duration::days(rng.gen_range(1..30)))
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert ledger credit entry for org {}: {}", org.name, e);
                }
            }

            // ----------------------------------------------------------
            // 19. Events (50-100 per org, spread over last 48 hours)
            // ----------------------------------------------------------
            let event_count = rng.gen_range(50..=100);
            let event_types = ["page_view", "click", "conversion", "sign_up", "purchase"];
            let event_weights = [60, 28, 10, 1, 1]; // cumulative-ish weights
            let pages = [
                "/dashboard",
                "/analytics",
                "/reports",
                "/settings",
                "/users",
                "/billing",
                "/integrations",
                "/help",
            ];
            let referrers = [
                "https://google.com/search",
                "https://twitter.com",
                "https://linkedin.com",
                "https://facebook.com",
                "direct",
            ];
            let mut batch_events: Vec<crate::models::Event> = Vec::with_capacity(event_count);

            for _ in 0..event_count {
                let ev_rand: i32 = rng.gen_range(0..100);
                let event_type = if ev_rand < event_weights[0] {
                    event_types[0]
                } else if ev_rand < event_weights[0] + event_weights[1] {
                    event_types[1]
                } else if ev_rand < event_weights[0] + event_weights[1] + event_weights[2] {
                    event_types[2]
                } else if ev_rand < 99 {
                    event_types[3]
                } else {
                    event_types[4]
                };

                let ev_user_id = if !user_ids.is_empty() && rng.gen_bool(0.8) {
                    Some(user_ids[rng.gen_range(0..user_ids.len())])
                } else {
                    None
                };

                let page = pages[rng.gen_range(0..pages.len())];
                let page_url = format!("https://app.example.com{}", page);
                let referrer = referrers[rng.gen_range(0..referrers.len())];
                let created_at = now - Duration::minutes(rng.gen_range(1..2880)); // last 48h

                let properties = match event_type {
                    "purchase" => {
                        json!({"total_amount": rng.gen_range(1000..50000), "currency": "USD", "item_count": rng.gen_range(1..5)})
                    }
                    "conversion" => json!({"plan": "pro", "amount": rng.gen_range(2900..19900)}),
                    _ => json!({"page_title": format!("{} Page", page.trim_start_matches('/'))}),
                };

                batch_events.push(crate::models::Event {
                    id: Uuid::new_v4(),
                    organization_id: org.id,
                    user_id: ev_user_id,
                    event_type: event_type.to_string(),
                    page_url: Some(page_url),
                    referrer: Some(referrer.to_string()),
                    user_agent: Some("Mozilla/5.0 (seed)".to_string()),
                    ip_address: Some(format!(
                        "10.{}.{}.{}",
                        rng.gen_range(0..255u8),
                        rng.gen_range(0..255u8),
                        rng.gen_range(1..255u8)
                    )),
                    properties,
                    created_at,
                });
            }

            if let Err(e) = self.insert_events_batch(&batch_events).await {
                error!("Failed to insert seed events for org {}: {}", org.name, e);
            }

            info!(
                "Seeded all tables for organization {} ({})",
                org_index + 1,
                org.name
            );
        }

        info!("Initial data seeding complete");
        Ok(())
    }

    /// Insert multiple events in a single batch operation
    pub async fn insert_events_batch(&self, events: &[Event]) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO events (id, organization_id, user_id, event_type, page_url, referrer, user_agent, ip_address, properties, created_at) "
        );

        query_builder.push_values(events, |mut b, event| {
            b.push_bind(event.id)
                .push_bind(event.organization_id)
                .push_bind(event.user_id)
                .push_bind(&event.event_type)
                .push_bind(&event.page_url)
                .push_bind(&event.referrer)
                .push_bind(&event.user_agent)
                .push_bind(&event.ip_address)
                .push_bind(&event.properties)
                .push_bind(event.created_at);
        });

        let result = query_builder.build().execute(&self.pool).await?;
        Ok(result.rows_affected())
    }

    /// Get analytics overview with time range
    pub async fn get_analytics_overview(
        &self,
        org_id: Uuid,
        hours: i32,
    ) -> Result<AnalyticsOverview> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) as total_events,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) FILTER (WHERE event_type = 'page_view') as page_views,
                COUNT(*) FILTER (WHERE event_type = 'conversion') as conversions
            FROM events
            WHERE organization_id = $1
            AND created_at >= NOW() - INTERVAL '1 hour' * $2
            "#,
        )
        .bind(org_id)
        .bind(hours)
        .fetch_optional(&self.pool)
        .await?;

        let (total_events, unique_users, page_views, conversions) = match row {
            Some(r) => (
                r.try_get::<i64, _>("total_events")?,
                r.try_get::<i64, _>("unique_users")?,
                r.try_get::<i64, _>("page_views")?,
                r.try_get::<i64, _>("conversions")?,
            ),
            None => (0, 0, 0, 0),
        };

        let conversion_rate = if page_views > 0 {
            (conversions as f64 / page_views as f64) * 100.0
        } else {
            0.0
        };

        Ok(AnalyticsOverview {
            organization_id: org_id,
            total_events,
            unique_users,
            page_views,
            conversions,
            conversion_rate,
            time_period: format!("last {} hours", hours),
        })
    }

    /// Get top pages by view count
    pub async fn get_top_pages(&self, org_id: Uuid, limit: i32) -> Result<Vec<TopPage>> {
        let rows = sqlx::query(
            r#"
            SELECT
                page_url as url,
                COUNT(*) as views,
                COUNT(DISTINCT user_id) as unique_visitors
            FROM events
            WHERE organization_id = $1
            AND event_type = 'page_view'
            AND page_url IS NOT NULL
            AND created_at >= NOW() - INTERVAL '24 hours'
            GROUP BY page_url
            ORDER BY views DESC
            LIMIT $2
            "#,
        )
        .bind(org_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut top_pages = Vec::new();
        for row in rows {
            top_pages.push(TopPage {
                url: row.try_get("url")?,
                views: row.try_get("views")?,
                unique_visitors: row.try_get("unique_visitors")?,
            });
        }

        Ok(top_pages)
    }

    /// Get hourly metrics for time-series caching
    pub async fn get_hourly_metrics(
        &self,
        org_id: Uuid,
        hour_offset: i32,
    ) -> Result<HourlyMetrics> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) as events,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) FILTER (WHERE event_type = 'page_view') as page_views,
                COUNT(*) FILTER (WHERE event_type = 'click') as clicks,
                COUNT(*) FILTER (WHERE event_type = 'conversion') as conversions,
                COUNT(*) FILTER (WHERE event_type = 'sign_up') as signups,
                COUNT(*) FILTER (WHERE event_type = 'purchase') as purchases,
                COALESCE(SUM(CASE
                    WHEN event_type = 'purchase'
                    THEN (properties->>'total_amount')::float / 100.0
                    ELSE 0
                END), 0) as revenue
            FROM events
            WHERE organization_id = $1
            AND created_at >= NOW() - INTERVAL '1 hour' * ($2 + 1)
            AND created_at < NOW() - INTERVAL '1 hour' * $2
            "#,
        )
        .bind(org_id)
        .bind(hour_offset)
        .fetch_optional(&self.pool)
        .await?;

        let hour = Utc::now() - Duration::hours(hour_offset as i64);

        Ok(match row {
            Some(r) => HourlyMetrics {
                organization_id: org_id,
                hour,
                events: r.try_get("events")?,
                unique_users: r.try_get("unique_users")?,
                page_views: r.try_get("page_views")?,
                clicks: r.try_get("clicks")?,
                conversions: r.try_get("conversions")?,
                signups: r.try_get("signups")?,
                purchases: r.try_get("purchases")?,
                revenue: r.try_get("revenue")?,
            },
            None => HourlyMetrics {
                organization_id: org_id,
                hour,
                events: 0,
                unique_users: 0,
                page_views: 0,
                clicks: 0,
                conversions: 0,
                signups: 0,
                purchases: 0,
                revenue: 0.0,
            },
        })
    }

    /// Get user activity summary
    pub async fn get_user_activity(&self, user_id: Uuid) -> Result<UserActivity> {
        let row = sqlx::query(
            r#"
            SELECT
                organization_id,
                COUNT(*) as total_events,
                MAX(created_at) as last_seen,
                COUNT(*) FILTER (WHERE event_type = 'page_view') as page_views,
                COUNT(*) FILTER (WHERE event_type = 'click') as clicks,
                COUNT(*) FILTER (WHERE event_type = 'conversion') as conversions,
                COALESCE(SUM(CASE
                    WHEN event_type = 'purchase'
                    THEN (properties->>'total_amount')::float / 100.0
                    ELSE 0
                END), 0) as lifetime_value
            FROM events
            WHERE user_id = $1
            GROUP BY organization_id
            "#,
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some(r) => UserActivity {
                user_id,
                organization_id: r.try_get("organization_id")?,
                total_events: r.try_get("total_events")?,
                last_seen: r.try_get("last_seen")?,
                page_views: r.try_get("page_views")?,
                clicks: r.try_get("clicks")?,
                conversions: r.try_get("conversions")?,
                lifetime_value: r.try_get("lifetime_value")?,
            },
            None => UserActivity {
                user_id,
                organization_id: Uuid::nil(),
                total_events: 0,
                last_seen: Utc::now(),
                page_views: 0,
                clicks: 0,
                conversions: 0,
                lifetime_value: 0.0,
            },
        })
    }

    /// Get page performance metrics
    pub async fn get_page_performance(
        &self,
        org_id: Uuid,
        page_url: &str,
    ) -> Result<PagePerformance> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) as views,
                COUNT(DISTINCT user_id) as unique_visitors,
                COUNT(*) FILTER (WHERE event_type = 'conversion') as conversions
            FROM events
            WHERE organization_id = $1
            AND page_url = $2
            AND created_at >= NOW() - INTERVAL '24 hours'
            "#,
        )
        .bind(org_id)
        .bind(page_url)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some(r) => PagePerformance {
                organization_id: org_id,
                page_url: page_url.to_string(),
                views: r.try_get("views")?,
                unique_visitors: r.try_get("unique_visitors")?,
                avg_time_on_page: 45.5,
                bounce_rate: 0.35,
                conversions: r.try_get("conversions")?,
            },
            None => PagePerformance {
                organization_id: org_id,
                page_url: page_url.to_string(),
                views: 0,
                unique_visitors: 0,
                avg_time_on_page: 0.0,
                bounce_rate: 0.0,
                conversions: 0,
            },
        })
    }

    /// Get event type distribution
    pub async fn get_event_distribution(&self, org_id: Uuid) -> Result<EventTypeDistribution> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE event_type = 'page_view') as page_views,
                COUNT(*) FILTER (WHERE event_type = 'click') as clicks,
                COUNT(*) FILTER (WHERE event_type = 'conversion') as conversions,
                COUNT(*) FILTER (WHERE event_type = 'sign_up') as signups,
                COUNT(*) FILTER (WHERE event_type = 'purchase') as purchases,
                COUNT(*) as total
            FROM events
            WHERE organization_id = $1
            AND created_at >= NOW() - INTERVAL '24 hours'
            "#,
        )
        .bind(org_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some(r) => EventTypeDistribution {
                organization_id: org_id,
                page_views: r.try_get("page_views")?,
                clicks: r.try_get("clicks")?,
                conversions: r.try_get("conversions")?,
                signups: r.try_get("signups")?,
                purchases: r.try_get("purchases")?,
                total: r.try_get("total")?,
            },
            None => EventTypeDistribution {
                organization_id: org_id,
                page_views: 0,
                clicks: 0,
                conversions: 0,
                signups: 0,
                purchases: 0,
                total: 0,
            },
        })
    }

    /// Get ALL organization IDs efficiently (no ORDER BY RANDOM)
    /// This is called once at startup and periodically refreshed
    pub async fn get_all_organization_ids(&self, limit: u32) -> Result<Vec<Uuid>> {
        let rows = sqlx::query("SELECT id FROM organizations LIMIT $1")
            .bind(limit as i32)
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter()
            .map(|row| Ok(row.try_get("id")?))
            .collect::<Result<Vec<_>>>()
    }

    /// List organizations for customer-facing APIs and UIs
    pub async fn list_organizations(&self, limit: u32) -> Result<Vec<Organization>> {
        let organizations = sqlx::query_as::<_, Organization>(
            r#"
            SELECT id, name, created_at
            FROM organizations
            ORDER BY created_at DESC
            LIMIT $1
            "#,
        )
        .bind(limit as i32)
        .fetch_all(&self.pool)
        .await?;

        Ok(organizations)
    }

    /// Get user IDs for a specific organization (no ORDER BY RANDOM)
    pub async fn get_user_ids_for_org(&self, org_id: Uuid, limit: u32) -> Result<Vec<Uuid>> {
        let rows = sqlx::query("SELECT id FROM users WHERE organization_id = $1 LIMIT $2")
            .bind(org_id)
            .bind(limit as i32)
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter()
            .map(|row| Ok(row.try_get("id")?))
            .collect::<Result<Vec<_>>>()
    }

    async fn choose_cart_product(
        tx: &mut Transaction<'_, Postgres>,
        org_id: Uuid,
        preferred_product_id: Option<Uuid>,
    ) -> Result<Option<(Uuid, i64)>> {
        let row = if let Some(product_id) = preferred_product_id {
            sqlx::query(
                r#"
                SELECT id, price_cents
                FROM products
                WHERE organization_id = $1
                  AND id = $2
                  AND is_active = true
                LIMIT 1
                "#,
            )
            .bind(org_id)
            .bind(product_id)
            .fetch_optional(&mut **tx)
            .await?
        } else {
            sqlx::query(
                r#"
                SELECT id, price_cents
                FROM products
                WHERE organization_id = $1
                  AND is_active = true
                ORDER BY rating_avg DESC, created_at DESC
                LIMIT 1
                "#,
            )
            .bind(org_id)
            .fetch_optional(&mut **tx)
            .await?
        };

        row.map(|row| Ok((row.try_get("id")?, row.try_get("price_cents")?)))
            .transpose()
    }

    async fn fallback_user_for_org(
        tx: &mut Transaction<'_, Postgres>,
        org_id: Uuid,
    ) -> Result<Option<Uuid>> {
        let row = sqlx::query(
            r#"
            SELECT id
            FROM users
            WHERE organization_id = $1
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .fetch_optional(&mut **tx)
        .await?;

        Ok(row.map(|row| row.try_get("id")).transpose()?)
    }

    /// Get a storefront-ready product catalog with inventory state
    pub async fn get_catalog_products_detailed(
        &self,
        org_id: Uuid,
        limit: i32,
    ) -> Result<Vec<CatalogProduct>> {
        let products = sqlx::query_as::<_, CatalogProduct>(
            r#"
            SELECT
                p.id,
                p.organization_id,
                p.sku,
                p.name,
                p.description,
                p.price_cents,
                p.compare_at_price_cents,
                p.currency,
                p.tags,
                p.images,
                p.rating_avg::float8 AS rating_avg,
                p.rating_count,
                COALESCE(SUM(i.quantity_available), 0)::int AS quantity_available,
                COALESCE(BOOL_OR(i.is_low_stock), false) AS is_low_stock
            FROM products p
            LEFT JOIN inventory i ON i.product_id = p.id
            WHERE p.organization_id = $1
              AND p.is_active = true
            GROUP BY p.id
            ORDER BY quantity_available DESC, p.rating_avg DESC, p.created_at DESC
            LIMIT $2
            "#,
        )
        .bind(org_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(products)
    }

    /// Get an active cart with its line items
    pub async fn get_cart_snapshot(
        &self,
        org_id: Uuid,
        cart_id: Uuid,
    ) -> Result<Option<CartSnapshot>> {
        let row = sqlx::query(
            r#"
            SELECT
                id,
                organization_id,
                user_id,
                status::text AS status,
                subtotal_cents,
                discount_cents,
                total_cents,
                item_count,
                updated_at,
                abandoned_at
            FROM carts
            WHERE organization_id = $1
              AND id = $2
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(cart_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let items = sqlx::query_as::<_, CartLineItemDetail>(
            r#"
            SELECT
                ci.id,
                ci.product_id,
                p.name AS product_name,
                p.sku,
                ci.quantity,
                ci.unit_price_cents,
                ci.line_total_cents,
                COALESCE(p.tags, '{}'::text[]) AS tags
            FROM cart_items ci
            JOIN products p ON p.id = ci.product_id
            WHERE ci.cart_id = $1
            ORDER BY ci.added_at DESC
            "#,
        )
        .bind(cart_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(Some(CartSnapshot {
            id: row.try_get("id")?,
            organization_id: row.try_get("organization_id")?,
            user_id: row.try_get("user_id")?,
            status: row.try_get("status")?,
            subtotal_cents: row.try_get("subtotal_cents")?,
            discount_cents: row.try_get("discount_cents")?,
            total_cents: row.try_get("total_cents")?,
            item_count: row.try_get("item_count")?,
            updated_at: row.try_get("updated_at")?,
            abandoned_at: row.try_get("abandoned_at")?,
            items,
        }))
    }

    /// Create a cart with an initial line item and record a click-style add-to-cart event
    pub async fn create_cart_with_item(
        &self,
        org_id: Uuid,
        user_id: Option<Uuid>,
        product_id: Option<Uuid>,
        quantity: i32,
        metadata: &Value,
    ) -> Result<CartSnapshot> {
        let mut tx = self.pool.begin().await?;
        let quantity = quantity.max(1);
        let (product_id, unit_price_cents) = Self::choose_cart_product(&mut tx, org_id, product_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("no active products available for organization {}", org_id)
            })?;

        let cart_id = Uuid::new_v4();
        let line_total_cents = unit_price_cents * i64::from(quantity);
        let now = Utc::now();

        sqlx::query(
            r#"
            INSERT INTO carts (
                id,
                organization_id,
                user_id,
                session_id,
                status,
                subtotal_cents,
                discount_cents,
                total_cents,
                item_count,
                metadata,
                created_at,
                updated_at,
                abandoned_at
            )
            VALUES ($1, $2, $3, NULL, 'active', $4, 0, $4, $5, $6, $7, $7, NULL)
            "#,
        )
        .bind(cart_id)
        .bind(org_id)
        .bind(user_id)
        .bind(line_total_cents)
        .bind(quantity)
        .bind(metadata)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO cart_items (
                id,
                cart_id,
                product_id,
                quantity,
                unit_price_cents,
                line_total_cents,
                added_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(cart_id)
        .bind(product_id)
        .bind(quantity)
        .bind(unit_price_cents)
        .bind(line_total_cents)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO events (
                id,
                organization_id,
                user_id,
                event_type,
                page_url,
                referrer,
                user_agent,
                ip_address,
                properties,
                created_at
            )
            VALUES ($1, $2, $3, 'click', $4, NULL, NULL, NULL, $5, $6)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(org_id)
        .bind(user_id)
        .bind("https://app.example.com/storefront")
        .bind(json!({
            "action": "create_cart",
            "cart_id": cart_id,
            "product_id": product_id,
            "quantity": quantity,
            "channel": "storefront_api",
        }))
        .bind(now)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        self.get_cart_snapshot(org_id, cart_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("cart {} disappeared after creation", cart_id))
    }

    /// Add a line item to an existing active cart
    pub async fn add_item_to_cart(
        &self,
        org_id: Uuid,
        cart_id: Uuid,
        product_id: Option<Uuid>,
        quantity: i32,
    ) -> Result<Option<CartSnapshot>> {
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query(
            r#"
            SELECT user_id
            FROM carts
            WHERE organization_id = $1
              AND id = $2
              AND status = 'active'
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(cart_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(cart_row) = existing else {
            return Ok(None);
        };

        let quantity = quantity.max(1);
        let user_id: Option<Uuid> = cart_row.try_get("user_id")?;
        let (product_id, unit_price_cents) = Self::choose_cart_product(&mut tx, org_id, product_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("no active products available for organization {}", org_id)
            })?;
        let line_total_cents = unit_price_cents * i64::from(quantity);
        let now = Utc::now();

        sqlx::query(
            r#"
            INSERT INTO cart_items (
                id,
                cart_id,
                product_id,
                quantity,
                unit_price_cents,
                line_total_cents,
                added_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(cart_id)
        .bind(product_id)
        .bind(quantity)
        .bind(unit_price_cents)
        .bind(line_total_cents)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE carts
            SET subtotal_cents = subtotal_cents + $3,
                total_cents = total_cents + $3,
                item_count = item_count + $4,
                updated_at = $5
            WHERE organization_id = $1
              AND id = $2
            "#,
        )
        .bind(org_id)
        .bind(cart_id)
        .bind(line_total_cents)
        .bind(quantity)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO events (
                id,
                organization_id,
                user_id,
                event_type,
                page_url,
                referrer,
                user_agent,
                ip_address,
                properties,
                created_at
            )
            VALUES ($1, $2, $3, 'click', $4, NULL, NULL, NULL, $5, $6)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(org_id)
        .bind(user_id)
        .bind("https://app.example.com/cart")
        .bind(json!({
            "action": "add_item_to_cart",
            "cart_id": cart_id,
            "product_id": product_id,
            "quantity": quantity,
            "channel": "storefront_api",
        }))
        .bind(now)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        self.get_cart_snapshot(org_id, cart_id).await
    }

    /// Checkout an active cart into an order and completed payment
    pub async fn checkout_cart(
        &self,
        org_id: Uuid,
        cart_id: Uuid,
        user_id: Option<Uuid>,
        payment_method: &str,
        notes: Option<&str>,
    ) -> Result<Option<CheckoutReceipt>> {
        let mut tx = self.pool.begin().await?;
        let cart_row = sqlx::query(
            r#"
            SELECT
                user_id,
                subtotal_cents,
                discount_cents,
                item_count
            FROM carts
            WHERE organization_id = $1
              AND id = $2
              AND status = 'active'
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(cart_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(cart_row) = cart_row else {
            return Ok(None);
        };

        let line_items = sqlx::query(
            r#"
            SELECT
                ci.product_id,
                p.name AS product_name,
                p.sku,
                ci.quantity,
                ci.unit_price_cents,
                ci.line_total_cents
            FROM cart_items ci
            JOIN products p ON p.id = ci.product_id
            WHERE ci.cart_id = $1
            ORDER BY ci.added_at ASC
            "#,
        )
        .bind(cart_id)
        .fetch_all(&mut *tx)
        .await?;

        if line_items.is_empty() {
            return Err(anyhow::anyhow!("cart {} has no line items", cart_id));
        }

        let cart_user_id: Option<Uuid> = cart_row.try_get("user_id")?;
        let resolved_user_id = match user_id.or(cart_user_id) {
            Some(user_id) => user_id,
            None => Self::fallback_user_for_org(&mut tx, org_id)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!("organization {} has no users for checkout", org_id)
                })?,
        };

        let subtotal_cents = cart_row.try_get::<i64, _>("subtotal_cents")?;
        let discount_cents = cart_row.try_get::<i64, _>("discount_cents")?;
        let item_count = cart_row.try_get::<i32, _>("item_count")?;
        let tax_cents = ((subtotal_cents - discount_cents) as f64 * 0.08).round() as i64;
        let shipping_cents = if subtotal_cents >= 7_500 { 0 } else { 899 };
        let total_cents = subtotal_cents - discount_cents + tax_cents + shipping_cents;
        let now = Utc::now();
        let order_id = Uuid::new_v4();
        let payment_id = Uuid::new_v4();
        let order_number = format!("ORD-{}", &Uuid::new_v4().simple().to_string()[..12]);

        sqlx::query(
            r#"
            INSERT INTO orders (
                id,
                organization_id,
                user_id,
                cart_id,
                order_number,
                status,
                subtotal_cents,
                discount_cents,
                tax_cents,
                shipping_cents,
                total_cents,
                currency,
                coupon_id,
                shipping_address,
                billing_address,
                notes,
                metadata,
                placed_at,
                shipped_at,
                delivered_at,
                cancelled_at,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, 'confirmed', $6, $7, $8, $9, $10, 'USD', NULL,
                $11, $11, $12, $13, $14, NULL, NULL, NULL, $14, $14
            )
            "#,
        )
        .bind(order_id)
        .bind(org_id)
        .bind(resolved_user_id)
        .bind(cart_id)
        .bind(order_number)
        .bind(subtotal_cents)
        .bind(discount_cents)
        .bind(tax_cents)
        .bind(shipping_cents)
        .bind(total_cents)
        .bind(json!({
            "country": "US",
            "city": "New York",
            "postal_code": "10001",
        }))
        .bind(notes)
        .bind(json!({
            "source": "storefront_checkout",
            "cart_id": cart_id,
            "item_count": item_count,
        }))
        .bind(now)
        .execute(&mut *tx)
        .await?;

        for row in &line_items {
            let product_id = row.try_get::<Uuid, _>("product_id")?;
            let quantity = row.try_get::<i32, _>("quantity")?;
            let unit_price_cents = row.try_get::<i64, _>("unit_price_cents")?;
            let line_total_cents = row.try_get::<i64, _>("line_total_cents")?;
            let product_name = row.try_get::<String, _>("product_name")?;
            let sku = row.try_get::<String, _>("sku")?;

            sqlx::query(
                r#"
                INSERT INTO order_items (
                    id,
                    order_id,
                    product_id,
                    product_name,
                    sku,
                    quantity,
                    unit_price_cents,
                    discount_cents,
                    line_total_cents,
                    metadata
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, 0, $8, $9)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(order_id)
            .bind(product_id)
            .bind(product_name)
            .bind(sku)
            .bind(quantity)
            .bind(unit_price_cents)
            .bind(line_total_cents)
            .bind(json!({ "source": "cart_checkout" }))
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                r#"
                UPDATE inventory
                SET quantity_on_hand = GREATEST(quantity_on_hand - $3, 0),
                    quantity_available = GREATEST(quantity_available - $3, 0),
                    is_low_stock = GREATEST(quantity_available - $3, 0) <= reorder_point,
                    updated_at = $4
                WHERE organization_id = $1
                  AND product_id = $2
                "#,
            )
            .bind(org_id)
            .bind(product_id)
            .bind(quantity)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }

        sqlx::query(
            r#"
            INSERT INTO payments (
                id,
                organization_id,
                user_id,
                invoice_id,
                order_id,
                amount_cents,
                currency,
                status,
                method,
                gateway_transaction_id,
                gateway_response,
                failure_reason,
                idempotency_key,
                processed_at,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, NULL, $4, $5, 'USD', 'completed', $6::payment_method,
                $7, $8, NULL, $9, $10, $10, $10
            )
            "#,
        )
        .bind(payment_id)
        .bind(org_id)
        .bind(resolved_user_id)
        .bind(order_id)
        .bind(total_cents)
        .bind(payment_method)
        .bind(format!("txn_{}", Uuid::new_v4().simple()))
        .bind(json!({
            "approved": true,
            "avs_result": "pass",
            "cvv_result": "pass",
        }))
        .bind(format!("checkout:{}", cart_id))
        .bind(now)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE carts
            SET status = 'converted',
                total_cents = $3,
                user_id = $4,
                updated_at = $5
            WHERE organization_id = $1
              AND id = $2
            "#,
        )
        .bind(org_id)
        .bind(cart_id)
        .bind(total_cents)
        .bind(resolved_user_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO events (
                id,
                organization_id,
                user_id,
                event_type,
                page_url,
                referrer,
                user_agent,
                ip_address,
                properties,
                created_at
            )
            VALUES ($1, $2, $3, 'purchase', $4, NULL, NULL, NULL, $5, $6)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(org_id)
        .bind(Some(resolved_user_id))
        .bind("https://app.example.com/checkout/success")
        .bind(json!({
            "action": "checkout",
            "cart_id": cart_id,
            "order_id": order_id,
            "payment_id": payment_id,
            "item_count": item_count,
            "total_amount": total_cents,
            "currency": "USD",
        }))
        .bind(now)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(Some(CheckoutReceipt {
            organization_id: org_id,
            cart_id,
            order_id,
            payment_id,
            user_id: resolved_user_id,
            total_cents,
            currency: "USD".to_string(),
            created_at: now,
        }))
    }

    /// DEPRECATED: Use get_all_organization_ids + in-memory random selection
    /// Keeping for backward compatibility but logs warning
    #[allow(dead_code)]
    pub async fn get_random_organization_ids(&self, limit: u32) -> Result<Vec<Uuid>> {
        warn!("get_random_organization_ids is deprecated - use get_all_organization_ids with OrgIdCache");
        let rows = sqlx::query("SELECT id FROM organizations ORDER BY RANDOM() LIMIT $1")
            .bind(limit as i32)
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter()
            .map(|row| Ok(row.try_get("id")?))
            .collect::<Result<Vec<_>>>()
    }

    // ============================================================
    // Additional PostgreSQL query varieties (used by pg_workers)
    // These provide a wide range of SQL patterns for load testing
    // ============================================================

    /// Upsert an event (INSERT ... ON CONFLICT DO UPDATE)
    pub async fn upsert_event(&self, event: &Event) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO events (id, organization_id, user_id, event_type, page_url, referrer, user_agent, ip_address, properties, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (id) DO UPDATE SET
                properties = EXCLUDED.properties,
                page_url = EXCLUDED.page_url
            "#,
        )
            .bind(event.id)
            .bind(event.organization_id)
            .bind(event.user_id)
            .bind(&event.event_type)
            .bind(&event.page_url)
            .bind(&event.referrer)
            .bind(&event.user_agent)
            .bind(&event.ip_address)
            .bind(&event.properties)
            .bind(event.created_at)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Update event properties using JSONB operators
    pub async fn update_event_properties(
        &self,
        org_id: Uuid,
        event_type: &str,
        new_props: serde_json::Value,
    ) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE events
            SET properties = properties || $3
            WHERE organization_id = $1
            AND event_type = $2
            AND created_at >= NOW() - INTERVAL '1 hour'
            "#,
        )
        .bind(org_id)
        .bind(event_type)
        .bind(&new_props)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Delete old events beyond retention window
    pub async fn delete_old_events(&self, org_id: Uuid, hours: i32) -> Result<u64> {
        let result = sqlx::query(
            r#"
            DELETE FROM events
            WHERE organization_id = $1
            AND created_at < NOW() - INTERVAL '1 hour' * $2
            LIMIT 1000
            "#,
        )
        .bind(org_id)
        .bind(hours)
        .execute(&self.pool)
        .await;

        // Fallback for databases without LIMIT on DELETE
        match result {
            Ok(r) => Ok(r.rows_affected()),
            Err(_) => {
                let result = sqlx::query(
                    r#"
                    DELETE FROM events
                    WHERE id IN (
                        SELECT id FROM events
                        WHERE organization_id = $1
                        AND created_at < NOW() - INTERVAL '1 hour' * $2
                        LIMIT 1000
                    )
                    "#,
                )
                .bind(org_id)
                .bind(hours)
                .execute(&self.pool)
                .await?;
                Ok(result.rows_affected())
            }
        }
    }

    /// Window function query: ranked page performance within org
    pub async fn get_ranked_pages(&self, org_id: Uuid) -> Result<Vec<(String, i64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT page_url, view_count, rank
            FROM (
                SELECT
                    page_url,
                    COUNT(*) as view_count,
                    RANK() OVER (ORDER BY COUNT(*) DESC) as rank
                FROM events
                WHERE organization_id = $1
                AND event_type = 'page_view'
                AND page_url IS NOT NULL
                AND created_at >= NOW() - INTERVAL '24 hours'
                GROUP BY page_url
            ) ranked
            WHERE rank <= 10
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("page_url")?,
                    r.try_get::<i64, _>("view_count")?,
                    r.try_get::<i64, _>("rank")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// CTE query: funnel analysis with multi-step conversion tracking
    pub async fn get_conversion_funnel(&self, org_id: Uuid) -> Result<Vec<(String, i64)>> {
        let rows = sqlx::query(
            r#"
            WITH funnel AS (
                SELECT
                    event_type,
                    COUNT(DISTINCT user_id) as users,
                    CASE event_type
                        WHEN 'page_view' THEN 1
                        WHEN 'click' THEN 2
                        WHEN 'sign_up' THEN 3
                        WHEN 'conversion' THEN 4
                        WHEN 'purchase' THEN 5
                    END as step_order
                FROM events
                WHERE organization_id = $1
                AND created_at >= NOW() - INTERVAL '7 days'
                AND user_id IS NOT NULL
                GROUP BY event_type
            )
            SELECT event_type, users
            FROM funnel
            WHERE step_order IS NOT NULL
            ORDER BY step_order
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("event_type")?,
                    r.try_get::<i64, _>("users")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// JOIN query: user activity with org details
    pub async fn get_active_users_with_orgs(
        &self,
        org_id: Uuid,
        limit: i32,
    ) -> Result<Vec<(Uuid, String, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT u.id, u.name, COUNT(e.id) as event_count
            FROM users u
            INNER JOIN events e ON e.user_id = u.id
            WHERE u.organization_id = $1
            AND e.created_at >= NOW() - INTERVAL '24 hours'
            GROUP BY u.id, u.name
            ORDER BY event_count DESC
            LIMIT $2
            "#,
        )
        .bind(org_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<Uuid, _>("id")?,
                    r.try_get::<String, _>("name")?,
                    r.try_get::<i64, _>("event_count")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Aggregate query: time-bucketed event counts (for time-series charts)
    pub async fn get_time_bucketed_events(
        &self,
        org_id: Uuid,
        bucket_minutes: i32,
    ) -> Result<Vec<(DateTime<Utc>, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                date_trunc('hour', created_at) + (EXTRACT(minute FROM created_at)::int / $2 * $2) * INTERVAL '1 minute' as bucket,
                COUNT(*) as event_count
            FROM events
            WHERE organization_id = $1
            AND created_at >= NOW() - INTERVAL '6 hours'
            GROUP BY bucket
            ORDER BY bucket DESC
            "#,
        )
            .bind(org_id)
            .bind(bucket_minutes)
            .fetch_all(&self.pool)
            .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<DateTime<Utc>, _>("bucket")?,
                    r.try_get::<i64, _>("event_count")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// JSONB query: extract and aggregate from properties column
    pub async fn get_revenue_by_plan(&self, org_id: Uuid) -> Result<Vec<(String, f64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                COALESCE(properties->>'plan', 'unknown') as plan,
                COALESCE(SUM((properties->>'amount')::float / 100.0), 0) as total_revenue,
                COUNT(*) as transaction_count
            FROM events
            WHERE organization_id = $1
            AND event_type IN ('conversion', 'purchase')
            AND created_at >= NOW() - INTERVAL '30 days'
            GROUP BY properties->>'plan'
            ORDER BY total_revenue DESC
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("plan")?,
                    r.try_get::<f64, _>("total_revenue")?,
                    r.try_get::<i64, _>("transaction_count")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Subquery: organizations with above-average event counts
    pub async fn get_high_activity_orgs(&self, limit: i32) -> Result<Vec<(Uuid, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT organization_id, event_count
            FROM (
                SELECT organization_id, COUNT(*) as event_count
                FROM events
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                GROUP BY organization_id
            ) org_events
            WHERE event_count > (
                SELECT AVG(cnt) FROM (
                    SELECT COUNT(*) as cnt
                    FROM events
                    WHERE created_at >= NOW() - INTERVAL '24 hours'
                    GROUP BY organization_id
                ) avg_calc
            )
            ORDER BY event_count DESC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<Uuid, _>("organization_id")?,
                    r.try_get::<i64, _>("event_count")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Referrer stats with JOIN and aggregation
    pub async fn get_referrer_stats(&self, org_id: Uuid) -> Result<Vec<(String, i64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                COALESCE(referrer, 'direct') as referrer,
                COUNT(*) as visits,
                COUNT(DISTINCT user_id) as unique_visitors
            FROM events
            WHERE organization_id = $1
            AND created_at >= NOW() - INTERVAL '24 hours'
            GROUP BY referrer
            ORDER BY visits DESC
            LIMIT 20
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("referrer")?,
                    r.try_get::<i64, _>("visits")?,
                    r.try_get::<i64, _>("unique_visitors")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Update user name (simple UPDATE)
    pub async fn update_user_name(&self, user_id: Uuid, new_name: &str) -> Result<bool> {
        let result = sqlx::query("UPDATE users SET name = $2 WHERE id = $1")
            .bind(user_id)
            .bind(new_name)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Count events by type for a given org (simple aggregate)
    pub async fn count_events_by_type(&self, org_id: Uuid) -> Result<Vec<(String, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT event_type, COUNT(*) as cnt
            FROM events
            WHERE organization_id = $1
            GROUP BY event_type
            ORDER BY cnt DESC
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("event_type")?,
                    r.try_get::<i64, _>("cnt")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Check if an organization exists (simple SELECT)
    pub async fn org_exists(&self, org_id: Uuid) -> Result<bool> {
        let row: (bool,) =
            sqlx::query_as("SELECT EXISTS(SELECT 1 FROM organizations WHERE id = $1)")
                .bind(org_id)
                .fetch_one(&self.pool)
                .await?;
        Ok(row.0)
    }

    // ============================================================
    // Analytics domain queries (sessions, campaigns, experiments, page views)
    // ============================================================

    /// Get device type distribution from sessions for an organization
    pub async fn get_session_stats(&self, org_id: Uuid) -> Result<Vec<(String, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                device::text AS device_type,
                COUNT(*) AS cnt
            FROM sessions
            WHERE organization_id = $1
            GROUP BY device
            ORDER BY cnt DESC
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("device_type")?,
                    r.try_get::<i64, _>("cnt")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Get campaign performance: name, click count, conversion count
    pub async fn get_campaign_performance(&self, org_id: Uuid) -> Result<Vec<(String, i64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                c.name,
                COALESCE(SUM(c.click_count), 0)::bigint AS total_clicks,
                COALESCE(SUM(c.conversion_count), 0)::bigint AS total_conversions
            FROM campaigns c
            WHERE c.organization_id = $1
            GROUP BY c.name
            ORDER BY total_clicks DESC
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("name")?,
                    r.try_get::<i64, _>("total_clicks")?,
                    r.try_get::<i64, _>("total_conversions")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Get experiment results: name, status, number of assignments
    pub async fn get_experiment_results(&self, org_id: Uuid) -> Result<Vec<(String, String, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                e.name,
                e.status::text AS status,
                COUNT(ea.id) AS assignment_count
            FROM experiments e
            LEFT JOIN experiment_assignments ea ON ea.experiment_id = e.id
            WHERE e.organization_id = $1
            GROUP BY e.id, e.name, e.status
            ORDER BY assignment_count DESC
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("name")?,
                    r.try_get::<String, _>("status")?,
                    r.try_get::<i64, _>("assignment_count")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Get page view metrics: page URL and average Largest Contentful Paint
    pub async fn get_page_view_metrics(&self, org_id: Uuid) -> Result<Vec<(String, f64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                page_url,
                AVG(largest_contentful_paint_ms)::float8 AS avg_lcp
            FROM page_views
            WHERE organization_id = $1
              AND largest_contentful_paint_ms IS NOT NULL
            GROUP BY page_url
            ORDER BY avg_lcp DESC
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("page_url")?,
                    r.try_get::<f64, _>("avg_lcp")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    // ============================================================
    // E-commerce domain queries (products, orders, carts, reviews, inventory, coupons)
    // ============================================================

    /// Get product catalog: product name, price_cents, rating_avg
    pub async fn get_product_catalog(
        &self,
        org_id: Uuid,
        limit: i32,
    ) -> Result<Vec<(String, i64, f64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                p.name,
                p.price_cents,
                p.rating_avg::float8 AS rating_avg
            FROM products p
            WHERE p.organization_id = $1
              AND p.is_active = true
            ORDER BY p.rating_avg DESC, p.created_at DESC
            LIMIT $2
            "#,
        )
        .bind(org_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("name")?,
                    r.try_get::<i64, _>("price_cents")?,
                    r.try_get::<f64, _>("rating_avg")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Get order summary: status, count, total_cents
    pub async fn get_order_summary(&self, org_id: Uuid) -> Result<Vec<(String, i64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                status::text AS status,
                COUNT(*) AS order_count,
                COALESCE(SUM(total_cents), 0)::bigint AS total_cents
            FROM orders
            WHERE organization_id = $1
            GROUP BY status
            ORDER BY order_count DESC
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("status")?,
                    r.try_get::<i64, _>("order_count")?,
                    r.try_get::<i64, _>("total_cents")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Get top products by revenue from order_items
    pub async fn get_top_products_by_revenue(
        &self,
        org_id: Uuid,
        limit: i32,
    ) -> Result<Vec<(String, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                oi.product_name,
                SUM(oi.line_total_cents)::bigint AS revenue
            FROM order_items oi
            JOIN orders o ON o.id = oi.order_id
            WHERE o.organization_id = $1
              AND o.status NOT IN ('cancelled', 'refunded')
            GROUP BY oi.product_name
            ORDER BY revenue DESC
            LIMIT $2
            "#,
        )
        .bind(org_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("product_name")?,
                    r.try_get::<i64, _>("revenue")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Get cart abandonment rate: (abandoned_count, total_count)
    pub async fn get_cart_abandonment_rate(&self, org_id: Uuid) -> Result<(i64, i64)> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE status = 'abandoned') AS abandoned,
                COUNT(*) AS total
            FROM carts
            WHERE organization_id = $1
            "#,
        )
        .bind(org_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(r) => Ok((
                r.try_get::<i64, _>("abandoned")?,
                r.try_get::<i64, _>("total")?,
            )),
            None => Ok((0, 0)),
        }
    }

    /// Get product reviews summary: product name, avg rating, review count
    pub async fn get_product_reviews_summary(
        &self,
        org_id: Uuid,
    ) -> Result<Vec<(String, f64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                p.name,
                AVG(r.rating)::float8 AS avg_rating,
                COUNT(r.id) AS review_count
            FROM products p
            JOIN reviews r ON r.product_id = p.id
            WHERE p.organization_id = $1
            GROUP BY p.id, p.name
            HAVING COUNT(r.id) > 0
            ORDER BY avg_rating DESC, review_count DESC
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("name")?,
                    r.try_get::<f64, _>("avg_rating")?,
                    r.try_get::<i64, _>("review_count")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Get inventory alerts: product name, quantity available, reorder point
    pub async fn get_inventory_alerts(&self, org_id: Uuid) -> Result<Vec<(String, i32, i32)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                p.name,
                i.quantity_available,
                i.reorder_point
            FROM inventory i
            JOIN products p ON p.id = i.product_id
            WHERE i.organization_id = $1
              AND i.is_low_stock = true
            ORDER BY i.quantity_available ASC
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("name")?,
                    r.try_get::<i32, _>("quantity_available")?,
                    r.try_get::<i32, _>("reorder_point")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Get coupon usage: code, current_uses, max_uses
    pub async fn get_coupon_usage(&self, org_id: Uuid) -> Result<Vec<(String, i32, Option<i32>)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                code,
                current_uses,
                max_uses
            FROM coupons
            WHERE organization_id = $1
            ORDER BY current_uses DESC
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("code")?,
                    r.try_get::<i32, _>("current_uses")?,
                    r.try_get::<i32, _>("max_uses").ok(),
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    // ============================================================
    // Finance domain queries (subscriptions, MRR, invoices, payments, ledger)
    // ============================================================

    /// Get subscription metrics: status, count, total MRR in cents
    pub async fn get_subscription_metrics(&self, org_id: Uuid) -> Result<Vec<(String, i64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                status::text AS status,
                COUNT(*) AS cnt,
                COALESCE(SUM(mrr_cents), 0)::bigint AS total_mrr
            FROM subscriptions
            WHERE organization_id = $1
            GROUP BY status
            ORDER BY total_mrr DESC
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("status")?,
                    r.try_get::<i64, _>("cnt")?,
                    r.try_get::<i64, _>("total_mrr")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Get MRR by plan: plan name, subscriber count, total MRR
    pub async fn get_mrr_by_plan(&self, org_id: Uuid) -> Result<Vec<(String, i64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                sp.name AS plan_name,
                COUNT(s.id) AS subscriber_count,
                COALESCE(SUM(s.mrr_cents), 0)::bigint AS mrr
            FROM subscription_plans sp
            LEFT JOIN subscriptions s ON s.plan_id = sp.id
                AND s.status IN ('active', 'trialing')
            WHERE sp.organization_id = $1
            GROUP BY sp.id, sp.name, sp.sort_order
            ORDER BY sp.sort_order
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("plan_name")?,
                    r.try_get::<i64, _>("subscriber_count")?,
                    r.try_get::<i64, _>("mrr")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Get invoice aging: status, count, total cents
    pub async fn get_invoice_aging(&self, org_id: Uuid) -> Result<Vec<(String, i64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                status::text AS status,
                COUNT(*) AS cnt,
                COALESCE(SUM(total_cents), 0)::bigint AS total
            FROM invoices
            WHERE organization_id = $1
            GROUP BY status
            ORDER BY total DESC
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("status")?,
                    r.try_get::<i64, _>("cnt")?,
                    r.try_get::<i64, _>("total")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Get payment method distribution: method, count, total amount
    pub async fn get_payment_method_distribution(
        &self,
        org_id: Uuid,
    ) -> Result<Vec<(String, i64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                method::text AS method,
                COUNT(*) AS cnt,
                COALESCE(SUM(amount_cents), 0)::bigint AS total_amount
            FROM payments
            WHERE organization_id = $1
              AND status = 'completed'
            GROUP BY method
            ORDER BY total_amount DESC
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("method")?,
                    r.try_get::<i64, _>("cnt")?,
                    r.try_get::<i64, _>("total_amount")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Get revenue timeline: daily payment totals
    pub async fn get_revenue_timeline(&self, org_id: Uuid) -> Result<Vec<(DateTime<Utc>, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                DATE_TRUNC('day', processed_at) AS day,
                SUM(amount_cents)::bigint AS daily_total
            FROM payments
            WHERE organization_id = $1
              AND status = 'completed'
              AND processed_at IS NOT NULL
            GROUP BY DATE_TRUNC('day', processed_at)
            ORDER BY day DESC
            LIMIT 90
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<DateTime<Utc>, _>("day")?,
                    r.try_get::<i64, _>("daily_total")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Get churn analysis: cancel reason, count
    pub async fn get_churn_analysis(&self, org_id: Uuid) -> Result<Vec<(String, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                COALESCE(cancel_reason, 'unknown') AS reason,
                COUNT(*) AS cnt
            FROM subscriptions
            WHERE organization_id = $1
              AND status = 'cancelled'
              AND cancelled_at IS NOT NULL
            GROUP BY cancel_reason
            ORDER BY cnt DESC
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("reason")?,
                    r.try_get::<i64, _>("cnt")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Get ledger balance for a specific account: (total debits, total credits)
    pub async fn get_ledger_balance(&self, org_id: Uuid, account_code: &str) -> Result<(i64, i64)> {
        let row = sqlx::query(
            r#"
            SELECT
                COALESCE(SUM(CASE WHEN entry_type = 'debit' THEN amount_cents ELSE 0 END), 0)::bigint AS total_debits,
                COALESCE(SUM(CASE WHEN entry_type = 'credit' THEN amount_cents ELSE 0 END), 0)::bigint AS total_credits
            FROM ledger_entries
            WHERE organization_id = $1
              AND account_code = $2
            "#,
        )
            .bind(org_id)
            .bind(account_code)
            .fetch_optional(&self.pool)
            .await?;

        match row {
            Some(r) => Ok((
                r.try_get::<i64, _>("total_debits")?,
                r.try_get::<i64, _>("total_credits")?,
            )),
            None => Ok((0, 0)),
        }
    }

    // ============================================================
    // Cross-domain queries (complex JOINs across multiple tables)
    // ============================================================

    /// Get user lifetime value: user_id, name, total spent from orders + payments
    pub async fn get_user_lifetime_value(
        &self,
        org_id: Uuid,
        limit: i32,
    ) -> Result<Vec<(Uuid, String, i64)>> {
        let rows = sqlx::query(
            r#"
            WITH user_order_spend AS (
                SELECT
                    u.id AS user_id,
                    u.name,
                    COALESCE(SUM(o.total_cents), 0)::bigint AS order_total
                FROM users u
                LEFT JOIN orders o ON o.user_id = u.id
                    AND o.status NOT IN ('cancelled', 'refunded')
                WHERE u.organization_id = $1
                GROUP BY u.id, u.name
            ),
            user_payment_spend AS (
                SELECT
                    u.id AS user_id,
                    COALESCE(SUM(p.amount_cents), 0)::bigint AS payment_total
                FROM users u
                LEFT JOIN payments p ON p.user_id = u.id
                    AND p.status = 'completed'
                WHERE u.organization_id = $1
                GROUP BY u.id
            )
            SELECT
                uos.user_id,
                uos.name,
                (uos.order_total + COALESCE(ups.payment_total, 0))::bigint AS total_spent
            FROM user_order_spend uos
            LEFT JOIN user_payment_spend ups ON ups.user_id = uos.user_id
            ORDER BY total_spent DESC
            LIMIT $2
            "#,
        )
        .bind(org_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<Uuid, _>("user_id")?,
                    r.try_get::<String, _>("name")?,
                    r.try_get::<i64, _>("total_spent")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Get product category revenue: category name, product count, total revenue
    pub async fn get_product_category_revenue(
        &self,
        org_id: Uuid,
    ) -> Result<Vec<(String, i64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                pc.name AS category_name,
                COUNT(DISTINCT p.id) AS product_count,
                COALESCE(SUM(oi.line_total_cents), 0)::bigint AS revenue
            FROM product_categories pc
            JOIN products p ON p.category_id = pc.id
            LEFT JOIN order_items oi ON oi.product_id = p.id
            LEFT JOIN orders o ON o.id = oi.order_id
                AND o.status NOT IN ('cancelled', 'refunded')
            WHERE pc.organization_id = $1
            GROUP BY pc.id, pc.name
            ORDER BY revenue DESC
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("category_name")?,
                    r.try_get::<i64, _>("product_count")?,
                    r.try_get::<i64, _>("revenue")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    // ============================================================
    // New queries for full 29-table coverage
    // ============================================================

    /// Organization summary: direct lookup on organizations table
    pub async fn get_organization_summary(&self, org_id: Uuid) -> Result<(String, i64)> {
        let row = sqlx::query(
            r#"
            SELECT o.name, COUNT(u.id) AS user_count
            FROM organizations o
            LEFT JOIN users u ON u.organization_id = o.id
            WHERE o.id = $1
            GROUP BY o.id, o.name
            "#,
        )
        .bind(org_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(r) => Ok((
                r.try_get::<String, _>("name")?,
                r.try_get::<i64, _>("user_count")?,
            )),
            None => Ok(("unknown".to_string(), 0)),
        }
    }

    /// Goal performance: name, completions_count, total_value
    pub async fn get_goal_performance(&self, org_id: Uuid) -> Result<Vec<(String, i32, f64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                name,
                completions_count,
                COALESCE(total_value, 0)::float8 AS total_value
            FROM goals
            WHERE organization_id = $1
              AND is_active = true
            ORDER BY completions_count DESC
            LIMIT 20
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("name")?,
                    r.try_get::<i32, _>("completions_count")?,
                    r.try_get::<f64, _>("total_value")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Goal completion stats from goal_completions table
    pub async fn get_goal_completion_funnel(&self, org_id: Uuid) -> Result<Vec<(Uuid, i64, f64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                goal_id,
                COUNT(*) AS completions,
                COALESCE(AVG(value), 0)::float8 AS avg_value
            FROM goal_completions
            WHERE organization_id = $1
            GROUP BY goal_id
            ORDER BY completions DESC
            LIMIT 20
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<Uuid, _>("goal_id")?,
                    r.try_get::<i64, _>("completions")?,
                    r.try_get::<f64, _>("avg_value")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Product tag distribution: tag, count (via products FK)
    pub async fn get_product_tag_distribution(&self, org_id: Uuid) -> Result<Vec<(String, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT pt.tag, COUNT(*) AS product_count
            FROM product_tags pt
            WHERE pt.product_id IN (
                SELECT id FROM products WHERE organization_id = $1
            )
            GROUP BY pt.tag
            ORDER BY product_count DESC
            LIMIT 20
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("tag")?,
                    r.try_get::<i64, _>("product_count")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Cart item stats: product_id, times added to cart, total quantity
    pub async fn get_cart_item_analysis(&self, org_id: Uuid) -> Result<Vec<(Uuid, i64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT ci.product_id, COUNT(*) AS times_in_cart,
                   COALESCE(SUM(ci.quantity), 0)::bigint AS total_quantity
            FROM cart_items ci
            WHERE ci.cart_id IN (
                SELECT id FROM carts WHERE organization_id = $1
            )
            GROUP BY ci.product_id
            ORDER BY times_in_cart DESC
            LIMIT 20
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<Uuid, _>("product_id")?,
                    r.try_get::<i64, _>("times_in_cart")?,
                    r.try_get::<i64, _>("total_quantity")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Chart of accounts: code, name, account_type
    pub async fn get_chart_of_accounts(
        &self,
        org_id: Uuid,
    ) -> Result<Vec<(String, String, String)>> {
        let rows = sqlx::query(
            r#"
            SELECT code, name, account_type
            FROM accounts
            WHERE organization_id = $1
              AND is_active = true
            ORDER BY code
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("code")?,
                    r.try_get::<String, _>("name")?,
                    r.try_get::<String, _>("account_type")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Invoice items: invoice_id, item count, total amount
    pub async fn get_invoice_line_item_breakdown(
        &self,
        org_id: Uuid,
    ) -> Result<Vec<(Uuid, i64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT ii.invoice_id, COUNT(*) AS item_count,
                   COALESCE(SUM(ii.amount_cents), 0)::bigint AS line_total
            FROM invoice_items ii
            WHERE ii.invoice_id IN (
                SELECT id FROM invoices WHERE organization_id = $1
            )
            GROUP BY ii.invoice_id
            ORDER BY line_total DESC
            LIMIT 20
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<Uuid, _>("invoice_id")?,
                    r.try_get::<i64, _>("item_count")?,
                    r.try_get::<i64, _>("line_total")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Refund summary: status, count, total refunded
    pub async fn get_refund_summary(&self, org_id: Uuid) -> Result<Vec<(String, i64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT status::text AS status, COUNT(*) AS cnt,
                   COALESCE(SUM(amount_cents), 0)::bigint AS total_refunded
            FROM refunds
            WHERE organization_id = $1
            GROUP BY status
            ORDER BY cnt DESC
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("status")?,
                    r.try_get::<i64, _>("cnt")?,
                    r.try_get::<i64, _>("total_refunded")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Subscription event stats: event_type, count, total MRR delta
    pub async fn get_subscription_event_timeline(
        &self,
        org_id: Uuid,
    ) -> Result<Vec<(String, i64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT event_type, COUNT(*) AS event_count,
                   COALESCE(SUM(mrr_delta_cents), 0)::bigint AS mrr_delta
            FROM subscription_events
            WHERE organization_id = $1
            GROUP BY event_type
            ORDER BY event_count DESC
            LIMIT 20
            "#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("event_type")?,
                    r.try_get::<i64, _>("event_count")?,
                    r.try_get::<i64, _>("mrr_delta")?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// DEPRECATED: Use get_user_ids_for_org + in-memory random selection
    #[allow(dead_code)]
    pub async fn get_random_user_ids(&self, org_id: Uuid, limit: u32) -> Result<Vec<Uuid>> {
        warn!("get_random_user_ids is deprecated - use get_user_ids_for_org with OrgIdCache");
        let rows = sqlx::query(
            "SELECT id FROM users WHERE organization_id = $1 ORDER BY RANDOM() LIMIT $2",
        )
        .bind(org_id)
        .bind(limit as i32)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| Ok(row.try_get("id")?))
            .collect::<Result<Vec<_>>>()
    }
}
