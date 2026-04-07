// Activity Event Catalog
//
// Centralizes the Datadog-facing event names and low-cardinality tags used by
// logs and metrics so the demo exports a stable, analyzable activity stream.

#[derive(Clone, Copy)]
pub struct ActivityEventDescriptor {
    pub stream: &'static str,
    pub event_name: &'static str,
    pub tags: &'static [&'static str],
}

const STARTUP_CONFIGURATION_TAGS: &[&str] = &[
    "stream:lifecycle",
    "domain:platform",
    "component:startup",
    "dataset:configuration",
];
const CACHE_WARMUP_BULK_TAGS: &[&str] = &[
    "stream:lifecycle",
    "domain:cache",
    "component:warmup",
    "phase:bulk_populate",
];
const CACHE_WARMUP_REFRESH_TAGS: &[&str] = &[
    "stream:lifecycle",
    "domain:cache",
    "component:warmup",
    "phase:refresh",
];
const EVENT_BATCH_TAGS: &[&str] = &[
    "stream:events",
    "domain:activity",
    "component:event_simulator",
    "operation:batch",
];
const SYSTEM_SNAPSHOT_TAGS: &[&str] = &[
    "stream:system",
    "domain:platform",
    "component:system_monitor",
    "dataset:snapshot",
];
const RUNTIME_CONTROL_UPDATE_TAGS: &[&str] = &[
    "stream:control",
    "domain:platform",
    "component:runtime_controls",
    "dataset:update",
];
const ORGANIZATION_LIST_TAGS: &[&str] = &[
    "stream:query",
    "domain:tenant",
    "dataset:organizations",
    "operation:list",
];
const DASHBOARD_TAGS: &[&str] = &[
    "stream:query",
    "domain:analytics",
    "dataset:dashboard",
    "analysis:aggregate",
];
const OVERVIEW_24H_TAGS: &[&str] = &[
    "stream:query",
    "domain:analytics",
    "dataset:overview",
    "time_window:24h",
];
const OVERVIEW_1H_TAGS: &[&str] = &[
    "stream:query",
    "domain:analytics",
    "dataset:overview",
    "time_window:1h",
];
const HOURLY_METRICS_TAGS: &[&str] = &[
    "stream:query",
    "domain:analytics",
    "dataset:hourly_metrics",
    "granularity:hour",
];
const TOP_PAGES_TAGS: &[&str] = &[
    "stream:query",
    "domain:analytics",
    "dataset:top_pages",
    "analysis:ranking",
];
const EVENT_DISTRIBUTION_TAGS: &[&str] = &[
    "stream:query",
    "domain:analytics",
    "dataset:event_distribution",
    "analysis:composition",
];
const REFERRER_BREAKDOWN_TAGS: &[&str] = &[
    "stream:query",
    "domain:marketing",
    "dataset:referrers",
    "analysis:attribution",
];
const FUNNEL_ANALYSIS_TAGS: &[&str] = &[
    "stream:query",
    "domain:marketing",
    "dataset:funnel",
    "analysis:journey",
];
const DEVICE_BREAKDOWN_TAGS: &[&str] = &[
    "stream:query",
    "domain:analytics",
    "dataset:devices",
    "analysis:platform",
];
const GEO_BREAKDOWN_TAGS: &[&str] = &[
    "stream:query",
    "domain:analytics",
    "dataset:geo",
    "analysis:regional",
];
const COHORT_BREAKDOWN_TAGS: &[&str] = &[
    "stream:query",
    "domain:analytics",
    "dataset:cohort",
    "analysis:retention",
];
const USER_ACTIVITY_TAGS: &[&str] = &[
    "stream:query",
    "domain:analytics",
    "dataset:user_activity",
    "audience:user",
];
const PAGE_PERFORMANCE_TAGS: &[&str] = &[
    "stream:query",
    "domain:analytics",
    "dataset:page_performance",
    "analysis:web_vitals",
];
const SESSION_SNAPSHOT_TAGS: &[&str] = &[
    "stream:query",
    "domain:analytics",
    "dataset:sessions",
    "analysis:realtime",
];
const MARKETING_SNAPSHOT_TAGS: &[&str] = &[
    "stream:query",
    "domain:marketing",
    "dataset:snapshot",
    "analysis:campaign_health",
];
const COMMERCE_SNAPSHOT_TAGS: &[&str] = &[
    "stream:query",
    "domain:commerce",
    "dataset:snapshot",
    "analysis:revenue_health",
];
const STOREFRONT_TAGS: &[&str] = &[
    "stream:query",
    "domain:commerce",
    "dataset:storefront",
    "analysis:landing_page",
];
const CATALOG_TAGS: &[&str] = &[
    "stream:query",
    "domain:commerce",
    "dataset:catalog",
    "analysis:browse",
];
const CART_DETAIL_TAGS: &[&str] = &[
    "stream:query",
    "domain:commerce",
    "dataset:cart",
    "analysis:cart_state",
];
const REALTIME_STATS_TAGS: &[&str] = &[
    "stream:query",
    "domain:analytics",
    "dataset:realtime",
    "analysis:live_usage",
];
const EVENT_INGEST_TAGS: &[&str] = &[
    "stream:write",
    "domain:activity",
    "dataset:events",
    "operation:ingest",
];
const CART_CREATE_TAGS: &[&str] = &[
    "stream:write",
    "domain:commerce",
    "dataset:cart",
    "operation:create",
];
const CART_ADD_ITEM_TAGS: &[&str] = &[
    "stream:write",
    "domain:commerce",
    "dataset:cart",
    "operation:add_item",
];
const CART_CHECKOUT_TAGS: &[&str] = &[
    "stream:write",
    "domain:commerce",
    "dataset:checkout",
    "operation:complete",
];

pub const KNOWN_ACTIVITY_EVENT_NAMES: &[&str] = &[
    "analytics.startup.configuration",
    "analytics.cache_warmup.bulk_populate",
    "analytics.cache_warmup.refresh",
    "analytics.event_batch.processed",
    "analytics.system.snapshot",
    "analytics.runtime_control.updated",
    "analytics.organization_list.load",
    "analytics.dashboard.load",
    "analytics.overview.load.24h",
    "analytics.overview.load.1h",
    "analytics.hourly_metrics.load",
    "analytics.top_pages.load",
    "analytics.event_distribution.load",
    "analytics.referrer_breakdown.load",
    "analytics.funnel_analysis.load",
    "analytics.device_breakdown.load",
    "analytics.geo_breakdown.load",
    "analytics.cohort_breakdown.load",
    "analytics.user_activity.load",
    "analytics.page_performance.load",
    "analytics.session_snapshot.load",
    "analytics.marketing_snapshot.load",
    "analytics.commerce_snapshot.load",
    "analytics.storefront.load",
    "analytics.catalog.load",
    "analytics.cart.load",
    "analytics.realtime_stats.load",
    "analytics.event.ingest",
    "analytics.cart.create",
    "analytics.cart.add_item",
    "analytics.cart.checkout",
];

pub const KNOWN_ACTIVITY_ERROR_TYPES: &[&str] = &[
    "query_execution_error",
    "cache_get_error",
    "cache_set_error",
    "http_error",
    "validation_error",
    "cache_invalidation_error",
    "telemetry_export_error",
];

pub fn startup_configuration() -> ActivityEventDescriptor {
    ActivityEventDescriptor {
        stream: "startup",
        event_name: "analytics.startup.configuration",
        tags: STARTUP_CONFIGURATION_TAGS,
    }
}

pub fn cache_warmup(phase: &str) -> ActivityEventDescriptor {
    match phase {
        "refresh" => ActivityEventDescriptor {
            stream: "cache_warmup",
            event_name: "analytics.cache_warmup.refresh",
            tags: CACHE_WARMUP_REFRESH_TAGS,
        },
        _ => ActivityEventDescriptor {
            stream: "cache_warmup",
            event_name: "analytics.cache_warmup.bulk_populate",
            tags: CACHE_WARMUP_BULK_TAGS,
        },
    }
}

pub fn event_batch() -> ActivityEventDescriptor {
    ActivityEventDescriptor {
        stream: "events",
        event_name: "analytics.event_batch.processed",
        tags: EVENT_BATCH_TAGS,
    }
}

pub fn system_snapshot() -> ActivityEventDescriptor {
    ActivityEventDescriptor {
        stream: "system",
        event_name: "analytics.system.snapshot",
        tags: SYSTEM_SNAPSHOT_TAGS,
    }
}

pub fn runtime_control_updated() -> ActivityEventDescriptor {
    ActivityEventDescriptor {
        stream: "control",
        event_name: "analytics.runtime_control.updated",
        tags: RUNTIME_CONTROL_UPDATE_TAGS,
    }
}

pub fn event_ingest() -> ActivityEventDescriptor {
    ActivityEventDescriptor {
        stream: "write",
        event_name: "analytics.event.ingest",
        tags: EVENT_INGEST_TAGS,
    }
}

pub fn cart_created() -> ActivityEventDescriptor {
    ActivityEventDescriptor {
        stream: "write",
        event_name: "analytics.cart.create",
        tags: CART_CREATE_TAGS,
    }
}

pub fn cart_item_added() -> ActivityEventDescriptor {
    ActivityEventDescriptor {
        stream: "write",
        event_name: "analytics.cart.add_item",
        tags: CART_ADD_ITEM_TAGS,
    }
}

pub fn cart_checked_out() -> ActivityEventDescriptor {
    ActivityEventDescriptor {
        stream: "write",
        event_name: "analytics.cart.checkout",
        tags: CART_CHECKOUT_TAGS,
    }
}

pub fn query(query_type: &str) -> ActivityEventDescriptor {
    match query_type {
        "organization_list" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.organization_list.load",
            tags: ORGANIZATION_LIST_TAGS,
        },
        "dashboard" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.dashboard.load",
            tags: DASHBOARD_TAGS,
        },
        "analytics_overview_1h" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.overview.load.1h",
            tags: OVERVIEW_1H_TAGS,
        },
        "analytics_overview_24h" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.overview.load.24h",
            tags: OVERVIEW_24H_TAGS,
        },
        "hourly_metrics" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.hourly_metrics.load",
            tags: HOURLY_METRICS_TAGS,
        },
        "top_pages" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.top_pages.load",
            tags: TOP_PAGES_TAGS,
        },
        "event_distribution" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.event_distribution.load",
            tags: EVENT_DISTRIBUTION_TAGS,
        },
        "referrer_breakdown" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.referrer_breakdown.load",
            tags: REFERRER_BREAKDOWN_TAGS,
        },
        "funnel_analysis" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.funnel_analysis.load",
            tags: FUNNEL_ANALYSIS_TAGS,
        },
        "device_breakdown" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.device_breakdown.load",
            tags: DEVICE_BREAKDOWN_TAGS,
        },
        "geo_breakdown" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.geo_breakdown.load",
            tags: GEO_BREAKDOWN_TAGS,
        },
        "cohort_breakdown" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.cohort_breakdown.load",
            tags: COHORT_BREAKDOWN_TAGS,
        },
        "user_activity" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.user_activity.load",
            tags: USER_ACTIVITY_TAGS,
        },
        "page_performance" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.page_performance.load",
            tags: PAGE_PERFORMANCE_TAGS,
        },
        "session_snapshot" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.session_snapshot.load",
            tags: SESSION_SNAPSHOT_TAGS,
        },
        "marketing_snapshot" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.marketing_snapshot.load",
            tags: MARKETING_SNAPSHOT_TAGS,
        },
        "commerce_snapshot" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.commerce_snapshot.load",
            tags: COMMERCE_SNAPSHOT_TAGS,
        },
        "storefront" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.storefront.load",
            tags: STOREFRONT_TAGS,
        },
        "catalog" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.catalog.load",
            tags: CATALOG_TAGS,
        },
        "cart_detail" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.cart.load",
            tags: CART_DETAIL_TAGS,
        },
        "realtime_stats" => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.realtime_stats.load",
            tags: REALTIME_STATS_TAGS,
        },
        _ => ActivityEventDescriptor {
            stream: "query",
            event_name: "analytics.overview.load.24h",
            tags: OVERVIEW_24H_TAGS,
        },
    }
}
