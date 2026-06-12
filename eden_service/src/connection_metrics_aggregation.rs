//! Organization-scoped roll-up for connection metrics.
//!
//! Several metric streams (endpoint pool gauges, proxy active
//! connections, in-flight API requests) are emitted with raw labels
//! by their producers. Reporting them per-organization requires regrouping
//! the raw points by `org_uuid` (or whichever organization axis is
//! configured) and summing the counts.
//!
//! [`aggregate_connection_metrics_by_tenant`] consumes the raw label
//! sets, performs the regrouping in-process, and yields a
//! [`TenantConnectionMetrics`] per organization ready for emission. This
//! keeps the cross-organization aggregation logic in one place rather than
//! duplicating it in every metrics callsite.

use std::collections::HashMap;

use eden_core::telemetry::labels::LABEL_ORG_UUID;
use fast_telemetry::DynamicLabelSet;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TenantConnectionMetrics {
    pub endpoint_connections_total: i64,
    pub endpoint_connections_by_type: HashMap<String, i64>,
    pub endpoint_connections_by_uuid: HashMap<String, i64>,
    pub endpoint_connections_in_use: i64,
    pub endpoint_connections_in_use_by_uuid: HashMap<String, i64>,
    pub proxy_connections_total: i64,
    pub proxy_connections_by_endpoint: HashMap<String, i64>,
    pub proxy_connections_by_client: HashMap<String, i64>,
    pub active_requests: i64,
    pub api_requests_total: u64,
}

pub(crate) fn aggregate_connection_metrics_by_tenant(
    endpoint_open: &[(&'static str, String, i64)],
    endpoint_in_use: &[(&'static str, String, i64)],
    proxy_active: &[(String, i64)],
    proxy_clients: &[(String, String, i64)],
    active_requests: &[(DynamicLabelSet, i64)],
    request_counts: &[(DynamicLabelSet, isize)],
    endpoint_tenants: &HashMap<String, String>,
    interlay_tenants: &HashMap<String, String>,
) -> HashMap<String, TenantConnectionMetrics> {
    let mut by_tenant = HashMap::<String, TenantConnectionMetrics>::new();

    for (db_type, endpoint_uuid, count) in endpoint_open {
        if endpoint_uuid.is_empty() || *count <= 0 {
            continue;
        }
        let Some(organization_uuid) = endpoint_tenants.get(endpoint_uuid) else {
            continue;
        };
        let metrics = by_tenant.entry(organization_uuid.clone()).or_default();
        metrics.endpoint_connections_total += *count;
        *metrics.endpoint_connections_by_type.entry((*db_type).to_string()).or_insert(0) += *count;
        *metrics.endpoint_connections_by_uuid.entry(endpoint_uuid.clone()).or_insert(0) += *count;
    }

    for (_db_type, endpoint_uuid, count) in endpoint_in_use {
        if endpoint_uuid.is_empty() || *count <= 0 {
            continue;
        }
        let Some(organization_uuid) = endpoint_tenants.get(endpoint_uuid) else {
            continue;
        };
        let metrics = by_tenant.entry(organization_uuid.clone()).or_default();
        metrics.endpoint_connections_in_use += *count;
        *metrics.endpoint_connections_in_use_by_uuid.entry(endpoint_uuid.clone()).or_insert(0) += *count;
    }

    for (interlay_id, count) in proxy_active {
        if *count <= 0 {
            continue;
        }
        let Some(organization_uuid) = interlay_tenants.get(interlay_id) else {
            continue;
        };
        let metrics = by_tenant.entry(organization_uuid.clone()).or_default();
        metrics.proxy_connections_total += *count;
        *metrics.proxy_connections_by_endpoint.entry(interlay_id.clone()).or_insert(0) += *count;
    }

    for (client_ip, interlay_id, count) in proxy_clients {
        if *count <= 0 {
            continue;
        }
        let Some(organization_uuid) = interlay_tenants.get(interlay_id) else {
            continue;
        };
        let metrics = by_tenant.entry(organization_uuid.clone()).or_default();
        *metrics.proxy_connections_by_client.entry(format!("{client_ip}|{interlay_id}")).or_insert(0) += *count;
    }

    for (labels, count) in active_requests {
        if *count <= 0 {
            continue;
        }
        let Some(organization_uuid) = extract_org_uuid(labels) else {
            continue;
        };
        by_tenant.entry(organization_uuid.to_string()).or_default().active_requests += *count;
    }

    for (labels, count) in request_counts {
        if *count <= 0 {
            continue;
        }
        let Some(organization_uuid) = extract_org_uuid(labels) else {
            continue;
        };
        by_tenant.entry(organization_uuid.to_string()).or_default().api_requests_total += *count as u64;
    }

    by_tenant
}

fn extract_org_uuid(labels: &DynamicLabelSet) -> Option<&str> {
    labels.pairs().iter().find_map(|(key, value)| (key == LABEL_ORG_UUID).then_some(value.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> DynamicLabelSet {
        DynamicLabelSet::from_pairs(pairs)
    }

    #[test]
    fn aggregates_metrics_per_tenant() {
        let tenant_a = "11111111-1111-1111-1111-111111111111".to_string();
        let tenant_b = "22222222-2222-2222-2222-222222222222".to_string();
        let endpoint_a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string();
        let endpoint_b = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".to_string();
        let interlay_a = "cccccccc-cccc-cccc-cccc-cccccccccccc".to_string();
        let interlay_b = "dddddddd-dddd-dddd-dddd-dddddddddddd".to_string();

        let aggregated = aggregate_connection_metrics_by_tenant(
            &[
                ("redis", endpoint_a.clone(), 3),
                ("postgres", endpoint_b.clone(), 5),
                ("redis", String::new(), 9),
            ],
            &[("redis", endpoint_a.clone(), 2), ("postgres", endpoint_b.clone(), 4)],
            &[(interlay_a.clone(), 7), (interlay_b.clone(), 11)],
            &[
                ("10.0.0.1".to_string(), interlay_a.clone(), 2),
                ("10.0.0.2".to_string(), interlay_b.clone(), 3),
            ],
            &[
                (labels(&[(LABEL_ORG_UUID, &tenant_a)]), 13),
                (labels(&[(LABEL_ORG_UUID, &tenant_b)]), 17),
            ],
            &[
                (labels(&[(LABEL_ORG_UUID, &tenant_a)]), 19),
                (labels(&[(LABEL_ORG_UUID, &tenant_b)]), 23),
            ],
            &HashMap::from([(endpoint_a.clone(), tenant_a.clone()), (endpoint_b.clone(), tenant_b.clone())]),
            &HashMap::from([(interlay_a.clone(), tenant_a.clone()), (interlay_b.clone(), tenant_b.clone())]),
        );

        let tenant_a_metrics = aggregated.get(&tenant_a).expect("tenant A metrics");
        assert_eq!(tenant_a_metrics.endpoint_connections_total, 3);
        assert_eq!(tenant_a_metrics.endpoint_connections_in_use, 2);
        assert_eq!(tenant_a_metrics.proxy_connections_total, 7);
        assert_eq!(tenant_a_metrics.active_requests, 13);
        assert_eq!(tenant_a_metrics.api_requests_total, 19);
        assert_eq!(tenant_a_metrics.endpoint_connections_by_uuid.get(&endpoint_a), Some(&3));
        assert_eq!(tenant_a_metrics.proxy_connections_by_client.get(&format!("10.0.0.1|{interlay_a}")), Some(&2));

        let tenant_b_metrics = aggregated.get(&tenant_b).expect("tenant B metrics");
        assert_eq!(tenant_b_metrics.endpoint_connections_total, 5);
        assert_eq!(tenant_b_metrics.endpoint_connections_in_use, 4);
        assert_eq!(tenant_b_metrics.proxy_connections_total, 11);
        assert_eq!(tenant_b_metrics.active_requests, 17);
        assert_eq!(tenant_b_metrics.api_requests_total, 23);
        assert_eq!(tenant_b_metrics.endpoint_connections_by_uuid.get(&endpoint_b), Some(&5));
        assert_eq!(tenant_b_metrics.proxy_connections_by_client.get(&format!("10.0.0.2|{interlay_b}")), Some(&3));
    }

    #[test]
    fn ignores_unmapped_connections_but_keeps_tenant_request_metrics() {
        let tenant = "11111111-1111-1111-1111-111111111111".to_string();
        let aggregated = aggregate_connection_metrics_by_tenant(
            &[("redis", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string(), 3)],
            &[],
            &[],
            &[],
            &[(labels(&[(LABEL_ORG_UUID, &tenant)]), 5)],
            &[],
            &HashMap::new(),
            &HashMap::new(),
        );

        let metrics = aggregated.get(&tenant).expect("tenant metrics");
        assert_eq!(metrics.endpoint_connections_total, 0);
        assert_eq!(metrics.active_requests, 5);
    }
}
