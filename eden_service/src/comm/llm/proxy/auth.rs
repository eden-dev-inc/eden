use actix_http::header;
use actix_web::HttpRequest;
use chrono::{DateTime, Utc};
use dashmap::mapref::entry::Entry;
use endpoint_core::llm_core::LlmGatewayCredential;
use uuid::Uuid;

use super::keys::{ApiKey, hash_api_key};
use super::state::{ProxyGatewayState, current_budget_month_bucket};

#[derive(Debug, Clone)]
pub(super) struct ProxyRateLimitWindow {
    pub minute_bucket: i64,
    pub request_count: u32,
}

#[derive(Debug, Clone)]
pub(super) struct ProxyBudgetWindow {
    pub month_bucket: i32,
    pub used_tokens: u64,
}

pub(super) fn bearer_api_key(req: &HttpRequest) -> Option<String> {
    let header = req.headers().get(header::AUTHORIZATION)?.to_str().ok()?;
    LlmGatewayCredential::bearer_api_key(header).map(ToOwned::to_owned)
}

impl ProxyGatewayState {
    pub(super) fn resolve_plaintext_key(&self, plaintext_key: &str) -> Option<ApiKey> {
        let key_hash = hash_api_key(plaintext_key);
        let key_id = self.ids_by_hash.get(&key_hash).map(|entry| *entry.value())?;
        self.keys_by_id.get(&key_id).map(|entry| entry.clone())
    }

    pub(super) fn check_rate_limit(&self, key_id: Uuid, limit_rpm: Option<u32>, now: DateTime<Utc>) -> bool {
        let Some(limit) = limit_rpm.filter(|limit| *limit > 0) else {
            return true;
        };

        let minute_bucket = now.timestamp() / 60;

        match self.rate_limits.entry(key_id) {
            Entry::Occupied(mut entry) => {
                let window = entry.get_mut();
                if window.minute_bucket != minute_bucket {
                    window.minute_bucket = minute_bucket;
                    window.request_count = 1;
                    true
                } else if window.request_count >= limit {
                    false
                } else {
                    window.request_count = window.request_count.saturating_add(1);
                    true
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(ProxyRateLimitWindow { minute_bucket, request_count: 1 });
                true
            }
        }
    }

    pub(super) fn mark_last_used(&self, key_id: Uuid, timestamp: DateTime<Utc>) {
        if let Some(mut entry) = self.keys_by_id.get_mut(&key_id) {
            entry.last_used_at = Some(timestamp);
            entry.updated_at = timestamp;
        }
    }

    pub(super) fn check_budget_limit(
        &self,
        key_id: Uuid,
        budget_tokens_monthly: Option<u64>,
        reserved_tokens: Option<u64>,
        now: DateTime<Utc>,
    ) -> bool {
        let Some(limit) = budget_tokens_monthly.filter(|limit| *limit > 0) else {
            return true;
        };

        let month_bucket = current_budget_month_bucket(now);
        let used_tokens = self
            .budget_usage
            .get(&key_id)
            .filter(|entry| entry.month_bucket == month_bucket)
            .map(|entry| entry.used_tokens)
            .unwrap_or_default();

        used_tokens.saturating_add(reserved_tokens.unwrap_or_default()) <= limit
    }

    pub(super) fn record_budget_usage(&self, key_id: Uuid, budget_tokens_monthly: Option<u64>, used_tokens: u64, now: DateTime<Utc>) {
        if used_tokens == 0 || budget_tokens_monthly.filter(|limit| *limit > 0).is_none() {
            return;
        }

        let month_bucket = current_budget_month_bucket(now);

        match self.budget_usage.entry(key_id) {
            Entry::Occupied(mut entry) => {
                let window = entry.get_mut();
                if window.month_bucket != month_bucket {
                    window.month_bucket = month_bucket;
                    window.used_tokens = used_tokens;
                } else {
                    window.used_tokens = window.used_tokens.saturating_add(used_tokens);
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(ProxyBudgetWindow { month_bucket, used_tokens });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn rate_limiter_enforces_per_minute_limit() {
        let state = ProxyGatewayState::new();
        let key_id = Uuid::new_v4();
        let now = Utc::now();

        assert!(state.check_rate_limit(key_id, Some(2), now));
        assert!(state.check_rate_limit(key_id, Some(2), now));
        assert!(!state.check_rate_limit(key_id, Some(2), now));
    }

    #[test]
    fn budget_limiter_enforces_monthly_limit() {
        let state = ProxyGatewayState::new();
        let key_id = Uuid::new_v4();
        let march = Utc.with_ymd_and_hms(2026, 3, 15, 12, 0, 0).single().expect("timestamp should be valid");
        let april = Utc.with_ymd_and_hms(2026, 4, 1, 0, 0, 0).single().expect("timestamp should be valid");

        assert!(state.check_budget_limit(key_id, Some(100), Some(60), march));
        state.record_budget_usage(key_id, Some(100), 60, march);
        assert!(!state.check_budget_limit(key_id, Some(100), Some(50), march));
        assert!(state.check_budget_limit(key_id, Some(100), Some(50), april));
    }

    #[test]
    fn bearer_api_key_accepts_gateway_and_legacy_prefixes() {
        let req = actix_web::test::TestRequest::default()
            .insert_header((header::AUTHORIZATION, "Bearer eden-gateway-example"))
            .to_http_request();
        assert_eq!(bearer_api_key(&req).as_deref(), Some("eden-gateway-example"));

        let req = actix_web::test::TestRequest::default()
            .insert_header((header::AUTHORIZATION, "Bearer eden-proxy-example"))
            .to_http_request();
        assert_eq!(bearer_api_key(&req).as_deref(), Some("eden-proxy-example"));
    }
}
