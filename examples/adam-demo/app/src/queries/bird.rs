use std::fs;
use std::sync::OnceLock;

use serde::Deserialize;
use serde_json::Value;

use super::Query;

#[derive(Debug, Deserialize)]
struct BirdManifest {
    #[allow(dead_code)]
    db_id: String,
    validated_queries: Vec<BirdManifestQuery>,
}

#[derive(Debug, Deserialize)]
struct BirdManifestQuery {
    #[allow(dead_code)]
    index: usize,
    question: String,
    #[allow(dead_code)]
    evidence: String,
    sql: String,
}

static BIRD_QUERIES: OnceLock<Vec<Query>> = OnceLock::new();

pub fn queries_for(endpoint_name: &str) -> Vec<Query> {
    match endpoint_name {
        "pg_bird" => load_queries().clone(),
        _ => load_queries().clone(),
    }
}

pub fn cross_db_queries() -> Vec<Vec<(&'static str, &'static str, Value)>> {
    Vec::new()
}

fn load_queries() -> &'static Vec<Query> {
    BIRD_QUERIES.get_or_init(|| {
        let manifest_path = std::env::var("BIRD_QUERY_MANIFEST")
            .unwrap_or_else(|_| "/app/data/bird/validated_queries.json".to_string());

        let fallback = || {
            vec![(
                "BIRD metadata status",
                super::pg_query(
                    "SELECT COUNT(*) AS public_table_count \
                     FROM information_schema.tables \
                     WHERE table_schema = 'public'",
                ),
            )]
        };

        let raw = match fs::read_to_string(&manifest_path) {
            Ok(contents) => contents,
            Err(_) => return fallback(),
        };

        let manifest: BirdManifest = match serde_json::from_str(&raw) {
            Ok(parsed) => parsed,
            Err(_) => return fallback(),
        };

        let queries: Vec<Query> = manifest
            .validated_queries
            .into_iter()
            .map(|entry| {
                let description = summarize_question(&entry.question);
                let leaked: &'static str = Box::leak(description.into_boxed_str());
                (leaked, super::pg_query(&entry.sql))
            })
            .collect();

        if queries.is_empty() {
            fallback()
        } else {
            queries
        }
    })
}

fn summarize_question(question: &str) -> String {
    const MAX_LEN: usize = 120;

    let compact = question.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.is_empty() {
        return "BIRD benchmark query".to_string();
    }

    if compact.len() <= MAX_LEN {
        format!("BIRD: {}", compact)
    } else {
        let shortened: String = compact.chars().take(MAX_LEN.saturating_sub(3)).collect();
        format!("BIRD: {}...", shortened)
    }
}
