use std::collections::HashMap;
use std::time::Duration;

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Scenario {
    pub meta: ScenarioMeta,
    pub keyspace: Option<KeyspaceConfig>,
    #[serde(rename = "phase")]
    pub phases: Vec<Phase>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ScenarioMeta {
    pub name: String,
    #[allow(dead_code)]
    pub description: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KeyspaceConfig {
    pub size: u64,
    #[allow(dead_code)]
    pub distribution: Option<String>,
    pub prefix: Option<String>,
}

impl Default for KeyspaceConfig {
    fn default() -> Self {
        Self {
            size: 100_000,
            distribution: Some("uniform".to_string()),
            prefix: Some("cacophony:".to_string()),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Phase {
    pub name: String,
    pub duration: String,
    pub connections: u32,
    pub pipeline_depth: Option<u32>,
    pub arrival: ArrivalConfig,
    pub commands: HashMap<String, f64>,
    pub payload: Option<PayloadConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ArrivalConfig {
    pub mode: String,
    /// For poisson mode: target arrival rate (requests/sec).
    pub lambda: Option<f64>,
    /// For deterministic mode: exact arrival rate (requests/sec).
    pub rate: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "mode")]
pub enum PayloadConfig {
    #[serde(rename = "fixed")]
    Fixed { size: usize },
    #[serde(rename = "set")]
    Set { sizes: Vec<usize> },
}

impl Default for PayloadConfig {
    fn default() -> Self {
        PayloadConfig::Fixed { size: 256 }
    }
}

/// Parse a duration string like "5s", "60s", "2m", "1h".
pub fn parse_duration(s: &str) -> Duration {
    let s = s.trim();
    if let Some(secs) = s.strip_suffix('s') {
        Duration::from_secs_f64(secs.parse().expect("invalid duration seconds"))
    } else if let Some(mins) = s.strip_suffix('m') {
        Duration::from_secs_f64(mins.parse::<f64>().expect("invalid duration minutes") * 60.0)
    } else if let Some(hours) = s.strip_suffix('h') {
        Duration::from_secs_f64(hours.parse::<f64>().expect("invalid duration hours") * 3600.0)
    } else {
        panic!("unsupported duration format: {s} (expected Ns, Nm, or Nh)")
    }
}

impl Phase {
    pub fn pipeline_depth(&self) -> u32 {
        let depth = self.pipeline_depth.unwrap_or(1);
        assert!(depth > 0, "pipeline_depth must be > 0 (phase '{}')", self.name);
        assert!(self.connections > 0, "connections must be > 0 (phase '{}')", self.name);
        depth
    }

    pub fn payload_config(&self) -> PayloadConfig {
        self.payload.clone().unwrap_or_default()
    }

    pub fn target_rate(&self) -> f64 {
        match self.arrival.mode.as_str() {
            "poisson" => self.arrival.lambda.expect("poisson mode requires lambda"),
            "deterministic" => self.arrival.rate.expect("deterministic mode requires rate"),
            other => panic!("unsupported arrival mode: {other}"),
        }
    }

    pub fn shard_for_loadgen(&self, shard_index: usize, shard_count: usize) -> Self {
        assert!(shard_count > 0, "shard_count must be > 0");
        assert!(shard_index < shard_count, "shard_index must be < shard_count");

        let mut phase = self.clone();
        let shard_count_u32 = u32::try_from(shard_count).expect("shard_count fits in u32");
        let shard_index_u32 = u32::try_from(shard_index).expect("shard_index fits in u32");
        let base_connections = self.connections / shard_count_u32;
        let extra = u32::from(shard_index_u32 < self.connections % shard_count_u32);
        phase.connections = (base_connections + extra).max(1);

        match phase.arrival.mode.as_str() {
            "poisson" => {
                let lambda = phase.arrival.lambda.expect("poisson mode requires lambda");
                phase.arrival.lambda = Some(lambda / shard_count as f64);
            }
            "deterministic" => {
                let rate = phase.arrival.rate.expect("deterministic mode requires rate");
                phase.arrival.rate = Some(rate / shard_count as f64);
            }
            other => panic!("unsupported arrival mode: {other}"),
        }

        phase
    }
}

impl KeyspaceConfig {
    pub fn shard_for_loadgen(&self, shard_index: usize) -> Self {
        let mut keyspace = self.clone();
        let prefix = keyspace.prefix.as_deref().unwrap_or("cacophony:");
        keyspace.prefix = Some(format!("{prefix}shard{shard_index}:"));
        keyspace
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{ArrivalConfig, KeyspaceConfig, PayloadConfig, Phase};

    #[test]
    fn shard_for_loadgen_splits_rate_and_connections() {
        let phase = Phase {
            name: "1M".to_string(),
            duration: "10s".to_string(),
            connections: 100,
            pipeline_depth: Some(10),
            arrival: ArrivalConfig {
                mode: "poisson".to_string(),
                lambda: Some(1_000_000.0),
                rate: None,
            },
            commands: HashMap::from([("get".to_string(), 1.0)]),
            payload: Some(PayloadConfig::Fixed { size: 256 }),
        };

        let connections: u32 = (0..8).map(|shard| phase.shard_for_loadgen(shard, 8).connections).sum();
        let rate: f64 = (0..8).map(|shard| phase.shard_for_loadgen(shard, 8).target_rate()).sum();

        assert_eq!(connections, 100);
        assert_eq!(rate, 1_000_000.0);
    }

    #[test]
    fn keyspace_shards_use_disjoint_prefixes() {
        let keyspace = KeyspaceConfig {
            size: 10_000,
            distribution: Some("uniform".to_string()),
            prefix: Some("bench:".to_string()),
        };

        assert_eq!(keyspace.shard_for_loadgen(0).prefix.as_deref(), Some("bench:shard0:"));
        assert_eq!(keyspace.shard_for_loadgen(7).prefix.as_deref(), Some("bench:shard7:"));
    }
}
