use std::collections::HashMap;
use std::time::Duration;

use rand::Rng;
use rand_distr::Exp;

use crate::scenario::{KeyspaceConfig, PayloadConfig, Phase, parse_duration};

/// A single command to be dispatched at a specific offset from phase start.
pub struct Arrival {
    /// Offset from phase start when this command should be dispatched.
    pub offset: Duration,
    pub spec: CommandSpec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandType {
    Get,
    Set,
}

pub struct CommandSpec {
    pub command_type: CommandType,
    pub key: String,
    /// For SET: the value to write. None for GET.
    pub value: Option<Vec<u8>>,
    /// Complete RESP request bytes, generated before the timed phase starts.
    pub encoded: Vec<u8>,
}

/// Generate the complete arrival schedule for a phase as a single global stream.
///
/// All arrival times are offsets from phase start, computed up front.
/// The arrival process is open-loop: times are determined by the
/// configured distribution, not by service times.
pub fn generate_arrivals(phase: &Phase, keyspace: &KeyspaceConfig) -> Vec<Arrival> {
    let duration = parse_duration(&phase.duration);
    let payload = phase.payload_config();
    let mut rng = rand::rng();

    let get_weight = *phase.commands.get("GET").unwrap_or(&0.0);
    let set_weight = *phase.commands.get("SET").unwrap_or(&0.0);
    let total_weight = get_weight + set_weight;
    assert!(total_weight > 0.0, "command mix must have nonzero total weight");
    let get_prob = get_weight / total_weight;

    let mut arrivals = Vec::new();
    let mut t = Duration::ZERO;

    match phase.arrival.mode.as_str() {
        "deterministic" => {
            let rate = phase.arrival.rate.expect("deterministic mode requires rate");
            assert!(rate > 0.0, "deterministic rate must be positive");
            let interval = Duration::from_secs_f64(1.0 / rate);
            while t < duration {
                arrivals.push(make_arrival(t, get_prob, keyspace, &payload, &mut rng));
                t += interval;
            }
        }
        "poisson" => {
            let lambda = phase.arrival.lambda.expect("poisson mode requires lambda");
            assert!(lambda > 0.0, "poisson lambda must be positive");
            let exp = Exp::new(lambda).expect("invalid lambda for exponential distribution");
            while t < duration {
                arrivals.push(make_arrival(t, get_prob, keyspace, &payload, &mut rng));
                let inter_arrival: f64 = rng.sample(exp);
                t += Duration::from_secs_f64(inter_arrival);
            }
        }
        other => panic!("unsupported arrival mode: {other}"),
    }

    arrivals
}

fn make_arrival(offset: Duration, get_prob: f64, keyspace: &KeyspaceConfig, payload: &PayloadConfig, rng: &mut impl Rng) -> Arrival {
    let command_type = if rng.random::<f64>() < get_prob {
        CommandType::Get
    } else {
        CommandType::Set
    };

    let key = generate_key(keyspace, rng);

    let value = match command_type {
        CommandType::Set => Some(generate_value(payload, rng)),
        CommandType::Get => None,
    };

    let encoded = match command_type {
        CommandType::Get => encode_get(&key),
        CommandType::Set => encode_set(&key, value.as_deref().unwrap_or(b"")),
    };

    Arrival {
        offset,
        spec: CommandSpec { command_type, key, value, encoded },
    }
}

pub fn planned_set_counts_for_arrivals(arrivals: &[Arrival]) -> Vec<u16> {
    let mut set_counts = HashMap::<&str, u16>::new();
    for arrival in arrivals {
        if matches!(arrival.spec.command_type, CommandType::Set) {
            let count = set_counts.entry(arrival.spec.key.as_str()).or_insert(0);
            *count = count.saturating_add(1);
        }
    }
    arrivals.iter().map(|arrival| set_counts.get(arrival.spec.key.as_str()).copied().unwrap_or(0)).collect()
}

fn generate_key(keyspace: &KeyspaceConfig, rng: &mut impl Rng) -> String {
    let prefix = keyspace.prefix.as_deref().unwrap_or("cacophony:");
    let idx: u64 = rng.random_range(0..keyspace.size);
    format!("{prefix}{idx}")
}

fn generate_value(payload: &PayloadConfig, rng: &mut impl Rng) -> Vec<u8> {
    let size = match payload {
        PayloadConfig::Fixed { size } => *size,
        PayloadConfig::Set { sizes } => {
            assert!(!sizes.is_empty(), "payload set must have at least one size");
            sizes[rng.random_range(0..sizes.len())]
        }
    };
    let mut value = vec![0u8; size];
    rng.fill(&mut value[..]);
    value
}

fn encode_command(args: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(args.iter().map(|arg| arg.len() + 16).sum());
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        buf.extend_from_slice(arg);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

fn encode_get(key: &str) -> Vec<u8> {
    encode_command(&[b"GET", key.as_bytes()])
}

fn encode_set(key: &str, value: &[u8]) -> Vec<u8> {
    encode_command(&[b"SET", key.as_bytes(), value])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_get_as_resp_array() {
        assert_eq!(encode_get("k"), b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
    }

    #[test]
    fn encodes_set_as_resp_array() {
        assert_eq!(encode_set("k", b"value"), b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nvalue\r\n");
    }

    #[test]
    fn returns_planned_set_count_for_each_key() {
        let arrivals = vec![
            Arrival {
                offset: Duration::ZERO,
                spec: CommandSpec {
                    command_type: CommandType::Set,
                    key: "k".to_string(),
                    value: Some(b"one".to_vec()),
                    encoded: Vec::new(),
                },
            },
            Arrival {
                offset: Duration::ZERO,
                spec: CommandSpec {
                    command_type: CommandType::Get,
                    key: "k".to_string(),
                    value: None,
                    encoded: Vec::new(),
                },
            },
            Arrival {
                offset: Duration::ZERO,
                spec: CommandSpec {
                    command_type: CommandType::Set,
                    key: "k".to_string(),
                    value: Some(b"two".to_vec()),
                    encoded: Vec::new(),
                },
            },
        ];

        let counts = planned_set_counts_for_arrivals(&arrivals);

        assert_eq!(counts, vec![2, 2, 2]);
    }
}
