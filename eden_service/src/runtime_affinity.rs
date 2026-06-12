use eden_config::GatewayCpuAffinityMode;
#[cfg(target_os = "linux")]
use std::collections::HashMap;
#[cfg(target_os = "linux")]
use std::fs;
use std::num::NonZeroUsize;
#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub const PROXY_SHARD_COUNT_ENV: &str = "EDEN_PROXY_SHARD_COUNT";
const DEFAULT_PROXY_SHARD_COUNT_FALLBACK: usize = 4;

#[cfg(target_os = "linux")]
const CPU_SYSFS_ROOT: &str = "/sys/devices/system/cpu";
#[cfg(target_os = "linux")]
const MIN_HETEROGENEITY_RATIO: f64 = 0.10;
#[cfg(target_os = "linux")]
const PERFORMANCE_THRESHOLD_RATIO: f64 = 0.90;

#[derive(Debug, Clone)]
pub struct PerformanceCoreSelection {
    pub logical_processor_ids: Vec<usize>,
    pub source: &'static str,
    pub threshold: u32,
    pub min_metric: u32,
    pub max_metric: u32,
}

#[derive(Debug, Clone)]
pub enum RuntimeAffinityPlan {
    PerformanceCores(PerformanceCoreSelection),
    Unpinned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyShardCountSource {
    Env,
    InvalidEnv,
    AvailableParallelism,
    AvailableParallelismFallback,
}

impl ProxyShardCountSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Env => PROXY_SHARD_COUNT_ENV,
            Self::InvalidEnv => "invalid_EDEN_PROXY_SHARD_COUNT",
            Self::AvailableParallelism => "available_parallelism",
            Self::AvailableParallelismFallback => "available_parallelism_fallback",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProxyShardRuntimeConfig {
    pub shard_count: usize,
    pub k_choice: usize,
    pub source: ProxyShardCountSource,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone)]
struct PhysicalCoreMetrics {
    logical_processor_ids: Vec<usize>,
    capacity: Option<u32>,
    max_freq_khz: Option<u32>,
}

pub fn configure_tokio_worker_affinity(
    builder: &mut tokio::runtime::Builder,
    mode: GatewayCpuAffinityMode,
) -> Result<RuntimeAffinityPlan, String> {
    let plan = match mode {
        GatewayCpuAffinityMode::Off => RuntimeAffinityPlan::Unpinned,
        GatewayCpuAffinityMode::Auto => match detect_performance_cores() {
            Ok(Some(selection)) => RuntimeAffinityPlan::PerformanceCores(selection),
            Ok(None) => RuntimeAffinityPlan::Unpinned,
            Err(_) => RuntimeAffinityPlan::Unpinned,
        },
        GatewayCpuAffinityMode::Perf => match detect_performance_cores() {
            Ok(Some(selection)) => RuntimeAffinityPlan::PerformanceCores(selection),
            Ok(None) => return Err("gateway_cpu_affinity=perf requires a detectable performance-core tier".to_string()),
            Err(error) => return Err(format!("gateway_cpu_affinity=perf requires performance-core detection to succeed: {error}")),
        },
    };

    if let RuntimeAffinityPlan::PerformanceCores(selection) = &plan {
        let logical_processor_ids = Arc::new(selection.logical_processor_ids.clone());
        let next_index = Arc::new(AtomicUsize::new(0));

        builder.on_thread_start(move || {
            let index = next_index.fetch_add(1, Ordering::Relaxed);
            let core_id = logical_processor_ids[index % logical_processor_ids.len()];

            if let Err(error) = pin_current_thread_to_core(core_id) {
                eprintln!("eden_service: failed to pin tokio worker thread to CPU {core_id}: {error}");
            }
        });
    }

    Ok(plan)
}

pub fn proxy_shard_runtime_config() -> ProxyShardRuntimeConfig {
    let env_value = std::env::var_os(PROXY_SHARD_COUNT_ENV);
    let env_value = env_value.as_deref().map(|value| value.to_str().ok_or(()));
    let available_parallelism = std::thread::available_parallelism().ok().map(NonZeroUsize::get);

    resolve_proxy_shard_runtime_config(env_value, available_parallelism)
}

fn resolve_proxy_shard_runtime_config(
    env_value: Option<Result<&str, ()>>,
    available_parallelism: Option<usize>,
) -> ProxyShardRuntimeConfig {
    if let Some(env_value) = env_value {
        let Ok(env_value) = env_value else {
            return proxy_shard_config(1, ProxyShardCountSource::InvalidEnv);
        };
        let Ok(shard_count) = env_value.trim().parse::<usize>() else {
            return proxy_shard_config(1, ProxyShardCountSource::InvalidEnv);
        };
        if shard_count == 0 {
            return proxy_shard_config(1, ProxyShardCountSource::InvalidEnv);
        }

        return proxy_shard_config(shard_count, ProxyShardCountSource::Env);
    }

    if let Some(available_parallelism) = available_parallelism {
        return proxy_shard_config(available_parallelism.max(1), ProxyShardCountSource::AvailableParallelism);
    }

    proxy_shard_config(DEFAULT_PROXY_SHARD_COUNT_FALLBACK, ProxyShardCountSource::AvailableParallelismFallback)
}

fn proxy_shard_config(shard_count: usize, source: ProxyShardCountSource) -> ProxyShardRuntimeConfig {
    let shard_count = shard_count.max(1);
    let k_choice = if shard_count >= 2 { 2 } else { 1 };

    ProxyShardRuntimeConfig { shard_count, k_choice, source }
}

fn pin_current_thread_to_core(core_id: usize) -> std::io::Result<()> {
    #[cfg(not(target_os = "linux"))]
    let _ = core_id;
    #[cfg(target_os = "linux")]
    {
        let mut cpuset: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        unsafe {
            libc::CPU_ZERO(&mut cpuset);
            libc::CPU_SET(core_id, &mut cpuset);
            let thread = libc::pthread_self();
            let result = libc::pthread_setaffinity_np(thread, std::mem::size_of::<libc::cpu_set_t>(), &cpuset);
            if result != 0 {
                return Err(std::io::Error::from_raw_os_error(result));
            }
        }
    }

    Ok(())
}

fn detect_performance_cores() -> std::io::Result<Option<PerformanceCoreSelection>> {
    #[cfg(not(target_os = "linux"))]
    {
        Ok(None)
    }

    #[cfg(target_os = "linux")]
    {
        let cores = read_physical_core_metrics()?;
        if cores.is_empty() {
            return Ok(None);
        }

        if let Some(selection) = select_performance_cores_by_metric(&cores, |core| core.capacity, "cpu_capacity") {
            return Ok(Some(selection));
        }

        if let Some(selection) = select_performance_cores_by_metric(&cores, |core| core.max_freq_khz, "cpuinfo_max_freq") {
            return Ok(Some(selection));
        }

        Ok(None)
    }
}

#[cfg(target_os = "linux")]
fn read_physical_core_metrics() -> std::io::Result<Vec<PhysicalCoreMetrics>> {
    let mut physical_core_map: HashMap<(usize, usize), PhysicalCoreMetrics> = HashMap::new();

    for logical_processor_id in read_logical_processor_ids()? {
        let topology_path = cpu_path(logical_processor_id).join("topology");
        let package_id = read_u32(topology_path.join("physical_package_id")).ok().map_or(0usize, |value| value as usize);
        let core_id = read_u32(topology_path.join("core_id")).ok().map_or(logical_processor_id, |value| value as usize);
        let entry = physical_core_map.entry((package_id, core_id)).or_insert_with(|| PhysicalCoreMetrics {
            logical_processor_ids: Vec::new(),
            capacity: None,
            max_freq_khz: None,
        });

        entry.logical_processor_ids.push(logical_processor_id);

        if let Ok(capacity) = read_u32(cpu_path(logical_processor_id).join("cpu_capacity")) {
            entry.capacity = Some(entry.capacity.map_or(capacity, |current| current.max(capacity)));
        }

        if let Ok(max_freq_khz) = read_u32(cpu_path(logical_processor_id).join("cpufreq").join("cpuinfo_max_freq")) {
            entry.max_freq_khz = Some(entry.max_freq_khz.map_or(max_freq_khz, |current| current.max(max_freq_khz)));
        }
    }

    let mut cores: Vec<_> = physical_core_map.into_values().collect();
    for core in &mut cores {
        core.logical_processor_ids.sort_unstable();
    }
    cores.sort_by(|left, right| left.logical_processor_ids.first().cmp(&right.logical_processor_ids.first()));

    Ok(cores)
}

#[cfg(target_os = "linux")]
fn read_logical_processor_ids() -> std::io::Result<Vec<usize>> {
    let mut logical_processor_ids = Vec::new();

    for entry in fs::read_dir(CPU_SYSFS_ROOT)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(cpu_suffix) = name.strip_prefix("cpu") else {
            continue;
        };
        let Ok(logical_processor_id) = cpu_suffix.parse::<usize>() else {
            continue;
        };

        if entry.path().join("topology").exists() {
            logical_processor_ids.push(logical_processor_id);
        }
    }

    logical_processor_ids.sort_unstable();
    Ok(logical_processor_ids)
}

#[cfg(target_os = "linux")]
fn select_performance_cores_by_metric(
    cores: &[PhysicalCoreMetrics],
    metric: impl Fn(&PhysicalCoreMetrics) -> Option<u32>,
    source: &'static str,
) -> Option<PerformanceCoreSelection> {
    let mut values = Vec::new();
    for core in cores {
        if let Some(value) = metric(core) {
            values.push(value);
        }
    }

    if values.len() < 2 {
        return None;
    }

    values.sort_unstable();
    values.dedup();
    if values.len() < 2 {
        return None;
    }

    let min_metric = *values.first()?;
    let max_metric = *values.last()?;
    if max_metric == 0 {
        return None;
    }

    let heterogeneity_ratio = (max_metric - min_metric) as f64 / max_metric as f64;
    if heterogeneity_ratio < MIN_HETEROGENEITY_RATIO {
        return None;
    }

    let threshold = (max_metric as f64 * PERFORMANCE_THRESHOLD_RATIO).round() as u32;
    let mut logical_processor_ids = Vec::new();
    let mut selected_cores = 0usize;

    for core in cores {
        let Some(core_metric) = metric(core) else {
            continue;
        };
        if core_metric < threshold {
            continue;
        }

        selected_cores += 1;
        logical_processor_ids.extend_from_slice(&core.logical_processor_ids);
    }

    if logical_processor_ids.is_empty() || selected_cores == cores.len() {
        return None;
    }

    logical_processor_ids.sort_unstable();
    logical_processor_ids.dedup();

    Some(PerformanceCoreSelection {
        logical_processor_ids,
        source,
        threshold,
        min_metric,
        max_metric,
    })
}

#[cfg(target_os = "linux")]
fn read_u32(path: PathBuf) -> std::io::Result<u32> {
    let value = fs::read_to_string(&path)?;
    value
        .trim()
        .parse::<u32>()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{path:?}: {error}")))
}

#[cfg(target_os = "linux")]
fn cpu_path(logical_processor_id: usize) -> PathBuf {
    Path::new(CPU_SYSFS_ROOT).join(format!("cpu{logical_processor_id}"))
}

#[cfg(test)]
mod proxy_shard_runtime_config_tests {
    use super::{ProxyShardCountSource, resolve_proxy_shard_runtime_config};

    #[test]
    fn proxy_shard_runtime_config_unset_env_uses_available_parallelism() {
        let config = resolve_proxy_shard_runtime_config(None, Some(8));

        assert_eq!(config.shard_count, 8);
        assert_eq!(config.k_choice, 2);
        assert_eq!(config.source, ProxyShardCountSource::AvailableParallelism);
    }

    #[test]
    fn proxy_shard_runtime_config_unset_env_uses_fallback_when_available_parallelism_unavailable() {
        let config = resolve_proxy_shard_runtime_config(None, None);

        assert_eq!(config.shard_count, 4);
        assert_eq!(config.k_choice, 2);
        assert_eq!(config.source, ProxyShardCountSource::AvailableParallelismFallback);
    }

    #[test]
    fn proxy_shard_runtime_config_valid_env_overrides_available_parallelism() {
        let config = resolve_proxy_shard_runtime_config(Some(Ok("3")), Some(8));

        assert_eq!(config.shard_count, 3);
        assert_eq!(config.k_choice, 2);
        assert_eq!(config.source, ProxyShardCountSource::Env);
    }

    #[test]
    fn proxy_shard_runtime_config_invalid_present_env_resolves_to_one() {
        for env_value in [Some(Ok("0")), Some(Ok("")), Some(Ok("abc")), Some(Ok("   ")), Some(Err(()))] {
            let config = resolve_proxy_shard_runtime_config(env_value, Some(8));

            assert_eq!(config.shard_count, 1);
            assert_eq!(config.k_choice, 1);
            assert_eq!(config.source, ProxyShardCountSource::InvalidEnv);
        }
    }

    #[test]
    fn proxy_shard_runtime_config_valid_whitespace_padded_env_parses_after_trim() {
        let config = resolve_proxy_shard_runtime_config(Some(Ok("  5\t")), Some(8));

        assert_eq!(config.shard_count, 5);
        assert_eq!(config.k_choice, 2);
        assert_eq!(config.source, ProxyShardCountSource::Env);
    }

    #[test]
    fn proxy_shard_runtime_config_k_choice_tracks_shard_count() {
        let one_shard = resolve_proxy_shard_runtime_config(Some(Ok("1")), Some(8));
        let two_shards = resolve_proxy_shard_runtime_config(Some(Ok("2")), Some(8));

        assert_eq!(one_shard.k_choice, 1);
        assert_eq!(two_shards.k_choice, 2);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{PhysicalCoreMetrics, select_performance_cores_by_metric};

    #[test]
    fn selects_high_tier_capacity_cores() {
        let cores = vec![
            PhysicalCoreMetrics {
                logical_processor_ids: vec![0],
                capacity: Some(718),
                max_freq_khz: None,
            },
            PhysicalCoreMetrics {
                logical_processor_ids: vec![1],
                capacity: Some(731),
                max_freq_khz: None,
            },
            PhysicalCoreMetrics {
                logical_processor_ids: vec![2],
                capacity: Some(997),
                max_freq_khz: None,
            },
            PhysicalCoreMetrics {
                logical_processor_ids: vec![3],
                capacity: Some(1024),
                max_freq_khz: None,
            },
        ];

        let selection =
            select_performance_cores_by_metric(&cores, |core| core.capacity, "cpu_capacity").expect("expected high-tier selection");

        assert_eq!(selection.logical_processor_ids, vec![2, 3]);
    }
}
