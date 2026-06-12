mod config;
mod job;
#[cfg(feature = "runtime")]
mod publisher;

pub use config::{BackoffConfig, MetadataConfig, SchedulerIntervals};
use futures::future::BoxFuture;
pub use job::{
    CapabilityChecker, CapabilityId, CollectorErrorPolicy, JobErrorMode, JobReport, JobStatus, MetadataBatch, MetadataJob,
    PermissiveCapabilities, SkipReason, UnknownCapabilities, job_timeout_duration, run_metadata_jobs, run_metadata_jobs_with_capabilities,
};
use linkme::distributed_slice;
#[cfg(feature = "runtime")]
pub use publisher::{MetadataOutputs, default_publisher, default_publisher_with_prefix};

use borsh::{BorshDeserialize, BorshSerialize};
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::fmt::Debug;
use std::io;
use std::io::Read;
use telemetry::TelemetryWrapper;
use utoipa::ToSchema;

/// Standardized traits for all metadata systems to ensure standardized behavior across endpoints
pub trait MetadataCollection {
    type Request;

    /// the collection metadata that must be passed to the endpoint
    fn request(&self) -> Self::Request;
    /// text based description of what the metadata is for
    fn description(&self) -> &'static str;
    /// stored metadata size in bytes
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
    /// Get the category/section name for this metadata
    fn category(&self) -> &'static str;
    /// Collection frequency recommendation in seconds
    fn interval(&self) -> SyncFrequency;
    /// Required MongoDB profiling level (defaults to none)
    fn profiling_requirement(&self) -> ProfilingRequirement {
        ProfilingRequirement::Off
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, ToSchema)]
pub enum SyncFrequency {
    High,
    Medium,
    Low,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataCollectorInfo {
    package: String,
    frequency: SyncFrequency,
}

impl MetadataCollectorInfo {
    pub fn new(package: impl Into<String>, frequency: SyncFrequency) -> Self {
        Self { package: package.into(), frequency }
    }

    pub fn package(&self) -> &str {
        &self.package
    }

    pub fn short_name(&self) -> &str {
        self.package.rsplit_once('.').map(|(_, tail)| tail).unwrap_or(self.package.as_str())
    }

    pub fn frequency(&self) -> SyncFrequency {
        self.frequency
    }
}

impl SyncFrequency {
    pub const fn as_str(self) -> &'static str {
        match self {
            SyncFrequency::High => "high",
            SyncFrequency::Medium => "medium",
            SyncFrequency::Low => "low",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "high" => Some(SyncFrequency::High),
            "medium" => Some(SyncFrequency::Medium),
            "low" => Some(SyncFrequency::Low),
            _ => None,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ProfilingRequirement {
    Off,
    Level1,
    Level2,
}

impl ProfilingRequirement {
    pub fn minimum_level(self) -> u8 {
        match self {
            ProfilingRequirement::Off => 0,
            ProfilingRequirement::Level1 => 1,
            ProfilingRequirement::Level2 => 2,
        }
    }

    pub fn requires_profiling(self) -> bool {
        !matches!(self, ProfilingRequirement::Off)
    }
}

/// Metadata collector that polls a live connection for fresh data.
///
/// Each implementation collects data from a live connection and returns
/// an updated copy of itself. The returned value replaces the old state in
/// the metadata struct.
///
/// The `&self` receiver acts as a template — the collector reads its own
/// configuration (which queries to run) and produces a fresh instance.
pub trait SyncCollector<A>: MetadataCollection + Clone + Send + Sync + 'static
where
    A: Clone + Send + Sync + 'static,
{
    /// Collect fresh metadata from `context` and return an updated instance.
    fn sync_metadata<'a>(
        &'a self,
        context: A,
        telemetry: &'a mut TelemetryWrapper,
        capabilities: &'a dyn CapabilityChecker,
    ) -> BoxFuture<'a, ResultEP<Self>>
    where
        Self: Sized;
}

pub trait SyncMetadata<A>: EpMetadata
where
    A: Clone + Send + Sync + 'static,
    Self: Serialize,
{
    fn collector_info() -> Vec<MetadataCollectorInfo>
    where
        Self: Default + Sized,
    {
        let mut metadata = Self::default();
        let mut collectors = Vec::new();

        for frequency in [SyncFrequency::High, SyncFrequency::Medium, SyncFrequency::Low] {
            collectors.extend(metadata.jobs(frequency).into_iter().map(|job| MetadataCollectorInfo::new(job.name(), job.frequency())));
        }

        collectors
    }

    fn jobs(&mut self, frequency: SyncFrequency) -> Vec<MetadataJob<A, Self>>
    where
        Self: Sized;

    fn packages(&mut self) -> Vec<MetadataJob<A, Self>>
    where
        Self: Sized,
    {
        let mut packages = Vec::new();
        for frequency in [SyncFrequency::High, SyncFrequency::Medium, SyncFrequency::Low] {
            packages.extend(self.jobs(frequency));
        }
        packages
    }

    fn package(&mut self, name: &str) -> Option<MetadataJob<A, Self>>
    where
        Self: Sized,
    {
        for frequency in [SyncFrequency::High, SyncFrequency::Medium, SyncFrequency::Low] {
            if let Some(job) = self.jobs(frequency).into_iter().find(|job| job.name() == name) {
                return Some(job);
            }
        }
        None
    }

    fn discover_capabilities<'a>(_connection: A, _telemetry: &'a mut TelemetryWrapper) -> BoxFuture<'a, Box<dyn CapabilityChecker>> {
        Box::pin(async { Box::new(UnknownCapabilities) as Box<dyn CapabilityChecker> })
    }
}

pub trait EpMetadata: Debug + Send + Sync + 'static {
    fn as_metadata(self: Box<Self>) -> Box<dyn EpMetadata>;
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
    fn kind(&self) -> EpKind;
    fn clone_box(&self) -> Box<dyn EpMetadata>;

    fn to_value(&self) -> Result<Value, serde_json::Error>;
    fn borsh_serialize(&self, writer: &mut dyn io::Write) -> io::Result<()>;
}

impl Clone for Box<dyn EpMetadata> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl Serialize for Box<dyn EpMetadata> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Helper struct to first capture the kind and data
        #[derive(Serialize)]
        struct MetadataHelper {
            kind: EpKind,
            #[serde(flatten)]
            data: Value,
        }

        let kind = self.kind();
        let value = self.to_value().map_err(serde::ser::Error::custom)?;
        let helper = MetadataHelper { kind, data: value };
        helper.serialize(serializer)
    }
}

impl BorshSerialize for Box<dyn EpMetadata> {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        // Write the kind first so the deserializer can route to the correct concrete type,
        // then serialize the metadata payload itself.
        borsh::to_writer(&mut *writer, &self.kind())?;
        self.borsh_serialize(writer)
    }
}

#[distributed_slice]
pub static METADATA_DESERIALIZERS: [(EpKind, fn(Value) -> Result<Box<dyn EpMetadata>, Box<dyn std::error::Error>>)];

impl<'de> Deserialize<'de> for Box<dyn EpMetadata> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Helper struct to first capture the kind and data
        #[derive(Deserialize)]
        struct MetadataHelper {
            kind: EpKind,
            #[serde(flatten)]
            data: Value,
        }

        let MetadataHelper { kind, data } = MetadataHelper::deserialize(deserializer)?;

        let metadata = 'metadata: {
            for &(de_kind, ref de_fn) in METADATA_DESERIALIZERS.iter() {
                if de_kind == kind {
                    break 'metadata de_fn(data).map_err(serde::de::Error::custom)?;
                }
            }

            return Err(serde::de::Error::custom(format!(
                "{kind} not supported; enable the corresponding feature in Cargo.toml"
            )));
        };

        Ok(metadata)
    }
}

#[distributed_slice]
pub static METADATA_BORSH_DESERIALIZERS: [(EpKind, fn(&mut dyn io::Read) -> io::Result<Box<dyn EpMetadata>>)];

impl BorshDeserialize for Box<dyn EpMetadata> {
    fn deserialize_reader<R: Read>(reader: &mut R) -> io::Result<Self> {
        // First deserialize the kind
        let kind = EpKind::deserialize_reader(reader)?;

        // Then deserialize the specific request based on the kind
        let metadata = 'metadata: {
            for &(de_kind, ref de_fn) in METADATA_BORSH_DESERIALIZERS.iter() {
                if de_kind == kind {
                    break 'metadata de_fn(reader)?;
                }
            }

            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{kind} not supported; enable the corresponding feature in Cargo.toml"),
            ));
        };

        Ok(metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use borsh::{BorshDeserialize, BorshSerialize};
    use linkme::distributed_slice;
    use serde::{Deserialize, Serialize};

    type DynMetadataError = Box<dyn std::error::Error>;
    type DynMetadataJsonDeserializer = fn(Value) -> Result<Box<dyn EpMetadata>, DynMetadataError>;
    type DynMetadataBorshDeserializer = fn(&mut dyn io::Read) -> io::Result<Box<dyn EpMetadata>>;

    #[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
    struct TestMetadata {
        marker: u32,
    }

    impl EpMetadata for TestMetadata {
        fn as_metadata(self: Box<Self>) -> Box<dyn EpMetadata> {
            self
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }

        fn kind(&self) -> EpKind {
            EpKind::Http
        }

        fn clone_box(&self) -> Box<dyn EpMetadata> {
            Box::new(self.clone())
        }

        fn to_value(&self) -> Result<Value, serde_json::Error> {
            serde_json::to_value(self)
        }

        fn borsh_serialize(&self, writer: &mut dyn io::Write) -> io::Result<()> {
            borsh::to_writer(writer, self)
        }
    }

    #[distributed_slice(METADATA_DESERIALIZERS)]
    static TEST_METADATA_JSON: (EpKind, DynMetadataJsonDeserializer) = (EpKind::Http, test_metadata_deserializer);

    fn test_metadata_deserializer(value: Value) -> Result<Box<dyn EpMetadata>, DynMetadataError> {
        let metadata: TestMetadata = serde_json::from_value(value)?;
        Ok(Box::new(metadata))
    }

    #[distributed_slice(METADATA_BORSH_DESERIALIZERS)]
    static TEST_METADATA_BORSH: (EpKind, DynMetadataBorshDeserializer) = (EpKind::Http, test_metadata_borsh_deserializer);

    fn test_metadata_borsh_deserializer(read: &mut dyn io::Read) -> io::Result<Box<dyn EpMetadata>> {
        struct ReadHelper<'a>(&'a mut dyn io::Read);

        impl<'a> io::Read for ReadHelper<'a> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                self.0.read(buf)
            }
        }

        let metadata: TestMetadata = TestMetadata::deserialize_reader(&mut ReadHelper(read))?;
        Ok(Box::new(metadata))
    }

    #[test]
    fn borsh_round_trip_preserves_kind_and_payload() {
        let original = TestMetadata { marker: 42 };
        let boxed: Box<dyn EpMetadata> = Box::new(original.clone());

        let serialized = borsh::to_vec(&boxed).expect("serialization should succeed");
        let deserialized: Box<dyn EpMetadata> =
            Box::deserialize_reader(&mut serialized.as_slice()).expect("deserialization should succeed");

        assert_eq!(deserialized.kind(), EpKind::Http);
        let restored = deserialized.as_any().downcast_ref::<TestMetadata>().expect("downcast to TestMetadata");
        assert_eq!(restored.marker, original.marker);
    }
}
