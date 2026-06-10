use format::hashtype::HashType;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Defines where read requests are routed during a database migration.
/// The proxy intercepts read queries and routes them according to this strategy.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum ReadRouting {
    /// Route all reads to the old database only.
    /// Use before migration starts or after rollback.
    #[default]
    Old,

    /// Route all reads to the new database only.
    /// Use after migration is complete and old database is deprecated.
    New,

    /// Route reads based on a percentage or other routing strategy.
    /// Useful for gradual traffic shift or canary testing.
    Ratio { strategy: RoutingStrategy },

    /// Attempt to read from new database first; if row not found, fallback to old database.
    /// Useful when new database is being populated incrementally and may have gaps.
    FallbackOnMiss,

    /// Read from both databases and return the version with the newer timestamp.
    /// Requires tables to have a version/timestamp column for comparison.
    /// Use when writes go to both systems and you need most recent data.
    VersionCompare,

    /// Read from both databases and compare results.
    /// Useful for validation and testing to ensure data consistency.
    /// Always returns the old database result (old is authoritative during the shadow phase);
    /// any discrepancy with the new database result is logged and emitted as a metric.
    Replicated,
}

impl ReadRouting {
    /// Returns true if this routing strategy requires reading from the old database.
    pub fn needs_old(&self) -> bool {
        matches!(
            self,
            Self::Old | Self::FallbackOnMiss | Self::VersionCompare | Self::Replicated | Self::Ratio { .. }
        )
    }

    /// Returns true if this routing strategy requires reading from the new database.
    pub fn needs_new(&self) -> bool {
        matches!(
            self,
            Self::New | Self::FallbackOnMiss | Self::VersionCompare | Self::Replicated | Self::Ratio { .. }
        )
    }

    /// Returns true if this strategy reads from both databases simultaneously.
    pub fn reads_both(&self) -> bool {
        matches!(self, Self::VersionCompare | Self::Replicated)
    }
}

/// Defines where write requests are routed during a database migration.
/// The proxy intercepts write queries (INSERT, UPDATE, DELETE) and routes them according to this strategy.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum WriteRouting {
    /// Route all writes to the old database only.
    /// Use before migration starts or during rollback.
    #[default]
    Old,

    /// Route all writes to the new database only.
    /// Use after migration is complete and old database is deprecated.
    New,

    /// Route writes to both old and new databases with a specified consistency policy.
    /// This is the key transition state during migration.
    Replicated { policy: WriteConsistencyPolicy },
}

impl WriteRouting {
    /// Returns true if this routing strategy requires writing to the old database.
    pub fn needs_old(&self) -> bool {
        matches!(self, Self::Old | Self::Replicated { .. })
    }

    /// Returns true if this routing strategy requires writing to the new database.
    pub fn needs_new(&self) -> bool {
        matches!(self, Self::New | Self::Replicated { .. })
    }

    /// Returns true if this strategy writes to both databases.
    pub fn writes_both(&self) -> bool {
        matches!(self, Self::Replicated { .. })
    }

    /// Returns the consistency policy if writing to both databases.
    pub fn consistency_policy(&self) -> Option<&WriteConsistencyPolicy> {
        match self {
            Self::Replicated { policy } => Some(policy),
            _ => None,
        }
    }
}

/// Defines how to handle success/failure when writing to both databases simultaneously.
/// These policies determine what constitutes a successful write and how to handle partial failures.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum WriteConsistencyPolicy {
    /// Accept the write if either database succeeds. Log failures from the other database.
    /// Fastest option but allows divergence. Use for shadow testing new database.
    #[default]
    BestEffort,

    /// Old database must succeed; new database failures are logged but ignored.
    /// Old database is the source of truth. Use when testing new database without risk.
    OldAuthoritative,

    /// New database must succeed; old database failures are logged but ignored.
    /// New database is the source of truth. Use when old database is being deprecated.
    NewAuthoritative,

    /// Both databases must succeed or the entire write is rolled back/aborted.
    /// Strictest consistency but highest latency. Use when exact parity is required.
    BothRequired,

    /// Accept whichever database responds successfully first, ignore the other.
    /// Optimizes for latency over consistency. Rarely used in practice.
    LastWriteWins,

    /// Both databases participate in every transaction using PostgreSQL two-phase
    /// commit (PREPARE TRANSACTION / COMMIT PREPARED). Every statement executes
    /// on both databases in real-time. On COMMIT, both sides PREPARE before either
    /// commits. If either PREPARE fails, both roll back.
    ///
    /// Requires `max_prepared_transactions > 0` on both databases.
    /// Highest consistency but highest latency.
    TwoPhaseCommit,
}

impl WriteConsistencyPolicy {
    /// Returns true if a write operation should be considered successful.
    ///
    /// # Arguments
    /// * `old_success` - Whether the write to the old database succeeded
    /// * `new_success` - Whether the write to the new database succeeded
    pub fn is_write_successful(&self, old_success: bool, new_success: bool) -> bool {
        match self {
            Self::BestEffort | Self::LastWriteWins => old_success || new_success,
            Self::OldAuthoritative => old_success,
            Self::NewAuthoritative => new_success,
            Self::BothRequired | Self::TwoPhaseCommit => old_success && new_success,
        }
    }

    /// Returns true if a failure in the old database should cause the write to fail.
    pub fn old_failure_is_critical(&self) -> bool {
        matches!(self, Self::OldAuthoritative | Self::BothRequired | Self::TwoPhaseCommit)
    }

    /// Returns true if a failure in the new database should cause the write to fail.
    pub fn new_failure_is_critical(&self) -> bool {
        matches!(self, Self::NewAuthoritative | Self::BothRequired | Self::TwoPhaseCommit)
    }
}

/// Complete traffic routing configuration combining read and write strategies.
/// This defines how the proxy routes all database traffic during a migration.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TrafficRouting {
    /// Strategy for routing read queries (SELECT)
    pub read: ReadRouting,

    /// Strategy for routing write queries (INSERT, UPDATE, DELETE)
    pub write: WriteRouting,
}

impl TrafficRouting {
    /// Create a new traffic routing configuration.
    pub fn new(read: ReadRouting, write: WriteRouting) -> Self {
        Self { read, write }
    }

    pub fn read(&self) -> &ReadRouting {
        &self.read
    }

    pub fn write(&self) -> &WriteRouting {
        &self.write
    }

    /// Pre-migration: All traffic to old database.
    pub fn old_only() -> Self {
        Self { read: ReadRouting::Old, write: WriteRouting::Old }
    }

    /// Post-migration: All traffic to new database.
    pub fn new_only() -> Self {
        Self { read: ReadRouting::New, write: WriteRouting::New }
    }

    /// Dual-write with reads from old (common transition state).
    pub fn dual_write_read_old(policy: WriteConsistencyPolicy) -> Self {
        Self {
            read: ReadRouting::Old,
            write: WriteRouting::Replicated { policy },
        }
    }

    /// Dual-write with reads from new (preparing for cutover).
    pub fn dual_write_read_new(policy: WriteConsistencyPolicy) -> Self {
        Self {
            read: ReadRouting::New,
            write: WriteRouting::Replicated { policy },
        }
    }

    /// Shadow testing: writes go to both, reads only from old, new failures ignored.
    pub fn shadow_mode() -> Self {
        Self {
            read: ReadRouting::Old,
            write: WriteRouting::Replicated { policy: WriteConsistencyPolicy::OldAuthoritative },
        }
    }

    /// Returns true if any routing requires accessing the old database.
    pub fn needs_old(&self) -> bool {
        self.read.needs_old() || self.write.needs_old()
    }

    /// Returns true if any routing requires accessing the new database.
    pub fn needs_new(&self) -> bool {
        self.read.needs_new() || self.write.needs_new()
    }
}

/// Strategy for distributing traffic between databases when using ratio-based routing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum RoutingStrategy {
    /// Randomly distribute requests based on a percentage.
    /// ratio: 0.0 = 0% to new, 1.0 = 100% to new
    Random { ratio: f64 },
    /// Route based on consistent hash of user ID.
    /// Users with the same ID always route to the same system.
    /// ratio: 0.0 = 0% to new, 1.0 = 100% to new
    UserHash { ratio: f64 },
    // Future strategies:
    // Geographic,  // Route based on user's geographic location/region
    // HeaderBased, // Route based on specific HTTP headers in request
    // Cookie,      // Maintain sticky sessions using browser cookies
    // TimeOfDay,   // Route based on time of day (e.g., off-peak vs peak hours)
    // DeviceType,  // Route based on device type (mobile, desktop, tablet)
    // AccountTier, // Route based on user account level (free, premium, enterprise)
}

impl Default for RoutingStrategy {
    fn default() -> Self {
        Self::Random { ratio: 0.1 }
    }
}

impl RoutingStrategy {
    /// Determines whether a request should be routed to the new database.
    ///
    /// # Arguments
    /// * `random_value` - A random f64 between 0.0 and 1.0 for Random strategy
    ///
    /// # Returns
    /// true if request should go to new database, false for old database
    pub fn should_route_to_new(&self, random_value: f64) -> bool {
        match self {
            Self::Random { ratio } => random_value < *ratio,
            Self::UserHash { ratio } => random_value < *ratio,
        }
    }

    /// Determines whether a request on a given connection should be routed to the new database.
    /// Hashes the connection ID so the decision is stable for the entire lifetime of the
    /// connection, preventing read-after-write violations caused by per-command random sampling.
    ///
    /// # Arguments
    /// * `connection_id` - Stable numeric ID for the current client connection
    ///
    /// # Returns
    /// true if request should go to new database, false for old database
    pub fn should_route_to_new_for_connection(&self, connection_id: u64) -> bool {
        let hash = HashType::hash(&connection_id.to_le_bytes());
        let normalized = hash.normalized_value();
        match self {
            Self::Random { ratio } => normalized < *ratio,
            Self::UserHash { ratio } => normalized < *ratio,
        }
    }

    /// Determines whether a request from a specific user should be routed to the new database.
    /// Uses consistent hashing so the same user always routes to the same system.
    ///
    /// # Arguments
    /// * `user_id` - A string identifier for the user (e.g., user UUID)
    ///
    /// # Returns
    /// true if request should go to new database, false for old database
    pub fn should_route_to_new_for_user(&self, user_id: &str) -> bool {
        match self {
            Self::UserHash { ratio } => {
                let hash = HashType::hash(user_id.as_bytes());
                let normalized = hash.normalized_value();
                normalized < *ratio
            }
            Self::Random { ratio } => {
                use rand::Rng;
                rand::rng().random::<f64>() < *ratio
            }
        }
    }
}
