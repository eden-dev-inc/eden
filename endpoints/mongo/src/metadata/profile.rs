use endpoint_types::metadata::ProfilingRequirement;
use error::{EpError, ResultEP};
use mongo_core::MongoAsync;
use mongodb::bson::doc;
use std::time::Duration;
use tokio::time::timeout;

/// Observed MongoDB profiling level.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum MongoProfilingLevel {
    Off,
    Level1,
    Level2,
    Unknown,
}

impl MongoProfilingLevel {
    pub fn as_u8(self) -> u8 {
        match self {
            MongoProfilingLevel::Off => 0,
            MongoProfilingLevel::Level1 => 1,
            MongoProfilingLevel::Level2 => 2,
            MongoProfilingLevel::Unknown => 0,
        }
    }

    pub fn satisfies(self, requirement: ProfilingRequirement) -> bool {
        self.as_u8() >= requirement.minimum_level()
    }
}

/// Set the profiling level on the admin database and return the previous level.
pub async fn ensure_profiling_level(context: MongoAsync, level: MongoProfilingLevel, slow_ms: u64) -> ResultEP<MongoProfilingLevel> {
    let target = match level {
        MongoProfilingLevel::Off => 0,
        MongoProfilingLevel::Level1 => 1,
        MongoProfilingLevel::Level2 => 2,
        MongoProfilingLevel::Unknown => return Err(EpError::metadata("cannot set profiling to unknown level")),
    };

    let client = context.get().await.map_err(EpError::connect)?;
    let admin_db = client.database("admin");
    let result = timeout(
        Duration::from_secs(5),
        admin_db.run_command(doc! { "profile": target, "slowms": slow_ms as i64 }, None),
    )
    .await
    .map_err(|_| EpError::metadata("Timeout setting profile level"))?
    .map_err(EpError::database)?;

    let was = result.get_i32("was").unwrap_or(0);
    Ok(match was {
        0 => MongoProfilingLevel::Off,
        1 => MongoProfilingLevel::Level1,
        2 => MongoProfilingLevel::Level2,
        _ => MongoProfilingLevel::Unknown,
    })
}

/// Fetch the current profiling level from the admin database.
pub async fn fetch_profiling_level(context: MongoAsync) -> ResultEP<MongoProfilingLevel> {
    let client = context.get().await.map_err(EpError::connect)?;
    let admin_db = client.database("admin");

    let result = timeout(Duration::from_secs(5), admin_db.run_command(doc! { "profile": -1 }, None))
        .await
        .map_err(|_| EpError::metadata("Query timeout for profile command"))?
        .map_err(EpError::database)?;

    let level = result.get_i32("was").or_else(|_| result.get_i32("profile")).unwrap_or(0);

    Ok(match level {
        0 => MongoProfilingLevel::Off,
        1 => MongoProfilingLevel::Level1,
        2 => MongoProfilingLevel::Level2,
        _ => MongoProfilingLevel::Unknown,
    })
}
