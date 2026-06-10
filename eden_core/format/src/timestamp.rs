use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Datelike, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use postgres::types::private::BytesMut;
use postgres::types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::ops::Deref;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};
use utoipa::ToSchema;

#[derive(PartialEq, Eq, Clone, Default, BorshDeserialize, BorshSerialize, ToSchema, JsonSchema)]
/// Duration wrapper for PostgreSQL interval type.
pub struct DurationWrapper(i64);

impl Debug for DurationWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", chrono::Duration::from(self))
    }
}

impl Display for DurationWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", chrono::Duration::from(self))
    }
}

impl DurationWrapper {
    pub fn into_inner(self) -> i64 {
        self.0
    }
    pub fn as_chrono_duration(&self) -> chrono::Duration {
        chrono::Duration::from(self)
    }
    pub fn as_duration(&self) -> chrono::Duration {
        chrono::Duration::from(self)
    }
    pub fn as_nanos(&self) -> u64 {
        self.0 as u64
    }
}

impl From<chrono::Duration> for DurationWrapper {
    fn from(t: chrono::Duration) -> Self {
        Self(t.num_nanoseconds().unwrap_or_default())
    }
}

impl From<&DurationWrapper> for chrono::Duration {
    fn from(dt: &DurationWrapper) -> Self {
        chrono::Duration::nanoseconds(dt.0)
    }
}

// Implement Serialize by converting to DateTime<Utc> and serializing that.
impl Serialize for DurationWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_chrono_duration().serialize(serializer)
    }
}

// Implement Deserialize by deserializing a DateTime<Utc> and converting it into a DateTimeWrapper.
impl<'de> Deserialize<'de> for DurationWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let dt = chrono::Duration::deserialize(deserializer)?;
        Ok(dt.into())
    }
}

#[derive(PartialEq, PartialOrd, Ord, Eq, Clone, Default, BorshDeserialize, BorshSerialize, ToSchema, JsonSchema)]
/// UTC datetime wrapper for PostgreSQL timestamptz type.
pub struct DateTimeWrapper(i64);

impl Debug for DateTimeWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", DateTime::from(self))
    }
}

impl Display for DateTimeWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", DateTime::from(self))
    }
}

impl DateTimeWrapper {
    fn normalize_to_micro(dt: DateTime<Utc>) -> i64 {
        let nanos = dt.timestamp_nanos_opt().unwrap_or_default();
        nanos - nanos.rem_euclid(1_000)
    }

    pub fn now() -> Self {
        // Postgres timestamptz stores microsecond precision; normalize to match DB values.
        Self(Self::normalize_to_micro(Utc::now()))
    }
    pub fn into_inner(self) -> i64 {
        self.0
    }
    pub fn as_datetime(&self) -> DateTime<Utc> {
        DateTime::from(self)
    }
    pub fn elapsed(&self) -> Duration {
        Duration::from_secs((Self::now().as_datetime().timestamp() - self.as_datetime().timestamp()) as u64)
    }
}

impl From<DateTime<Utc>> for DateTimeWrapper {
    fn from(t: DateTime<Utc>) -> Self {
        Self(Self::normalize_to_micro(t))
    }
}

impl From<&DateTimeWrapper> for DateTime<Utc> {
    fn from(dt: &DateTimeWrapper) -> Self {
        DateTime::from_timestamp_nanos(dt.0)
    }
}

// Implement Serialize by converting to DateTime<Utc> and serializing that.
impl Serialize for DateTimeWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_datetime().serialize(serializer)
    }
}

// Implement Deserialize by deserializing a DateTime<Utc> and converting it into a DateTimeWrapper.
impl<'de> Deserialize<'de> for DateTimeWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let dt = DateTime::<Utc>::deserialize(deserializer)?;
        Ok(dt.into())
    }
}

impl<'a> FromSql<'a> for DateTimeWrapper {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match *ty {
            Type::TIMESTAMPTZ | Type::TIMESTAMP => {
                let dt = DateTime::<Utc>::from_sql(ty, raw)?;
                Ok(DateTimeWrapper::from(dt))
            }
            Type::INT8 => {
                let nanos = i64::from_sql(ty, raw)?;
                Ok(DateTimeWrapper(nanos))
            }
            Type::JSON | Type::JSONB => {
                let json_str = std::str::from_utf8(raw)?;
                let dt: DateTime<Utc> = serde_json::from_str(json_str)?;
                Ok(DateTimeWrapper::from(dt))
            }
            Type::TEXT | Type::VARCHAR => {
                let text_str = std::str::from_utf8(raw)?;
                let dt: DateTime<Utc> = text_str.parse()?;
                Ok(DateTimeWrapper::from(dt))
            }
            _ => Err(format!("cannot convert from SQL type {ty} to DateTimeWrapper").into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(
            *ty,
            Type::TIMESTAMPTZ | Type::TIMESTAMP | Type::INT8 | Type::JSON | Type::JSONB | Type::TEXT | Type::VARCHAR
        )
    }
}

impl ToSql for DateTimeWrapper {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match *ty {
            Type::TIMESTAMPTZ | Type::TIMESTAMP => {
                let dt = self.as_datetime();
                dt.to_sql(ty, out)
            }
            Type::INT8 => self.0.to_sql(ty, out),
            Type::JSON | Type::JSONB => {
                let dt = self.as_datetime();
                let json_string = serde_json::to_string(&dt)?;
                json_string.to_sql(ty, out)
            }
            Type::TEXT | Type::VARCHAR => {
                let dt = self.as_datetime();
                dt.to_string().to_sql(ty, out)
            }
            _ => Err(format!("cannot convert DateTimeWrapper to SQL type {ty}").into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(
            *ty,
            Type::TIMESTAMPTZ | Type::TIMESTAMP | Type::INT8 | Type::JSON | Type::JSONB | Type::TEXT | Type::VARCHAR
        )
    }

    to_sql_checked!();
}

#[derive(PartialEq, PartialOrd, Ord, Eq, Clone, Default, BorshDeserialize, BorshSerialize, ToSchema, JsonSchema)]
/// Local datetime wrapper for PostgreSQL timestamp type.
pub struct DateTimeLocalWrapper(i64);

impl Debug for DateTimeLocalWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", DateTime::from(self))
    }
}

impl Display for DateTimeLocalWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", DateTime::from(self))
    }
}

impl DateTimeLocalWrapper {
    pub fn now() -> Self {
        Self(Utc::now().timestamp_nanos_opt().unwrap_or_default())
    }
    pub fn into_inner(self) -> i64 {
        self.0
    }
    pub fn as_datetime(&self) -> DateTime<Local> {
        DateTime::from(self)
    }
}

impl From<DateTime<Utc>> for DateTimeLocalWrapper {
    fn from(t: DateTime<Utc>) -> Self {
        Self(t.timestamp_nanos_opt().unwrap_or_default())
    }
}

impl From<&DateTimeLocalWrapper> for DateTime<Local> {
    fn from(dt: &DateTimeLocalWrapper) -> Self {
        Local.timestamp_nanos(dt.0)
    }
}

// Implement Serialize by converting to DateTime<Utc> and serializing that.
impl Serialize for DateTimeLocalWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_datetime().serialize(serializer)
    }
}

// Implement Deserialize by deserializing a DateTime<Utc> and converting it into a DateTimeWrapper.
impl<'de> Deserialize<'de> for DateTimeLocalWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let dt = DateTime::<Utc>::deserialize(deserializer)?;
        Ok(dt.into())
    }
}

#[derive(PartialEq, PartialOrd, Ord, Eq, Clone, Default, BorshDeserialize, BorshSerialize, ToSchema, JsonSchema)]
pub struct NaiveDateTimeWrapper {
    year: i32,
    month: u32,
    day: u32,
    hours: u32,
    minutes: u32,
    seconds: u32,
    nanoseconds: u32,
}

impl Debug for NaiveDateTimeWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", NaiveDateTime::from(self))
    }
}

impl Display for NaiveDateTimeWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", NaiveDateTime::from(self))
    }
}

impl NaiveDateTimeWrapper {
    pub fn now(year: i32, month: u32, day: u32, hours: u32, minutes: u32, seconds: u32, nanoseconds: u32) -> Self {
        Self { year, month, day, hours, minutes, seconds, nanoseconds }
    }
    pub fn as_naive_datetime(&self) -> NaiveDateTime {
        NaiveDateTime::from(self)
    }
    pub fn year(&self) -> i32 {
        self.year
    }
    pub fn month(&self) -> u32 {
        self.month
    }
    pub fn day(&self) -> u32 {
        self.day
    }
    pub fn hours(&self) -> u32 {
        self.hours
    }
    pub fn minutes(&self) -> u32 {
        self.minutes
    }
    pub fn seconds(&self) -> u32 {
        self.seconds
    }
    pub fn nanoseconds(&self) -> u32 {
        self.nanoseconds
    }
}

impl From<NaiveDateTime> for NaiveDateTimeWrapper {
    fn from(t: NaiveDateTime) -> Self {
        Self {
            year: t.year(),
            month: t.month(),
            day: t.day(),
            hours: t.hour(),
            minutes: t.minute(),
            seconds: t.second(),
            nanoseconds: t.nanosecond(),
        }
    }
}

impl From<&NaiveDateTimeWrapper> for NaiveDateTime {
    fn from(dt: &NaiveDateTimeWrapper) -> Self {
        NaiveDateTime::new(
            NaiveDate::from_ymd_opt(dt.year, dt.month, dt.day).unwrap_or_default(),
            NaiveTime::from_hms_nano_opt(dt.hours, dt.minutes, dt.seconds, dt.nanoseconds).unwrap_or_default(),
        )
    }
}

// Implement Serialize by converting to DateTime<Utc> and serializing that.
impl Serialize for NaiveDateTimeWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_naive_datetime().serialize(serializer)
    }
}

// Implement Deserialize by deserializing a DateTime<Utc> and converting it into a DateTimeWrapper.
impl<'de> Deserialize<'de> for NaiveDateTimeWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let dt = NaiveDateTime::deserialize(deserializer)?;
        Ok(dt.into())
    }
}

#[derive(PartialEq, PartialOrd, Ord, Eq, Clone, Default, BorshDeserialize, BorshSerialize, ToSchema, JsonSchema)]
pub struct NaiveDateWrapper {
    year: i32,
    month: u32,
    day: u32,
}

impl Debug for NaiveDateWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", NaiveDate::from(self))
    }
}

impl Display for NaiveDateWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", NaiveDate::from(self))
    }
}

impl NaiveDateWrapper {
    pub fn now(year: i32, month: u32, day: u32) -> Self {
        Self { year, month, day }
    }
    pub fn as_naive_date(&self) -> NaiveDate {
        NaiveDate::from(self)
    }
}

impl From<NaiveDate> for NaiveDateWrapper {
    fn from(t: NaiveDate) -> Self {
        Self { year: t.year(), month: t.month(), day: t.day() }
    }
}

impl From<&NaiveDateWrapper> for NaiveDate {
    fn from(dt: &NaiveDateWrapper) -> Self {
        NaiveDate::from_ymd_opt(dt.year, dt.month, dt.day).unwrap_or_default()
    }
}

// Implement Serialize by converting to DateTime<Utc> and serializing that.
impl Serialize for NaiveDateWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_naive_date().serialize(serializer)
    }
}

// Implement Deserialize by deserializing a DateTime<Utc> and converting it into a DateTimeWrapper.
impl<'de> Deserialize<'de> for NaiveDateWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let dt = NaiveDate::deserialize(deserializer)?;
        Ok(dt.into())
    }
}
// You might also want to add these additional tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datetime_serde_json_roundtrip() {
        let custom_dt = DateTimeWrapper::now();

        let serialized = serde_json::to_string(&custom_dt).unwrap_or_default();
        let deserialized: DateTimeWrapper = serde_json::from_str(&serialized).unwrap_or_default();

        assert_eq!(custom_dt, deserialized);
    }

    #[test]
    fn test_datetime_borsh_roundtrip() {
        let custom_dt = DateTimeWrapper::now();
        println!("time = {:?}", custom_dt);

        let serialized = borsh::to_vec(&custom_dt).unwrap_or_default();
        let deserialized = borsh::from_slice(&serialized).unwrap_or_default();

        println!("time = {:?}", deserialized);

        assert_eq!(custom_dt, deserialized);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Copy, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Default)]
/// High-precision timestamp in microseconds since UNIX epoch.
pub struct Timestamp(u128);

impl Deref for Timestamp {
    type Target = u128;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Timestamp {
    /// generate new timestamp from UNIX in milliseconds
    pub fn new() -> Self {
        Timestamp(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_else(|_| Duration::from_secs(0)).as_millis())
    }

    /// return vector of bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_le_bytes().to_vec()
    }

    pub fn sub(&self, u: &Timestamp) -> u128 {
        self.0 - u.0
    }

    pub fn add(&self, u: &Timestamp) -> u128 {
        self.0 + u.0
    }
    pub fn time(&self) -> SystemTime {
        let duration = std::time::Duration::from_millis(self.0 as u64);
        UNIX_EPOCH + duration
    }
    pub fn duration_since(&self, earlier: Timestamp) -> Result<Duration, SystemTimeError> {
        self.time().duration_since(earlier.time())
    }
    pub fn replace(&mut self, u: Timestamp) {
        self.0 = u.0
    }
}

impl From<u128> for Timestamp {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

impl From<u64> for Timestamp {
    fn from(value: u64) -> Self {
        Self(value as u128)
    }
}
// impl AsRef<[u8]> for Timestamp {
//     #[inline]
//     fn as_ref(&self) -> &[u8] {
//         &self.0.to_le_bytes().as_slice()
//     }
// }

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        let _ = write!(f, "{}", &self.0);
        Ok(())
    }
}

// impl From<&Timestamp> for String {
//     fn from(time: &Timestamp) -> String {
//         time.to_string()
//     }
// }
