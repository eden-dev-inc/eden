//! Oracle DATE and TIMESTAMP encoding/decoding.
//!
//! Oracle uses a unique 7-byte format for DATE values and extended formats
//! for TIMESTAMP variants.
//!
//! # DATE Format (7 bytes)
//!
//! ```text
//! Byte 0: Century + 100 (e.g., 20th century = 119, 21st = 120)
//! Byte 1: Year within century + 100 (e.g., year 24 = 124)
//! Byte 2: Month (1-12)
//! Byte 3: Day (1-31)
//! Byte 4: Hour + 1 (1-24, where 1 = midnight)
//! Byte 5: Minute + 1 (1-60)
//! Byte 6: Second + 1 (1-60)
//! ```
//!
//! # TIMESTAMP Format (11 bytes for basic, 13 for TZ)
//!
//! Same as DATE for first 7 bytes, plus:
//! - Bytes 7-10: Fractional seconds (nanoseconds, big-endian u32)
//! - For TIMESTAMP WITH TIME ZONE: additional 2 bytes for timezone offset
//!
//! # Validation
//!
//! All date/time types include validation for:
//! - Year range: -4712 (4712 BC) to 9999 AD
//! - Month range: 1-12
//! - Day range: Based on month and leap year
//! - Time range: 0-23 hours, 0-59 minutes/seconds
//! - Nanoseconds: 0-999,999,999
//! - Timezone: -12:00 to +14:00

use super::limits;

/// Oracle DATE value (7 bytes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OracleDate {
    /// Year (negative for BC).
    pub year: i16,
    /// Month (1-12).
    pub month: u8,
    /// Day (1-31).
    pub day: u8,
    /// Hour (0-23).
    pub hour: u8,
    /// Minute (0-59).
    pub minute: u8,
    /// Second (0-59).
    pub second: u8,
}

/// Oracle TIMESTAMP value (11+ bytes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OracleTimestamp {
    /// Base date/time.
    pub date: OracleDate,
    /// Fractional seconds in nanoseconds (0-999999999).
    pub nanoseconds: u32,
}

/// Oracle TIMESTAMP WITH TIME ZONE value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OracleTimestampTz {
    /// Base timestamp.
    pub timestamp: OracleTimestamp,
    /// Timezone hour offset (-12 to +14).
    pub tz_hour: i8,
    /// Timezone minute offset (0-59).
    pub tz_minute: i8,
}

/// Oracle INTERVAL YEAR TO MONTH value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OracleIntervalYm {
    /// Number of years (can be negative).
    pub years: i32,
    /// Number of months (0-11).
    pub months: i8,
}

/// Oracle INTERVAL DAY TO SECOND value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OracleIntervalDs {
    /// Number of days (can be negative).
    pub days: i32,
    /// Hours (0-23).
    pub hours: i8,
    /// Minutes (0-59).
    pub minutes: i8,
    /// Seconds (0-59).
    pub seconds: i8,
    /// Fractional seconds in nanoseconds.
    pub nanoseconds: u32,
}

/// Error when parsing a date/time value.
#[derive(Clone, Debug, thiserror::Error)]
pub enum DateTimeParseError {
    #[error("data too short: expected {expected} bytes, got {actual}")]
    TooShort { expected: usize, actual: usize },
    #[error("invalid century byte: {0}")]
    InvalidCentury(u8),
    #[error("invalid year byte: {0}")]
    InvalidYear(u8),
    #[error("invalid month: {0}")]
    InvalidMonth(u8),
    #[error("invalid day: {0}")]
    InvalidDay(u8),
    #[error("invalid hour: {0}")]
    InvalidHour(u8),
    #[error("invalid minute: {0}")]
    InvalidMinute(u8),
    #[error("invalid second: {0}")]
    InvalidSecond(u8),
    #[error("invalid nanoseconds: {0}")]
    InvalidNanoseconds(u32),
    #[error("invalid timezone hour: {0}")]
    InvalidTzHour(i8),
    #[error("invalid timezone minute: {0}")]
    InvalidTzMinute(i8),
    #[error("year {0} out of range [{}, {}]", limits::MIN_YEAR, limits::MAX_YEAR)]
    YearOutOfRange(i16),
    #[error("day {day} invalid for {month}/{year}")]
    DayOutOfRangeForMonth { year: i16, month: u8, day: u8 },
}

impl OracleDate {
    /// Wire format length in bytes.
    pub const WIRE_LENGTH: usize = 7;

    /// Create a new Oracle DATE.
    ///
    /// Note: This does not validate the date components. Use `try_new` for validation.
    pub fn new(year: i16, month: u8, day: u8, hour: u8, minute: u8, second: u8) -> Self {
        Self { year, month, day, hour, minute, second }
    }

    /// Create a new Oracle DATE with validation.
    ///
    /// Returns an error if any component is out of range or if the day is invalid
    /// for the given month/year.
    pub fn try_new(year: i16, month: u8, day: u8, hour: u8, minute: u8, second: u8) -> Result<Self, DateTimeParseError> {
        // Validate year
        if !(limits::MIN_YEAR..=limits::MAX_YEAR).contains(&year) {
            return Err(DateTimeParseError::YearOutOfRange(year));
        }

        // Validate month
        if !(1..=12).contains(&month) {
            return Err(DateTimeParseError::InvalidMonth(month));
        }

        // Validate day for the given month/year
        let max_day = limits::days_in_month(year, month).ok_or(DateTimeParseError::InvalidMonth(month))?;
        if day < 1 || day > max_day {
            return Err(DateTimeParseError::DayOutOfRangeForMonth { year, month, day });
        }

        // Validate time components
        if hour > 23 {
            return Err(DateTimeParseError::InvalidHour(hour));
        }
        if minute > 59 {
            return Err(DateTimeParseError::InvalidMinute(minute));
        }
        if second > 59 {
            return Err(DateTimeParseError::InvalidSecond(second));
        }

        Ok(Self { year, month, day, hour, minute, second })
    }

    /// Create a date-only value (time set to midnight).
    pub fn date_only(year: i16, month: u8, day: u8) -> Self {
        Self::new(year, month, day, 0, 0, 0)
    }

    /// Create a date-only value with validation.
    pub fn try_date_only(year: i16, month: u8, day: u8) -> Result<Self, DateTimeParseError> {
        Self::try_new(year, month, day, 0, 0, 0)
    }

    /// Validate this date's components.
    pub fn validate(&self) -> Result<(), DateTimeParseError> {
        if self.year < limits::MIN_YEAR || self.year > limits::MAX_YEAR {
            return Err(DateTimeParseError::YearOutOfRange(self.year));
        }
        if !(1..=12).contains(&self.month) {
            return Err(DateTimeParseError::InvalidMonth(self.month));
        }
        let max_day = limits::days_in_month(self.year, self.month).ok_or(DateTimeParseError::InvalidMonth(self.month))?;
        if self.day < 1 || self.day > max_day {
            return Err(DateTimeParseError::DayOutOfRangeForMonth { year: self.year, month: self.month, day: self.day });
        }
        if self.hour > 23 {
            return Err(DateTimeParseError::InvalidHour(self.hour));
        }
        if self.minute > 59 {
            return Err(DateTimeParseError::InvalidMinute(self.minute));
        }
        if self.second > 59 {
            return Err(DateTimeParseError::InvalidSecond(self.second));
        }
        Ok(())
    }

    /// Parse from Oracle's 7-byte wire format.
    pub fn from_bytes(data: &[u8]) -> Result<Self, DateTimeParseError> {
        if data.len() < Self::WIRE_LENGTH {
            return Err(DateTimeParseError::TooShort { expected: Self::WIRE_LENGTH, actual: data.len() });
        }

        let century_byte = data[0];
        let year_byte = data[1];
        let month = data[2];
        let day = data[3];
        let hour_byte = data[4];
        let minute_byte = data[5];
        let second_byte = data[6];

        // Decode century and year
        // Century byte: 100 + century (e.g., 119 = 19th century, 120 = 20th century)
        // Year byte: 100 + year within century
        let century = (century_byte as i16) - 100;
        let year_in_century = (year_byte as i16) - 100;

        // Handle BC dates (century < 0)
        let year = if century >= 0 {
            (century - 1) * 100 + year_in_century
        } else {
            century * 100 + year_in_century
        };

        // Validate and decode time components (stored as value + 1)
        if !(1..=12).contains(&month) {
            return Err(DateTimeParseError::InvalidMonth(month));
        }
        if !(1..=31).contains(&day) {
            return Err(DateTimeParseError::InvalidDay(day));
        }
        if !(1..=24).contains(&hour_byte) {
            return Err(DateTimeParseError::InvalidHour(hour_byte));
        }
        if !(1..=60).contains(&minute_byte) {
            return Err(DateTimeParseError::InvalidMinute(minute_byte));
        }
        if !(1..=60).contains(&second_byte) {
            return Err(DateTimeParseError::InvalidSecond(second_byte));
        }

        Ok(Self {
            year,
            month,
            day,
            hour: hour_byte - 1,
            minute: minute_byte - 1,
            second: second_byte - 1,
        })
    }

    /// Encode to Oracle's 7-byte wire format.
    pub fn to_bytes(&self) -> [u8; Self::WIRE_LENGTH] {
        let mut bytes = [0u8; Self::WIRE_LENGTH];

        // Encode century and year
        let (century, year_in_century) = if self.year >= 0 {
            let century = (self.year / 100) + 1;
            let year_in_century = self.year % 100;
            (century, year_in_century)
        } else {
            let century = self.year / 100;
            let year_in_century = self.year.abs() % 100;
            (century, year_in_century)
        };

        bytes[0] = (century + 100) as u8;
        bytes[1] = (year_in_century + 100) as u8;
        bytes[2] = self.month;
        bytes[3] = self.day;
        bytes[4] = self.hour + 1;
        bytes[5] = self.minute + 1;
        bytes[6] = self.second + 1;

        bytes
    }

    /// Check if this represents a date without a time component.
    pub fn is_date_only(&self) -> bool {
        self.hour == 0 && self.minute == 0 && self.second == 0
    }
}

impl OracleTimestamp {
    /// Wire format length in bytes.
    pub const WIRE_LENGTH: usize = 11;

    /// Create a new Oracle TIMESTAMP.
    pub fn new(date: OracleDate, nanoseconds: u32) -> Self {
        Self { date, nanoseconds }
    }

    /// Create a new Oracle TIMESTAMP with validation.
    pub fn try_new(date: OracleDate, nanoseconds: u32) -> Result<Self, DateTimeParseError> {
        date.validate()?;
        if nanoseconds > limits::MAX_NANOSECONDS {
            return Err(DateTimeParseError::InvalidNanoseconds(nanoseconds));
        }
        Ok(Self { date, nanoseconds })
    }

    /// Create from individual components.
    pub fn from_parts(year: i16, month: u8, day: u8, hour: u8, minute: u8, second: u8, nanoseconds: u32) -> Self {
        Self {
            date: OracleDate::new(year, month, day, hour, minute, second),
            nanoseconds,
        }
    }

    /// Create from individual components with validation.
    pub fn try_from_parts(
        year: i16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nanoseconds: u32,
    ) -> Result<Self, DateTimeParseError> {
        let date = OracleDate::try_new(year, month, day, hour, minute, second)?;
        if nanoseconds > limits::MAX_NANOSECONDS {
            return Err(DateTimeParseError::InvalidNanoseconds(nanoseconds));
        }
        Ok(Self { date, nanoseconds })
    }

    /// Validate this timestamp's components.
    pub fn validate(&self) -> Result<(), DateTimeParseError> {
        self.date.validate()?;
        if self.nanoseconds > limits::MAX_NANOSECONDS {
            return Err(DateTimeParseError::InvalidNanoseconds(self.nanoseconds));
        }
        Ok(())
    }

    /// Parse from Oracle's 11-byte wire format.
    pub fn from_bytes(data: &[u8]) -> Result<Self, DateTimeParseError> {
        if data.len() < Self::WIRE_LENGTH {
            return Err(DateTimeParseError::TooShort { expected: Self::WIRE_LENGTH, actual: data.len() });
        }

        let date = OracleDate::from_bytes(&data[0..7])?;

        let nanoseconds = u32::from_be_bytes([data[7], data[8], data[9], data[10]]);

        Ok(Self { date, nanoseconds })
    }

    /// Encode to Oracle's 11-byte wire format.
    pub fn to_bytes(&self) -> [u8; Self::WIRE_LENGTH] {
        let mut bytes = [0u8; Self::WIRE_LENGTH];

        bytes[0..7].copy_from_slice(&self.date.to_bytes());
        bytes[7..11].copy_from_slice(&self.nanoseconds.to_be_bytes());

        bytes
    }

    /// Get fractional seconds as microseconds.
    pub fn microseconds(&self) -> u32 {
        self.nanoseconds / 1000
    }

    /// Get fractional seconds as milliseconds.
    pub fn milliseconds(&self) -> u32 {
        self.nanoseconds / 1_000_000
    }
}

impl OracleTimestampTz {
    /// Wire format length in bytes.
    pub const WIRE_LENGTH: usize = 13;

    /// Create a new Oracle TIMESTAMP WITH TIME ZONE.
    pub fn new(timestamp: OracleTimestamp, tz_hour: i8, tz_minute: i8) -> Self {
        Self { timestamp, tz_hour, tz_minute }
    }

    /// Create a new Oracle TIMESTAMP WITH TIME ZONE with validation.
    pub fn try_new(timestamp: OracleTimestamp, tz_hour: i8, tz_minute: i8) -> Result<Self, DateTimeParseError> {
        timestamp.validate()?;
        if !(limits::MIN_TZ_HOUR..=limits::MAX_TZ_HOUR).contains(&tz_hour) {
            return Err(DateTimeParseError::InvalidTzHour(tz_hour));
        }
        if !(0..=59).contains(&tz_minute) {
            return Err(DateTimeParseError::InvalidTzMinute(tz_minute));
        }
        Ok(Self { timestamp, tz_hour, tz_minute })
    }

    /// Create with UTC timezone.
    pub fn utc(timestamp: OracleTimestamp) -> Self {
        Self::new(timestamp, 0, 0)
    }

    /// Validate this timestamp's components.
    pub fn validate(&self) -> Result<(), DateTimeParseError> {
        self.timestamp.validate()?;
        if self.tz_hour < limits::MIN_TZ_HOUR || self.tz_hour > limits::MAX_TZ_HOUR {
            return Err(DateTimeParseError::InvalidTzHour(self.tz_hour));
        }
        if self.tz_minute < 0 || self.tz_minute > 59 {
            return Err(DateTimeParseError::InvalidTzMinute(self.tz_minute));
        }
        Ok(())
    }

    /// Parse from Oracle's 13-byte wire format.
    pub fn from_bytes(data: &[u8]) -> Result<Self, DateTimeParseError> {
        if data.len() < Self::WIRE_LENGTH {
            return Err(DateTimeParseError::TooShort { expected: Self::WIRE_LENGTH, actual: data.len() });
        }

        let timestamp = OracleTimestamp::from_bytes(&data[0..11])?;

        // Timezone is stored as hour + 20 and minute + 60
        let tz_hour = (data[11] as i8) - 20;
        let tz_minute = (data[12] as i8) - 60;

        Ok(Self { timestamp, tz_hour, tz_minute })
    }

    /// Encode to Oracle's 13-byte wire format.
    pub fn to_bytes(&self) -> [u8; Self::WIRE_LENGTH] {
        let mut bytes = [0u8; Self::WIRE_LENGTH];

        bytes[0..11].copy_from_slice(&self.timestamp.to_bytes());
        bytes[11] = (self.tz_hour + 20) as u8;
        bytes[12] = (self.tz_minute + 60) as u8;

        bytes
    }

    /// Get the timezone offset in minutes.
    pub fn tz_offset_minutes(&self) -> i16 {
        (self.tz_hour as i16) * 60 + (self.tz_minute as i16)
    }

    /// Check if this is UTC.
    pub fn is_utc(&self) -> bool {
        self.tz_hour == 0 && self.tz_minute == 0
    }
}

impl OracleIntervalYm {
    /// Wire format length in bytes.
    pub const WIRE_LENGTH: usize = 5;

    /// Create a new INTERVAL YEAR TO MONTH.
    pub fn new(years: i32, months: i8) -> Self {
        Self { years, months }
    }

    /// Create from total months.
    pub fn from_months(total_months: i32) -> Self {
        let years = total_months / 12;
        let months = (total_months % 12) as i8;
        Self { years, months }
    }

    /// Parse from Oracle's wire format.
    pub fn from_bytes(data: &[u8]) -> Result<Self, DateTimeParseError> {
        if data.len() < Self::WIRE_LENGTH {
            return Err(DateTimeParseError::TooShort { expected: Self::WIRE_LENGTH, actual: data.len() });
        }

        // Years are stored as i32 with 0x80000000 bias
        let years_raw = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let years = (years_raw as i64 - 0x80000000) as i32;

        // Months stored with 60 bias
        let months = (data[4] as i8) - 60;

        Ok(Self { years, months })
    }

    /// Encode to Oracle's wire format.
    pub fn to_bytes(&self) -> [u8; Self::WIRE_LENGTH] {
        let mut bytes = [0u8; Self::WIRE_LENGTH];

        let years_biased = (self.years as i64 + 0x80000000) as u32;
        bytes[0..4].copy_from_slice(&years_biased.to_be_bytes());
        bytes[4] = (self.months + 60) as u8;

        bytes
    }

    /// Get total months.
    pub fn total_months(&self) -> i32 {
        self.years * 12 + self.months as i32
    }

    /// Check if this interval is zero.
    pub fn is_zero(&self) -> bool {
        self.years == 0 && self.months == 0
    }

    /// Check if this interval is negative.
    pub fn is_negative(&self) -> bool {
        self.total_months() < 0
    }
}

impl OracleIntervalDs {
    /// Wire format length in bytes.
    pub const WIRE_LENGTH: usize = 11;

    /// Create a new INTERVAL DAY TO SECOND.
    pub fn new(days: i32, hours: i8, minutes: i8, seconds: i8, nanoseconds: u32) -> Self {
        Self { days, hours, minutes, seconds, nanoseconds }
    }

    /// Parse from Oracle's wire format.
    pub fn from_bytes(data: &[u8]) -> Result<Self, DateTimeParseError> {
        if data.len() < Self::WIRE_LENGTH {
            return Err(DateTimeParseError::TooShort { expected: Self::WIRE_LENGTH, actual: data.len() });
        }

        // Days stored with 0x80000000 bias
        let days_raw = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let days = (days_raw as i64 - 0x80000000) as i32;

        // Time components stored with 60 bias
        let hours = (data[4] as i8) - 60;
        let minutes = (data[5] as i8) - 60;
        let seconds = (data[6] as i8) - 60;

        // Nanoseconds stored with 0x80000000 bias
        let nanos_raw = u32::from_be_bytes([data[7], data[8], data[9], data[10]]);
        let nanoseconds = (nanos_raw as i64 - 0x80000000) as u32;

        Ok(Self { days, hours, minutes, seconds, nanoseconds })
    }

    /// Encode to Oracle's wire format.
    pub fn to_bytes(&self) -> [u8; Self::WIRE_LENGTH] {
        let mut bytes = [0u8; Self::WIRE_LENGTH];

        let days_biased = (self.days as i64 + 0x80000000) as u32;
        bytes[0..4].copy_from_slice(&days_biased.to_be_bytes());

        bytes[4] = (self.hours + 60) as u8;
        bytes[5] = (self.minutes + 60) as u8;
        bytes[6] = (self.seconds + 60) as u8;

        let nanos_biased = (self.nanoseconds as i64 + 0x80000000) as u32;
        bytes[7..11].copy_from_slice(&nanos_biased.to_be_bytes());

        bytes
    }

    /// Get total seconds (excluding fractional part).
    pub fn total_seconds(&self) -> i64 {
        (self.days as i64) * 86400 + (self.hours as i64) * 3600 + (self.minutes as i64) * 60 + (self.seconds as i64)
    }

    /// Check if this interval is zero.
    pub fn is_zero(&self) -> bool {
        self.days == 0 && self.hours == 0 && self.minutes == 0 && self.seconds == 0 && self.nanoseconds == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_roundtrip() {
        let dates = [
            OracleDate::new(2024, 1, 15, 10, 30, 45),
            OracleDate::new(2000, 12, 31, 23, 59, 59),
            OracleDate::new(1999, 6, 15, 0, 0, 0),
            OracleDate::date_only(2024, 7, 4),
        ];

        for original in dates {
            let bytes = original.to_bytes();
            let parsed = OracleDate::from_bytes(&bytes).unwrap();
            assert_eq!(parsed, original, "Roundtrip failed for {:?}", original);
        }
    }

    #[test]
    fn test_date_encoding() {
        // Test a known date: 2024-01-15 10:30:45
        let date = OracleDate::new(2024, 1, 15, 10, 30, 45);
        let bytes = date.to_bytes();

        // Century 21 = 120, Year 24 = 124
        assert_eq!(bytes[0], 121); // Century 21 (2000s) = 100 + 21
        assert_eq!(bytes[1], 124); // Year 24 = 100 + 24
        assert_eq!(bytes[2], 1); // January
        assert_eq!(bytes[3], 15); // Day 15
        assert_eq!(bytes[4], 11); // Hour 10 + 1
        assert_eq!(bytes[5], 31); // Minute 30 + 1
        assert_eq!(bytes[6], 46); // Second 45 + 1
    }

    #[test]
    fn test_timestamp_roundtrip() {
        let ts = OracleTimestamp::from_parts(2024, 6, 15, 14, 30, 0, 123456789);
        let bytes = ts.to_bytes();
        let parsed = OracleTimestamp::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.date, ts.date);
        assert_eq!(parsed.nanoseconds, ts.nanoseconds);
    }

    #[test]
    fn test_timestamp_tz_roundtrip() {
        let ts = OracleTimestamp::from_parts(2024, 6, 15, 14, 30, 0, 0);
        let ts_tz = OracleTimestampTz::new(ts, -5, 0); // EST

        let bytes = ts_tz.to_bytes();
        let parsed = OracleTimestampTz::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.timestamp.date, ts.date);
        assert_eq!(parsed.tz_hour, -5);
        assert_eq!(parsed.tz_minute, 0);
        assert_eq!(parsed.tz_offset_minutes(), -300);
    }

    #[test]
    fn test_interval_ym_roundtrip() {
        let intervals = [
            OracleIntervalYm::new(5, 6),
            OracleIntervalYm::new(-2, -3),
            OracleIntervalYm::new(0, 0),
            OracleIntervalYm::from_months(18),
        ];

        for original in intervals {
            let bytes = original.to_bytes();
            let parsed = OracleIntervalYm::from_bytes(&bytes).unwrap();
            assert_eq!(parsed, original);
        }
    }

    #[test]
    fn test_interval_ds_roundtrip() {
        let intervals = [
            OracleIntervalDs::new(5, 10, 30, 15, 123456789),
            OracleIntervalDs::new(-1, -2, -30, -45, 0),
            OracleIntervalDs::new(0, 0, 0, 0, 0),
        ];

        for original in intervals {
            let bytes = original.to_bytes();
            let parsed = OracleIntervalDs::from_bytes(&bytes).unwrap();
            assert_eq!(parsed, original);
        }
    }

    #[test]
    fn test_interval_ym_total_months() {
        assert_eq!(OracleIntervalYm::new(1, 6).total_months(), 18);
        assert_eq!(OracleIntervalYm::new(2, 0).total_months(), 24);
        assert_eq!(OracleIntervalYm::from_months(30).total_months(), 30);
    }

    #[test]
    fn test_date_is_date_only() {
        assert!(OracleDate::date_only(2024, 1, 1).is_date_only());
        assert!(!OracleDate::new(2024, 1, 1, 12, 0, 0).is_date_only());
    }

    #[test]
    fn test_validated_date_creation() {
        // Valid dates
        assert!(OracleDate::try_new(2024, 1, 31, 12, 30, 45).is_ok());
        assert!(OracleDate::try_new(2024, 2, 29, 0, 0, 0).is_ok()); // Leap year
        assert!(OracleDate::try_new(2023, 2, 28, 0, 0, 0).is_ok()); // Non-leap year

        // Invalid month
        assert!(OracleDate::try_new(2024, 0, 1, 0, 0, 0).is_err());
        assert!(OracleDate::try_new(2024, 13, 1, 0, 0, 0).is_err());

        // Invalid day for month
        assert!(OracleDate::try_new(2024, 2, 30, 0, 0, 0).is_err()); // Feb only has 29 days in leap year
        assert!(OracleDate::try_new(2023, 2, 29, 0, 0, 0).is_err()); // Feb only has 28 days
        assert!(OracleDate::try_new(2024, 4, 31, 0, 0, 0).is_err()); // April has 30 days

        // Invalid time
        assert!(OracleDate::try_new(2024, 1, 1, 24, 0, 0).is_err());
        assert!(OracleDate::try_new(2024, 1, 1, 0, 60, 0).is_err());
        assert!(OracleDate::try_new(2024, 1, 1, 0, 0, 60).is_err());

        // Year out of range
        assert!(OracleDate::try_new(10000, 1, 1, 0, 0, 0).is_err());
        assert!(OracleDate::try_new(-5000, 1, 1, 0, 0, 0).is_err());
    }

    #[test]
    fn test_validated_timestamp_creation() {
        // Valid timestamp
        assert!(OracleTimestamp::try_from_parts(2024, 6, 15, 14, 30, 0, 123456789).is_ok());

        // Invalid nanoseconds
        assert!(OracleTimestamp::try_from_parts(2024, 6, 15, 14, 30, 0, 1_000_000_000).is_err());

        // Invalid date part
        assert!(OracleTimestamp::try_from_parts(2024, 2, 30, 14, 30, 0, 0).is_err());
    }

    #[test]
    fn test_validated_timestamp_tz_creation() {
        let ts = OracleTimestamp::from_parts(2024, 6, 15, 14, 30, 0, 0);

        // Valid timezones
        assert!(OracleTimestampTz::try_new(ts, 0, 0).is_ok()); // UTC
        assert!(OracleTimestampTz::try_new(ts, -12, 0).is_ok()); // Min offset
        assert!(OracleTimestampTz::try_new(ts, 14, 0).is_ok()); // Max offset
        assert!(OracleTimestampTz::try_new(ts, 5, 30).is_ok()); // +5:30

        // Invalid timezone hour
        assert!(OracleTimestampTz::try_new(ts, -13, 0).is_err());
        assert!(OracleTimestampTz::try_new(ts, 15, 0).is_err());

        // Invalid timezone minute
        assert!(OracleTimestampTz::try_new(ts, 0, 60).is_err());
        assert!(OracleTimestampTz::try_new(ts, 0, -1).is_err());
    }

    #[test]
    fn test_date_validation() {
        // Valid date should pass validation
        let valid = OracleDate::new(2024, 6, 15, 12, 30, 45);
        assert!(valid.validate().is_ok());

        // Invalid date should fail validation (manually constructed)
        let invalid_month = OracleDate { year: 2024, month: 13, day: 1, hour: 0, minute: 0, second: 0 };
        assert!(invalid_month.validate().is_err());

        let invalid_day = OracleDate { year: 2024, month: 2, day: 30, hour: 0, minute: 0, second: 0 };
        assert!(invalid_day.validate().is_err());
    }

    #[test]
    fn test_leap_year_handling() {
        // Feb 29 in leap years
        assert!(OracleDate::try_new(2000, 2, 29, 0, 0, 0).is_ok()); // Divisible by 400
        assert!(OracleDate::try_new(2024, 2, 29, 0, 0, 0).is_ok()); // Divisible by 4

        // Feb 29 in non-leap years
        assert!(OracleDate::try_new(1900, 2, 29, 0, 0, 0).is_err()); // Divisible by 100 but not 400
        assert!(OracleDate::try_new(2023, 2, 29, 0, 0, 0).is_err()); // Not divisible by 4
    }
}
