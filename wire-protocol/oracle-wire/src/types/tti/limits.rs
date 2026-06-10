//! Oracle protocol limits and validation.
//!
//! This module defines limits for Oracle wire protocol operations and provides
//! validation functions to enforce these limits consistently across the codebase.

/// Maximum bytes for a VARCHAR2 column.
pub const MAX_VARCHAR2_BYTES: u32 = 32767;

/// Maximum bytes for a CHAR column.
pub const MAX_CHAR_BYTES: u32 = 2000;

/// Maximum bytes for a RAW column.
pub const MAX_RAW_BYTES: u32 = 32767;

/// Maximum bytes for a LONG column.
pub const MAX_LONG_BYTES: u32 = 2_147_483_647; // 2GB - 1

/// Maximum bytes for a LONG RAW column.
pub const MAX_LONG_RAW_BYTES: u32 = 2_147_483_647; // 2GB - 1

/// Maximum LOB size (4GB - 1 for older Oracle, larger for 12c+).
pub const MAX_LOB_BYTES: u64 = 4_294_967_295; // 4GB - 1

/// Maximum LOB size for Oracle 12c+ (128TB).
pub const MAX_LOB_BYTES_12C: u64 = 140_737_488_355_328; // 128TB

/// Maximum chunk size for LOB read/write operations.
pub const MAX_LOB_CHUNK_SIZE: u32 = 32767;

/// Recommended chunk size for LOB operations.
pub const DEFAULT_LOB_CHUNK_SIZE: u32 = 8060;

/// Maximum Oracle NUMBER precision (digits).
pub const MAX_NUMBER_PRECISION: u8 = 38;

/// Maximum Oracle NUMBER scale.
pub const MAX_NUMBER_SCALE: i8 = 127;

/// Minimum Oracle NUMBER scale.
pub const MIN_NUMBER_SCALE: i8 = -84;

/// Maximum Oracle NUMBER wire length.
pub const MAX_NUMBER_WIRE_LENGTH: usize = 22;

/// Maximum identifier length (table names, column names, etc.).
pub const MAX_IDENTIFIER_LENGTH: usize = 128; // 30 for older Oracle versions

/// Maximum identifier length for Oracle 11g and earlier.
pub const MAX_IDENTIFIER_LENGTH_LEGACY: usize = 30;

/// Maximum SQL statement length.
pub const MAX_SQL_LENGTH: usize = 65535;

/// Maximum number of bind variables in a statement.
pub const MAX_BIND_VARIABLES: usize = 65535;

/// Maximum columns in a result set.
pub const MAX_COLUMNS: usize = 1000;

/// Maximum fetch array size (rows per fetch).
pub const MAX_FETCH_SIZE: u32 = 65535;

/// Default fetch size.
pub const DEFAULT_FETCH_SIZE: u32 = 100;

/// Maximum year value in Oracle DATE/TIMESTAMP.
pub const MAX_YEAR: i16 = 9999;

/// Minimum year value in Oracle DATE/TIMESTAMP.
pub const MIN_YEAR: i16 = -4712; // 4712 BC

/// Maximum nanoseconds value.
pub const MAX_NANOSECONDS: u32 = 999_999_999;

/// Maximum timezone hour offset.
pub const MAX_TZ_HOUR: i8 = 14;

/// Minimum timezone hour offset.
pub const MIN_TZ_HOUR: i8 = -12;

/// Maximum connection string length.
pub const MAX_CONNECT_STRING_LENGTH: usize = 4096;

/// Maximum username length.
pub const MAX_USERNAME_LENGTH: usize = 128;

/// Maximum password length.
pub const MAX_PASSWORD_LENGTH: usize = 30;

/// Error for limit violations.
#[derive(Clone, Debug, thiserror::Error)]
pub enum LimitError {
    #[error("VARCHAR2 size {0} exceeds maximum of {MAX_VARCHAR2_BYTES}")]
    Varchar2TooLong(u32),

    #[error("CHAR size {0} exceeds maximum of {MAX_CHAR_BYTES}")]
    CharTooLong(u32),

    #[error("RAW size {0} exceeds maximum of {MAX_RAW_BYTES}")]
    RawTooLong(u32),

    #[error("LOB chunk size {0} exceeds maximum of {MAX_LOB_CHUNK_SIZE}")]
    LobChunkTooLarge(u32),

    #[error("LOB offset {offset} exceeds LOB length {length}")]
    LobOffsetOutOfBounds { offset: u64, length: u64 },

    #[error("NUMBER precision {0} exceeds maximum of {MAX_NUMBER_PRECISION}")]
    NumberPrecisionTooHigh(u8),

    #[error("NUMBER scale {0} outside valid range [{MIN_NUMBER_SCALE}, {MAX_NUMBER_SCALE}]")]
    NumberScaleOutOfRange(i8),

    #[error("year {0} outside valid range [{MIN_YEAR}, {MAX_YEAR}]")]
    YearOutOfRange(i16),

    #[error("month {0} must be between 1 and 12")]
    MonthOutOfRange(u8),

    #[error("day {day} invalid for month {month} in year {year}")]
    DayOutOfRange { year: i16, month: u8, day: u8 },

    #[error("hour {0} must be between 0 and 23")]
    HourOutOfRange(u8),

    #[error("minute {0} must be between 0 and 59")]
    MinuteOutOfRange(u8),

    #[error("second {0} must be between 0 and 59")]
    SecondOutOfRange(u8),

    #[error("nanoseconds {0} exceeds maximum of {MAX_NANOSECONDS}")]
    NanosecondsOutOfRange(u32),

    #[error("timezone hour {0} outside valid range [{MIN_TZ_HOUR}, {MAX_TZ_HOUR}]")]
    TzHourOutOfRange(i8),

    #[error("timezone minute {0} must be between 0 and 59")]
    TzMinuteOutOfRange(i8),

    #[error("identifier length {0} exceeds maximum of {1}")]
    IdentifierTooLong(usize, usize),

    #[error("SQL statement length {0} exceeds maximum of {MAX_SQL_LENGTH}")]
    SqlTooLong(usize),

    #[error("bind variable count {0} exceeds maximum of {MAX_BIND_VARIABLES}")]
    TooManyBindVariables(usize),

    #[error("column count {0} exceeds maximum of {MAX_COLUMNS}")]
    TooManyColumns(usize),

    #[error("fetch size {0} exceeds maximum of {MAX_FETCH_SIZE}")]
    FetchSizeTooLarge(u32),
}

/// Validate a VARCHAR2 size.
pub fn validate_varchar2_size(size: u32) -> Result<(), LimitError> {
    if size > MAX_VARCHAR2_BYTES {
        Err(LimitError::Varchar2TooLong(size))
    } else {
        Ok(())
    }
}

/// Validate a CHAR size.
pub fn validate_char_size(size: u32) -> Result<(), LimitError> {
    if size > MAX_CHAR_BYTES {
        Err(LimitError::CharTooLong(size))
    } else {
        Ok(())
    }
}

/// Validate a RAW size.
pub fn validate_raw_size(size: u32) -> Result<(), LimitError> {
    if size > MAX_RAW_BYTES {
        Err(LimitError::RawTooLong(size))
    } else {
        Ok(())
    }
}

/// Validate a LOB chunk size.
pub fn validate_lob_chunk_size(size: u32) -> Result<(), LimitError> {
    if size > MAX_LOB_CHUNK_SIZE {
        Err(LimitError::LobChunkTooLarge(size))
    } else {
        Ok(())
    }
}

/// Validate a LOB offset against a LOB length.
pub fn validate_lob_offset(offset: u64, length: u64) -> Result<(), LimitError> {
    if offset > length {
        Err(LimitError::LobOffsetOutOfBounds { offset, length })
    } else {
        Ok(())
    }
}

/// Validate NUMBER precision.
pub fn validate_number_precision(precision: u8) -> Result<(), LimitError> {
    if precision > MAX_NUMBER_PRECISION {
        Err(LimitError::NumberPrecisionTooHigh(precision))
    } else {
        Ok(())
    }
}

/// Validate NUMBER scale.
pub fn validate_number_scale(scale: i8) -> Result<(), LimitError> {
    if !(MIN_NUMBER_SCALE..=MAX_NUMBER_SCALE).contains(&scale) {
        Err(LimitError::NumberScaleOutOfRange(scale))
    } else {
        Ok(())
    }
}

/// Check if a year is a leap year.
///
/// For BC years in historical notation (e.g., -4712 for 4712 BC):
/// - Year 1 BC (year = -1 in our system, year 0 in astronomical) is a leap year
/// - Year 4 BC (year = -4 in our system, year -3 in astronomical) is NOT a leap year
/// - Year 5 BC (year = -5 in our system, year -4 in astronomical) IS a leap year
pub fn is_leap_year(year: i16) -> bool {
    if year <= 0 {
        // Convert from historical BC notation to astronomical year numbering
        // Historical year X BC = -(X-1) in astronomical = X+1 when X is negative
        // So year -1 (1 BC) -> 0, year -4 (4 BC) -> -3, year -5 (5 BC) -> -4
        let astronomical = year + 1;
        let abs_year = astronomical.abs();
        (abs_year % 4 == 0) && (abs_year % 100 != 0 || abs_year % 400 == 0)
    } else {
        (year % 4 == 0) && (year % 100 != 0 || year % 400 == 0)
    }
}

/// Get the number of days in a month.
pub fn days_in_month(year: i16, month: u8) -> Option<u8> {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => Some(31),
        4 | 6 | 9 | 11 => Some(30),
        2 => Some(if is_leap_year(year) { 29 } else { 28 }),
        _ => None,
    }
}

/// Validate date components.
pub fn validate_date(year: i16, month: u8, day: u8) -> Result<(), LimitError> {
    if !(MIN_YEAR..=MAX_YEAR).contains(&year) {
        return Err(LimitError::YearOutOfRange(year));
    }
    if !(1..=12).contains(&month) {
        return Err(LimitError::MonthOutOfRange(month));
    }

    let max_day = days_in_month(year, month).ok_or(LimitError::MonthOutOfRange(month))?;
    if day < 1 || day > max_day {
        return Err(LimitError::DayOutOfRange { year, month, day });
    }

    Ok(())
}

/// Validate time components.
pub fn validate_time(hour: u8, minute: u8, second: u8) -> Result<(), LimitError> {
    if hour > 23 {
        return Err(LimitError::HourOutOfRange(hour));
    }
    if minute > 59 {
        return Err(LimitError::MinuteOutOfRange(minute));
    }
    if second > 59 {
        return Err(LimitError::SecondOutOfRange(second));
    }
    Ok(())
}

/// Validate nanoseconds.
pub fn validate_nanoseconds(nanos: u32) -> Result<(), LimitError> {
    if nanos > MAX_NANOSECONDS {
        Err(LimitError::NanosecondsOutOfRange(nanos))
    } else {
        Ok(())
    }
}

/// Validate timezone offset.
pub fn validate_timezone(hour: i8, minute: i8) -> Result<(), LimitError> {
    if !(MIN_TZ_HOUR..=MAX_TZ_HOUR).contains(&hour) {
        return Err(LimitError::TzHourOutOfRange(hour));
    }
    if !(0..=59).contains(&minute) {
        return Err(LimitError::TzMinuteOutOfRange(minute));
    }
    Ok(())
}

/// Validate a complete datetime.
pub fn validate_datetime(year: i16, month: u8, day: u8, hour: u8, minute: u8, second: u8) -> Result<(), LimitError> {
    validate_date(year, month, day)?;
    validate_time(hour, minute, second)?;
    Ok(())
}

/// Validate a complete timestamp with nanoseconds.
pub fn validate_timestamp(year: i16, month: u8, day: u8, hour: u8, minute: u8, second: u8, nanoseconds: u32) -> Result<(), LimitError> {
    validate_datetime(year, month, day, hour, minute, second)?;
    validate_nanoseconds(nanoseconds)?;
    Ok(())
}

/// Validate a timestamp with timezone.
// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub fn validate_timestamp_tz(
    year: i16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    nanoseconds: u32,
    tz_hour: i8,
    tz_minute: i8,
) -> Result<(), LimitError> {
    validate_timestamp(year, month, day, hour, minute, second, nanoseconds)?;
    validate_timezone(tz_hour, tz_minute)?;
    Ok(())
}

/// Validate an identifier length.
pub fn validate_identifier(name: &str, max_length: usize) -> Result<(), LimitError> {
    if name.len() > max_length {
        Err(LimitError::IdentifierTooLong(name.len(), max_length))
    } else {
        Ok(())
    }
}

/// Validate SQL statement length.
pub fn validate_sql_length(sql: &str) -> Result<(), LimitError> {
    if sql.len() > MAX_SQL_LENGTH {
        Err(LimitError::SqlTooLong(sql.len()))
    } else {
        Ok(())
    }
}

/// Validate bind variable count.
pub fn validate_bind_count(count: usize) -> Result<(), LimitError> {
    if count > MAX_BIND_VARIABLES {
        Err(LimitError::TooManyBindVariables(count))
    } else {
        Ok(())
    }
}

/// Validate column count.
pub fn validate_column_count(count: usize) -> Result<(), LimitError> {
    if count > MAX_COLUMNS {
        Err(LimitError::TooManyColumns(count))
    } else {
        Ok(())
    }
}

/// Validate fetch size.
pub fn validate_fetch_size(size: u32) -> Result<(), LimitError> {
    if size > MAX_FETCH_SIZE {
        Err(LimitError::FetchSizeTooLarge(size))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varchar2_size_validation() {
        assert!(validate_varchar2_size(1000).is_ok());
        assert!(validate_varchar2_size(32767).is_ok());
        assert!(validate_varchar2_size(32768).is_err());
    }

    #[test]
    fn test_lob_chunk_validation() {
        assert!(validate_lob_chunk_size(8060).is_ok());
        assert!(validate_lob_chunk_size(32767).is_ok());
        assert!(validate_lob_chunk_size(32768).is_err());
    }

    #[test]
    fn test_lob_offset_validation() {
        assert!(validate_lob_offset(0, 100).is_ok());
        assert!(validate_lob_offset(100, 100).is_ok());
        assert!(validate_lob_offset(101, 100).is_err());
    }

    #[test]
    fn test_leap_year() {
        assert!(is_leap_year(2000)); // divisible by 400
        assert!(!is_leap_year(1900)); // divisible by 100 but not 400
        assert!(is_leap_year(2024)); // divisible by 4
        assert!(!is_leap_year(2023)); // not divisible by 4
    }

    #[test]
    fn test_days_in_month() {
        assert_eq!(days_in_month(2024, 1), Some(31)); // January
        assert_eq!(days_in_month(2024, 2), Some(29)); // February leap year
        assert_eq!(days_in_month(2023, 2), Some(28)); // February non-leap
        assert_eq!(days_in_month(2024, 4), Some(30)); // April
        assert_eq!(days_in_month(2024, 0), None); // Invalid month
        assert_eq!(days_in_month(2024, 13), None); // Invalid month
    }

    #[test]
    fn test_date_validation() {
        // Valid dates
        assert!(validate_date(2024, 1, 15).is_ok());
        assert!(validate_date(2024, 2, 29).is_ok()); // leap year
        assert!(validate_date(2023, 2, 28).is_ok()); // non-leap year

        // Invalid year
        assert!(validate_date(10000, 1, 1).is_err());
        assert!(validate_date(-5000, 1, 1).is_err());

        // Invalid month
        assert!(validate_date(2024, 0, 1).is_err());
        assert!(validate_date(2024, 13, 1).is_err());

        // Invalid day
        assert!(validate_date(2024, 1, 0).is_err());
        assert!(validate_date(2024, 1, 32).is_err());
        assert!(validate_date(2023, 2, 29).is_err()); // not leap year
        assert!(validate_date(2024, 4, 31).is_err()); // April has 30 days
    }

    #[test]
    fn test_time_validation() {
        assert!(validate_time(0, 0, 0).is_ok());
        assert!(validate_time(23, 59, 59).is_ok());
        assert!(validate_time(24, 0, 0).is_err());
        assert!(validate_time(0, 60, 0).is_err());
        assert!(validate_time(0, 0, 60).is_err());
    }

    #[test]
    fn test_timezone_validation() {
        assert!(validate_timezone(0, 0).is_ok());
        assert!(validate_timezone(-12, 0).is_ok());
        assert!(validate_timezone(14, 0).is_ok());
        assert!(validate_timezone(5, 30).is_ok()); // India timezone
        assert!(validate_timezone(-13, 0).is_err());
        assert!(validate_timezone(15, 0).is_err());
        assert!(validate_timezone(0, 60).is_err());
        assert!(validate_timezone(0, -1).is_err());
    }

    #[test]
    fn test_number_validation() {
        assert!(validate_number_precision(38).is_ok());
        assert!(validate_number_precision(39).is_err());
        assert!(validate_number_scale(127).is_ok());
        assert!(validate_number_scale(-84).is_ok());
        // Note: i8 max is 127 so we can't test beyond that range
    }

    #[test]
    fn test_nanoseconds_validation() {
        assert!(validate_nanoseconds(0).is_ok());
        assert!(validate_nanoseconds(999_999_999).is_ok());
        assert!(validate_nanoseconds(1_000_000_000).is_err());
    }

    #[test]
    fn test_identifier_validation() {
        assert!(validate_identifier("EMPLOYEE_ID", MAX_IDENTIFIER_LENGTH).is_ok());
        let long_name = "A".repeat(129);
        assert!(validate_identifier(&long_name, MAX_IDENTIFIER_LENGTH).is_err());
    }

    #[test]
    fn test_sql_validation() {
        let short_sql = "SELECT * FROM dual";
        assert!(validate_sql_length(short_sql).is_ok());

        let long_sql = "SELECT ".to_string() + &"x, ".repeat(30000);
        assert!(validate_sql_length(&long_sql).is_err());
    }

    #[test]
    fn test_bc_leap_year() {
        // In our system: year -1 = 1 BC, year -4 = 4 BC, year -5 = 5 BC
        // In astronomical: year 0 = 1 BC, year -3 = 4 BC, year -4 = 5 BC
        // Leap years in astronomical system are those divisible by 4

        // 1 BC (year -1 -> astronomical 0) IS a leap year (0 % 4 == 0)
        assert!(is_leap_year(-1));

        // 4 BC (year -4 -> astronomical -3) is NOT a leap year
        assert!(!is_leap_year(-4));

        // 5 BC (year -5 -> astronomical -4) IS a leap year (4 % 4 == 0)
        assert!(is_leap_year(-5));

        // 101 BC (year -101 -> astronomical -100) is NOT a leap year (100 rule)
        assert!(!is_leap_year(-101));

        // 401 BC (year -401 -> astronomical -400) IS a leap year (400 rule)
        assert!(is_leap_year(-401));
    }
}
