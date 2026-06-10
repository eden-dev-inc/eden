//! MySQL date/time value parsing.
//!
//! MySQL's binary protocol uses a packed format for date/time values.

/// MySQL date value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MysqlDate {
    pub year: u16,
    pub month: u8,
    pub day: u8,
}

impl MysqlDate {
    /// Create a new date.
    pub fn new(year: u16, month: u8, day: u8) -> Self {
        Self { year, month, day }
    }

    /// Parse from binary protocol bytes.
    ///
    /// Format:
    /// - 0 bytes: zero date
    /// - 4 bytes: year (2), month (1), day (1)
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        match data.len() {
            0 => Some(Self::zero()),
            4.. => {
                let year = u16::from_le_bytes([data[0], data[1]]);
                let month = data[2];
                let day = data[3];
                Some(Self { year, month, day })
            }
            _ => None,
        }
    }

    /// Encode to binary protocol bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        if self.is_zero() {
            return vec![0]; // Length byte only
        }

        vec![
            4, // Length
            self.year as u8,
            (self.year >> 8) as u8,
            self.month,
            self.day,
        ]
    }

    /// Create a zero date (0000-00-00).
    pub fn zero() -> Self {
        Self { year: 0, month: 0, day: 0 }
    }

    /// Check if this is a zero date.
    pub fn is_zero(&self) -> bool {
        self.year == 0 && self.month == 0 && self.day == 0
    }

    /// Format as ISO date string (YYYY-MM-DD).
    pub fn to_iso_string(&self) -> String {
        format!("{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }

    /// Parse from ISO date string (YYYY-MM-DD).
    pub fn from_iso_string(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 3 {
            return None;
        }

        let year = parts[0].parse().ok()?;
        let month = parts[1].parse().ok()?;
        let day = parts[2].parse().ok()?;

        Some(Self { year, month, day })
    }
}

impl std::fmt::Display for MysqlDate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_iso_string())
    }
}

/// MySQL time value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MysqlTime {
    /// Whether this is a negative time.
    pub is_negative: bool,
    /// Days component (for time intervals).
    pub days: u32,
    /// Hours (0-23 for time of day, can be larger for intervals).
    pub hours: u8,
    /// Minutes (0-59).
    pub minutes: u8,
    /// Seconds (0-59).
    pub seconds: u8,
    /// Microseconds (0-999999).
    pub microseconds: u32,
}

impl MysqlTime {
    /// Create a new time.
    pub fn new(hours: u8, minutes: u8, seconds: u8) -> Self {
        Self {
            is_negative: false,
            days: 0,
            hours,
            minutes,
            seconds,
            microseconds: 0,
        }
    }

    /// Create a time with microseconds.
    pub fn with_micros(hours: u8, minutes: u8, seconds: u8, microseconds: u32) -> Self {
        Self {
            is_negative: false,
            days: 0,
            hours,
            minutes,
            seconds,
            microseconds,
        }
    }

    /// Parse from binary protocol bytes.
    ///
    /// Format:
    /// - 0 bytes: zero time
    /// - 8 bytes: is_negative (1), days (4), hours (1), minutes (1), seconds (1)
    /// - 12 bytes: above + microseconds (4)
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        match data.len() {
            0 => Some(Self::zero()),
            8.. => {
                let is_negative = data[0] != 0;
                let days = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
                let hours = data[5];
                let minutes = data[6];
                let seconds = data[7];
                let microseconds = if data.len() >= 12 {
                    u32::from_le_bytes([data[8], data[9], data[10], data[11]])
                } else {
                    0
                };

                Some(Self { is_negative, days, hours, minutes, seconds, microseconds })
            }
            _ => None,
        }
    }

    /// Encode to binary protocol bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        if self.is_zero() {
            return vec![0];
        }

        let mut bytes = Vec::with_capacity(13);

        if self.microseconds > 0 {
            bytes.push(12); // Length
        } else {
            bytes.push(8); // Length
        }

        bytes.push(if self.is_negative { 1 } else { 0 });
        bytes.extend_from_slice(&self.days.to_le_bytes());
        bytes.push(self.hours);
        bytes.push(self.minutes);
        bytes.push(self.seconds);

        if self.microseconds > 0 {
            bytes.extend_from_slice(&self.microseconds.to_le_bytes());
        }

        bytes
    }

    /// Create a zero time (00:00:00).
    pub fn zero() -> Self {
        Self {
            is_negative: false,
            days: 0,
            hours: 0,
            minutes: 0,
            seconds: 0,
            microseconds: 0,
        }
    }

    /// Check if this is a zero time.
    pub fn is_zero(&self) -> bool {
        self.days == 0 && self.hours == 0 && self.minutes == 0 && self.seconds == 0 && self.microseconds == 0
    }

    /// Get total seconds (ignoring sign and microseconds).
    pub fn total_seconds(&self) -> u64 {
        (self.days as u64 * 86400) + (self.hours as u64 * 3600) + (self.minutes as u64 * 60) + self.seconds as u64
    }

    /// Format as time string (HH:MM:SS or HH:MM:SS.ffffff).
    pub fn to_string_with_micros(&self) -> String {
        let sign = if self.is_negative { "-" } else { "" };
        let total_hours = self.days * 24 + self.hours as u32;

        if self.microseconds > 0 {
            format!("{}{:02}:{:02}:{:02}.{:06}", sign, total_hours, self.minutes, self.seconds, self.microseconds)
        } else {
            format!("{}{:02}:{:02}:{:02}", sign, total_hours, self.minutes, self.seconds)
        }
    }
}

impl std::fmt::Display for MysqlTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string_with_micros())
    }
}

/// MySQL datetime value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MysqlDateTime {
    pub date: MysqlDate,
    pub time: MysqlTime,
}

impl MysqlDateTime {
    /// Create a new datetime.
    pub fn new(date: MysqlDate, time: MysqlTime) -> Self {
        Self { date, time }
    }

    /// Create from components.
    pub fn from_parts(year: u16, month: u8, day: u8, hour: u8, minute: u8, second: u8) -> Self {
        Self {
            date: MysqlDate::new(year, month, day),
            time: MysqlTime::new(hour, minute, second),
        }
    }

    /// Parse from binary protocol bytes.
    ///
    /// Format:
    /// - 0 bytes: zero datetime
    /// - 4 bytes: date only
    /// - 7 bytes: date + time (no microseconds)
    /// - 11 bytes: date + time + microseconds
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        match data.len() {
            0 => Some(Self::zero()),
            4 => {
                let date = MysqlDate::from_bytes(data)?;
                Some(Self { date, time: MysqlTime::zero() })
            }
            7.. => {
                let year = u16::from_le_bytes([data[0], data[1]]);
                let month = data[2];
                let day = data[3];
                let hours = data[4];
                let minutes = data[5];
                let seconds = data[6];
                let microseconds = if data.len() >= 11 {
                    u32::from_le_bytes([data[7], data[8], data[9], data[10]])
                } else {
                    0
                };

                Some(Self {
                    date: MysqlDate { year, month, day },
                    time: MysqlTime {
                        is_negative: false,
                        days: 0,
                        hours,
                        minutes,
                        seconds,
                        microseconds,
                    },
                })
            }
            _ => None,
        }
    }

    /// Encode to binary protocol bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        if self.is_zero() {
            return vec![0];
        }

        let has_time = !self.time.is_zero();
        let has_micros = self.time.microseconds > 0;

        let mut bytes = Vec::with_capacity(12);

        if has_micros {
            bytes.push(11);
        } else if has_time {
            bytes.push(7);
        } else {
            bytes.push(4);
        }

        bytes.push(self.date.year as u8);
        bytes.push((self.date.year >> 8) as u8);
        bytes.push(self.date.month);
        bytes.push(self.date.day);

        if has_time || has_micros {
            bytes.push(self.time.hours);
            bytes.push(self.time.minutes);
            bytes.push(self.time.seconds);
        }

        if has_micros {
            bytes.extend_from_slice(&self.time.microseconds.to_le_bytes());
        }

        bytes
    }

    /// Create a zero datetime.
    pub fn zero() -> Self {
        Self { date: MysqlDate::zero(), time: MysqlTime::zero() }
    }

    /// Check if this is a zero datetime.
    pub fn is_zero(&self) -> bool {
        self.date.is_zero() && self.time.is_zero()
    }

    /// Format as ISO datetime string.
    pub fn to_iso_string(&self) -> String {
        if self.time.microseconds > 0 {
            format!(
                "{} {:02}:{:02}:{:02}.{:06}",
                self.date.to_iso_string(),
                self.time.hours,
                self.time.minutes,
                self.time.seconds,
                self.time.microseconds
            )
        } else {
            format!(
                "{} {:02}:{:02}:{:02}",
                self.date.to_iso_string(),
                self.time.hours,
                self.time.minutes,
                self.time.seconds
            )
        }
    }
}

impl std::fmt::Display for MysqlDateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_iso_string())
    }
}

/// MySQL YEAR value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MysqlYear(pub u16);

impl MysqlYear {
    /// Parse from binary protocol bytes (2 bytes).
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() >= 2 {
            Some(Self(u16::from_le_bytes([data[0], data[1]])))
        } else if data.is_empty() {
            Some(Self(0))
        } else {
            None
        }
    }

    /// Encode to binary protocol bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_le_bytes().to_vec()
    }
}

impl std::fmt::Display for MysqlYear {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Extension trait for parsing datetime values from binary row data.
pub trait DateTimeExt {
    /// Parse as a MySQL date.
    fn as_date(&self) -> Option<MysqlDate>;

    /// Parse as a MySQL time.
    fn as_time(&self) -> Option<MysqlTime>;

    /// Parse as a MySQL datetime.
    fn as_datetime(&self) -> Option<MysqlDateTime>;

    /// Parse as a MySQL year.
    fn as_year(&self) -> Option<MysqlYear>;
}

impl DateTimeExt for [u8] {
    fn as_date(&self) -> Option<MysqlDate> {
        MysqlDate::from_bytes(self)
    }

    fn as_time(&self) -> Option<MysqlTime> {
        MysqlTime::from_bytes(self)
    }

    fn as_datetime(&self) -> Option<MysqlDateTime> {
        MysqlDateTime::from_bytes(self)
    }

    fn as_year(&self) -> Option<MysqlYear> {
        MysqlYear::from_bytes(self)
    }
}

impl DateTimeExt for Vec<u8> {
    fn as_date(&self) -> Option<MysqlDate> {
        self.as_slice().as_date()
    }

    fn as_time(&self) -> Option<MysqlTime> {
        self.as_slice().as_time()
    }

    fn as_datetime(&self) -> Option<MysqlDateTime> {
        self.as_slice().as_datetime()
    }

    fn as_year(&self) -> Option<MysqlYear> {
        self.as_slice().as_year()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_roundtrip() {
        let date = MysqlDate::new(2024, 12, 25);
        let bytes = date.to_bytes();
        let parsed = MysqlDate::from_bytes(&bytes[1..]).unwrap(); // Skip length byte

        assert_eq!(parsed, date);
        assert_eq!(date.to_iso_string(), "2024-12-25");
    }

    #[test]
    fn test_date_from_iso() {
        let date = MysqlDate::from_iso_string("2024-01-15").unwrap();
        assert_eq!(date.year, 2024);
        assert_eq!(date.month, 1);
        assert_eq!(date.day, 15);
    }

    #[test]
    fn test_time_roundtrip() {
        let time = MysqlTime::with_micros(14, 30, 45, 123456);
        let bytes = time.to_bytes();
        let parsed = MysqlTime::from_bytes(&bytes[1..]).unwrap(); // Skip length byte

        assert_eq!(parsed, time);
    }

    #[test]
    fn test_time_display() {
        let time = MysqlTime::new(14, 30, 45);
        assert_eq!(time.to_string(), "14:30:45");

        let time_micros = MysqlTime::with_micros(14, 30, 45, 123000);
        assert_eq!(time_micros.to_string(), "14:30:45.123000");
    }

    #[test]
    fn test_datetime_roundtrip() {
        let dt = MysqlDateTime::from_parts(2024, 12, 25, 14, 30, 45);
        let bytes = dt.to_bytes();
        let parsed = MysqlDateTime::from_bytes(&bytes[1..]).unwrap(); // Skip length byte

        assert_eq!(parsed, dt);
        assert_eq!(dt.to_iso_string(), "2024-12-25 14:30:45");
    }

    #[test]
    fn test_zero_values() {
        assert!(MysqlDate::zero().is_zero());
        assert!(MysqlTime::zero().is_zero());
        assert!(MysqlDateTime::zero().is_zero());
    }

    #[test]
    fn test_datetime_ext() {
        let data = vec![0xE8, 0x07, 12, 25]; // 2024-12-25
        assert_eq!(data.as_date().unwrap().year, 2024);
    }
}
