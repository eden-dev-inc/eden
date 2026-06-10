//! Oracle character set handling.
//!
//! Oracle uses character set IDs to identify encoding. The most common are:
//! - US7ASCII (1): 7-bit ASCII
//! - WE8ISO8859P1 (31): ISO 8859-1 Western European
//! - AL32UTF8 (873): Unicode UTF-8 (recommended)
//! - UTF8 (871): Unicode UTF-8 (deprecated, use AL32UTF8)
//! - AL16UTF16 (2000): Unicode UTF-16

/// Oracle character set ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CharsetId(pub u16);

impl CharsetId {
    // Common character set IDs
    /// US ASCII 7-bit.
    pub const US7ASCII: Self = Self(1);
    /// ISO 8859-1 Western European.
    pub const WE8ISO8859P1: Self = Self(31);
    /// ISO 8859-15 Western European (with Euro).
    pub const WE8ISO8859P15: Self = Self(46);
    /// Windows-1252 Western European.
    pub const WE8MSWIN1252: Self = Self(178);
    /// Shift-JIS Japanese.
    pub const JA16SJIS: Self = Self(832);
    /// EUC Japanese.
    pub const JA16EUC: Self = Self(830);
    /// Unicode UTF-8 (deprecated, use AL32UTF8).
    pub const UTF8: Self = Self(871);
    /// Unicode UTF-8 (recommended).
    pub const AL32UTF8: Self = Self(873);
    /// Unicode UTF-16.
    pub const AL16UTF16: Self = Self(2000);
    /// GB2312 Simplified Chinese.
    pub const ZHS16GBK: Self = Self(852);
    /// Big5 Traditional Chinese.
    pub const ZHT16BIG5: Self = Self(865);
    /// Korean.
    pub const KO16MSWIN949: Self = Self(846);

    /// Create a charset ID from a raw value.
    pub const fn new(id: u16) -> Self {
        Self(id)
    }

    /// Get the raw ID value.
    pub const fn id(self) -> u16 {
        self.0
    }

    /// Check if this is a Unicode character set.
    pub const fn is_unicode(self) -> bool {
        matches!(self.0, 871 | 873 | 2000)
    }

    /// Check if this is a multibyte character set.
    pub const fn is_multibyte(self) -> bool {
        // Unicode and CJK character sets
        matches!(self.0, 830 | 832 | 846 | 852 | 865 | 871 | 873 | 2000)
    }

    /// Check if this is UTF-8 compatible.
    pub const fn is_utf8(self) -> bool {
        matches!(self.0, 871 | 873)
    }

    /// Get the name of this character set.
    pub const fn name(self) -> &'static str {
        match self.0 {
            1 => "US7ASCII",
            31 => "WE8ISO8859P1",
            46 => "WE8ISO8859P15",
            178 => "WE8MSWIN1252",
            830 => "JA16EUC",
            832 => "JA16SJIS",
            846 => "KO16MSWIN949",
            852 => "ZHS16GBK",
            865 => "ZHT16BIG5",
            871 => "UTF8",
            873 => "AL32UTF8",
            2000 => "AL16UTF16",
            _ => "UNKNOWN",
        }
    }

    /// Get max bytes per character for this charset.
    pub const fn max_bytes_per_char(self) -> u8 {
        match self.0 {
            1 => 1,               // US7ASCII
            31 | 46 | 178 => 1,   // ISO-8859-x, Windows-1252
            830 | 832 => 3,       // Japanese
            846 | 852 | 865 => 2, // Korean, Chinese
            871 | 873 => 4,       // UTF-8
            2000 => 4,            // UTF-16 (surrogate pairs)
            _ => 4,               // Conservative default
        }
    }
}

impl Default for CharsetId {
    fn default() -> Self {
        Self::AL32UTF8
    }
}

impl From<u16> for CharsetId {
    fn from(id: u16) -> Self {
        Self(id)
    }
}

impl From<CharsetId> for u16 {
    fn from(charset: CharsetId) -> Self {
        charset.0
    }
}

/// National character set ID (for NCHAR/NVARCHAR2).
///
/// Oracle uses a separate character set for national character types.
/// This is typically AL16UTF16 or UTF8.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NCharsetId(pub u16);

impl NCharsetId {
    /// Unicode UTF-8.
    pub const UTF8: Self = Self(871);
    /// Unicode UTF-8 (AL32UTF8).
    pub const AL32UTF8: Self = Self(873);
    /// Unicode UTF-16 (default for NCHAR).
    pub const AL16UTF16: Self = Self(2000);

    /// Create from raw value.
    pub const fn new(id: u16) -> Self {
        Self(id)
    }

    /// Get the raw ID.
    pub const fn id(self) -> u16 {
        self.0
    }

    /// Get the name.
    pub const fn name(self) -> &'static str {
        match self.0 {
            871 => "UTF8",
            873 => "AL32UTF8",
            2000 => "AL16UTF16",
            _ => "UNKNOWN",
        }
    }
}

impl Default for NCharsetId {
    fn default() -> Self {
        Self::AL16UTF16
    }
}

impl From<u16> for NCharsetId {
    fn from(id: u16) -> Self {
        Self(id)
    }
}

/// Character set configuration for a session.
#[derive(Clone, Debug)]
pub struct CharsetConfig {
    /// Database character set (CHAR, VARCHAR2).
    pub db_charset: CharsetId,
    /// National character set (NCHAR, NVARCHAR2).
    pub nchar_charset: NCharsetId,
    /// Client character set (for conversion).
    pub client_charset: CharsetId,
}

impl CharsetConfig {
    /// Create a new charset config.
    pub fn new(db_charset: CharsetId, nchar_charset: NCharsetId) -> Self {
        Self {
            db_charset,
            nchar_charset,
            client_charset: CharsetId::AL32UTF8,
        }
    }

    /// Create UTF-8 only configuration.
    pub fn utf8() -> Self {
        Self {
            db_charset: CharsetId::AL32UTF8,
            nchar_charset: NCharsetId::AL32UTF8,
            client_charset: CharsetId::AL32UTF8,
        }
    }

    /// Check if conversion is needed between client and database.
    pub fn needs_conversion(&self) -> bool {
        self.client_charset != self.db_charset
    }

    /// Check if NCHAR conversion is needed.
    pub fn needs_nchar_conversion(&self) -> bool {
        self.client_charset.0 != self.nchar_charset.0
    }
}

impl Default for CharsetConfig {
    fn default() -> Self {
        Self::utf8()
    }
}

/// Charset conversion error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum CharsetError {
    #[error("unsupported character set: {0}")]
    UnsupportedCharset(u16),
    #[error("invalid byte sequence for charset {charset} at position {position}")]
    InvalidSequence { charset: &'static str, position: usize },
    #[error("invalid UTF-8 sequence: {0}")]
    InvalidUtf8(String),
    #[error("character cannot be represented in target charset")]
    UnrepresentableChar,
    #[error("truncated multibyte sequence at position {0}")]
    TruncatedSequence(usize),
    #[error("overlong UTF-8 encoding detected at position {0}")]
    OverlongEncoding(usize),
    #[error("invalid UTF-8 continuation byte at position {0}")]
    InvalidContinuationByte(usize),
    #[error("surrogate code point in UTF-8 at position {0}")]
    SurrogateCodePoint(usize),
}

/// Decode bytes to a string using the specified character set.
///
/// Currently supports:
/// - US7ASCII (charset ID 1)
/// - UTF-8 (charset IDs 871, 873)
/// - ISO-8859-1 (charset ID 31)
/// - Windows-1252 (charset ID 178)
///
/// Other character sets will return an error.
pub fn decode_string(bytes: &[u8], charset: CharsetId) -> Result<String, CharsetError> {
    match charset.0 {
        1 => {
            // US7ASCII - validate and convert
            for (i, &b) in bytes.iter().enumerate() {
                if b >= 128 {
                    return Err(CharsetError::InvalidSequence { charset: "US7ASCII", position: i });
                }
            }
            Ok(String::from_utf8_lossy(bytes).into_owned())
        }
        871 | 873 => {
            // UTF-8 with strict validation
            validate_utf8_strict(bytes)?;
            // After validation, this should not fail
            String::from_utf8(bytes.to_vec()).map_err(|e| CharsetError::InvalidUtf8(e.to_string()))
        }
        31 | 178 => {
            // ISO-8859-1 / Windows-1252 - convert to UTF-8
            // These are single-byte encodings that map directly to Unicode code points 0-255
            Ok(bytes.iter().map(|&b| b as char).collect())
        }
        _ => Err(CharsetError::UnsupportedCharset(charset.0)),
    }
}

/// Decode bytes to a string, replacing invalid sequences with the replacement character.
///
/// This is useful when you need to display data that might be corrupted.
pub fn decode_string_lossy(bytes: &[u8], charset: CharsetId) -> String {
    match charset.0 {
        1 => {
            // US7ASCII - replace bytes >= 128 with replacement char
            bytes.iter().map(|&b| if b < 128 { b as char } else { '\u{FFFD}' }).collect()
        }
        871 | 873 => {
            // UTF-8 - use lossy conversion
            String::from_utf8_lossy(bytes).into_owned()
        }
        31 | 178 => {
            // ISO-8859-1 / Windows-1252
            bytes.iter().map(|&b| b as char).collect()
        }
        _ => {
            // Unknown charset - try UTF-8 lossy
            String::from_utf8_lossy(bytes).into_owned()
        }
    }
}

/// Validate UTF-8 bytes strictly, checking for:
/// - Invalid byte sequences
/// - Overlong encodings
/// - Surrogate code points
/// - Truncated sequences
fn validate_utf8_strict(bytes: &[u8]) -> Result<(), CharsetError> {
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];

        // Determine the expected length of the sequence
        let len = if b < 0x80 {
            1 // ASCII
        } else if b < 0xC0 {
            // Unexpected continuation byte
            return Err(CharsetError::InvalidContinuationByte(i));
        } else if b < 0xE0 {
            2 // 2-byte sequence
        } else if b < 0xF0 {
            3 // 3-byte sequence
        } else if b < 0xF8 {
            4 // 4-byte sequence
        } else {
            // Invalid start byte
            return Err(CharsetError::InvalidUtf8(format!("invalid start byte 0x{:02X} at position {}", b, i)));
        };

        // Check for truncation
        if i + len > bytes.len() {
            return Err(CharsetError::TruncatedSequence(i));
        }

        // Validate continuation bytes
        for j in 1..len {
            let cont = bytes[i + j];
            if !(0x80..0xC0).contains(&cont) {
                return Err(CharsetError::InvalidContinuationByte(i + j));
            }
        }

        // Check for overlong encodings and decode the code point
        let code_point = match len {
            1 => b as u32,
            2 => {
                let cp = ((b as u32 & 0x1F) << 6) | (bytes[i + 1] as u32 & 0x3F);
                if cp < 0x80 {
                    return Err(CharsetError::OverlongEncoding(i));
                }
                cp
            }
            3 => {
                let cp = ((b as u32 & 0x0F) << 12) | ((bytes[i + 1] as u32 & 0x3F) << 6) | (bytes[i + 2] as u32 & 0x3F);
                if cp < 0x800 {
                    return Err(CharsetError::OverlongEncoding(i));
                }
                cp
            }
            4 => {
                let cp = ((b as u32 & 0x07) << 18)
                    | ((bytes[i + 1] as u32 & 0x3F) << 12)
                    | ((bytes[i + 2] as u32 & 0x3F) << 6)
                    | (bytes[i + 3] as u32 & 0x3F);
                if cp < 0x10000 {
                    return Err(CharsetError::OverlongEncoding(i));
                }
                cp
            }
            _ => unreachable!(),
        };

        // Check for surrogate code points (U+D800 to U+DFFF)
        if (0xD800..=0xDFFF).contains(&code_point) {
            return Err(CharsetError::SurrogateCodePoint(i));
        }

        // Check for code points beyond U+10FFFF
        if code_point > 0x10FFFF {
            return Err(CharsetError::InvalidUtf8(format!("code point U+{:X} out of range at position {}", code_point, i)));
        }

        i += len;
    }

    Ok(())
}

/// Check if a byte sequence is valid UTF-8.
pub fn is_valid_utf8(bytes: &[u8]) -> bool {
    validate_utf8_strict(bytes).is_ok()
}

/// Encode a string to bytes using the specified character set.
///
/// Currently supports:
/// - US7ASCII (charset ID 1)
/// - UTF-8 (charset IDs 871, 873)
/// - ISO-8859-1 (charset ID 31)
///
/// Other character sets will return an error.
pub fn encode_string(s: &str, charset: CharsetId) -> Result<Vec<u8>, CharsetError> {
    match charset.0 {
        1 => {
            // US7ASCII
            for c in s.chars() {
                if c as u32 > 127 {
                    return Err(CharsetError::UnrepresentableChar);
                }
            }
            Ok(s.as_bytes().to_vec())
        }
        871 | 873 => {
            // UTF-8 - Rust strings are always valid UTF-8
            Ok(s.as_bytes().to_vec())
        }
        31 => {
            // ISO-8859-1 - check all chars are representable
            let mut bytes = Vec::with_capacity(s.len());
            for c in s.chars() {
                if c as u32 <= 255 {
                    bytes.push(c as u8);
                } else {
                    return Err(CharsetError::UnrepresentableChar);
                }
            }
            Ok(bytes)
        }
        _ => Err(CharsetError::UnsupportedCharset(charset.0)),
    }
}

/// Estimate the byte length of a string when encoded in a given charset.
///
/// This is useful for buffer allocation.
pub fn estimate_encoded_length(s: &str, charset: CharsetId) -> usize {
    match charset.0 {
        1 => s.len(),                                         // ASCII is 1:1
        871 | 873 => s.len(),                                 // UTF-8 bytes are already correct
        31 | 178 => s.chars().count(),                        // Single byte per char
        2000 => s.chars().count() * 2,                        // UTF-16 is 2 bytes per BMP char
        _ => s.len() * charset.max_bytes_per_char() as usize, // Conservative estimate
    }
}

/// Get the maximum bytes needed for a single character in the given charset.
pub fn max_char_bytes(charset: CharsetId) -> usize {
    charset.max_bytes_per_char() as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_charset_id() {
        assert!(CharsetId::AL32UTF8.is_unicode());
        assert!(CharsetId::AL32UTF8.is_utf8());
        assert!(!CharsetId::US7ASCII.is_unicode());
        assert_eq!(CharsetId::AL32UTF8.name(), "AL32UTF8");
        assert_eq!(CharsetId::US7ASCII.max_bytes_per_char(), 1);
        assert_eq!(CharsetId::AL32UTF8.max_bytes_per_char(), 4);
    }

    #[test]
    fn test_charset_config() {
        let config = CharsetConfig::utf8();
        assert!(!config.needs_conversion());

        let config = CharsetConfig::new(CharsetId::WE8ISO8859P1, NCharsetId::AL16UTF16);
        assert!(config.needs_conversion());
    }

    #[test]
    fn test_decode_utf8() {
        let bytes = "Hello, World!".as_bytes();
        let result = decode_string(bytes, CharsetId::AL32UTF8).unwrap();
        assert_eq!(result, "Hello, World!");
    }

    #[test]
    fn test_decode_ascii() {
        let bytes = b"Hello";
        let result = decode_string(bytes, CharsetId::US7ASCII).unwrap();
        assert_eq!(result, "Hello");

        // High bit set should fail
        let bytes = &[0x80];
        assert!(decode_string(bytes, CharsetId::US7ASCII).is_err());
    }

    #[test]
    fn test_decode_iso8859() {
        // ISO-8859-1: byte 0xE9 is 'é'
        let bytes = &[0xE9];
        let result = decode_string(bytes, CharsetId::WE8ISO8859P1).unwrap();
        assert_eq!(result, "é");
    }

    #[test]
    fn test_encode_utf8() {
        let s = "Hello, World!";
        let result = encode_string(s, CharsetId::AL32UTF8).unwrap();
        assert_eq!(result, s.as_bytes());
    }

    #[test]
    fn test_encode_ascii() {
        let s = "Hello";
        let result = encode_string(s, CharsetId::US7ASCII).unwrap();
        assert_eq!(result, s.as_bytes());

        // Non-ASCII should fail
        let s = "Héllo";
        assert!(encode_string(s, CharsetId::US7ASCII).is_err());
    }

    #[test]
    fn test_encode_iso8859() {
        let s = "é";
        let result = encode_string(s, CharsetId::WE8ISO8859P1).unwrap();
        assert_eq!(result, &[0xE9]);

        // Characters outside ISO-8859-1 should fail
        let s = "日本語";
        assert!(encode_string(s, CharsetId::WE8ISO8859P1).is_err());
    }

    #[test]
    fn test_default_charset() {
        assert_eq!(CharsetId::default(), CharsetId::AL32UTF8);
        assert_eq!(NCharsetId::default(), NCharsetId::AL16UTF16);
    }

    #[test]
    fn test_utf8_validation_valid() {
        // Valid UTF-8 sequences
        assert!(is_valid_utf8(b"Hello, World!"));
        assert!(is_valid_utf8("日本語".as_bytes()));
        assert!(is_valid_utf8("🎉".as_bytes())); // 4-byte emoji
        assert!(is_valid_utf8(b"")); // Empty is valid
    }

    #[test]
    fn test_utf8_validation_invalid() {
        // Invalid start byte
        assert!(!is_valid_utf8(&[0xFF]));
        assert!(!is_valid_utf8(&[0xFE]));

        // Unexpected continuation byte
        assert!(!is_valid_utf8(&[0x80]));

        // Truncated sequence
        assert!(!is_valid_utf8(&[0xC2])); // Missing continuation
        assert!(!is_valid_utf8(&[0xE0, 0xA0])); // Missing one byte
        assert!(!is_valid_utf8(&[0xF0, 0x90, 0x80])); // Missing one byte
    }

    #[test]
    fn test_utf8_overlong_encoding() {
        // Overlong encoding of '/' (U+002F)
        // Should be encoded as 0x2F, not as 0xC0 0xAF
        assert!(!is_valid_utf8(&[0xC0, 0xAF]));

        // Overlong encoding of U+0080
        // Should be 0xC2 0x80, not 0xE0 0x80 0x80
        assert!(!is_valid_utf8(&[0xE0, 0x80, 0x80]));
    }

    #[test]
    fn test_utf8_surrogate_code_points() {
        // Surrogate code points U+D800 to U+DFFF are invalid in UTF-8
        // ED A0 80 would be U+D800
        assert!(!is_valid_utf8(&[0xED, 0xA0, 0x80]));
        // ED BF BF would be U+DFFF
        assert!(!is_valid_utf8(&[0xED, 0xBF, 0xBF]));
    }

    #[test]
    fn test_decode_string_lossy() {
        // Valid UTF-8
        let valid = decode_string_lossy(b"Hello", CharsetId::AL32UTF8);
        assert_eq!(valid, "Hello");

        // Invalid UTF-8 should use replacement character
        let invalid = decode_string_lossy(&[0xFF, 0xFE], CharsetId::AL32UTF8);
        assert!(invalid.contains('\u{FFFD}'));

        // ASCII with high bytes
        let high_ascii = decode_string_lossy(&[0x41, 0x80, 0x42], CharsetId::US7ASCII);
        assert_eq!(high_ascii, "A\u{FFFD}B");
    }

    #[test]
    fn test_decode_string_with_position() {
        // ASCII error should include position
        let result = decode_string(&[0x41, 0x42, 0x80, 0x43], CharsetId::US7ASCII);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("position"));
    }

    #[test]
    fn test_estimate_encoded_length() {
        let s = "Hello";
        assert_eq!(estimate_encoded_length(s, CharsetId::US7ASCII), 5);
        assert_eq!(estimate_encoded_length(s, CharsetId::AL32UTF8), 5);

        let japanese = "日本語";
        // UTF-8 encoding is 9 bytes
        assert_eq!(estimate_encoded_length(japanese, CharsetId::AL32UTF8), 9);
        // ISO-8859-1 would be 3 chars
        assert_eq!(estimate_encoded_length(japanese, CharsetId::WE8ISO8859P1), 3);
    }

    #[test]
    fn test_max_char_bytes() {
        assert_eq!(max_char_bytes(CharsetId::US7ASCII), 1);
        assert_eq!(max_char_bytes(CharsetId::AL32UTF8), 4);
        assert_eq!(max_char_bytes(CharsetId::WE8ISO8859P1), 1);
        assert_eq!(max_char_bytes(CharsetId::JA16SJIS), 3);
    }

    #[test]
    fn test_decode_utf8_multibyte() {
        // 2-byte sequence
        let two_byte = decode_string("é".as_bytes(), CharsetId::AL32UTF8).unwrap();
        assert_eq!(two_byte, "é");

        // 3-byte sequence
        let three_byte = decode_string("日".as_bytes(), CharsetId::AL32UTF8).unwrap();
        assert_eq!(three_byte, "日");

        // 4-byte sequence (emoji)
        let four_byte = decode_string("🎉".as_bytes(), CharsetId::AL32UTF8).unwrap();
        assert_eq!(four_byte, "🎉");
    }

    #[test]
    fn test_roundtrip_encoding() {
        let test_strings = ["Hello", "Héllo", "日本語", "🎉🎊"];

        for s in test_strings {
            let encoded = encode_string(s, CharsetId::AL32UTF8).unwrap();
            let decoded = decode_string(&encoded, CharsetId::AL32UTF8).unwrap();
            assert_eq!(decoded, s);
        }
    }
}
