//! MySQL character set utilities.
//!
//! MySQL identifies character sets and collations by numeric IDs.
//! This module provides mappings between IDs and names.

/// Common MySQL character set IDs.
pub mod charset_ids {
    /// Latin1 (cp1252 Western European).
    pub const LATIN1: u16 = 8;
    /// Latin1 with Swedish collation (default before MySQL 8.0).
    pub const LATIN1_SWEDISH_CI: u16 = 8;
    /// UTF-8 (3-byte, deprecated).
    pub const UTF8: u16 = 33;
    /// UTF-8 with general collation.
    pub const UTF8_GENERAL_CI: u16 = 33;
    /// UTF-8 with binary collation.
    pub const UTF8_BIN: u16 = 83;
    /// UTF-8MB4 (4-byte, full Unicode).
    pub const UTF8MB4: u16 = 45;
    /// UTF-8MB4 with general collation.
    pub const UTF8MB4_GENERAL_CI: u16 = 45;
    /// UTF-8MB4 with Unicode 9.0 collation (MySQL 8.0 default).
    pub const UTF8MB4_0900_AI_CI: u16 = 255;
    /// UTF-8MB4 with binary collation.
    pub const UTF8MB4_BIN: u16 = 46;
    /// Binary.
    pub const BINARY: u16 = 63;
    /// ASCII.
    pub const ASCII: u16 = 11;
    /// ASCII with general collation.
    pub const ASCII_GENERAL_CI: u16 = 11;
    /// Big5 (Traditional Chinese).
    pub const BIG5: u16 = 1;
    /// GBK (Simplified Chinese).
    pub const GBK: u16 = 28;
    /// Shift_JIS (Japanese).
    pub const SJIS: u16 = 13;
    /// EUC-JP (Japanese).
    pub const EUCJPMS: u16 = 97;
    /// EUC-KR (Korean).
    pub const EUCKR: u16 = 19;
    /// GB2312 (Simplified Chinese).
    pub const GB2312: u16 = 24;
    /// Greek.
    pub const GREEK: u16 = 25;
    /// Hebrew.
    pub const HEBREW: u16 = 16;
    /// Latin2 (Central European).
    pub const LATIN2: u16 = 9;
    /// Latin5 (Turkish).
    pub const LATIN5: u16 = 30;
    /// Latin7 (Baltic).
    pub const LATIN7: u16 = 41;
    /// CP1250 (Central European).
    pub const CP1250: u16 = 26;
    /// CP1251 (Cyrillic).
    pub const CP1251: u16 = 51;
    /// CP1256 (Arabic).
    pub const CP1256: u16 = 57;
    /// CP1257 (Baltic).
    pub const CP1257: u16 = 59;
    /// CP850 (DOS Latin 1).
    pub const CP850: u16 = 4;
    /// CP852 (DOS Central European).
    pub const CP852: u16 = 40;
    /// CP866 (DOS Cyrillic).
    pub const CP866: u16 = 36;
    /// KOI8-R (Russian).
    pub const KOI8R: u16 = 7;
    /// KOI8-U (Ukrainian).
    pub const KOI8U: u16 = 22;
    /// TIS620 (Thai).
    pub const TIS620: u16 = 18;
    /// UCS-2 (2-byte Unicode).
    pub const UCS2: u16 = 35;
    /// UTF-16.
    pub const UTF16: u16 = 54;
    /// UTF-16LE.
    pub const UTF16LE: u16 = 56;
    /// UTF-32.
    pub const UTF32: u16 = 60;
}

/// Character set information.
#[derive(Clone, Debug)]
pub struct CharsetInfo {
    /// Character set ID.
    pub id: u16,
    /// Character set name.
    pub name: &'static str,
    /// Collation name.
    pub collation: &'static str,
    /// Maximum bytes per character.
    pub max_len: u8,
}

impl CharsetInfo {
    /// Create a new charset info.
    pub const fn new(id: u16, name: &'static str, collation: &'static str, max_len: u8) -> Self {
        Self { id, name, collation, max_len }
    }

    /// Check if this charset is a Unicode charset.
    pub fn is_unicode(&self) -> bool {
        matches!(self.name, "utf8" | "utf8mb4" | "ucs2" | "utf16" | "utf16le" | "utf32")
    }

    /// Check if this charset is multi-byte.
    pub fn is_multibyte(&self) -> bool {
        self.max_len > 1
    }
}

/// Known character sets with their info.
static CHARSETS: &[CharsetInfo] = &[
    CharsetInfo::new(1, "big5", "big5_chinese_ci", 2),
    CharsetInfo::new(4, "cp850", "cp850_general_ci", 1),
    CharsetInfo::new(7, "koi8r", "koi8r_general_ci", 1),
    CharsetInfo::new(8, "latin1", "latin1_swedish_ci", 1),
    CharsetInfo::new(9, "latin2", "latin2_general_ci", 1),
    CharsetInfo::new(11, "ascii", "ascii_general_ci", 1),
    CharsetInfo::new(13, "sjis", "sjis_japanese_ci", 2),
    CharsetInfo::new(16, "hebrew", "hebrew_general_ci", 1),
    CharsetInfo::new(18, "tis620", "tis620_thai_ci", 1),
    CharsetInfo::new(19, "euckr", "euckr_korean_ci", 2),
    CharsetInfo::new(22, "koi8u", "koi8u_general_ci", 1),
    CharsetInfo::new(24, "gb2312", "gb2312_chinese_ci", 2),
    CharsetInfo::new(25, "greek", "greek_general_ci", 1),
    CharsetInfo::new(26, "cp1250", "cp1250_general_ci", 1),
    CharsetInfo::new(28, "gbk", "gbk_chinese_ci", 2),
    CharsetInfo::new(30, "latin5", "latin5_turkish_ci", 1),
    CharsetInfo::new(33, "utf8", "utf8_general_ci", 3),
    CharsetInfo::new(35, "ucs2", "ucs2_general_ci", 2),
    CharsetInfo::new(36, "cp866", "cp866_general_ci", 1),
    CharsetInfo::new(40, "cp852", "cp852_general_ci", 1),
    CharsetInfo::new(41, "latin7", "latin7_general_ci", 1),
    CharsetInfo::new(45, "utf8mb4", "utf8mb4_general_ci", 4),
    CharsetInfo::new(46, "utf8mb4", "utf8mb4_bin", 4),
    CharsetInfo::new(51, "cp1251", "cp1251_general_ci", 1),
    CharsetInfo::new(54, "utf16", "utf16_general_ci", 4),
    CharsetInfo::new(56, "utf16le", "utf16le_general_ci", 4),
    CharsetInfo::new(57, "cp1256", "cp1256_general_ci", 1),
    CharsetInfo::new(59, "cp1257", "cp1257_general_ci", 1),
    CharsetInfo::new(60, "utf32", "utf32_general_ci", 4),
    CharsetInfo::new(63, "binary", "binary", 1),
    CharsetInfo::new(83, "utf8", "utf8_bin", 3),
    CharsetInfo::new(97, "eucjpms", "eucjpms_japanese_ci", 3),
    CharsetInfo::new(255, "utf8mb4", "utf8mb4_0900_ai_ci", 4),
];

/// Get charset info by ID.
pub fn charset_by_id(id: u16) -> Option<&'static CharsetInfo> {
    CHARSETS.iter().find(|c| c.id == id)
}

/// Get charset info by name (case-insensitive).
pub fn charset_by_name(name: &str) -> Option<&'static CharsetInfo> {
    let name_lower = name.to_lowercase();
    CHARSETS.iter().find(|c| c.name.eq_ignore_ascii_case(&name_lower))
}

/// Get charset info by collation name (case-insensitive).
pub fn charset_by_collation(collation: &str) -> Option<&'static CharsetInfo> {
    let collation_lower = collation.to_lowercase();
    CHARSETS.iter().find(|c| c.collation.eq_ignore_ascii_case(&collation_lower))
}

/// Get the default charset ID for MySQL 8.0+ (utf8mb4_0900_ai_ci).
pub fn default_charset_8x() -> u16 {
    charset_ids::UTF8MB4_0900_AI_CI
}

/// Get the default charset ID for MySQL 5.x (latin1_swedish_ci).
pub fn default_charset_5x() -> u16 {
    charset_ids::LATIN1_SWEDISH_CI
}

/// Get the recommended charset ID for new connections (utf8mb4).
pub fn recommended_charset() -> u16 {
    charset_ids::UTF8MB4_GENERAL_CI
}

/// Charset name mapping for common names.
pub fn normalize_charset_name(name: &str) -> &'static str {
    match name.to_lowercase().as_str() {
        "utf-8" | "utf8" => "utf8",
        "utf-8mb4" | "utf8mb4" => "utf8mb4",
        "latin-1" | "latin1" | "iso-8859-1" | "iso8859-1" => "latin1",
        "ascii" | "us-ascii" => "ascii",
        "binary" | "bin" => "binary",
        "gbk" | "cp936" => "gbk",
        "gb2312" | "euc-cn" => "gb2312",
        "big5" | "big-5" => "big5",
        "shift_jis" | "shift-jis" | "sjis" => "sjis",
        "euc-jp" | "eucjp" => "eucjpms",
        "euc-kr" | "euckr" => "euckr",
        _ => name.to_lowercase().leak(),
    }
}

/// Check if a charset ID represents a binary charset.
pub fn is_binary_charset(id: u16) -> bool {
    id == charset_ids::BINARY
}

/// Check if a charset ID represents a Unicode charset.
pub fn is_unicode_charset(id: u16) -> bool {
    charset_by_id(id).map(|c| c.is_unicode()).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_charset_by_id() {
        let info = charset_by_id(45).unwrap();
        assert_eq!(info.name, "utf8mb4");
        assert_eq!(info.max_len, 4);
    }

    #[test]
    fn test_charset_by_name() {
        let info = charset_by_name("utf8mb4").unwrap();
        assert!(info.id == 45 || info.id == 46 || info.id == 255);
    }

    #[test]
    fn test_charset_by_collation() {
        let info = charset_by_collation("utf8mb4_0900_ai_ci").unwrap();
        assert_eq!(info.id, 255);
    }

    #[test]
    fn test_is_unicode() {
        assert!(charset_by_id(45).unwrap().is_unicode());
        assert!(charset_by_id(33).unwrap().is_unicode());
        assert!(!charset_by_id(8).unwrap().is_unicode());
    }

    #[test]
    fn test_is_multibyte() {
        assert!(charset_by_id(45).unwrap().is_multibyte()); // utf8mb4
        assert!(!charset_by_id(8).unwrap().is_multibyte()); // latin1
    }

    #[test]
    fn test_normalize_charset_name() {
        assert_eq!(normalize_charset_name("UTF-8"), "utf8");
        assert_eq!(normalize_charset_name("UTF-8MB4"), "utf8mb4");
        assert_eq!(normalize_charset_name("ISO-8859-1"), "latin1");
    }

    #[test]
    fn test_is_binary_charset() {
        assert!(is_binary_charset(63));
        assert!(!is_binary_charset(45));
    }

    #[test]
    fn test_defaults() {
        assert_eq!(default_charset_8x(), 255);
        assert_eq!(default_charset_5x(), 8);
        assert_eq!(recommended_charset(), 45);
    }
}
