//! PostgreSQL extension support.
//!
//! This module provides helpers for working with common PostgreSQL extensions
//! at the wire protocol level. Since extension type OIDs are assigned dynamically
//! when extensions are installed, this module provides:
//!
//! - Well-known extension type names for `pg_type` lookups
//! - Binary format parsing helpers for common extension types
//! - Documentation on wire protocol formats
//!
//! # Extension Type OIDs
//!
//! Extension types get OIDs >= 16384 (the first user-defined OID). To find the
//! actual OID for an extension type, query `pg_type`:
//!
//! ```sql
//! SELECT oid, typname FROM pg_type WHERE typname = 'geometry';
//! ```
//!
//! # Supported Extensions
//!
//! - **PostGIS**: Geometry and geography types
//! - **pgvector**: Vector similarity search
//! - **hstore**: Key-value pairs
//! - **ltree**: Hierarchical labels
//! - **citext**: Case-insensitive text
//! - **pg_trgm**: Trigram matching (uses built-in types)

/// Well-known extension type names.
///
/// These are the type names as they appear in `pg_type.typname`.
/// Use these to look up the actual OID for each type.
pub mod type_name {
    // PostGIS types
    /// PostGIS geometry type (2D).
    pub const GEOMETRY: &str = "geometry";
    /// PostGIS geography type (geodetic).
    pub const GEOGRAPHY: &str = "geography";
    /// PostGIS box2d type.
    pub const BOX2D: &str = "box2d";
    /// PostGIS box3d type.
    pub const BOX3D: &str = "box3d";
    /// PostGIS geometry array.
    pub const GEOMETRY_ARRAY: &str = "_geometry";
    /// PostGIS geography array.
    pub const GEOGRAPHY_ARRAY: &str = "_geography";

    // pgvector types
    /// pgvector vector type.
    pub const VECTOR: &str = "vector";
    /// pgvector halfvec type (half-precision).
    pub const HALFVEC: &str = "halfvec";
    /// pgvector sparsevec type.
    pub const SPARSEVEC: &str = "sparsevec";
    /// pgvector bit type for binary vectors.
    pub const VECTOR_BIT: &str = "bit";
    /// pgvector vector array.
    pub const VECTOR_ARRAY: &str = "_vector";

    // hstore types
    /// hstore key-value type.
    pub const HSTORE: &str = "hstore";
    /// hstore array.
    pub const HSTORE_ARRAY: &str = "_hstore";

    // ltree types
    /// ltree hierarchical label type.
    pub const LTREE: &str = "ltree";
    /// ltree query type.
    pub const LQUERY: &str = "lquery";
    /// ltree text query type.
    pub const LTXTQUERY: &str = "ltxtquery";
    /// ltree array.
    pub const LTREE_ARRAY: &str = "_ltree";

    // citext types
    /// Case-insensitive text type.
    pub const CITEXT: &str = "citext";
    /// citext array.
    pub const CITEXT_ARRAY: &str = "_citext";

    // cube extension
    /// N-dimensional cube type.
    pub const CUBE: &str = "cube";
    /// cube array.
    pub const CUBE_ARRAY: &str = "_cube";

    // seg extension
    /// Floating-point interval type.
    pub const SEG: &str = "seg";

    // isn extension (ISBN, ISSN, etc.)
    /// ISBN type.
    pub const ISBN: &str = "isbn";
    /// ISBN13 type.
    pub const ISBN13: &str = "isbn13";
    /// ISSN type.
    pub const ISSN: &str = "issn";
    /// ISSN13 type.
    pub const ISSN13: &str = "issn13";
    /// ISMN type.
    pub const ISMN: &str = "ismn";
    /// ISMN13 type.
    pub const ISMN13: &str = "ismn13";
    /// UPC type.
    pub const UPC: &str = "upc";
    /// EAN13 type.
    pub const EAN13: &str = "ean13";

    // intarray extension
    /// Integer array query type.
    pub const QUERY_INT: &str = "query_int";

    // ip4r extension (IP address ranges)
    /// IPv4 address type.
    pub const IP4: &str = "ip4";
    /// IPv4 range type.
    pub const IP4R: &str = "ip4r";
    /// IPv6 address type.
    pub const IP6: &str = "ip6";
    /// IPv6 range type.
    pub const IP6R: &str = "ip6r";
    /// Generic IP address type (v4 or v6).
    pub const IPADDRESS: &str = "ipaddress";
    /// Generic IP range type.
    pub const IPRANGE: &str = "iprange";

    // semver extension (semantic versioning)
    /// Semantic version type.
    pub const SEMVER: &str = "semver";
    /// Semantic version range type.
    pub const SEMVERRANGE: &str = "semverrange";

    // prefix extension (prefix matching)
    /// Prefix range type.
    pub const PREFIX_RANGE: &str = "prefix_range";

    // roaringbitmap extension
    /// Roaring bitmap type.
    pub const ROARINGBITMAP: &str = "roaringbitmap";

    // Apache AGE extension (graph database)
    /// AGE graph element type.
    pub const AGTYPE: &str = "agtype";
    /// AGE vertex type.
    pub const VERTEX: &str = "vertex";
    /// AGE edge type.
    pub const EDGE: &str = "edge";
    /// AGE graphid type.
    pub const GRAPHID: &str = "graphid";

    // unit extension (physical units)
    /// Physical unit type.
    pub const UNIT: &str = "unit";

    // pg_rational extension
    /// Rational number type.
    pub const RATIONAL: &str = "rational";

    // periods extension (temporal)
    /// Period type for temporal data.
    pub const PERIOD: &str = "period";

    // pg_sphere extension (spherical geometry)
    /// Spherical point type.
    pub const SPOINT: &str = "spoint";
    /// Spherical circle type.
    pub const SCIRCLE: &str = "scircle";
    /// Spherical line type.
    pub const SLINE: &str = "sline";
    /// Spherical ellipse type.
    pub const SELLIPSE: &str = "sellipse";
    /// Spherical box type.
    pub const SBOX: &str = "sbox";
    /// Spherical path type.
    pub const SPATH: &str = "spath";
    /// Spherical polygon type.
    pub const SPOLY: &str = "spoly";

    // pg_uuidv7 extension
    /// UUIDv7 type (time-sortable UUID).
    pub const UUIDV7: &str = "uuidv7";

    // pgroonga extension (full-text search)
    /// PGroonga full-text search index type.
    pub const PGROONGA: &str = "pgroonga";

    // pgvector additional types
    /// Sparse vector type for high-dimensional data.
    pub const SVECTOR: &str = "svector";

    // h3 extension (Uber H3 geospatial indexing)
    /// H3 cell index type.
    pub const H3INDEX: &str = "h3index";

    // pg_net extension (async HTTP client)
    /// HTTP request type.
    pub const HTTP_REQUEST: &str = "http_request";
    /// HTTP response type.
    pub const HTTP_RESPONSE: &str = "http_response";
    /// HTTP header type.
    pub const HTTP_HEADER: &str = "http_header";

    // pgsodium extension (libsodium cryptography)
    /// Symmetric key type.
    pub const BYTEA_KEY: &str = "bytea";
    /// Key ID type.
    pub const KEY_ID: &str = "key_id";

    // orafce extension compatibility aliases
    /// VARCHAR2 compatibility type.
    pub const VARCHAR2: &str = "varchar2";
    /// NVARCHAR2 compatibility type.
    pub const NVARCHAR2: &str = "nvarchar2";
    /// DATE compatibility type (includes time).
    pub const ORADATE: &str = "date";

    // mobilitydb extension (temporal/trajectory data)
    /// Temporal boolean.
    pub const TBOOL: &str = "tbool";
    /// Temporal integer.
    pub const TINT: &str = "tint";
    /// Temporal float.
    pub const TFLOAT: &str = "tfloat";
    /// Temporal text.
    pub const TTEXT: &str = "ttext";
    /// Temporal geometry point.
    pub const TGEOMPOINT: &str = "tgeompoint";
    /// Temporal geography point.
    pub const TGEOGPOINT: &str = "tgeogpoint";
    /// Span of integers.
    pub const INTSPAN: &str = "intspan";
    /// Span of floats.
    pub const FLOATSPAN: &str = "floatspan";
    /// Period (time span).
    pub const MOBILITY_PERIOD: &str = "period";
    /// STBox (spatiotemporal box).
    pub const STBOX: &str = "stbox";

    // timescaledb extension (time-series)
    /// Compressed chunk type (internal).
    pub const COMPRESSED_DATA: &str = "_timescaledb_internal.compressed_data";

    // citus extension (distributed PostgreSQL)
    /// Shard interval type (internal).
    pub const SHARD_INTERVAL: &str = "shard_interval";

    // pgmp extension (multi-precision arithmetic)
    /// Multi-precision integer.
    pub const MPZ: &str = "mpz";
    /// Multi-precision rational.
    pub const MPQ: &str = "mpq";
    /// Multi-precision float.
    pub const MPF: &str = "mpf";

    // pg_graphql extension
    /// GraphQL query type.
    pub const GRAPHQL: &str = "graphql";

    // pg_jsonschema extension (uses jsonb, no custom types)

    // rum extension (uses tsvector, no custom types)

    // pg_search / ParadeDB extension
    /// BM25 ranking type.
    pub const BM25: &str = "bm25";
    /// Tantivy search type.
    pub const TANTIVY: &str = "tantivy";

    // pg_tle extension (trusted language extensions)
    /// TLE feature info type.
    pub const PG_TLE_FEATURE_INFO: &str = "pg_tle_feature_info";

    // pg_uuidv6 extension
    /// UUIDv6 type.
    pub const UUIDV6: &str = "uuidv6";

    // ulid extension
    /// ULID type (Universally Unique Lexicographically Sortable Identifier).
    pub const ULID: &str = "ulid";

    // pg_rrule extension (recurring rules)
    /// RRule type for recurring schedules.
    pub const RRULE: &str = "rrule";
    /// RRuleSet type.
    pub const RRULESET: &str = "rruleset";

    // temporal_tables extension (uses tstzrange, no custom types)

    // pg_partman extension (uses built-in types)

    // plv8 extension (JavaScript - uses jsonb)

    // pg_cron extension (uses built-in types)

    // pg_hint_plan extension (uses text)

    // address_standardizer extension (PostGIS component)
    /// Standardized address type.
    pub const STDADDR: &str = "stdaddr";

    // pg_similarity extension (uses float for similarity scores, no custom types)

    // pg_trgm (uses built-in text type, no custom types)

    // timescaledb (mostly uses built-in types)
}

/// PostGIS geometry/geography binary format helpers.
///
/// PostGIS uses EWKB (Extended Well-Known Binary) format which extends
/// the OGC WKB format with SRID support.
pub mod postgis {
    /// EWKB geometry type codes.
    pub mod geometry_type {
        pub const POINT: u32 = 1;
        pub const LINESTRING: u32 = 2;
        pub const POLYGON: u32 = 3;
        pub const MULTIPOINT: u32 = 4;
        pub const MULTILINESTRING: u32 = 5;
        pub const MULTIPOLYGON: u32 = 6;
        pub const GEOMETRYCOLLECTION: u32 = 7;
        pub const CIRCULARSTRING: u32 = 8;
        pub const COMPOUNDCURVE: u32 = 9;
        pub const CURVEPOLYGON: u32 = 10;
        pub const MULTICURVE: u32 = 11;
        pub const MULTISURFACE: u32 = 12;
        pub const CURVE: u32 = 13;
        pub const SURFACE: u32 = 14;
        pub const POLYHEDRALSURFACE: u32 = 15;
        pub const TIN: u32 = 16;
        pub const TRIANGLE: u32 = 17;
    }

    /// EWKB flags (OR'd with geometry type).
    pub mod ewkb_flags {
        /// Geometry has Z coordinate.
        pub const HAS_Z: u32 = 0x80000000;
        /// Geometry has M coordinate.
        pub const HAS_M: u32 = 0x40000000;
        /// Geometry has SRID.
        pub const HAS_SRID: u32 = 0x20000000;
    }

    /// Parse EWKB header to extract geometry info.
    ///
    /// Returns (byte_order, geometry_type, has_z, has_m, srid) if valid.
    pub fn parse_ewkb_header(data: &[u8]) -> Option<EwkbHeader> {
        if data.len() < 5 {
            return None;
        }

        let byte_order = data[0];
        let is_little_endian = byte_order == 1;

        let type_word = if is_little_endian {
            u32::from_le_bytes([data[1], data[2], data[3], data[4]])
        } else {
            u32::from_be_bytes([data[1], data[2], data[3], data[4]])
        };

        let has_z = (type_word & ewkb_flags::HAS_Z) != 0;
        let has_m = (type_word & ewkb_flags::HAS_M) != 0;
        let has_srid = (type_word & ewkb_flags::HAS_SRID) != 0;
        let geometry_type = type_word & 0x0FFFFFFF;

        let (srid, data_offset) = if has_srid {
            if data.len() < 9 {
                return None;
            }
            let srid = if is_little_endian {
                i32::from_le_bytes([data[5], data[6], data[7], data[8]])
            } else {
                i32::from_be_bytes([data[5], data[6], data[7], data[8]])
            };
            (Some(srid), 9)
        } else {
            (None, 5)
        };

        Some(EwkbHeader {
            is_little_endian,
            geometry_type,
            has_z,
            has_m,
            srid,
            data_offset,
        })
    }

    /// EWKB header information.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct EwkbHeader {
        /// True if little-endian byte order.
        pub is_little_endian: bool,
        /// Geometry type code (without flags).
        pub geometry_type: u32,
        /// True if geometry has Z coordinates.
        pub has_z: bool,
        /// True if geometry has M coordinates.
        pub has_m: bool,
        /// SRID if present.
        pub srid: Option<i32>,
        /// Offset to actual geometry data.
        pub data_offset: usize,
    }

    impl EwkbHeader {
        /// Get the geometry type name.
        pub fn geometry_type_name(&self) -> &'static str {
            match self.geometry_type {
                geometry_type::POINT => "Point",
                geometry_type::LINESTRING => "LineString",
                geometry_type::POLYGON => "Polygon",
                geometry_type::MULTIPOINT => "MultiPoint",
                geometry_type::MULTILINESTRING => "MultiLineString",
                geometry_type::MULTIPOLYGON => "MultiPolygon",
                geometry_type::GEOMETRYCOLLECTION => "GeometryCollection",
                geometry_type::CIRCULARSTRING => "CircularString",
                geometry_type::COMPOUNDCURVE => "CompoundCurve",
                geometry_type::CURVEPOLYGON => "CurvePolygon",
                geometry_type::MULTICURVE => "MultiCurve",
                geometry_type::MULTISURFACE => "MultiSurface",
                geometry_type::POLYHEDRALSURFACE => "PolyhedralSurface",
                geometry_type::TIN => "Tin",
                geometry_type::TRIANGLE => "Triangle",
                _ => "Unknown",
            }
        }

        /// Get the number of dimensions (2, 3, or 4).
        pub fn dimensions(&self) -> u8 {
            match (self.has_z, self.has_m) {
                (false, false) => 2,
                (true, false) | (false, true) => 3,
                (true, true) => 4,
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_parse_point_2d() {
            // Little-endian 2D point without SRID
            let data = [
                0x01, // little endian
                0x01, 0x00, 0x00, 0x00, // type = Point (1)
                      // coordinates would follow...
            ];
            let header = parse_ewkb_header(&data).unwrap();
            assert!(header.is_little_endian);
            assert_eq!(header.geometry_type, geometry_type::POINT);
            assert!(!header.has_z);
            assert!(!header.has_m);
            assert_eq!(header.srid, None);
            assert_eq!(header.data_offset, 5);
        }

        #[test]
        fn test_parse_point_with_srid() {
            // Little-endian 2D point with SRID 4326
            let data = [
                0x01, // little endian
                0x01, 0x00, 0x00, 0x20, // type = Point (1) | HAS_SRID
                0xE6, 0x10, 0x00, 0x00, // SRID = 4326 (little endian)
            ];
            let header = parse_ewkb_header(&data).unwrap();
            assert_eq!(header.geometry_type, geometry_type::POINT);
            assert_eq!(header.srid, Some(4326));
            assert_eq!(header.data_offset, 9);
        }

        #[test]
        fn test_parse_point_3d() {
            // Little-endian 3D point (XYZ)
            let data = [
                0x01, // little endian
                0x01, 0x00, 0x00, 0x80, // type = Point (1) | HAS_Z
            ];
            let header = parse_ewkb_header(&data).unwrap();
            assert!(header.has_z);
            assert!(!header.has_m);
            assert_eq!(header.dimensions(), 3);
        }

        #[test]
        fn test_parse_point_4d() {
            // Little-endian 4D point (XYZM)
            let data = [
                0x01, // little endian
                0x01, 0x00, 0x00, 0xC0, // type = Point (1) | HAS_Z | HAS_M
            ];
            let header = parse_ewkb_header(&data).unwrap();
            assert!(header.has_z);
            assert!(header.has_m);
            assert_eq!(header.dimensions(), 4);
        }

        #[test]
        fn test_parse_point_m_only() {
            // Little-endian point with M only
            let data = [
                0x01, // little endian
                0x01, 0x00, 0x00, 0x40, // type = Point (1) | HAS_M
            ];
            let header = parse_ewkb_header(&data).unwrap();
            assert!(!header.has_z);
            assert!(header.has_m);
            assert_eq!(header.dimensions(), 3);
        }

        #[test]
        fn test_parse_big_endian() {
            // Big-endian 2D point without SRID
            let data = [
                0x00, // big endian
                0x00, 0x00, 0x00, 0x01, // type = Point (1)
            ];
            let header = parse_ewkb_header(&data).unwrap();
            assert!(!header.is_little_endian);
            assert_eq!(header.geometry_type, geometry_type::POINT);
        }

        #[test]
        fn test_parse_big_endian_with_srid() {
            // Big-endian point with SRID 4326
            let data = [
                0x00, // big endian
                0x20, 0x00, 0x00, 0x01, // type = Point (1) | HAS_SRID
                0x00, 0x00, 0x10, 0xE6, // SRID = 4326 (big endian)
            ];
            let header = parse_ewkb_header(&data).unwrap();
            assert!(!header.is_little_endian);
            assert_eq!(header.srid, Some(4326));
            assert_eq!(header.data_offset, 9);
        }

        #[test]
        fn test_geometry_type_names() {
            let tests = [
                (geometry_type::POINT, "Point"),
                (geometry_type::LINESTRING, "LineString"),
                (geometry_type::POLYGON, "Polygon"),
                (geometry_type::MULTIPOINT, "MultiPoint"),
                (geometry_type::MULTILINESTRING, "MultiLineString"),
                (geometry_type::MULTIPOLYGON, "MultiPolygon"),
                (geometry_type::GEOMETRYCOLLECTION, "GeometryCollection"),
                (geometry_type::CIRCULARSTRING, "CircularString"),
                (geometry_type::COMPOUNDCURVE, "CompoundCurve"),
                (geometry_type::CURVEPOLYGON, "CurvePolygon"),
                (geometry_type::MULTICURVE, "MultiCurve"),
                (geometry_type::MULTISURFACE, "MultiSurface"),
                (geometry_type::POLYHEDRALSURFACE, "PolyhedralSurface"),
                (geometry_type::TIN, "Tin"),
                (geometry_type::TRIANGLE, "Triangle"),
                (999, "Unknown"),
            ];

            for (geo_type, expected_name) in tests {
                let header = EwkbHeader {
                    is_little_endian: true,
                    geometry_type: geo_type,
                    has_z: false,
                    has_m: false,
                    srid: None,
                    data_offset: 5,
                };
                assert_eq!(header.geometry_type_name(), expected_name);
            }
        }

        #[test]
        fn test_too_short() {
            let data = [0x01, 0x01, 0x00, 0x00]; // Only 4 bytes
            assert!(parse_ewkb_header(&data).is_none());
        }

        #[test]
        fn test_srid_too_short() {
            // Claims to have SRID but not enough bytes
            let data = [
                0x01, // little endian
                0x01, 0x00, 0x00, 0x20, // type with HAS_SRID
                0xE6, 0x10, // Only 2 bytes instead of 4
            ];
            assert!(parse_ewkb_header(&data).is_none());
        }
    }
}

/// pgvector binary format helpers.
///
/// pgvector stores vectors in a simple binary format:
/// - 2 bytes: dimension count (u16)
/// - 2 bytes: unused (reserved)
/// - N * 4 bytes: float32 values
pub mod pgvector {
    /// Parse a pgvector binary vector.
    ///
    /// Returns the vector dimensions as f32 values.
    pub fn parse_vector(data: &[u8]) -> Option<Vec<f32>> {
        if data.len() < 4 {
            return None;
        }

        let dim = u16::from_le_bytes([data[0], data[1]]) as usize;
        let expected_len = 4 + dim * 4;

        if data.len() < expected_len {
            return None;
        }

        let mut values = Vec::with_capacity(dim);
        for i in 0..dim {
            let offset = 4 + i * 4;
            let value = f32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
            values.push(value);
        }

        Some(values)
    }

    /// Encode a vector to pgvector binary format.
    pub fn encode_vector(values: &[f32]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + values.len() * 4);

        // Dimension count (u16)
        buf.extend_from_slice(&(values.len() as u16).to_le_bytes());
        // Reserved (u16)
        buf.extend_from_slice(&0u16.to_le_bytes());

        // Float values
        for &value in values {
            buf.extend_from_slice(&value.to_le_bytes());
        }

        buf
    }

    /// Parse a halfvec binary vector (half-precision floats).
    ///
    /// Note: This returns f32 values after converting from f16.
    /// Requires the `half` crate for proper f16 support in production.
    pub fn parse_halfvec(data: &[u8]) -> Option<Vec<f32>> {
        if data.len() < 4 {
            return None;
        }

        let dim = u16::from_le_bytes([data[0], data[1]]) as usize;
        let expected_len = 4 + dim * 2;

        if data.len() < expected_len {
            return None;
        }

        let mut values = Vec::with_capacity(dim);
        for i in 0..dim {
            let offset = 4 + i * 2;
            let bits = u16::from_le_bytes([data[offset], data[offset + 1]]);
            // Simplified f16 to f32 conversion (not fully accurate for all values)
            let value = f16_to_f32_approx(bits);
            values.push(value);
        }

        Some(values)
    }

    // Approximate f16 to f32 conversion
    fn f16_to_f32_approx(bits: u16) -> f32 {
        let sign = (bits >> 15) & 1;
        let exp = (bits >> 10) & 0x1F;
        let mant = bits & 0x3FF;

        if exp == 0 {
            if mant == 0 {
                // Zero
                if sign == 1 { -0.0 } else { 0.0 }
            } else {
                // Subnormal
                let value = (mant as f32) / 1024.0 * (2.0_f32).powi(-14);
                if sign == 1 { -value } else { value }
            }
        } else if exp == 31 {
            if mant == 0 {
                // Infinity
                if sign == 1 { f32::NEG_INFINITY } else { f32::INFINITY }
            } else {
                // NaN
                f32::NAN
            }
        } else {
            // Normal number
            let value = (1.0 + (mant as f32) / 1024.0) * (2.0_f32).powi(exp as i32 - 15);
            if sign == 1 { -value } else { value }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_vector_roundtrip() {
            let original = vec![1.0, 2.0, 3.0, 4.5];
            let encoded = encode_vector(&original);
            let decoded = parse_vector(&encoded).unwrap();

            assert_eq!(original.len(), decoded.len());
            for (a, b) in original.iter().zip(decoded.iter()) {
                assert!((a - b).abs() < f32::EPSILON);
            }
        }

        #[test]
        fn test_vector_empty() {
            let original: Vec<f32> = vec![];
            let encoded = encode_vector(&original);
            let decoded = parse_vector(&encoded).unwrap();
            assert!(decoded.is_empty());
        }

        #[test]
        fn test_vector_too_short() {
            // Less than 4 bytes header
            assert!(parse_vector(&[0, 0]).is_none());
            assert!(parse_vector(&[]).is_none());
        }

        #[test]
        fn test_vector_truncated() {
            // Header says 3 elements but only has data for 1
            let mut data = vec![];
            data.extend_from_slice(&3u16.to_le_bytes()); // dimension = 3
            data.extend_from_slice(&0u16.to_le_bytes()); // reserved
            data.extend_from_slice(&1.0f32.to_le_bytes()); // only one value

            assert!(parse_vector(&data).is_none());
        }

        #[test]
        fn test_halfvec_basic() {
            // Create halfvec with known values
            let mut data = vec![];
            data.extend_from_slice(&2u16.to_le_bytes()); // dimension = 2
            data.extend_from_slice(&0u16.to_le_bytes()); // reserved

            // f16 for 1.0: sign=0, exp=15 (0b01111), mant=0 -> 0x3C00
            data.extend_from_slice(&0x3C00u16.to_le_bytes());
            // f16 for 2.0: sign=0, exp=16 (0b10000), mant=0 -> 0x4000
            data.extend_from_slice(&0x4000u16.to_le_bytes());

            let values = parse_halfvec(&data).unwrap();
            assert_eq!(values.len(), 2);
            assert!((values[0] - 1.0).abs() < 0.01);
            assert!((values[1] - 2.0).abs() < 0.01);
        }

        #[test]
        fn test_halfvec_special_values() {
            let mut data = vec![];
            data.extend_from_slice(&4u16.to_le_bytes()); // dimension = 4
            data.extend_from_slice(&0u16.to_le_bytes()); // reserved

            // Zero: 0x0000
            data.extend_from_slice(&0x0000u16.to_le_bytes());
            // Negative zero: 0x8000
            data.extend_from_slice(&0x8000u16.to_le_bytes());
            // Infinity: 0x7C00
            data.extend_from_slice(&0x7C00u16.to_le_bytes());
            // NaN: 0x7C01
            data.extend_from_slice(&0x7C01u16.to_le_bytes());

            let values = parse_halfvec(&data).unwrap();
            assert_eq!(values[0], 0.0);
            assert_eq!(values[1], -0.0);
            assert!(values[2].is_infinite() && values[2] > 0.0);
            assert!(values[3].is_nan());
        }

        #[test]
        fn test_halfvec_too_short() {
            assert!(parse_halfvec(&[0, 0]).is_none());
            assert!(parse_halfvec(&[]).is_none());
        }

        #[test]
        fn test_halfvec_truncated() {
            let mut data = vec![];
            data.extend_from_slice(&3u16.to_le_bytes()); // dimension = 3
            data.extend_from_slice(&0u16.to_le_bytes()); // reserved
            data.extend_from_slice(&0x3C00u16.to_le_bytes()); // only one value

            assert!(parse_halfvec(&data).is_none());
        }

        #[test]
        fn test_halfvec_subnormal() {
            let mut data = vec![];
            data.extend_from_slice(&1u16.to_le_bytes()); // dimension = 1
            data.extend_from_slice(&0u16.to_le_bytes()); // reserved

            // Subnormal: exp=0, mant=1 -> smallest positive subnormal
            data.extend_from_slice(&0x0001u16.to_le_bytes());

            let values = parse_halfvec(&data).unwrap();
            assert!(values[0] > 0.0 && values[0] < 1e-6);
        }
    }
}

/// hstore binary format helpers.
///
/// hstore stores key-value pairs in binary format:
/// - 4 bytes: entry count (i32)
/// - For each entry:
///   - 4 bytes: key length (i32)
///   - N bytes: key data
///   - 4 bytes: value length (i32, -1 for NULL)
///   - N bytes: value data (if not NULL)
pub mod hstore {
    /// Parse hstore binary data into key-value pairs.
    ///
    /// Values can be None (NULL in PostgreSQL).
    pub fn parse_hstore(data: &[u8]) -> Option<Vec<(String, Option<String>)>> {
        if data.len() < 4 {
            return None;
        }

        let count = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut entries = Vec::with_capacity(count.min(10000));
        let mut offset = 4;

        for _ in 0..count {
            // Read key
            if offset + 4 > data.len() {
                return None;
            }
            let key_len = i32::from_be_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]) as usize;
            offset += 4;

            if offset + key_len > data.len() {
                return None;
            }
            let key = String::from_utf8(data[offset..offset + key_len].to_vec()).ok()?;
            offset += key_len;

            // Read value
            if offset + 4 > data.len() {
                return None;
            }
            let value_len = i32::from_be_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
            offset += 4;

            let value = if value_len < 0 {
                None
            } else {
                let len = value_len as usize;
                if offset + len > data.len() {
                    return None;
                }
                let v = String::from_utf8(data[offset..offset + len].to_vec()).ok()?;
                offset += len;
                Some(v)
            };

            entries.push((key, value));
        }

        Some(entries)
    }

    /// Encode key-value pairs to hstore binary format.
    pub fn encode_hstore(entries: &[(String, Option<String>)]) -> Vec<u8> {
        let mut buf = Vec::new();

        // Entry count
        buf.extend_from_slice(&(entries.len() as i32).to_be_bytes());

        for (key, value) in entries {
            // Key
            buf.extend_from_slice(&(key.len() as i32).to_be_bytes());
            buf.extend_from_slice(key.as_bytes());

            // Value
            match value {
                Some(v) => {
                    buf.extend_from_slice(&(v.len() as i32).to_be_bytes());
                    buf.extend_from_slice(v.as_bytes());
                }
                None => {
                    buf.extend_from_slice(&(-1_i32).to_be_bytes());
                }
            }
        }

        buf
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_hstore_roundtrip() {
            let original = vec![
                ("key1".to_string(), Some("value1".to_string())),
                ("key2".to_string(), None),
                ("key3".to_string(), Some("value3".to_string())),
            ];

            let encoded = encode_hstore(&original);
            let decoded = parse_hstore(&encoded).unwrap();

            assert_eq!(original, decoded);
        }

        #[test]
        fn test_hstore_empty() {
            let original: Vec<(String, Option<String>)> = vec![];
            let encoded = encode_hstore(&original);
            let decoded = parse_hstore(&encoded).unwrap();
            assert!(decoded.is_empty());
        }

        #[test]
        fn test_hstore_too_short() {
            assert!(parse_hstore(&[]).is_none());
            assert!(parse_hstore(&[0, 0, 0]).is_none()); // Less than 4 bytes
        }

        #[test]
        fn test_hstore_truncated_key_length() {
            // Says 1 entry but not enough data for key length
            let mut data = vec![];
            data.extend_from_slice(&1i32.to_be_bytes()); // 1 entry
            // No more data for key length
            assert!(parse_hstore(&data).is_none());
        }

        #[test]
        fn test_hstore_truncated_key_data() {
            // Has key length but not enough data for key
            let mut data = vec![];
            data.extend_from_slice(&1i32.to_be_bytes()); // 1 entry
            data.extend_from_slice(&10i32.to_be_bytes()); // key length = 10
            data.extend_from_slice(b"short"); // Only 5 bytes
            assert!(parse_hstore(&data).is_none());
        }

        #[test]
        fn test_hstore_truncated_value_length() {
            // Has key but not enough data for value length
            let mut data = vec![];
            data.extend_from_slice(&1i32.to_be_bytes()); // 1 entry
            data.extend_from_slice(&3i32.to_be_bytes()); // key length = 3
            data.extend_from_slice(b"key"); // key data
            // No data for value length
            assert!(parse_hstore(&data).is_none());
        }

        #[test]
        fn test_hstore_truncated_value_data() {
            // Has value length but not enough data for value
            let mut data = vec![];
            data.extend_from_slice(&1i32.to_be_bytes()); // 1 entry
            data.extend_from_slice(&3i32.to_be_bytes()); // key length = 3
            data.extend_from_slice(b"key"); // key data
            data.extend_from_slice(&10i32.to_be_bytes()); // value length = 10
            data.extend_from_slice(b"short"); // Only 5 bytes
            assert!(parse_hstore(&data).is_none());
        }

        #[test]
        fn test_hstore_invalid_utf8_key() {
            let mut data = vec![];
            data.extend_from_slice(&1i32.to_be_bytes()); // 1 entry
            data.extend_from_slice(&2i32.to_be_bytes()); // key length = 2
            data.extend_from_slice(&[0xFF, 0xFE]); // Invalid UTF-8
            data.extend_from_slice(&(-1i32).to_be_bytes()); // NULL value
            assert!(parse_hstore(&data).is_none());
        }

        #[test]
        fn test_hstore_invalid_utf8_value() {
            let mut data = vec![];
            data.extend_from_slice(&1i32.to_be_bytes()); // 1 entry
            data.extend_from_slice(&3i32.to_be_bytes()); // key length = 3
            data.extend_from_slice(b"key"); // valid key
            data.extend_from_slice(&2i32.to_be_bytes()); // value length = 2
            data.extend_from_slice(&[0xFF, 0xFE]); // Invalid UTF-8
            assert!(parse_hstore(&data).is_none());
        }
    }
}

/// ltree binary format helpers.
///
/// ltree stores hierarchical labels separated by dots.
/// Binary format is simply the text representation.
pub mod ltree {
    /// Parse ltree labels from text representation.
    pub fn parse_labels(text: &str) -> Vec<&str> {
        if text.is_empty() { vec![] } else { text.split('.').collect() }
    }

    /// Join labels into ltree text representation.
    pub fn join_labels(labels: &[&str]) -> String {
        labels.join(".")
    }

    /// Check if a label is valid (alphanumeric and underscore only).
    pub fn is_valid_label(label: &str) -> bool {
        !label.is_empty() && label.len() <= 255 && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    }

    /// Check if an ltree path is valid.
    pub fn is_valid_path(path: &str) -> bool {
        if path.is_empty() {
            return true; // Empty path is valid
        }
        parse_labels(path).iter().all(|label| is_valid_label(label))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_parse_labels() {
            assert_eq!(parse_labels("Top.Science.Astronomy"), vec!["Top", "Science", "Astronomy"]);
            assert_eq!(parse_labels("Single"), vec!["Single"]);
            assert!(parse_labels("").is_empty());
        }

        #[test]
        fn test_valid_labels() {
            assert!(is_valid_label("Science"));
            assert!(is_valid_label("foo_bar"));
            assert!(is_valid_label("test123"));
            assert!(!is_valid_label("")); // Empty
            assert!(!is_valid_label("foo.bar")); // Contains dot
            assert!(!is_valid_label("foo-bar")); // Contains hyphen
        }

        #[test]
        fn test_join_labels() {
            assert_eq!(join_labels(&["Top", "Science", "Astronomy"]), "Top.Science.Astronomy");
            assert_eq!(join_labels(&["Single"]), "Single");
            assert_eq!(join_labels(&[]), "");
        }

        #[test]
        fn test_is_valid_path() {
            assert!(is_valid_path("Top.Science.Astronomy"));
            assert!(is_valid_path("Single"));
            assert!(is_valid_path("")); // Empty path is valid
            assert!(is_valid_path("foo_bar.test123"));
            assert!(!is_valid_path("foo.bar-baz")); // Contains hyphen
            assert!(!is_valid_path("foo..bar")); // Empty label between dots
        }

        #[test]
        fn test_label_max_length() {
            let long_label = "a".repeat(255);
            assert!(is_valid_label(&long_label));

            let too_long = "a".repeat(256);
            assert!(!is_valid_label(&too_long));
        }
    }
}

/// citext comparison helpers.
///
/// citext (case-insensitive text) is stored as regular text but compared
/// case-insensitively. This is handled at the PostgreSQL level.
pub mod citext {
    /// Compare two strings case-insensitively (ASCII).
    pub fn eq_ignore_ascii_case(a: &str, b: &str) -> bool {
        a.eq_ignore_ascii_case(b)
    }

    /// Compare two strings case-insensitively (Unicode).
    /// Note: For full Unicode case folding, use the `unicase` crate.
    pub fn compare_ignore_case(a: &str, b: &str) -> std::cmp::Ordering {
        a.to_lowercase().cmp(&b.to_lowercase())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_eq_ignore_ascii_case() {
            assert!(eq_ignore_ascii_case("Hello", "hello"));
            assert!(eq_ignore_ascii_case("WORLD", "world"));
            assert!(eq_ignore_ascii_case("MixedCase", "mixedcase"));
            assert!(!eq_ignore_ascii_case("Hello", "World"));
            assert!(eq_ignore_ascii_case("", ""));
        }

        #[test]
        fn test_compare_ignore_case() {
            use std::cmp::Ordering;
            assert_eq!(compare_ignore_case("abc", "ABC"), Ordering::Equal);
            assert_eq!(compare_ignore_case("abc", "def"), Ordering::Less);
            assert_eq!(compare_ignore_case("xyz", "ABC"), Ordering::Greater);
            assert_eq!(compare_ignore_case("", ""), Ordering::Equal);
        }
    }
}

/// ip4r extension helpers for IP address types.
///
/// ip4r provides ip4, ip4r, ip6, ip6r, ipaddress, and iprange types.
/// Binary format:
/// - ip4: 4 bytes (network byte order)
/// - ip6: 16 bytes (network byte order)
/// - ip4r: 8 bytes (lower ip4 + upper ip4)
/// - ip6r: 32 bytes (lower ip6 + upper ip6)
pub mod ip4r {
    use std::net::{Ipv4Addr, Ipv6Addr};

    /// Parse an ip4 (IPv4 address) from binary format.
    pub fn parse_ip4(data: &[u8]) -> Option<Ipv4Addr> {
        if data.len() < 4 {
            return None;
        }
        Some(Ipv4Addr::new(data[0], data[1], data[2], data[3]))
    }

    /// Encode an IPv4 address to binary format.
    pub fn encode_ip4(addr: Ipv4Addr) -> [u8; 4] {
        addr.octets()
    }

    /// Parse an ip6 (IPv6 address) from binary format.
    pub fn parse_ip6(data: &[u8]) -> Option<Ipv6Addr> {
        if data.len() < 16 {
            return None;
        }
        let octets: [u8; 16] = data[..16].try_into().ok()?;
        Some(Ipv6Addr::from(octets))
    }

    /// Encode an IPv6 address to binary format.
    pub fn encode_ip6(addr: Ipv6Addr) -> [u8; 16] {
        addr.octets()
    }

    /// IPv4 range.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Ip4Range {
        pub lower: Ipv4Addr,
        pub upper: Ipv4Addr,
    }

    /// Parse an ip4r (IPv4 range) from binary format.
    pub fn parse_ip4r(data: &[u8]) -> Option<Ip4Range> {
        if data.len() < 8 {
            return None;
        }
        let lower = parse_ip4(&data[0..4])?;
        let upper = parse_ip4(&data[4..8])?;
        Some(Ip4Range { lower, upper })
    }

    /// Encode an IPv4 range to binary format.
    pub fn encode_ip4r(range: &Ip4Range) -> [u8; 8] {
        let mut buf = [0u8; 8];
        buf[0..4].copy_from_slice(&encode_ip4(range.lower));
        buf[4..8].copy_from_slice(&encode_ip4(range.upper));
        buf
    }

    /// IPv6 range.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Ip6Range {
        pub lower: Ipv6Addr,
        pub upper: Ipv6Addr,
    }

    /// Parse an ip6r (IPv6 range) from binary format.
    pub fn parse_ip6r(data: &[u8]) -> Option<Ip6Range> {
        if data.len() < 32 {
            return None;
        }
        let lower = parse_ip6(&data[0..16])?;
        let upper = parse_ip6(&data[16..32])?;
        Some(Ip6Range { lower, upper })
    }

    /// Encode an IPv6 range to binary format.
    pub fn encode_ip6r(range: &Ip6Range) -> [u8; 32] {
        let mut buf = [0u8; 32];
        buf[0..16].copy_from_slice(&encode_ip6(range.lower));
        buf[16..32].copy_from_slice(&encode_ip6(range.upper));
        buf
    }

    /// Check if an IPv4 address is in a range.
    pub fn ip4_in_range(addr: Ipv4Addr, range: &Ip4Range) -> bool {
        let addr_u32 = u32::from(addr);
        let lower_u32 = u32::from(range.lower);
        let upper_u32 = u32::from(range.upper);
        addr_u32 >= lower_u32 && addr_u32 <= upper_u32
    }

    /// Check if an IPv6 address is in a range.
    pub fn ip6_in_range(addr: Ipv6Addr, range: &Ip6Range) -> bool {
        let addr_u128 = u128::from(addr);
        let lower_u128 = u128::from(range.lower);
        let upper_u128 = u128::from(range.upper);
        addr_u128 >= lower_u128 && addr_u128 <= upper_u128
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_ip4_roundtrip() {
            let addr = Ipv4Addr::new(192, 168, 1, 100);
            let encoded = encode_ip4(addr);
            let decoded = parse_ip4(&encoded).unwrap();
            assert_eq!(addr, decoded);
        }

        #[test]
        fn test_ip6_roundtrip() {
            let addr = Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1);
            let encoded = encode_ip6(addr);
            let decoded = parse_ip6(&encoded).unwrap();
            assert_eq!(addr, decoded);
        }

        #[test]
        fn test_ip4r_range_check() {
            let range = Ip4Range {
                lower: Ipv4Addr::new(192, 168, 1, 0),
                upper: Ipv4Addr::new(192, 168, 1, 255),
            };
            assert!(ip4_in_range(Ipv4Addr::new(192, 168, 1, 100), &range));
            assert!(!ip4_in_range(Ipv4Addr::new(192, 168, 2, 1), &range));
        }

        #[test]
        fn test_ip4r_roundtrip() {
            let range = Ip4Range {
                lower: Ipv4Addr::new(10, 0, 0, 0),
                upper: Ipv4Addr::new(10, 255, 255, 255),
            };
            let encoded = encode_ip4r(&range);
            let decoded = parse_ip4r(&encoded).unwrap();
            assert_eq!(range, decoded);
        }

        #[test]
        fn test_ip6r_roundtrip() {
            let range = Ip6Range {
                lower: Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0),
                upper: Ipv6Addr::new(0xfe80, 0, 0, 0, 0xffff, 0xffff, 0xffff, 0xffff),
            };
            let encoded = encode_ip6r(&range);
            let decoded = parse_ip6r(&encoded).unwrap();
            assert_eq!(range, decoded);
        }

        #[test]
        fn test_ip6_in_range() {
            let range = Ip6Range {
                lower: Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0),
                upper: Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0xffff, 0xffff, 0xffff, 0xffff),
            };
            assert!(ip6_in_range(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1), &range));
            assert!(!ip6_in_range(Ipv6Addr::new(0x2001, 0xdb9, 0, 0, 0, 0, 0, 1), &range));
        }

        #[test]
        fn test_ip4_too_short() {
            assert!(parse_ip4(&[192, 168, 1]).is_none());
            assert!(parse_ip4(&[]).is_none());
        }

        #[test]
        fn test_ip6_too_short() {
            assert!(parse_ip6(&[0; 15]).is_none());
            assert!(parse_ip6(&[]).is_none());
        }

        #[test]
        fn test_ip4r_too_short() {
            assert!(parse_ip4r(&[0; 7]).is_none());
        }

        #[test]
        fn test_ip6r_too_short() {
            assert!(parse_ip6r(&[0; 31]).is_none());
        }

        #[test]
        fn test_ip4_range_boundaries() {
            let range = Ip4Range {
                lower: Ipv4Addr::new(192, 168, 1, 0),
                upper: Ipv4Addr::new(192, 168, 1, 255),
            };
            // Test exact boundaries
            assert!(ip4_in_range(Ipv4Addr::new(192, 168, 1, 0), &range));
            assert!(ip4_in_range(Ipv4Addr::new(192, 168, 1, 255), &range));
        }
    }
}

/// semver extension helpers for semantic versioning.
///
/// semver provides semantic version types following the semver.org specification.
/// Binary format is version-specific; text format is preferred for parsing.
pub mod semver {
    /// Parsed semantic version.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct SemVer {
        pub major: u32,
        pub minor: u32,
        pub patch: u32,
        pub prerelease: Option<String>,
        pub build: Option<String>,
    }

    impl SemVer {
        /// Create a new semantic version.
        pub fn new(major: u32, minor: u32, patch: u32) -> Self {
            Self { major, minor, patch, prerelease: None, build: None }
        }

        /// Set prerelease identifier.
        pub fn with_prerelease(mut self, prerelease: &str) -> Self {
            self.prerelease = Some(prerelease.to_string());
            self
        }

        /// Set build metadata.
        pub fn with_build(mut self, build: &str) -> Self {
            self.build = Some(build.to_string());
            self
        }
    }

    /// Parse a semantic version from text format.
    ///
    /// Supports formats like: 1.2.3, 1.2.3-alpha, 1.2.3+build, 1.2.3-alpha+build
    pub fn parse_semver(text: &str) -> Option<SemVer> {
        let text = text.trim();
        if text.is_empty() {
            return None;
        }

        // Split off build metadata first
        let (version_pre, build) = if let Some(pos) = text.find('+') {
            (&text[..pos], Some(&text[pos + 1..]))
        } else {
            (text, None)
        };

        // Split off prerelease
        let (version, prerelease) = if let Some(pos) = version_pre.find('-') {
            (&version_pre[..pos], Some(&version_pre[pos + 1..]))
        } else {
            (version_pre, None)
        };

        // Parse major.minor.patch
        let parts: Vec<&str> = version.split('.').collect();
        if parts.len() != 3 {
            return None;
        }

        let major = parts[0].parse().ok()?;
        let minor = parts[1].parse().ok()?;
        let patch = parts[2].parse().ok()?;

        Some(SemVer {
            major,
            minor,
            patch,
            prerelease: prerelease.map(String::from),
            build: build.map(String::from),
        })
    }

    /// Format a semantic version to text.
    pub fn format_semver(ver: &SemVer) -> String {
        let mut s = format!("{}.{}.{}", ver.major, ver.minor, ver.patch);
        if let Some(ref pre) = ver.prerelease {
            s.push('-');
            s.push_str(pre);
        }
        if let Some(ref build) = ver.build {
            s.push('+');
            s.push_str(build);
        }
        s
    }

    /// Compare two semantic versions (ignoring build metadata per spec).
    pub fn compare(a: &SemVer, b: &SemVer) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        // Compare major.minor.patch
        match a.major.cmp(&b.major) {
            Ordering::Equal => {}
            ord => return ord,
        }
        match a.minor.cmp(&b.minor) {
            Ordering::Equal => {}
            ord => return ord,
        }
        match a.patch.cmp(&b.patch) {
            Ordering::Equal => {}
            ord => return ord,
        }

        // Prerelease has lower precedence than release
        match (&a.prerelease, &b.prerelease) {
            (None, None) => Ordering::Equal,
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some(pre_a), Some(pre_b)) => pre_a.cmp(pre_b),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_parse_semver() {
            let ver = parse_semver("1.2.3").unwrap();
            assert_eq!(ver.major, 1);
            assert_eq!(ver.minor, 2);
            assert_eq!(ver.patch, 3);

            let ver = parse_semver("1.0.0-alpha+build.123").unwrap();
            assert_eq!(ver.prerelease, Some("alpha".to_string()));
            assert_eq!(ver.build, Some("build.123".to_string()));
        }

        #[test]
        fn test_semver_compare() {
            let v1 = parse_semver("1.0.0").unwrap();
            let v2 = parse_semver("2.0.0").unwrap();
            assert_eq!(compare(&v1, &v2), std::cmp::Ordering::Less);

            let v1 = parse_semver("1.0.0-alpha").unwrap();
            let v2 = parse_semver("1.0.0").unwrap();
            assert_eq!(compare(&v1, &v2), std::cmp::Ordering::Less);
        }

        #[test]
        fn test_format_semver() {
            let ver = SemVer::new(1, 2, 3);
            assert_eq!(format_semver(&ver), "1.2.3");

            let ver = SemVer::new(1, 0, 0).with_prerelease("alpha");
            assert_eq!(format_semver(&ver), "1.0.0-alpha");

            let ver = SemVer::new(1, 0, 0).with_build("build.123");
            assert_eq!(format_semver(&ver), "1.0.0+build.123");

            let ver = SemVer::new(1, 0, 0).with_prerelease("beta.1").with_build("20230101");
            assert_eq!(format_semver(&ver), "1.0.0-beta.1+20230101");
        }

        #[test]
        fn test_parse_semver_invalid() {
            assert!(parse_semver("").is_none());
            assert!(parse_semver("1").is_none());
            assert!(parse_semver("1.2").is_none());
            assert!(parse_semver("1.2.x").is_none());
            assert!(parse_semver("a.b.c").is_none());
        }

        #[test]
        fn test_semver_compare_minor_patch() {
            use std::cmp::Ordering;

            let v1 = parse_semver("1.0.0").unwrap();
            let v2 = parse_semver("1.1.0").unwrap();
            assert_eq!(compare(&v1, &v2), Ordering::Less);

            let v1 = parse_semver("1.1.0").unwrap();
            let v2 = parse_semver("1.1.1").unwrap();
            assert_eq!(compare(&v1, &v2), Ordering::Less);

            let v1 = parse_semver("1.1.1").unwrap();
            let v2 = parse_semver("1.1.1").unwrap();
            assert_eq!(compare(&v1, &v2), Ordering::Equal);
        }

        #[test]
        fn test_semver_compare_prerelease() {
            use std::cmp::Ordering;

            // Two prereleases are compared lexically
            let v1 = parse_semver("1.0.0-alpha").unwrap();
            let v2 = parse_semver("1.0.0-beta").unwrap();
            assert_eq!(compare(&v1, &v2), Ordering::Less);

            // Release version is greater than prerelease
            let v1 = parse_semver("1.0.0").unwrap();
            let v2 = parse_semver("1.0.0-alpha").unwrap();
            assert_eq!(compare(&v1, &v2), Ordering::Greater);
        }
    }
}

/// cube extension helpers for N-dimensional cubes.
///
/// cube provides n-dimensional cube/point types for spatial operations.
/// Binary format: header + float8 coordinates.
pub mod cube {
    /// N-dimensional cube or point.
    #[derive(Clone, Debug, PartialEq)]
    pub struct Cube {
        /// Lower-left corner coordinates.
        pub ll: Vec<f64>,
        /// Upper-right corner coordinates (same as ll for points).
        pub ur: Vec<f64>,
    }

    impl Cube {
        /// Create a point (zero-volume cube).
        pub fn point(coords: Vec<f64>) -> Self {
            Self { ll: coords.clone(), ur: coords }
        }

        /// Create a cube from two corners.
        pub fn from_corners(ll: Vec<f64>, ur: Vec<f64>) -> Option<Self> {
            if ll.len() != ur.len() {
                return None;
            }
            Some(Self { ll, ur })
        }

        /// Get the number of dimensions.
        pub fn dimensions(&self) -> usize {
            self.ll.len()
        }

        /// Check if this is a point (zero-volume cube).
        pub fn is_point(&self) -> bool {
            self.ll == self.ur
        }
    }

    /// Parse a cube from text format.
    ///
    /// Supports formats:
    /// - Point: (1, 2, 3)
    /// - Cube: (0, 0), (1, 1)
    pub fn parse_cube_text(text: &str) -> Option<Cube> {
        let text = text.trim();

        // Check for two-corner format: (x1, y1), (x2, y2)
        if let Some(comma_pos) = text.find("), (") {
            let ll_str = &text[1..comma_pos];
            let ur_str = &text[comma_pos + 4..text.len() - 1];

            let ll: Vec<f64> = ll_str.split(',').map(|s| s.trim().parse().ok()).collect::<Option<_>>()?;
            let ur: Vec<f64> = ur_str.split(',').map(|s| s.trim().parse().ok()).collect::<Option<_>>()?;

            return Cube::from_corners(ll, ur);
        }

        // Single point format: (x, y, z)
        if text.starts_with('(') && text.ends_with(')') {
            let coords_str = &text[1..text.len() - 1];
            let coords: Vec<f64> = coords_str.split(',').map(|s| s.trim().parse().ok()).collect::<Option<_>>()?;
            return Some(Cube::point(coords));
        }

        None
    }

    /// Format a cube to text.
    pub fn format_cube(cube: &Cube) -> String {
        if cube.is_point() {
            let coords: Vec<String> = cube.ll.iter().map(|c| c.to_string()).collect();
            format!("({})", coords.join(", "))
        } else {
            let ll: Vec<String> = cube.ll.iter().map(|c| c.to_string()).collect();
            let ur: Vec<String> = cube.ur.iter().map(|c| c.to_string()).collect();
            format!("({}), ({})", ll.join(", "), ur.join(", "))
        }
    }

    /// Calculate Euclidean distance between two points/cubes.
    pub fn distance(a: &Cube, b: &Cube) -> Option<f64> {
        if a.dimensions() != b.dimensions() {
            return None;
        }

        // Use centroids for cubes
        let center_a: Vec<f64> = a.ll.iter().zip(&a.ur).map(|(l, u)| (l + u) / 2.0).collect();
        let center_b: Vec<f64> = b.ll.iter().zip(&b.ur).map(|(l, u)| (l + u) / 2.0).collect();

        let sum_sq: f64 = center_a.iter().zip(&center_b).map(|(a, b)| (a - b).powi(2)).sum();

        Some(sum_sq.sqrt())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_parse_point() {
            let cube = parse_cube_text("(1, 2, 3)").unwrap();
            assert!(cube.is_point());
            assert_eq!(cube.dimensions(), 3);
            assert_eq!(cube.ll, vec![1.0, 2.0, 3.0]);
        }

        #[test]
        fn test_parse_cube() {
            let cube = parse_cube_text("(0, 0), (1, 1)").unwrap();
            assert!(!cube.is_point());
            assert_eq!(cube.ll, vec![0.0, 0.0]);
            assert_eq!(cube.ur, vec![1.0, 1.0]);
        }

        #[test]
        fn test_distance() {
            let a = Cube::point(vec![0.0, 0.0]);
            let b = Cube::point(vec![3.0, 4.0]);
            let dist = distance(&a, &b).unwrap();
            assert!((dist - 5.0).abs() < 1e-10);
        }

        #[test]
        fn test_format_cube_point() {
            let cube = Cube::point(vec![1.0, 2.0, 3.0]);
            assert_eq!(format_cube(&cube), "(1, 2, 3)");
        }

        #[test]
        fn test_format_cube_box() {
            let cube = Cube::from_corners(vec![0.0, 0.0], vec![1.0, 1.0]).unwrap();
            assert_eq!(format_cube(&cube), "(0, 0), (1, 1)");
        }

        #[test]
        fn test_from_corners_mismatch() {
            let result = Cube::from_corners(vec![0.0, 0.0], vec![1.0]);
            assert!(result.is_none());
        }

        #[test]
        fn test_distance_dimension_mismatch() {
            let a = Cube::point(vec![0.0, 0.0]);
            let b = Cube::point(vec![1.0, 1.0, 1.0]);
            assert!(distance(&a, &b).is_none());
        }

        #[test]
        fn test_parse_cube_invalid() {
            assert!(parse_cube_text("not a cube").is_none());
            assert!(parse_cube_text("[1, 2]").is_none()); // Wrong brackets
        }

        #[test]
        fn test_cube_dimensions() {
            let cube = Cube::point(vec![1.0, 2.0, 3.0, 4.0]);
            assert_eq!(cube.dimensions(), 4);
        }

        #[test]
        fn test_distance_cubes_uses_centroids() {
            // Distance between cubes uses their centroids
            let a = Cube::from_corners(vec![0.0, 0.0], vec![2.0, 2.0]).unwrap(); // center (1, 1)
            let b = Cube::from_corners(vec![4.0, 1.0], vec![6.0, 1.0]).unwrap(); // center (5, 1)
            let dist = distance(&a, &b).unwrap();
            assert!((dist - 4.0).abs() < 1e-10);
        }
    }
}

/// seg extension helpers for floating-point intervals.
///
/// seg provides a floating-point interval type with optional uncertainty.
pub mod seg {
    /// Floating-point interval.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct Seg {
        pub lower: f32,
        pub upper: f32,
    }

    impl Seg {
        /// Create an interval from two bounds.
        pub fn new(lower: f32, upper: f32) -> Self {
            Self { lower, upper }
        }

        /// Create a point interval.
        pub fn point(value: f32) -> Self {
            Self { lower: value, upper: value }
        }

        /// Check if this is a point (zero-width interval).
        pub fn is_point(&self) -> bool {
            (self.lower - self.upper).abs() < f32::EPSILON
        }

        /// Get the center of the interval.
        pub fn center(&self) -> f32 {
            (self.lower + self.upper) / 2.0
        }

        /// Get the width of the interval.
        pub fn width(&self) -> f32 {
            self.upper - self.lower
        }

        /// Check if a value is contained in the interval.
        pub fn contains(&self, value: f32) -> bool {
            value >= self.lower && value <= self.upper
        }

        /// Check if two intervals overlap.
        pub fn overlaps(&self, other: &Seg) -> bool {
            self.lower <= other.upper && other.lower <= self.upper
        }
    }

    /// Parse a seg from text format.
    ///
    /// Supports formats: "1.5", "1 .. 2", "1.5 <-> 2.5"
    pub fn parse_seg_text(text: &str) -> Option<Seg> {
        let text = text.trim();

        // Range format with ..
        if let Some(pos) = text.find("..") {
            let lower = text[..pos].trim().parse().ok()?;
            let upper = text[pos + 2..].trim().parse().ok()?;
            return Some(Seg::new(lower, upper));
        }

        // Range format with <->
        if let Some(pos) = text.find("<->") {
            let lower = text[..pos].trim().parse().ok()?;
            let upper = text[pos + 3..].trim().parse().ok()?;
            return Some(Seg::new(lower, upper));
        }

        // Single value
        let value = text.parse().ok()?;
        Some(Seg::point(value))
    }

    /// Format a seg to text.
    pub fn format_seg(seg: &Seg) -> String {
        if seg.is_point() {
            seg.lower.to_string()
        } else {
            format!("{} .. {}", seg.lower, seg.upper)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_parse_seg() {
            let seg = parse_seg_text("1.5").unwrap();
            assert!(seg.is_point());

            let seg = parse_seg_text("1 .. 2").unwrap();
            assert_eq!(seg.lower, 1.0);
            assert_eq!(seg.upper, 2.0);
        }

        #[test]
        fn test_seg_operations() {
            let seg = Seg::new(1.0, 3.0);
            assert!(seg.contains(2.0));
            assert!(!seg.contains(4.0));
            assert_eq!(seg.center(), 2.0);
            assert_eq!(seg.width(), 2.0);
        }

        #[test]
        fn test_format_seg_point() {
            let seg = Seg::point(1.5);
            assert_eq!(format_seg(&seg), "1.5");
        }

        #[test]
        fn test_format_seg_range() {
            let seg = Seg::new(1.0, 2.0);
            assert_eq!(format_seg(&seg), "1 .. 2");
        }

        #[test]
        fn test_parse_seg_arrow_format() {
            let seg = parse_seg_text("1.5 <-> 2.5").unwrap();
            assert_eq!(seg.lower, 1.5);
            assert_eq!(seg.upper, 2.5);
        }

        #[test]
        fn test_seg_overlaps() {
            let a = Seg::new(1.0, 3.0);
            let b = Seg::new(2.0, 4.0);
            let c = Seg::new(4.0, 5.0);

            assert!(a.overlaps(&b));
            assert!(!a.overlaps(&c));
        }

        #[test]
        fn test_seg_contains_boundaries() {
            let seg = Seg::new(1.0, 3.0);
            assert!(seg.contains(1.0)); // Lower boundary
            assert!(seg.contains(3.0)); // Upper boundary
        }

        #[test]
        fn test_parse_seg_invalid() {
            assert!(parse_seg_text("not a number").is_none());
        }
    }
}

/// Apache AGE extension helpers for graph data.
///
/// AGE provides a graph database layer on top of PostgreSQL with Cypher query support.
/// agtype is a JSON-like type that can represent vertices, edges, and paths.
pub mod age {
    /// Graph element ID (vertex or edge).
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct GraphId {
        /// Graph namespace ID.
        pub graph_id: i64,
        /// Local element ID within the graph.
        pub local_id: i64,
    }

    impl GraphId {
        /// Create a new graph ID.
        pub fn new(graph_id: i64, local_id: i64) -> Self {
            Self { graph_id, local_id }
        }

        /// Parse a graph ID from the combined 64-bit representation.
        pub fn from_i64(id: i64) -> Self {
            // AGE uses: (graph_id << 48) | local_id
            let graph_id = id >> 48;
            let local_id = id & 0x0000_FFFF_FFFF_FFFF;
            Self { graph_id, local_id }
        }

        /// Convert to the combined 64-bit representation.
        pub fn to_i64(&self) -> i64 {
            (self.graph_id << 48) | (self.local_id & 0x0000_FFFF_FFFF_FFFF)
        }
    }

    /// Graph vertex.
    #[derive(Clone, Debug, PartialEq)]
    pub struct Vertex {
        pub id: GraphId,
        pub label: String,
        pub properties: Vec<(String, String)>,
    }

    /// Graph edge.
    #[derive(Clone, Debug, PartialEq)]
    pub struct Edge {
        pub id: GraphId,
        pub start_id: GraphId,
        pub end_id: GraphId,
        pub label: String,
        pub properties: Vec<(String, String)>,
    }

    /// Parse a graph ID from text format (e.g., "1234567890123456789").
    pub fn parse_graphid(text: &str) -> Option<GraphId> {
        let id: i64 = text.trim().parse().ok()?;
        Some(GraphId::from_i64(id))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_graphid_roundtrip() {
            let id = GraphId::new(1, 42);
            let i64_val = id.to_i64();
            let parsed = GraphId::from_i64(i64_val);
            assert_eq!(id, parsed);
        }

        #[test]
        fn test_parse_graphid() {
            let id = parse_graphid("281474976710656").unwrap();
            // 281474976710656 = 1 << 48 = graph_id=1, local_id=0
            assert_eq!(id.graph_id, 1);
            assert_eq!(id.local_id, 0);
        }

        #[test]
        fn test_parse_graphid_invalid() {
            assert!(parse_graphid("not a number").is_none());
            assert!(parse_graphid("").is_none());
        }

        #[test]
        fn test_graphid_with_local_id() {
            let id = GraphId::new(2, 12345);
            let i64_val = id.to_i64();
            let parsed = GraphId::from_i64(i64_val);
            assert_eq!(parsed.graph_id, 2);
            assert_eq!(parsed.local_id, 12345);
        }

        #[test]
        fn test_vertex_edge_types() {
            // Just verify these compile and can be constructed
            let vertex = Vertex {
                id: GraphId::new(1, 1),
                label: "Person".to_string(),
                properties: vec![("name".to_string(), "Alice".to_string())],
            };
            assert_eq!(vertex.label, "Person");

            let edge = Edge {
                id: GraphId::new(1, 100),
                start_id: GraphId::new(1, 1),
                end_id: GraphId::new(1, 2),
                label: "KNOWS".to_string(),
                properties: vec![],
            };
            assert_eq!(edge.label, "KNOWS");
        }
    }
}

/// roaringbitmap extension helpers.
///
/// roaringbitmap provides compressed bitmap functionality using the Roaring format.
/// The binary format follows the Roaring Bitmap serialization specification.
pub mod roaringbitmap {
    /// Roaring bitmap header info.
    #[derive(Clone, Debug)]
    pub struct RoaringHeader {
        /// Number of containers.
        pub container_count: u32,
        /// Total cardinality (number of set bits).
        pub cardinality: Option<u64>,
    }

    /// Parse the header of a serialized roaring bitmap.
    ///
    /// Returns basic info about the bitmap without fully parsing it.
    pub fn parse_header(data: &[u8]) -> Option<RoaringHeader> {
        if data.len() < 8 {
            return None;
        }

        // Cookie and container count (little endian)
        let cookie = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);

        // Standard format: cookie = 12346
        // Run format: cookie = 12347
        let is_valid = cookie == 12346 || cookie == 12347;
        if !is_valid {
            return None;
        }

        let container_count = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);

        Some(RoaringHeader {
            container_count,
            cardinality: None, // Would need full parse to compute
        })
    }

    /// Estimate the memory size of a roaring bitmap from its serialized form.
    pub fn estimate_memory_size(data: &[u8]) -> usize {
        // Rough estimate: serialized size is typically close to memory size
        // with some overhead for the container structures
        data.len() + 64 // Base overhead for container management
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_parse_header_standard_format() {
            // Standard format: cookie = 12346, 2 containers
            let mut data = vec![];
            data.extend_from_slice(&12346u32.to_le_bytes()); // cookie
            data.extend_from_slice(&2u32.to_le_bytes()); // container count

            let header = parse_header(&data).unwrap();
            assert_eq!(header.container_count, 2);
        }

        #[test]
        fn test_parse_header_run_format() {
            // Run format: cookie = 12347, 5 containers
            let mut data = vec![];
            data.extend_from_slice(&12347u32.to_le_bytes()); // cookie
            data.extend_from_slice(&5u32.to_le_bytes()); // container count

            let header = parse_header(&data).unwrap();
            assert_eq!(header.container_count, 5);
        }

        #[test]
        fn test_parse_header_invalid_cookie() {
            let mut data = vec![];
            data.extend_from_slice(&99999u32.to_le_bytes()); // invalid cookie
            data.extend_from_slice(&1u32.to_le_bytes());

            assert!(parse_header(&data).is_none());
        }

        #[test]
        fn test_parse_header_too_short() {
            let data = [0u8; 4]; // Only 4 bytes, need 8
            assert!(parse_header(&data).is_none());
        }

        #[test]
        fn test_estimate_memory_size() {
            let data = [0u8; 100];
            let estimate = estimate_memory_size(&data);
            assert_eq!(estimate, 164); // 100 + 64 overhead
        }
    }
}

/// H3 extension helpers for Uber's H3 geospatial indexing.
///
/// H3 is a hexagonal hierarchical spatial index. H3 indexes are 64-bit integers
/// that encode a cell at a specific resolution (0-15).
pub mod h3 {
    /// H3 cell index.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct H3Index(pub u64);

    impl H3Index {
        /// Create a new H3 index from a u64.
        pub fn new(index: u64) -> Self {
            Self(index)
        }

        /// Get the resolution (0-15) of this cell.
        pub fn resolution(&self) -> u8 {
            ((self.0 >> 52) & 0x0F) as u8
        }

        /// Get the base cell number (0-121).
        pub fn base_cell(&self) -> u8 {
            ((self.0 >> 45) & 0x7F) as u8
        }

        /// Check if this is a valid H3 index.
        pub fn is_valid(&self) -> bool {
            // Mode must be 1 (hexagon) in bits 59-62
            let mode = (self.0 >> 59) & 0x0F;
            mode == 1 && self.resolution() <= 15 && self.base_cell() <= 121
        }

        /// Check if this cell is a pentagon.
        pub fn is_pentagon(&self) -> bool {
            // Base cells 4, 14, 24, 38, 49, 58, 63, 72, 83, 97, 107, 117 are pentagons
            const PENTAGON_BASE_CELLS: [u8; 12] = [4, 14, 24, 38, 49, 58, 63, 72, 83, 97, 107, 117];
            PENTAGON_BASE_CELLS.contains(&self.base_cell())
        }

        /// Get the parent cell at a coarser resolution.
        pub fn parent(&self, parent_res: u8) -> Option<H3Index> {
            let res = self.resolution();
            if parent_res >= res {
                return None;
            }

            // Clear the digits for resolutions finer than parent_res
            let mut index = self.0;

            // Set the new resolution
            index = (index & !(0x0F << 52)) | ((parent_res as u64) << 52);

            // Clear child digits (set to 7 = center)
            for r in (parent_res + 1)..=res {
                let shift = (15 - r) * 3;
                index = (index & !(0x07 << shift)) | (0x07 << shift);
            }

            Some(H3Index(index))
        }
    }

    /// Parse an H3 index from binary (8 bytes, big-endian).
    pub fn parse_h3_binary(data: &[u8]) -> Option<H3Index> {
        if data.len() < 8 {
            return None;
        }
        let index = u64::from_be_bytes([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]]);
        Some(H3Index(index))
    }

    /// Encode an H3 index to binary (8 bytes, big-endian).
    pub fn encode_h3_binary(index: H3Index) -> [u8; 8] {
        index.0.to_be_bytes()
    }

    /// Parse an H3 index from text (hex string).
    pub fn parse_h3_text(text: &str) -> Option<H3Index> {
        let text = text.trim().trim_start_matches("0x").trim_start_matches("0X");
        let index = u64::from_str_radix(text, 16).ok()?;
        Some(H3Index(index))
    }

    /// Format an H3 index to text (hex string).
    pub fn format_h3_text(index: H3Index) -> String {
        format!("{:016x}", index.0)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_h3_resolution() {
            // Resolution 9 cell
            let h3 = H3Index(0x8928308280fffff);
            assert_eq!(h3.resolution(), 9);
        }

        #[test]
        fn test_h3_parse_text() {
            let h3 = parse_h3_text("8928308280fffff").unwrap();
            assert_eq!(format_h3_text(h3), "08928308280fffff");
        }

        #[test]
        fn test_h3_binary_roundtrip() {
            let original = H3Index(0x8928308280fffff);
            let encoded = encode_h3_binary(original);
            let decoded = parse_h3_binary(&encoded).unwrap();
            assert_eq!(original, decoded);
        }

        #[test]
        fn test_h3_binary_too_short() {
            assert!(parse_h3_binary(&[0; 7]).is_none());
            assert!(parse_h3_binary(&[]).is_none());
        }

        #[test]
        fn test_h3_is_valid() {
            // Valid hexagon cell
            let h3 = H3Index(0x8928308280fffff);
            assert!(h3.is_valid());

            // Invalid: resolution > 15
            let invalid = H3Index(0x0); // Mode bits are 0, not 1
            assert!(!invalid.is_valid());
        }

        #[test]
        fn test_h3_base_cell() {
            let h3 = H3Index(0x8928308280fffff);
            let base = h3.base_cell();
            assert!(base <= 121);
        }

        #[test]
        #[allow(clippy::identity_op)]
        fn test_h3_is_pentagon() {
            // Pentagon base cells: 4, 14, 24, 38, 49, 58, 63, 72, 83, 97, 107, 117
            // Create an H3 index with base cell 4 at resolution 0
            // Mode=1 (hex) at bits 59-62 = 0x1, res=0 at bits 52-55
            // Base cell=4 at bits 45-51
            let pentagon_index = (1u64 << 59) | (4u64 << 45);
            let h3 = H3Index(pentagon_index);
            assert!(h3.is_pentagon());

            // Non-pentagon base cell (e.g., 0)
            let non_pentagon = (1u64 << 59) | (0u64 << 45);
            let h3_non = H3Index(non_pentagon);
            assert!(!h3_non.is_pentagon());
        }

        #[test]
        fn test_h3_parent() {
            // Create a resolution 5 cell and get its parent at resolution 3
            let h3 = H3Index(0x8528308280fffff);
            let res = h3.resolution();

            if res > 0 {
                let parent = h3.parent(res - 1);
                assert!(parent.is_some());
                let p = parent.unwrap();
                assert_eq!(p.resolution(), res - 1);
            }
        }

        #[test]
        fn test_h3_parent_invalid() {
            let h3 = H3Index(0x8928308280fffff);
            let res = h3.resolution();

            // Can't get parent at same or higher resolution
            assert!(h3.parent(res).is_none());
            assert!(h3.parent(res + 1).is_none());
        }

        #[test]
        fn test_h3_parse_with_prefix() {
            let h3 = parse_h3_text("0x8928308280fffff").unwrap();
            assert_eq!(h3.resolution(), 9);

            let h3 = parse_h3_text("0X8928308280fffff").unwrap();
            assert_eq!(h3.resolution(), 9);
        }
    }
}

/// MobilityDB extension helpers for temporal/trajectory data.
///
/// MobilityDB extends PostgreSQL with temporal types for moving objects.
pub mod mobilitydb {
    /// Temporal instant (value at a specific time).
    #[derive(Clone, Debug, PartialEq)]
    pub struct TInstant<T> {
        pub value: T,
        pub timestamp: i64, // microseconds since epoch
    }

    /// Temporal sequence (continuous values over a time period).
    #[derive(Clone, Debug, PartialEq)]
    pub struct TSequence<T> {
        pub instants: Vec<TInstant<T>>,
        pub lower_inc: bool,
        pub upper_inc: bool,
    }

    /// Integer span.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct IntSpan {
        pub lower: i32,
        pub upper: i32,
        pub lower_inc: bool,
        pub upper_inc: bool,
    }

    impl IntSpan {
        /// Create a new integer span.
        pub fn new(lower: i32, upper: i32, lower_inc: bool, upper_inc: bool) -> Self {
            Self { lower, upper, lower_inc, upper_inc }
        }

        /// Check if a value is contained in the span.
        pub fn contains(&self, value: i32) -> bool {
            let lower_ok = if self.lower_inc { value >= self.lower } else { value > self.lower };
            let upper_ok = if self.upper_inc { value <= self.upper } else { value < self.upper };
            lower_ok && upper_ok
        }

        /// Get the width of the span.
        pub fn width(&self) -> i32 {
            self.upper - self.lower
        }
    }

    /// Float span.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct FloatSpan {
        pub lower: f64,
        pub upper: f64,
        pub lower_inc: bool,
        pub upper_inc: bool,
    }

    impl FloatSpan {
        /// Create a new float span.
        pub fn new(lower: f64, upper: f64, lower_inc: bool, upper_inc: bool) -> Self {
            Self { lower, upper, lower_inc, upper_inc }
        }

        /// Check if a value is contained in the span.
        pub fn contains(&self, value: f64) -> bool {
            let lower_ok = if self.lower_inc { value >= self.lower } else { value > self.lower };
            let upper_ok = if self.upper_inc { value <= self.upper } else { value < self.upper };
            lower_ok && upper_ok
        }
    }

    /// Spatiotemporal box (STBox).
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct STBox {
        pub xmin: Option<f64>,
        pub xmax: Option<f64>,
        pub ymin: Option<f64>,
        pub ymax: Option<f64>,
        pub zmin: Option<f64>,
        pub zmax: Option<f64>,
        pub tmin: Option<i64>,
        pub tmax: Option<i64>,
        pub srid: Option<i32>,
    }

    impl STBox {
        /// Create an empty STBox.
        pub fn empty() -> Self {
            Self {
                xmin: None,
                xmax: None,
                ymin: None,
                ymax: None,
                zmin: None,
                zmax: None,
                tmin: None,
                tmax: None,
                srid: None,
            }
        }

        /// Create a 2D spatial box.
        pub fn from_xy(xmin: f64, ymin: f64, xmax: f64, ymax: f64) -> Self {
            Self {
                xmin: Some(xmin),
                xmax: Some(xmax),
                ymin: Some(ymin),
                ymax: Some(ymax),
                zmin: None,
                zmax: None,
                tmin: None,
                tmax: None,
                srid: None,
            }
        }

        /// Check if this box has spatial dimensions.
        pub fn has_space(&self) -> bool {
            self.xmin.is_some()
        }

        /// Check if this box has temporal dimension.
        pub fn has_time(&self) -> bool {
            self.tmin.is_some()
        }

        /// Check if this box has Z dimension.
        pub fn has_z(&self) -> bool {
            self.zmin.is_some()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_intspan_contains() {
            let span = IntSpan::new(0, 10, true, false);
            assert!(span.contains(0));
            assert!(span.contains(5));
            assert!(!span.contains(10));
            assert!(!span.contains(-1));
        }

        #[test]
        fn test_stbox_creation() {
            let box_ = STBox::from_xy(0.0, 0.0, 10.0, 10.0);
            assert!(box_.has_space());
            assert!(!box_.has_time());
            assert!(!box_.has_z());
        }

        #[test]
        fn test_floatspan_contains() {
            let span = FloatSpan::new(0.0, 10.0, true, false);
            assert!(span.contains(0.0)); // Lower inclusive
            assert!(span.contains(5.0));
            assert!(!span.contains(10.0)); // Upper exclusive
            assert!(!span.contains(-1.0));
        }

        #[test]
        fn test_floatspan_exclusive_bounds() {
            let span = FloatSpan::new(0.0, 10.0, false, false);
            assert!(!span.contains(0.0)); // Lower exclusive
            assert!(!span.contains(10.0)); // Upper exclusive
            assert!(span.contains(5.0));
        }

        #[test]
        fn test_intspan_width() {
            let span = IntSpan::new(0, 10, true, true);
            assert_eq!(span.width(), 10);
        }

        #[test]
        fn test_tinstant() {
            let instant: TInstant<f64> = TInstant {
                value: 42.0,
                timestamp: 1704067200000000, // 2024-01-01 00:00:00 UTC in microseconds
            };
            assert_eq!(instant.value, 42.0);
        }

        #[test]
        fn test_tsequence() {
            let seq: TSequence<i32> = TSequence {
                instants: vec![TInstant { value: 1, timestamp: 1000000 }, TInstant { value: 2, timestamp: 2000000 }],
                lower_inc: true,
                upper_inc: false,
            };
            assert_eq!(seq.instants.len(), 2);
            assert!(seq.lower_inc);
            assert!(!seq.upper_inc);
        }

        #[test]
        fn test_stbox_empty() {
            let box_ = STBox::empty();
            assert!(!box_.has_space());
            assert!(!box_.has_time());
            assert!(!box_.has_z());
            assert!(box_.srid.is_none());
        }

        #[test]
        fn test_stbox_with_z() {
            let mut box_ = STBox::from_xy(0.0, 0.0, 10.0, 10.0);
            box_.zmin = Some(0.0);
            box_.zmax = Some(5.0);
            assert!(box_.has_z());
        }

        #[test]
        fn test_stbox_with_time() {
            let mut box_ = STBox::empty();
            box_.tmin = Some(1000000);
            box_.tmax = Some(2000000);
            assert!(box_.has_time());
            assert!(!box_.has_space());
        }
    }
}

/// pgmp extension helpers for multi-precision arithmetic.
///
/// pgmp provides arbitrary precision integer (mpz), rational (mpq),
/// and floating-point (mpf) types using GMP library.
pub mod pgmp {
    /// Multi-precision integer representation.
    ///
    /// For actual arbitrary-precision arithmetic, use the `num-bigint` crate.
    /// This module provides helpers for the wire protocol format.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct Mpz {
        /// Sign: 1 for positive, -1 for negative, 0 for zero.
        pub sign: i8,
        /// Magnitude as big-endian bytes.
        pub magnitude: Vec<u8>,
    }

    impl Mpz {
        /// Create zero.
        pub fn zero() -> Self {
            Self { sign: 0, magnitude: vec![] }
        }

        /// Create from i64.
        pub fn from_i64(value: i64) -> Self {
            if value == 0 {
                return Self::zero();
            }

            let sign = if value > 0 { 1 } else { -1 };
            let abs_value = value.unsigned_abs();

            // Convert to big-endian bytes, stripping leading zeros
            let bytes = abs_value.to_be_bytes();
            let magnitude: Vec<u8> = bytes.iter().skip_while(|&&b| b == 0).copied().collect();

            Self { sign, magnitude }
        }

        /// Try to convert to i64 (returns None if too large).
        pub fn to_i64(&self) -> Option<i64> {
            if self.sign == 0 {
                return Some(0);
            }

            if self.magnitude.len() > 8 {
                return None;
            }

            let mut bytes = [0u8; 8];
            let offset = 8 - self.magnitude.len();
            bytes[offset..].copy_from_slice(&self.magnitude);

            let abs_value = u64::from_be_bytes(bytes);

            if self.sign > 0 {
                if abs_value <= i64::MAX as u64 {
                    Some(abs_value as i64)
                } else {
                    None
                }
            } else {
                // Handle i64::MIN specially to avoid overflow
                // i64::MIN.unsigned_abs() == 9223372036854775808
                let min_abs = (i64::MIN as u64).wrapping_neg();
                if abs_value == min_abs {
                    Some(i64::MIN)
                } else if abs_value < min_abs {
                    Some(-(abs_value as i64))
                } else {
                    None
                }
            }
        }

        /// Check if zero.
        pub fn is_zero(&self) -> bool {
            self.sign == 0
        }

        /// Check if negative.
        pub fn is_negative(&self) -> bool {
            self.sign < 0
        }
    }

    /// Multi-precision rational representation (numerator / denominator).
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct Mpq {
        pub numerator: Mpz,
        pub denominator: Mpz,
    }

    impl Mpq {
        /// Create a new rational.
        pub fn new(numerator: Mpz, denominator: Mpz) -> Self {
            Self { numerator, denominator }
        }

        /// Create from two i64 values.
        pub fn from_i64(num: i64, den: i64) -> Self {
            Self {
                numerator: Mpz::from_i64(num),
                denominator: Mpz::from_i64(den),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_mpz_roundtrip() {
            for value in [0i64, 1, -1, 100, -100, i64::MAX, i64::MIN] {
                let mpz = Mpz::from_i64(value);
                assert_eq!(mpz.to_i64(), Some(value));
            }
        }

        #[test]
        fn test_mpz_zero() {
            let mpz = Mpz::zero();
            assert!(mpz.is_zero());
            assert_eq!(mpz.to_i64(), Some(0));
        }

        #[test]
        fn test_mpz_is_negative() {
            let positive = Mpz::from_i64(42);
            assert!(!positive.is_negative());

            let negative = Mpz::from_i64(-42);
            assert!(negative.is_negative());

            let zero = Mpz::zero();
            assert!(!zero.is_negative());
        }

        #[test]
        fn test_mpz_large_value() {
            // Test a value larger than i64::MAX
            let mpz = Mpz {
                sign: 1,
                magnitude: vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01],
            };
            assert!(mpz.to_i64().is_none());
        }

        #[test]
        fn test_mpq_creation() {
            let q = Mpq::from_i64(1, 2); // 1/2
            assert_eq!(q.numerator.to_i64(), Some(1));
            assert_eq!(q.denominator.to_i64(), Some(2));
        }

        #[test]
        fn test_mpq_new() {
            let num = Mpz::from_i64(3);
            let den = Mpz::from_i64(4);
            let q = Mpq::new(num.clone(), den.clone());
            assert_eq!(q.numerator, num);
            assert_eq!(q.denominator, den);
        }

        #[test]
        fn test_mpz_magnitude_stripping() {
            // Verify leading zeros are stripped
            let mpz = Mpz::from_i64(256); // 0x100
            assert_eq!(mpz.magnitude, vec![0x01, 0x00]);
        }
    }
}

/// ULID (Universally Unique Lexicographically Sortable Identifier) helpers.
///
/// ULIDs are 128-bit identifiers that encode a timestamp and random component.
pub mod ulid {
    /// ULID value.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct Ulid {
        /// High 64 bits (timestamp in high 48 bits, random in low 16 bits).
        pub high: u64,
        /// Low 64 bits (random).
        pub low: u64,
    }

    impl Ulid {
        /// Create a new ULID from high and low parts.
        pub fn new(high: u64, low: u64) -> Self {
            Self { high, low }
        }

        /// Get the timestamp in milliseconds since Unix epoch.
        pub fn timestamp_ms(&self) -> u64 {
            self.high >> 16
        }

        /// Create from 16 bytes.
        pub fn from_bytes(bytes: [u8; 16]) -> Self {
            let high = u64::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]);
            let low = u64::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]]);
            Self { high, low }
        }

        /// Convert to 16 bytes.
        pub fn to_bytes(&self) -> [u8; 16] {
            let mut bytes = [0u8; 16];
            bytes[0..8].copy_from_slice(&self.high.to_be_bytes());
            bytes[8..16].copy_from_slice(&self.low.to_be_bytes());
            bytes
        }
    }

    /// Crockford Base32 alphabet for ULID encoding.
    const CROCKFORD_ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

    /// Format a ULID as a 26-character string.
    pub fn format_ulid(ulid: &Ulid) -> String {
        let mut result = String::with_capacity(26);
        let bits: u128 = ((ulid.high as u128) << 64) | (ulid.low as u128);

        // ULID uses 128 bits encoded as 26 base32 chars
        // 26 * 5 = 130 bits, so the first character only uses 3 bits (top 2 are always 0)

        // First character: bits 127-125 (3 bits, max value 7)
        let idx = ((bits >> 125) & 0x07) as usize;
        result.push(CROCKFORD_ALPHABET[idx] as char);

        // Remaining 25 characters: 5 bits each starting from bit 124
        for i in 0..25 {
            let shift = 120 - (i * 5); // 120, 115, 110, ..., 0
            let idx = ((bits >> shift) & 0x1F) as usize;
            result.push(CROCKFORD_ALPHABET[idx] as char);
        }

        result
    }

    /// Parse a ULID from a 26-character string.
    pub fn parse_ulid(text: &str) -> Option<Ulid> {
        let text = text.trim().to_uppercase();
        if text.len() != 26 {
            return None;
        }

        let mut bits: u128 = 0;

        for (i, c) in text.chars().enumerate() {
            let idx = match c {
                '0'..='9' => (c as u8 - b'0') as u128,
                'A'..='H' => (c as u8 - b'A' + 10) as u128,
                'J'..='K' => (c as u8 - b'J' + 18) as u128,
                'M'..='N' => (c as u8 - b'M' + 20) as u128,
                'P'..='T' => (c as u8 - b'P' + 22) as u128,
                'V'..='Z' => (c as u8 - b'V' + 27) as u128,
                _ => return None,
            };

            if i == 0 {
                // First character only contributes 3 bits (must be 0-7)
                if idx > 7 {
                    return None;
                }
                bits = idx;
            } else {
                bits = (bits << 5) | idx;
            }
        }

        let high = (bits >> 64) as u64;
        let low = bits as u64;
        Some(Ulid::new(high, low))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_ulid_bytes_roundtrip() {
            let bytes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
            let ulid = Ulid::from_bytes(bytes);
            assert_eq!(ulid.to_bytes(), bytes);
        }

        #[test]
        fn test_ulid_timestamp() {
            // A ULID with known timestamp
            let ulid = Ulid::new(0x0001_8A6E_D1C0_0000, 0);
            assert!(ulid.timestamp_ms() > 0);
        }

        #[test]
        fn test_ulid_format_parse_roundtrip() {
            let original = Ulid::new(0x0001_8A6E_D1C0_1234, 0xABCD_EF01_2345_6789);
            let formatted = format_ulid(&original);
            let parsed = parse_ulid(&formatted).unwrap();
            assert_eq!(original, parsed);
        }

        #[test]
        fn test_ulid_parse_lowercase() {
            // parse_ulid should handle lowercase (using a valid ULID)
            let ulid = parse_ulid("01arj9g5bj0000000000000000");
            assert!(ulid.is_some());
        }

        #[test]
        fn test_ulid_parse_invalid_length() {
            assert!(parse_ulid("01ARJ9G5BJ").is_none()); // Too short
            assert!(parse_ulid("01ARJ9G5BJ00000000000000000000").is_none()); // Too long
        }

        #[test]
        fn test_ulid_parse_invalid_chars() {
            // I, L, O, U are not valid in Crockford Base32
            assert!(parse_ulid("0IARJ9G5BJ0000000000000000").is_none());
            assert!(parse_ulid("0LARJ9G5BJ0000000000000000").is_none());
            assert!(parse_ulid("0OARJ9G5BJ0000000000000000").is_none());
            assert!(parse_ulid("0UARJ9G5BJ0000000000000000").is_none());
        }

        #[test]
        fn test_ulid_all_zeros() {
            let ulid = Ulid::new(0, 0);
            let formatted = format_ulid(&ulid);
            assert_eq!(formatted, "00000000000000000000000000");
            let parsed = parse_ulid(&formatted).unwrap();
            assert_eq!(ulid, parsed);
        }

        #[test]
        fn test_ulid_max_valid_value() {
            // Maximum valid 128-bit ULID: all bits set to 1
            // bits127-125 = 111 = 7, so first char is '7'
            let ulid = Ulid::new(u64::MAX, u64::MAX);
            let formatted = format_ulid(&ulid);
            let parsed = parse_ulid(&formatted).unwrap();
            assert_eq!(ulid, parsed);
            // First char should be '7' (index 7 in Crockford Base32)
            assert!(formatted.starts_with('7'));
            // All remaining chars should be 'Z' (index 31)
            assert!(formatted.chars().skip(1).all(|c| c == 'Z'));
        }

        #[test]
        fn test_ulid_first_char_overflow() {
            // If we somehow create a ULID where first char would be > 7,
            // the formatted version will still parse back to a different value
            // since those high bits get truncated. This tests that parse rejects
            // invalid first characters.
            assert!(parse_ulid("8ARJJ9G5BJ0000000000000000").is_none());
            assert!(parse_ulid("ZARJJ9G5BJ0000000000000000").is_none());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_names_exist() {
        let type_names = [
            // Just verify the constants compile
            type_name::GEOMETRY,
            type_name::VECTOR,
            type_name::HSTORE,
            type_name::LTREE,
            type_name::CITEXT,
            // New extension types
            type_name::IP4,
            type_name::IP6,
            type_name::SEMVER,
            type_name::AGTYPE,
            type_name::ROARINGBITMAP,
            type_name::SPOINT,
            type_name::UNIT,
            // Additional extension types
            type_name::H3INDEX,
            type_name::TGEOMPOINT,
            type_name::MPZ,
            type_name::ULID,
            type_name::BM25,
        ];

        assert!(type_names.iter().all(|name| !name.is_empty()));
    }
}
