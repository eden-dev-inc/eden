//! TDS RPC (Remote Procedure Call) packet types.
//!
//! RPC packets are used to call stored procedures with parameters.

use crate::error::{SybaseWireError, data_types};
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use crate::types::packet::PacketType;
use crate::write::{PacketBuilder, write_u16_le, write_varchar};
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

type ParsedValue = (Option<u16>, Option<u8>, Option<u8>, Option<Vec<u8>>);

/// RPC option flags.
pub mod rpc_options {
    /// Recompile the procedure before execution.
    pub const RECOMPILE: u16 = 0x0001;
    /// No metadata in the result set.
    pub const NO_METADATA: u16 = 0x0002;
}

/// Parameter status flags.
pub mod param_status {
    /// Parameter is an output parameter.
    pub const OUTPUT: u8 = 0x01;
    /// Parameter can be null.
    pub const NULLABLE: u8 = 0x02;
    /// Parameter has a default value.
    pub const DEFAULT: u8 = 0x04;
}

/// A single RPC parameter.
#[derive(Clone, Debug)]
pub struct RpcParameter {
    /// Parameter name (optional, can be empty for positional params).
    pub name: String,
    /// Status flags.
    pub status: u8,
    /// Data type.
    pub data_type: u8,
    /// Maximum length (for variable-length types).
    pub max_length: Option<u16>,
    /// Precision (for numeric types).
    pub precision: Option<u8>,
    /// Scale (for numeric types).
    pub scale: Option<u8>,
    /// Parameter value (raw bytes).
    pub value: Option<Vec<u8>>,
}

impl RpcParameter {
    /// Create a new input parameter.
    pub fn input(name: impl Into<String>, data_type: u8, value: Vec<u8>) -> Self {
        Self {
            name: name.into(),
            status: 0,
            data_type,
            max_length: None,
            precision: None,
            scale: None,
            value: Some(value),
        }
    }

    /// Create a new output parameter.
    pub fn output(name: impl Into<String>, data_type: u8) -> Self {
        Self {
            name: name.into(),
            status: param_status::OUTPUT,
            data_type,
            max_length: None,
            precision: None,
            scale: None,
            value: None,
        }
    }

    /// Create a null parameter.
    pub fn null(name: impl Into<String>, data_type: u8) -> Self {
        Self {
            name: name.into(),
            status: param_status::NULLABLE,
            data_type,
            max_length: None,
            precision: None,
            scale: None,
            value: None,
        }
    }

    /// Set the maximum length.
    pub fn with_max_length(mut self, len: u16) -> Self {
        self.max_length = Some(len);
        self
    }

    /// Set precision and scale for numeric types.
    pub fn with_precision_scale(mut self, precision: u8, scale: u8) -> Self {
        self.precision = Some(precision);
        self.scale = Some(scale);
        self
    }

    /// Check if this is an output parameter.
    pub fn is_output(&self) -> bool {
        self.status & param_status::OUTPUT != 0
    }

    /// Check if this parameter is null.
    pub fn is_null(&self) -> bool {
        self.value.is_none()
    }
}

/// RPC request packet.
#[derive(Clone, Debug)]
pub struct Rpc {
    /// Procedure name.
    pub procedure_name: String,
    /// Option flags.
    pub options: u16,
    /// Parameters.
    pub parameters: Vec<RpcParameter>,
}

impl Rpc {
    /// Create a new RPC request.
    pub fn new(procedure_name: impl Into<String>) -> Self {
        Self {
            procedure_name: procedure_name.into(),
            options: 0,
            parameters: Vec::new(),
        }
    }

    /// Add a parameter.
    pub fn with_parameter(mut self, param: RpcParameter) -> Self {
        self.parameters.push(param);
        self
    }

    /// Set recompile option.
    pub fn with_recompile(mut self) -> Self {
        self.options |= rpc_options::RECOMPILE;
        self
    }

    /// Parse an RPC packet from a stream.
    pub fn parse_sync<'s>(stream: &'s SliceStream<'s>) -> Result<Rpc, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Procedure name length and value
        let name_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let procedure_name = if name_len > 0 {
            let borrow = stream.peek(Some(name_len)).map_err(SybaseParseError::Stream)?;
            let name = String::from_utf8_lossy(&borrow[..name_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            name
        } else {
            String::new()
        };

        // Options (2 bytes)
        let options = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Parse parameters until end of stream
        let mut parameters = Vec::new();
        while let Ok(borrow) = stream.peek(Some(1)) {
            // Try to read parameter name length
            let param_name_len = borrow[0] as usize;
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;

            // Parameter name
            let param_name = if param_name_len > 0 {
                let borrow = stream.peek(Some(param_name_len)).map_err(SybaseParseError::Stream)?;
                let name = String::from_utf8_lossy(&borrow[..param_name_len]).into_owned();
                stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                name
            } else {
                String::new()
            };

            // Status byte
            let status = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

            // Data type
            let data_type = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

            // Parse type-specific metadata and value
            let (max_length, precision, scale, value) = Self::parse_param_value(stream, data_type)?;

            parameters.push(RpcParameter {
                name: param_name,
                status,
                data_type,
                max_length,
                precision,
                scale,
                value,
            });
        }

        Ok(Rpc { procedure_name, options, parameters })
    }

    /// Parse parameter value based on data type.
    fn parse_param_value<'s>(
        stream: &'s SliceStream<'s>,
        data_type: u8,
    ) -> Result<ParsedValue, SybaseParseError<SliceReadError, SybaseWireError>> {
        match data_type {
            // Fixed-length types
            data_types::INT1TYPE => {
                let v = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                Ok((None, None, None, Some(vec![v])))
            }
            data_types::INT2TYPE => {
                let v = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;
                Ok((None, None, None, Some(v.to_le_bytes().to_vec())))
            }
            data_types::INT4TYPE => {
                let v = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;
                Ok((None, None, None, Some(v.to_le_bytes().to_vec())))
            }

            // Variable-length types
            data_types::INTNTYPE => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                let value = if len > 0 {
                    let borrow = stream.peek(Some(len as usize)).map_err(SybaseParseError::Stream)?;
                    let data = borrow[..len as usize].to_vec();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Some(data)
                } else {
                    None
                };
                Ok((Some(len as u16), None, None, value))
            }

            data_types::VARCHARTYPE | data_types::CHARTYPE => {
                let max_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as u16;
                let actual_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
                let value = if actual_len > 0 {
                    let borrow = stream.peek(Some(actual_len)).map_err(SybaseParseError::Stream)?;
                    let data = borrow[..actual_len].to_vec();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Some(data)
                } else {
                    None
                };
                Ok((Some(max_len), None, None, value))
            }

            // Default: read as binary with length prefix
            _ => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
                let value = if len > 0 && len != 255 {
                    let borrow = stream.peek(Some(len)).map_err(SybaseParseError::Stream)?;
                    let data = borrow[..len].to_vec();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Some(data)
                } else {
                    None
                };
                Ok((None, None, None, value))
            }
        }
    }
}

/// Builder for RPC packets.
pub struct RpcBuilder {
    procedure_name: String,
    options: u16,
    params: Vec<u8>,
}

impl RpcBuilder {
    /// Create a new RPC builder.
    pub fn new(procedure_name: impl Into<String>) -> Self {
        Self {
            procedure_name: procedure_name.into(),
            options: 0,
            params: Vec::new(),
        }
    }

    /// Set recompile option.
    pub fn recompile(mut self) -> Self {
        self.options |= rpc_options::RECOMPILE;
        self
    }

    /// Add an integer parameter.
    pub fn add_int(mut self, name: &str, value: i32) -> Self {
        // Name
        write_varchar(&mut self.params, name.as_bytes());
        // Status (input)
        self.params.push(0);
        // Type
        self.params.push(data_types::INT4TYPE);
        // Value
        self.params.extend_from_slice(&value.to_le_bytes());
        self
    }

    /// Add a string parameter.
    pub fn add_string(mut self, name: &str, value: &str, max_len: u8) -> Self {
        // Name
        write_varchar(&mut self.params, name.as_bytes());
        // Status (input)
        self.params.push(0);
        // Type
        self.params.push(data_types::VARCHARTYPE);
        // Max length
        self.params.push(max_len);
        // Actual length and value
        let bytes = value.as_bytes();
        self.params.push(bytes.len().min(255) as u8);
        self.params.extend_from_slice(&bytes[..bytes.len().min(max_len as usize)]);
        self
    }

    /// Add a null parameter.
    pub fn add_null(mut self, name: &str, data_type: u8) -> Self {
        // Name
        write_varchar(&mut self.params, name.as_bytes());
        // Status (nullable)
        self.params.push(param_status::NULLABLE);
        // Type
        self.params.push(data_type);
        // Null indicator (0 length for most types)
        self.params.push(0);
        self
    }

    /// Add an output parameter.
    pub fn add_output(mut self, name: &str, data_type: u8, max_len: u8) -> Self {
        // Name
        write_varchar(&mut self.params, name.as_bytes());
        // Status (output)
        self.params.push(param_status::OUTPUT);
        // Type
        self.params.push(data_type);
        // Max length
        self.params.push(max_len);
        // No value for output params
        self.params.push(0);
        self
    }

    /// Build the RPC packet.
    pub fn build(self) -> Vec<u8> {
        let mut data = Vec::new();

        // Procedure name
        write_varchar(&mut data, self.procedure_name.as_bytes());

        // Options
        write_u16_le(&mut data, self.options);

        // Parameters
        data.extend(self.params);

        PacketBuilder::new(PacketType::Rpc).write_bytes(&data).build()
    }
}
