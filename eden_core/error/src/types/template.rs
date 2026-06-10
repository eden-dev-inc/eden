use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum TemplateError {
    TemplateNotFound,  // 0x01
    CompilationFailed, // 0x02
    RenderingFailed,   // 0x03
    InvalidSyntax,     // 0x04
    VariableMissing,   // 0x05
    Custom(String),    // 0xFF - For backward compatibility with string errors
}

impl TemplateError {
    pub fn error_code(&self) -> u8 {
        match self {
            TemplateError::TemplateNotFound => 0x01,
            TemplateError::CompilationFailed => 0x02,
            TemplateError::RenderingFailed => 0x03,
            TemplateError::InvalidSyntax => 0x04,
            TemplateError::VariableMissing => 0x05,
            TemplateError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for TemplateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            TemplateError::TemplateNotFound => "Template not found. Please verify the template ID is correct",
            TemplateError::CompilationFailed => "Template compilation failed. Please check template syntax",
            TemplateError::RenderingFailed => "Template rendering failed. Please check template variables",
            TemplateError::InvalidSyntax => "Invalid template syntax detected",
            TemplateError::VariableMissing => "Required template variable is missing",
            TemplateError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
