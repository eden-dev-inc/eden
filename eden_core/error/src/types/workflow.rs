use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Workflow execution errors (0x14XX error codes).
///
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum WorkflowError {
    WorkflowNotFound,  // 0x01
    ExecutionFailed,   // 0x02
    InvalidDefinition, // 0x03
    StepFailed,        // 0x04
    TimeoutExceeded,   // 0x05
    NoInputsProvided,  // 0x06 - "No inputs provided" (1x)
    CycleDetected,     // 0x07 - "Cycle detected in DAG" (1x)
    ChannelSendError,  // 0x08 - "Channel send error" (1x)
    Custom(String),    // 0xFF - For backward compatibility with string errors
}

impl WorkflowError {
    /// Returns the specific error code (0x01-0xFF) for this workflow error.
    pub fn error_code(&self) -> u8 {
        match self {
            WorkflowError::WorkflowNotFound => 0x01,
            WorkflowError::ExecutionFailed => 0x02,
            WorkflowError::InvalidDefinition => 0x03,
            WorkflowError::StepFailed => 0x04,
            WorkflowError::TimeoutExceeded => 0x05,
            WorkflowError::NoInputsProvided => 0x06,
            WorkflowError::CycleDetected => 0x07,
            WorkflowError::ChannelSendError => 0x08,
            WorkflowError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            WorkflowError::WorkflowNotFound => "Workflow not found. Please verify the workflow ID is correct",
            WorkflowError::ExecutionFailed => "Workflow execution failed",
            WorkflowError::InvalidDefinition => "Workflow definition is invalid or corrupted",
            WorkflowError::StepFailed => "Workflow step failed to execute",
            WorkflowError::TimeoutExceeded => "Workflow execution timeout exceeded",
            WorkflowError::NoInputsProvided => "No inputs provided",
            WorkflowError::CycleDetected => "Cycle detected in DAG",
            WorkflowError::ChannelSendError => "Channel send error",
            WorkflowError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
