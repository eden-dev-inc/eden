use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};
use eden_logger_internal::{LogAudience, ctx_with_trace, log_error, log_info, log_warn};
use function_name::named;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ParsingErrors {
    pub critical_errors: Vec<String>,
    pub warning_errors: Vec<String>,
    pub info_messages: Vec<String>,
    pub section_errors: HashMap<String, Vec<String>>,
}

impl ParsingErrors {
    pub fn add_critical(&mut self, error: String) {
        self.critical_errors.push(error);
    }

    pub fn add_warning(&mut self, warning: String) {
        self.warning_errors.push(warning);
    }

    pub fn add_info(&mut self, info: String) {
        self.info_messages.push(info);
    }

    pub fn add_section_error(&mut self, section: String, error: String) {
        self.section_errors.entry(section).or_default().push(error);
    }

    pub fn has_critical_errors(&self) -> bool {
        !self.critical_errors.is_empty()
    }

    pub fn total_errors(&self) -> usize {
        self.critical_errors.len() + self.warning_errors.len() + self.section_errors.values().map(|v| v.len()).sum::<usize>()
    }

    #[named]
    pub fn log_all(&self) {
        let ctx = ctx_with_trace!();
        if !self.critical_errors.is_empty() {
            log_error!(
                ctx.clone(),
                format!("Critical parsing errors ({}):", self.critical_errors.len()),
                audience = LogAudience::Internal
            );
            for error in &self.critical_errors {
                log_error!(ctx.clone(), format!("  {}", error), audience = LogAudience::Internal);
            }
        }

        if !self.warning_errors.is_empty() {
            log_warn!(
                ctx.clone(),
                format!("Parsing warnings ({}):", self.warning_errors.len()),
                audience = LogAudience::Internal
            );
            for warning in &self.warning_errors {
                log_warn!(ctx.clone(), format!("  {}", warning), audience = LogAudience::Internal);
            }
        }

        for (section, errors) in &self.section_errors {
            if !errors.is_empty() {
                log_warn!(
                    ctx.clone(),
                    format!("Section '{}' parsing errors ({}):", section, errors.len()),
                    audience = LogAudience::Internal
                );
                for error in errors {
                    log_warn!(ctx.clone(), format!("  {}", error), audience = LogAudience::Internal);
                }
            }
        }

        if !self.info_messages.is_empty() {
            log_info!(
                ctx.clone(),
                format!("Parsing info messages ({}):", self.info_messages.len()),
                audience = LogAudience::Internal
            );
            for info in &self.info_messages {
                log_info!(ctx.clone(), format!("  {}", info), audience = LogAudience::Internal);
            }
        }
    }
}
