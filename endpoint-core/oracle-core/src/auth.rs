use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq, ToSchema)]
pub struct OracleAuth {
    pub username: String,
    pub password: String,
    pub privilege: Option<String>,
}

impl OracleAuth {
    pub fn get_privelege(&self) -> Option<oracle::Privilege> {
        match &self.privilege {
            Some(text) => match text.to_lowercase().as_str() {
                "sysasm" => Some(oracle::Privilege::Sysasm),
                "sysbackup" => Some(oracle::Privilege::Sysbackup),
                "sysdba" => Some(oracle::Privilege::Sysdba),
                "sysdg" => Some(oracle::Privilege::Sysdg),
                "syskm" => Some(oracle::Privilege::Syskm),
                "sysoper" => Some(oracle::Privilege::Sysoper),
                "sysrac" => Some(oracle::Privilege::Sysrac),
                _ => None,
            },
            None => None,
        }
    }
}
