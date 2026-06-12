use std::{error, fmt, result};

use crate::DBError;

/// Result type alias for verification operations.
pub type ResultVerification<T> = result::Result<T, VerificationError>;

/// Cryptographic verification and blockchain-related errors.
///
/// Used for transaction verification, signature validation, consensus operations,
/// and cryptographic primitive failures.
#[derive(Debug, PartialEq)]
pub enum VerificationError {
    AccountAlreadyExists,
    CommunicationError,
    ConsensusMissingReceiver,
    ConsensusMissingSender,
    CustomAccountCannotBeActor,
    CustomAccountCannotOwnEden,
    CustomAccountCannotOwnSubs,
    CustomAccountIncorrectNonce,
    DBError(String),
    EpochMismatch,
    ExecutorError,
    FailedAccountTransfer,
    FailedAssetTransfer,
    FailedCoinTransfer,
    FailedConsensus,
    FailedMixedTransfer,
    HashMismatch,
    Ignored,
    InvalidAccount,
    InvalidAsset,
    InvalidDelegate,
    InvaliDbtxRequestKind,
    InvalidEpochInfo,
    InvalidFee,
    InvalidHash,
    InvalidHex,
    InvalidOwner,
    InvalidParent,
    InvalidReceiver,
    InvalidSender,
    InvalidSubNet,
    InvaliDbtxHash,
    InvalidVerifier,
    MembersCannotBeZero,
    MissingAccount,
    MissingAsset,
    MissingFunds,
    NotFound,
    ProposedTooManyMembers,
    PubKeyError,
    ResultIsAboveMax,
    ResultIsLessThanZero,
    RoundEarly,
    RoundLate,
    SecretKeyError,
    SignatureError,
    VerificationError,
}

impl fmt::Display for VerificationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        let _ = match self {
            VerificationError::AccountAlreadyExists => write!(f, "account_already_exists"),
            VerificationError::CommunicationError => write!(f, "communication_error"),
            VerificationError::ConsensusMissingReceiver => write!(f, "consensus_missing_receiver"),
            VerificationError::ConsensusMissingSender => write!(f, "consensus_missing_sender"),
            VerificationError::CustomAccountCannotOwnSubs => {
                write!(f, "custom_account_cannot_own_subs")
            }
            VerificationError::CustomAccountCannotOwnEden => {
                write!(f, "custom_account_cannot_own_eden")
            }
            VerificationError::CustomAccountCannotBeActor => {
                write!(f, "custom_account_cannot_be_actor")
            }

            VerificationError::CustomAccountIncorrectNonce => {
                write!(f, "custom_account_incorrect_nonce")
            }
            VerificationError::DBError(err) => write!(f, "DB error: {err}"),
            VerificationError::EpochMismatch => write!(f, "epoch_mismatch"),
            VerificationError::ExecutorError => write!(f, "executor_error"),
            VerificationError::FailedAccountTransfer => write!(f, "failed_account_transfer"),
            VerificationError::FailedAssetTransfer => write!(f, "failed_asset_transfer"),
            VerificationError::FailedCoinTransfer => write!(f, "failed_coin_transfer"),
            VerificationError::FailedConsensus => write!(f, "failed_consensus"),
            VerificationError::FailedMixedTransfer => write!(f, "failed_mixed_transfer"),
            VerificationError::HashMismatch => write!(f, "hash_mismatch"),
            VerificationError::Ignored => write!(f, "ignored"),
            VerificationError::InvalidAccount => write!(f, "invalid_account"),
            VerificationError::InvalidAsset => write!(f, "invalid_asset"),
            VerificationError::InvalidDelegate => write!(f, "invalid_delegate"),
            VerificationError::InvalidHash => write!(f, "invalid_hash"),
            VerificationError::InvalidHex => write!(f, "invalid_hex_encoding"),
            VerificationError::InvaliDbtxRequestKind => write!(f, "invalid_envelope_kind"),
            VerificationError::InvalidEpochInfo => write!(f, "invalid_epoch"),
            VerificationError::InvalidFee => write!(f, "invalid_fee"),
            VerificationError::NotFound => write!(f, "not_found"),
            VerificationError::InvalidOwner => write!(f, "invalid_owner"),
            VerificationError::InvalidParent => write!(f, "invalid_parent"),
            VerificationError::InvalidReceiver => write!(f, "invalid_receiver"),
            VerificationError::InvalidSender => write!(f, "invalid_sender"),
            VerificationError::InvalidSubNet => write!(f, "invalid_subnet"),
            VerificationError::InvaliDbtxHash => write!(f, "invalid_tx_hash"),
            VerificationError::InvalidVerifier => write!(f, "invalid_verifier"),
            VerificationError::MembersCannotBeZero => write!(f, "members_cannot_be_zero"),
            VerificationError::MissingAccount => write!(f, "missing_account"),
            VerificationError::MissingAsset => write!(f, "missing_asset"),
            VerificationError::MissingFunds => write!(f, "missing_funds"),
            VerificationError::ProposedTooManyMembers => write!(f, "proposed_too_many_members"),
            VerificationError::PubKeyError => write!(f, "public_key_error"),
            VerificationError::ResultIsAboveMax => write!(f, "operation_result_is_above_max"),
            VerificationError::ResultIsLessThanZero => {
                write!(f, "operation_result_is_less_than_zero")
            }
            VerificationError::RoundEarly => write!(f, "round_early"),
            VerificationError::RoundLate => write!(f, "round_late"),
            VerificationError::SecretKeyError => write!(f, "secret_key_error"),
            VerificationError::SignatureError => write!(f, "signature_error"),
            VerificationError::VerificationError => write!(f, "verification_error"),
        };
        Ok(())
    }
}

impl error::Error for VerificationError {}

impl From<DBError> for VerificationError {
    fn from(value: DBError) -> Self {
        if value == DBError::Ignored {
            return Self::Ignored;
        }
        Self::DBError(value.to_string())
    }
}
