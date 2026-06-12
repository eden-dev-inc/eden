use crate::api::lib::RedisCommandInput;
use error::{EpError, ResultEP};

/// Splits a typed multi-key Redis command into its single-key variant and
/// back. The runtime RESP-level policy and exclusion list live in
/// [`crate::api::lib::multi_key_policy`].
pub trait MultiCommand: RedisCommandInput {
    /// The single-key variant input type (e.g. single-key `MgetInput` for
    /// `MgetInput`).
    type Single: RedisCommandInput;

    /// The output type of the single-key variant.
    type SingleOutput;

    /// The output type of the multi-key form (e.g. `MgetOutput`).
    type Output;

    /// Returns one single-key command per key, in `self.keys()` order.
    fn deconstruct(&self) -> Vec<Self::Single>;

    /// Recomposes per-key results into the multi-key result.
    ///
    /// `parts.len()` must equal `self.deconstruct().len()`.
    fn reconstruct(parts: Vec<Result<Self::SingleOutput, EpError>>) -> ResultEP<Self::Output>;
}
