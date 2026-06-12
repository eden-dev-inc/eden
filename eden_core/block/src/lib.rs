#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Block Structures
//!
//! Immutable block-based data structures with cryptographic verification.
//!
//! Provides Merkle tree support and signature verification for block data.

use borsh::{BorshDeserialize, BorshSerialize};
use ed25519_dalek::SigningKey;
use eden_logger_internal::{ctx_with_trace, log_trace};
use error::VerificationError;
use format::{hashtype::HashType, nonce::Nonce, timestamp::Timestamp};
use request::EdenRequest;
use rs_merkle::{MerkleTree, algorithms::Sha256};
use serde::{Deserialize, Serialize};

use function_name::named;
use node::{PubKey, Signature};

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct Block {
    pub hash: HashType,
    pub signature: Signature,
    pub body: BlockBody,
}

impl Block {
    #[named]
    pub fn empty_with_nonce(nonce: Nonce) -> Self {
        let _ctx = ctx_with_trace!().with_feature("block");

        log_trace!(
            _ctx,
            "Returning empty tx with nonce",
            audience = eden_logger_internal::LogAudience::Internal,
            nonce = nonce.to_string()
        );
        let mut empty_block = Block::default();
        empty_block.body.nonce = nonce;
        empty_block.set_hash();
        empty_block
    }

    // pub fn add_tx(&mut self, req: EdenRequest) -> (TxHash, usize) {
    //     log::trace!(
    //         "tx {}/{} added to block with {} existing transactions",
    //         req.hash(),
    //         req.nonce(),
    //         self.body.txs.len()
    //     );
    //     let h = req.hash();
    //     self.body.txs.push(req);
    //     self.set_hash();
    //     (h, self.body.txs.len())
    // }

    pub fn to_bytes(&self) -> Vec<u8> {
        [self.hash.as_ref(), self.signature.as_ref(), self.body.to_bytes().as_ref()].concat()
    }

    pub fn set_hash(&mut self) {
        self.hash = self.body.hash();
    }

    pub fn merkle_root(&self) -> Option<HashType> {
        Some(HashType::from_byte_hash(self.body.merkle_tree().root()?))
    }

    pub fn merkle_proof(&self, leaf_indeces: Vec<usize>) -> Vec<u8> {
        self.body.merkle_tree().proof(&leaf_indeces).to_bytes()
    }

    pub fn verify_merkle_root(&self, root: HashType) -> bool {
        match self.merkle_root() {
            Some(r) => r.0 == root.0,
            None => false,
        }
    }

    pub fn hash(&self) -> HashType {
        self.body.hash()
    }

    pub fn sign(&mut self, signing_key: &[u8]) -> Result<(), VerificationError> {
        let key_bytes: &[u8; 32] = signing_key.try_into().map_err(|_| VerificationError::SecretKeyError)?;
        self.body.validator = PubKey::from(SigningKey::from_bytes(key_bytes).as_ref().as_bytes());
        let bytes = self.body.to_bytes();
        let secret = signing_key.try_into().map_err(|_| VerificationError::SecretKeyError)?;
        self.signature = Signature::sign(&bytes, secret);
        self.set_hash();
        Ok(())
    }

    pub fn verify_signature(&self, verifying_key: &[u8]) -> Result<(), VerificationError> {
        let bytes = self.body.to_bytes();
        let pubkey = verifying_key.try_into().map_err(|_| VerificationError::PubKeyError)?;
        self.signature.verify(&bytes, &pubkey)
    }

    pub fn len(&self) -> usize {
        self.body.txs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.body.txs.is_empty()
    }
}

// the data within each block
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct BlockBody {
    pub validator: PubKey,     // validator public key
    pub last_epoch: HashType,  // last epoch hash
    pub nonce: Nonce,          // nonce of blokc
    pub created: Timestamp,    // timestamp in microseconds
    pub dispatched: Timestamp, // timestamp in microseconds
    pub merkle_root: HashType, // transactions merkle tree root
    pub txs: Vec<EdenRequest>, // transactions within the block (0-1024) (keep the order of arrivals, so Vec)
}

impl BlockBody {
    pub fn to_bytes(&self) -> Vec<u8> {
        // [
        //     self.validator.as_ref(),
        //     self.last_epoch.as_ref(),
        //     self.nonce.to_bytes().as_slice(),
        //     self.created.to_bytes().as_slice(),
        //     self.dispatched.to_bytes().as_slice(),
        //     self.merkle_root.as_ref(),
        //     self.txs
        //         .iter()
        //         .flat_map(|tx| tx.to_bytes())
        //         .collect::<Vec<u8>>()
        //         .as_ref(),
        // ]
        // .concat()
        todo!()
    }

    /// the transaction hash includes all body fields, except 'txs.value' becuase,
    /// the merkle_root should include all transaction hashes.
    pub fn hash(&self) -> HashType {
        HashType::hash(&self.to_bytes())
    }

    pub fn merkle_tree(&self) -> MerkleTree<Sha256> {
        // let leaves: Vec<[u8; 32]> = self
        //     .txs
        //     .iter()
        //     .map(|tx| <[u8; 32]>::try_from(tx.hash().as_ref()).unwrap_or_default())
        //     .collect();
        // MerkleTree::<Sha256>::from_leaves(&leaves)
        todo!()
    }

    pub fn merkle_root(&self) -> Option<HashType> {
        Some(HashType::from_byte_hash(self.merkle_tree().root()?))
    }

    pub fn merkle_proof(&self, leaf_indeces: Vec<usize>) -> Vec<u8> {
        self.merkle_tree().proof(&leaf_indeces).to_bytes()
    }

    pub fn verify_merkle_root(&self, root: HashType) -> bool {
        match self.merkle_root() {
            Some(r) => r.0 == root.0,
            None => false,
        }
    }
}
