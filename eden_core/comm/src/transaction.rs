use std::ops::Deref;

use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpTransaction;
use error::{EpError, ParseError, ResultEP};
use format::endpoint::EpKind;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use utoipa::openapi::{OneOf, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

pub trait TransactionSerde {
    fn serde_deserialize<T>(&self, s: &str) -> ResultEP<T>
    where
        T: DeserializeOwned,
    {
        serde_json::from_str(s).map_err(EpError::serde)
    }

    fn serde_serialize<T>(&self) -> ResultEP<String>
    where
        Self: Serialize + TransactionDowncast,
        T: 'static + BorshSerialize,
    {
        if let Some(data) = self.downcast_ref::<T>() {
            serde_json::to_string(&self).map_err(EpError::serde)
        } else {
            Err(EpError::Parse(ParseError::FailedToDowncastInput))
        }
    }

    fn borsh_deserialize<T>(data: &[u8]) -> ResultEP<T>
    where
        T: BorshDeserialize,
    {
        borsh::from_slice::<T>(data).map_err(EpError::serde)
    }

    fn borsh_serialize<T>(&self) -> ResultEP<Vec<u8>>
    where
        Self: TransactionDowncast,
        T: 'static + BorshSerialize,
    {
        if let Some(data) = self.downcast_ref::<T>() {
            borsh::to_vec(data).map_err(EpError::serde)
        } else {
            Err(EpError::Parse(ParseError::FailedToDowncastInput))
        }
    }
}

impl TransactionSerde for dyn EpTransaction {}

pub trait TransactionDowncast {
    fn downcast_ref<T: 'static>(&self) -> Option<&T>;
}

impl<T: EpTransaction + ?Sized> TransactionDowncast for T {
    fn downcast_ref<U: 'static>(&self) -> Option<&U> {
        self.as_any().downcast_ref::<U>()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct EndpointTransactionInput(Box<dyn EpTransaction>);

impl EndpointTransactionInput {
    pub fn new(transaction: Box<dyn EpTransaction>) -> Self {
        Self(transaction)
    }
    pub fn transaction(&self) -> &Box<dyn EpTransaction> {
        &self.0
    }
    pub fn kind(&self) -> EpKind {
        self.0.kind()
    }
}

impl Deref for EndpointTransactionInput {
    type Target = Box<dyn EpTransaction>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToSchema for EndpointTransactionInput {}
impl PartialSchema for EndpointTransactionInput {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::OneOf(OneOf::default()))
    }
}
