use crate::Operation;
use error::ResultEP;
use std::fmt::Debug;
use std::marker::PhantomData;

type MethodResponseOpt<I, A, K, X, Op> = Option<MethodResponse<I, A, K, X, Op>>;

pub trait EpProtocol<I, P, O, A, K, X, C, Op: Operation<A, K, X> + ?Sized>: Send + Sync + Debug {
    fn decode_buffer(buffer: &[u8]) -> Option<(I, usize)>;
    fn parse_buffer(buffer: &[u8]) -> ResultEP<Option<(P, usize)>>;
    fn parse_conflict_from_buffer(buffer: &[u8]) -> ResultEP<C>;
    fn parse_buffer_to_operation(buffer: &[u8]) -> ResultEP<Option<(Box<Op>, usize)>>;
    fn encode_to_buffer(response: &O) -> ResultEP<Vec<u8>>;
    fn validate_buffer(method: Method, buffer: &[u8]) -> ResultEP<MethodResponseOpt<I, A, K, X, Op>> {
        match method {
            Method::Simple => {
                Ok(Self::decode_buffer(buffer).map(|(_, consumed)| MethodResponse::Simple { consumed, _phantom: PhantomData }))
            }
            Method::Decode => Ok(Self::decode_buffer(buffer).map(|(decoded, consumed)| MethodResponse::Decode {
                decoded,
                consumed,
                _phantom: PhantomData,
            })),
            Method::Parse => Ok(Self::parse_buffer_to_operation(buffer)?.map(|(operation, consumed)| MethodResponse::Parse {
                operation,
                consumed,
                _phantom: PhantomData,
            })),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum Method {
    #[default]
    /// Use the most performant option, avoid parsing/decoding if possible
    Simple,
    /// Decode the bytes into the protocol format expected by the endpoint
    Decode,
    /// Parse the bytes into the Eden `Operation` format
    Parse,
}

#[derive(Debug)]
pub enum MethodResponse<I, A, K, X, Op>
where
    Op: Operation<A, K, X> + ?Sized,
{
    Simple {
        consumed: usize,
        _phantom: PhantomData<(I, A, K, X, Op)>,
    },
    Decode {
        decoded: I,
        consumed: usize,
        _phantom: PhantomData<(A, K, X, Op)>,
    },
    Parse {
        operation: Box<Op>,
        consumed: usize,
        _phantom: PhantomData<(I, A, K, X)>,
    },
}
