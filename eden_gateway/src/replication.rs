use eden_core::error::ResultEP;
use std::future::Future;

#[allow(dead_code)]
pub trait ReplicationExporter {
    fn connect_to_source(&self) -> impl Future<Output = ResultEP<()>>;
    fn write_to_sync<T>(&self, source: T) -> impl Future<Output = ResultEP<()>>;
}
