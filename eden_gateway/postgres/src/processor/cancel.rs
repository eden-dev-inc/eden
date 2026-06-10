use dashmap::DashMap;
use endpoints::endpoint::postgres::protocol::PgPinnedConnection;

#[derive(Clone)]
pub(crate) struct CancelTarget {
    host: String,
    port: u16,
    backend_pid: i32,
    backend_secret: i32,
}

static CANCEL_REGISTRY: std::sync::LazyLock<DashMap<(i32, i32), Vec<CancelTarget>>> = std::sync::LazyLock::new(DashMap::new);

pub(crate) fn cancel_registry_add(client_pid: i32, client_secret: i32, target: CancelTarget) {
    CANCEL_REGISTRY.entry((client_pid, client_secret)).or_default().push(target);
}

pub(crate) fn cancel_registry_clear(client_pid: i32, client_secret: i32) {
    CANCEL_REGISTRY.remove(&(client_pid, client_secret));
}

pub(crate) fn cancel_targets(client_pid: i32, client_secret: i32) -> Option<Vec<CancelTarget>> {
    CANCEL_REGISTRY.get(&(client_pid, client_secret)).map(|targets| targets.clone())
}

pub(crate) fn cancel_target_from_conn(conn: &PgPinnedConnection) -> Option<CancelTarget> {
    let (backend_pid, backend_secret) = conn.backend_key_data()?;
    let config = conn.config();
    Some(CancelTarget {
        host: config.host.clone(),
        port: config.port,
        backend_pid,
        backend_secret,
    })
}

pub(crate) async fn forward_cancel_request(target: &CancelTarget) -> Result<(), std::io::Error> {
    use tokio::io::AsyncWriteExt;

    let mut stream = tokio::net::TcpStream::connect((target.host.as_str(), target.port)).await?;
    let msg = postgres_wire::types::CancelRequest::new(target.backend_pid, target.backend_secret).encode();
    stream.write_all(&msg).await?;
    stream.shutdown().await?;
    Ok(())
}
