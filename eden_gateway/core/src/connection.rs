use ep_core::database::schema::interlay_tls::InterlayTls;
use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
};
use std::{io::BufReader, sync::Arc};
use tokio::{io, net::TcpStream};
use tokio_rustls::{TlsAcceptor, server::TlsStream};

// TODO: Consider boxing to reduce size differences between variants.
#[allow(clippy::large_enum_variant)]
pub enum InterlayStream {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

pub async fn open(tcp_stream: TcpStream, tls_acceptor: &Option<TlsAcceptor>) -> io::Result<InterlayStream> {
    if let Some(acceptor) = tls_acceptor {
        let tls_stream = acceptor.accept(tcp_stream).await?;
        Ok(InterlayStream::Tls(tls_stream))
    } else {
        Ok(InterlayStream::Tcp(tcp_stream))
    }
}

pub fn build_tls_acceptor(settings: &InterlayTls) -> io::Result<TlsAcceptor> {
    let server_key = load_server_key(settings.server_key())?;
    let server_cert = load_server_cert(settings.server_cert())?;

    let config = match settings.client_ca_cert() {
        Some(client_ca_cert) if !client_ca_cert.is_empty() => {
            let root_cert_store = load_root_cert_store(client_ca_cert)?;

            let verifier_builder = WebPkiClientVerifier::builder(Arc::new(root_cert_store));
            let verifier = if settings.require_client_certificate() {
                verifier_builder.build()
            } else {
                verifier_builder.allow_unauthenticated().build()
            }
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

            ServerConfig::builder().with_client_cert_verifier(verifier)
        }

        _ => ServerConfig::builder().with_no_client_auth(),
    };

    let config = config.with_single_cert(server_cert, server_key).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn load_server_key(server_key: &str) -> io::Result<PrivateKeyDer<'static>> {
    let mut reader = BufReader::new(server_key.as_bytes());

    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no server key found in interlay tls settings"))
}

fn load_server_cert(server_cert: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    let mut reader = BufReader::new(server_cert.as_bytes());

    let certs = rustls_pemfile::certs(&mut reader).collect::<io::Result<Vec<_>>>()?;

    if certs.is_empty() {
        Err(io::Error::new(io::ErrorKind::NotFound, "no server certificates found in interlay tls settings"))
    } else {
        Ok(certs)
    }
}

fn load_root_cert_store(client_ca_cert: &str) -> io::Result<RootCertStore> {
    let mut reader = BufReader::new(client_ca_cert.as_bytes());

    let ca_certs = rustls_pemfile::certs(&mut reader).collect::<io::Result<Vec<_>>>()?;

    let mut store = RootCertStore::empty();

    match store.add_parsable_certificates(ca_certs) {
        (added, _) if added > 0 => Ok(store),

        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no CA certificates could be added to the root store (interlay tls settings)",
        )),
    }
}
