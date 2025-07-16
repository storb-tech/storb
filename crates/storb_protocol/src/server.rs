//! RPC server

use quinn::rustls::pki_types::PrivatePkcs8KeyDer;
use quinn::ServerConfig;
use rcgen::{generate_simple_self_signed, Error as RcgenError};
use rustls::Error as RustlsError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("rcgen error: {0}")]
    RcgenError(#[from] RcgenError),

    #[error("rustls error: {0}")]
    RustlsError(#[from] RustlsError),
}

/// Generate a self‐signed cert for `localhost` to provide to the external IP.
pub fn configure_server(external_ip: &str) -> Result<ServerConfig, ServerError> {
    let cert = generate_simple_self_signed(vec!["localhost".into(), external_ip.into()])?;
    let cert_der = cert.cert;
    let key_der = cert.key_pair.serialize_der();
    let priv_key = PrivatePkcs8KeyDer::from(key_der);

    // Build Quinn ServerConfig with our cert key
    let server_config = ServerConfig::with_single_cert(vec![cert_der.into()], priv_key.into())?;
    Ok(server_config)
}
