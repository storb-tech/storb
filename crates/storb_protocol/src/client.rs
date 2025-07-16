//! RPC client

use std::net::SocketAddr;
use std::sync::Arc;

use quinn::crypto::rustls::{NoInitialCipherSuite, QuicClientConfig};
use quinn::{ClientConfig, ConnectError, Connection, ConnectionError, Endpoint};
use rustls::ClientConfig as RustlsClientConfig;
use storb_core::signature::InsecureCertVerifier;
use thiserror::Error;
use tokio::time::error::Elapsed;
use tracing::debug;

use crate::constants::{MAX_ATTEMPTS, QUIC_CONNECTION_TIMEOUT};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Failed to establish QUIC connection: {0}")]
    Connect(#[from] ConnectError),

    #[error("Lost the QUIC connection: {0}")]
    ConnectionError(#[from] ConnectionError),

    #[error("Timeout expired: {0}")]
    TimeoutExpired(#[from] Elapsed),

    #[error("Failed to create QUIC config: {0}")]
    ConfigError(#[from] NoInitialCipherSuite),

    #[error("Failed to create endpoint: {0}")]
    EndpointCreationError(String),
}

/// Client configuration options.
pub struct ClientOptions {
    pub insecure: bool,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self { insecure: false }
    }
}

/// Configures a QUIC client with certificate verification.
/// Certificate verification can be disabled if desired, such as for development purposes.
pub fn configure_client(options: ClientOptions) -> Result<ClientConfig, ClientError> {
    let mut tls_config = RustlsClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();

    if options.insecure {
        tls_config
            .dangerous()
            .set_certificate_verifier(Arc::new(InsecureCertVerifier));
    }

    let quic_config = QuicClientConfig::try_from(tls_config)?;
    let client_config = ClientConfig::new(Arc::new(quic_config));
    Ok(client_config)
}

/// Creates a QUIC client endpoint bound to specified address with automatic retries.
pub fn create_client_endpoint(
    bind_addr: SocketAddr,
    client_options: ClientOptions,
) -> Result<Endpoint, ClientError> {
    let client_cfg = configure_client(client_options)?;

    let mut attempts = 0;
    while attempts < MAX_ATTEMPTS {
        match Endpoint::client(bind_addr) {
            Ok(mut endpoint) => {
                endpoint.set_default_client_config(client_cfg.clone());
                return Ok(endpoint);
            }
            Err(_) if attempts < MAX_ATTEMPTS - 1 => {
                attempts += 1;
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }
            Err(err) => return Err(ClientError::EndpointCreationError(format!("{err}"))),
        }
    }

    Err(ClientError::EndpointCreationError(
        "Exceeded maximum allowed attempts".into(),
    ))
}

/// Creates QUIC client connection with the provided endpoint and address.
pub async fn create_quic_client(
    client: &Endpoint,
    addr: SocketAddr,
) -> Result<Connection, ClientError> {
    debug!("Creating QUIC client connection to {addr}");

    let connection = tokio::time::timeout(QUIC_CONNECTION_TIMEOUT, async {
        let conn = match client.connect(addr, "0.0.0.0") {
            Ok(conn) => conn,
            Err(err) => {
                return Err(ClientError::Connect(err));
            }
        };
        match conn.await {
            Ok(connection) => Ok(connection),
            Err(err) => Err(ClientError::ConnectionError(err)),
        }
    })
    .await??;

    debug!("QUIC connection established successfully");
    Ok(connection)
}
