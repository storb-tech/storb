use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use base::utils::multiaddr_to_socketaddr;
use libp2p::Multiaddr;
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{ClientConfig, Connection, Endpoint};
use rustls::ClientConfig as RustlsClientConfig;
use tracing::{error, info};

use crate::signature::InsecureCertVerifier;

pub const QUIC_CONNECTION_TIMEOUT: Duration = Duration::from_secs(1);
// TODO: turn this into a setting?
const MIN_REQUIRED_MINERS: usize = 1; // Minimum number of miners needed for operation
const MAX_ATTEMPTS: i32 = 5;

/// Establishes QUIC connections to miners from a list of QUIC addresses.
pub async fn establish_miner_connections(
    quic_addresses: Vec<Multiaddr>,
) -> Result<Vec<(SocketAddr, Connection)>> {
    let mut connection_futures = Vec::new();

    for quic_addr in quic_addresses {
        let client = make_client_endpoint(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .map_err(|e| anyhow!(e.to_string()))?;
        let socket_addr = multiaddr_to_socketaddr(&quic_addr)
            .context("Could not get SocketAddr from Multiaddr. Ensure that the Multiaddr is not missing any components")?;

        connection_futures.push(async move {
            match create_quic_client(&client, socket_addr).await {
                Ok(connection) => Some((socket_addr, connection)),
                Err(e) => {
                    error!("Failed to establish connection with {}: {}", socket_addr, e);
                    None
                }
            }
        });
    }

    let connections: Vec<_> = futures::future::join_all(connection_futures)
        .await
        .into_iter()
        .flatten()
        .collect();

    if connections.len() < MIN_REQUIRED_MINERS {
        return Err(anyhow!(
            "Failed to establish minimum required connections. Got {} out of minimum {}",
            connections.len(),
            MIN_REQUIRED_MINERS
        ));
    }

    Ok(connections)
}

/// Configures a QUIC client with insecure certificate verification
pub fn configure_client() -> Result<ClientConfig, Box<dyn Error + Send + Sync + 'static>> {
    let mut tls_config = RustlsClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();

    tls_config
        .dangerous()
        .set_certificate_verifier(Arc::new(InsecureCertVerifier));

    let quic_config = QuicClientConfig::try_from(tls_config).unwrap_or_else(|e| {
        panic!("Failed to create QUIC client config: {:?}", e);
    });

    let client_config = ClientConfig::new(Arc::new(quic_config));
    Ok(client_config)
}

/// Creates a QUIC client endpoint bound to specified address with automatic retries
pub fn make_client_endpoint(
    bind_addr: SocketAddr,
) -> Result<Endpoint, Box<dyn Error + Send + Sync + 'static>> {
    let client_cfg = configure_client()?;

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
            Err(e) => return Err(e.into()),
        }
    }

    Err("Failed to create endpoint after maximum attempts".into())
}

/// Creates QUIC client connection with provided endpoint and address
pub async fn create_quic_client(client: &Endpoint, addr: SocketAddr) -> Result<Connection> {
    info!("Creating QUIC client connection to {}", addr);

    let connection = tokio::time::timeout(QUIC_CONNECTION_TIMEOUT, async {
        let conn = client.connect(addr, "0.0.0.0")?;
        match conn.await {
            Ok(connection) => Ok(connection),
            Err(e) => {
                error!("Failed to establish QUIC connection: {}", e);
                Err(anyhow!("QUIC onnection failed: {}", e))
            }
        }
    })
    .await
    .map_err(|_| anyhow!("QUIC connection timed out"))??;

    info!("QUIC connection established successfully");
    Ok(connection)
}
