use crate::signature::InsecureCertVerifier;
use anyhow::{anyhow, Result};
use crabtensor::rpc::types::NeuronInfoLite;
use quinn::{crypto::rustls::QuicClientConfig, TransportConfig};
use quinn::{ClientConfig, Connection, Endpoint, IdleTimeout};
use rustls::ClientConfig as RustlsClientConfig;
use std::{error::Error, net::SocketAddr, sync::Arc, time::Duration};
use tracing::{error, info};

pub const QUIC_CONNECTION_TIMEOUT: Duration = Duration::from_secs(1);

// TODO:
// 1. Producer produces bytes
//  1a. Read collection of bytes from multipart form
//  1b. Fill up a shared buffer with that collection
//  1c. Signal that its done
// 2. Consumer consumes bytes
//  2a. Reads a certain chunk of the collection bytes from shared buffer
//  2b. FECs it into pieces. A background thread is spawned
//      to:
//      - distribute these pieces to a selected set of miners
//      - verify pieces are being stored
//      - update miner statistics

const MIN_REQUIRED_MINERS: usize = 1; // Minimum number of miners needed for operation

pub async fn establish_miner_connections(
    miners: &[NeuronInfoLite],
) -> Result<Vec<(SocketAddr, Connection)>> {
    let mut connection_futures = Vec::new();

    for miner in miners {
        let ip_u32 = miner.axon_info.ip;
        let ip_bytes: [u8; 4] = [
            (ip_u32 >> 24) as u8,
            (ip_u32 >> 16) as u8,
            (ip_u32 >> 8) as u8,
            ip_u32 as u8,
        ];
        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::from(ip_bytes));
        let addr = SocketAddr::new(ip, miner.axon_info.port);

        let client = make_client_endpoint(SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            0,
        ))
        .map_err(|e| anyhow!(e.to_string()))?;

        connection_futures.push(async move {
            match create_quic_client(&client, addr).await {
                Ok(connection) => Some((addr, connection)),
                Err(_) => None,
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

    let mut client_config = ClientConfig::new(Arc::new(quic_config));
    // Disable segmentation offload
    let mut transport_config = TransportConfig::default();
    let timeout = IdleTimeout::try_from(Duration::from_secs(30))?;
    transport_config
        .enable_segmentation_offload(true) // Disable segmentation offload
        .keep_alive_interval(Some(Duration::from_secs(15))) // Add keepalive
        .max_idle_timeout(Some(timeout)) // Set idle timeout
        .initial_rtt(Duration::from_millis(100)); // Set initial RTT estimate
    client_config.transport_config(Arc::new(transport_config));
    Ok(client_config)
}

/// Creates a QUIC client endpoint bound to specified address with automatic retries
pub fn make_client_endpoint(
    bind_addr: SocketAddr,
) -> Result<Endpoint, Box<dyn Error + Send + Sync + 'static>> {
    let client_cfg = configure_client()?;

    let mut attempts = 0;
    let max_attempts = 5;

    while attempts < max_attempts {
        match Endpoint::client(bind_addr) {
            Ok(mut endpoint) => {
                endpoint.set_default_client_config(client_cfg.clone());
                return Ok(endpoint);
            }
            Err(_) if attempts < max_attempts - 1 => {
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
        let conn = client.connect(addr, "localhost")?;
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
