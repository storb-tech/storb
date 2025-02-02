use bytes::Bytes;
use miner::{Miner, MinerConfig};
use neuron::dht::StorbDHT;
use quinn::rustls::pki_types::PrivatePkcs8KeyDer;
use quinn::{Endpoint, ServerConfig};
use rcgen::generate_simple_self_signed;
use std::error::Error;
use std::net::UdpSocket;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;
use tracing::{error, info};

pub mod miner;

/// Processes a chunk of bytes by computing its BLAKE3 hash
/// and returning it as a hexadecimal string
async fn process_chunk(chunk: Bytes) -> String {
    blake3::hash(&chunk).to_hex().to_string()
}

/// Configures the QUIC server with a self-signed certificate for localhost
///
/// # Returns
/// - A ServerConfig containing the server configuration with the self-signed cert
/// - An error if server configuration fails
fn configure_server() -> Result<ServerConfig, Box<dyn Error + Send + Sync + 'static>> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert;
    let priv_key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

    let server_config = ServerConfig::with_single_cert(vec![cert_der.into()], priv_key.into());

    Ok(server_config?)
}

async fn main(config: MinerConfig) {
    info!("Configuring miner server...");
    let server_config = configure_server().unwrap();

    let miner = Arc::new(Mutex::new(Miner::new(config.clone()).await.unwrap()));

    // Create weak reference for sync task
    let sync_miner = Arc::downgrade(&miner);

    // Spawn background sync task
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(
            config.neuron_config.neuron.sync_frequency,
        ));
        loop {
            interval.tick().await;
            if let Some(miner) = sync_miner.upgrade() {
                let mut guard = miner.lock().await;
                guard.sync().await;
            }
        }
    });

    let socket = UdpSocket::bind(format!("0.0.0.0:{}", config.neuron_config.api_port))
        .expect("Failed to bind UDP socket");

    let endpoint = Endpoint::new(
        Default::default(),
        Some(server_config),
        socket,
        Arc::new(quinn::TokioRuntime),
    )
    .expect("Failed to create QUIC endpoint");

    info!(
        "Miner listening for chunks on quic://0.0.0.0:{}",
        config.neuron_config.api_port
    );

    // TODO: Add error handling for the Swarm initialization.
    let dht = StorbDHT::new().unwrap();
    dht.run().await.unwrap();

    while let Some(incoming) = endpoint.accept().await {
        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    info!("New connection from {}", conn.remote_address());

                    while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                        info!("New bidirectional stream opened");

                        // Read the total size first
                        let mut size_buf = [0u8; 8];
                        if let Err(e) = recv.read_exact(&mut size_buf).await {
                            error!("Failed to read total size: {}", e);
                            continue;
                        }
                        let total_size = u64::from_be_bytes(size_buf);
                        info!("Expected total size: {} bytes", total_size);

                        // Process chunks in a loop
                        let mut processed = 0;
                        let chunk_size = 64 * 1024; // 64KB buffer
                        let mut buffer = vec![0u8; chunk_size];

                        while processed < total_size {
                            let remaining = (total_size - processed) as usize;
                            let to_read = remaining.min(chunk_size);

                            // Ensure we read exactly the amount we expect
                            let mut chunk = Vec::new();
                            let mut bytes_read = 0;

                            while bytes_read < to_read {
                                match recv.read(&mut buffer[..to_read - bytes_read]).await {
                                    Ok(Some(n)) if n > 0 => {
                                        chunk.extend_from_slice(&buffer[..n]);
                                        bytes_read += n;
                                    }
                                    Ok(_) => break, // End of stream
                                    Err(e) => {
                                        error!("Error reading chunk: {}", e);
                                        break;
                                    }
                                }
                            }

                            if bytes_read == 0 {
                                break; // No more data to read
                            }

                            processed += bytes_read as u64;
                            info!("Received chunk of exactly {} bytes", bytes_read);

                            let hash = process_chunk(chunk.into()).await;
                            if let Err(e) = send.write_all(hash.as_bytes()).await {
                                error!("Failed to send hash: {}", e);
                                break;
                            }
                            // Send delimiter after each hash
                            if let Err(e) = send.write_all(b"\n").await {
                                error!("Failed to send delimiter: {}", e);
                                break;
                            }
                        }

                        info!(
                            "Finished processing file. Total bytes processed: {}",
                            processed
                        );
                    }
                }
                Err(e) => {
                    error!("Connection failed: {}", e);
                }
            }
        });
    }
}

/// Run the miner
pub fn run(config: MinerConfig) {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(main(config))
}
