use bytes::Bytes;
use neuron::NeuronConfig;
use quinn::rustls::pki_types::PrivatePkcs8KeyDer;
use quinn::{Endpoint, ServerConfig};
use rcgen::{generate_simple_self_signed, Certificate};
use std::error::Error;
use std::net::UdpSocket;
use std::sync::Arc;
use tracing::{error, info};

/// Miner configuration
#[derive(Clone)]
pub struct MinerConfig {
    pub neuron_config: NeuronConfig,
}

/// The Storb miner
#[derive(Clone)]
struct Miner {
    config: MinerConfig,
}

impl Miner {
    pub fn new(config: MinerConfig) -> Self {
        Miner { config }
    }
}

async fn process_chunk(chunk: Bytes) -> Result<String, Box<dyn Error + Send + Sync>> {
    let hash = blake3::hash(&chunk).to_hex();
    Ok(hash.to_string())
}

async fn main() {
    let quic_port = 5000;
    info!("Configuring miner server...");
    let server_config = configure_server().unwrap();

    let socket =
        UdpSocket::bind(format!("0.0.0.0:{}", quic_port)).expect("Failed to bind UDP socket");

    let endpoint = Endpoint::new(
        Default::default(),
        Some(server_config),
        socket,
        Arc::new(quinn::TokioRuntime),
    )
    .expect("Failed to create QUIC endpoint");

    info!("Miner listening for chunks on quic://0.0.0.0:{}", quic_port);

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

                            if bytes_read > 0 {
                                processed += bytes_read as u64;
                                info!("Received chunk of exactly {} bytes", bytes_read);

                                match process_chunk(chunk.into()).await {
                                    Ok(hash) => {
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
                                    Err(e) => {
                                        error!("Chunk processing error: {}", e);
                                        break;
                                    }
                                }
                            } else {
                                break; // No more data to read
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
pub fn run() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(main())
}

fn configure_server() -> Result<ServerConfig, Box<dyn Error + Send + Sync + 'static>> {
    let cert = generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = Certificate::from(cert.cert);
    let priv_key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

    let server_config = ServerConfig::with_single_cert(vec![cert_der.into()], priv_key.into());

    Ok(server_config.unwrap())
}
