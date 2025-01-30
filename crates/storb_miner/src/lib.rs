use bytes::Bytes;
use neuron::NeuronConfig;
use quinn::rustls::pki_types::PrivatePkcs8KeyDer;
use quinn::{Endpoint, ServerConfig};
use rcgen::{generate_simple_self_signed, Certificate};
use std::error::Error;
use std::net::UdpSocket;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tracing::{debug, error, info};

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

                        let mut chunk_buffer = Vec::new();
                        match recv.read_buf(&mut chunk_buffer).await {
                            Ok(_) => {
                                info!("Received chunk of {:?} bytes", chunk_buffer.len());

                                match process_chunk(chunk_buffer.into()).await {
                                    Ok(result) => {
                                        if let Err(e) = send.write_all(result.as_bytes()).await {
                                            error!("Failed to send result: {}", e);
                                        }
                                    }
                                    Err(e) => {
                                        error!("Chunk processing error: {}", e);
                                        if let Err(e) = send.write_all(b"PROCESSING_ERROR").await {
                                            error!("Failed to send error response: {}", e);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Error reading chunk: {}", e);
                            }
                        }
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
