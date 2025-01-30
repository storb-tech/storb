use neuron::NeuronConfig;
use quinn::rustls::pki_types::PrivatePkcs8KeyDer;
use quinn::{Endpoint, ServerConfig};
use rcgen::{generate_simple_self_signed, Certificate};
use std::error::Error;
use std::net::UdpSocket;
use std::sync::Arc;
use tracing::info;

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

async fn main() {
    let quic_port = 5000;
    let server_config = configure_server().unwrap();

    let socket = UdpSocket::bind(format!("0.0.0.0:{}", quic_port)).unwrap();

    let endpoint = Endpoint::new(
        Default::default(),
        Some(server_config),
        socket,
        Arc::new(quinn::TokioRuntime),
    )
    .unwrap();

    info!("Miner is listening on quic://0.0.0.0:{}", quic_port);

    while let Some(conn) = endpoint.accept().await {
        tokio::spawn(async move {
            let conn = conn.await.unwrap();
            info!("Accepted connection from: {}", conn.remote_address());

            while let Ok((mut send_stream, mut rcv_stream)) = conn.accept_bi().await {
                // Handle the incoming stream
                tokio::spawn(async move {
                    let mut buffer = Vec::new();
                    while let Ok(Some(data)) = rcv_stream.read(&mut buffer).await {
                        let data = &buffer[..data];
                        let data_hash = blake3::hash(data).to_hex();
                        send_stream
                            .write_all(format!("received data {}", data_hash).as_bytes())
                            .await
                            .unwrap();
                    }
                });
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
