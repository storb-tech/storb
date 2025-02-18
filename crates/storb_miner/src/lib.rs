use anyhow::{Context, Result};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Json;
use base::piece_hash::PieceHash;
use libp2p;
use miner::{Miner, MinerConfig};
use quinn::rustls::pki_types::PrivatePkcs8KeyDer;
use quinn::{Endpoint, ServerConfig};
use rcgen::generate_simple_self_signed;
use std::error::Error;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::Duration;
use store::ObjectStore;
use tokio::sync::Mutex;
use tokio::time;
use tracing::{error, info};

pub mod miner;
pub mod store;

#[derive(Clone)]
pub struct MinerState {
    pub miner: Arc<Mutex<Miner>>,
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

async fn main(config: MinerConfig) -> Result<()> {
    info!("Configuring miner server...");
    let server_config = configure_server().unwrap();

    let miner = Arc::new(Mutex::new(Miner::new(config.clone()).await.unwrap()));

    // Create weak reference for sync task
    let sync_miner = Arc::downgrade(&miner);

    let object_store =
        Arc::new(ObjectStore::new(&config.store_dir).expect("Failed to initialize object store"));

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

    let state = MinerState { miner };

    let socket = UdpSocket::bind(format!("0.0.0.0:{}", config.neuron_config.quic_port))
        .expect("Failed to bind UDP socket");

    let endpoint = Endpoint::new(
        Default::default(),
        Some(server_config),
        socket,
        Arc::new(quinn::TokioRuntime),
    )
    .expect("Failed to create QUIC endpoint");

    info!(
        "Miner listening for pieces on quic://0.0.0.0:{}",
        config.neuron_config.quic_port
    );

    let app = axum::Router::new()
        .route("/peerid", get(peer_id))
        .route("/quic", get(quic_port))
        .with_state(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], config.neuron_config.api_port)); // TODO: hard code this for now
    info!("Miner HTTP server listening on {}", addr);

    let http_server = tokio::spawn(async move {
        axum::serve(
            tokio::net::TcpListener::bind(addr)
                .await
                .context("Failed to bind HTTP server")
                .unwrap(),
            app,
        )
        .await
        .context("HTTP server failed")
        .unwrap();
    });

    let quic_server = tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let object_store = Arc::clone(&object_store);
            tokio::spawn(async move {
                match incoming.await {
                    Ok(conn) => {
                        info!("New connection from {}", conn.remote_address());

                        while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                            info!("New bidirectional stream opened");

                            // Read the piece size
                            let mut piece_size_buf = [0u8; 8];
                            if let Err(e) = recv.read_exact(&mut piece_size_buf).await {
                                error!("Failed to read piece size: {}", e);
                                continue;
                            }
                            let piece_size = u64::from_be_bytes(piece_size_buf) as usize;
                            info!("Received piece size: {} bytes", piece_size);

                            // Process the piece
                            let mut buffer = vec![0u8; piece_size];
                            let mut bytes_read = 0;
                            let mut piece = Vec::new();

                            while bytes_read < piece_size {
                                match recv.read(&mut buffer[..piece_size - bytes_read]).await {
                                    Ok(Some(n)) if n > 0 => {
                                        piece.extend_from_slice(&buffer[..n]);
                                        bytes_read += n;
                                    }
                                    Ok(_) => break, // End of stream
                                    Err(e) => {
                                        error!("Error reading piece: {}", e);
                                        break;
                                    }
                                }
                            }

                            if bytes_read == 0 {
                                continue; // No data received
                            }

                            info!("Received piece of exactly {} bytes", bytes_read);

                            let hash_raw = blake3::hash(&piece);
                            let hash = hash_raw.to_hex().to_string();
                            let piece_key = libp2p::kad::RecordKey::new(&hash.as_bytes());

                            // Add miner as a provider for the piece
                            // let dht_sender = state.miner.lock().await.neuron.command_sender.clone();
                            // match swarm::dht::StorbDHT::start_providing_piece(dht_sender, piece_key) {
                            //     Ok(_) => info!("Added miner as provider for piece"),
                            //     Err(e) => error!("Failed to add miner as provider for piece: {}", e),
                            // }

                            if let Err(e) = send.write_all(hash.as_bytes()).await {
                                error!("Failed to send hash: {}", e);
                                continue;
                            }
                            // Send delimiter after hash
                            if let Err(e) = send.write_all(b"\n").await {
                                error!("Failed to send delimiter: {}", e);
                                continue;
                            }

                            let piece_hash =
                                PieceHash::new(hash).expect("Failed to create PieceHash"); // TODO: handle error
                            object_store
                                .write(&piece_hash, &piece)
                                .await
                                .expect("Failed to write piece to store");
                            info!("Finished storing piece of size: {}", bytes_read);
                        }
                    }
                    Err(e) => {
                        error!("Connection failed: {}", e);
                    }
                }
            });
        }
    });

    let _ = tokio::try_join!(http_server, quic_server);

    Ok(())
}

/// Get the peer ID of the miner
pub async fn peer_id(
    state: axum::extract::State<MinerState>,
) -> Result<impl IntoResponse, (StatusCode, Vec<u8>)> {
    let peer_id = state.miner.lock().await.neuron.peer_id;
    Ok(peer_id.to_bytes())
}

pub async fn quic_port(
    state: axum::extract::State<MinerState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let miner = state.miner.lock().await;
    info!("Got quic port req");
    let quic_port = miner.config.neuron_config.quic_port;
    Ok(Json(quic_port))
}

/// Run the miner
pub fn run(config: MinerConfig) {
    let _ = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(main(config));
}
