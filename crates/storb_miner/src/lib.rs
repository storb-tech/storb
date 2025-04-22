use std::error::Error;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::middleware::from_fn;
use axum::routing::{get, post};
use axum::Extension;
use base::piece_hash::PieceHash;
use base::swarm;
use base::verification::HandshakePayload;
use crabtensor::sign::verify_signature;
use dashmap::DashMap;
use middleware::InfoApiRateLimiter;
use miner::{Miner, MinerConfig};
use quinn::rustls::pki_types::PrivatePkcs8KeyDer;
use quinn::{Endpoint, ServerConfig, VarInt};
use rcgen::generate_simple_self_signed;
use store::ObjectStore;
use tokio::sync::Mutex;
use tokio::time;
use tracing::{debug, error, info};

pub mod constants;
mod middleware;
pub mod miner;
mod routes;
pub mod store;

#[derive(Clone)]
pub struct MinerState {
    pub miner: Arc<Mutex<Miner>>,
    pub object_store: Arc<Mutex<ObjectStore>>,
}

/// Configures the QUIC server with a self-signed certificate for localhost
///
/// # Returns
/// - A ServerConfig containing the server configuration with the self-signed cert
/// - An error if server configuration fails
fn configure_server(
    external_ip: String,
) -> Result<ServerConfig, Box<dyn Error + Send + Sync + 'static>> {
    let cert = generate_simple_self_signed(vec!["localhost".into(), external_ip])?;
    let cert_der = cert.cert;
    let priv_key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

    let server_config = ServerConfig::with_single_cert(vec![cert_der.into()], priv_key.into());

    Ok(server_config?)
}

async fn main(config: MinerConfig) -> Result<()> {
    info!("Configuring miner server...");
    let server_config = configure_server(config.clone().neuron_config.external_ip).unwrap();

    let miner = Arc::new(Mutex::new(Miner::new(config.clone()).await.unwrap()));

    // Create weak reference for sync task
    let sync_miner = Arc::downgrade(&miner);

    let object_store = Arc::new(Mutex::new(
        ObjectStore::new(&config.store_dir).expect("Failed to initialize object store"),
    ));

    // Spawn background sync task
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(
            config.neuron_config.neuron.sync_frequency,
        ));
        loop {
            interval.tick().await;
            if let Some(miner) = sync_miner.upgrade() {
                let mut guard = miner.lock().await;
                let _ = guard.sync().await; // TODO: handle error properly
            }
        }
    });

    let state = Arc::new(Mutex::new(MinerState {
        miner,
        object_store,
    }));

    let assigned_quic_port = config
        .neuron_config
        .quic_port
        .expect("Could not assign quic port");

    let socket = UdpSocket::bind(format!("0.0.0.0:{}", assigned_quic_port))
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
        config
            .neuron_config
            .quic_port
            .expect("Could not get quic port")
    );
    let info_api_rate_limit_state: InfoApiRateLimiter = Arc::new(DashMap::new());
    let app = axum::Router::new()
        .route(
            "/info",
            get(routes::node_info).route_layer(from_fn(middleware::info_api_rate_limit_middleware)),
        )
        .layer(Extension(info_api_rate_limit_state))
        .route("/handshake", post(routes::handshake))
        .route("/piece", get(routes::get_piece))
        .with_state(state.clone());

    let addr = SocketAddr::from(([0, 0, 0, 0], config.neuron_config.api_port));
    info!("Miner HTTP server listening on {}", addr);

    let http_server = tokio::spawn(async move {
        axum::serve(
            tokio::net::TcpListener::bind(addr)
                .await
                .context("Failed to bind HTTP server")
                .unwrap(),
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .context("HTTP server failed")
        .unwrap();
    });

    let dht_sender = state
        .lock()
        .await
        .miner
        .lock()
        .await
        .neuron
        .command_sender
        .clone();

    let quic_server = tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let dht_sender_clone = dht_sender.clone();
            let object_store_clone = state.lock().await.object_store.lock().await.clone();
            let state_clone = state.clone();

            tokio::spawn(async move {
                match incoming.await {
                    Ok(conn) => {
                        debug!("New connection from {}", conn.remote_address());

                        while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                            debug!("New bidirectional stream opened");

                            // Read the payload size
                            let mut payload_size_buf = [0u8; 8];
                            if let Err(e) = recv.read_exact(&mut payload_size_buf).await {
                                error!("Failed to read piece size: {e}");
                                continue;
                            }
                            let payload_size = u64::from_be_bytes(payload_size_buf) as usize;

                            // Read signature payload and verify
                            // let mut signature_buf = [0u8; size_of::<HandshakePayload>()];
                            let mut signature_buf = vec![0u8; payload_size];
                            if let Err(e) = recv.read_exact(&mut signature_buf).await {
                                error!("Failed to read signature: {e}");
                                continue;
                            }
                            debug!("signature_buf: {:?}", signature_buf);
                            debug!("payload size: {:?}", payload_size);
                            let signature_payload =
                                match bincode::deserialize::<HandshakePayload>(&signature_buf) {
                                    Ok(data) => data,
                                    Err(err) => {
                                        error!(
                                            "Failed to deserialize payload from bytes: {:?}",
                                            err
                                        );
                                        continue;
                                    }
                                };
                            let verified = verify_signature(
                                &signature_payload.message.validator.account_id,
                                &signature_payload.signature,
                                &signature_payload.message,
                            );

                            let miner_base_neuron = state_clone
                                .clone()
                                .lock()
                                .await
                                .miner
                                .clone()
                                .lock()
                                .await
                                .neuron
                                .clone();
                            let validator_peer_id = if let Some(peer_id) = miner_base_neuron
                                .peer_node_uid
                                .get_by_right(&signature_payload.message.validator.uid)
                            {
                                peer_id
                            } else {
                                error!("Could not get validator peer ID");
                                continue;
                            };
                            let validator_info = if let Some(vali_info) = miner_base_neuron
                                .address_book
                                .clone()
                                .get(validator_peer_id)
                            {
                                vali_info.clone()
                            } else {
                                error!("Error while getting validator info");
                                continue;
                            };
                            let validator_hotkey = validator_info.neuron_info.hotkey.clone();

                            if !verified
                                || validator_hotkey
                                    != signature_payload.message.validator.account_id
                            {
                                error!(
                                    "Failed to verify signature from validator with uid {}",
                                    signature_payload.message.validator.uid
                                );
                                conn.close(
                                    VarInt::from_u32(401),
                                    "Signature verification failed".as_bytes(),
                                );
                                return;
                            }
                            debug!("Signature verification successful:");
                            debug!("{:?}", signature_payload);

                            // Read the piece size
                            let mut piece_size_buf = [0u8; 8];
                            if let Err(e) = recv.read_exact(&mut piece_size_buf).await {
                                error!("Failed to read piece size: {e}");
                                continue;
                            }
                            let piece_size = u64::from_be_bytes(piece_size_buf) as usize;
                            debug!("Received piece size: {piece_size} bytes");

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
                                        error!("Error reading piece: {e}");
                                        break;
                                    }
                                }
                            }

                            if bytes_read == 0 {
                                continue; // No data received
                            }

                            info!("Received piece of exactly {} bytes", bytes_read);

                            let hash_raw = blake3::hash(&piece);
                            let hash: String = hash_raw.to_hex().to_string();
                            let piece_key = libp2p::kad::RecordKey::new(&hash_raw.as_bytes());

                            // Add miner as a provider for the piece

                            match swarm::dht::StorbDHT::start_providing_piece(
                                dht_sender_clone.clone(),
                                piece_key.clone(),
                            )
                            .await
                            {
                                Ok(_) => debug!(
                                    "Added miner as provider for piece with key {:?}",
                                    piece_key
                                ),
                                Err(e) => {
                                    error!("Failed to add miner as provider for piece: {}", e)
                                }
                            }

                            if let Err(e) = send.write_all(hash_raw.as_bytes()).await {
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
                            object_store_clone
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

/// Run the miner
pub fn run(config: MinerConfig) {
    let _ = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(main(config));
}
