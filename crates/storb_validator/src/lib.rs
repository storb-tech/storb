mod quic;
mod routes;
mod signature;
pub mod validator;

use anyhow::{Context, Result};
use axum::{extract::DefaultBodyLimit, routing::post, Router};
use neuron::dht::StorbDHT;
use routes::upload_file;
use rustls::{self};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{sync::Mutex, time};
use tracing::info;
use validator::{Validator, ValidatorConfig};

const MAX_BODY_SIZE: usize = 10 * 1024 * 1024 * 1024; // 10GiB

/// State maintained by the validator service
///
/// We derive Clone here to allow this state to be shared between request handlers,
/// as Axum requires state types to be cloneable to share them across requests.
#[derive(Clone)]
struct ValidatorState {
    validator: Arc<Mutex<Validator>>,
}

/// QUIC validator server that accepts file uploads, sends files to miner via QUIC, and returns hashes.
/// This server serves as an intermediary between HTTP clients and the backing storage+processing miner.
/// Below is an overview of how it works:
/// 1. Producer produces bytes
///  1a. Read collection of bytes from multipart form
///  1b. Fill up a shared buffer with that collection
///  1c. Signal that its done
/// 2. Consumer consumes bytes
///  2a. Reads a certain chunk of the collection bytes from shared buffer
///  2b. FECs it into pieces. A background thread is spawned
///      to:
///      - distribute these pieces to a selected set of miners
///      - verify pieces are being stored
///      - update miner statistics
///
/// On success returns Ok(()).
/// On failure returns error with details of the failure.
pub async fn run_validator(config: ValidatorConfig) -> Result<()> {
    // Load or generate server certificate
    // let server_cert = ensure_certificate_exists().await?;

    // Clone config before first use to avoid move
    let config_clone = config.clone();
    let validator = Arc::new(Mutex::new(Validator::new(config_clone).await?));

    // Create weak reference for sync task
    let sync_validator = Arc::downgrade(&validator);

    // Spawn background sync task
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(
            config.neuron_config.neuron.sync_frequency,
        ));
        loop {
            interval.tick().await;
            if let Some(validator) = sync_validator.upgrade() {
                let mut guard = validator.lock().await;
                guard.sync().await;
            }
        }
    });

    let state = ValidatorState { validator };

    let app = Router::new()
        .route("/upload", post(upload_file))
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], config.neuron_config.api_port));
    info!("Validator HTTP server listening on {}", addr);

    // TODO: Add error handling for the Swarm initialization.
    let dht = StorbDHT::new().unwrap();
    dht.run().await.unwrap();

    axum::serve(
        tokio::net::TcpListener::bind(addr)
            .await
            .context("Failed to bind HTTP server")?,
        app,
    )
    .await
    .context("HTTP server failed")?;

    Ok(())
}

/// Main entry point for the validator service
async fn main(config: ValidatorConfig) {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");
    run_validator(config).await.unwrap()
}

/// Runs the main async runtime
pub fn run(config: ValidatorConfig) {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(main(config))
}
