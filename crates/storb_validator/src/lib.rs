use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use axum::Router;
use constants::SYNTHETIC_CHALLENGE_FREQUENCY;
use tokio::{sync::RwLock, time};
use tracing::{debug, error, info};

use routes::{download_file, node_info, upload_file};
use validator::{Validator, ValidatorConfig};

mod constants;
mod download;
mod quic;
mod routes;
mod scoring;
mod signature;
mod upload;
mod utils;
pub mod validator;

const MAX_BODY_SIZE: usize = 10 * 1024 * 1024 * 1024; // 10 GiB

/// State maintained by the validator service
///
/// We derive Clone here to allow this state to be shared between request handlers,
/// as Axum requires state types to be cloneable to share them across requests.
#[derive(Clone)]
struct ValidatorState {
    pub validator: Arc<RwLock<Validator>>,
}

/// QUIC validator server that accepts file uploads, sends files to miner via QUIC,
/// and returns hashes. This server serves as an intermediary between HTTP clients
/// and the backing storage+processing miner. Below is an overview of how it works:
///
/// 1. Producer produces bytes
///    - a. Read collection of bytes from multipart form
///    - b. Fill up a shared buffer with that collection
///    - c. Signal that its done
/// 2. Consumer consumes bytes
///    - a. Reads a certain chunk of the collection bytes from shared buffer
///    - b. FECs it into pieces. A background thread is spawned to:
///        - distribute these pieces to a selected set of miners
///        - verify pieces are being stored
///        - update miner statistics
///
/// On success, returns Ok(()).
/// On failure, returns error with details of the failure.
pub async fn run_validator(config: ValidatorConfig) -> Result<()> {
    // Load or generate server certificate
    // let server_cert = ensure_certificate_exists().await?;

    // Clone config before first use to avoid move
    let config_clone = config.clone();
    let validator = Arc::new(RwLock::new(Validator::new(config_clone).await?));

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
                let mut guard = validator.write().await;
                match guard.sync().await {
                    Ok(_) => debug!("Sync ran successfully"),
                    Err(err) => error!("Failed to run sync: {err}"),
                }
            }
        }
    });

    // Spawn background synthetic challenge tasks
    let validator_clone = validator.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(SYNTHETIC_CHALLENGE_FREQUENCY));
        loop {
            // Interval tick MUST be called before the validator write, or else it will block
            // for in the initial loop-through
            interval.tick().await;
            let guard = validator_clone.write().await;
            info!("Running synthetic challenges");
            match guard.run_synthetic_challenges().await {
                Ok(_) => debug!("Synthetic challenges ran successfully"),
                Err(err) => error!("Synthetic challenges failed to run: {err}"),
            };
        }
    });

    // Spawn background backup task
    let validator_clone = validator.clone();
    tokio::spawn(async move {
        validator_clone
            .write()
            .await
            .scoring_system
            .write()
            .await
            .db
            .start_periodic_backup(config.neuron_config.neuron.sync_frequency)
            .await; // TODO: constant
    });

    let state = ValidatorState { validator };

    let app = Router::new()
        .route("/file", post(upload_file))
        .route("/file", get(download_file))
        .route("/info", get(node_info))
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], config.neuron_config.api_port));
    info!("Validator HTTP server listening on {}", addr);

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
    run_validator(config)
        .await
        .expect("Failed to run the validator")
}

/// Runs the main async runtime
pub fn run(config: ValidatorConfig) {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(main(config))
}
