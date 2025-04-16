use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::DefaultBodyLimit;
use axum::middleware::{from_fn, from_fn_with_state};
use axum::routing::{get, post};
use axum::{Extension, Router};
use base::sync::Synchronizable;
use constants::SYNTHETIC_CHALLENGE_FREQUENCY;
use dashmap::DashMap;
use middleware::{require_api_key, InfoApiRateLimiter};
use routes::{download_file, node_info, upload_file};
use tokio::time::interval;
use tokio::{sync::RwLock, time};
use tracing::{debug, error, info};
use validator::{Validator, ValidatorConfig};

pub mod apikey;
mod constants;
mod download;
mod middleware;
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
    pub validator: Arc<Validator>,
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
    // TODO: Load or generate server certificate
    // let server_cert = ensure_certificate_exists().await?;

    let validator = Arc::new(Validator::new(config.clone()).await?);

    let neuron = validator.neuron.clone();

    let validator_for_sync = validator.clone();
    let validator_for_challenges = validator.clone();
    let validator_for_backup = validator.clone();
    let sync_frequency = config.clone().neuron_config.neuron.sync_frequency;

    let sync_config = config.clone();

    tokio::spawn(async move {
        let local_validator = validator_for_sync;
        let scoring_system = local_validator.scoring_system.clone();
        let neuron = neuron.clone();

        let mut interval = time::interval(Duration::from_secs(sync_frequency));
        loop {
            interval.tick().await;
            info!("Syncing validator");
            let uids_to_update = match neuron.write().await.sync_metagraph().await {
                Ok(res) => res,
                Err(err) => {
                    error!("Failed to sync metagraph: {err}");
                    continue;
                }
            };
            let neuron_count = neuron.read().await.neurons.read().await.len();
            scoring_system
                .write()
                .await
                .update_scores(neuron_count, uids_to_update)
                .await;

            let ema_scores = scoring_system.clone().read().await.state.ema_scores.clone();

            match Validator::set_weights(
                neuron.read().await.clone(),
                ema_scores,
                sync_config.clone(),
            )
            .await
            {
                Ok(_) => info!("Successfully set weights on chain"),
                Err(e) => error!("Failed to set weights on chain: {}", e),
            };

            info!("Done syncing validator");
        }
    });

    // Spawn background synthetic challenge tasks
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(SYNTHETIC_CHALLENGE_FREQUENCY));
        loop {
            // Interval tick MUST be called before the validator write, or else it will block
            // for in the initial loop-through
            interval.tick().await;
            info!("Running synthetic challenges");
            match validator_for_challenges.run_synthetic_challenges().await {
                Ok(_) => debug!("Synthetic challenges ran successfully"),
                Err(err) => error!("Synthetic challenges failed to run: {err}"),
            };
        }
    });

    let vali_clone = validator_for_backup.clone();
    // Spawn background backup task
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(sync_frequency));
        loop {
            interval.tick().await;
            vali_clone
                .scoring_system
                .write()
                .await
                .db
                .run_backup()
                .await; // TODO: constant
        }
    });

    let db_path = config.clone().api_keys_db.clone();
    // Initialize API key manager
    let api_key_manager = Arc::new(RwLock::new(apikey::ApiKeyManager::new(db_path)?));
    let info_api_rate_limit_state: InfoApiRateLimiter = Arc::new(DashMap::new());

    // Create protected routes that require API key
    let protected_routes = Router::new()
        .route("/file", post(upload_file))
        .route("/file", get(download_file))
        .route_layer(from_fn(require_api_key));

    // Create main router with all routes
    let app = Router::new()
        .merge(protected_routes) // Add protected routes
        .route(
            "/info",
            get(node_info).route_layer(from_fn(middleware::info_api_rate_limit_middleware)),
        )
        // Public route
        .layer(Extension(info_api_rate_limit_state))
        .layer(Extension(api_key_manager))
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
        .with_state(ValidatorState {
            validator: validator.clone(),
        });

    // Start server
    let addr = SocketAddr::from(([0, 0, 0, 0], config.clone().neuron_config.api_port));
    info!("Validator HTTP server listening on {}", addr);

    axum::serve(
        tokio::net::TcpListener::bind(addr)
            .await
            .context("Failed to bind HTTP server")?,
        app.into_make_service_with_connect_info::<SocketAddr>(),
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
