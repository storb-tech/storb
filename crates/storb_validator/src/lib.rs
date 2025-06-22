#![allow(clippy::uninlined_format_args)]
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::DefaultBodyLimit;
use axum::middleware::from_fn;
use axum::routing::{delete, get, post};
use axum::{Extension, Router};
use base::constants::NEURON_SYNC_TIMEOUT;
use base::sync::Synchronizable;
use base::{LocalNodeInfo, NeuronError};
use constants::{METADATADB_SYNC_FREQUENCY, SYNTHETIC_CHALLENGE_FREQUENCY};
use dashmap::DashMap;
use middleware::{require_api_key, InfoApiRateLimiter};
use opentelemetry::global;
use opentelemetry_otlp::{MetricExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::Resource;
use routes::{download_file, node_info, upload_file};
use tokio::time::interval;
use tokio::{sync::RwLock, time};
use tracing::{debug, error, info};
use validator::{Validator, ValidatorConfig};

use crate::constants::NONCE_CLEANUP_FREQUENCY;
use crate::metadata::sync::sync_metadata_db;
use crate::routes::{delete_file, generate_nonce, get_crsqlite_changes};

pub mod apikey;
mod constants;
mod download;
mod metadata;
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
    pub local_node_info: LocalNodeInfo,
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
    let validator = Arc::new(Validator::new(config.clone()).await?);

    let neuron = validator.neuron.clone();

    let validator_for_sync = validator.clone();
    let validator_for_metadatadb = validator.clone();
    let validator_for_challenges = validator.clone();
    let validator_for_backup = validator.clone();
    let validator_for_nonce_cleanup = validator.clone();
    let sync_frequency = config.clone().neuron_config.neuron.sync_frequency;

    let sync_config = config.clone();

    let identifier_resource = Resource::builder()
        .with_attribute(opentelemetry::KeyValue::new(
            "service.name",
            config.otel_service_name,
        ))
        .build();
    let mut otel_headers: HashMap<String, String> = HashMap::new();
    otel_headers.insert("X-Api-Key".to_string(), config.otel_api_key.clone());
    let url = config.otel_endpoint.clone() + "metrics";
    let metrics_exporter = MetricExporter::builder()
        .with_http()
        .with_endpoint(url)
        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
        .with_headers(otel_headers)
        .build()
        .map_err(|e| {
            NeuronError::ConfigError(format!("Failed to build OTEL MetricExporter: {:?}", e))
        })?;

    let metrics_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metrics_exporter)
        .with_resource(identifier_resource)
        .build();

    global::set_meter_provider(metrics_provider.clone());

    let meter = global::meter("validator::metrics");
    let weights_counter = meter
        .f64_counter("miner.weight")
        .with_description("Current weight per miner")
        .with_unit("score")
        .build();

    info!("Set up OTEL metrics exporter");

    tokio::spawn(async move {
        let local_validator = validator_for_sync;
        let scoring_system = local_validator.scoring_system.clone();
        let neuron = neuron.clone();

        info!(
            "Starting validator sync task with frequency: {} seconds",
            sync_frequency
        );
        let mut interval = time::interval(Duration::from_secs(sync_frequency));
        loop {
            interval.tick().await;
            info!("Syncing validator");
            // Wrap the sync operation in a timeout
            match tokio::time::timeout(NEURON_SYNC_TIMEOUT, async {
                let start = std::time::Instant::now();
                let sync_result = neuron.write().await.sync_metagraph().await;
                (sync_result, start.elapsed())
            })
            .await
            {
                Ok((result, duration)) => match result {
                    Ok(uids) => {
                        info!("Sync completed in {:?}", duration);
                        let neuron_guard = neuron.read().await;
                        let neuron_count = neuron_guard.neurons.read().await.len();
                        scoring_system
                            .write()
                            .await
                            .update_scores(neuron_count, uids)
                            .await;

                        let ema_scores =
                            scoring_system.clone().read().await.state.ema_scores.clone();

                        match Validator::set_weights(
                            neuron_guard.clone(),
                            ema_scores,
                            sync_config.clone(),
                            weights_counter.clone(),
                        )
                        .await
                        {
                            Ok(_) => info!("Successfully set weights on chain"),
                            Err(e) => error!("Failed to set weights on chain: {}", e),
                        };
                    }
                    Err(err) => {
                        error!("Failed to sync metagraph: {}", err);
                    }
                },
                Err(_elapsed) => {
                    error!(
                        "Sync operation timed out after {} seconds",
                        NEURON_SYNC_TIMEOUT.as_secs_f32()
                    );
                    error!(
                        "Current stack trace:\n{:?}",
                        std::backtrace::Backtrace::capture()
                    );

                    // Additional debugging info
                    let guard = neuron.try_read();
                    match guard {
                        Ok(_) => info!("Neuron read lock is available"),
                        Err(_) => error!("Neuron read lock is held - possible deadlock"),
                    };

                    let scoring_guard = scoring_system.try_write();
                    match scoring_guard {
                        Ok(_) => info!("Scoring system write lock is available"),
                        Err(_) => error!("Scoring system write lock is held - possible deadlock"),
                    };
                }
            }
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

    // Spawn background metadata database syncing task with metadata::sync::sync_metadata_db
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(METADATADB_SYNC_FREQUENCY));
        loop {
            interval.tick().await;
            info!("Starting MetadataDB sync task");
            match sync_metadata_db(validator_for_metadatadb.clone()).await {
                Ok(_) => info!("Metadata DB sync completed successfully"),
                Err(err) => error!("Metadata DB sync failed: {}", err),
            }
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

    // Add a periodic cleanup task for expired nonces
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(NONCE_CLEANUP_FREQUENCY)); // Every hour

        loop {
            interval.tick().await;
            info!("Running nonce cleanup task");
            // get the command sender from the validator
            let command_sender = validator_for_nonce_cleanup.metadatadb_sender.clone();
            match metadata::db::MetadataDB::cleanup_expired_nonces(&command_sender, 1).await {
                Ok(deleted_count) => {
                    if deleted_count > 0 {
                        info!("Cleaned up {} expired nonces", deleted_count);
                    }
                }
                Err(e) => {
                    error!("Failed to cleanup expired nonces: {:?}", e);
                }
            }
        }
    });

    let db_path = config.api_keys_db.clone();
    // Initialize API key manager
    let api_key_manager = Arc::new(RwLock::new(apikey::ApiKeyManager::new(db_path)?));
    let info_api_rate_limit_state: InfoApiRateLimiter = Arc::new(DashMap::new());

    // Create protected routes that require API key
    let protected_routes = Router::new()
        .route("/file", post(upload_file))
        .route("/file", get(download_file))
        .route("/file", delete(delete_file))
        .route("/nonce", post(generate_nonce))
        .route_layer(from_fn(require_api_key));

    // Create main router with all routes
    let app = Router::new()
        .merge(protected_routes) // Add protected routes
        .route(
            "/info",
            get(node_info).route_layer(from_fn(middleware::info_api_rate_limit_middleware)),
        )
        .layer(Extension(info_api_rate_limit_state))
        .layer(Extension(api_key_manager))
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
        .route(
            "/db_changes",
            get(get_crsqlite_changes), // TODO(syncing): Add db change rate limiting middleware?
        )
        .with_state(ValidatorState {
            validator: validator.clone(),
            local_node_info: validator.neuron.read().await.local_node_info.clone(),
        });

    // Start server
    let addr = SocketAddr::from(([0, 0, 0, 0], config.neuron_config.api_port));
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
    if std::env::var("PANIC_ABORT")
        .map(|v| v == "1")
        .unwrap_or(false)
    {
        std::panic::set_hook(Box::new(|info| {
            eprintln!("One of the threads panicked {}", info);
            std::process::abort();
        }));
    }
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
