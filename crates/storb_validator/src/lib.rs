use std::sync::Arc;

use anyhow;
use axum::{
    extract::{DefaultBodyLimit, Multipart},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Router,
};
// use capnp::{message::ReaderOptions, serialize};
use neuron::NeuronConfig;
use tarpc::context;
use tarpc::serde_transport::tcp;
use tokio_serde::formats::Json;
use tracing::{error, info};

/// Example config
#[derive(Clone)]
pub struct ValidatorConfig {
    pub neuron_config: NeuronConfig,
}

/// Our main validator type
#[derive(Clone)]
struct Validator {
    config: ValidatorConfig,
}

impl Validator {
    pub fn new(config: ValidatorConfig) -> Self {
        Validator { config }
    }
}

#[axum::debug_handler]
async fn upload_file(mut multipart: Multipart) -> Result<impl IntoResponse, StatusCode> {
    while let Some(field) = multipart.next_field().await.map_err(|e| {
        error!("Error reading field: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })? {
        // First collect all the data we need
        let filename = match field.file_name() {
            Some(name) => name.to_string(),
            None => "unknown".to_string(),
        };
        let data = field.bytes().await.map_err(|e| {
            error!("Error reading field bytes: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        // // Create the request bytes in a separate function to avoid Send issues
        // let request_bytes = serialize_upload_request(&filename, &data).map_err(|_| {
        //     error!("Failed to serialize request");
        //     StatusCode::INTERNAL_SERVER_ERROR
        // })?;

        // Process the request directly instead of spawning
    }

    Ok((StatusCode::OK, "File uploaded"))
}

async fn main() {
    info!("Running the Storb validator...");

    let validator = Validator::new(ValidatorConfig {
        neuron_config: NeuronConfig {
            netuid: 0,
            wallet_name: "default".to_string(),
            hotkey_name: "default".to_string(),
        },
    });

    let app = Router::new()
        .route("/store", post(upload_file))
        .layer(DefaultBodyLimit::max(u128::MAX as usize));

    let addr = "127.0.0.1:5000";
    println!("Validator HTTP server listening on {addr}");

    let result = tokio::net::TcpListener::bind(addr).await;
    match result {
        Ok(listener) => {
            if let Err(e) = axum::serve(listener, app.into_make_service()).await {
                error!("Server error: {e}");
            }
        }
        Err(e) => error!("Failed to bind to {addr}: {e}"),
    }
}

pub fn run() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(main())
}
