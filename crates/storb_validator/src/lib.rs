use std::sync::Arc;

use anyhow;
use axum::{
    extract::{DefaultBodyLimit, Multipart},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Router,
};
use capnp::{message::ReaderOptions, serialize};
use neuron::NeuronConfig;
use storb_miner::MinerServiceClient;
use storb_schemas::schemas::message_capnp;
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

    async fn handle_upload(&self, mut multipart: Multipart) -> impl IntoResponse {
        if let Ok(field) = multipart.next_field().await {
            if let Some(field) = field {
                let filename = field.file_name().unwrap_or("unknown").to_string();
                let data = field.bytes().await.unwrap_or_default();

                // Create Cap'n Proto request
                let mut request = capnp::message::Builder::new_default();
                let mut file_upload =
                    request.init_root::<message_capnp::file_upload_request::Builder>();
                file_upload.set_filename(&filename);
                file_upload.set_data(&data);

                // Serialize request
                let mut request_bytes = Vec::new();
                capnp::serialize::write_message(&mut request_bytes, &request).unwrap_or_else(|e| {
                    error!("Error serializing request: {e}");
                });
                match process_request(request_bytes).await {
                    Ok(response) => response,
                    Err(e) => {
                        error!("Error processing request: {e}");
                        Vec::new()
                    }
                }
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
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

        // Create the request bytes in a separate function to avoid Send issues
        let request_bytes = serialize_upload_request(&filename, &data).map_err(|_| {
            error!("Failed to serialize request");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        // Process the request directly instead of spawning
        match process_request(request_bytes).await {
            Ok(_) => info!("Request processed successfully"),
            Err(e) => {
                error!("Error processing request: {e}");
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    Ok((StatusCode::OK, "File uploaded"))
}

// Helper function to handle Cap'n Proto serialization
fn serialize_upload_request(filename: &str, data: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    let mut file_upload = message.init_root::<message_capnp::file_upload_request::Builder>();
    file_upload.set_filename(filename);
    file_upload.set_data(data);

    let mut bytes = Vec::new();
    capnp::serialize::write_message(&mut bytes, &message)?;
    Ok(bytes)
}

async fn process_request(request_bytes: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    // Create transport and client in a separate function
    let client = create_miner_client().await?;

    // Deserialize the FileUploadRequest
    let request_reader = serialize::read_message(
        &mut &request_bytes[..],
        ReaderOptions {
            traversal_limit_in_words: Some(1_000_000_000), // Increased from default of 64M words
            nesting_limit: 64,
        },
    )?;
    let request = request_reader.get_root::<message_capnp::file_upload_request::Reader>()?;

    let _filename = request.get_filename()?.to_string();
    let data = request.get_data()?;

    // Split data into chunks of 1MB
    const CHUNK_SIZE: usize = 1024 * 1024;
    let data_len = data.len();
    let mut responses = Vec::new();

    for chunk_start in (0..data_len).step_by(CHUNK_SIZE) {
        let chunk_end = std::cmp::min(chunk_start + CHUNK_SIZE, data_len);
        let chunk = &data[chunk_start..chunk_end];

        let miner_request_bytes = serialize_miner_request(chunk)?;

        // Send chunk to miner
        let miner_response_bytes = client
            .store_piece(context::current(), miner_request_bytes)
            .await?;

        // Deserialize and log the piece ID from response
        let response_reader =
            serialize::read_message(&mut &miner_response_bytes[..], ReaderOptions::default())?;
        let response = response_reader.get_root::<message_capnp::miner_store_response::Reader>()?;
        info!("Stored piece with ID: {:?}", response.get_piece_id()?);

        responses.push(miner_response_bytes);
    }

    create_response(responses.len())
}

// Helper function to create response
fn create_response(num_pieces: usize) -> anyhow::Result<Vec<u8>> {
    let mut response_builder =
        capnp::message::TypedBuilder::<message_capnp::file_upload_response::Owned>::new_default();
    {
        let mut root = response_builder.init_root();
        root.set_success(true);
        root.set_message(
            format!("File split into {} pieces and sent to miner", num_pieces).as_str(),
        );
        root.set_file_id("miner_stored");
    }

    let mut response_bytes = Vec::new();
    serialize::write_message(&mut response_bytes, &response_builder.into_inner())?;
    Ok(response_bytes)
}

async fn create_miner_client() -> anyhow::Result<MinerServiceClient> {
    let transport = tcp::connect("127.0.0.1:9003", Json::default).await?;
    Ok(MinerServiceClient::new(tarpc::client::Config::default(), transport).spawn())
}

// Helper function to serialize miner request
fn serialize_miner_request(chunk: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut miner_request = capnp::message::Builder::new_default();
    let mut miner_store = miner_request.init_root::<message_capnp::miner_store_request::Builder>();
    miner_store.set_data(chunk);

    let mut miner_request_bytes = Vec::new();
    capnp::serialize::write_message(&mut miner_request_bytes, &miner_request)?;
    Ok(miner_request_bytes)
}

fn handler() -> &'static str {
    "Hello, World!"
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

    // I am doing it
    let app = Router::new()
        .route("/store", post(upload_file))
        .layer(DefaultBodyLimit::max(u128::MAX as usize));

    // okay run

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
