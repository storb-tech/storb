use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use axum::extract::{Extension, Multipart, Query};
use axum::http::header::CONTENT_LENGTH;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{AppendHeaders, IntoResponse};
use base::piece::InfoHash;
use crabtensor::sign::KeypairSignature;
use crabtensor::AccountId;
use subxt::ext::sp_core::crypto::Ss58Codec;
use subxt::ext::sp_core::ByteArray;
use subxt::ext::sp_runtime::AccountId32;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

use crate::apikey::ApiKeyManager;
use crate::download::DownloadProcessor;
use crate::metadata;
use crate::upload::UploadProcessor;
use crate::ValidatorState;

// TODO: add route for deleting a file

/// Router function to get information on a given node
#[utoipa::path(
    get,
    path = "/info",
    responses(
        (status = 200, description = "Successfully got node info", body = String),
        (status = 500, description = "Internal server error", body = String)
    ),
    tag = "Info"
)]
pub async fn node_info(
    state: axum::extract::State<ValidatorState>,
) -> Result<impl IntoResponse, (StatusCode, Vec<u8>)> {
    info!("Got node info req");
    let local_node_info = state.local_node_info.clone();
    let serialized_local_node_info = bincode::serialize(&local_node_info).map_err(|e| {
        error!("Error while deserialising local node info: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            bincode::serialize("An internal server error occurred").unwrap_or_default(),
        )
    })?;

    Ok((StatusCode::OK, serialized_local_node_info))
}

/// Router function to get crsqlite changes for syncing
#[utoipa::path(
    get,
    path = "/db_changes",
    responses(
        (status = 200, description = "Successfully got crsqlite changes", body = String),
        (status = 500, description = "Internal server error", body = String)
    ),
    tag = "DBChanges"
)]
pub async fn get_crsqlite_changes(
    state: axum::extract::State<ValidatorState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // TODO(syncing): Add verification step to check if request is from a validator?
    debug!("Got crsqlite changes request");
    let min_db_version = params
        .get("min_db_version")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    let site_id_disclude = params
        .get("site_id_disclude")
        .map(|s| hex::decode(s).unwrap_or_default())
        .unwrap_or_default();

    debug!(
        "Getting crsqlite changes with min_db_version: {}, site_id_disclude: {}",
        min_db_version,
        hex::encode(&site_id_disclude)
    );

    let command_sender = state.validator.metadatadb_sender.clone();
    let changes = metadata::db::MetadataDB::get_crsqlite_changes(
        &command_sender,
        min_db_version,
        site_id_disclude,
    )
    .await
    .map_err(|e| {
        error!("Failed to get crsqlite changes: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error".to_string(),
        )
    })?;

    // serialize the changes
    let changes = bincode::serialize(&changes).map_err(|e| {
        error!("Failed to serialize crsqlite changes: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to serialize changes".to_string(),
        )
    })?;

    Ok((StatusCode::OK, changes))
}

// route for generating a nonce for a given account_id
#[utoipa::path(
    post,
    path = "/nonce",
    responses(
        (status = 200, description = "Successfully got nonce", body = String),
        (status = 500, description = "Internal server error", body = String)
    ),
    params(
        ("account_id" = String, Query, description = "The account ID to generate a nonce for."),
    ),
    tag = "Nonce"
)]
#[axum::debug_handler]
pub async fn generate_nonce(
    state: axum::extract::State<ValidatorState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let account_id = params.get("account_id").ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "Missing account_id parameter".to_string(),
        )
    })?;
    debug!("Generating nonce for account_id: {}", account_id);
    let metadatadb_sender = state.validator.metadatadb_sender.clone();
    // create an AccountId from the ss58 string account_id
    let acc_id: AccountId = match AccountId32::from_string(account_id) {
        Ok(acc_id) => acc_id.into(),
        Err(e) => {
            error!("Failed to parse account_id: {}: {}", account_id, e);
            return Err((
                StatusCode::BAD_REQUEST,
                "Invalid account_id format".to_string(),
            ));
        }
    };

    let nonce = metadata::db::MetadataDB::generate_nonce(&metadatadb_sender, acc_id)
        .await
        .map_err(|e| {
            error!("Failed to generate nonce: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_string(),
            )
        })?;
    let nonce_str = hex::encode(nonce);
    debug!(
        "Generated nonce: {} for account id: {}",
        nonce_str, account_id
    );
    Ok((StatusCode::OK, nonce_str))
}

#[utoipa::path(
    post,
    path = "/file",
    responses(
        (status = 200, description = "File uploaded successfully", body = String),
        (status = 400, description = "Bad request - invalid file or missing content length", body = String),
        (status = 500, description = "Internal server error during upload", body = String)
    ),
    tag = "Upload"
)]
#[axum::debug_handler]
pub async fn upload_file(
    state: axum::extract::State<ValidatorState>,
    Extension(api_key_manager): Extension<Arc<RwLock<ApiKeyManager>>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let sig_str = params.get("signature").ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "Missing signature parameter".to_string(),
        )
    })?;

    debug!("Got upload request with signature: {}", sig_str);

    // let slice = bytes.as_slice();
    // KeypairSignature::from_slice(data);
    let signature = match hex::decode(sig_str) {
        // get from slice if OK
        Ok(sig) => KeypairSignature::from_slice(sig.as_slice()).map_err(|e| {
            error!("Failed to parse signature: {}: {:?}", sig_str, e);
            (
                StatusCode::BAD_REQUEST,
                "Invalid signature format".to_string(),
            )
        })?,
        Err(e) => {
            error!("Failed to decode signature hex: {}: {}", sig_str, e);
            return Err((
                StatusCode::BAD_REQUEST,
                "Invalid signature format".to_string(),
            ));
        }
    };

    let account_id_str = params.get("account_id").ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "Missing account_id parameter".to_string(),
        )
    })?;

    let account_id: AccountId = match AccountId32::from_string(account_id_str) {
        Ok(acc_id) => acc_id.into(),
        Err(e) => {
            error!("Failed to parse account_id: {}: {}", account_id_str, e);
            return Err((
                StatusCode::BAD_REQUEST,
                "Invalid account_id format".to_string(),
            ));
        }
    };

    let content_length = headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "Content-Length header is required".to_string(),
            )
        })?;

    info!("Content-Length header: {} bytes", content_length);

    // Get API key and update usage
    let api_key = get_api_key(&headers)
        .await
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "API key required".to_string()))?;

    debug!("Got API key of: {api_key}");

    // Check quota before processing
    let key_manager = api_key_manager.read().await;
    debug!("Got key manager");
    let has_quota = key_manager
        .check_quota(&api_key, content_length, 0)
        .await
        .map_err(|e| {
            error!("Failed to check API key quota: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_string(),
            )
        })?;
    debug!("Checked key manager quota");

    if !has_quota {
        return Err((StatusCode::FORBIDDEN, "Upload quota exceeded".to_string()));
    }
    drop(key_manager);
    debug!("Dropped key manager");

    let processor = UploadProcessor::new(&state).await.map_err(|e| {
        error!("Failed to initialise the upload processor: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "An internal server error occurred".to_string(),
        )
    })?;
    debug!("Get processor for uploading");

    // Extract field and get bytes
    let field = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "No fields found".to_string()))?;
    debug!("Got field from multipart");

    if field.name() != Some("file") {
        return Err((StatusCode::BAD_REQUEST, "No file field found".to_string()));
    }

    // get filename
    let filename = field
        .file_name()
        .map(|name| name.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    debug!("Got filename: {filename}");

    let bytes = field.bytes().await.map_err(|e| {
        error!("Could not get bytes from the file field: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "An internal server error occurred".to_string(),
        )
    })?;
    debug!("Got bytes from file field");

    // Create pinned stream from bytes with explicit error type
    let stream = Box::pin(futures::stream::once(async move {
        Ok::<_, std::io::Error>(bytes)
    }));

    match processor
        .process_upload(stream, content_length, filename, account_id, signature)
        .await
    {
        Ok(infohash) => {
            // Update API key usage after successful upload
            // Log API usage after successful upload
            api_key_manager
                .write()
                .await
                .log_api_usage(&api_key, "/file", content_length, 0)
                .await
                .map_err(|e| {
                    error!("Failed to log API usage: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Internal server error".to_string(),
                    )
                })?;

            api_key_manager
                .write()
                .await // Changed from expect()
                .update_usage(&api_key, content_length, 0)
                .await
                .map_err(|e| {
                    error!("Failed to update API key usage: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Internal server error".to_string(),
                    )
                })?;

            Ok((StatusCode::OK, infohash))
        }
        Err(e) => {
            error!("The processor failed to process the upload: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "An internal server error occurred".to_string(),
            ))
        }
    }
}

#[utoipa::path(
    get,
    path = "/file",
    responses(
        (status = 200, description = "File downloaded successfully", body = String),
        (status = 400, description = "Missing infohash parameter", body = String),
        (status = 500, description = "Internal server error during download", body = String)
    ),
    params(
        ("infohash" = String, Path, description = "The infohash of the file to retrieve."),
    ),
    tag = "Download"
)]
#[axum::debug_handler]
pub async fn download_file(
    state: axum::extract::State<ValidatorState>,
    Extension(api_key_manager): Extension<Arc<RwLock<ApiKeyManager>>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let api_key = get_api_key(&headers)
        .await
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "API key required".to_string()))?;

    let infohash = params.get("infohash").ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "Missing infohash parameter".to_string(),
        )
    })?;

    let processor = DownloadProcessor::new(&state).await.map_err(|e| {
        error!("Failed to initialise the download processor: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "An internal server error occurred".to_string(),
        )
    })?;

    // change this
    let headers = AppendHeaders([
        // NOTE: the specific content type should be left to the clients downloading it
        ("Content-Type", "application/octet-stream"),
        (
            "Content-Disposition",
            "inline; filename=\"downloaded_file\"", // TODO: use file name that's stored
        ),
    ]);

    // get tracker info
    let infohash_bytes: InfoHash = match hex::decode(infohash) {
        Ok(bytes) if bytes.len() == 32 => bytes.try_into().unwrap(),
        _ => {
            error!("Invalid infohash format: {}", infohash);
            return Err((
                StatusCode::BAD_REQUEST,
                "Invalid infohash format".to_string(),
            ));
        }
    };

    let tracker = metadata::db::MetadataDB::get_infohash(
        &processor.metadatadb_sender.clone(),
        infohash_bytes,
    )
    .await
    .map_err(|e| {
        error!("Failed to get infohash from the database: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error".to_string(),
        )
    })?;

    // get file size
    let content_length = tracker.length;

    // Check quota before processing
    let key_manager = api_key_manager.read().await;
    let has_quota = key_manager
        .check_quota(&api_key, 0, content_length)
        .await
        .map_err(|e| {
            error!("Failed to check API key quota: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_string(),
            )
        })?;
    if !has_quota {
        return Err((StatusCode::FORBIDDEN, "Download quota exceeded".to_string()));
    }
    drop(key_manager);

    // Process the download
    match processor.process_download(tracker).await {
        Ok(body) => {
            // Update API key usage after successful download
            api_key_manager
                .write()
                .await
                .update_usage(&api_key, 0, content_length)
                .await
                .map_err(|e| {
                    error!("Failed to update API key usage: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Internal server error".to_string(),
                    )
                })?;

            // Log API usage after successful download start
            api_key_manager
                .write()
                .await
                .log_api_usage(&api_key, "/file", 0, content_length)
                .await
                .map_err(|e| {
                    error!("Failed to log API usage: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Internal server error".to_string(),
                    )
                })?;

            Ok((headers, body))
        }
        Err(e) => {
            error!("The processor failed to process the upload: {:?}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "An internal server error occurred".to_string(),
            ))
        }
    }
}

async fn get_api_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}
