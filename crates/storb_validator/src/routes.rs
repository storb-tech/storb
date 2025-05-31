use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use axum::extract::{Extension, Multipart, Query};
use axum::http::header::CONTENT_LENGTH;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{AppendHeaders, IntoResponse};
use base::swarm;
use libp2p::kad::RecordKey;
use tokio::sync::RwLock; // Changed from std::sync::RwLock
use tracing::{debug, error, info};

use crate::apikey::ApiKeyManager;
// TODO(restore): restore download processor
// use crate::download::DownloadProcessor;
use crate::upload::UploadProcessor;
use crate::ValidatorState;

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
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, String)> {
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
        .process_upload(stream, content_length, processor.validator_id)
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

// TODO(restore): restore download route
// #[utoipa::path(
//     get,
//     path = "/file",
//     responses(
//         (status = 200, description = "File downloaded successfully", body = String),
//         (status = 400, description = "Missing infohash parameter", body = String),
//         (status = 500, description = "Internal server error during download", body = String)
//     ),
//     params(
//         ("infohash" = String, Path, description = "The infohash of the file to retrieve."),
//     ),
//     tag = "Download"
// )]
// #[axum::debug_handler]
// pub async fn download_file(
//     state: axum::extract::State<ValidatorState>,
//     Extension(api_key_manager): Extension<Arc<RwLock<ApiKeyManager>>>,
//     headers: HeaderMap,
//     Query(params): Query<HashMap<String, String>>,
// ) -> Result<impl IntoResponse, (StatusCode, String)> {
//     let api_key = get_api_key(&headers)
//         .await
//         .ok_or_else(|| (StatusCode::UNAUTHORIZED, "API key required".to_string()))?;

//     let infohash = params.get("infohash").ok_or_else(|| {
//         (
//             StatusCode::BAD_REQUEST,
//             "Missing infohash parameter".to_string(),
//         )
//     })?;

//     let processor = DownloadProcessor::new(&state).await.map_err(|e| {
//         error!("Failed to initialise the download processor: {e}");
//         (
//             StatusCode::INTERNAL_SERVER_ERROR,
//             "An internal server error occurred".to_string(),
//         )
//     })?;

//     // change this
//     let headers = AppendHeaders([
//         // NOTE: the specific content type should be left to the clients downloading it
//         ("Content-Type", "application/octet-stream"),
//         (
//             "Content-Disposition",
//             "inline; filename=\"downloaded_file\"", // TODO: use file name that's stored
//         ),
//     ]);

//     // get tracker info
//     let key = RecordKey::new(&infohash.as_bytes().to_vec());
//     let tracker_res = swarm::dht::StorbDHT::get_tracker_entry(processor.dht_sender.clone(), key)
//         .await
//         .map_err(|e| {
//             error!("Error getting tracker entry: {}", e);
//             (
//                 StatusCode::INTERNAL_SERVER_ERROR,
//                 "Internal server error".to_string(),
//             )
//         })?;
//     let tracker = tracker_res.ok_or_else(|| {
//         error!("Tracker entry not found for infohash: {}", infohash);
//         (
//             StatusCode::INTERNAL_SERVER_ERROR,
//             "Internal server error".to_string(),
//         )
//     })?;

//     // get file size
//     let content_length = tracker.length;

//     // Check quota before processing
//     let key_manager = api_key_manager.read().await;
//     let has_quota = key_manager
//         .check_quota(&api_key, 0, content_length)
//         .await
//         .map_err(|e| {
//             error!("Failed to check API key quota: {}", e);
//             (
//                 StatusCode::INTERNAL_SERVER_ERROR,
//                 "Internal server error".to_string(),
//             )
//         })?;
//     if !has_quota {
//         return Err((StatusCode::FORBIDDEN, "Download quota exceeded".to_string()));
//     }
//     drop(key_manager);

//     // Process the download
//     match processor.process_download(infohash.clone()).await {
//         Ok(body) => {
//             // Update API key usage after successful download
//             api_key_manager
//                 .write()
//                 .await
//                 .update_usage(&api_key, 0, content_length)
//                 .await
//                 .map_err(|e| {
//                     error!("Failed to update API key usage: {}", e);
//                     (
//                         StatusCode::INTERNAL_SERVER_ERROR,
//                         "Internal server error".to_string(),
//                     )
//                 })?;

//             // Log API usage after successful download start
//             api_key_manager
//                 .write()
//                 .await
//                 .log_api_usage(&api_key, "/file", 0, content_length)
//                 .await
//                 .map_err(|e| {
//                     error!("Failed to log API usage: {}", e);
//                     (
//                         StatusCode::INTERNAL_SERVER_ERROR,
//                         "Internal server error".to_string(),
//                     )
//                 })?;

//             Ok((headers, body))
//         }
//         Err(e) => {
//             error!("The processor failed to process the upload: {:?}", e);
//             Err((
//                 StatusCode::INTERNAL_SERVER_ERROR,
//                 "An internal server error occurred".to_string(),
//             ))
//         }
//     }
// }

async fn get_api_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}
