use std::collections::HashMap;

use anyhow::Result;
use axum::extract::{Multipart, Query};
use axum::http::header::CONTENT_LENGTH;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{AppendHeaders, IntoResponse};
use tracing::{error, info};

use crate::download::DownloadProcessor;
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
    let neuron = state.validator.read().await.neuron.clone();
    let serialized_local_node_info = bincode::serialize(&neuron.local_node_info).map_err(|e| {
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

    let processor = UploadProcessor::new(&state).await.map_err(|e| {
        error!("Failed to initialise the upload processor: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "An internal server error occurred".to_string(),
        )
    })?;

    // Extract field and get bytes
    let field = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "No fields found".to_string()))?;

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

    // Create pinned stream from bytes with explicit error type
    let stream = Box::pin(futures::stream::once(async move {
        Ok::<_, std::io::Error>(bytes)
    }));

    match processor
        .process_upload(stream, content_length, processor.validator_id)
        .await
    {
        Ok(infohash) => Ok((StatusCode::OK, infohash)),
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
    _headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
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

    match processor.process_download(infohash.clone()).await {
        Ok(body) => Ok((headers, body)),
        Err(e) => {
            error!("The processor failed to process the upload: {:?}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "An internal server error occurred".to_string(),
            ))
        }
    }
}
