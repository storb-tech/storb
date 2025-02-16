use std::collections::HashMap;

use anyhow::Result;
use axum::{
    extract::{Multipart, Query},
    http::{header::CONTENT_LENGTH, HeaderMap, StatusCode},
    response::IntoResponse,
};
use libp2p::kad::RecordKey;
use tracing::{error, info};

use crate::{upload::UploadProcessor, ValidatorState};
use base::swarm;

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

    let processor = UploadProcessor::new(&state)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Extract field and get bytes
    let field = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "No fields found".to_string()))?;

    if field.name() != Some("file") {
        return Err((StatusCode::BAD_REQUEST, "No file field found".to_string()));
    }

    let bytes = field
        .bytes()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Create pinned stream from bytes with explicit error type
    let stream = Box::pin(futures::stream::once(async move {
        Ok::<_, std::io::Error>(bytes)
    }));

    match processor
        .process_upload(stream, content_length, processor.validator_id)
        .await
    {
        Ok(_) => Ok((StatusCode::OK, "Upload successful".to_string())),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

#[utoipa::path(
    get,
    path = "/file",
    responses(
        (status = 200, description = "File downloaded successfully", body = String),
        (status = 500, description = "Internal server error during download", body = String)
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
    let key = RecordKey::new(&infohash.as_bytes().to_vec());
    info!("Downloading file with infohash: {:?}", &key);

    let dht_sender = state.validator.lock().await.neuron.command_sender.clone();
    let tracker_res = swarm::dht::StorbDHT::get_tracker_entry(dht_sender.clone(), key)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error getting tracker entry: {}", e),
            )
        })?;
    let tracker = tracker_res.ok_or((
        StatusCode::INTERNAL_SERVER_ERROR,
        "Tracker entry not found".to_string(),
    ))?;
    info!("Tracker hash: {:?}", tracker.infohash);
    let chunk_hashes = tracker.chunk_hashes;
    let mut pieces: Vec<swarm::models::PieceDHTValue> = Vec::new();
    for chunk_hash in chunk_hashes {
        // convert chunk_hash to a string
        let chunk_key = RecordKey::new(&chunk_hash);
        info!("RecordKey of chunk hash: {:?}", chunk_key);
        let chunk_res =
            match swarm::dht::StorbDHT::get_chunk_entry(dht_sender.clone(), chunk_key).await {
                Ok(chunk) => Some(chunk),
                Err(e) => {
                    error!("Error getting chunk entry: {}", e);
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Error getting chunk entry: {}", e),
                    ));
                }
            };

        let chunk_pieces_data: Vec<u8> = Vec::new();

        let chunk = match chunk_res {
            Some(chunk) => chunk,
            None => {
                error!("Chunk entry not found");
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Chunk entry not found".to_string(),
                ));
            }
        };
        let chunk = chunk.ok_or((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Chunk entry not found".to_string(),
        ))?;
        info!(
            "Found chunk hash: {:?} with {:?} pieces",
            &chunk.chunk_hash,
            &chunk.piece_hashes.len()
        );
        let piece_hashes = chunk.piece_hashes;
        for piece_hash in piece_hashes {
            let piece_key = RecordKey::new(&piece_hash);
            info!("RecordKey of piece hash: {:?}", &piece_key);
            let piece_res = swarm::dht::StorbDHT::get_piece_entry(dht_sender.clone(), piece_key)
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Error getting piece entry: {}", e),
                    )
                })?;
            let piece = match piece_res {
                Some(piece) => piece,
                None => {
                    error!("Piece entry not found");
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Piece entry not found".to_string(),
                    ));
                }
            };

            pieces.push(piece.clone());
            info!(
                "Found Piece hash: {:?} | Piece type {:?}",
                piece.piece_hash, piece.piece_type
            );
        }
    }
    Ok((StatusCode::OK, "Download successful".to_string()))
}
