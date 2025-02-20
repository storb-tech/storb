use anyhow::Result;
use axum::{
    extract::{Multipart, Query},
    http::{header::CONTENT_LENGTH, HeaderMap, StatusCode},
    response::IntoResponse,
};
use libp2p::kad::RecordKey;
use std::collections::HashMap;
use tracing::{error, info};

use crate::{upload::UploadProcessor, ValidatorState};
use base::swarm;

/// Router function to get information on a given node
pub async fn node_info(
    state: axum::extract::State<ValidatorState>,
) -> Result<impl IntoResponse, (StatusCode, Vec<u8>)> {
    info!("Got node info req");
    let neuron = state.validator.read().await.neuron.clone();
    let serialized_local_node_info = bincode::serialize(&neuron.local_node_info).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            bincode::serialize(&format!("Failed to serialize local node info: {}", e))
                .unwrap_or_default(),
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
        Ok(infohash) => Ok((StatusCode::OK, infohash)),
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

    let dht_sender = state.validator.read().await.neuron.command_sender.clone();
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
    let mut piece_infos: Vec<swarm::models::PieceDHTValue> = Vec::new();
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

        let _chunk_pieces_data: Vec<u8> = Vec::new();

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
            let piece_res =
                swarm::dht::StorbDHT::get_piece_entry(dht_sender.clone(), piece_key.clone())
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Error getting piece entry: {}", e),
                        )
                    })?;
            let piece_info = match piece_res {
                Some(piece_entry) => piece_entry,
                None => {
                    error!("Piece entry not found");
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Piece entry not found".to_string(),
                    ));
                }
            };
            piece_infos.push(piece_info.clone());
            info!(
                "Looking for piece providers for {:?}",
                &piece_info.piece_hash
            );
            let piece_providers = swarm::dht::StorbDHT::get_piece_providers(
                dht_sender.clone(),
                piece_info.piece_hash.clone(),
            )
            .await
            .map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "No Miner Found".to_string(),
                )
            })
            .unwrap();
            if piece_providers.is_empty() {
                error!("No providers found for piece");
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "No providers found for piece".to_string(),
                ));
            }
            info!(
                "Found Piece hash: {:?} | Piece type {:?} | Providers: {:?}",
                piece_info.piece_hash, piece_info.piece_type, piece_providers
            );

            for provider in piece_providers {
                info!("Provider: {:?}", provider);
            }
        }
    }
    Ok((StatusCode::OK, "Download successful".to_string()))
}
