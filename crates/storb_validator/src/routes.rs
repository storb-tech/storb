use std::collections::HashMap;

use crate::{upload::UploadProcessor, ValidatorState};
use anyhow::Result;
use axum::extract::{Multipart, Query};
use axum::http::{header::CONTENT_LENGTH, HeaderMap, StatusCode};
use axum::response::{AppendHeaders, IntoResponse};
use base::constants::CLIENT_TIMEOUT;
use base::swarm;
use futures::stream::StreamExt;
use libp2p::kad::RecordKey;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

const MPSC_BUFFER_SIZE: usize = 10;

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
            bincode::serialize("Failed to serialize local node info").unwrap_or_default(),
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

    let (tx, rx) = mpsc::channel::<Vec<u8>>(MPSC_BUFFER_SIZE);

    tokio::spawn(async move {
        for chunk_hash in chunk_hashes.clone() {
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
                            "An internal server error occurred".to_string(),
                        ));
                    }
                };

            let chunk = match chunk_res {
                Some(Some(chunk)) => chunk,
                Some(None) | None => {
                    error!("Chunk entry not found");
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "An internal server error occurred".to_string(),
                    ));
                }
            };
            info!(
                "Found chunk hash: {:?} with {:?} pieces",
                &chunk.chunk_hash,
                &chunk.piece_hashes.len()
            );

            let piece_hashes = chunk.piece_hashes;
            let mut piece_data: Vec<base::piece::Piece> = Vec::with_capacity(piece_hashes.len());

            for piece_hash in piece_hashes {
                let piece_key = RecordKey::new(&piece_hash);
                info!("RecordKey of piece hash: {:?}", &piece_key);
                let piece_res =
                    swarm::dht::StorbDHT::get_piece_entry(dht_sender.clone(), piece_key.clone())
                        .await
                        .map_err(|e| {
                            error!("Error getting piece entry: {}", e);
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "An internal server error occurred".to_string(),
                            )
                        })?;
                let piece_info = match piece_res {
                    Some(piece_entry) => piece_entry,
                    None => {
                        error!("Piece entry not found");
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "An internal server error occurred".to_string(),
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
                    error!("No miner found for piece distribution");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "An internal server error occurred".to_string(),
                    )
                })
                .unwrap(); // TODO: proper error handling

                if piece_providers.is_empty() {
                    error!("No providers found for piece");
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "An internal server error occurred".to_string(),
                    ));
                }
                let state = state.validator.read().await;
                let local_address_book = state.neuron.address_book.read().await;
                for provider in piece_providers {
                    let node_info = local_address_book.get(&provider);
                    if let Some(node_info) = node_info {
                        info!(
                            "Found Provider: {:?} for piece {:?}",
                            node_info, piece_info.piece_hash
                        );

                        let req_client = reqwest::Client::builder()
                            .timeout(CLIENT_TIMEOUT)
                            .build()
                            .map_err(|e| {
                                error!("Failed to build client: {}", e);
                                (
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "An internal server error occurred".to_string(),
                                )
                            })?;

                        let url = node_info
                            .http_address
                            .as_ref()
                            .and_then(base::utils::multiaddr_to_socketaddr)
                            .map(|socket_addr| {
                                format!(
                                    "http://{}:{}/piece?piecehash={}",
                                    socket_addr.ip(),
                                    socket_addr.port(),
                                    hex::encode(piece_hash)
                                )
                            })
                            .ok_or_else(|| {
                                error!("Invalid HTTP address in node_info");
                                (
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "An internal server error occurred".to_string(),
                                )
                            })?;

                        let node_response = req_client.get(&url).send().await.map_err(|e| {
                            error!("Failed to query node: {}", e);
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "An internal server error occurred".to_string(),
                            )
                        })?;

                        debug!("Node response: {:?}", node_response);
                        let body_bytes = node_response.bytes().await.map_err(|e| {
                            error!("Failed to read response body: {}", e);
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "An internal server error occurred".to_string(),
                            )
                        })?;

                        let deserialised_data =
                            match base::piece::deserialise_piece_response(&body_bytes, &piece_hash)
                            {
                                Ok(data) => {
                                    debug!("Deserialised piece response");
                                    data
                                }
                                Err(e) => {
                                    error!("Failed to deserialise piece response: {}", e);
                                    return Err((
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                        "An internal server error occurred".to_string(),
                                    ));
                                }
                            };

                        let piece = base::piece::Piece {
                            chunk_idx: piece_info.chunk_idx,
                            piece_idx: piece_info.piece_idx,
                            piece_type: piece_info.piece_type.clone(),
                            data: deserialised_data,
                        };

                        piece_data.push(piece);
                    }
                }
            }

            let chunk_info = base::piece::EncodedChunk {
                pieces: piece_data,
                chunk_idx: chunk.chunk_idx,
                k: chunk.k,
                m: chunk.m,
                chunk_size: chunk.chunk_size,
                padlen: chunk.padlen,
                original_chunk_size: chunk.original_chunk_size,
            };

            // Reconstruct chunk from pieces
            let reconstructed_chunk = match base::piece::reconstruct_chunk(&chunk_info) {
                Ok(reconstructed_chunk) => reconstructed_chunk,
                Err(e) => {
                    error!("Failed to reconstruct chunk: {}", e);
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "An internal server error occurred".to_string(),
                    ));
                }
            };

            if tx.send(reconstructed_chunk).await.is_err() {
                info!("Receiver dropped, stopping stream.");
                break;
            }
        }
        Ok(())
    });

    // Reconstruct file from chunks
    //
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok::<_, std::io::Error>);
    let body = axum::body::Body::from_stream(stream);

    // change this
    let headers = AppendHeaders([
        // NOTE: the specific content type should be left to the clients downloading it
        ("Content-Type", "application/octet-stream"),
        (
            "Content-Disposition",
            "inline; filename=\"downloaded_file\"", // TODO: use file name that's stored
        ),
    ]);

    Ok((headers, body))
}
