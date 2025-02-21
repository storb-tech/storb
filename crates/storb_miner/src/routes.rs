use axum::extract::{self, Query};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use base::piece_hash::{piecehash_to_bytes_raw, PieceHash};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::MinerState;

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
    state: extract::State<Arc<Mutex<MinerState>>>,
) -> Result<impl IntoResponse, (StatusCode, Vec<u8>)> {
    info!("Got node info req");

    let neuron = state.lock().await.miner.lock().await.neuron.clone();
    let serialized_local_node_info = bincode::serialize(&neuron.local_node_info).map_err(|e| {
        error!("Error while deserialising local node info: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            bincode::serialize(&"Failed to serialize local node info").unwrap_or_default(),
        )
    })?;

    Ok((StatusCode::OK, serialized_local_node_info))
}

/// Get a file piece.
#[utoipa::path(
    get,
    path = "/piece",
    responses(
        (status = 200, description = "Piece fetched successfully", body = String),
        (status = 400, description = "Missing piecehash parameter", body = String),
        (status = 500, description = "Internal server error during piece retrieval", body = String)
    ),
    params(
        ("piecehash" = String, Path, description = "The piecehash of the piece to retrieve."),
    ),
    tag = "Piece Download"
)]
pub async fn get_piece(
    state: extract::State<Arc<Mutex<MinerState>>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, (StatusCode, Vec<u8>)> {
    info!("Get piece request");

    let piece_hash_query = params.get("piecehash").ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            bincode::serialize("Missing piecehash parameter").unwrap_or_default(),
        )
    })?;

    info!("Piece hash: {}", piece_hash_query);
    let object_store = state.lock().await.object_store.lock().await.clone();
    // If an error occurs during piece hash decode, it is bound to be a user error.
    let piece_hash = PieceHash::new(piece_hash_query.clone()).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            bincode::serialize("The piecehash is invalid. Was it the correct length?")
                .unwrap_or_default(),
        )
    })?;

    let data = object_store.read(&piece_hash).await.map_err(|err| {
        error!("Failed to retrieve data for piecehash {piece_hash}: {err}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            bincode::serialize("An internal server error occurred").unwrap_or_default(),
        )
    })?;

    let serialised_data = base::piece::serialise_piece_response(&base::piece::PieceResponse {
        piece_hash: piecehash_to_bytes_raw(&piece_hash).map_err(|err| {
            error!("Failed to convert piece hash to raw bytes: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                bincode::serialize("An internal server error occurred").unwrap_or_default(),
            )
        })?,
        piece_data: data,
    })
    .map_err(|err| {
        error!("Failed to serialise piece data (piecehash={piece_hash}): {err}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            bincode::serialize("An internal server error occurred").unwrap_or_default(),
        )
    })?;

    Ok((StatusCode::OK, serialised_data))
}
