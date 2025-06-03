use std::collections::HashMap;

use axum::body::Bytes;
use axum::extract::{self, Query};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use base::piece_hash::{piecehash_str_to_bytes, PieceHashStr};
use base::verification::HandshakePayload;
use crabtensor::sign::verify_signature;
use tracing::{debug, error, info};

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
    state: axum::extract::State<MinerState>,
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

/// Handshake verification between a miner and a validator
#[utoipa::path(
    post,
    path = "/handshake",
    responses(
        (status = 200, description = "Successfully shaken hands", body = String),
        (status = 401, description = "Unauthorized", body = String),
        (status = 500, description = "Internal server error", body = String)
    ),
    tag = "Handshake"
)]
pub async fn handshake(
    state: extract::State<MinerState>,
    bytes: Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    info!("Got handshake request");

    let payload = bincode::deserialize::<HandshakePayload>(&bytes).map_err(|err| {
        error!("Error while deserializing bytes: {err}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let verified = verify_signature(
        &payload.message.validator.account_id,
        &payload.signature,
        &payload.message,
    );

    let address_book = state.clone().miner.neuron.read().await.address_book.clone();

    let validator_info = address_book
        .get(&payload.message.validator.uid.clone())
        .ok_or_else(|| {
            error!("Error while getting validator info");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .clone();
    let validator_hotkey = validator_info.neuron_info.hotkey.clone();

    if !verified || validator_hotkey != payload.message.validator.account_id {
        return Ok(StatusCode::UNAUTHORIZED);
    }

    Ok(StatusCode::OK)
}

/// Get a file piece.
#[utoipa::path(
    get,
    path = "/piece",
    responses(
        (status = 200, description = "Piece fetched successfully", body = String),
        (status = 400, description = "Missing piecehash parameter", body = String),
        (status = 401, description = "Unauthorized", body = String),
        (status = 500, description = "Internal server error during piece retrieval", body = String)
    ),
    params(
        ("piecehash" = String, Path, description = "The piecehash of the piece to retrieve."),
        ("handshake" = String, Path, description = "The handshake payload containing the signature to verify the validator."),
    ),
    tag = "Piece Download"
)]
pub async fn get_piece(
    state: extract::State<MinerState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, (StatusCode, Vec<u8>)> {
    info!("Get piece request");

    // Verify that the signature is valid. If not, the miner can reject the request from the validator
    let handshake = params.get("handshake").ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            bincode::serialize("Missing handshake parameter").unwrap_or_default(),
        )
    })?;
    let handshake_data = hex::decode(handshake).map_err(|err| {
        error!("Error while deserializing hex string: {err}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            bincode::serialize("An internal server error occurred").unwrap_or_default(),
        )
    })?;
    let payload = bincode::deserialize::<HandshakePayload>(&handshake_data).map_err(|err| {
        error!("Error while deserializing bytes: {err}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            bincode::serialize("An internal server error occurred").unwrap_or_default(),
        )
    })?;
    let verified = verify_signature(
        &payload.message.validator.account_id,
        &payload.signature,
        &payload.message,
    );

    let address_book = state.clone().miner.neuron.read().await.address_book.clone();

    let validator_info = address_book
        .get(&payload.message.validator.uid.clone())
        .ok_or_else(|| {
            error!("Error while getting validator info");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                bincode::serialize("An internal server error occurred").unwrap_or_default(),
            )
        })?
        .clone();
    let validator_hotkey = validator_info.neuron_info.hotkey.clone();

    if !verified || validator_hotkey != payload.message.validator.account_id {
        return Ok((
            StatusCode::UNAUTHORIZED,
            bincode::serialize("Signature verification failed. Unauthorized access")
                .unwrap_or_default(),
        ));
    }

    debug!("Signature verification successful:");
    debug!("{:?}", payload);

    // Continue if the signature is valid:

    let piece_hash_query = params.get("piecehash").ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            bincode::serialize("Missing piecehash parameter").unwrap_or_default(),
        )
    })?;

    info!("Piece hash: {}", piece_hash_query);
    let store = state.clone().object_store.clone();
    let object_store = store.lock().await;
    // If an error occurs during piece hash decode, it is bound to be a user error.
    let piece_hash = PieceHashStr::new(piece_hash_query.clone()).map_err(|_| {
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
        piece_hash: piecehash_str_to_bytes(&piece_hash).map_err(|err| {
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
