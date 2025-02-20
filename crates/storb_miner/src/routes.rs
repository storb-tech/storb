use std::sync::Arc;

use axum::{extract, http::StatusCode, response::IntoResponse};
use tokio::sync::Mutex;
use tracing::info;

use crate::MinerState;

/// Router function to get information on a given node
pub async fn node_info(
    state: extract::State<Arc<Mutex<MinerState>>>,
) -> Result<impl IntoResponse, (StatusCode, Vec<u8>)> {
    info!("Got node info req");

    let neuron = state.lock().await.miner.lock().await.neuron.clone();
    let serialized_local_node_info = bincode::serialize(&neuron.local_node_info).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            bincode::serialize(&format!("Failed to serialize local node info: {}", e))
                .unwrap_or_default(),
        )
    })?;

    Ok((StatusCode::OK, serialized_local_node_info))
}
