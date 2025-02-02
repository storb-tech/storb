use std::net::SocketAddr;

use crate::quic::send_via_quic;
use crate::ValidatorState;
use axum::{extract::Multipart, http::StatusCode, response::IntoResponse};
use futures::future::join_all;
use tracing::{error, info};

#[utoipa::path(
    post,
    path = "/upload",
    request_body(description = "File upload multipart form data", content_type = "multipart/form-data"),
    responses(
        (status = 200, description = "File uploaded successfully", body = String),
        (status = 400, description = "Invalid request or no file provided", body = String),
        (status = 502, description = "Failed to store file on miners", body = String),
        (status = 500, description = "Internal server error", body = String)
    )
)]
pub async fn upload_file(
    state: axum::extract::State<ValidatorState>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let file_data;

    // Get the first field from multipart
    let field = multipart.next_field().await.map_err(|e| {
        error!("Error reading multipart field: {}", e);
        (StatusCode::BAD_REQUEST, format!("Invalid request: {}", e))
    })?;

    // Check if there's a field
    if let Some(field) = field {
        file_data = field
            .bytes()
            .await
            .map_err(|e| {
                error!("Error reading file data: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to read file".into(),
                )
            })?
            .to_vec();
    } else {
        return Err((StatusCode::BAD_REQUEST, "No file provided".into()));
    }

    // Scope the MutexGuard to drop it after we're done with it
    let miners = {
        let validator_guard = state.validator.lock().await;
        let neurons_guard = validator_guard.neuron.neurons.read().unwrap();
        neurons_guard.clone()
    };

    let mut tasks = Vec::new();
    for miner in &miners {
        let ip_u32 = miner.axon_info.ip;
        let ip_bytes: [u8; 4] = [
            (ip_u32 >> 24) as u8,
            (ip_u32 >> 16) as u8,
            (ip_u32 >> 8) as u8,
            ip_u32 as u8,
        ];
        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::from(ip_bytes));
        let miner_addr = SocketAddr::new(ip, miner.axon_info.port);
        let file_data = file_data.clone();

        tasks.push(tokio::spawn(async move {
            match send_via_quic(&miner_addr, &file_data).await {
                Ok(response) => {
                    info!("File hashes from {}: {}", miner_addr, response);
                    Ok(miner_addr)
                }
                Err(e) => {
                    error!("QUIC transfer failed for miner at {}: {}", miner_addr, e);
                    Err(miner_addr)
                }
            }
        }));
    }

    let results = join_all(tasks).await;
    let mut failed_deliveries = Vec::new();

    for result in results {
        match result {
            Ok(Ok(_)) => (), // Successfully delivered
            Ok(Err(addr)) => failed_deliveries.push(addr),
            Err(e) => error!("Task failed: {}", e),
        }
    }

    if failed_deliveries.len() == miners.len() {
        return Err((
            StatusCode::BAD_GATEWAY,
            "Failed to store file on any miners".into(),
        ));
    }

    Ok((StatusCode::OK, "Uploaded! :)"))
}
