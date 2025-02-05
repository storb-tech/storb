use crate::{quic::establish_miner_connections, ValidatorState};
use anyhow::Result;
use axum::{
    body::Bytes,
    extract::Multipart,
    http::{header::CONTENT_LENGTH, HeaderMap, StatusCode},
    response::IntoResponse,
};
use base::piece::{encode_chunk, piece_length};
use futures::{Stream, TryStreamExt};
use quinn::Connection;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

const MIN_REQUIRED_MINERS: usize = 1;

struct UploadProcessor {
    miner_connections: Vec<(SocketAddr, Connection)>,
}

impl UploadProcessor {
    async fn new(state: &ValidatorState) -> Result<Self> {
        // Get miner addresses from validator state
        let miners = {
            let validator_guard = state.validator.lock().await;
            let neurons_guard = validator_guard.neuron.neurons.read().unwrap();
            neurons_guard.clone()
        };

        if miners.is_empty() {
            return Err(anyhow::anyhow!("No miners available in validator state"));
        }

        // Establish QUIC connections with miners
        let miner_connections = match establish_miner_connections(&miners).await {
            Ok(connections) => {
                if connections.is_empty() {
                    return Err(anyhow::anyhow!(
                        "Failed to establish connections with any miners"
                    ));
                }
                if connections.len() < MIN_REQUIRED_MINERS {
                    warn!(
                        "Connected to fewer miners than recommended: {}",
                        connections.len()
                    );
                }
                connections
            }
            Err(e) => {
                error!("Failed to establish connections with miners: {}", e);
                return Err(anyhow::anyhow!(
                    "Failed to establish miner connections: {}",
                    e
                ));
            }
        };

        info!(
            "Successfully connected to {} miners",
            miner_connections.len()
        );
        Ok(Self { miner_connections })
    }

    async fn process_upload<S, E>(&self, stream: S, total_size: u64) -> Result<String>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
    {
        info!("Processing upload of size {} bytes...", total_size);

        if self.miner_connections.is_empty() {
            return Err(anyhow::anyhow!("No active miner connections available"));
        }

        let chunk_size = piece_length(total_size, None, None) as usize;
        let (tx, rx) = mpsc::channel(chunk_size);

        // Spawn producer task
        let producer_handle = {
            let stream = Box::pin(stream);
            tokio::spawn(async move {
                if let Err(e) = produce_bytes(stream, tx, total_size, chunk_size).await {
                    error!("Producer error: {}", e);
                    return Err(e);
                }
                Ok(())
            })
        };

        // Spawn consumer task
        let miner_conns = self.miner_connections.clone();
        let consumer_handle = tokio::spawn(async move {
            if let Err(e) = consume_bytes(rx, miner_conns).await {
                error!("Consumer error: {}", e);
                return Err(e);
            }
            Ok(())
        });

        // Wait for both tasks to complete
        let (producer_result, consumer_result) = tokio::join!(producer_handle, consumer_handle);
        producer_result??;
        consumer_result??;

        Ok("Upload processed successfully".to_string())
    }
}

async fn produce_bytes<S, E>(
    stream: S,
    tx: mpsc::Sender<Vec<u8>>,
    total_size: u64,
    chunk_size: usize,
) -> Result<()>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let mut total_processed: u64 = 0;
    let mut buffer = Vec::with_capacity(chunk_size);

    let mut stream = stream.map_err(|e| anyhow::anyhow!("Stream error: {}", e.into()));

    while let Some(chunk_result) = stream.try_next().await? {
        buffer.extend_from_slice(&chunk_result);

        // Process full chunks
        while buffer.len() >= chunk_size {
            let chunk_data = buffer.drain(..chunk_size).collect::<Vec<u8>>();
            total_processed += chunk_data.len() as u64;

            if total_processed > total_size {
                return Err(anyhow::anyhow!("Upload size exceeds expected size"));
            }

            // Log progress every 100MB
            if total_processed % (100 * 1024 * 1024) == 0 {
                info!(
                    "Processing: {} MB / {} MB",
                    total_processed / (1024 * 1024),
                    total_size / (1024 * 1024)
                );
            }

            tx.send(chunk_data).await?;
        }
    }

    // Send any remaining data
    if !buffer.is_empty() {
        total_processed += buffer.len() as u64;
        tx.send(buffer).await?;
    }

    info!("Total bytes processed: {}", total_processed);
    Ok(())
}

async fn consume_bytes(
    mut rx: mpsc::Receiver<Vec<u8>>,
    miner_connections: Vec<(SocketAddr, Connection)>,
) -> Result<()> {
    let mut chunk_idx = 0;
    while let Some(chunk) = rx.recv().await {
        // Encode the chunk using FEC
        let encoded = encode_chunk(&chunk, chunk_idx);
        chunk_idx += 1;

        // Distribute pieces to miners
        if let Some(pieces) = encoded.pieces {
            for (piece_idx, piece) in pieces.iter().enumerate() {
                // Round-robin distribution to miners
                let (addr, conn) = &miner_connections[piece_idx % miner_connections.len()];

                info!("Sending piece {} to miner {}", piece_idx, addr);

                // Send piece to miner via QUIC connection
                let (mut send_stream, mut recv_stream) = conn.open_bi().await?;

                send_stream
                    .write_all(&(piece.data.len() as u64).to_be_bytes())
                    .await?;
                send_stream.write_all(&piece.data).await?;
                send_stream.finish()?;

                // Read hash response
                let mut hash = String::new();
                let mut buf = [0u8; 1];
                while let Ok(Some(n)) = recv_stream.read(&mut buf).await {
                    if n == 0 || buf[0] == b'\n' {
                        break;
                    }
                    hash.push(buf[0] as char);
                }

                info!(
                    "Received hash {} for piece {} from miner {}",
                    hash, piece_idx, addr
                );
            }
        }
    }

    Ok(())
}

#[utoipa::path(
    post,
    path = "/upload",
    responses(
        (status = 200, description = "File uploaded successfully", body = String),
        (status = 400, description = "Bad request - invalid file or missing content length", body = String),
        (status = 500, description = "Internal server error during upload", body = String)
    ),
    tag = "Upload"
)]
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

    match processor.process_upload(stream, content_length).await {
        Ok(_) => Ok((StatusCode::OK, "Upload successful".to_string())),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}
