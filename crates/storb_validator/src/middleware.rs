use crate::apikey::ApiKeyManager;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use std::sync::Arc;
use tokio::sync::RwLock;

pub async fn require_api_key(req: Request<Body>, next: Next) -> Result<Response, StatusCode> {
    tracing::debug!("In require_api_key middleware");

    // Extract API key from header
    let api_key = req
        .headers()
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            tracing::error!("No API key found in request headers");
            StatusCode::UNAUTHORIZED
        })?;

    // Get API key manager from extensions
    let api_key_manager = req
        .extensions()
        .get::<Arc<RwLock<ApiKeyManager>>>()
        .ok_or_else(|| {
            tracing::error!("ApiKeyManager not found in request extensions");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Validate API key
    let key_info = api_key_manager
        .read()
        .await
        .validate_key(api_key)
        .await
        .map_err(|e| {
            tracing::error!("Error validating API key: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or_else(|| {
            tracing::error!("Invalid API key: {}", api_key);
            StatusCode::UNAUTHORIZED
        })?;

    tracing::debug!("Key info found: {:?}", key_info);

    // Check rate limit
    if let Some(rate_limit) = key_info.rate_limit {
        // TODO: Implement rate limiting
        tracing::debug!("Rate limit found: {}", rate_limit);
    }

    tracing::debug!("Checking upload/download limits");

    // Check upload/download limits
    if let Some(upload_limit) = key_info.upload_limit {
        if key_info.upload_used >= upload_limit {
            tracing::error!(
                "Upload limit exceeded: used={}, limit={}",
                key_info.upload_used,
                upload_limit
            );
            return Err(StatusCode::FORBIDDEN);
        }
    }

    if let Some(download_limit) = key_info.download_limit {
        if key_info.download_used >= download_limit {
            tracing::error!(
                "Download limit exceeded: used={}, limit={}",
                key_info.download_used,
                download_limit
            );
            return Err(StatusCode::FORBIDDEN);
        }
    }

    tracing::debug!("API key validated successfully");

    Ok(next.run(req).await)
}
