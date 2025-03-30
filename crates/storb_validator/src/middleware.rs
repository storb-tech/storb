use crate::apikey::ApiKeyManager;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use std::sync::Arc;
use tokio::sync::RwLock;

pub async fn require_api_key(
    req: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    tracing::debug!("In require_api_key middleware");

    let api_key = req
        .headers()
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            tracing::error!("No API key found in request headers");
            (StatusCode::UNAUTHORIZED, "Missing API key".to_string())
        })?;

    let api_key_manager = req
        .extensions()
        .get::<Arc<RwLock<ApiKeyManager>>>()
        .ok_or_else(|| {
            tracing::error!("ApiKeyManager not found in request extensions");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "ApiKeyManager not found".to_string(),
            )
        })?;

    let key_info = api_key_manager
        .read()
        .await
        .validate_key(api_key)
        .await
        .map_err(|e| {
            tracing::error!("Error validating API key: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error validating API key: {}", e),
            )
        })?
        .ok_or_else(|| {
            tracing::error!("Invalid API key: {}", api_key);
            (StatusCode::UNAUTHORIZED, "Invalid API key".to_string())
        })?;

    // Check rate limit only
    if let Some(rate_limit) = key_info.rate_limit {
        let within_limit = api_key_manager
            .read()
            .await
            .check_rate_limit(api_key, rate_limit)
            .await
            .map_err(|e| {
                tracing::error!("Failed to check rate limit: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to check rate limit".to_string(),
                )
            })?;

        if !within_limit {
            tracing::error!("Rate limit exceeded for API key: {}", api_key);
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                "Rate limit exceeded".to_string(),
            ));
        }
    }

    Ok(next.run(req).await)
}
