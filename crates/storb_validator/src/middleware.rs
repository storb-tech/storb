use std::collections::VecDeque;
use std::net::IpAddr;
use std::sync::Arc;
use std::{net::SocketAddr, time::Instant};

use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
    Extension,
};
use dashmap::DashMap;
use tokio::sync::RwLock;
use tracing::warn;

use crate::constants::INFO_API_RATE_LIMIT_MAX_REQUESTS;
use crate::{apikey::ApiKeyManager, constants::INFO_API_RATE_LIMIT_DURATION};

pub type InfoApiRateLimiter = Arc<DashMap<IpAddr, VecDeque<Instant>>>;

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
                "Internal server error".to_string(),
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
                "Internal server error".to_string(),
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
                    "Internal server error".to_string(),
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

pub async fn info_api_rate_limit_middleware(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Extension(limiter): Extension<InfoApiRateLimiter>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let ip = addr.ip();

    let now = Instant::now();
    let limit_start_time = now - INFO_API_RATE_LIMIT_DURATION;

    let mut entry = limiter.entry(ip).or_default();
    let timestamps: &mut VecDeque<Instant> = entry.value_mut();

    while let Some(ts) = timestamps.front() {
        if *ts >= limit_start_time {
            break;
        }
        timestamps.pop_front();
    }

    if timestamps.len() >= INFO_API_RATE_LIMIT_MAX_REQUESTS {
        drop(entry);
        warn!("Rate limit exceeded for IP: {}", ip);
        Err(StatusCode::TOO_MANY_REQUESTS)
    } else {
        timestamps.push_back(now);
        drop(entry);
        Ok(next.run(request).await)
    }
}
