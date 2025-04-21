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
use tracing::warn;

use crate::constants::{INFO_API_RATE_LIMIT_DURATION, INFO_API_RATE_LIMIT_MAX_REQUESTS};

pub type InfoApiRateLimiter = Arc<DashMap<IpAddr, VecDeque<Instant>>>;

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
