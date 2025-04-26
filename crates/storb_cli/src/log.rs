use std::io::stderr;

use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::logs::{SdkLogger, SdkLoggerProvider};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::Registry;
use tracing_subscriber::{fmt, EnvFilter};

/// Print to stderr and exit with a non-zero exit code
#[macro_export]
macro_rules! fatal {
    ($($arg:tt)*) => {{
        eprintln!($($arg)*);
        std::process::exit(1);
    }};
}

/// Initialise the global logger
pub fn new(
    log_level: &str,
    otel_layer: OpenTelemetryTracingBridge<SdkLoggerProvider, SdkLogger>,
) -> (WorkerGuard, WorkerGuard) {
    match log_level {
        "TRACE" | "DEBUG" | "INFO" | "WARN" | "ERROR" => {}
        _ => {
            fatal!("Invalid log level `{log_level}`. Valid levels are: TRACE, DEBUG, INFO, WARN, ERROR");
        }
    };

    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(log_level))
        .expect("Failed to create log filter");

    let appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_suffix("log")
        .build("logs")
        .expect("Failed to initialise rolling file appender");

    let (non_blocking_file, file_guard) = tracing_appender::non_blocking(appender);
    let (non_blocking_stdout, stdout_guard) = tracing_appender::non_blocking(stderr());

    let logger = Registry::default()
        .with(filter)
        .with(
            fmt::Layer::default()
                .with_writer(non_blocking_stdout)
                .with_line_number(true),
        )
        .with(
            fmt::Layer::default()
                .with_writer(non_blocking_file)
                .with_line_number(true)
                .with_ansi(false),
        )
        .with(otel_layer);

    tracing::subscriber::set_global_default(logger).expect("Failed to initialise logger");

    (file_guard, stdout_guard)
}
