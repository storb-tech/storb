use std::io::stdout;

use tracing::level_filters::LevelFilter;
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::Registry;

/// Initialise the global logger
pub fn new(log_level: &str) -> (WorkerGuard, WorkerGuard) {
    let level: Level = match log_level {
        "TRACE" => Level::TRACE,
        "DEBUG" => Level::DEBUG,
        "INFO" => Level::INFO,
        "WARN" => Level::WARN,
        "ERROR" => Level::ERROR,
        _ => {
            // TODO: log warning that option was invalid
            Level::INFO
        }
    };

    let appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_suffix("log")
        .build("logs")
        .expect("Failed to initialise rolling file appender");

    let (non_blocking_file, file_guard) = tracing_appender::non_blocking(appender);
    let (non_blocking_stdout, stdout_guard) = tracing_appender::non_blocking(stdout());

    let logger = Registry::default()
        .with(LevelFilter::from_level(level))
        .with(fmt::Layer::default().with_writer(non_blocking_stdout))
        .with(
            fmt::Layer::default()
                .with_writer(non_blocking_file)
                .with_ansi(false),
        );

    tracing::subscriber::set_global_default(logger).expect("Failed to initialise logger");

    (file_guard, stdout_guard)
}
