use std::io::stderr;

use tracing::level_filters::LevelFilter;
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::Registry;

/// Print to stderr and exit with a non-zero exit code
#[macro_export]
macro_rules! fatal {
    ($($arg:tt)*) => {{
        eprintln!($($arg)*);
        std::process::exit(1);
    }};
}

/// Initialise the global logger
pub fn new(log_level: &str) -> (WorkerGuard, WorkerGuard) {
    let level: Level = match log_level {
        "TRACE" => Level::TRACE,
        "DEBUG" => Level::DEBUG,
        "INFO" => Level::INFO,
        "WARN" => Level::WARN,
        "ERROR" => Level::ERROR,
        _ => {
            fatal!("Invalid log level `{log_level}`. Valid levels are: TRACE, DEBUG, INFO, WARN, ERROR");
        }
    };

    let appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_suffix("log")
        .build("logs")
        .expect("Failed to initialise rolling file appender");

    let (non_blocking_file, file_guard) = tracing_appender::non_blocking(appender);
    let (non_blocking_stdout, stdout_guard) = tracing_appender::non_blocking(stderr());

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
