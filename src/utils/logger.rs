use std::path::Path;
use std::sync::OnceLock;
use tracing_appender::rolling;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

static GUARD: OnceLock<tracing_appender::non_blocking::WorkerGuard> = OnceLock::new();

pub fn init_logger(level: &str, to_file: bool, file_path: &str) {
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));

    // Build console Layer
    let console_layer = fmt::layer()
        .with_target(false)
        .with_ansi(true)
        .with_writer(std::io::stdout);

    if to_file {
        // Build file Layer
        let dir = Path::new(file_path)
            .parent()
            .unwrap_or_else(|| Path::new("./logs"));
        let name = Path::new(file_path)
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("scanner.log"));
        let appender = rolling::daily(dir, name);
        let (non_blocking, guard) = tracing_appender::non_blocking(appender);
        GUARD.set(guard).ok();

        let file_layer = fmt::layer()
            .with_target(false)
            .with_ansi(false)
            .with_writer(non_blocking);

        tracing_subscriber::registry()
            .with(filter)
            .with(console_layer) // Also output to console
            .with(file_layer) // Also output to file
            .init();
    } else {
        // Only output to console
        tracing_subscriber::registry()
            .with(filter)
            .with(console_layer)
            .init();
    }
}
