use chrono::{FixedOffset, Local};
use std::{fs, path::Path, sync::OnceLock};
use tracing_appender::rolling;
use tracing_subscriber::fmt::time::{ChronoLocal, ChronoUtc, FormatTime};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

static GUARD: OnceLock<tracing_appender::non_blocking::WorkerGuard> = OnceLock::new();

/// 自訂格式化時間（支援固定 UTC 偏移）
#[derive(Clone)]
struct FixedOffsetTime {
    offset: FixedOffset,
    format: String,
}

impl FormatTime for FixedOffsetTime {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        let now = Local::now().with_timezone(&self.offset);
        write!(w, "{}", now.format(&self.format))
    }
}

/// 建立時間格式器
fn build_timer(timezone: String) -> impl FormatTime + Clone {
    let fmt_str = "%Y-%m-%d %H:%M:%S%.3f %:z";

    match timezone.as_str() {
        "utc" | "UTC" => TimerType::Utc(ChronoUtc::new(fmt_str.to_string())),
        "local" | "LOCAL" => TimerType::Local(ChronoLocal::new(fmt_str.to_string())),
        "utc+8" | "UTC+8" | "china" | "CHINA" | "cst" | "CST" => {
            // 中国标准时间 (UTC+8)
            TimerType::Fixed(FixedOffsetTime {
                offset: FixedOffset::east_opt(8 * 3600).unwrap(),
                format: fmt_str.to_string(),
            })
        }
        _ => TimerType::Local(ChronoLocal::new(fmt_str.to_string())), // 預設使用本地時間
    }
}

/// 枚举来持有不同的计时器类型
#[derive(Clone)]
enum TimerType {
    Local(ChronoLocal),
    Utc(ChronoUtc),
    Fixed(FixedOffsetTime),
}

impl FormatTime for TimerType {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        match self {
            TimerType::Local(timer) => timer.format_time(w),
            TimerType::Utc(timer) => timer.format_time(w),
            TimerType::Fixed(timer) => timer.format_time(w),
        }
    }
}

/// 初始化 logger
pub fn init_logger(level: &str, to_file: bool, file_path: &str, timezone: &str) {
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));

    let console_timer = build_timer(timezone.to_string());
    let console_layer = fmt::layer()
        .with_timer(console_timer.clone())
        .with_target(false)
        .with_ansi(true)
        .with_writer(std::io::stdout);

    if to_file {
        let path = Path::new(file_path);
        let (dir, name) = if path.is_dir() {
            (path, std::ffi::OsStr::new("app.log"))
        } else {
            (
                path.parent().unwrap_or_else(|| Path::new("./logs")),
                path.file_name()
                    .unwrap_or_else(|| std::ffi::OsStr::new("app.log")),
            )
        };
        fs::create_dir_all(dir).ok();

        let appender = rolling::daily(dir, name);
        let (non_blocking, guard) = tracing_appender::non_blocking(appender);
        if GUARD.set(guard).is_err() {
            eprintln!("Logger guard already initialized, skipping re-init");
        }

        let file_timer = build_timer(timezone.to_string());
        let file_layer = fmt::layer()
            .with_timer(file_timer)
            .with_target(false)
            .with_ansi(false)
            .with_writer(non_blocking);

        tracing_subscriber::registry()
            .with(filter)
            .with(console_layer)
            .with(file_layer)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(console_layer)
            .init();
    }
}
