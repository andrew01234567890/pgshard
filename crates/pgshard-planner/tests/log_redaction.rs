//! Regression that dependency debug logging cannot expose query text.

use std::fmt::Write as _;
use std::sync::Mutex;

use pgshard_planner::parse_one;

struct CaptureLogger {
    messages: Mutex<String>,
}

impl log::Log for CaptureLogger {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        let mut messages = self.messages.lock().expect("logger mutex");
        writeln!(messages, "{}", record.args()).expect("capture log record");
    }

    fn flush(&self) {}
}

static LOGGER: CaptureLogger = CaptureLogger {
    messages: Mutex::new(String::new()),
};

#[test]
fn dependency_logging_is_statically_disabled() {
    const SECRET: &str = "release-secret-must-not-be-logged";
    log::set_logger(&LOGGER).expect("install isolated test logger");
    log::set_max_level(log::LevelFilter::Trace);

    assert_eq!(log::STATIC_MAX_LEVEL, log::LevelFilter::Off);
    parse_one(&format!("select '{SECRET}'")).expect("parse secret-bearing query");

    let messages = LOGGER.messages.lock().expect("logger mutex");
    assert!(messages.is_empty(), "dependency emitted a log record");
    assert!(!messages.contains(SECRET));
}
