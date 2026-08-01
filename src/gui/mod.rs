pub mod dashboard;
pub mod results_panel;
pub mod scraper_panel;
pub mod settings;

pub use dashboard::*;
pub use results_panel::*;
pub use scraper_panel::*;
pub use settings::*;

use chrono::TimeZone;

pub fn fmt_local(ts: &chrono::NaiveDateTime) -> String {
    chrono::Local
        .from_utc_datetime(ts)
        .format("%Y-%m-%d %H:%M")
        .to_string()
}
