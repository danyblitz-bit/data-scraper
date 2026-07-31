mod app;
mod engine;
mod gui;
mod storage;
mod types;

use app::DataScraperApp;
use eframe::egui;
use types::AppConfig;

const CONFIG_PATH: &str = "data_scraper.yaml";

fn load_config() -> AppConfig {
    std::fs::read_to_string(CONFIG_PATH)
        .ok()
        .and_then(|s| serde_yaml::from_str(&s).ok())
        .unwrap_or_default()
}

pub fn save_config(config: &AppConfig) {
    let mut cfg = config.clone();
    cfg.jobs.clear();
    if let Ok(s) = serde_yaml::to_string(&cfg) {
        let _ = std::fs::write(CONFIG_PATH, s);
    }
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    let runtime_handle = rt.handle().clone();

    let config = load_config();
    log::info!(
        "Loaded config: {} concurrent, {}s timeout, theme {:?}",
        config.max_concurrent_requests,
        config.request_timeout_secs,
        config.theme
    );

    let storage = rt.block_on(async {
        storage::Storage::new(&config.database_path).expect("Failed to initialize database")
    });

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([config.window_width, config.window_height])
            .with_min_inner_size([800.0, 600.0])
            .with_title("Data Scraper"),
        ..Default::default()
    };

    log::info!("Starting Data Scraper...");

    eframe::run_native(
        "Data Scraper",
        options,
        Box::new(|cc| {
            Ok(Box::new(DataScraperApp::new(
                cc,
                config,
                storage,
                runtime_handle,
            )))
        }),
    )
    .expect("Failed to start application");
}
