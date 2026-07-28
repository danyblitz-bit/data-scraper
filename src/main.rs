mod app;
mod engine;
mod gui;
mod storage;
mod types;

use app::DataScraperApp;
use eframe::egui;
use types::AppConfig;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    let runtime_handle = rt.handle().clone();

    let config = AppConfig::default();

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
