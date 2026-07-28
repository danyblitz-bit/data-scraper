use eframe::egui;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::engine::{EngineStats, ScraperEngine};
use crate::gui::{ResultsPanel, ScraperPanel, SettingsPanel};
use crate::storage::Storage;
use crate::types::{AppConfig, ScrapeJob, ScrapeResult, Theme};

enum AppCommand {
    RunJob(String),
    RunAllJobs,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum AppView {
    #[default]
    Dashboard,
    Scraper,
    Results,
    Settings,
}

pub struct DataScraperApp {
    pub current_view: AppView,
    pub config: AppConfig,
    pub jobs: Vec<ScrapeJob>,
    pub results: Vec<ScrapeResult>,
    pub stats: EngineStats,
    pub total_jobs: u64,
    pub total_results: u64,
    pub total_bytes: u64,
    pub scraper_panel: ScraperPanel,
    pub results_panel: ResultsPanel,
    pub settings_panel: SettingsPanel,
    pub engine: Arc<ScraperEngine>,
    pub storage: Arc<RwLock<Storage>>,
    pub runtime: tokio::runtime::Handle,
}

impl DataScraperApp {
    pub fn new(
        _cc: &eframe::CreationContext<'_>,
        config: AppConfig,
        storage: Storage,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        let storage = Arc::new(RwLock::new(storage));
        let engine = Arc::new(ScraperEngine::new(
            storage.clone(),
            config.max_concurrent_requests,
        ));
        let jobs = config.jobs.clone();

        let mut app = Self {
            current_view: AppView::Dashboard,
            config,
            jobs,
            results: Vec::new(),
            stats: EngineStats::default(),
            total_jobs: 0,
            total_results: 0,
            total_bytes: 0,
            scraper_panel: ScraperPanel::default(),
            results_panel: ResultsPanel::default(),
            settings_panel: SettingsPanel::default(),
            engine,
            storage,
            runtime: runtime_handle,
        };

        app.refresh_stats();
        app
    }

    pub fn refresh_stats(&mut self) {
        let storage = self.storage.clone();
        let engine = self.engine.clone();

        let stats = self.runtime.block_on(async {
            let s = engine.get_stats().await;
            let stats_result = storage.read().await.get_stats_summary().await;
            match stats_result {
                Ok((tj, tr, tb, _)) => (s, tj, tr, tb),
                Err(_) => (s, 0, 0, 0),
            }
        });
        self.stats = stats.0;
        self.total_jobs = stats.1;
        self.total_results = stats.2;
        self.total_bytes = stats.3;
    }

    pub fn run_job(&mut self, job_id: String) {
        let engine = self.engine.clone();
        let jobs = self.jobs.clone();

        self.runtime.spawn(async move {
            if let Some(job) = jobs.iter().find(|j| j.id == job_id) {
                let job = job.clone();
                log::info!("Running job: {} ({})", job.name, job.url);
                match engine.run_job(job).await {
                    Ok(result) => {
                        log::info!(
                            "Job completed: {} records in {}ms",
                            result.data.len(),
                            result.duration_ms
                        );
                    }
                    Err(e) => {
                        log::error!("Job failed: {}", e);
                    }
                }
            }
        });
    }

    pub fn run_all_jobs(&mut self) {
        let engine = self.engine.clone();
        let jobs = self.jobs.clone();

        self.runtime.spawn(async move {
            log::info!("Running all enabled jobs...");
            let results = engine.run_all_jobs(&jobs).await;
            let success = results.iter().filter(|r| r.is_ok()).count();
            let failed = results.iter().filter(|r| r.is_err()).count();
            log::info!("All jobs completed: {} success, {} failed", success, failed);
        });
    }
}

impl eframe::App for DataScraperApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx();
        ctx.request_repaint_after(std::time::Duration::from_millis(200));
        self.refresh_stats();

        match self.config.theme {
            Theme::Dark => ctx.set_visuals(egui::Visuals::dark()),
            Theme::Light => ctx.set_visuals(egui::Visuals::light()),
            Theme::System => {}
        }

        egui::Panel::top("top_bar").show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                ui.heading("Data Scraper");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.colored_label(egui::Color32::GRAY, "v0.1.0");
                    ui.separator();
                    if ui.button("Dashboard").clicked() {
                        self.current_view = AppView::Dashboard;
                    }
                    if ui.button("Scraper Jobs").clicked() {
                        self.current_view = AppView::Scraper;
                    }
                    if ui.button("Results").clicked() {
                        self.current_view = AppView::Results;
                    }
                    if ui.button("Settings").clicked() {
                        self.current_view = AppView::Settings;
                    }
                    ui.label("View:");
                });
            });
        });

        egui::CentralPanel::default().show(ui, |ui| {
            egui::Frame::new()
                .inner_margin(egui::Margin::symmetric(16, 12))
                .show(ui, |ui| {
                    match self.current_view {
                        AppView::Dashboard => {
                            crate::gui::show_dashboard(
                                ui,
                                &self.stats,
                                self.total_jobs,
                                self.total_results,
                                self.total_bytes,
                            );
                        }
                        AppView::Scraper => {
                            use std::cell::RefCell;
                            let commands = RefCell::new(Vec::new());
                            self.scraper_panel.show(
                                ui,
                                &mut self.jobs,
                                &self.stats,
                                &mut |job_id| commands.borrow_mut().push(AppCommand::RunJob(job_id)),
                                &mut || commands.borrow_mut().push(AppCommand::RunAllJobs),
                            );
                            for cmd in commands.into_inner() {
                                match cmd {
                                    AppCommand::RunJob(job_id) => self.run_job(job_id),
                                    AppCommand::RunAllJobs => self.run_all_jobs(),
                                }
                            }
                        }
                        AppView::Results => {
                            let job_names: Vec<(String, String)> =
                                self.jobs.iter().map(|j| (j.id.clone(), j.name.clone())).collect();
                            self.results_panel.show(ui, &self.results, &job_names);
                        }
                        AppView::Settings => {
                            self.settings_panel.show(ui, &mut self.config);
                        }
                    }
                });
        });
    }
}
