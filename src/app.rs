use eframe::egui;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use tokio::sync::RwLock;

use crate::engine::{EngineStats, ScraperEngine, Scheduler};
use crate::gui::{ResultsPanel, ScraperPanel, SettingsPanel};
use crate::gui::scraper_panel::TestOutcome;
use crate::storage::Storage;
use crate::types::{AppConfig, ScrapeJob, ScrapeResult, Theme};

enum AppCommand {
    RunJob(String),
    RunAllJobs,
    TestJob(ScrapeJob),
    NavigateTo(AppView),
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
    pub last_view: AppView,
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
    pub test_tx: Sender<TestOutcome>,
    pub last_results_refresh: std::time::Instant,
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

        let scheduler = Scheduler::new(engine.clone(), storage.clone());
        let rt_handle = runtime_handle.clone();

        let (test_tx, test_rx) = std::sync::mpsc::channel();

        let mut app = Self {
            current_view: AppView::Dashboard,
            last_view: AppView::Dashboard,
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
            test_tx,
            last_results_refresh: std::time::Instant::now(),
        };
        app.scraper_panel.test_rx = Some(test_rx);

        app.load_jobs_from_db();
        app.refresh_stats();

        rt_handle.block_on(scheduler.start());

        app
    }

    fn load_jobs_from_db(&mut self) {
        let storage = self.storage.clone();
        self.jobs = self.runtime.block_on(async {
            storage.read().await.get_all_jobs().await.unwrap_or_default()
        });
    }

    pub fn refresh_results(&mut self) {
        let storage = self.storage.clone();
        self.results = self.runtime.block_on(async {
            storage.read().await.get_all_results(500).await.unwrap_or_default()
        });
    }

    fn sync_jobs_to_db(&mut self) {
        let storage = self.storage.clone();
        let jobs = self.jobs.clone();
        self.runtime.spawn(async move {
            let s = storage.write().await;
            let db_jobs = s.get_all_jobs().await.unwrap_or_default();
            for j in &db_jobs {
                if !jobs.iter().any(|x| x.id == j.id) {
                    let _ = s.delete_job(&j.id).await;
                }
            }
            for j in &jobs {
                let _ = s.save_job(j).await;
            }
        });
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

    fn test_job(&mut self, mut job: ScrapeJob) {
        job.max_pages = Some(1);
        let engine = self.engine.clone();
        let tx = self.test_tx.clone();
        let job_id = job.id.clone();
        self.runtime.spawn(async move {
            let outcome = match engine.test_job(&job).await {
                Ok(r) => TestOutcome {
                    job_id,
                    result: Ok(r),
                },
                Err(e) => TestOutcome {
                    job_id,
                    result: Err(e.to_string()),
                },
            };
            let _ = tx.send(outcome);
        });
    }
}

impl eframe::App for DataScraperApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx();
        ctx.request_repaint_after(std::time::Duration::from_millis(200));
        let entering_dashboard =
            self.current_view == AppView::Dashboard && self.current_view != self.last_view;
        let entering_results = self.current_view == AppView::Results && self.current_view != self.last_view;
        self.last_view = self.current_view.clone();
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
                            use std::cell::RefCell;
                            if entering_dashboard
                                || self.last_results_refresh.elapsed() >= std::time::Duration::from_secs(2)
                            {
                                self.last_results_refresh = std::time::Instant::now();
                                self.refresh_results();
                            }
                            let job_names: Vec<(String, String)> =
                                self.jobs.iter().map(|j| (j.id.clone(), j.name.clone())).collect();
                            let commands = RefCell::new(Vec::new());
                            crate::gui::show_dashboard(
                                ui,
                                &self.stats,
                                self.total_jobs,
                                self.total_results,
                                self.total_bytes,
                                &job_names,
                                &self.results,
                                &mut || commands.borrow_mut().push(AppCommand::RunAllJobs),
                                &mut |view| commands.borrow_mut().push(AppCommand::NavigateTo(view)),
                            );
                            for cmd in commands.into_inner() {
                                match cmd {
                                    AppCommand::RunAllJobs => self.run_all_jobs(),
                                    AppCommand::NavigateTo(view) => self.current_view = view,
                                    _ => {}
                                }
                            }
                        }
                        AppView::Scraper => {
                            use std::cell::RefCell;
                            loop {
                                let recv = self.scraper_panel.test_rx.as_ref().unwrap().try_recv();
                                match recv {
                                    Ok(outcome) => self.scraper_panel.test_preview = Some(outcome),
                                    Err(_) => break,
                                }
                            }
                            let commands = RefCell::new(Vec::new());
                            self.scraper_panel.show(
                                ui,
                                &mut self.jobs,
                                &self.stats,
                                &mut |job_id| commands.borrow_mut().push(AppCommand::RunJob(job_id)),
                                &mut || commands.borrow_mut().push(AppCommand::RunAllJobs),
                                &mut |job| commands.borrow_mut().push(AppCommand::TestJob(job)),
                            );
                            for cmd in commands.into_inner() {
                                match cmd {
                                    AppCommand::RunJob(job_id) => self.run_job(job_id),
                                    AppCommand::RunAllJobs => self.run_all_jobs(),
                                    AppCommand::TestJob(job) => self.test_job(job),
                                    AppCommand::NavigateTo(_) => {}
                                }
                            }
                            if self.scraper_panel.jobs_modified {
                                self.scraper_panel.jobs_modified = false;
                                self.sync_jobs_to_db();
                            }
                        }
                        AppView::Results => {
                            use std::cell::RefCell;
                            if entering_results
                                || self.last_results_refresh.elapsed() >= std::time::Duration::from_secs(2)
                            {
                                self.last_results_refresh = std::time::Instant::now();
                                self.refresh_results();
                            }
                            let job_names: Vec<(String, String)> =
                                self.jobs.iter().map(|j| (j.id.clone(), j.name.clone())).collect();
                            let storage = self.storage.clone();
                            let runtime = self.runtime.clone();
                            let to_delete = RefCell::new(None);
                            self.results_panel.show(ui, &self.results, &job_names, &mut |id| {
                                *to_delete.borrow_mut() = Some(id);
                            });
                            if let Some(id) = to_delete.into_inner() {
                                let storage = storage.clone();
                                runtime.spawn(async move {
                                    let _ = storage.write().await.delete_result(id).await;
                                });
                                self.refresh_results();
                            }
                        }
                        AppView::Settings => {
                            self.settings_panel.show(ui, &mut self.config);
                            if self.settings_panel.changed {
                                crate::save_config(&self.config);
                            }
                        }
                    }
                });
        });
    }
}
