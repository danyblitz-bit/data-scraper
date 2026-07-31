use eframe::egui::{self, Color32, Frame, Margin, Stroke};

use crate::engine::EngineStats;
use crate::types::*;

#[derive(Default)]
pub struct ScraperPanel {
    pub new_job_name: String,
    pub new_job_url: String,
    pub new_selector_name: String,
    pub new_selector_css: String,
    pub editing_job: Option<ScrapeJob>,
    pub show_edit_dialog: bool,
    pub log_messages: Vec<String>,
    pub jobs_modified: bool,
}

impl ScraperPanel {
    pub fn show(
        &mut self,
        ui: &mut egui::Ui,
        jobs: &mut Vec<ScrapeJob>,
        _stats: &EngineStats,
        on_run_job: &mut dyn FnMut(String),
        on_run_all: &mut dyn FnMut(),
    ) {
        ui.heading("Scraper Jobs");
        ui.separator();
        ui.add_space(8.0);

        ui.horizontal(|ui| {
            if ui.button("+ New Job").clicked() {
                let id = uuid::Uuid::new_v4().to_string();
                let job = ScrapeJob {
                    id,
                    name: String::new(),
                    url: String::new(),
                    selectors: Vec::new(),
                    headers: std::collections::HashMap::new(),
                    method: HttpMethod::Get,
                    body: None,
                    interval_minutes: None,
                    max_pages: Some(1),
                    concurrency: 1,
                    proxy: None,
                    user_agent: None,
                    output_format: OutputFormat::Csv,
                    enabled: true,
                };
                self.editing_job = Some(job);
                self.show_edit_dialog = true;
            }
            if ui.button("Run All Jobs").clicked() {
                on_run_all();
            }
        });

        ui.add_space(12.0);

        if jobs.is_empty() {
            Frame::new()
                .fill(Color32::from_rgb(25, 25, 35))
                .corner_radius(8.0)
                .inner_margin(Margin::symmetric(20, 30))
                .show(ui, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.colored_label(Color32::GRAY, "No jobs configured");
                        ui.label("Click '+ New Job' to create your first scraper job");
                    });
                });
        } else {
            let mut job_to_run: Option<String> = None;
            let mut job_to_delete: Option<usize> = None;
            let mut job_to_edit: Option<usize> = None;

            egui::ScrollArea::vertical()
                .max_height(ui.available_height())
                .show(ui, |ui| {
                    for (i, job) in jobs.iter().enumerate() {
                        let frame_color = if job.enabled {
                            Color32::from_rgb(30, 35, 45)
                        } else {
                            Color32::from_rgb(25, 25, 30)
                        };

                        Frame::new()
                            .fill(frame_color)
                            .stroke(Stroke::new(1.0, Color32::from_rgb(50, 55, 65)))
                            .corner_radius(6.0)
                            .inner_margin(Margin::symmetric(12, 8))
                            .show(ui, |ui| {
                                ui.horizontal(|ui| {
                                    ui.strong(&job.name);
                                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                        if ui.button("Run").clicked() {
                                            job_to_run = Some(job.id.clone());
                                        }
                                        if ui.button("Edit").clicked() {
                                            job_to_edit = Some(i);
                                        }
                                        if ui.button("Delete").clicked() {
                                            job_to_delete = Some(i);
                                        }
                                    });
                                });
                                ui.add_space(4.0);
                                ui.label(&job.url);
                                ui.horizontal(|ui| {
                                    ui.label(format!("Selectors: {}", job.selectors.len()));
                                    ui.separator();
                                    ui.label(format!("Max pages: {}", job.max_pages.unwrap_or(1)));
                                    ui.separator();
                                    ui.label(format!("Concurrency: {}", job.concurrency));
                                });
                            });
                        ui.add_space(6.0);
                    }
                });

            if let Some(idx) = job_to_delete {
                jobs.remove(idx);
                self.jobs_modified = true;
            }

            if let Some(idx) = job_to_edit {
                if let Some(job) = jobs.get(idx) {
                    self.editing_job = Some(job.clone());
                    self.show_edit_dialog = true;
                }
            }

            if let Some(job_id) = job_to_run {
                on_run_job(job_id);
            }
        }

        if self.show_edit_dialog {
            self.show_job_editor(ui, jobs);
        }
    }

    fn show_job_editor(&mut self, ui: &mut egui::Ui, jobs: &mut Vec<ScrapeJob>) {
        let mut job = match self.editing_job.clone() {
            Some(j) => j,
            None => return,
        };

        let mut save = false;
        let mut cancel = false;

        egui::Window::new("Edit Job")
            .id(egui::Id::new("job_editor"))
            .resizable(true)
            .default_size([600.0, 500.0])
            .show(ui.ctx(), |ui| {
                egui::Grid::new("job_editor_grid")
                    .num_columns(2)
                    .spacing([8.0, 6.0])
                    .min_col_width(100.0)
                    .show(ui, |ui| {
                        ui.label("Name:");
                        ui.text_edit_singleline(&mut job.name);
                        ui.end_row();
                        ui.label("URL:");
                        ui.text_edit_singleline(&mut job.url);
                        ui.end_row();
                        ui.label("Method:");
                        egui::ComboBox::from_id_salt("http_method")
                            .selected_text(format!("{:?}", job.method))
                            .show_ui(ui, |ui| {
                                ui.selectable_value(&mut job.method, HttpMethod::Get, "GET");
                                ui.selectable_value(&mut job.method, HttpMethod::Post, "POST");
                                ui.selectable_value(&mut job.method, HttpMethod::Put, "PUT");
                                ui.selectable_value(&mut job.method, HttpMethod::Delete, "DELETE");
                            });
                        ui.end_row();
                        ui.label("Max Pages:");
                        let mut max_pages = job.max_pages.unwrap_or(1) as i32;
                        ui.add(egui::Slider::new(&mut max_pages, 1..=1000).text("pages"));
                        job.max_pages = Some(max_pages as u32);
                        ui.end_row();
                        ui.label("Concurrency:");
                        let mut concurrency = job.concurrency as i32;
                        ui.add(egui::Slider::new(&mut concurrency, 1..=100).text("threads"));
                        job.concurrency = concurrency as u32;
                        ui.end_row();
                        ui.label("Interval (min):");
                        let mut interval = job.interval_minutes.unwrap_or(0) as i32;
                        ui.add(egui::Slider::new(&mut interval, 0..=1440).text("0 = manual"));
                        job.interval_minutes = if interval > 0 { Some(interval as u64) } else { None };
                        ui.end_row();
                        ui.label("Proxy:");
                        let mut proxy = job.proxy.clone().unwrap_or_default();
                        ui.text_edit_singleline(&mut proxy);
                        job.proxy = if proxy.is_empty() { None } else { Some(proxy) };
                        ui.end_row();
                        ui.label("Enabled:");
                        ui.checkbox(&mut job.enabled, "");
                        ui.end_row();
                    });

                ui.add_space(16.0);
                ui.separator();
                ui.heading("CSS Selectors");
                ui.add_space(8.0);

                let mut delete_selector: Option<usize> = None;
                for (i, sel) in job.selectors.iter_mut().enumerate() {
                    ui.horizontal(|ui| {
                        ui.label(format!("#{}", i + 1));
                        ui.text_edit_singleline(&mut sel.name);
                        ui.label("CSS:");
                        ui.text_edit_singleline(&mut sel.css_selector);
                        if ui.button("X").clicked() {
                            delete_selector = Some(i);
                        }
                    });
                }
                if let Some(idx) = delete_selector {
                    job.selectors.remove(idx);
                }

                ui.horizontal(|ui| {
                    ui.label("Name:");
                    ui.text_edit_singleline(&mut self.new_selector_name);
                    ui.label("CSS:");
                    ui.text_edit_singleline(&mut self.new_selector_css);
                    if ui.button("Add Selector").clicked() {
                        if !self.new_selector_name.is_empty() && !self.new_selector_css.is_empty() {
                            job.selectors.push(Selector {
                                name: self.new_selector_name.clone(),
                                css_selector: self.new_selector_css.clone(),
                                attribute: None,
                                extract: ExtractType::Text,
                            });
                            self.new_selector_name.clear();
                            self.new_selector_css.clear();
                        }
                    }
                });

                ui.add_space(16.0);
                ui.horizontal(|ui| {
                    if ui.button("Save").clicked() {
                        save = true;
                    }
                    if ui.button("Cancel").clicked() {
                        cancel = true;
                    }
                });
            });

        if save {
            if !job.name.is_empty() && !job.url.is_empty() {
                let is_new = !jobs.iter().any(|j| j.id == job.id);
                if is_new {
                    jobs.push(job);
                } else if let Some(existing) = jobs.iter_mut().find(|j| j.id == job.id) {
                    *existing = job;
                }
                self.show_edit_dialog = false;
                self.editing_job = None;
                self.jobs_modified = true;
            }
        }
        if cancel {
            self.show_edit_dialog = false;
            self.editing_job = None;
        }
    }
}
