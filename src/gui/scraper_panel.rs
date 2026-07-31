use eframe::egui::{self, Color32, Frame, Margin, Stroke};
use std::sync::mpsc::Receiver;

use crate::engine::EngineStats;
use crate::types::*;

pub struct TestOutcome {
    pub job_id: String,
    pub result: Result<ScrapeResult, String>,
}

#[derive(Default)]
pub struct ScraperPanel {
    pub new_selector_name: String,
    pub new_selector_css: String,
    pub new_selector_extract: ExtractType,
    pub new_selector_attr: String,
    pub header_rows: Vec<(String, String)>,
    pub editing_job: Option<ScrapeJob>,
    pub show_edit_dialog: bool,
    pub jobs_modified: bool,
    pub editor_just_opened: bool,
    pub test_rx: Option<Receiver<TestOutcome>>,
    pub test_preview: Option<TestOutcome>,
}

impl ScraperPanel {
    pub fn show(
        &mut self,
        ui: &mut egui::Ui,
        jobs: &mut Vec<ScrapeJob>,
        stats: &EngineStats,
        on_run_job: &mut dyn FnMut(String),
        on_run_all: &mut dyn FnMut(),
        on_test_job: &mut dyn FnMut(ScrapeJob),
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
                    next_link_selector: None,
                    concurrency: 1,
                    proxy: None,
                    user_agent: None,
                    output_format: OutputFormat::Csv,
                    enabled: true,
                };
                self.editing_job = Some(job);
                self.show_edit_dialog = true;
                self.editor_just_opened = true;
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
                                    if stats.running_job_ids.contains(&job.id) {
                                        ui.colored_label(Color32::from_rgb(241, 196, 15), "Running...");
                                    } else if let Some(last) = stats.last_runs.get(&job.id) {
                                        let (color, text) = match &last.status {
                                            ScrapeStatus::Success => (
                                                Color32::LIGHT_GREEN,
                                                format!("Last run: {} records", last.data.len()),
                                            ),
                                            ScrapeStatus::Partial => (
                                                Color32::LIGHT_YELLOW,
                                                "Last run: partial".to_string(),
                                            ),
                                            _ => (
                                                Color32::from_rgb(231, 76, 60),
                                                "Last run: failed".to_string(),
                                            ),
                                        };
                                        ui.colored_label(color, text);
                                        if let Some(err) = &last.error {
                                            ui.label(format!("({})", err));
                                        }
                                    }
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
                    self.editor_just_opened = true;
                }
            }

            if let Some(job_id) = job_to_run {
                on_run_job(job_id);
            }
        }

        if self.show_edit_dialog {
            self.show_job_editor(ui, jobs, on_test_job);
        }
    }

    fn show_job_editor(
        &mut self,
        ui: &mut egui::Ui,
        jobs: &mut Vec<ScrapeJob>,
        on_test_job: &mut dyn FnMut(ScrapeJob),
    ) {
        let mut job = match self.editing_job.take() {
            Some(j) => j,
            None => return,
        };

        let mut save = false;
        let mut cancel = false;
        let mut request_test = false;

        if self.editor_just_opened {
            self.header_rows = job
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
        }

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
                        let name_field = ui.text_edit_singleline(&mut job.name);
                        if self.editor_just_opened {
                            name_field.request_focus();
                            self.editor_just_opened = false;
                        }
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
                        ui.label("Next Link CSS:");
                        let mut next_link = job.next_link_selector.clone().unwrap_or_default();
                        ui.add(
                            egui::TextEdit::singleline(&mut next_link)
                                .hint_text("e.g. li.next a"),
                        );
                        job.next_link_selector = if next_link.is_empty() {
                            None
                        } else {
                            Some(next_link)
                        };
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
                        ui.label("User Agent:");
                        let mut ua = job.user_agent.clone().unwrap_or_default();
                        ui.text_edit_singleline(&mut ua);
                        job.user_agent = if ua.is_empty() { None } else { Some(ua) };
                        ui.end_row();
                        if job.method != HttpMethod::Get {
                            ui.label("Body:");
                            let mut body = job.body.clone().unwrap_or_default();
                            ui.add(
                                egui::TextEdit::multiline(&mut body)
                                    .desired_rows(4)
                                    .hint_text("JSON body (Content-Type: application/json if it starts with { or [)"),
                            );
                            job.body = if body.is_empty() { None } else { Some(body) };
                            ui.end_row();
                        }
                        ui.label("Enabled:");
                        ui.checkbox(&mut job.enabled, "");
                        ui.end_row();
                    });

                ui.colored_label(
                    Color32::GRAY,
                    "Pagination: put {page} in the URL (e.g. ?page={page}) or set a Next Link CSS selector (e.g. li.next a) and set Max Pages above",
                );

                ui.add_space(16.0);
                ui.separator();
                ui.heading("Custom Headers");
                ui.add_space(8.0);

                let mut remove_header: Option<usize> = None;
                for (i, (key, value)) in self.header_rows.iter_mut().enumerate() {
                    ui.horizontal(|ui| {
                        ui.label(format!("#{}", i + 1));
                        ui.add(
                            egui::TextEdit::singleline(key)
                                .desired_width(180.0)
                                .hint_text("Header name"),
                        );
                        ui.add(
                            egui::TextEdit::singleline(value)
                                .desired_width(320.0)
                                .hint_text("Value"),
                        );
                        if ui.button("X").clicked() {
                            remove_header = Some(i);
                        }
                    });
                }
                if let Some(idx) = remove_header {
                    self.header_rows.remove(idx);
                }
                ui.horizontal(|ui| {
                    if ui.button("+ Add Header").clicked() {
                        self.header_rows.push((String::new(), String::new()));
                    }
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
                        ui.label("CSS/Path:");
                        ui.text_edit_singleline(&mut sel.css_selector);
                        extract_combo(ui, format!("extract_{}", i), &mut sel.extract);                        if let ExtractType::Attribute(ref mut attr) = sel.extract {
                            ui.label("attr:");
                            ui.text_edit_singleline(attr);
                        }
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
                    ui.label("CSS/Path:");
                    ui.text_edit_singleline(&mut self.new_selector_css);
                    extract_combo(ui, "extract_new".to_string(), &mut self.new_selector_extract);
                    if let ExtractType::Attribute(ref mut attr) = self.new_selector_extract {
                        ui.label("attr:");
                        ui.text_edit_singleline(&mut self.new_selector_attr);
                        attr.clone_from(&self.new_selector_attr);
                    }
                    if ui.button("Add Selector").clicked() {
                        if !self.new_selector_name.is_empty() && !self.new_selector_css.is_empty() {
                            job.selectors.push(Selector {
                                name: self.new_selector_name.clone(),
                                css_selector: self.new_selector_css.clone(),
                                attribute: None,
                                extract: self.new_selector_extract.clone(),
                            });
                            self.new_selector_name.clear();
                            self.new_selector_css.clear();
                            self.new_selector_attr.clear();
                            self.new_selector_extract = ExtractType::Text;
                        }
                    }
                });

                ui.add_space(16.0);
                ui.separator();
                ui.heading("Test Result");
                match &self.test_preview {
                    Some(outcome) if outcome.job_id == job.id => match &outcome.result {
                        Ok(r) => {
                            ui.colored_label(
                                Color32::LIGHT_GREEN,
                                format!("{} records", r.data.len()),
                            );
                            if !r.data.is_empty() {
                                let keys: Vec<&String> = r.data[0].keys().collect();
                                egui::Grid::new("test_result_grid")
                                    .striped(true)
                                    .min_col_width(120.0)
                                    .show(ui, |ui| {
                                        for k in &keys {
                                            ui.strong(*k);
                                        }
                                        ui.end_row();
                                        for row in r.data.iter().take(10) {
                                            for k in &keys {
                                                ui.label(row.get(*k).cloned().unwrap_or_default());
                                            }
                                            ui.end_row();
                                        }
                                        if r.data.len() > 10 {
                                            ui.colored_label(
                                                Color32::GRAY,
                                                format!("... and {} more", r.data.len() - 10),
                                            );
                                        }
                                    });
                            }
                        }
                        Err(e) => {
                            ui.colored_label(
                                Color32::from_rgb(231, 76, 60),
                                format!("Test failed: {}", e),
                            );
                        }
                    },
                    _ => {
                        ui.colored_label(
                            Color32::GRAY,
                            "Click 'Test Selectors' to run the selectors against the URL",
                        );
                    }
                }

                ui.add_space(16.0);
                ui.horizontal(|ui| {
                    if ui.button("Test Selectors").clicked() {
                        request_test = true;
                    }
                    if ui.button("Save").clicked() {
                        save = true;
                    }
                    if ui.button("Cancel").clicked() {
                        cancel = true;
                    }
                });
            });

        if request_test {
            self.editing_job = Some(job.clone());
            on_test_job(job);
        } else if save {
            if !job.name.is_empty() && !job.url.is_empty() {
                job.headers = self
                    .header_rows
                    .iter()
                    .filter(|(k, v)| !k.trim().is_empty() && !v.trim().is_empty())
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let is_new = !jobs.iter().any(|j| j.id == job.id);
                if is_new {
                    jobs.push(job);
                } else if let Some(existing) = jobs.iter_mut().find(|j| j.id == job.id) {
                    *existing = job;
                }
                self.show_edit_dialog = false;
                self.jobs_modified = true;
            } else {
                self.editing_job = Some(job);
            }
        } else if cancel {
            self.show_edit_dialog = false;
        } else {
            self.editing_job = Some(job);
        }
    }
}

fn extract_combo(ui: &mut egui::Ui, id: String, extract: &mut ExtractType) {
    let label = match extract {
        ExtractType::Text => "Text".to_string(),
        ExtractType::Html => "HTML".to_string(),
        ExtractType::Attribute(a) if a.is_empty() => "Attribute".to_string(),
        ExtractType::Attribute(a) => format!("Attr: {}", a),
        ExtractType::Link => "Link".to_string(),
        ExtractType::Image => "Image".to_string(),
    };

    egui::ComboBox::from_id_salt(id)
        .selected_text(label)
        .show_ui(ui, |ui| {
            ui.selectable_value(extract, ExtractType::Text, "Text");
            ui.selectable_value(extract, ExtractType::Html, "HTML");
            ui.selectable_value(extract, ExtractType::Link, "Link");
            ui.selectable_value(extract, ExtractType::Image, "Image");
            ui.selectable_value(
                extract,
                ExtractType::Attribute(String::new()),
                "Attribute",
            );
        });
}
