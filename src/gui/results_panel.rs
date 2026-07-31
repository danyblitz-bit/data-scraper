use eframe::egui::{self, Color32, Frame, Margin};

use crate::storage::export::{export_to_csv, export_to_json};
use crate::types::ScrapeResult;

#[derive(Default)]
pub struct ResultsPanel {
    pub search_query: String,
    pub selected_job_filter: Option<String>,
    pub selected_result: Option<usize>,
    pub last_export: Option<String>,
}

impl ResultsPanel {
    pub fn show(
        &mut self,
        ui: &mut egui::Ui,
        results: &[ScrapeResult],
        job_names: &[(String, String)],
    ) {
        ui.heading("Results");
        ui.separator();
        ui.add_space(8.0);

        ui.horizontal(|ui| {
            ui.label("Filter:");
            ui.text_edit_singleline(&mut self.search_query);
            ui.separator();
            ui.label("Job:");
            for (job_id, job_name) in job_names {
                let is_selected = self.selected_job_filter.as_deref() == Some(job_id);
                if ui.selectable_label(is_selected, job_name).clicked() {
                    self.selected_job_filter = if is_selected {
                        None
                    } else {
                        Some(job_id.clone())
                    };
                }
            }
            if self.selected_job_filter.is_some() {
                if ui.button("Clear filter").clicked() {
                    self.selected_job_filter = None;
                }
            }
        });

        ui.add_space(8.0);

        if results.is_empty() {
            Frame::new()
                .fill(Color32::from_rgb(25, 25, 35))
                .corner_radius(8.0)
                .inner_margin(Margin::symmetric(20, 30))
                .show(ui, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.colored_label(Color32::GRAY, "No results yet");
                        ui.label("Run a scraper job to see results here");
                    });
                });
            return;
        }

        let filtered: Vec<&ScrapeResult> = results
            .iter()
            .filter(|r| {
                if let Some(ref job_filter) = self.selected_job_filter {
                    if r.job_id != *job_filter {
                        return false;
                    }
                }
                if self.search_query.is_empty() {
                    return true;
                }
                let q = self.search_query.to_lowercase();
                r.url.to_lowercase().contains(&q)
                    || r.job_id.to_lowercase().contains(&q)
                    || r.data.iter().any(|d| {
                        d.values().any(|v| v.to_lowercase().contains(&q))
                    })
            })
            .collect();

        let total_records: usize = filtered.iter().map(|r| r.data.len()).sum();

        ui.horizontal(|ui| {
            ui.label(format!(
                "Showing {} results ({} records)",
                filtered.len(),
                total_records
            ));
            if !filtered.is_empty() {
                if ui.button("Export CSV").clicked() {
                    self.export(filtered.clone(), "csv");
                }
                if ui.button("Export JSON").clicked() {
                    self.export(filtered.clone(), "json");
                }
            }
            if let Some(ref path) = self.last_export {
                ui.separator();
                ui.colored_label(Color32::LIGHT_GREEN, format!("Saved: {}", path));
            }
        });

        ui.add_space(8.0);

        egui::ScrollArea::vertical()
            .max_height(ui.available_height())
            .show(ui, |ui| {
                for (i, result) in filtered.iter().enumerate() {
                    let is_selected = self.selected_result == Some(i);
                    Frame::new()
                        .fill(if is_selected {
                            Color32::from_rgb(35, 45, 60)
                        } else {
                            Color32::from_rgb(25, 30, 40)
                        })
                        .corner_radius(4.0)
                        .inner_margin(Margin::symmetric(8, 6))
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                let job_name = job_names
                                    .iter()
                                    .find(|(id, _)| id == &result.job_id)
                                    .map(|(_, name)| name.clone())
                                    .unwrap_or_else(|| result.job_id.clone());
                                ui.colored_label(Color32::LIGHT_BLUE, &job_name);
                                ui.separator();
                                ui.label(format!("{} records", result.data.len()));
                                ui.separator();
                                ui.label(format!("{}ms", result.duration_ms));
                                ui.separator();
                                ui.label(&result.timestamp.format("%Y-%m-%d %H:%M").to_string());
                            });
                            if is_selected {
                                ui.add_space(4.0);
                                ui.label(&result.url);
                                if !result.data.is_empty() {
                                    ui.add_space(4.0);
                                    let first = &result.data[0];
                    let mut table = egui::Grid::new(format!("table_{}", i))
                        .striped(true)
                        .min_col_width(120.0);
                    let keys: Vec<&String> = first.keys().collect();
                    for key in &keys {
                        table = table.min_col_width(
                            180.0_f32.max(
                                key.len() as f32 * 8.0
                            ),
                        );
                    }
                                    table.show(ui, |ui| {
                                        for key in &keys {
                                            ui.strong(*key);
                                        }
                                        ui.end_row();

                                        let max_rows = 20.min(result.data.len());
                                        for row in 0..max_rows {
                                            for key in &keys {
                                                ui.label(
                                                    result.data[row]
                                                        .get(*key)
                                                        .cloned()
                                                        .unwrap_or_default(),
                                                );
                                            }
                                            ui.end_row();
                                        }
                                        if result.data.len() > max_rows {
                                            ui.colored_label(
                                                Color32::GRAY,
                                                format!("... and {} more rows", result.data.len() - max_rows),
                                            );
                                        }
                                    });
                                }
                            }
                        });
                    ui.add_space(4.0);
                }
            });
    }

    fn export(&mut self, filtered: Vec<&ScrapeResult>, format: &str) {
        let results: Vec<ScrapeResult> = filtered.into_iter().cloned().collect();
        let ts = chrono::Local::now().format("%Y%m%d_%H%M%S");
        let path = format!("exports/export_{}.{}", ts, format);

        let result = match format {
            "csv" => export_to_csv(&results, &path),
            _ => export_to_json(&results, &path),
        };

        match result {
            Ok(_) => {
                let abs = std::path::Path::new(&path)
                    .canonicalize()
                    .unwrap_or_else(|_| std::path::PathBuf::from(&path));
                self.last_export = Some(abs.to_string_lossy().to_string());
            }
            Err(e) => self.last_export = Some(format!("Export failed: {}", e)),
        }
    }
}
