use eframe::egui::{self, Slider};

use crate::types::{AppConfig, Theme};

#[derive(Default)]
pub struct SettingsPanel {
    pub changed: bool,
}

impl SettingsPanel {
    pub fn show(&mut self, ui: &mut egui::Ui, config: &mut AppConfig) {
        self.changed = false;
        ui.heading("Settings");
        ui.separator();
        ui.add_space(8.0);

        egui::Grid::new("settings_grid")
            .num_columns(2)
            .spacing([12.0, 8.0])
            .min_col_width(200.0)
            .max_col_width(400.0)
            .show(ui, |ui| {
                ui.label("Max Concurrent Requests:");
                let mut val = config.max_concurrent_requests as i32;
                if ui
                    .add(Slider::new(&mut val, 1..=500).text("requests"))
                    .changed()
                {
                    config.max_concurrent_requests = val as u32;
                    self.changed = true;
                }
                ui.end_row();

                ui.label("Request Timeout (sec):");
                let mut val = config.request_timeout_secs as i32;
                if ui
                    .add(Slider::new(&mut val, 1..=300).text("seconds"))
                    .changed()
                {
                    config.request_timeout_secs = val as u64;
                    self.changed = true;
                }
                ui.end_row();

                ui.label("User Agent:");
                if ui.text_edit_singleline(&mut config.user_agent).changed() {
                    self.changed = true;
                }
                ui.end_row();

                ui.label("Database Path:");
                if ui.text_edit_singleline(&mut config.database_path).changed() {
                    self.changed = true;
                }
                ui.end_row();

                ui.label("Export Path:");
                if ui.text_edit_singleline(&mut config.export_path).changed() {
                    self.changed = true;
                }
                ui.end_row();

                ui.label("Theme:");
                let theme_str = match config.theme {
                    Theme::Dark => "Dark",
                    Theme::Light => "Light",
                    Theme::System => "System",
                };
                egui::ComboBox::from_id_salt("theme_select")
                    .selected_text(theme_str)
                    .show_ui(ui, |ui| {
                        if ui
                            .selectable_label(config.theme == Theme::Dark, "Dark")
                            .clicked()
                        {
                            config.theme = Theme::Dark;
                            self.changed = true;
                        }
                        if ui
                            .selectable_label(config.theme == Theme::Light, "Light")
                            .clicked()
                        {
                            config.theme = Theme::Light;
                            self.changed = true;
                        }
                        if ui
                            .selectable_label(config.theme == Theme::System, "System")
                            .clicked()
                        {
                            config.theme = Theme::System;
                            self.changed = true;
                        }
                    });
                ui.end_row();
            });

        ui.add_space(16.0);
        ui.separator();
        ui.add_space(8.0);
        ui.heading("About");
        ui.label("Data Scraper v0.1.0");
        ui.label("High-performance Windows data scraper built with Rust + egui");
        ui.hyperlink_to("GitHub", "https://github.com/datascraper");
    }
}
