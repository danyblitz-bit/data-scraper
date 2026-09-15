use crate::engine::EngineStats;
use crate::types::ScrapeResult;
use eframe::egui::{self, Color32, Frame, Margin, Stroke, Vec2};

#[derive(Clone)]
pub struct StatCard {
    pub label: String,
    pub value: String,
    pub color: Color32,
}

impl StatCard {
    pub fn new(label: &str, value: &str, color: Color32) -> Self {
        Self {
            label: label.to_string(),
            value: value.to_string(),
            color,
        }
    }
}

pub fn show_dashboard(
    ui: &mut egui::Ui,
    stats: &EngineStats,
    total_jobs: u64,
    total_results: u64,
    total_bytes: u64,
    avg_duration_ms: f64,
    job_names: &[(String, String)],
    recent: &[ScrapeResult],
    on_run_all: &mut dyn FnMut(),
    on_navigate: &mut dyn FnMut(crate::app::AppView),
) {
    ui.heading("Dashboard");
    ui.separator();
    ui.add_space(10.0);

    let avg_label = if avg_duration_ms <= 0.0 {
        "—".to_owned()
    } else if avg_duration_ms >= 1000.0 {
        format!("{:.2} s", avg_duration_ms / 1000.0)
    } else {
        format!("{:.0} ms", avg_duration_ms)
    };
    let cards = vec![
        StatCard::new(
            "Total Jobs",
            &total_jobs.to_string(),
            Color32::from_rgb(52, 152, 219),
        ),
        StatCard::new(
            "Total Results",
            &total_results.to_string(),
            Color32::from_rgb(46, 204, 113),
        ),
        StatCard::new(
            "Active Jobs",
            &stats.active_jobs.to_string(),
            Color32::from_rgb(155, 89, 182),
        ),
        StatCard::new(
            "Successful Requests",
            &stats.successful_requests.to_string(),
            Color32::from_rgb(39, 174, 96),
        ),
        StatCard::new(
            "Failed Requests",
            &stats.failed_requests.to_string(),
            Color32::from_rgb(231, 76, 60),
        ),
        StatCard::new(
            "Total Decoded Data",
            &format_bytes(total_bytes),
            Color32::from_rgb(243, 156, 18),
        ),
        StatCard::new(
            "Avg Run Duration",
            &avg_label,
            Color32::from_rgb(26, 188, 156),
        ),
    ];

    let narrow = ui.available_width() < 700.0;

    if narrow {
        egui::ScrollArea::horizontal()
            .max_height(140.0)
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    for card in &cards {
                        show_stat_card(ui, card);
                    }
                });
            });
    } else {
        egui::Grid::new("stats_grid")
            .min_col_width(180.0)
            .max_col_width(250.0)
            .spacing([16.0, 12.0])
            .show(ui, |ui| {
                for chunk in cards.chunks(3) {
                    for card in chunk {
                        show_stat_card(ui, card);
                    }
                    ui.end_row();
                }
            });
    }

    ui.add_space(20.0);
    ui.separator();
    ui.add_space(10.0);
    ui.heading("Quick Actions");
    if narrow {
        ui.vertical(|ui| {
            if ui
                .add(egui::Button::new("Run All Jobs").min_size(egui::vec2(24.0, 24.0)))
                .clicked()
            {
                on_run_all();
            }
            if ui
                .add(egui::Button::new("Scraper Jobs").min_size(egui::vec2(24.0, 24.0)))
                .clicked()
            {
                on_navigate(crate::app::AppView::Scraper);
            }
            if ui
                .add(egui::Button::new("Results").min_size(egui::vec2(24.0, 24.0)))
                .clicked()
            {
                on_navigate(crate::app::AppView::Results);
            }
            if ui
                .add(egui::Button::new("Settings").min_size(egui::vec2(24.0, 24.0)))
                .clicked()
            {
                on_navigate(crate::app::AppView::Settings);
            }
        });
    } else {
        ui.horizontal(|ui| {
            if ui.button("Run All Jobs").clicked() {
                on_run_all();
            }
            if ui.button("Scraper Jobs").clicked() {
                on_navigate(crate::app::AppView::Scraper);
            }
            if ui.button("Results").clicked() {
                on_navigate(crate::app::AppView::Results);
            }
            if ui.button("Settings").clicked() {
                on_navigate(crate::app::AppView::Settings);
            }
        });
    }

    ui.add_space(16.0);
    ui.separator();
    ui.add_space(10.0);
    ui.heading("Recent Results");
    if recent.is_empty() {
        ui.colored_label(Color32::GRAY, "No results yet - run a job to get started");
    } else {
        for r in recent.iter().take(5) {
            let job_name = job_names
                .iter()
                .find(|(id, _)| id == &r.job_id)
                .map(|(_, n)| n.clone())
                .unwrap_or_else(|| r.job_id.clone());
            ui.horizontal(|ui| {
                ui.colored_label(Color32::LIGHT_BLUE, &job_name);
                ui.separator();
                ui.label(format!("{} records", r.record_count));
                ui.separator();
                ui.label(crate::gui::fmt_local(&r.timestamp));
            });
        }
    }
}

fn show_stat_card(ui: &mut egui::Ui, card: &StatCard) {
    Frame::new()
        .fill(Color32::from_rgb(30, 30, 40))
        .stroke(Stroke::new(1.0, card.color.gamma_multiply(0.3)))
        .corner_radius(8.0)
        .inner_margin(Margin::symmetric(16, 12))
        .show(ui, |ui| {
            ui.set_min_size(Vec2::new(160.0, 60.0));
            ui.vertical_centered(|ui| {
                ui.label(&card.label);
                ui.add_space(4.0);
                ui.colored_label(card.color, &card.value);
            });
        });
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}
