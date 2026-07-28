---
name: data-scraper
description: >
  Use ONLY when working on the Rust egui Data Scraper project at
  C:\Users\danyb\Documents\Develope\data-scraper. Covers project structure,
  build commands, MSVC linker setup, and egui 0.35 API conventions.
---

# Data Scraper — Rust egui Windows App

High-performance Windows data scraper with native GUI, async I/O, parallel processing, and SQLite storage.

## Tech Stack
- **GUI**: egui 0.35 + eframe 0.35
- **Async**: tokio 1.53
- **HTTP**: reqwest 0.13 (cookies, gzip, brotli, socks, json)
- **HTML parsing**: scraper 0.27 (CSS selectors)
- **DB**: rusqlite 0.40 (bundled, WAL mode)
- **Parallelism**: rayon 1.12
- **Serialization**: serde / serde_json / serde_yaml / csv
- **Config persistence**: YAML

## Project Structure
```
src/
├── main.rs              — entry point, tokio runtime, native options
├── app.rs               — DataScraperApp (eframe::App impl), view routing
├── engine/
│   ├── mod.rs
│   ├── http_client.rs   — pooled HTTP clients, retry logic
│   ├── parser.rs        — HTML/JSON parsing with CSS selectors
│   ├── scraper.rs       — ScraperEngine (semaphore throttled)
│   └── scheduler.rs     — interval-based job scheduler
├── gui/
│   ├── mod.rs
│   ├── dashboard.rs     — stat cards view
│   ├── scraper_panel.rs — job CRUD + editor dialog
│   ├── results_panel.rs — results listing with filtering
│   └── settings.rs      — config grid (concurrency, timeout, theme)
├── storage/
│   ├── mod.rs
│   ├── sqlite.rs        — Storage: jobs, results, raw_data tables
│   └── export.rs        — CSV/JSON export
└── types/
    ├── mod.rs
    └── config.rs        — ScrapeJob, ScrapeResult, AppConfig, enums
```

## Build & Run

**Critical**: MSVC `link.exe` must come before Git for Windows' `link.exe` in PATH.
Git's `C:\Program Files\Git\usr\bin\link.exe` breaks the `cc` crate.

```powershell
# Build (ensure MSVC linker is found first):
$env:PATH = ("C:\Users\danyb\.cargo\bin;" + ($env:PATH -replace 'C:\\Program Files\\Git\\usr\\bin;?',''))
cargo build

# Run:
cargo run
```

MSVC `link.exe` location: `C:\Program Files\Microsoft Visual Studio\18\Community\VC\Tools\MSVC\14.51.36231\bin\Hostx64\x64\link.exe`

## egui 0.35 API Notes
- `eframe::App` trait uses `fn ui(&mut self, ui: &mut egui::Ui, frame: &mut eframe::Frame)` — NOT `update()`
- Panels: `egui::Panel::top("id")`, `egui::Panel::bottom("id")`, `egui::CentralPanel::default()` — `TopBottomPanel` was removed
- `Frame::corner_radius()` replaces `Frame::rounding()`
- `ComboBox::from_id_salt()` replaces `ComboBox::from_id_src()`
- `egui::Id::new()` expects `impl Into<Id>` — String no longer works directly
- Grid: use `egui::Grid::new("id").num_columns(n).spacing([x,y]).min_col_width(w)`

## Git
- Repo at `C:\Users\danyb\Documents\Develope\data-scraper`
- Linked data folder: `C:\Users\danyb\Documents\Develope\Data Scraper`
- Commit convention: English, imperative mood, detailed multi-line messages
