# DataScraper

A high-performance desktop web scraper built with Rust and eframe. Extract structured data from any website using CSS selectors, with built-in scheduling, concurrency, and export.

![Platform](https://img.shields.io/badge/platform-Windows-blue)
![Rust](https://img.shields.io/badge/rust-2024-orange)
![License](https://img.shields.io/badge/license-MIT-green)

## Features

- **CSS Selector Extraction** — Pull text, HTML, attributes, links, and images using standard CSS selectors
- **Multi-Format Export** — Auto-export results to CSV or JSON
- **Scheduled Scraping** — Set intervals for recurring data collection
- **Concurrent Requests** — Configurable parallelism with semaphore-based throttling
- **Pagination Support** — Automatically follow "next page" links via selector
- **Proxy Support** — Route requests through HTTP/SOCKS5 proxies
- **Custom Headers & User-Agent** — Override per-job or globally
- **SQLite Storage** — All results persist locally with zero setup
- **Dark/Light/System Themes** — Native Windows feel
- **Import/Export Jobs** — Share scrape configurations as YAML files

## Screenshots

> _Screenshots coming soon_

## Download

Pre-built binaries available in [Releases](../../releases/latest).

Or build from source:

```bash
git clone <repo-url>
cd data-scraper
cargo build --release
```

Binary will be at `target/release/data-scraper.exe` (~17 MB, fully self-contained).

## Usage

1. Launch `data-scraper.exe`
2. Go to **Scraper** tab → click **+ New Job**
3. Enter a URL and add CSS selectors for the data you want
4. Click **Test** to preview, then **Run** to scrape
5. View results in the **Results** tab, export to CSV/JSON from there

### Example: Scraping product prices

```
URL: https://example.com/products
Selector: price
CSS: .product-price
Extract: Text

Selector: name
CSS: .product-title
Extract: Text
```

## Configuration

Settings are stored in `data_scraper.yaml` next to the executable. You can adjust:

- Max concurrent requests (default: 10)
- Request timeout (default: 30s)
- User agent string
- Database and export paths
- Theme (Dark/Light/System)
- Max response size (default: 64 MB)

## Tech Stack

| Component | Library |
|-----------|---------|
| GUI | [eframe](https://github.com/emilk/egui) (egui) |
| Async | Tokio |
| HTTP | reqwest (gzip, brotli, SOCKS) |
| HTML Parser | scraper |
| Storage | SQLite (rusqlite, bundled) |
| Serialization | serde + serde_yaml + serde_json |

## License

MIT
