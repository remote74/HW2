# Asynchronous Web Crawler

A modular Python crawler built with `asyncio`, `aiohttp`, BeautifulSoup, bounded workers, politeness controls, retries, and asynchronous persistence.

## Setup

Use a virtual environment and install the dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## CLI

```bash
python main.py --urls https://example.com/ --max-pages 20 --max-depth 1 \
  --rate-limit 2 --respect-robots --output pages.json
python main.py --config crawler.json
```

`--urls` accepts one or more page URLs or sitemap XML URLs. `--output` selects JSON Lines (`.json`/`.jsonl`), CSV (`.csv`), or SQLite (`.sqlite`/`.db`) storage. The command writes `crawl_report.html`, `crawl_stats.json`, and a rotating `crawler.log` by default. Run the older focused demonstrations with `python main.py 1` through `python main.py 6`.

## Configuration

The CLI accepts JSON configuration. Values supplied on the command line override values in the file; omitted command-line options retain file settings. The explicit `--output` path replaces the configured storage entry. `python main.py 7` runs the complete example configured in `crawler.example.json`.

```json
{
  "urls": ["https://example.com/sitemap.xml"],
  "max_pages": 100,
  "max_depth": 2,
  "concurrency": 20,
  "per_domain_concurrent": 2,
  "rate_limit": 1.0,
  "respect_robots": true,
  "min_delay": 0.1,
  "user_agent": "CourseCrawler/1.0 (contact: crawler@example.org)",
  "include_patterns": ["https://example.com/*"],
  "exclude_patterns": ["*logout*"],
  "same_domain_only": true,
  "max_retries": 2,
  "storage": {"type": "sqlite", "path": "pages.sqlite"},
  "logging": {"level": "INFO", "filename": "crawler.log", "max_bytes": 2000000, "backup_count": 3},
  "report": "crawl_report.html",
  "stats_output": "crawl_stats.json"
}
```

The `rate_limit` is requests per second per domain. Sitemap indexes are expanded recursively and duplicate sitemap URLs and page URLs are removed. Progress includes completed pages, rate, estimated remaining time, and active work.

## Python API

`AsyncCrawler` exposes the stage-level fetch, parse, and crawl methods. `AdvancedCrawler(config)` integrates configuration, start URLs, sitemap expansion, filters, storage, progress, and `CrawlerStats`. Use it as an async context manager and call `await crawler.crawl()`; then use `crawler.stats.as_dict()`, `export_to_json(path)`, or `export_to_html_report(path)`. `SitemapParser.fetch_sitemap(url)` returns page URLs from standard sitemap and sitemap-index XML.

Storage implementations live in `storage.py`: `JSONStorage`, `CSVStorage`, and `SQLiteStorage`. JSON stores one complete record per line unless configured with an indent, while CSV and SQLite encode nested links and metadata as JSON strings.

## Performance notes

`performance.py` runs a deterministic local HTTP benchmark at 100, 500, and 1,000 pages. It compares bounded asynchronous crawling with a sequential `urllib` baseline and records elapsed time, throughput, and peak Python allocation measured by `tracemalloc` in `PERFORMANCE.md`. Local measurements isolate crawler overhead from internet variability; throughput is controlled by the configured rate limiter, so use a high rate for the benchmark.
