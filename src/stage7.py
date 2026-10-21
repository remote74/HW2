"""Final integration helpers: configuration, sitemaps, statistics and CLI crawler."""
from __future__ import annotations

import argparse
import asyncio
from html import escape
import json
import logging
from logging.handlers import RotatingFileHandler
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urljoin, urlsplit
import xml.etree.ElementTree as ET

import aiohttp

from src.async_crawler import AsyncCrawler
from src.storage import JSONStorage, CSVStorage, SQLiteStorage


class SitemapParser:
    """Fetch sitemap XML asynchronously and recursively expand indexes."""
    def __init__(self, session: aiohttp.ClientSession | None = None):
        self.session = session
        self._owns_session = session is None

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        visited_maps, urls = set(), []
        async def fetch(url):
            if url in visited_maps:
                return
            visited_maps.add(url)
            session = self.session
            if session is None:
                session = aiohttp.ClientSession()
                self.session = session
            async with session.get(url) as response:
                response.raise_for_status()
                body = await response.read()
            try:
                root = ET.fromstring(body)
            except ET.ParseError as exc:
                raise ValueError(f"Invalid sitemap XML at {url}: {exc}") from exc
            kind = root.tag.rsplit("}", 1)[-1].lower()
            locs = [urljoin(url, (node.text or "").strip()) for node in root.iter()
                    if node.tag.rsplit("}", 1)[-1].lower() == "loc"]
            if kind == "sitemapindex":
                for nested in locs:
                    await fetch(nested)
            elif kind == "urlset":
                urls.extend(loc for loc in locs if loc.startswith(("http://", "https://")))
            else:
                raise ValueError(f"Unsupported sitemap root {kind!r} at {url}")
        try:
            await fetch(sitemap_url)
            return list(dict.fromkeys(urls))
        finally:
            if self._owns_session and self.session is not None:
                await self.session.close()
                self.session = None


class CrawlerStats:
    """Collect crawl outcomes and export portable JSON and self-contained HTML."""
    def __init__(self):
        self.started_at = time.perf_counter()
        self.finished_at = None
        self.successful_requests = 0
        self.failed_requests = 0
        self.status_codes = Counter()
        self.domains = Counter()

    def record(self, pages: dict, failures: dict):
        self.successful_requests = len(pages)
        self.failed_requests = len(failures)
        self.status_codes.clear()
        self.domains.clear()
        for url, page in pages.items():
            status = page.get("status_code")
            if status is not None:
                self.status_codes[str(status)] += 1
            self.domains[urlsplit(url).netloc.lower()] += 1
        self.finished_at = time.perf_counter()

    def as_dict(self):
        runtime = max(0.0, (self.finished_at or time.perf_counter()) - self.started_at)
        processed = self.successful_requests + self.failed_requests
        return {"total_processed_pages": processed, "successful_requests": self.successful_requests,
                "failed_requests": self.failed_requests,
                "average_processing_speed_pages_per_second": processed / runtime if runtime else 0.0,
                "status_code_distribution": dict(self.status_codes),
                "top_domains": [{"domain": domain, "pages": count} for domain, count in self.domains.most_common(10)],
                "total_runtime_seconds": runtime}

    def export_to_json(self, filename: str):
        Path(filename).write_text(json.dumps(self.as_dict(), indent=2), encoding="utf-8")

    def export_to_html_report(self, filename: str):
        stats = self.as_dict()
        rows = "".join(f"<tr><td>{code}</td><td>{count}</td></tr>" for code, count in stats["status_code_distribution"].items()) or "<tr><td colspan='2'>No responses</td></tr>"
        domains = "".join(f"<tr><td>{escape(item['domain'])}</td><td>{item['pages']}</td></tr>" for item in stats["top_domains"]) or "<tr><td colspan='2'>No domains</td></tr>"
        bars = "".join(f"<div class='bar' style='width:{max(3, count/max(1,max(self.status_codes.values(), default=1))*100)}%'>{code}: {count}</div>" for code, count in self.status_codes.items())
        html = f"""<!doctype html><html><head><meta charset='utf-8'><title>Crawl report</title><style>body{{font:16px system-ui;max-width:900px;margin:2rem auto;color:#18212b}}table{{border-collapse:collapse;width:100%;margin:1rem 0 2rem}}td,th{{padding:.55rem;border:1px solid #ccd3da;text-align:left}}.bar{{background:#2376c7;color:white;margin:.4rem 0;padding:.4rem;min-width:3rem}}</style></head><body><h1>Crawl report</h1><table><tbody>{''.join(f'<tr><th>{k.replace("_", " ").title()}</th><td>{v:.3f}' + (' s' if k.endswith('seconds') else '') + '</td></tr>' if isinstance(v, float) else f'<tr><th>{k.replace("_", " ").title()}</th><td>{v}</td></tr>' for k,v in stats.items() if k not in ('status_code_distribution','top_domains'))}</tbody></table><h2>Responses by status</h2>{bars}<table><thead><tr><th>Status</th><th>Pages</th></tr></thead><tbody>{rows}</tbody></table><h2>Top domains</h2><table><thead><tr><th>Domain</th><th>Pages</th></tr></thead><tbody>{domains}</tbody></table></body></html>"""
        Path(filename).write_text(html, encoding="utf-8")


def load_config(path: str | None) -> dict:
    if not path:
        return {}
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError("Configuration root must be a JSON object")
    return config


def configure_logging(level="INFO", filename="crawler.log", max_bytes=2_000_000, backup_count=3):
    root = logging.getLogger()
    root.setLevel(getattr(logging, str(level).upper(), logging.INFO))
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        handler.close()
    for handler in (logging.StreamHandler(), RotatingFileHandler(filename, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")):
        handler.setFormatter(formatter)
        root.addHandler(handler)


class AdvancedCrawler:
    """Integrate stage components with config, sitemap seeds, reporting and progress."""
    def __init__(self, config: dict | None = None, *, storage=None, progress_callback=None):
        self.config = dict(config or {})
        self.progress_callback = progress_callback or self._print_progress
        self.crawler = AsyncCrawler(max_concurrent=self.config.get("concurrency", 10),
            per_domain_concurrent=self.config.get("per_domain_concurrent", 2),
            max_depth=self.config.get("max_depth", 2), requests_per_second=self.config.get("rate_limit", 1.0),
            respect_robots=self.config.get("respect_robots", True), min_delay=self.config.get("min_delay", 0),
            user_agent=self.config.get("user_agent", "AsyncCrawler/1.0"), max_retries=self.config.get("max_retries", 2),
            storage=storage if storage is not None else self._make_storage(self.config.get("storage")))
        self.stats = CrawlerStats()

    @staticmethod
    def _make_storage(config):
        if not config:
            return None
        kind, path = config.get("type", "json").lower(), config.get("path", "crawl_results.json")
        return {"json": JSONStorage, "csv": CSVStorage, "sqlite": SQLiteStorage}[kind](path)

    @staticmethod
    def _print_progress(progress):
        total = progress.get("total", 0)
        done = progress["processed"] + progress["failures"]
        rate = progress.get("pages_per_second", 0)
        eta = max(0, total - done) / rate if rate else 0
        print(f"\r{done}/{total} ({100*done/total if total else 100:.1f}%) | {rate:.2f} pages/s | ETA {eta:.1f}s | active {progress.get('active_tasks', 0)}", end="", flush=True)

    async def crawl(self, urls: list[str] | None = None, *, max_pages=None, max_depth=None):
        urls = list(urls if urls is not None else self.config.get("urls", []))
        sitemap_parser = SitemapParser()
        expanded = []
        for url in urls:
            if urlsplit(url).path.lower().endswith(".xml") or "sitemap" in urlsplit(url).path.lower():
                expanded.extend(await sitemap_parser.fetch_sitemap(url))
            else:
                expanded.append(url)
        urls = list(dict.fromkeys(expanded))
        self.stats = CrawlerStats()
        def progress(value):
            value["total"] = min(max_pages or self.config.get("max_pages", 100), len(urls))
            value["active_tasks"] = len(self.crawler.semaphore_manager.active_tasks)
            self.progress_callback(value)
        result = await self.crawler.crawl(urls, max_pages=max_pages or self.config.get("max_pages", 100),
            max_depth=max_depth, same_domain_only=self.config.get("same_domain_only", False),
            include_patterns=self.config.get("include_patterns"), exclude_patterns=self.config.get("exclude_patterns"),
            progress_callback=progress)
        self.stats.record(result, self.crawler.failed_urls)
        if self.progress_callback == self._print_progress:
            print()
        return result

    async def close(self):
        await self.crawler.close()

    async def __aenter__(self):
        await self.crawler.__aenter__()
        return self

    async def __aexit__(self, *args):
        await self.close()


def build_cli_parser():
    parser = argparse.ArgumentParser(description="Asynchronous web crawler")
    parser.add_argument("--urls", nargs="+", help="Start URLs or sitemap URLs")
    parser.add_argument("--max-pages", type=int)
    parser.add_argument("--max-depth", type=int)
    parser.add_argument("--output", help="JSON, CSV, or SQLite output path")
    parser.add_argument("--config", help="JSON configuration path")
    parser.add_argument("--respect-robots", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--rate-limit", type=float)
    return parser


async def run_cli(args):
    config = load_config(args.config)
    # Explicit CLI values override file values; omitted CLI options preserve config.
    for key, value in (("urls", args.urls), ("max_pages", args.max_pages), ("max_depth", args.max_depth),
                       ("rate_limit", args.rate_limit), ("respect_robots", args.respect_robots)):
        if value is not None:
            config[key] = value
    if args.output:
        suffix = Path(args.output).suffix.lower()
        kind = {".json": "json", ".jsonl": "json", ".csv": "csv", ".sqlite": "sqlite", ".db": "sqlite"}.get(suffix)
        if not kind:
            raise ValueError("--output extension must be .json, .jsonl, .csv, .sqlite or .db")
        config["storage"] = {"type": kind, "path": args.output}
    if not config.get("urls"):
        raise ValueError("Provide at least one start URL with --urls or in the config file")
    logging_config = config.get("logging", {})
    configure_logging(**logging_config)
    async with AdvancedCrawler(config) as crawler:
        await crawler.crawl()
        summary = crawler.stats.as_dict()
        print(json.dumps(summary, indent=2))
        crawler.stats.export_to_html_report(config.get("report", "crawl_report.html"))
        crawler.stats.export_to_json(config.get("stats_output", "crawl_stats.json"))


def main(argv=None):
    args = build_cli_parser().parse_args(argv)
    asyncio.run(run_cli(args))

