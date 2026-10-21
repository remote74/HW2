"""Runnable demonstration for Stage 1: asynchronous HTTP fetching."""

import asyncio
import argparse
import logging
import time
import json
import csv
import aiofiles

from src.async_crawler import AsyncCrawler
from aiohttp import web
from src.storage import JSONStorage, CSVStorage, SQLiteStorage
from src.stage7 import main as stage7_main


async def demonstrate_stage_1() -> None:
    # Stage 1 demonstration: sequential versus bounded parallel loading.
    # httpbin delays each response, making the concurrency benefit visible.
    urls = [f"https://httpbin.org/delay/1?request={number}" for number in range(1, 6)]
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    async with AsyncCrawler(max_concurrent=5) as crawler:
        start = time.perf_counter()
        sequential = {}
        for url in urls:
            sequential[url] = await crawler.fetch_url(url)
        sequential_time = time.perf_counter() - start

        start = time.perf_counter()
        parallel = await crawler.fetch_urls(urls)
        parallel_time = time.perf_counter() - start

    print("Sequential request outcomes:")
    for url, body in sequential.items():
        print(f"  {'success' if body else 'failed'}: {url}")
    print("Parallel request outcomes:")
    for url, body in parallel.items():
        print(f"  {'success' if body else 'failed'}: {url}")
    print(f"Sequential elapsed: {sequential_time:.2f}s")
    print(f"Parallel elapsed:  {parallel_time:.2f}s")
    if all(parallel.values()):
        print("With five one-second delayed responses, parallel loading should take about one second instead of about five.")
    else:
        print("Some requests failed; compare elapsed times only for successful runs with network access.")


async def demonstrate_stage_2() -> None:
    # Stage 2 demonstration: fetch pages and summarize extracted structures.
    urls = ["https://example.com/", "https://www.python.org/", "https://httpbin.org/html"]
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    async with AsyncCrawler(max_concurrent=3) as crawler:
        pages = await asyncio.gather(*(crawler.fetch_and_parse(url) for url in urls))

    for page in pages:
        print(f"\n{page['title'] or '(untitled)'} — {page['url']}")
        print("Headings:")
        for heading in page["headings"]:
            print(f"  {heading['level']}: {heading['text']}")
        print("Links:")
        for link in page["links"][:10]:
            print(f"  {link}")
        print(
            f"Summary: {len(page['links'])} links, {len(page['text'])} text characters, "
            f"{len(page['images'])} images"
        )


async def demonstrate_stage_3() -> None:
    # Stage 3 demonstration: bounded link discovery with live crawl statistics.
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    def show_progress(stats):
        print(
            f"Processed {stats['processed']} pages | queued {stats['queue_size']} | "
            f"failed {stats['failures']} | {stats['pages_per_second']:.2f} pages/s",
            end="\r",
            flush=True,
        )

    async with AsyncCrawler(max_concurrent=5, per_domain_concurrent=2, max_depth=1) as crawler:
        pages = await crawler.crawl(
            ["https://example.com/"], max_pages=10, same_domain_only=True,
            progress_callback=show_progress,
        )
    print()
    with open("crawl_results.json", "w", encoding="utf-8") as output:
        json.dump(pages, output, ensure_ascii=False, indent=2, default=str)
    print(f"Saved {len(pages)} parsed pages to crawl_results.json")


async def demonstrate_stage_4() -> None:
    # Stage 4 demonstration: a local site makes the robots block deterministic.
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    async def robots(_request):
        return web.Response(text="User-agent: *\nDisallow: /private\nCrawl-delay: 0.1\n")

    async def page(request):
        if request.path == "/":
            return web.Response(text='<title>Polite demo</title><a href="/public">public</a><a href="/private">private</a>', content_type="text/html")
        return web.Response(text=f"<title>{request.path}</title><p>Demo content</p>", content_type="text/html")

    app = web.Application()
    app.router.add_get("/robots.txt", robots)
    app.router.add_get("/{tail:.*}", page)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    async with AsyncCrawler(max_concurrent=3, requests_per_second=5, respect_robots=True,
                            min_delay=0.1, user_agent="Stage4Demo/1.0") as crawler:
        pages = await crawler.crawl([f"http://127.0.0.1:{port}/"], max_pages=5,
                                    same_domain_only=True, max_depth=1)
        # Also request it directly to make the skip visible in the demo log/stats.
        await crawler.fetch_url(f"http://127.0.0.1:{port}/private")
        print(f"Fetched {len(pages)} pages; delay statistics: {crawler.get_stats()}")
    await runner.cleanup()


async def demonstrate_stage_5() -> None:
    # Stage 5 demonstration: transient responses retry, permanent responses do not.
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    counts = {"flaky": 0}

    async def robots(_request):
        return web.Response(text="User-agent: *\nAllow: /\n")

    async def page(request):
        if request.path == "/flaky":
            counts["flaky"] += 1
            if counts["flaky"] == 1:
                return web.Response(status=503, text="temporary outage")
        if request.path == "/missing":
            return web.Response(status=404, text="not found")
        return web.Response(text=f"<title>{request.path}</title><p>Recovered</p>", content_type="text/html")

    app = web.Application()
    app.router.add_get("/robots.txt", robots)
    app.router.add_get("/{tail:.*}", page)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    root = f"http://127.0.0.1:{port}"
    async with AsyncCrawler(max_concurrent=2, requests_per_second=100, max_retries=2,
                            backoff_factor=0.05, respect_robots=True) as crawler:
        outcomes = await crawler.fetch_urls([root + "/flaky", root + "/missing"])
        crawler.save_error_report("crawl_errors.json")
        print(f"Outcomes: { {url: 'success' if body else 'failed' for url, body in outcomes.items()} }")
        print(f"Retry and error statistics: {crawler.get_stats()}")
        print("Saved detailed error report to crawl_errors.json")
    await runner.cleanup()


async def demonstrate_stage_6() -> None:
    # Stage 6 demonstration: persist a crawl in JSON Lines, CSV and SQLite.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    async def robots(_request):
        return web.Response(text="User-agent: *\nAllow: /\n")

    async def page(request):
        if request.path == "/":
            return web.Response(text='<title>Storage demo</title><p>Special: comma, quote " and newline</p><a href="/child">child</a>', content_type="text/html")
        return web.Response(text='<title>Child page</title><p>Stored asynchronously.</p>', content_type="text/html")

    app = web.Application()
    app.router.add_get("/robots.txt", robots)
    app.router.add_get("/{tail:.*}", page)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    root = f"http://127.0.0.1:{port}/"
    backends = [JSONStorage("crawl_results.json", indent=2),
                CSVStorage("crawl_results.csv"),
                SQLiteStorage("crawl_results.sqlite3", batch_size=10)]
    for storage in backends:
        async with AsyncCrawler(max_concurrent=3, requests_per_second=100,
                                respect_robots=True, storage=storage) as crawler:
            pages = await crawler.crawl([root], max_pages=5, same_domain_only=True, max_depth=1)
            print(f"{type(storage).__name__}: crawled {len(pages)}, persistence {crawler.storage_stats}")
    sqlite = backends[-1]
    await sqlite.init_db()
    saved = await sqlite.fetch_all()
    print(f"Read {len(saved)} records back from SQLite: {[record['url'] for record in saved]}")
    await sqlite.close()
    async with aiofiles.open("crawl_results.json", encoding="utf-8") as source:
        json_rows = json.loads(await source.read())
    async with aiofiles.open("crawl_results.csv", encoding="utf-8", newline="") as source:
        csv_rows = list(csv.DictReader(await source.readlines()))
    print(f"Read {len(json_rows)} records from JSON and {len(csv_rows)} rows from CSV")
    await runner.cleanup()


if __name__ == "__main__":
    if any(argument.startswith("--") for argument in __import__("sys").argv[1:]):
        stage7_main()
        raise SystemExit(0)
    parser = argparse.ArgumentParser(description="Run an asynchronous crawler stage demonstration")
    parser.add_argument("stage", choices=("1", "2", "3", "4", "5", "6", "7"), nargs="?", default="1")
    selected = parser.parse_args().stage
    demos = {"1": demonstrate_stage_1, "2": demonstrate_stage_2, "3": demonstrate_stage_3,
             "4": demonstrate_stage_4, "5": demonstrate_stage_5, "6": demonstrate_stage_6,
             "7": lambda: stage7_main(["--config", "crawler.example.json"])}
    asyncio.run(demos[selected]())
