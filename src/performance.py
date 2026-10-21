"""Local async-vs-sync benchmark for 100/500/1,000 generated pages."""
import asyncio
import json
import time
import tracemalloc
from urllib.request import urlopen

from aiohttp import web
from src.async_crawler import AsyncCrawler


async def run_size(size):
    async def robots(_request):
        return web.Response(text="User-agent: *\nAllow: /\n")

    async def page(request):
        await asyncio.sleep(0.005)  # Small network-like latency makes concurrency measurable.
        return web.Response(text=f"<title>{request.match_info['id']}</title><p>benchmark payload</p>", content_type="text/html")

    app = web.Application()
    app.router.add_get("/robots.txt", robots)
    app.router.add_get("/page/{id}", page)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
    urls = [f"{base}/page/{i}" for i in range(size)]
    try:
        tracemalloc.start()
        async with AsyncCrawler(max_concurrent=50, per_domain_concurrent=50, requests_per_second=10_000,
                                respect_robots=False, max_retries=0) as crawler:
            started = time.perf_counter()
            results = await crawler.crawl(urls, max_pages=size, max_depth=0)
            async_elapsed = time.perf_counter() - started
        _, async_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        # The sync baseline runs in a worker thread so it does not block the local server loop.
        def sync_run():
            for url in urls:
                with urlopen(url, timeout=5) as response:
                    response.read()
        started = time.perf_counter()
        await asyncio.to_thread(sync_run)
        sync_elapsed = time.perf_counter() - started
    finally:
        await runner.cleanup()
    return {"pages": size, "async_success": len(results), "async_seconds": round(async_elapsed, 4),
            "async_pages_per_second": round(len(results) / async_elapsed, 2),
            "sync_seconds": round(sync_elapsed, 4), "speedup": round(sync_elapsed / async_elapsed, 2),
            "async_peak_tracemalloc_mib": round(async_peak / 1024 / 1024, 2)}


async def main():
    rows = [await run_size(size) for size in (100, 500, 1000)]
    print(json.dumps(rows, indent=2))
    with open("PERFORMANCE.md", "w", encoding="utf-8") as output:
        output.write("# Performance measurements\n\n")
        output.write("Synthetic local pages with 5 ms server latency, sequential urllib baseline, bounded aiohttp crawl (50 workers), robots disabled, Python peak allocations via tracemalloc. These figures characterize this local environment and are not internet throughput claims.\n\n")
        output.write("| Pages | Async seconds | Async pages/s | Sync seconds | Speedup | Async peak MiB |\n|---:|---:|---:|---:|---:|---:|\n")
        for row in rows:
            output.write(f"| {row['pages']} | {row['async_seconds']} | {row['async_pages_per_second']} | {row['sync_seconds']} | {row['speedup']}x | {row['async_peak_tracemalloc_mib']} |\n")
        output.write("\nThe benchmark keeps the URL set bounded and reuses one aiohttp session. The primary scaling costs are retained parsed page data and the rate limiter; applications that need lower memory usage can persist records and avoid keeping a large in-memory result mapping.\n")


if __name__ == "__main__":
    asyncio.run(main())
