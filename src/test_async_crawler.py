import asyncio
import time
import unittest

from aiohttp import web
from aiohttp.test_utils import TestServer

from src.async_crawler import AsyncCrawler, CrawlerQueue, SemaphoreManager


class AsyncCrawlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.active = 0
        self.peak_active = 0

        async def success(request):
            self.active += 1
            self.peak_active = max(self.peak_active, self.active)
            await asyncio.sleep(0.08)
            self.active -= 1
            return web.Response(text="page body")

        async def missing(request):
            return web.Response(status=404, text="missing")

        async def slow(request):
            await asyncio.sleep(0.2)
            return web.Response(text="too late")

        app = web.Application()
        app.router.add_get("/ok", success)
        app.router.add_get("/missing", missing)
        app.router.add_get("/slow", slow)
        self.server = TestServer(app)
        await self.server.start_server()
        self.base_url = str(self.server.make_url(""))

    async def asyncTearDown(self):
        await self.server.close()

    async def test_success_and_http_failure(self):
        crawler = AsyncCrawler()
        try:
            self.assertEqual(await crawler.fetch_url(self.base_url + "/ok"), "page body")
            self.assertEqual(await crawler.fetch_url(self.base_url + "/missing"), "")
        finally:
            await crawler.close()
        self.assertTrue(crawler._session.closed)

    async def test_timeout_is_an_individual_failure(self):
        crawler = AsyncCrawler()
        crawler._session = __import__("aiohttp").ClientSession(
            # Long enough for the 80 ms success handler, but shorter than /slow.
            timeout=__import__("aiohttp").ClientTimeout(total=0.12)
        )
        try:
            self.assertEqual(await crawler.fetch_url(self.base_url + "/slow"), "")
            self.assertEqual(await crawler.fetch_url(self.base_url + "/ok"), "page body")
        finally:
            await crawler.close()

    async def test_unreachable_host_is_an_individual_failure(self):
        crawler = AsyncCrawler()
        try:
            self.assertEqual(await crawler.fetch_url("http://127.0.0.1:1/not-running"), "")
        finally:
            await crawler.close()

    async def test_batch_respects_concurrency_and_parallel_is_faster(self):
        urls = [self.base_url + "/ok?item=" + str(index) for index in range(5)]
        crawler = AsyncCrawler(max_concurrent=2)
        try:
            start = time.perf_counter()
            results = await crawler.fetch_urls(urls)
            parallel_time = time.perf_counter() - start
            self.assertEqual(list(results.values()), ["page body"] * 5)
            self.assertLessEqual(self.peak_active, 2)

            start = time.perf_counter()
            for url in urls:
                await crawler.fetch_url(url)
            sequential_time = time.perf_counter() - start
            self.assertLess(parallel_time, sequential_time)
        finally:
            await crawler.close()

    async def test_priority_queue_is_stable_and_deduplicates(self):
        queue = CrawlerQueue()
        queue.add_url("low", 0)
        queue.add_url("high-a", 2)
        queue.add_url("high-b", 2)
        self.assertFalse(queue.add_url("high-a", 9))
        self.assertEqual([await queue.get_next() for _ in range(3)], ["high-a", "high-b", "low"])

    async def test_crawl_depth_filters_and_duplicate_suppression(self):
        app = web.Application()
        pages = {
            "/": '<a href="/a">a</a><a href="/b">b</a><a href="/shared">shared</a><a href="/skip">skip</a>',
            "/a": '<a href="/shared">shared</a><a href="/deep">deep</a>',
            "/b": '<a href="/shared">shared</a>',
            "/skip": "excluded",
            "/shared": "shared page",
            "/deep": "deep page",
        }

        async def page(request):
            return web.Response(text=f"<title>{request.path}</title><p>{pages[request.path]}</p>", content_type="text/html")

        app.router.add_get("/{tail:.*}", page)
        server = TestServer(app)
        await server.start_server()
        crawler = AsyncCrawler(max_concurrent=3, per_domain_concurrent=2, max_depth=1)
        try:
            root = str(server.make_url("/"))
            results = await crawler.crawl([root], max_pages=10, same_domain_only=True,
                                           exclude_patterns=["*/skip"], include_patterns=["*" ])
            self.assertEqual(set(results), {root, str(server.make_url("/a")), str(server.make_url("/b")),
                                            str(server.make_url("/shared"))})
            self.assertIn(str(server.make_url("/shared")), crawler.visited_urls)
            self.assertNotIn(str(server.make_url("/deep")), crawler.visited_urls)
            self.assertNotIn(str(server.make_url("/skip")), crawler.visited_urls)
            self.assertEqual(len(crawler.visited_urls), len(set(crawler.visited_urls)))
        finally:
            await crawler.close()
            await server.close()

    async def test_semaphore_manager_tracks_active_tasks(self):
        manager = SemaphoreManager(2, 1)
        async with manager.limit("https://example.test/a"):
            self.assertEqual(len(manager.active_tasks), 1)
        self.assertEqual(len(manager.active_tasks), 0)


if __name__ == "__main__":
    unittest.main()
