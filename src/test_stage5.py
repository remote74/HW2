import asyncio
import unittest

from aiohttp import web
from aiohttp.test_utils import TestServer
from unittest.mock import patch

from src.async_crawler import AsyncCrawler, RetryStrategy, TransientError


class Stage5Tests(unittest.IsolatedAsyncioTestCase):
    async def test_exponential_backoff(self):
        strategy = RetryStrategy(max_retries=2, backoff_factor=0.01)
        sleeps = []

        async def sleep(delay):
            sleeps.append(delay)

        calls = 0

        async def flaky():
            nonlocal calls
            calls += 1
            if calls < 3:
                raise TransientError("temporary")
            return "ok"

        with patch("async_crawler.asyncio.sleep", side_effect=sleep):
            self.assertEqual(await strategy.execute_with_retry(flaky), "ok")
        self.assertEqual(sleeps, [0.01, 0.02])

    async def asyncSetUp(self):
        self.hits = {}

        async def robots(_request):
            return web.Response(text="User-agent: *\nAllow: /\n")

        async def page(request):
            path = request.path
            self.hits[path] = self.hits.get(path, 0) + 1
            if path == "/flaky" and self.hits[path] < 3:
                return web.Response(status=503, text="retry")
            if path == "/missing":
                return web.Response(status=404, text="missing")
            if path == "/slow":
                await asyncio.sleep(0.08)
            return web.Response(text="ok")

        app = web.Application()
        app.router.add_get("/robots.txt", robots)
        app.router.add_get("/{tail:.*}", page)
        self.server = TestServer(app)
        await self.server.start_server()
        self.root = str(self.server.make_url("/"))[:-1]
        self.crawler = AsyncCrawler(requests_per_second=100, max_retries=2,
                                    backoff_factor=0.001, respect_robots=True,
                                    total_timeout=0.03, read_timeout=0.03)

    async def asyncTearDown(self):
        await self.crawler.close()
        await self.server.close()

    async def test_503_retries_and_records_errors(self):
        self.assertEqual(await self.crawler.fetch_url(self.root + "/flaky"), "ok")
        self.assertEqual(self.hits["/flaky"], 3)
        stats = self.crawler.get_stats()
        self.assertEqual(stats["successful_retries"], 1)
        self.assertEqual(stats["errors_by_type"], {"TransientError": 2})
        self.assertGreater(stats["average_retry_wait"], 0)

    async def test_404_is_not_retried(self):
        self.assertEqual(await self.crawler.fetch_url(self.root + "/missing"), "")
        self.assertEqual(self.hits["/missing"], 1)
        self.assertEqual(self.crawler.get_stats()["permanent_error_urls"], [self.root + "/missing"])

    async def test_timeout_is_retried_and_bounded(self):
        self.assertEqual(await self.crawler.fetch_url(self.root + "/slow"), "")
        self.assertEqual(self.hits["/slow"], 3)
        self.assertEqual(self.crawler.get_stats()["errors_by_type"], {"TimeoutError": 3})


if __name__ == "__main__":
    unittest.main()
