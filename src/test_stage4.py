import asyncio
import time
import unittest

from aiohttp import web
from aiohttp.test_utils import TestServer

from src.async_crawler import AsyncCrawler, RateLimiter, RobotsParser


class Stage4Tests(unittest.IsolatedAsyncioTestCase):
    async def test_rate_limiter_per_domain_and_global(self):
        limiter = RateLimiter(20, per_domain=True)
        start = time.perf_counter()
        await asyncio.gather(*(limiter.acquire("one.test") for _ in range(3)))
        same_domain = time.perf_counter() - start
        self.assertGreaterEqual(same_domain, 0.09)

        limiter = RateLimiter(20, per_domain=True)
        start = time.perf_counter()
        await asyncio.gather(limiter.acquire("one.test"), limiter.acquire("two.test"))
        self.assertLess(time.perf_counter() - start, 0.05)

        limiter = RateLimiter(20, per_domain=False)
        start = time.perf_counter()
        await asyncio.gather(limiter.acquire("one.test"), limiter.acquire("two.test"))
        self.assertGreaterEqual(time.perf_counter() - start, 0.04)

    async def test_robots_cache_blocking_and_crawl_delay(self):
        hits = 0

        async def robots(_request):
            nonlocal hits
            hits += 1
            return web.Response(text="User-agent: TestBot\nDisallow: /private\nCrawl-delay: 0.2\n")

        app = web.Application()
        app.router.add_get("/robots.txt", robots)
        app.router.add_get("/{tail:.*}", lambda _request: web.Response(text="page"))
        server = TestServer(app)
        await server.start_server()
        crawler = AsyncCrawler(user_agent="TestBot", requests_per_second=100, min_delay=0.0)
        try:
            root = str(server.make_url("/"))
            private = str(server.make_url("/private"))
            await crawler.robots_parser.fetch_robots(root)
            await crawler.robots_parser.fetch_robots(root)
            self.assertEqual(hits, 1)
            self.assertFalse(crawler.robots_parser.can_fetch(private, "TestBot"))
            self.assertAlmostEqual(crawler.robots_parser.get_crawl_delay("TestBot"), 0.2)
            self.assertEqual(await crawler.fetch_url(private), "")
            self.assertEqual(crawler.get_stats()["robots_blocked"], 1)
            self.assertEqual(await crawler.fetch_url(root), "page")
            stamps = crawler.request_timestamps
            self.assertEqual(len(stamps), 1)  # robots.txt fetches are excluded
        finally:
            await crawler.close()
            await server.close()

    async def test_minimum_delay_applies_to_request_starts(self):
        async def page(_request):
            return web.Response(text="page")

        app = web.Application()
        app.router.add_get("/robots.txt", lambda _request: web.Response(text="User-agent: *\nAllow: /\n"))
        app.router.add_get("/{tail:.*}", page)
        server = TestServer(app)
        await server.start_server()
        crawler = AsyncCrawler(requests_per_second=100, min_delay=0.06, respect_robots=True)
        try:
            root = str(server.make_url("/"))
            await asyncio.gather(crawler.fetch_url(root + "a"), crawler.fetch_url(root + "b"))
            self.assertGreaterEqual(crawler.request_timestamps[1] - crawler.request_timestamps[0], 0.05)
        finally:
            await crawler.close()
            await server.close()


if __name__ == "__main__":
    unittest.main()
