import tempfile
import unittest
from pathlib import Path

from aiohttp import web
from aiohttp.test_utils import TestServer

from src.async_crawler import AsyncCrawler, RetryStrategy, TransientError, RobotsParser
from src.html_parser import HTMLParser
from src.stage7 import AdvancedCrawler, CrawlerStats, SitemapParser
from src.storage import SQLiteStorage


class FixRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_retry_forwards_arguments_and_zero_backoff_retries(self):
        strategy = RetryStrategy(max_retries=2, backoff_factor=0)
        calls = []

        async def flaky(value, *, suffix):
            calls.append(value)
            if len(calls) == 1:
                raise TransientError("try again")
            return value + suffix

        self.assertEqual(await strategy.execute_with_retry(flaky, "page", suffix="!"), "page!")
        self.assertEqual(calls, ["page", "page"])

    async def test_crawl_persists_empty_success_and_removes_fragment_duplicates(self):
        requests = {"/": 0, "/empty": 0}
        stored = []

        async def robots(_request):
            return web.Response(text="User-agent: *\nAllow: /\n")

        async def page(request):
            requests[request.path] += 1
            if request.path == "/":
                return web.Response(text='<a href="/empty#one">one</a><a href="/empty#two">two</a>',
                                    content_type="text/html")
            return web.Response(text="", content_type="text/html")

        app = web.Application()
        app.router.add_get("/robots.txt", robots)
        app.router.add_get("/{tail:.*}", page)
        server = TestServer(app)
        await server.start_server()

        class Storage:
            async def save(self, record):
                stored.append(record)
                return 1

            async def close(self):
                return 0

        crawler = AsyncCrawler(requests_per_second=100, respect_robots=True, storage=Storage())
        try:
            root = str(server.make_url("/"))
            results = await crawler.crawl([root], max_pages=4, same_domain_only=True, max_depth=1)
            empty_url = str(server.make_url("/empty"))
            self.assertEqual(requests["/empty"], 1)
            self.assertIn(empty_url, results)
            self.assertEqual(results[empty_url]["text"], "")
            self.assertEqual(len(stored), 2)
            self.assertEqual(crawler.storage_stats["saved"], 2)
            self.assertEqual(len(crawler.status_codes), 1)
            self.assertEqual(crawler.status_codes[200], 2)
        finally:
            await crawler.close()
            await server.close()

    async def test_parser_strips_fragments_and_robots_delay_groups_match_agent_prefix(self):
        parsed = await HTMLParser().parse_html(
            '<a href="/page#first">a</a><a href="/page#second">b</a>',
            "https://example.test/start")
        self.assertEqual(parsed["links"], ["https://example.test/page"])
        delays = RobotsParser._parse_crawl_delays([
            "User-agent: MyBot", "Crawl-delay: 3", "Disallow: /private",
            "User-agent: OtherBot", "Crawl-delay: 9",
        ])
        self.assertEqual(delays, {"mybot": 3.0, "otherbot": 9.0})
        parser = RobotsParser(lambda: None, "MyBot/1.0")
        parser._cache["https://example.test"] = {
            "crawl_delays": delays,
            "parser": __import__("urllib.robotparser", fromlist=["RobotFileParser"]).RobotFileParser(),
        }
        self.assertEqual(parser._delay_for("https://example.test/a", "MyBot/1.0"), 3.0)

    async def test_sqlite_batch_counts_only_committed_rows_and_close_flushes(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = SQLiteStorage(Path(directory) / "pages.db", batch_size=5)
            self.assertEqual(await storage.save({"url": "https://example.test/"}), 0)
            self.assertEqual(await storage.flush(), 1)
            self.assertEqual(await storage.close(), 0)

    async def test_sitemap_nested_failure_does_not_discard_other_entries(self):
        async def fetch(url):
            if url.endswith("index.xml"):
                return ('<sitemapindex><sitemap><loc>https://example.test/bad.xml</loc></sitemap>'
                        '<sitemap><loc>https://example.test/good.xml</loc></sitemap></sitemapindex>')
            if url.endswith("bad.xml"):
                raise __import__("aiohttp").ClientError("temporary sitemap failure")
            return '<urlset><url><loc>https://example.test/page</loc></url></urlset>'

        self.assertEqual(await SitemapParser(request_fetcher=fetch).fetch_sitemap(
            "https://example.test/index.xml"), ["https://example.test/page"])

    def test_status_stats_accept_failed_response_codes(self):
        stats = CrawlerStats()
        stats.record({}, {"https://example.test/missing": "404"}, {404: 1, 503: 2})
        self.assertEqual(stats.as_dict()["status_code_distribution"], {"404": 1, "503": 2})

    async def test_advanced_crawler_status_distribution_is_per_crawl(self):
        async def ok(_request):
            return web.Response(text="<title>ok</title>", content_type="text/html")

        async def missing(_request):
            return web.Response(status=404, text="missing")

        app = web.Application()
        app.router.add_get("/ok", ok)
        app.router.add_get("/missing", missing)
        server = TestServer(app)
        await server.start_server()
        base_url = str(server.make_url("/"))
        crawler = AdvancedCrawler(
            {"respect_robots": False, "rate_limit": 100},
            progress_callback=lambda _progress: None,
        )
        try:
            await crawler.crawl([base_url + "ok"], max_pages=1)
            self.assertEqual(crawler.stats.as_dict()["status_code_distribution"], {"200": 1})

            await crawler.crawl([base_url + "missing"], max_pages=1)
            self.assertEqual(crawler.stats.as_dict()["status_code_distribution"], {"404": 1})
        finally:
            await crawler.close()
            await server.close()


if __name__ == "__main__":
    unittest.main()
