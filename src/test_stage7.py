import json
import tempfile
import unittest
from pathlib import Path

from aiohttp import web

from src.stage7 import AdvancedCrawler, CrawlerStats, SitemapParser, build_cli_parser, load_config


class Stage7Tests(unittest.IsolatedAsyncioTestCase):
    async def test_advanced_crawler_integrates_parse_stats_and_progress(self):
        async def robots(_request):
            return web.Response(text="User-agent: *\nAllow: /\n")
        async def page(_request):
            return web.Response(text="<title>Integrated</title><p>crawler integration</p>", content_type="text/html")
        app = web.Application()
        app.router.add_get("/robots.txt", robots)
        app.router.add_get("/", page)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/"
        progress = []
        try:
            async with AdvancedCrawler({"urls": [url], "concurrency": 2, "rate_limit": 100,
                                       "respect_robots": True}, progress_callback=progress.append) as crawler:
                pages = await crawler.crawl(max_pages=1)
                self.assertEqual(next(iter(pages.values()))["title"], "Integrated")
                self.assertEqual(crawler.stats.as_dict()["successful_requests"], 1)
                self.assertTrue(progress)
        finally:
            await runner.cleanup()

    async def test_sitemap_index_recursion_and_deduplication(self):
        async def root(_request):
            return web.Response(text='''<s:sitemapindex xmlns:s="http://www.sitemaps.org/schemas/sitemap/0.9">
              <s:sitemap><s:loc>/child.xml</s:loc></s:sitemap><s:sitemap><s:loc>/child.xml</s:loc></s:sitemap>
            </s:sitemapindex>''', content_type="application/xml")
        async def child(_request):
            return web.Response(text='''<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <url><loc>https://example.test/a</loc></url><url><loc>https://example.test/a</loc></url>
            </urlset>''', content_type="application/xml")
        app = web.Application()
        app.router.add_get("/index.xml", root)
        app.router.add_get("/child.xml", child)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/index.xml"
        try:
            self.assertEqual(await SitemapParser().fetch_sitemap(url), ["https://example.test/a"])
        finally:
            await runner.cleanup()

    async def test_stats_exports_and_cli_config(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps({"concurrency": 4, "urls": ["https://example.test"]}))
            self.assertEqual(load_config(str(path))["concurrency"], 4)
            args = build_cli_parser().parse_args(["--urls", "https://example.test", "--max-pages", "7", "--rate-limit", "2"])
            self.assertEqual(args.max_pages, 7)
            self.assertEqual(args.rate_limit, 2)
            stats = CrawlerStats()
            stats.record({"https://example.test/a": {"status_code": 200}}, {"https://other.test": "error"})
            stats.export_to_json(str(Path(directory) / "stats.json"))
            stats.export_to_html_report(str(Path(directory) / "report.html"))
            data = json.loads((Path(directory) / "stats.json").read_text())
            self.assertEqual(data["total_processed_pages"], 2)
            self.assertEqual(data["status_code_distribution"], {"200": 1})
            self.assertIn("Top domains", (Path(directory) / "report.html").read_text())


if __name__ == "__main__":
    unittest.main()
