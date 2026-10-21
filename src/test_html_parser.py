import unittest

from aiohttp import web
from aiohttp.test_utils import TestServer
from bs4 import BeautifulSoup

from src.async_crawler import AsyncCrawler
from src.html_parser import HTMLParser


SAMPLE = """<!doctype html><html><head><title>Example title</title>
<meta name="description" content="A description">
<meta name="keywords" content="alpha, beta"></head><body>
<main><h1>Welcome</h1><h2>Details</h2><p>Hello <b>world</b>.</p>
<a href="/relative">relative</a><a href="child/page">child</a>
<a href="https://other.test/path">external</a><a href="javascript:alert(1)">bad</a>
<img src="/image.png" alt="A picture"><table><tr><th>Name</th></tr><tr><td>Ada</td></tr></table>
<ul><li>One</li><li>Two</li></ul><ol><li>Three</li></ol></main></body></html>"""


class HTMLParserTests(unittest.IsolatedAsyncioTestCase):
    async def test_structured_extraction_and_absolute_links(self):
        parser = HTMLParser()
        result = await parser.parse_html(SAMPLE, "https://example.test/a/index.html")

        self.assertEqual(result["title"], "Example title")
        self.assertIn("Hello world", result["text"])
        self.assertEqual(result["metadata"]["description"], "A description")
        self.assertEqual(result["metadata"]["keywords"], "alpha, beta")
        self.assertEqual(result["links"], [
            "https://example.test/relative",
            "https://example.test/a/child/page",
            "https://other.test/path",
        ])
        self.assertEqual(result["images"], [{"src": "https://example.test/image.png", "alt": "A picture"}])
        self.assertEqual([h["level"] for h in result["headings"]], ["h1", "h2"])
        self.assertEqual(result["tables"][0]["rows"], [["Name"], ["Ada"]])
        self.assertEqual([item["type"] for item in result["lists"]], ["ul", "ol"])

    async def test_selector_extraction(self):
        parser = HTMLParser()
        soup = BeautifulSoup("<main>Keep<script>ignore()</script></main><footer>Other</footer>", "lxml")
        self.assertEqual(parser.extract_text(soup, "main"), "Keep")

    async def test_malformed_html_returns_partial_data(self):
        parser = HTMLParser()
        result = await parser.parse_html(
            "<html><head><title>Broken</title><body><h1>Partial</h1><a href='/ok'>ok",
            "https://example.test/path",
        )
        self.assertEqual(result["title"], "Broken")
        self.assertEqual(result["headings"][0]["text"], "Partial")
        self.assertEqual(result["links"], ["https://example.test/ok"])

    async def test_crawler_fetch_and_parse(self):
        app = web.Application()

        async def page(request):
            return web.Response(text=SAMPLE, content_type="text/html")

        app.router.add_get("/page", page)
        server = TestServer(app)
        await server.start_server()
        crawler = AsyncCrawler()
        try:
            url = str(server.make_url("/page"))
            result = await crawler.fetch_and_parse(url)
            self.assertEqual(result["url"], url)
            self.assertEqual(result["title"], "Example title")
            self.assertTrue(result["links"])
        finally:
            await crawler.close()
            await server.close()


if __name__ == "__main__":
    unittest.main()
