import asyncio
import csv
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from aiohttp import web

from src.async_crawler import AsyncCrawler
from src.storage import CSVStorage, JSONStorage, SQLiteStorage


RECORD = {"url": "https://example.test/a", "title": 'Hello, "world"',
          "text": "one\ntwo", "links": ["https://example.test/b"],
          "metadata": {"description": "quoted, value"},
          "crawled_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
          "status_code": 200, "content_type": "text/html; charset=utf-8"}


class Stage6StorageTests(unittest.IsolatedAsyncioTestCase):
    async def test_json_lines_round_trip_and_append(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pages.jsonl"
            storage = JSONStorage(path, indent=2)
            await storage.save(RECORD)
            await storage.save(RECORD)
            await storage.close()
            rows = json.loads(path.read_text())
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["links"], RECORD["links"])
            self.assertEqual(rows[0]["metadata"], RECORD["metadata"])
            self.assertTrue(rows[0]["crawled_at"].endswith("+00:00"))

    async def test_csv_headers_special_characters_and_nested_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pages.csv"
            storage = CSVStorage(path)
            await storage.save(RECORD)
            await storage.save(RECORD)
            await storage.close()
            with path.open(encoding="utf-8", newline="") as source:
                rows = list(csv.DictReader(source))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["title"], RECORD["title"])
            self.assertEqual(rows[0]["text"], RECORD["text"])
            self.assertEqual(json.loads(rows[0]["links"]), RECORD["links"])
            self.assertEqual(json.loads(rows[0]["metadata"]), RECORD["metadata"])

    async def test_sqlite_batch_round_trip_and_indexes(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = SQLiteStorage(Path(directory) / "pages.sqlite", batch_size=2)
            await storage.save(RECORD)
            await storage.save({**RECORD, "url": "https://example.test/b"})
            rows = await storage.fetch_all()
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["links"], RECORD["links"])
            self.assertEqual(rows[0]["metadata"], RECORD["metadata"])
            cursor = await storage._connection.execute("PRAGMA index_list(pages)")
            indexes = {row[1] for row in await cursor.fetchall()}
            self.assertIn("idx_pages_url", indexes)
            await storage.close()

    async def test_crawler_storage_failure_does_not_stop_crawl(self):
        async def robots(_request):
            return web.Response(text="User-agent: *\nAllow: /\n")

        async def page(request):
            return web.Response(text=f"<title>{request.path}</title><p>ok</p>", content_type="text/html")

        app = web.Application()
        app.router.add_get("/robots.txt", robots)
        app.router.add_get("/{tail:.*}", page)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        root = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/"

        class FailingStorage:
            async def save(self, _record):
                raise OSError("disk full")

            async def close(self):
                return None

        try:
            async with AsyncCrawler(max_concurrent=2, requests_per_second=100,
                                    storage=FailingStorage()) as crawler:
                result = await crawler.crawl([root], max_pages=1)
                self.assertEqual(len(result), 1)
                self.assertEqual(crawler.storage_stats, {"saved": 0, "failed": 1})
                record = next(iter(result.values()))
                self.assertEqual(record["status_code"], 200)
                self.assertIn("text/html", record["content_type"])
                self.assertIsInstance(record["crawled_at"], datetime)
        finally:
            await runner.cleanup()


if __name__ == "__main__":
    unittest.main()
