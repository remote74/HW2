"""Asynchronous persistence backends for standardized crawler page records.

JSONStorage writes JSON Lines (one complete JSON object per line), making appends
safe and allowing large output files to be consumed incrementally. CSV and SQLite
store ``links`` and ``metadata`` as compact JSON strings, which can be decoded with
``json.loads`` without losing nested values. ``crawled_at`` is ISO-8601 UTC text.
"""
from __future__ import annotations

import csv
import io
import json
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

import aiofiles
import aiosqlite

FIELDS = ("url", "title", "text", "links", "metadata", "crawled_at", "status_code", "content_type")


def _record(data: dict) -> dict:
    result = {key: data.get(key) for key in FIELDS}
    result["links"] = result["links"] if result["links"] is not None else []
    result["metadata"] = result["metadata"] if result["metadata"] is not None else {}
    stamp = result["crawled_at"]
    if isinstance(stamp, datetime):
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        result["crawled_at"] = stamp.astimezone(timezone.utc).isoformat()
    return result


class DataStorage(ABC):
    """Interface implemented by crawler storage backends."""

    @abstractmethod
    async def save(self, data: dict) -> None:
        """Persist one standardized page record."""

    @abstractmethod
    async def close(self) -> None:
        """Flush and release backend resources."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()


class JSONStorage(DataStorage):
    """Append page objects as JSON Lines; optionally pretty-print each object."""

    def __init__(self, filename: str | os.PathLike, *, indent: int | None = None,
                 encoding: str = "utf-8"):
        self.filename = Path(filename)
        self.indent = indent
        self.encoding = encoding
        self._lock = __import__("asyncio").Lock()
        self._count = None
        self._closed = False

    async def _initialize_formatted(self):
        if self._count is not None:
            return
        async with aiofiles.open(self.filename, "a+", encoding=self.encoding) as source:
            await source.seek(0, os.SEEK_END)
            size = await source.tell()
            if size:
                await source.seek(0)
                existing = await source.read()
                parsed = json.loads(existing)
                if not isinstance(parsed, list):
                    raise ValueError("Formatted JSON output must contain a JSON array")
                self._count = len(parsed)
            else:
                self._count = 0
                await source.write("[\n]")

    async def save(self, data: dict) -> None:
        record = _record(data)
        async with self._lock:
            if self._closed:
                raise RuntimeError("storage is closed")
            if self.indent is None:
                line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
                async with aiofiles.open(self.filename, "a", encoding=self.encoding, newline="") as output:
                    await output.write(line + "\n")
            else:
                await self._initialize_formatted()
                line = json.dumps(record, ensure_ascii=False, indent=self.indent)
                async with aiofiles.open(self.filename, "r+", encoding=self.encoding) as output:
                    await output.seek(0)
                    existing = await output.read()
                    closing = existing.rfind("]")
                    prefix = existing[:closing].rstrip()
                    await output.seek(0)
                    await output.write(prefix + (",\n" if self._count else "\n") + line + "\n]")
                    await output.truncate()
                self._count += 1

    async def close(self) -> None:
        self._closed = True


class CSVStorage(DataStorage):
    """Append CSV rows with a stable header and reversible JSON nested fields."""

    def __init__(self, filename: str | os.PathLike, *, encoding: str = "utf-8", delimiter: str = ","):
        if len(delimiter) != 1:
            raise ValueError("CSV delimiter must be exactly one character")
        self.filename = Path(filename)
        self.encoding = encoding
        self.delimiter = delimiter
        self._lock = __import__("asyncio").Lock()

    async def save(self, data: dict) -> None:
        record = _record(data)
        record["links"] = json.dumps(record["links"], ensure_ascii=False, separators=(",", ":"))
        record["metadata"] = json.dumps(record["metadata"], ensure_ascii=False, separators=(",", ":"))
        async with self._lock:
            async with aiofiles.open(self.filename, "a+", encoding=self.encoding, newline="") as output:
                await output.seek(0, os.SEEK_END)
                empty = await output.tell() == 0
                buffer = io.StringIO(newline="")
                writer = csv.DictWriter(buffer, fieldnames=FIELDS, delimiter=self.delimiter, lineterminator="\n")
                if empty:
                    writer.writeheader()
                writer.writerow(record)
                await output.write(buffer.getvalue())

    async def close(self) -> None:
        return None


class SQLiteStorage(DataStorage):
    """SQLite backend with a URL index and asynchronous batched inserts."""

    def __init__(self, filename: str | os.PathLike, *, batch_size: int = 1):
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        self.filename = str(filename)
        self.batch_size = batch_size
        self._connection: aiosqlite.Connection | None = None
        self._pending: list[tuple] = []
        self._lock = __import__("asyncio").Lock()

    async def init_db(self) -> None:
        if self._connection is not None:
            return
        self._connection = await aiosqlite.connect(self.filename)
        await self._connection.execute("""CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT NOT NULL, title TEXT, text TEXT,
            links TEXT NOT NULL, metadata TEXT NOT NULL, crawled_at TEXT,
            status_code INTEGER, content_type TEXT)""")
        await self._connection.execute("CREATE INDEX IF NOT EXISTS idx_pages_url ON pages(url)")
        await self._connection.execute("CREATE INDEX IF NOT EXISTS idx_pages_crawled_at ON pages(crawled_at)")
        await self._connection.commit()

    async def save(self, data: dict) -> None:
        record = _record(data)
        row = (record["url"], record["title"], record["text"],
               json.dumps(record["links"], ensure_ascii=False),
               json.dumps(record["metadata"], ensure_ascii=False), record["crawled_at"],
               record["status_code"], record["content_type"])
        async with self._lock:
            await self.init_db()
            self._pending.append(row)
            if len(self._pending) >= self.batch_size:
                await self._flush()

    async def _flush(self) -> None:
        if self._pending:
            rows, self._pending = self._pending, []
            try:
                await self._connection.executemany(
                    "INSERT INTO pages(url,title,text,links,metadata,crawled_at,status_code,content_type) VALUES(?,?,?,?,?,?,?,?)", rows)
                await self._connection.commit()
            except Exception:
                self._pending = rows + self._pending
                raise

    async def fetch_all(self) -> list[dict]:
        """Read persisted records back as standardized dictionaries."""
        async with self._lock:
            await self.init_db()
            await self._flush()
            cursor = await self._connection.execute("SELECT url,title,text,links,metadata,crawled_at,status_code,content_type FROM pages ORDER BY id")
            rows = await cursor.fetchall()
            await cursor.close()
        return [dict(zip(FIELDS, row[:3] + (json.loads(row[3]), json.loads(row[4])) + row[5:])) for row in rows]

    async def close(self) -> None:
        async with self._lock:
            if self._connection is not None:
                await self._flush()
                await self._connection.close()
                self._connection = None

