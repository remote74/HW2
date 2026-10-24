"""A small reusable asynchronous HTTP client for crawler stages."""

import asyncio
import logging
import fnmatch
import heapq
import time
import random
import json
from datetime import datetime, timezone
from collections import Counter
from contextlib import asynccontextmanager
from typing import Sequence
from urllib.parse import urlsplit, urldefrag
from urllib.robotparser import RobotFileParser

import aiohttp
from src.html_parser import HTMLParser

logger = logging.getLogger(__name__)


class TransientError(Exception):
    """An HTTP or transport failure that may succeed on a later attempt."""


class PermanentError(Exception):
    """A request failure that should not be retried (for example, HTTP 404)."""


class NetworkError(TransientError):
    """A connection or DNS failure that may be transient."""


class ParseError(Exception):
    """A response could not be parsed; parsing failures are not retried."""


class RetryStrategy:
    """Retry selected exception types with bounded, type-specific backoff."""

    def __init__(self, max_retries: int = 3, backoff_factor: float = 2.0,
                 retry_on: list | None = None, *, retry_limits: dict | None = None,
                 backoff_factors: dict | None = None, on_attempt=None):
        if max_retries < 0 or backoff_factor < 0:
            raise ValueError("retry settings must be non-negative")
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on = tuple(retry_on or (TransientError, NetworkError, asyncio.TimeoutError))
        self.retry_limits = dict(retry_limits or {})
        self.backoff_factors = dict(backoff_factors or {})
        self.on_attempt = on_attempt
        self.attempts = []
        self.wait_times = []

    def _limit_for(self, error):
        status = getattr(error, "status", None)
        if status == 500:
            return min(self.max_retries, 1)
        if status == 429:
            return self.max_retries
        for cls, limit in self.retry_limits.items():
            if isinstance(error, cls):
                return limit
        return self.max_retries

    def _delay_for(self, error, retry_number):
        factor = next((value for cls, value in self.backoff_factors.items()
                       if isinstance(error, cls)), self.backoff_factor)
        multiplier = 2 if isinstance(error, TransientError) and getattr(error, "status", None) == 429 else 1
        return factor * (2 ** (retry_number - 1)) * multiplier

    async def execute_with_retry(self, coro, *args, **kwargs):
        attempts_by_type = Counter()
        attempt = 0
        while True:
            attempt += 1
            try:
                result = await coro(*args, **kwargs)
            except Exception as error:
                retryable = isinstance(error, self.retry_on) and not isinstance(error, PermanentError)
                attempts_by_type[type(error)] += 1
                limit = self._limit_for(error)
                should_retry = retryable and attempts_by_type[type(error)] <= limit
                delay = self._delay_for(error, attempts_by_type[type(error)]) if should_retry else 0.0
                record = {"url": kwargs.get("url", args[0] if args else ""), "attempt": attempt,
                          "error_type": type(error).__name__, "error": str(error), "delay": delay}
                self.attempts.append(record)
                logger.warning("Request attempt url=%s attempt=%d error_type=%s delay=%.3fs error=%s",
                               record["url"], attempt, record["error_type"], delay, error)
                if self.on_attempt:
                    self.on_attempt(error, record)
                if not should_retry:
                    raise
                self.wait_times.append(delay)
                if delay > 0:
                    await asyncio.sleep(delay)
            else:
                return result


class RateLimiter:
    """Serialize request start times to enforce global or per-domain rates."""

    def __init__(self, requests_per_second: float = 1.0, per_domain: bool = True):
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self.interval = 1.0 / requests_per_second
        self.per_domain = per_domain
        self._locks = {}
        self._last_request = {}

    async def acquire(self, domain: str = None, minimum_interval: float = 0.0) -> None:
        key = (domain or "").lower() if self.per_domain else "__global__"
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            now = time.perf_counter()
            wait = max(self.interval, minimum_interval) - (now - self._last_request.get(key, 0.0))
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request[key] = time.perf_counter()


class RobotsParser:
    """Fetch and cache robots.txt rules once per origin."""

    def __init__(self, session_getter, user_agent: str = "*"):
        self._session_getter = session_getter
        self.user_agent = user_agent
        self._cache = {}
        self._locks = {}

    async def fetch_robots(self, base_url: str) -> dict:
        parts = urlsplit(base_url)
        origin = f"{parts.scheme}://{parts.netloc}"
        key = origin.lower()
        if key in self._cache:
            return self._cache[key]
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            if key not in self._cache:
                robots_url = origin + "/robots.txt"
                parser = RobotFileParser(robots_url)
                lines = []
                try:
                    session = await self._session_getter()
                    async with session.get(robots_url, headers={"User-Agent": self.user_agent}) as response:
                        if response.status < 400:
                            lines = (await response.text()).splitlines()
                            parser.parse(lines)
                        else:
                            parser.parse([])
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    logger.warning("Could not fetch robots.txt url=%s error=%s", robots_url, exc)
                    parser.parse([])
                # Keep standard RobotFileParser behavior for Crawl-delay, which is
                # supported by some Python versions but not exposed by all of them.
                rules = {"parser": parser, "text": lines,
                         "crawl_delays": self._parse_crawl_delays(lines)}
                self._cache[key] = rules
        return self._cache[key]

    def can_fetch(self, url: str, user_agent: str = "") -> bool:
        rules = self._cache.get(self._origin(url))
        if rules is None:
            return True
        return rules["parser"].can_fetch(user_agent or self.user_agent, url)

    def get_crawl_delay(self, user_agent: str = "") -> float:
        # RobotFileParser may expose crawl_delay in newer Python versions. The
        # parser is domain-specific, so use the most recently fetched entry when
        # called directly; crawler code passes its domain through _delay_for.
        agent = (user_agent or self.user_agent).split("/", 1)[0].lower()
        delays = [item["crawl_delays"].get(agent, item["crawl_delays"].get("*"))
                  for item in self._cache.values()]
        return float(next((delay for delay in delays if delay is not None), 0.0))

    def _delay_for(self, url: str, user_agent: str = "") -> float:
        rules = self._cache.get(self._origin(url))
        if not rules:
            return 0.0
        delays = rules["crawl_delays"]
        agent = (user_agent or self.user_agent).split("/", 1)[0].lower()
        delay = delays.get(agent, delays.get("*"))
        return float(delay or 0.0)

    @staticmethod
    def _parse_crawl_delays(lines: list[str]) -> dict[str, float]:
        # Crawl-delay is a common extension that urllib.robotparser does not
        # expose on every supported Python release. Track group agents directly.
        result = {}
        agents = []
        group_has_directive = False
        for raw in lines:
            line = raw.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue
            name, value = (part.strip() for part in line.split(":", 1))
            if name.lower() == "user-agent":
                agent = value.lower()
                if group_has_directive:
                    agents = []
                    group_has_directive = False
                agents.append(agent)
            elif name.lower() == "crawl-delay" and agents:
                try:
                    for agent in agents:
                        result[agent] = float(value)
                    group_has_directive = True
                except ValueError:
                    continue
            elif name.lower() not in {"user-agent"} and agents:
                group_has_directive = True
        return result

    @staticmethod
    def _origin(url: str) -> str:
        parts = urlsplit(url)
        return f"{parts.scheme}://{parts.netloc}".lower()


class CrawlerQueue:
    """Stable priority queue with URL de-duplication and outcome statistics."""

    def __init__(self):
        self._heap = []
        self._sequence = 0
        self._known = set()
        self.processed = set()
        self.failed = {}

    def add_url(self, url: str, priority: int = 0) -> bool:
        if url in self._known:
            return False
        self._known.add(url)
        heapq.heappush(self._heap, (-priority, self._sequence, url))
        self._sequence += 1
        return True

    async def get_next(self) -> str | None:
        return heapq.heappop(self._heap)[2] if self._heap else None

    def mark_processed(self, url: str) -> None:
        self.processed.add(url)
        self.failed.pop(url, None)

    def mark_failed(self, url: str, error: str) -> None:
        self.failed[url] = error

    def get_stats(self) -> dict:
        return {"queued": len(self._heap), "processed": len(self.processed), "failed": len(self.failed)}


class SemaphoreManager:
    """Enforce global and per-host concurrency limits and track active requests."""

    def __init__(self, global_limit: int, per_domain_limit: int):
        if global_limit < 1 or per_domain_limit < 1:
            raise ValueError("concurrency limits must be at least 1")
        self.global_limit = asyncio.Semaphore(global_limit)
        self.per_domain_limit = per_domain_limit
        self._domain_semaphores = {}
        self.active_tasks = set()

    @asynccontextmanager
    async def limit(self, url: str):
        domain = urlsplit(url).netloc.lower()
        semaphore = self._domain_semaphores.setdefault(domain, asyncio.Semaphore(self.per_domain_limit))
        async with self.global_limit, semaphore:
            task = asyncio.current_task()
            self.active_tasks.add(task)
            try:
                yield
            finally:
                self.active_tasks.discard(task)


class AsyncCrawler:
    """Fetch pages with a pooled aiohttp session and bounded concurrency.

    A failed request is represented by an empty string in the required return
    types. Successful responses return their decoded response body. Call
    :meth:`close` (or use ``async with``) to release the pooled session.
    """

    def __init__(self, max_concurrent: int = 10, html_parser: HTMLParser | None = None,
                 max_depth: int = 2, per_domain_concurrent: int = 2,
                 requests_per_second: float = 1.0, per_domain_rate: bool = True,
                 respect_robots: bool = True, min_delay: float = 0.0,
                 user_agent: str = "AsyncCrawler/1.0", jitter: float = 0.0,
                 max_retries: int = 2, backoff_factor: float = 0.5,
                 connect_timeout: float = 10.0, read_timeout: float = 30.0,
                 total_timeout: float = 60.0, storage=None):
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be at least 1")
        self.max_concurrent = max_concurrent
        if min_delay < 0 or jitter < 0 or max_retries < 0 or backoff_factor < 0:
            raise ValueError("delays and retry settings must be non-negative")
        self.requests_per_second = requests_per_second
        self.rate_limiter = RateLimiter(requests_per_second, per_domain_rate)
        self.respect_robots = respect_robots
        self.min_delay = min_delay
        self.user_agent = user_agent
        self.jitter = jitter
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.connect_timeout, self.read_timeout, self.total_timeout = connect_timeout, read_timeout, total_timeout
        self.error_counts = Counter()
        self.error_details = {}
        self.permanent_error_urls = set()
        self.successful_retries = 0
        self.retry_strategy = RetryStrategy(max_retries, backoff_factor,
                                            retry_limits={NetworkError: max_retries},
                                            on_attempt=self._record_attempt)
        self.robots_parser = RobotsParser(self._get_session, user_agent)
        self.robots_blocked = 0
        self.request_timestamps = []
        self._delay_locks = {}
        self._last_request_by_domain = {}
        self._request_lock = asyncio.Lock()
        self._retry_wait = {}
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session: aiohttp.ClientSession | None = None
        self.html_parser = html_parser or HTMLParser()
        self.storage = storage
        self.storage_stats = {"saved": 0, "failed": 0}
        self._response_info = {}
        self.status_codes = Counter()
        self.max_depth = max_depth
        self.semaphore_manager = SemaphoreManager(max_concurrent, per_domain_concurrent)
        self.queue = CrawlerQueue()
        self.visited_urls = set()
        self.failed_urls = {}
        self.processed_urls = {}
        self.url_depths = {}
        self.progress = {}

    async def fetch_and_parse(self, url: str) -> dict:
        """Fetch and parse a page, preserving its source URL on failures."""
        html = await self.fetch_url(url)
        try:
            page = await self.html_parser.parse_html(html, url)
            status_code, content_type = self._response_info.get(url, (None, ""))
            page.update({"crawled_at": datetime.now(timezone.utc), "status_code": status_code,
                         "content_type": content_type})
            return page
        except Exception as exc:
            error = ParseError(str(exc))
            self.error_counts["ParseError"] += 1
            self.error_details[url] = {"type": "ParseError", "message": str(exc)}
            logger.exception("HTML parse failed url=%s", url)
            return {"url": url, "title": "", "text": "", "links": [],
                    "metadata": {}, "images": [], "headings": [], "tables": [], "lists": [],
                    "error": str(error)}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.total_timeout, connect=self.connect_timeout, sock_read=self.read_timeout)
            connector = aiohttp.TCPConnector(limit=self.max_concurrent)
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self._session

    async def fetch_url(self, url: str) -> str:
        """Fetch one URL; return its body, or ``""`` on an expected failure."""
        logger.info("Starting request url=%s", url)
        parts = urlsplit(url)
        domain = parts.netloc.lower()
        if self.respect_robots:
            await self.robots_parser.fetch_robots(url)
            if not self.robots_parser.can_fetch(url, self.user_agent):
                self.robots_blocked += 1
                logger.info("Blocked by robots.txt url=%s", url)
                return ""
        try:
            async with self._semaphore, self.semaphore_manager.limit(url):
                attempt_number = 0
                async def request(url):
                    nonlocal attempt_number
                    attempt_number += 1
                    required = max(self.min_delay, self.robots_parser._delay_for(url, self.user_agent))
                    combined_delay = required + random.uniform(0.0, self.jitter)
                    await self.rate_limiter.acquire(domain, combined_delay)
                    session = await self._get_session()
                    configured = session.timeout
                    timeout = aiohttp.ClientTimeout(
                        total=configured.total or self.total_timeout,
                        connect=configured.connect or self.connect_timeout,
                        sock_read=configured.sock_read or self.read_timeout)
                    async with self._request_lock:
                        self.request_timestamps.append(time.perf_counter())
                    try:
                        async with session.get(url, headers={"User-Agent": self.user_agent}, timeout=timeout) as response:
                            self.status_codes[response.status] += 1
                            self._response_info[url] = (response.status, response.headers.get("Content-Type", ""))
                            if response.status in (401, 403, 404):
                                raise PermanentError(f"HTTP {response.status} for {url}")
                            if response.status in (429, 500, 503):
                                error = TransientError(f"HTTP {response.status} for {url}")
                                error.status = response.status
                                raise error
                            if response.status >= 400:
                                raise PermanentError(f"HTTP {response.status} for {url}")
                            body = await response.text()
                            logger.info("Request succeeded url=%s status=%s", url, response.status)
                            return body
                    except (PermanentError, TransientError, asyncio.TimeoutError):
                        raise
                    except aiohttp.ClientResponseError as exc:
                        raise PermanentError(f"HTTP {exc.status} for {url}") from exc
                    except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError) as exc:
                        raise NetworkError(str(exc)) from exc
                    except aiohttp.ClientError as exc:
                        raise NetworkError(str(exc)) from exc
                try:
                    result = await self.retry_strategy.execute_with_retry(request, url)
                    if attempt_number > 1:
                        self.successful_retries += 1
                    self.retry_strategy.attempts.append({"url": url, "attempt": attempt_number,
                                                         "error_type": None, "error": None, "delay": 0.0,
                                                         "outcome": "success"})
                    return result
                except (PermanentError, TransientError, NetworkError, asyncio.TimeoutError) as exc:
                    self.error_details[url] = {"type": type(exc).__name__, "message": str(exc)}
                    if isinstance(exc, PermanentError):
                        self.permanent_error_urls.add(url)
                    logger.error("Request failed url=%s error_type=%s error=%s", url, type(exc).__name__, exc)
                    return ""
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.error("Request failed url=%s error_type=%s error=%s", url, type(exc).__name__, exc)
            return ""

    def get_stats(self) -> dict:
        stamps = self.request_timestamps
        gaps = [later - earlier for earlier, later in zip(stamps, stamps[1:])]
        elapsed = stamps[-1] - stamps[0] if len(stamps) > 1 else 0.0
        return {"requests": len(stamps), "request_rate": (len(stamps) - 1) / elapsed if elapsed > 0 else 0.0,
                "average_inter_request_delay": sum(gaps) / len(gaps) if gaps else 0.0,
                "robots_blocked": self.robots_blocked,
                "errors_by_type": dict(self.error_counts),
                "successful_retries": self.successful_retries,
                "average_retry_wait": (sum(self.retry_strategy.wait_times) / len(self.retry_strategy.wait_times)
                                       if self.retry_strategy.wait_times else 0.0),
                "storage_saved": self.storage_stats["saved"],
                "storage_failed": self.storage_stats["failed"],
                "permanent_error_urls": sorted(self.permanent_error_urls)}

    def _record_attempt(self, error, record):
        self.error_counts[type(error).__name__] += 1
        self.error_details[record["url"]] = {"type": type(error).__name__, "message": str(error),
                                              "attempt": record["attempt"], "next_delay": record["delay"]}

    def save_error_report(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as output:
            json.dump({"stats": self.get_stats(), "errors": self.error_details}, output, indent=2)

    async def fetch_urls(self, urls: Sequence[str]) -> dict[str, str]:
        """Fetch a batch concurrently; expected per-URL failures map to ``""``.

        If an unexpected exception escapes a request, close the session before
        re-raising it so partially completed batches do not leak connections.
        """
        try:
            values = await asyncio.gather(*(self.fetch_url(url) for url in urls))
        except Exception:
            await self.close()
            raise
        return dict(zip(urls, values))

    async def crawl(self, start_urls: list[str], max_pages: int = 100, *, max_depth: int | None = None,
                    same_domain_only: bool = False, exclude_patterns: Sequence[str] | None = None,
                    include_patterns: Sequence[str] | None = None, progress_callback=None,
                    _reset_status_codes: bool = True) -> dict:
        """Crawl linked pages with bounded workers, depth and URL filters.

        Progress callbacks receive ``total`` as the number of URLs accepted so
        far. It grows as pages reveal links, so the displayed percentage stays
        bounded while the ETA estimates the work currently known to the queue.
        """
        if _reset_status_codes:
            self.status_codes.clear()
        if max_pages < 1:
            return {}
        depth_limit = self.max_depth if max_depth is None else max_depth
        if depth_limit < 0:
            raise ValueError("max_depth must be non-negative")
        self.queue = CrawlerQueue()
        self.visited_urls, self.failed_urls, self.processed_urls = set(), {}, {}
        self.url_depths = {}
        self.progress = {"processed": 0, "queue_size": 0, "failures": 0,
                         "pages_per_second": 0.0, "total": 0}
        seeds = list(dict.fromkeys(start_urls))
        seed_domains = {urlsplit(url).netloc.lower() for url in seeds}
        scheduled = set()
        pending = 0
        accepted = 0
        work_lock = asyncio.Lock()
        started = time.perf_counter()
        workers = min(self.max_concurrent, max_pages)
        wake = asyncio.Event()

        def allowed(url):
            if not urlsplit(url).scheme.startswith("http"):
                return False
            if same_domain_only and urlsplit(url).netloc.lower() not in seed_domains:
                return False
            if include_patterns and not any(fnmatch.fnmatch(url, pattern) for pattern in include_patterns):
                return False
            return not any(fnmatch.fnmatch(url, pattern) for pattern in (exclude_patterns or ()))

        async def enqueue(url, depth):
            nonlocal pending, accepted
            url = urldefrag(url)[0]
            if depth > depth_limit or not allowed(url) or url in scheduled or accepted >= max_pages:
                return
            scheduled.add(url)
            self.visited_urls.add(url)
            self.url_depths[url] = depth
            self.queue.add_url(url)
            pending += 1
            accepted += 1
            wake.set()

        for url in seeds:
            await enqueue(url, 0)

        async def worker():
            nonlocal pending
            while True:
                url = await self.queue.get_next()
                if url is None:
                    async with work_lock:
                        if pending == 0:
                            return
                    wake.clear()
                    try:
                        await asyncio.wait_for(wake.wait(), timeout=0.1)
                    except asyncio.TimeoutError:
                        pass
                    continue
                try:
                    result = await self.fetch_and_parse(url)
                    status_code = result.get("status_code")
                    if status_code is None or status_code >= 400 or "error" in result:
                        self.failed_urls[url] = self.error_details.get(
                            url, {}).get("message", "Request failed or returned no parseable content")
                        self.queue.mark_failed(url, self.failed_urls[url])
                    else:
                        if self.storage is not None:
                            saved = await self._save_with_retry(result, url)
                            self.storage_stats["saved"] += saved
                        self.processed_urls[url] = result
                        self.queue.mark_processed(url)
                        for link in result.get("links", []):
                            await enqueue(link, self.url_depths[url] + 1)
                except Exception as exc:
                    self.failed_urls[url] = f"{type(exc).__name__}: {exc}"
                    self.queue.mark_failed(url, self.failed_urls[url])
                    logger.exception("Unexpected crawl failure url=%s", url)
                finally:
                    async with work_lock:
                        pending -= 1
                    wake.set()
                    elapsed = max(time.perf_counter() - started, 1e-9)
                    self.progress = {"processed": len(self.processed_urls), "queue_size": self.queue.get_stats()["queued"],
                                     "failures": len(self.failed_urls), "pages_per_second":
                                     (len(self.processed_urls) + len(self.failed_urls)) / elapsed,
                                     "total": accepted}
                    if progress_callback:
                        value = progress_callback(dict(self.progress))
                        if asyncio.iscoroutine(value):
                            await value

        await asyncio.gather(*(worker() for _ in range(workers)))
        if self.storage is not None and callable(getattr(self.storage, "flush", None)):
            for attempt in range(1, self.max_retries + 2):
                try:
                    self.storage_stats["saved"] += int(await self.storage.flush() or 0)
                    break
                except Exception as exc:
                    if attempt > self.max_retries:
                        self.storage_stats["failed"] += 1
                        logger.exception("Final storage batch flush failed")
                        break
                    delay = self.backoff_factor * (2 ** (attempt - 1))
                    logger.warning("Storage flush retry attempt=%d delay=%.3fs error=%s", attempt, delay, exc)
                    if delay:
                        await asyncio.sleep(delay)
        return dict(self.processed_urls)

    async def _save_with_retry(self, result: dict, url: str) -> int:
        for attempt in range(1, self.max_retries + 2):
            try:
                saved = await self.storage.save(result)
                return int(saved if saved is not None else 1)
            except Exception as exc:
                if attempt > self.max_retries:
                    self.storage_stats["failed"] += 1
                    logger.exception("Page storage failed after retries url=%s", url)
                    return 0
                delay = self.backoff_factor * (2 ** (attempt - 1))
                logger.warning("Storage save retry url=%s attempt=%d delay=%.3fs error=%s", url, attempt, delay, exc)
                if delay:
                    await asyncio.sleep(delay)

    async def close(self) -> None:
        """Close the pooled session, if one has been created."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
        if self.storage is not None:
            try:
                flushed = await self.storage.close()
                self.storage_stats["saved"] += int(flushed or 0)
            except Exception:
                self.storage_stats["failed"] += 1
                logger.exception("Failed to close configured storage")

    async def __aenter__(self) -> "AsyncCrawler":
        await self._get_session()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.close()
