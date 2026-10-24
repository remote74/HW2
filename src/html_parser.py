"""Tolerant HTML parsing and structured page extraction."""

import logging
from urllib.parse import urldefrag, urljoin, urlsplit

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class HTMLParser:
    """Extract useful, serializable content from HTML documents."""

    async def parse_html(self, html: str, url: str) -> dict:
        """Parse a document, returning partial structured results on errors."""
        result = {
            "url": url,
            "title": "",
            "text": "",
            "links": [],
            "metadata": {"title": "", "description": "", "keywords": ""},
            "images": [],
            "headings": [],
            "tables": [],
            "lists": [],
        }
        try:
            soup = BeautifulSoup(html or "", "lxml")
            result["title"] = (soup.title.get_text(" ", strip=True) if soup.title else "")
            result["text"] = self.extract_text(soup)
            result["links"] = self.extract_links(soup, url)
            result["metadata"] = self.extract_metadata(soup)
            result["images"] = [
                {"src": urljoin(url, tag.get("src", "").strip()), "alt": tag.get("alt", "")}
                for tag in soup.find_all("img")
                if tag.get("src", "").strip()
                and self._valid_url(urljoin(url, tag.get("src", "").strip()))
            ]
            result["headings"] = [
                {"level": tag.name, "text": tag.get_text(" ", strip=True)}
                for tag in soup.find_all(["h1", "h2", "h3"])
            ]
            result["tables"] = [self._table_data(table) for table in soup.find_all("table")]
            result["lists"] = [self._list_data(tag) for tag in soup.find_all(["ul", "ol"])]
        except Exception:
            logger.warning("HTML parsing failed; returning fields extracted so far url=%s", url, exc_info=True)
        return result

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """Return unique valid HTTP(S) links resolved against ``base_url``."""
        links = []
        seen = set()
        for tag in soup.find_all("a", href=True):
            href = tag.get("href", "").strip()
            if not href:
                continue
            absolute = urljoin(base_url, href)
            absolute = urldefrag(absolute).url
            if self._valid_url(absolute) and absolute not in seen:
                links.append(absolute)
                seen.add(absolute)
        return links

    def extract_text(self, soup: BeautifulSoup, selector: str = None) -> str:
        """Extract visible text from the document or matching CSS selector."""
        target = soup.select(selector) if selector else [soup]
        for element in target:
            for unwanted in element.select("script, style, noscript, template"):
                unwanted.decompose()
        return " ".join(element.get_text(" ", strip=True) for element in target).strip()

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        """Extract title, description, and keywords metadata."""
        def meta_content(*, name=None, prop=None):
            tag = soup.find("meta", attrs={"name": name}) if name else None
            if tag is None and prop:
                tag = soup.find("meta", attrs={"property": prop})
            return (tag.get("content") or "").strip() if tag else ""

        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        return {
            "title": meta_content(name="title") or meta_content(prop="og:title") or title,
            "description": meta_content(name="description") or meta_content(prop="og:description"),
            "keywords": meta_content(name="keywords"),
        }

    @staticmethod
    def _valid_url(url: str) -> bool:
        try:
            parts = urlsplit(url)
            return parts.scheme.lower() in {"http", "https"} and bool(parts.netloc)
        except ValueError:
            return False

    @staticmethod
    def _table_data(table) -> dict:
        rows = []
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"], recursive=False)
            rows.append([cell.get_text(" ", strip=True) for cell in cells])
        return {"rows": rows}

    @staticmethod
    def _list_data(list_tag) -> dict:
        items = [item.get_text(" ", strip=True) for item in list_tag.find_all("li", recursive=False)]
        return {"type": list_tag.name, "items": items}
