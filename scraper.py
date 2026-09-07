"""
scraper.py
----------
Generic scraper: feed it ANY website URL and it comes back with a list of
flat dict records ready to be handed to the Bronze layer.

Strategy (in order of preference, since we don't know the shape of an
arbitrary page ahead of time):
  1. HTML <table> elements  -> one record per table row (record_type='table_row')
  2. Paragraph text         -> one record per <p> (record_type='paragraph')
  3. Links                  -> one record per <a href> (record_type='link')
  4. Meta tags               -> one record per <meta> (record_type='metadata')

Every record carries the same audit columns so Bronze can union them into a
single DataFrame regardless of which extraction path produced them.
"""

import logging
import uuid
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("analyst_world.scraper")

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 AnalystWorldBot/1.0"
    )
}


class ScrapeError(Exception):
    pass


def fetch_html(url: str, timeout: int = 20) -> str:
    """Download the raw HTML for a URL. Raises ScrapeError on failure."""
    try:
        resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or resp.encoding
        return resp.text
    except requests.exceptions.RequestException as exc:
        raise ScrapeError(f"Failed to fetch {url}: {exc}") from exc


def _extract_tables(soup: BeautifulSoup, url: str, scraped_at: str, batch_id: str):
    records = []
    tables = soup.find_all("table")
    for t_idx, table in enumerate(tables):
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        rows = table.find_all("tr")
        for r_idx, row in enumerate(rows):
            # A row made up entirely of <th> cells is the header row itself
            # (already captured in `headers`) - skip it so it isn't also
            # loaded as a data row.
            if row.find_all("td") == [] and row.find_all("th"):
                continue
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells:
                continue
            row_dict = {}
            if headers and len(headers) == len(cells):
                row_dict = dict(zip(headers, cells))
            else:
                row_dict = {f"col_{i}": v for i, v in enumerate(cells)}
            records.append({
                "record_id": str(uuid.uuid4()),
                "record_type": "table_row",
                "table_index": t_idx,
                "row_index": r_idx,
                "content": row_dict,
                "source_url": url,
                "scraped_at": scraped_at,
                "batch_id": batch_id,
            })
    return records


def _extract_paragraphs(soup: BeautifulSoup, url: str, scraped_at: str, batch_id: str):
    records = []
    for idx, p in enumerate(soup.find_all("p")):
        text = p.get_text(strip=True)
        if not text:
            continue
        records.append({
            "record_id": str(uuid.uuid4()),
            "record_type": "paragraph",
            "table_index": None,
            "row_index": idx,
            "content": {"text": text},
            "source_url": url,
            "scraped_at": scraped_at,
            "batch_id": batch_id,
        })
    return records


def _extract_links(soup: BeautifulSoup, url: str, scraped_at: str, batch_id: str):
    records = []
    for idx, a in enumerate(soup.find_all("a", href=True)):
        text = a.get_text(strip=True)
        href = a["href"]
        if not href:
            continue
        records.append({
            "record_id": str(uuid.uuid4()),
            "record_type": "link",
            "table_index": None,
            "row_index": idx,
            "content": {"text": text, "href": href},
            "source_url": url,
            "scraped_at": scraped_at,
            "batch_id": batch_id,
        })
    return records


def _extract_metadata(soup: BeautifulSoup, url: str, scraped_at: str, batch_id: str):
    records = []
    title = soup.title.get_text(strip=True) if soup.title else None
    if title:
        records.append({
            "record_id": str(uuid.uuid4()),
            "record_type": "metadata",
            "table_index": None,
            "row_index": 0,
            "content": {"key": "title", "value": title},
            "source_url": url,
            "scraped_at": scraped_at,
            "batch_id": batch_id,
        })
    for idx, meta in enumerate(soup.find_all("meta")):
        name = meta.get("name") or meta.get("property")
        value = meta.get("content")
        if name and value:
            records.append({
                "record_id": str(uuid.uuid4()),
                "record_type": "metadata",
                "table_index": None,
                "row_index": idx + 1,
                "content": {"key": name, "value": value},
                "source_url": url,
                "scraped_at": scraped_at,
                "batch_id": batch_id,
            })
    return records


def scrape_website(url: str, html: str = None) -> list:
    """Scrape a URL (or pre-fetched HTML, useful for testing) and return a
    flat list of dict records tagged by record_type.

    Tables are prioritized because they carry the most analytically useful,
    already-structured data; paragraphs/links/metadata are captured too so
    nothing is silently dropped for pages without tables.
    """
    scraped_at = datetime.now(timezone.utc).isoformat()
    batch_id = str(uuid.uuid4())

    if html is None:
        html = fetch_html(url)

    soup = BeautifulSoup(html, "lxml")

    records = []
    records.extend(_extract_tables(soup, url, scraped_at, batch_id))
    records.extend(_extract_paragraphs(soup, url, scraped_at, batch_id))
    records.extend(_extract_links(soup, url, scraped_at, batch_id))
    records.extend(_extract_metadata(soup, url, scraped_at, batch_id))

    if not records:
        logger.warning("No extractable content found at %s", url)
    else:
        logger.info("Scraped %d records from %s (batch_id=%s)", len(records), url, batch_id)

    return records
