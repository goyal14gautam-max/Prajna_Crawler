"""
SEBI Legal Spider for Prajna Knowledge Base.

Crawls https://www.sebi.gov.in/legal.html across 10 categories.
Pattern: category listing -> detail page -> PDF download.

Arguments:
    category   Single category to crawl (default: all)
    since      Only process docs newer than YYYY-MM-DD
    max_docs   Stop after ingesting this many docs (default: unlimited)

Usage:
    scrapy crawl sebi_legal
    scrapy crawl sebi_legal -a category=circulars
    scrapy crawl sebi_legal -a since=2024-01-01 -a max_docs=100
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, date
from urllib.parse import urljoin

import scrapy
from scrapy.http import Response

CATEGORIES = {
    "acts": 1,
    "rules": 2,
    "regulations": 3,
    "general_orders": 4,
    "guidelines": 5,
    "master_circulars": 6,
    "circulars": 7,
    "advisory_guidance": 96,
    "gazette_notification": 82,
    "guidance_notes": 85,
}

LISTING_URL = (
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do"
    "?doListing=yes&sid=1&ssid={ssid}&smid=0"
)

# Category mapping for Prajna knowledge base
CATEGORY_MAP = {
    "acts": "securities_law",
    "rules": "securities_law",
    "regulations": "securities_law",
    "general_orders": "securities_law",
    "guidelines": "securities_law",
    "master_circulars": "securities_law",
    "circulars": "securities_law",
    "advisory_guidance": "securities_law",
    "gazette_notification": "securities_law",
    "guidance_notes": "securities_law",
}


class SebiLegalSpider(scrapy.Spider):
    name = "sebi_legal"
    source_name = "sebi"
    allowed_domains = ["sebi.gov.in"]

    custom_settings = {
        "DOWNLOAD_DELAY": 2.0,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "USER_AGENT": (
            "PrajnaLegalBot/1.0 (Legal research crawler for Indian law firms; "
            "contact: aditi@prajna.ai)"
        ),
        "ROBOTSTXT_OBEY": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504, 522, 524, 408],
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 2,
        "AUTOTHROTTLE_MAX_DELAY": 30,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.5,
        "HTTPCACHE_ENABLED": True,
        "HTTPCACHE_EXPIRATION_SECS": 3600,
    }

    def __init__(
        self,
        category: str | None = None,
        since: str | None = None,
        max_docs: str | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.target_categories = (
            [category] if category else list(CATEGORIES.keys())
        )
        self.since: date | None = (
            datetime.strptime(since, "%Y-%m-%d").date() if since else None
        )
        self.max_docs: int | None = int(max_docs) if max_docs else None
        self.docs_ingested = 0
        self.seen_hashes: set[str] = set()

    def _limit_reached(self) -> bool:
        return self.max_docs is not None and self.docs_ingested >= self.max_docs

    def start_requests(self):
        for cat_name in self.target_categories:
            if cat_name not in CATEGORIES:
                self.logger.warning("Unknown category: %s", cat_name)
                continue
            if self._limit_reached():
                break
            url = LISTING_URL.format(ssid=CATEGORIES[cat_name])
            yield scrapy.Request(
                url,
                callback=self.parse_listing,
                meta={"category": cat_name, "page": 1},
                dont_filter=True,
            )

    def parse_listing(self, response: Response):
        if self._limit_reached():
            return

        category = response.meta["category"]
        page = response.meta["page"]

        detail_links = response.css(
            'a[href*="/legal/"]::attr(href)'
        ).getall()

        seen = set()
        for href in detail_links:
            if self._limit_reached():
                return
            if href in seen:
                continue
            seen.add(href)

            if "HomeAction.do" in href or href.endswith("legal.html"):
                continue

            full_url = urljoin(response.url, href)
            row = response.xpath(
                f'//a[@href="{href}"]/ancestor::tr[1]'
            )
            row_text = " ".join(row.xpath(".//text()").getall())
            doc_date = self._parse_date(row_text)

            if self.since and doc_date and doc_date < self.since:
                continue

            title = (
                row.xpath('.//a[contains(@href, "/legal/")]/text()')
                .get() or ""
            ).strip()

            yield scrapy.Request(
                full_url,
                callback=self.parse_detail,
                meta={
                    "category": category,
                    "listing_title": title,
                    "doc_date": doc_date.isoformat() if doc_date else None,
                },
            )

        # Pagination
        if not self._limit_reached():
            next_href = response.css(
                'a[rel="next"]::attr(href), '
                'a:contains("Next")::attr(href)'
            ).get()
            if next_href:
                yield scrapy.Request(
                    urljoin(response.url, next_href),
                    callback=self.parse_listing,
                    meta={"category": category, "page": page + 1},
                )

    def parse_detail(self, response: Response):
        if self._limit_reached():
            return

        pdf_links = response.css(
            'a[href$=".pdf"]::attr(href), '
            'a[href*="attachdocs"]::attr(href)'
        ).getall()

        title = (
            response.css("h1::text, h2::text").get()
            or response.meta.get("listing_title")
            or ""
        ).strip()

        body_text = " ".join(response.css("body *::text").getall())
        circular_no = self._extract_circular_number(body_text)
        doc_date = (
            response.meta.get("doc_date")
            or (self._parse_date(body_text).isoformat()
                if self._parse_date(body_text) else None)
        )

        for href in pdf_links:
            if self._limit_reached():
                return
            pdf_url = urljoin(response.url, href)
            yield scrapy.Request(
                pdf_url,
                callback=self.parse_pdf,
                meta={
                    "category": response.meta["category"],
                    "title": title,
                    "circular_no": circular_no,
                    "doc_date": doc_date,
                    "detail_url": response.url,
                    "pdf_url": pdf_url,
                },
            )

    def parse_pdf(self, response: Response):
        if self._limit_reached():
            return

        body = response.body
        sha256 = hashlib.sha256(body).hexdigest()

        if sha256 in self.seen_hashes:
            self.logger.info("Skip duplicate: %s", response.url)
            return
        self.seen_hashes.add(sha256)
        self.docs_ingested += 1

        self.logger.info(
            "Document %d/%s: %s",
            self.docs_ingested,
            self.max_docs or "unlimited",
            response.meta.get("title", response.url)[:80],
        )

        cat = response.meta["category"]

        yield {
            "source": "sebi",
            "category": CATEGORY_MAP.get(cat, "securities_law"),
            "title": response.meta.get("title", ""),
            "circular_no": response.meta.get("circular_no"),
            "doc_date": response.meta.get("doc_date"),
            "detail_url": response.meta["detail_url"],
            "pdf_url": response.meta["pdf_url"],
            "sha256": sha256,
            "size_bytes": len(body),
            "pdf_body": body,
            "scraped_at": datetime.utcnow().isoformat(),
        }

    _DATE_PATTERNS = [
        (r"([A-Za-z]{3,9})\s+(\d{1,2}),\s+(\d{4})", "%B %d %Y"),
        (r"(\d{1,2})/(\d{1,2})/(\d{4})", "%d/%m/%Y"),
        (r"(\d{1,2})-([A-Za-z]{3})-(\d{4})", "%d-%b-%Y"),
        (r"(\d{4})-(\d{2})-(\d{2})", "%Y-%m-%d"),
    ]

    def _parse_date(self, text: str) -> date | None:
        if not text:
            return None
        for pattern, fmt in self._DATE_PATTERNS:
            m = re.search(pattern, text)
            if m:
                try:
                    parts = " ".join(m.groups()).replace(",", "")
                    return datetime.strptime(
                        parts, fmt.replace(",", "")
                    ).date()
                except ValueError:
                    continue
        return None

    _CIRCULAR_RE = re.compile(
        r"SEBI/[A-Z0-9]+(?:/[A-Z0-9\-]+){2,5}/\d{4}/\d+",
        re.IGNORECASE,
    )

    def _extract_circular_number(self, text: str) -> str | None:
        m = self._CIRCULAR_RE.search(text or "")
        return m.group(0) if m else None
