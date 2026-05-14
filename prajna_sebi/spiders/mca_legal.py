"""
MCA Notifications and Circulars Spider for Prajna Knowledge Base.

Crawls MCA notification and circular listing pages.

Arguments:
    category   Single category (default: all)
    since      Only process docs newer than YYYY-MM-DD
    max_docs   Stop after ingesting this many docs (default: unlimited)
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, date
from urllib.parse import urljoin

import scrapy
from scrapy.http import Response

MCA_URLS = {
    "notifications": (
        "https://www.mca.gov.in/content/mca/global/en/acts-rules/notifications.html"
    ),
    "circulars": (
        "https://www.mca.gov.in/content/mca/global/en/acts-rules/general-circulars.html"
    ),
    "rules": (
        "https://www.mca.gov.in/content/mca/global/en/acts-rules/company-rules.html"
    ),
}

CATEGORY_MAP = {
    "notifications": "corporate_law",
    "circulars": "corporate_law",
    "rules": "corporate_law",
}


class McaLegalSpider(scrapy.Spider):
    name = "mca_legal"
    source_name = "mca"
    allowed_domains = ["mca.gov.in"]

    custom_settings = {
        "DOWNLOAD_DELAY": 3.0,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "USER_AGENT": (
            "PrajnaLegalBot/1.0 (Legal research crawler for Indian law firms; "
            "contact: aditi@prajna.ai)"
        ),
        "ROBOTSTXT_OBEY": True,
        "RETRY_TIMES": 3,
        "AUTOTHROTTLE_ENABLED": True,
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
            [category]
            if category and category in MCA_URLS
            else list(MCA_URLS.keys())
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
            if self._limit_reached():
                break
            yield scrapy.Request(
                MCA_URLS[cat_name],
                callback=self.parse_listing,
                meta={"category": cat_name},
                dont_filter=True,
            )

    def parse_listing(self, response: Response):
        if self._limit_reached():
            return

        category = response.meta["category"]

        for href in response.css('a[href$=".pdf"]::attr(href)').getall():
            if self._limit_reached():
                return

            pdf_url = urljoin(response.url, href)
            row = response.xpath(
                f'//a[contains(@href, "{href.split("/")[-1]}")]/ancestor::tr[1]'
            )
            row_text = " ".join(row.xpath(".//text()").getall())
            doc_date = self._parse_date(row_text)

            if self.since and doc_date and doc_date < self.since:
                continue

            title = (
                row.xpath(".//a/text()").get()
                or href.split("/")[-1]
            ).strip()

            yield scrapy.Request(
                pdf_url,
                callback=self.parse_pdf,
                meta={
                    "category": category,
                    "title": title,
                    "doc_date": doc_date.isoformat() if doc_date else None,
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
            "source": "mca",
            "category": CATEGORY_MAP.get(cat, "corporate_law"),
            "title": response.meta.get("title", ""),
            "circular_no": None,
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
