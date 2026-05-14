"""
Prajna Ingestion Pipeline

For each scraped PDF item:
1. Upload PDF bytes to Supabase Storage (prajna-raw bucket)
2. Get a signed URL for the uploaded file
3. POST to Prajna /api/admin/ingest-global with the signed URL
   and document metadata — Prajna handles all text extraction,
   chunking, and embedding using its existing pipeline.

This keeps all ingestion logic in one place (Prajna) and means
any improvements to Prajna's ingest pipeline automatically apply
to crawler-ingested documents.
"""
from __future__ import annotations

import os
import time
from datetime import datetime

import requests
from itemadapter import ItemAdapter
from supabase import create_client, Client


class PrajnaIngestionPipeline:

    def __init__(
        self,
        supabase_url: str,
        supabase_key: str,
        bucket: str,
        prajna_url: str,
        ingest_secret: str,
    ):
        self.supabase_url = supabase_url
        self.supabase_key = supabase_key
        self.bucket = bucket
        self.prajna_url = prajna_url.rstrip("/")
        self.ingest_secret = ingest_secret
        self.client: Client | None = None
        self.existing_hashes: set[str] = set()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            supabase_url=os.environ["SUPABASE_URL"],
            supabase_key=os.environ["SUPABASE_SERVICE_KEY"],
            bucket=os.environ.get("SUPABASE_BUCKET", "Prajna_raw"),
            prajna_url=os.environ["PRAJNA_API_URL"],
            ingest_secret=os.environ["PRAJNA_INGEST_SECRET"],
        )

    def open_spider(self, spider):
        self.client = create_client(self.supabase_url, self.supabase_key)

        # Preload existing source hashes for fast dedup
        # Only loads hashes for the current spider's source
        # to keep memory usage small
        source = getattr(spider, "source_name", None)
        query = (
            self.client.table("global_documents")
            .select("source_hash")
            .not_.is_("source_hash", "null")
        )
        if source:
            query = query.eq("source", source)

        resp = query.execute()
        self.existing_hashes = {
            r["source_hash"]
            for r in resp.data
            if r.get("source_hash")
        }
        # Share with spider for in-process dedup
        spider.seen_hashes |= self.existing_hashes
        spider.logger.info(
            "Loaded %d existing hashes for source=%s",
            len(self.existing_hashes),
            source or "all",
        )

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        sha = adapter["sha256"]

        if sha in self.existing_hashes:
            spider.logger.debug("Skip (already ingested): %s", sha)
            return item

        # Build storage path: source/yyyy/mm/sha256.pdf
        doc_date = adapter.get("doc_date") or datetime.utcnow().date().isoformat()
        yyyy = doc_date[:4] if doc_date else "unknown"
        mm = doc_date[5:7] if doc_date and len(doc_date) >= 7 else "unknown"
        source = adapter.get("source", "unknown")
        storage_path = f"{source}/{yyyy}/{mm}/{sha}.pdf"

        # Step 1 — upload PDF to Supabase Storage
        try:
            self.client.storage.from_(self.bucket).upload(
                storage_path,
                adapter["pdf_body"],
                {"content-type": "application/pdf", "upsert": "true"},
            )
            spider.logger.info("Uploaded: %s", storage_path)
        except Exception as exc:
            spider.logger.error(
                "Storage upload failed for %s: %s", sha, exc
            )
            raise

        # Step 2 — get signed URL (1 hour expiry — enough for ingest)
        signed = self.client.storage.from_(self.bucket).create_signed_url(
            storage_path, 3600
        )
        signed_url = signed.get("signedURL") or signed.get("signedUrl")

        if not signed_url:
            spider.logger.error("No signed URL for %s", storage_path)
            raise ValueError(f"Could not get signed URL for {storage_path}")

        # Step 3 — build safe display name from title
        title = adapter.get("title") or sha
        safe_title = (
            title[:80]
            .replace("/", "-")
            .replace("\\", "-")
            .replace(":", "-")
            .strip()
        )

        # Step 4 — POST to Prajna ingest endpoint
        payload = {
            "signed_url": signed_url,
            "file_name": f"{safe_title}.pdf",
            "file_type": "application/pdf",
            "category": adapter.get("category", "general"),
            "display_name": title[:200] if title else safe_title,
            "source": source,
            "source_hash": sha,
            "source_url": adapter.get("detail_url") or adapter.get("pdf_url"),
            "circular_no": adapter.get("circular_no"),
            "doc_date": adapter.get("doc_date"),
        }

        max_retries = 3
        ingested = False

        for attempt in range(max_retries):
            try:
                resp = requests.post(
                    f"{self.prajna_url}/api/admin/ingest-global",
                    json=payload,
                    headers={"x-admin-key": self.ingest_secret},
                    timeout=120,
                )

                if resp.status_code == 200:
                    result = resp.json()
                    spider.logger.info(
                        "Ingested: %s -> doc_id=%s chunks=%s",
                        safe_title,
                        result.get("document_id"),
                        result.get("chunks_created"),
                    )
                    self.existing_hashes.add(sha)
                    ingested = True
                    break

                elif resp.status_code == 409:
                    # Already exists — race condition or late dedup catch
                    spider.logger.info(
                        "Already exists (409): %s", sha
                    )
                    self.existing_hashes.add(sha)
                    ingested = True
                    break

                else:
                    spider.logger.warning(
                        "Ingest attempt %d failed: %s %s",
                        attempt + 1,
                        resp.status_code,
                        resp.text[:300],
                    )

            except requests.RequestException as exc:
                spider.logger.warning(
                    "Request error attempt %d: %s", attempt + 1, exc
                )

            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))

        if not ingested:
            spider.logger.error(
                "Failed to ingest after %d attempts: %s", max_retries, sha
            )

        # Free memory — drop binary body
        adapter["pdf_body"] = None
        return item
