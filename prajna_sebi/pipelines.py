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
        body_len = len(adapter.get("pdf_body") or b"")

        spider.logger.info(
            "[PIPE-ENTER] sha=%s body_bytes=%d title=%r",
            sha[:12], body_len, (adapter.get("title") or "")[:60],
        )

        if sha in self.existing_hashes:
            spider.logger.info("[PIPE-SKIP] already ingested: %s", sha[:12])
            return item

        # Build storage path: source/yyyy/mm/sha256.pdf
        doc_date = adapter.get("doc_date") or datetime.utcnow().date().isoformat()
        yyyy = doc_date[:4] if doc_date else "unknown"
        mm = doc_date[5:7] if doc_date and len(doc_date) >= 7 else "unknown"
        source = adapter.get("source", "unknown")
        storage_path = f"{source}/{yyyy}/{mm}/{sha}.pdf"

        # Step 1 — upload PDF to Supabase Storage
        spider.logger.info("[STEP1-UPLOAD-START] %s", storage_path)
        try:
            upload_result = self.client.storage.from_(self.bucket).upload(
                storage_path,
                adapter["pdf_body"],
                {"content-type": "application/pdf", "upsert": "true"},
            )
            spider.logger.info(
                "[STEP1-UPLOAD-OK] %s result_type=%s",
                storage_path, type(upload_result).__name__,
            )
        except Exception as exc:
            spider.logger.error(
                "[STEP1-UPLOAD-FAIL] %s exc_type=%s msg=%s",
                storage_path, type(exc).__name__, exc,
            )
            return item  # Continue to next item instead of raising

        # Step 2 — get signed URL (1 hour expiry — enough for ingest)
        spider.logger.info("[STEP2-SIGNURL-START] %s", storage_path)
        try:
            signed = self.client.storage.from_(self.bucket).create_signed_url(
                storage_path, 3600
            )
        except Exception as exc:
            spider.logger.error(
                "[STEP2-SIGNURL-EXC] %s exc_type=%s msg=%s",
                storage_path, type(exc).__name__, exc,
            )
            return item

        spider.logger.info(
            "[STEP2-SIGNURL-RESP] type=%s keys=%s repr=%s",
            type(signed).__name__,
            list(signed.keys()) if isinstance(signed, dict) else "N/A",
            repr(signed)[:300],
        )

        signed_url = None
        if isinstance(signed, dict):
            signed_url = (
                signed.get("signedURL")
                or signed.get("signedUrl")
                or signed.get("signed_url")
            )
        elif isinstance(signed, str):
            signed_url = signed

        if not signed_url:
            spider.logger.error(
                "[STEP2-SIGNURL-NONE] %s — could not extract URL", storage_path,
            )
            return item

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

        spider.logger.info(
            "[STEP4-POST-START] sha=%s safe_title=%r",
            sha[:12], safe_title[:60],
        )

        for attempt in range(max_retries):
            try:
                resp = requests.post(
                    f"{self.prajna_url}/api/admin/ingest-global",
                    json=payload,
                    headers={"x-admin-key": self.ingest_secret},
                    timeout=120,
                )

                spider.logger.info(
                    "[STEP4-POST-RESP] attempt=%d status=%d body=%s",
                    attempt + 1, resp.status_code, resp.text[:200],
                )

                if resp.status_code == 200:
                    result = resp.json()
                    spider.logger.info(
                        "[STEP4-POST-OK] sha=%s doc_id=%s chunks=%s",
                        sha[:12],
                        result.get("document_id"),
                        result.get("chunks_created"),
                    )
                    self.existing_hashes.add(sha)
                    ingested = True
                    break

                elif resp.status_code == 409:
                    # Already exists — race condition or late dedup catch
                    spider.logger.info("[STEP4-POST-DUP] sha=%s", sha[:12])
                    self.existing_hashes.add(sha)
                    ingested = True
                    break

                else:
                    spider.logger.warning(
                        "[STEP4-POST-FAIL] attempt=%d status=%d body=%s",
                        attempt + 1, resp.status_code, resp.text[:200],
                    )

            except requests.RequestException as exc:
                spider.logger.warning(
                    "[STEP4-POST-EXC] attempt=%d exc_type=%s msg=%s",
                    attempt + 1, type(exc).__name__, exc,
                )

            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))

        if not ingested:
            spider.logger.error(
                "[STEP4-POST-GIVEUP] sha=%s after %d attempts",
                sha[:12], max_retries,
            )

        # Free memory — drop binary body
        adapter["pdf_body"] = None
        return item
